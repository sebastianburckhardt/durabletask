//  ----------------------------------------------------------------------------------
//  Copyright Microsoft Corporation
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  ----------------------------------------------------------------------------------

using Microsoft.Azure.EventHubs.Processor;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventSourced.Faster
{
    internal class FasterStorage : StorageAbstraction.IPartitionState
    {
        internal const string UseLocalFileStorage = "UseLocalFileStorage";
        private readonly string connectionString;
        private readonly string taskHubName;
        private readonly ILogger logger;

        private Partition partition;
        private BlobManager blobManager;
        private LogWorker logWorker;
        private StoreWorker storeWorker;
        private FasterLog log;
        private FasterKV store;

        private CancellationToken terminationToken;

        internal FasterTraceHelper TraceHelper { get; private set; }

        public FasterStorage(string connectionString, string taskHubName, ILogger logger)
        {
            this.connectionString = connectionString;
            this.taskHubName = taskHubName;
            this.logger = logger;
        }

        public static Task DeleteTaskhubStorageAsync(string connectionString, string taskHubName)
        {
            return BlobManager.DeleteTaskhubStorageAsync(connectionString, taskHubName);
        }

        public async Task<ulong> CreateOrRestoreAsync(Partition partition, IPartitionErrorHandler errorHandler, ulong firstInputQueuePosition)
        {
            this.partition = partition;
            this.terminationToken = errorHandler.Token;

            this.TraceHelper = new FasterTraceHelper(this.logger, (int)partition.PartitionId);

            if (this.connectionString == UseLocalFileStorage)
            {
                BlobManager.SetLocalFileDirectoryForTestingAndDebugging(true);
            }

            this.blobManager = new BlobManager(this.connectionString, this.taskHubName, this.logger, partition.PartitionId, errorHandler);

            await blobManager.StartAsync();

            var stopwatch = new System.Diagnostics.Stopwatch();
            stopwatch.Start();

            this.log = new FasterLog(this.blobManager);
            this.store = new FasterKV(this.partition, this.blobManager);

            this.storeWorker = new StoreWorker(store, this.partition, this.TraceHelper, this.terminationToken);
            this.logWorker = new LogWorker(this.blobManager, this.log, this.partition, this.storeWorker, this.TraceHelper, this.terminationToken);

            if (this.log.TailAddress == this.log.BeginAddress)
            {
                // take an (empty) checkpoint immediately to ensure the paths are working
                try
                {
                    this.TraceHelper.FasterProgress("creating store");

                    // this is a fresh partition
                    await storeWorker.Initialize(firstInputQueuePosition);

                    await this.TakeCheckpointAsync("initial checkpoint");
                    this.TraceHelper.FasterStoreCreated(storeWorker.InputQueuePosition, stopwatch.ElapsedMilliseconds);

                    this.partition.Assert(!FASTER.core.LightEpoch.AnyInstanceProtected());
                }
                catch (Exception e)
                {
                    this.TraceHelper.FasterStorageError("creating store", e);
                    throw;
                }
            }
            else
            {
                this.TraceHelper.FasterProgress("loading checkpoint");

                try
                {
                    // we are recovering the last checkpoint of the store
                    this.store.Recover();
                    await storeWorker.Recover();

                    this.TraceHelper.FasterCheckpointLoaded(storeWorker.CommitLogPosition, storeWorker.InputQueuePosition, stopwatch.ElapsedMilliseconds);
                }
                catch (Exception e)
                {
                    this.TraceHelper.FasterStorageError("loading checkpoint", e);
                    throw;
                }

                this.partition.Assert(!FASTER.core.LightEpoch.AnyInstanceProtected());
                stopwatch.Restart();

                try
                {
                    if (this.log.TailAddress > (long)storeWorker.CommitLogPosition)
                    {
                        this.TraceHelper.FasterProgress("replaying log");

                        // replay log as the store checkpoint lags behind the log
                        await this.storeWorker.ReplayCommitLog(this.logWorker);

                        this.TraceHelper.FasterLogReplayed(storeWorker.CommitLogPosition, storeWorker.InputQueuePosition, stopwatch.ElapsedMilliseconds);
                    }
                }
                catch (Exception e)
                {
                    this.TraceHelper.FasterStorageError("replaying log", e);
                    throw;
                }

                // restart pending actitivities, timers, work items etc.
                foreach (var key in TrackedObjectKey.GetSingletons())
                {
                    var target = (TrackedObject)await store.GetOrCreate(key);
                    target.OnRecoveryCompleted();
                }

                this.TraceHelper.FasterProgress("recovery complete");
            }

            return storeWorker.InputQueuePosition;
        }

        public async Task CleanShutdown(bool takeFinalCheckpoint)
        {
            this.TraceHelper.FasterProgress("stopping workers");
            
            // in parallel, finish processing log requests and stop processing store requests
            Task t1 = this.logWorker.PersistAndShutdownAsync();
            Task t2 = this.storeWorker.CancelAndShutdown();

            // observe exceptions if the clean shutdown is not working correctly
            await t1;
            await t2;

            // if the the settings indicate we want to take a final checkpoint, do so now.
            if (takeFinalCheckpoint)
            {
                this.TraceHelper.FasterProgress("writing final checkpoint");
                await this.TakeCheckpointAsync("final checkpoint");
            }

            this.TraceHelper.FasterProgress("stopping BlobManager");
            await this.blobManager.StopAsync();
        }

        private async ValueTask TakeCheckpointAsync(string reason)
        {
            var stopwatch = new System.Diagnostics.Stopwatch();
            stopwatch.Start();
            bool success = this.store.TakeFullCheckpoint(out var checkpointGuid);
            if (success)
            {
                this.TraceHelper.FasterCheckpointStarted(checkpointGuid, reason, this.storeWorker.CommitLogPosition, this.storeWorker.InputQueuePosition);
            }
            else
            {
                this.TraceHelper.FasterProgress($"checkpoint skipped: {reason}");
            }
            await this.store.CompleteCheckpointAsync();
            if (success)
            {
                this.TraceHelper.FasterCheckpointPersisted(checkpointGuid, reason, this.storeWorker.CommitLogPosition, this.storeWorker.InputQueuePosition, stopwatch.ElapsedMilliseconds);
            }
        }


        public void ScheduleRead(StorageAbstraction.IReadContinuation readContinuation)
        {
            this.storeWorker.Submit(readContinuation);
        }

        public void SubmitInputEvents(IEnumerable<PartitionEvent> evts)
        {
            this.logWorker.SubmitIncomingBatch(evts.Where(e => e is IPartitionEventWithSideEffects));
            this.storeWorker.SubmitIncomingBatch(evts.Where(e => e is IReadonlyPartitionEvent));
        }

        public void Submit(PartitionEvent evt)
        {
            if (evt is IPartitionEventWithSideEffects)
            {
                this.logWorker.Submit(evt);
            }
            else
            {
                partition.Assert(evt is IReadonlyPartitionEvent);
                this.storeWorker.Submit(evt);
            }
        }
    }
}