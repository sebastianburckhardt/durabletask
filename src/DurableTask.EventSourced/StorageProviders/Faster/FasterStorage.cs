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
       
        internal TraceHelper TraceHelper { get; private set; }

        private volatile bool shuttingDown;

        public CancellationToken OwnershipCancellationToken { get; private set; }

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

        public async Task<ulong> CreateOrRestoreAsync(Partition partition, CancellationToken token)
        {
            this.partition = partition;
            this.TraceHelper = new TraceHelper(this.logger, (int) partition.PartitionId);

            if (this.connectionString == UseLocalFileStorage)
            {
                BlobManager.SetLocalFileDirectoryForTestingAndDebugging(true);
            }
            this.blobManager = new BlobManager(this.connectionString, this.taskHubName, this.logger, partition.PartitionId);

            await blobManager.StartAsync();
            this.OwnershipCancellationToken = await blobManager.AcquireOwnership(token);

            var stopwatch = new System.Diagnostics.Stopwatch();
            stopwatch.Start();

            this.log = new FasterLog(this.blobManager);
            this.store = new FasterKV(this.partition, this.blobManager);

            this.storeWorker = new StoreWorker(store, this.partition, this.TraceHelper);
            this.logWorker = new LogWorker(log, this.partition, this.storeWorker, this.TraceHelper);

            if (this.log.TailAddress == this.log.BeginAddress)
            {
                // this is a fresh partition
                await storeWorker.Initialize();

                // take an (empty) checkpoint immediately to ensure the paths are working
                try
                {
                    this.TraceHelper.FasterProgress("creating store");

                    this.store.TakeFullCheckpoint(out _);
                    await this.store.CompleteCheckpointAsync();
                    this.TraceHelper.FasterStoreCreated(storeWorker.InputQueuePosition, stopwatch.ElapsedMilliseconds);
                }
                catch (Exception e)
                {
                    this.TraceHelper.FasterStorageError("creating store", e);
                    throw;
                }
            }
            else
            {
                try
                {
                    this.TraceHelper.FasterProgress("loading checkpoint");

                    // we are recovering
                    // recover the last checkpoint of the store
                    this.store.Recover();
                    await storeWorker.Recover();

                    this.TraceHelper.FasterCheckpointLoaded(storeWorker.CommitLogPosition, storeWorker.InputQueuePosition, stopwatch.ElapsedMilliseconds);
                }
                catch(Exception e)
                {
                    this.TraceHelper.FasterStorageError("loading checkpoint", e);
                    throw;
                }

                stopwatch.Restart();

                try
                {
                    // replay log if the store checkpoint lags behind the log
                    if (this.log.TailAddress > (long)storeWorker.CommitLogPosition)
                    {
                        await this.storeWorker.ReplayCommitLog(log);
                    }

                    this.TraceHelper.FasterLogReplayed(storeWorker.CommitLogPosition, storeWorker.InputQueuePosition, stopwatch.ElapsedMilliseconds);
                }
                catch (Exception e)
                {
                    this.TraceHelper.FasterStorageError("replay log", e);
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

        public async Task PersistAndShutdownAsync()
        {
            bool workersStoppedCleanly;

            try
            {
                this.TraceHelper.FasterProgress("stopping workers");
                this.shuttingDown = true;

                Task t1 = this.logWorker.PersistAndShutdownAsync();
                Task t2 = this.storeWorker.CancelAndShutdown();

                await t1;
                await t2;
     
                workersStoppedCleanly = true;
            }
            catch (Exception e)
            {
                this.TraceHelper.FasterStorageError("stopping workers", e);
                // swallow this exception and continue shutdown
                workersStoppedCleanly = false;
            }

            if (workersStoppedCleanly)
            {
                // at this point we know the log was successfully persisted, so can we persist latest store
                try
                {
                    this.TraceHelper.FasterProgress("writing final checkpoint");
                    var stopwatch = new System.Diagnostics.Stopwatch();
                    stopwatch.Start();
                    this.store.TakeFullCheckpoint(out _);
                    await this.store.CompleteCheckpointAsync();
                    this.TraceHelper.FasterCheckpointSaved(storeWorker.CommitLogPosition, storeWorker.InputQueuePosition, stopwatch.ElapsedMilliseconds);
                }
                catch (Exception e)
                {
                    this.TraceHelper.FasterStorageError("writing final checkpoint", e);
                    // swallow this exception and continue shutdown
                }
            }

            try
            {
                this.TraceHelper.FasterProgress("disposing store");
                this.store.Dispose();
            }
            catch (Exception e)
            {
                this.TraceHelper.FasterStorageError("disposing store", e);
            }

            try
            {
                this.TraceHelper.FasterProgress("stopping BlobManager");
                await blobManager.StopAsync();
            }
            catch (Exception e)
            {
                this.TraceHelper.FasterStorageError("stopping BlobManager", e);
            }
        }

        public void ScheduleRead(StorageAbstraction.IReadContinuation readContinuation)
        {
            if (!this.shuttingDown)
            {
                this.storeWorker.Submit(readContinuation);
            }
        }

        public void SubmitRange(IEnumerable<PartitionEvent> evts)
        {
            if (!this.shuttingDown)
            {
                this.logWorker.SubmitRange(evts.Where(e => e is IPartitionEventWithSideEffects));
                this.storeWorker.SubmitRange(evts.Where(e => e is IReadonlyPartitionEvent));
            }
        }

        public void Submit(PartitionEvent evt)
        {
            if (!this.shuttingDown)
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
}