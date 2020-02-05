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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventSourced.Faster
{
    internal class FasterStorage : StorageAbstraction.IPartitionState
    {
        private readonly string connectionString;
        private readonly string taskHubName;

        private Partition partition;
        private BlobManager blobManager;
        private LogWorker logWorker;
        private StoreWorker storeWorker;
        private FasterLog log;
        private FasterKV store;

        private volatile bool shuttingDown;

        public CancellationToken OwnershipCancellationToken { get; private set; }

        public FasterStorage(string connectionString, string taskHubName)
        {
            this.connectionString = connectionString;
            this.taskHubName = taskHubName;
        }

        public async Task<ulong> CreateOrRestoreAsync(Partition partition, CancellationToken token)
        {
            this.partition = partition;
            this.blobManager = new BlobManager(this.connectionString, this.taskHubName, partition.PartitionId);

            this.blobManager.UseLocalFilesForTestingAndDebugging = true;

            await blobManager.StartAsync();
            this.OwnershipCancellationToken = await blobManager.AcquireOwnership(token);

            var stopwatch = new System.Diagnostics.Stopwatch();
            stopwatch.Start();

            this.log = new FasterLog(this.blobManager);
            this.store = new FasterKV(this.partition, this.blobManager);

            this.storeWorker = new StoreWorker(store, this.partition);
            this.logWorker = new LogWorker(log, this.partition, this.storeWorker);

            if (this.log.TailAddress == this.log.BeginAddress)
            {
                // this is a fresh partition
                await storeWorker.Initialize();

                // take an (empty) checkpoint immediately to ensure the paths are working
                this.store.TakeFullCheckpoint(out _);
                await this.store.CompleteCheckpointAsync();

                partition.TracePartitionStoreCreated(storeWorker.InputQueuePosition, stopwatch.ElapsedMilliseconds);
            }
            else
            {
                // we are recovering
                // recover the last checkpoint of the store
                this.store.Recover();
                await storeWorker.Recover();

                partition.TracePartitionCheckpointLoaded(storeWorker.CommitLogPosition, storeWorker.InputQueuePosition, stopwatch.ElapsedMilliseconds);

                stopwatch.Restart();

                // replay log if the store checkpoint lags behind the log
                if (this.log.TailAddress > (long) storeWorker.CommitLogPosition)
                {
                    await this.storeWorker.ReplayCommitLog(log);
                }

                partition.TracePartitionLogReplayed(storeWorker.CommitLogPosition, storeWorker.InputQueuePosition, stopwatch.ElapsedMilliseconds);

                // restart pending actitivities, timers, work items etc.
                foreach (var key in TrackedObjectKey.GetSingletons())
                {
                    var target = (TrackedObject)await store.GetOrCreate(key);
                    target.OnRecoveryCompleted();
                }

                this.partition.DiagnosticsTrace($"Recovery complete.");
            }

            return storeWorker.InputQueuePosition;
        }

        public async Task PersistAndShutdownAsync()
        {
            try
            {
                this.shuttingDown = true;

                await Task.WhenAll(
                    this.logWorker.PersistAndShutdownAsync(), // persist all current entries in the commit log
                    this.storeWorker.CancelAndShutdown() // cancel all pending entries in the store queue, we'll replay on recovery
                );

                // at this point we know the log was successfully persisted, so can we persist a store checkpoint
                var stopwatch = new System.Diagnostics.Stopwatch();
                stopwatch.Start();
                this.store.TakeFullCheckpoint(out _);
                await this.store.CompleteCheckpointAsync();
                partition.TracePartitionCheckpointSaved(storeWorker.CommitLogPosition, storeWorker.InputQueuePosition, stopwatch.ElapsedMilliseconds);
            }
            catch (Exception e)
            {
                this.partition.ReportError($"{nameof(FasterStorage)}.{nameof(PersistAndShutdownAsync)}", e);
            }
            finally
            {
                // we are done with the store
                this.store.Dispose();
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
                this.logWorker.SubmitRange(evts.Where(e => !e.IsReadOnly));
                this.storeWorker.SubmitRange(evts.Where(e => e.IsReadOnly));
            }
        }

        public void Submit(PartitionEvent evt)
        {
            if (!this.shuttingDown)
            {
                if (!evt.IsReadOnly)
                {
                    this.logWorker.Submit(evt);
                }
                else
                {
                    this.storeWorker.Submit(evt);
                }
            }
        }
    }
}