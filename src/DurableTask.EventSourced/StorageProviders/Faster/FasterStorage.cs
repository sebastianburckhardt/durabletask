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

        public static Task DeleteTaskhubStorageAsync(string connectionString, string taskHubName)
        {
            return BlobManager.DeleteTaskhubStorageAsync(connectionString, taskHubName);
        }

        public async Task<ulong> CreateOrRestoreAsync(Partition partition, CancellationToken token)
        {
            this.partition = partition;
            this.blobManager = new BlobManager(this.connectionString, this.taskHubName, partition.PartitionId);

            //this.blobManager.LocalFileDirectoryForTestingAndDebugging = "E:\\faster";

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
                try
                {
                    EtwSource.Log.FasterProgress((int)this.partition.PartitionId, "creating store");

                    this.store.TakeFullCheckpoint(out _);
                    await this.store.CompleteCheckpointAsync();
                    EtwSource.Log.FasterStoreCreated((int) this.partition.PartitionId, storeWorker.InputQueuePosition, stopwatch.ElapsedMilliseconds);
                }
                catch (Exception e)
                {
                    EtwSource.Log.FasterStorageError((int) this.partition.PartitionId, "creating store", e.ToString());
                    throw;
                }
            }
            else
            {
                try
                {
                    EtwSource.Log.FasterProgress((int)this.partition.PartitionId, "loading checkpoint");

                    // we are recovering
                    // recover the last checkpoint of the store
                    this.store.Recover();
                    await storeWorker.Recover();

                    EtwSource.Log.FasterCheckpointLoaded((int)this.partition.PartitionId, storeWorker.CommitLogPosition, storeWorker.InputQueuePosition, stopwatch.ElapsedMilliseconds);
                }
                catch(Exception e)
                {
                    EtwSource.Log.FasterStorageError((int)this.partition.PartitionId, "loading checkpoint", e.ToString());
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

                    EtwSource.Log.FasterLogReplayed((int)this.partition.PartitionId, storeWorker.CommitLogPosition, storeWorker.InputQueuePosition, stopwatch.ElapsedMilliseconds);
                }
                catch (Exception e)
                {
                    EtwSource.Log.FasterStorageError((int)this.partition.PartitionId, "replay log", e.ToString());
                    throw;
                }

                // restart pending actitivities, timers, work items etc.
                foreach (var key in TrackedObjectKey.GetSingletons())
                {
                    var target = (TrackedObject)await store.GetOrCreate(key);
                    target.OnRecoveryCompleted();
                }

                this.partition.TraceDetail($"Recovery complete.");
            }

            return storeWorker.InputQueuePosition;
        }

        public async Task PersistAndShutdownAsync()
        {
            bool workersStoppedCleanly;

            try
            {
                EtwSource.Log.FasterProgress((int)this.partition.PartitionId, "stopping workers");
                this.shuttingDown = true;

                Task t1 = this.logWorker.PersistAndShutdownAsync();
                Task t2 = this.storeWorker.CancelAndShutdown();

                await t1;
                await t2;
     
                workersStoppedCleanly = true;
            }
            catch (Exception e)
            {
                EtwSource.Log.FasterStorageError((int)this.partition.PartitionId, "stopping workers", e.ToString());
                // swallow this exception and continue shutdown
                workersStoppedCleanly = false;
            }

            if (workersStoppedCleanly)
            {
                // at this point we know the log was successfully persisted, so can we persist latest store
                try
                {
                    EtwSource.Log.FasterProgress((int)this.partition.PartitionId, "writing final checkpoint");
                    var stopwatch = new System.Diagnostics.Stopwatch();
                    stopwatch.Start();
                    this.store.TakeFullCheckpoint(out _);
                    await this.store.CompleteCheckpointAsync();
                    EtwSource.Log.FasterCheckpointSaved((int)this.partition.PartitionId, storeWorker.CommitLogPosition, storeWorker.InputQueuePosition, stopwatch.ElapsedMilliseconds);
                }
                catch (Exception e)
                {
                    EtwSource.Log.FasterStorageError((int)this.partition.PartitionId, "writing final checkpoint", e.ToString());
                    // swallow this exception and continue shutdown
                }
            }

            try
            {
                EtwSource.Log.FasterProgress((int)this.partition.PartitionId, "disposing store");
                this.store.Dispose();
            }
            catch (Exception e)
            {
                EtwSource.Log.FasterStorageError((int)this.partition.PartitionId, "disposing store", e.ToString());
            }

            try
            {
                EtwSource.Log.FasterProgress((int)this.partition.PartitionId, "stopping BlobManager");
                await blobManager.StopAsync();
            }
            catch (Exception e)
            {
                EtwSource.Log.FasterStorageError((int)this.partition.PartitionId, "stopping BlobManager", e.ToString());
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