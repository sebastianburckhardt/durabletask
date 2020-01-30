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
using System.Collections.Concurrent;
using System.Collections.Generic;
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

        private bool shuttingDown;

        private long inputQueuePosition;

        public CancellationToken OwnershipCancellationToken { get; private set; }

        public FasterStorage(string connectionString, string taskHubName)
        {
            this.connectionString = connectionString;
            this.taskHubName = taskHubName;
        }

        public async Task<long> RestoreAsync(Partition partition)
        {
            this.partition = partition;
            this.blobManager = new BlobManager(this.connectionString, this.taskHubName, partition.PartitionId);

            await blobManager.StartAsync();
            this.OwnershipCancellationToken = await blobManager.AcquireOwnership(CancellationToken.None);

            this.log = new FasterLog(this.blobManager);
            this.store = new FasterKV(this.partition, this.blobManager);

            this.logWorker = new LogWorker(log, this.partition);
            this.storeWorker = new StoreWorker(store, this.partition);

            // read all singleton objects and determine from where to resume the commit log
            await storeWorker.Initialize();

            // replay the commit log to the end, if necessary
            await storeWorker.ReplayCommitLog(log);

            // finish the recovery
            this.inputQueuePosition = await storeWorker.FinishRecovery();

            // resume the input queue from the last processed position
            return inputQueuePosition;
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
                this.store.TakeFullCheckpoint(out _);
                await this.store.CompleteCheckpointAsync();

            }
            catch(Exception e)
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
            if (this.shuttingDown)
            {
                return; // as we are already shutting down, we will never run this action
            }

            this.storeWorker.Submit(readContinuation);
        }

        public void SubmitRange(IEnumerable<PartitionEvent> evts)
        {
            if (this.shuttingDown)
            {
                return; // as we are already shutting down, ignore the event
            }

            this.logWorker.AddToLog(evts);
            this.storeWorker.SubmitRange(evts);
        }

        public void Submit(PartitionEvent evt)
        {
            if (this.shuttingDown)
            {
                return; // as we are already shutting down, ignore the event
            }

            this.logWorker.AddToLog(evt);
            this.storeWorker.Submit(evt);
        }
    }
}