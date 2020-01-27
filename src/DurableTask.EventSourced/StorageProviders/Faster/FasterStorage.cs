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
using System.Threading.Tasks;

namespace DurableTask.EventSourced.Faster
{
    internal class FasterStorage : StorageAbstraction.IPartitionState
    {
        private readonly string connectionString;

        private Partition partition;
        private BlobManager blobManager;
        private LogWorker logWorker;
        private StoreWorker storeWorker;
        private FasterLog log;
        private FasterKV store;

        private bool shuttingDown;

        public FasterStorage(string connectionString)
        {
            this.connectionString = connectionString;
        }

        public async Task RestoreAsync(Partition partition)
        {
            this.partition = partition;
            this.blobManager = new BlobManager(this.connectionString, "faster", partition.PartitionId);

            await blobManager.StartAsync();

            this.log = new FasterLog(this.blobManager);
            this.store = new FasterKV(this.partition, this.blobManager);

            this.logWorker = new LogWorker(log, this.partition);
            this.storeWorker = new StoreWorker(store, this.partition);

            // initialize all singleton objects
            await storeWorker.Initialize();
        
            // TODO recovery
        }

        public async Task PersistAndShutdownAsync()
        {
            this.shuttingDown = true;

            // persist the latest log
            await this.logWorker.PersistAndShutdownAsync();

            // cancel all pending events and reads in the store queue
            // (for faster shutdown)
            await this.storeWorker.ShutdownAsync();

            // at this point we know the log was persisted, so can we persist a store checkpoint
            this.store.TakeFullCheckpoint(out _);
            await this.store.CompleteCheckpointAsync();

            // we are done with the store
            this.store.Dispose();
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