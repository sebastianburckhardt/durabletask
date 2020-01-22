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
using System.IO;
using System.Net.Http.Headers;
using System.Runtime.Serialization;
using System.Threading;
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
        
        private CancellationToken token;

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

            this.token = partition.PartitionShutdownToken;

            this.logWorker = new LogWorker(this.partition, this.blobManager, this.token);
            this.storeWorker = new StoreWorker(this.partition, this.blobManager, this.token);
        }

        public async Task WaitForTerminationAsync()
        {
            await Task.WhenAll(this.storeWorker.JoinAsync(), this.logWorker.JoinAsync());
        }

        public Task<TResult> ReadAsync<TObject, TResult>(TrackedObjectKey key, Func<TObject, TResult> read) where TObject : TrackedObject
        {
            return storeWorker.ProcessRead(key, read);
        }

        public void SubmitRange(IEnumerable<PartitionEvent> evts)
        {
            logWorker.AddToLog(evts);
            storeWorker.Process(evts);
        }

        public void Submit(PartitionEvent evt)
        {
            logWorker.AddToLog(evt);
            storeWorker.Process(evt);
        }
    }
}