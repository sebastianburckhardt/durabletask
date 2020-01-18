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

using FASTER.core;
using FASTER.devices;
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

            this.log = new FasterLog(new FasterLogSettings 
            { 
                LogDevice = this.blobManager.EventLogDevice, 
                LogCommitManager = blobManager 
            });

            this.store = new FasterKV(this.partition, this.blobManager);

            this.token = partition.PartitionShutdownToken;

            this.logWorker = new LogWorker(this.log, this.partition, this.token);
            this.storeWorker = new StoreWorker(this.store, this.log, this.partition, this.token);
        }

        public Task WaitForTerminationAsync()
        {
            return this.storeWorker.JoinAsync();
        }

        public Task<TResult> ReadAsync<TObject, TResult>(TrackedObjectKey key, Func<TObject, TResult> read) where TObject : TrackedObject
        {
            var task = new Task<TResult>(() =>
            {
                try
                {
                    var target = store.GetOrCreate(key);

                    TResult result;

                        result = read((TObject)target);

                    return result;
                }
                catch (Exception e)
                {
                    // TODO

                    throw e;
                }
            });

            storeWorker.Process(task);
            return task;
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

        public void StartIterator(long startPosition, Func<CancellationToken, IList<PartitionEvent>, Task> body)
        {
            Task.Run(async () =>
            {
                using (var iter = log.Scan(startPosition, long.MaxValue))
                {
                    var batch = new List<PartitionEvent>();

                    while (!this.token.IsCancellationRequested)
                    {
                        while (iter.GetNext(out var result, out var entryLength, out var currentAddress))
                        {
                            var arraySegment = new ArraySegment<byte>(result, 0, entryLength);
                            var partitionEvent = (PartitionEvent)Serializer.DeserializeEvent(arraySegment);
                            batch.Add(partitionEvent);
                        }

                        if (batch.Count == 0)
                        {
                            await iter.WaitAsync(token);
                            continue;
                        }

                        await body(this.token, batch);

                        batch.Clear();
                    }
                }
            });
        }
    }
}