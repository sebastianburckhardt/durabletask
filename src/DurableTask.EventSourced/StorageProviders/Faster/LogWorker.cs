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
using System;
using System.Collections.Generic;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventSourced.Faster
{
    internal class LogWorker : BatchWorker<PartitionEvent>
    {
        private readonly FasterLog log;
        private readonly Partition partition;

        private volatile TaskCompletionSource<bool> shutdownWaiter;

        private bool IsShuttingDown => this.shutdownWaiter != null;

        public LogWorker(Partition partition, BlobManager blobManager)
            : base()
        {
            this.log = new FasterLog(blobManager);
            this.partition = partition;
        }

        public Task StartAsync()
        {
            return Task.CompletedTask;
        }

        private void EnqueueEvent(PartitionEvent evt)
        {
            if (evt.Serialized.Count == 0)
            {
                byte[] bytes = Serializer.SerializeEvent(evt);
                evt.CommitPosition = this.log.Enqueue(bytes);
            }
            else
            {
                evt.CommitPosition = this.log.Enqueue(evt.Serialized.AsSpan<byte>());
            }

            this.partition.TraceSubmit(evt);
        }

        public void AddToLog(IEnumerable<PartitionEvent> evts)
        {
            foreach (var evt in evts)
            {
                if (evt.PersistInLog)
                {
                    EnqueueEvent(evt);
                    this.Submit(evt);
                }
            }
        }

        public void AddToLog(PartitionEvent evt)
        {
            if (evt.PersistInLog)
            {
                EnqueueEvent(evt);
                this.Submit(evt);
            }
        }

        protected override async Task Process(IList<PartitionEvent> batch)
        {
            try
            {
                await log.CommitAsync(this.cancellationToken);

                if (this.IsShuttingDown)
                {
                    // at this point we know all the enqueued entries have been persisted
                    this.shutdownWaiter.TrySetResult(true);
                }
                else
                {
                    foreach (var evt in batch)
                    {
                        AckListeners.Acknowledge(evt);
                    }
                }
            }
            catch (Exception e)
            {
                if (this.IsShuttingDown)
                {
                    // lets the caller know that shutdown did not successfully persist the latest log
                    this.shutdownWaiter.TrySetException(e);
                }
            }      
        }

        public async Task PersistAndShutdownAsync()
        {
            lock (this.lockable)
            {
                this.shutdownWaiter = new TaskCompletionSource<bool>();
                this.Notify();
            }

            await this.shutdownWaiter.Task; // waits for all the enqueued entries to be persisted
        }
    }
}
