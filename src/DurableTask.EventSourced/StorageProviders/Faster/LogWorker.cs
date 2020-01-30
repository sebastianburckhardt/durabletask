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

        public LogWorker(FasterLog log, Partition partition)
            : base()
        {
            this.log = log;
            this.partition = partition;
        }

        private void EnqueueEvent(PartitionEvent evt)
        {
            if (evt.Serialized.Count == 0)
            {
                byte[] bytes = Serializer.SerializeEvent(evt);
                evt.CommitLogPosition = this.log.Enqueue(bytes);
            }
            else
            {
                evt.CommitLogPosition = this.log.Enqueue(evt.Serialized.AsSpan<byte>());
            }

            this.partition.TraceSubmit(evt);
        }

        public void AddToLog(IEnumerable<PartitionEvent> evts)
        {
            lock (this.thisLock) // we need the lock so concurrent submissions have consistent ordering
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
        }

        public void AddToLog(PartitionEvent evt)
        {
            lock (this.thisLock) // we need the lock so concurrent submissions have consistent ordering
            {
                if (evt.PersistInLog)
                {
                    EnqueueEvent(evt);
                    this.Submit(evt);
                }
            }
        }

        protected override async Task Process(IList<PartitionEvent> batch)
        {
            try
            {
                await log.CommitAsync(this.cancellationToken);

                foreach (var evt in batch)
                {
                    if (evt == null)
                    {
                        this.shutdownWaiter.TrySetResult(true);
                        return;
                    }
                    
                    if (!this.IsShuttingDown)
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
            lock (this.thisLock)
            {
                this.shutdownWaiter = new TaskCompletionSource<bool>();
                this.Submit(null);
            }

            await this.shutdownWaiter.Task; // waits for all the enqueued entries to be persisted
        }
    }
}
