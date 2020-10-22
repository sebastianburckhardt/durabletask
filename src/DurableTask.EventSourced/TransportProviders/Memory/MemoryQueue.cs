﻿//  ----------------------------------------------------------------------------------
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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace DurableTask.EventSourced.Emulated
{
    /// <summary>
    /// Simulates a in-memory queue for delivering events. Used for local testing and debugging.
    /// </summary>
    internal abstract class MemoryQueue<T,B> : BatchWorker<B> where T:Event
    {
        private long position = 0;
        private readonly ILogger logger;

        public MemoryQueue(CancellationToken cancellationToken, ILogger logger) : base(nameof(MemoryQueue<T,B>), cancellationToken, true)
        {
            this.logger = logger;
        }

        protected abstract B Serialize(T evt);
        protected abstract T Deserialize(B evt);

        protected abstract void Deliver(T evt);

        public long FirstInputQueuePosition { get; set; }

        protected override Task Process(IList<B> batch)
        {
            try
            {
                if (batch.Count > 0)
                {
                    var eventbatch = new T[batch.Count];

                    for (int i = 0; i < batch.Count; i++)
                    {
                        if (cancellationToken.IsCancellationRequested)
                        {
                            return Task.CompletedTask;
                        }

                        var evt = this.Deserialize(batch[i]);

                        if (evt is PartitionEvent partitionEvent)
                        {
                            partitionEvent.NextInputQueuePosition = FirstInputQueuePosition + this.position + i + 1;
                        }

                        eventbatch[i] = evt;
                    }

                    foreach (var evt in eventbatch)
                    {
                        if (cancellationToken.IsCancellationRequested)
                        {
                            return Task.CompletedTask;
                        }

                        Deliver(evt);
                    }

                    this.position = this.position + batch.Count;
                }
            }
            catch(Exception e)
            {
                this.logger.LogError("Exception in MemoryQueue BatchWorker: {exception}", e);
            }

            return Task.CompletedTask;
        }

        public void Send(T evt)
        {
            var serialized = Serialize(evt);

            this.Submit(serialized);
        }
    }
}
