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

namespace DurableTask.EventSourced.Emulated
{
    /// <summary>
    /// Simulates a in-memory queue for delivering events. Used for local testing and debugging.
    /// </summary>
    internal class EmulatedClientQueue : EventHubs.BatchWorker
    {
        private readonly Backend.IClient client;
        private readonly CancellationToken cancellationToken;

        private List<ClientEvent> batch = new List<ClientEvent>();
        private List<ClientEvent> queue = new List<ClientEvent>();

        private long position = 0;

        public EmulatedClientQueue(Backend.IClient client, CancellationToken cancellationToken)
        {
            this.client = client;
            this.cancellationToken = cancellationToken;
        }

        protected override Task Work()
        {
            lock (this.lockable)
            { 
                var temp = queue;
                queue = batch;
                batch = temp;
            }

            for (int i = 0; i < batch.Count; i++)
            {
                batch[i].QueuePosition = position + i;
            }

            foreach (var clientEvent in batch)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    break;
                }

                try
                {
                    client.Process(clientEvent);
                }
                catch (System.Threading.Tasks.TaskCanceledException)
                {
                    // this is normal during shutdown
                }
                catch (Exception e)
                {
                    client.ReportError(nameof(EmulatedClientQueue), e);
                }
            }
                
            position = position + batch.Count;
            batch.Clear();

            return Task.CompletedTask;
        }

        public void Send(ClientEvent @event)
        {
            lock (this.lockable)
            {
                queue.Add(@event);
                this.Notify();
            }
        }
    }
}
