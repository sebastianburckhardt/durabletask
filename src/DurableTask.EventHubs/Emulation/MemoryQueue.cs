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
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventHubs
{
    internal class MemoryQueue : IPartitionedQueue
    {
        private readonly SemaphoreSlim asyncLock = new SemaphoreSlim(1, 1);  
        
        private List<PartitionEvent> messages = new List<PartitionEvent>();

        public bool IsLocal(string instanceId)
        {
            return true; // single partition
        }

        public async Task<List<PartitionEvent>> ReceiveBatchAsync(long startPosition)
        {
            await this.asyncLock.WaitAsync();
            try
            {
                await Task.Delay(20);
                return this.messages.GetRange((int) startPosition, (int) (this.messages.Count - startPosition));
            }
            finally
            {
                this.asyncLock.Release(1);
            }
        }

        public async Task SendAsync(PartitionEvent @event)
        {
            await this.asyncLock.WaitAsync();
            try
            {
                await Task.Delay(20);
                this.messages.Add(@event);
            }
            finally
            {
                this.asyncLock.Release(1);
            }       
        }

        public async Task SendBatchAsync(IEnumerable<PartitionEvent> events)
        {
            await this.asyncLock.WaitAsync();
            try
            {
                await Task.Delay(20);
                this.messages.AddRange(events);
            }
            finally
            {
                this.asyncLock.Release(1);
            }
        }
    }
}
