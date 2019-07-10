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
                this.messages.AddRange(events);
            }
            finally
            {
                this.asyncLock.Release(1);
            }
        }
    }
}
