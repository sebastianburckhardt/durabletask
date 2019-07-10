using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace DurableTask.EventHubs
{
    internal class MemoryTaskHub : ITaskHub
    {
        private MemoryQueue queue = new MemoryQueue();
        private MemoryState state = new MemoryState();

        public IPartitionedQueue Queue => queue;
        public IPartitionState State => state;

        public Task CreateAsync()
        {
            return Task.FromResult<object>(null);
        }

        public Task DeleteAsync()
        {
            return Task.FromResult<object>(null);
        }

        public Task<bool> ExistsAsync()
        {
            return Task.FromResult(true);
        }
    }
}
