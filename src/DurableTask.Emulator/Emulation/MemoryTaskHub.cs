using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace DurableTask.Emulator
{
    internal class MemoryTaskHub : ITaskHub
    {
        private MemoryQueue queue;
        private MemoryState state;

        public IPartitionedQueue Queue => queue;
        public IPartitionState State => state;

        public Task CreateAsync()
        {
            this.queue = new MemoryQueue();
            this.state = new MemoryState();
            return Task.FromResult<object>(null);
        }

        public Task DeleteAsync()
        {
            this.queue = null;
            this.state = null;
            return Task.FromResult<object>(null);
        }

        public Task<bool> ExistsAsync()
        {
            return Task.FromResult(this.queue != null);
        }
    }
}
