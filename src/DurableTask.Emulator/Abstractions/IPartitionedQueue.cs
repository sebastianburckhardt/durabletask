using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace DurableTask.Emulator
{
    interface IPartitionedQueue
    {
        Task<List<PartitionEvent>> ReceiveBatch(long startPosition);

        Task Send(PartitionEvent @event);

        Task SendBatch(IEnumerable<PartitionEvent> events);

        bool IsLocal(string instanceId);
    }
}
