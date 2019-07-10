using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace DurableTask.EventHubs
{
    interface IPartitionedQueue
    {
        Task<List<PartitionEvent>> ReceiveBatchAsync(long startPosition);

        Task SendAsync(PartitionEvent @event);

        Task SendBatchAsync(IEnumerable<PartitionEvent> events);

        bool IsLocal(string instanceId);
    }
}
