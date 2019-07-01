using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace DurableTask.Emulator
{
    interface IPartitionSender
    {
        Task Send(ProcessorEvent @event);

        Task Send(IEnumerable<ProcessorEvent> events);
    }
}
