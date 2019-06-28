using System;
using System.Collections.Generic;
using System.Text;

namespace DurableTask.Emulator
{
    interface IPartitionReceiver
    {
        IEnumerable<ProcessorEvent> Receive(long startPosition);
    }
}
