using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using DurableTask.Core;

namespace DurableTask.Emulator
{
    [DataContract]
    internal class OutgoingMessagesAcked : ProcessorEvent
    {
        [DataMember]
        public long LastAckedQueuePosition { get; set; }

        public override TrackedObject Scope(State state)
        {
            return state.Outbox;
        }
    }
}
