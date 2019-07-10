using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using DurableTask.Core;

namespace DurableTask.EventHubs
{
    [DataContract]
    internal class OutgoingMessagesAcked : PartitionEvent
    {
        [DataMember]
        public long LastAckedQueuePosition { get; set; }

        public override TrackedObject Scope(IPartitionState state)
        {
            return state.Outbox;
        }
    }
}
