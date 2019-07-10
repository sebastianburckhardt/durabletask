using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using DurableTask.Core;
using DurableTask.Core.History;

namespace DurableTask.EventHubs
{
    [DataContract]
    internal class TimerFired : PartitionEvent
    {
        [DataMember]
        public long TimerId { get; set; }

        [DataMember]
        public TaskMessage TimerFiredMessage { get; set; }

        [IgnoreDataMember]
        public TimerFiredEvent TimerFiredEvent => (TimerFiredMessage.Event as TimerFiredEvent);

        public override TrackedObject Scope(IPartitionState state)
        {
            return state.Timers;
        }
    }
}
