using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using DurableTask.Core;

namespace DurableTask.Emulator
{
    [DataContract]
    internal class TimerFired : ProcessorEvent
    {
        [DataMember]
        public long TimerId { get; set; }

        [DataMember]
        public TaskMessage TimerFiredMessage { get; set; }

        public override TrackedObject Scope(State state)
        {
            return state.Timers;
        }
    }
}
