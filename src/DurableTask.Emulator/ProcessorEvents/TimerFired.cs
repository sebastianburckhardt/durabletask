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

        public override IEnumerable<TrackedObject> UpdateSequence(FasterState fasterState)
        {
            yield return fasterState.Timers;

            yield return fasterState.Clocks;
        }
    }
}
