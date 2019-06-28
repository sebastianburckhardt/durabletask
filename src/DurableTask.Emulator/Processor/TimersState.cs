using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using DurableTask.Core;
using DurableTask.Core.History;

namespace DurableTask.Emulator
{
    [DataContract]
    internal class TimersState : TrackedObject
    {
        [DataMember]
        public Dictionary<long, TaskMessage> PendingTimers { get; set; }

        [DataMember]
        public long SequenceNumber { get; set; }

        public bool Apply(TimerFired evt)
        {
            if (!AlreadyApplied(evt))
            {
                return PendingTimers.Remove(evt.TimerId);
            }

            return true;
        }
    }
}
