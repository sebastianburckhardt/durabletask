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

        [IgnoreDataMember]
        public override string Key => "@@timers";

        public override void Restore(LocalPartition LocalPartition)
        {
            // restore the pending timers
            foreach (var kvp in PendingTimers)
            {
                var expirationEvent = new TimerFired()
                {
                    TimerId = kvp.Key,
                    TimerFiredMessage = kvp.Value,
                };

                LocalPartition.PendingTimers.Schedule(expirationEvent.TimerFiredEvent.FireAt, expirationEvent);
            }
        }

        public void Scope(TimerFired evt, List<TrackedObject> scope, List<TrackedObject> apply)
        {
            if (PendingTimers.ContainsKey(evt.TimerId))
            {
                apply.Add(State.Sessions);
                apply.Add(this);
            }
        }

        private void Apply(TimerFired evt)
        {
            PendingTimers.Remove(evt.TimerId);
        }

        public void Apply(BatchProcessed evt)
        {
            foreach(var t in evt.TimerMessages)
            {
                var timerId = SequenceNumber++;
                PendingTimers.Add(timerId, t);

                var expirationEvent = new TimerFired()
                {
                    TimerId = timerId,
                    TimerFiredMessage = t,
                };

                LocalPartition.PendingTimers.Schedule(expirationEvent.TimerFiredEvent.FireAt, expirationEvent);
            }
        }
    }
}
