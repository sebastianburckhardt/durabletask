using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using DurableTask.Core;

namespace DurableTask.Emulator
{
    [DataContract]
    internal class OutboxState : TrackedObject
    {
        [DataMember]
        public SortedDictionary<long, List<TaskMessage>> Outbox { get; set; }

        [DataMember]
        public long LastAckedQueuePosition { get; set; } = -1;

        [IgnoreDataMember]
        public override string Key => "@@outbox";

        public void Apply(BatchProcessed evt)
        {
            Outbox.Add(evt.QueuePosition, evt.RemoteOrchestratorMessages);
        }

        public void Scope(OutgoingMessagesAcked evt, List<TrackedObject> scope, List<TrackedObject> apply)
        {
            if (Outbox.Count > 0 && Outbox.First().Key < evt.LastAckedQueuePosition)
            {
                apply.Add(this);
            }
        }

        public void Apply(OutgoingMessagesAcked evt)
        {
            while (Outbox.Count > 0)
            {
                var first = Outbox.First();

                if (first.Key < evt.LastAckedQueuePosition)
                {
                    Outbox.Remove(first.Key);
                }
            }

            LastAckedQueuePosition = evt.LastAckedQueuePosition;
        }
    }
}
