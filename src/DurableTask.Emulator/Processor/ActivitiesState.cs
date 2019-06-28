using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using DurableTask.Core;
using DurableTask.Core.History;

namespace DurableTask.Emulator
{
    [DataContract]
    internal class ActivitiesState : TrackedObject
    {
        [DataMember]
        public Dictionary<long, TaskMessage> PendingActivities { get; set; }

        [DataMember]
        public long SequenceNumber { get; set; }

        public bool Apply(BatchProcessed evt)
        {
            if (!AlreadyApplied(evt))
            {
                foreach (var msg in evt.OutboundMessages)
                {
                    PendingActivities.Add(SequenceNumber++, msg);
                }
            }

            return true;
        }

        public bool Apply(ActivityCompleted evt)
        {
            return PendingActivities.Remove(evt.ActivityId);
        }
    }
}
