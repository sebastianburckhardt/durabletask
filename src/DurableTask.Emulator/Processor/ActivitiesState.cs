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

        [IgnoreDataMember]
        public override string Key => "@@activities";

        public void Scope(ActivityCompleted evt, List<TrackedObject> scope, List<TrackedObject> apply)
        {
            if (PendingActivities.ContainsKey(evt.ActivityId))
            {
                apply.Add(State.Sessions);
                apply.Add(this);
            }
        }

        public void Apply(ActivityCompleted evt)
        {
            PendingActivities.Remove(evt.ActivityId);
        }

        public void Apply(BatchProcessed evt)
        {
            foreach (var msg in evt.ActivityMessages)
            {
                PendingActivities.Add(SequenceNumber++, msg);
            }
        }

    }
}
