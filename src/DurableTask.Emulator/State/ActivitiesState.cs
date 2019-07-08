using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.Serialization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DurableTask.Core;
using DurableTask.Core.History;

namespace DurableTask.Emulator
{
    [DataContract]
    internal class ActivitiesState : TrackedObject
    {
        [DataMember]
        public Dictionary<long, TaskMessage> PendingActivities { get; private set; } = new Dictionary<long, TaskMessage>();

        [DataMember]
        public long SequenceNumber { get; set; }

        [IgnoreDataMember]
        public override string Key => "@@activities";


        protected override void Restore()
        {
            // reschedule work items
            foreach (var pending in PendingActivities)
            {
                LocalPartition.ActivityWorkItemQueue.Add(new ActivityWorkItem(pending.Key, pending.Value));
            }
        }

        // *************  event processing *****************

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
                var activityId = SequenceNumber++;
                PendingActivities.Add(activityId, msg);

                LocalPartition.ActivityWorkItemQueue.Add(new ActivityWorkItem(activityId, msg));
            }
        }
    }
}
