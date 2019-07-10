using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using DurableTask.Core;

namespace DurableTask.EventHubs
{
    [DataContract]
    internal class ActivityCompleted : PartitionEvent
    {
        [DataMember]
        public long ActivityId { get; set; }

        [DataMember]
        public TaskMessage Response { get; set; }

        public override TrackedObject Scope(IPartitionState state)
        {
            return state.Activities;
        }
    }
}
