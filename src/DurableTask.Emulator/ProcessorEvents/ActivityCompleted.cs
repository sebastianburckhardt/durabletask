using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using DurableTask.Core;

namespace DurableTask.Emulator
{
    [DataContract]
    internal class ActivityCompleted : ProcessorEvent
    {
        [DataMember]
        public long ActivityId { get; set; }

        [DataMember]
        public TaskMessage Response { get; set; }

        public override IEnumerable<TrackedObject> UpdateSequence(FasterState fasterState)
        {
            yield return fasterState.Activities;

            yield return fasterState.Sessions;
        }
    }
}
