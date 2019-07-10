using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using DurableTask.Core;
using DurableTask.Core.History;

namespace DurableTask.EventHubs
{
    [DataContract]
    internal class ClocksState : TrackedObject
    {
        [IgnoreDataMember]
        public override string Key => "@@clocks";

        public void Scope(TaskMessageReceived evt, List<TrackedObject> scope, List<TrackedObject> apply)
        {
            // todo: use vector clock to deduplicate
            //
            // if (already processed this remote request)
            //    return false;
            //

            if (evt is OrchestrationCreationMessageReceived msg)
            {
                scope.Add(State.GetInstance(msg.TaskMessage.OrchestrationInstance.InstanceId));
            }
            else
            {
                apply.Add(State.Sessions);
            }

            apply.Add(this);
        }

        public void Apply(TaskMessageReceived evt)
        {
            // update vector clock
        }
    }
}
