using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using DurableTask.Core;

namespace DurableTask.Emulator
{
    [DataContract]
    internal class BatchProcessed : ProcessorEvent
    {
        [DataMember]
        public long SessionId { get; set; }

        [DataMember]
        public long BatchStart { get; set; }

        [DataMember]
        public int BatchLength { get; set; }

        [DataMember]
        public RuntimeStateUpdate RuntimeStateUpdate { get; set; }

        [DataMember]
        public List<TaskMessage> OutboundMessages { get; set; }

        [DataMember]
        public List<TaskMessage> OrchestratorMessages { get; set; }

        [DataMember]
        public List<TaskMessage> WorkItemTimerMessages { get; set; }

        [DataMember]
        public TaskMessage ContinuedAsNewMessage { get; set; }

        [DataMember]
        public OrchestrationState State { get; set; }

        [IgnoreDataMember]
        public string InstanceId => State.OrchestrationInstance.InstanceId;

        public override IEnumerable<TrackedObject> UpdateSequence(FasterState fasterState)
        {
            yield return fasterState.Sessions;

            yield return fasterState.GetInstance(State.OrchestrationInstance.InstanceId);

            if (OrchestratorMessages?.Count > 0)
            {
                yield return fasterState.Outbox;
            }

            if (OutboundMessages?.Count > 0)
            {
                yield return fasterState.Activities;
            }

            if (WorkItemTimerMessages?.Count > 0)
            {
                yield return fasterState.Timers;
            }

        }

    }

}
