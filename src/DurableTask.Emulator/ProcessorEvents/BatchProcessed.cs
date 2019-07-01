using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using DurableTask.Core;
using DurableTask.Core.History;

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
        public List<HistoryEvent> NewEvents { get; set; }

        [DataMember]
        public OrchestrationState State { get; set; }

        [DataMember]
        public List<TaskMessage> ActivityMessages { get; set; }

        [DataMember]
        public List<TaskMessage> LocalOrchestratorMessages { get; set; }

        [DataMember]
        public List<TaskMessage> RemoteOrchestratorMessages { get; set; }

        [DataMember]
        public List<TaskMessage> WorkItemTimerMessages { get; set; }

        [DataMember]
        public DateTime Timestamp { get; set; }

        [IgnoreDataMember]
        public string InstanceId => State.OrchestrationInstance.InstanceId;

        public override TrackedObject Scope(State state)
        {
            return state.Sessions;
        }
    }

}
