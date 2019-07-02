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
        public long StartPosition { get; set; }

        [DataMember]
        public int Length { get; set; }

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
        public List<TaskMessage> TimerMessages { get; set; }

        [DataMember]
        public DateTime Timestamp { get; set; }

        [IgnoreDataMember]
        public string InstanceId => State.OrchestrationInstance.InstanceId;

        public override TrackedObject Scope(IState state)
        {
            return state.Sessions;
        }
    }

}
