using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using DurableTask.Core;
using DurableTask.Core.Exceptions;
using DurableTask.Core.History;

namespace DurableTask.Emulator
{
    [DataContract]
    internal class TaskMessageReceived : ProcessorEvent
    {
        [DataMember]
        public TaskMessage TaskMessage { get; set; }

        public override TrackedObject Scope(IState state)
        {
            return state.Clocks;
        }
    }


    [DataContract]
    internal class OrchestrationCreationMessageReceived: TaskMessageReceived
    {
        [DataMember]
        public OrchestrationStatus[] DedupeStatuses { get; set; }

        [DataMember]
        public DateTime Timestamp { get; set; }

        [IgnoreDataMember]
        public ExecutionStartedEvent ExecutionStartedEvent => this.TaskMessage.Event as ExecutionStartedEvent;
    }

}