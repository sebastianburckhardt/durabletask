using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using DurableTask.Core.History;

namespace DurableTask.Emulator
{
    [DataContract]
    internal class RuntimeStateUpdate
    {
        [DataMember]
        public bool NewExecutionId { get; set; }

        [DataMember]
        public string ExecutionId { get; set; }

        [DataMember]
        public bool NewCustomStatus { get; set; }

        [DataMember]
        public string CustomStatus { get; set; }

        [DataMember]
        public int StartAtSequenceNumber { get; set; }

        [DataMember]
        public List<HistoryEvent> NewEvents { get; set; }
    }
}
