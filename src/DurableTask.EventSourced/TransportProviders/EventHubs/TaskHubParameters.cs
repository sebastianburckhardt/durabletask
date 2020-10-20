using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace DurableTask.EventSourced.EventHubs
{
    [DataContract]
    internal class TaskhubParameters
    {
        [DataMember]
        public string TaskhubName { get; set; }

        [DataMember]
        public Guid TaskhubGuid { get; set; }

        [DataMember]
        public DateTime CreationTimestamp { get; set; }

        [DataMember]
        public string[] PartitionHubs { get; set; }

        [DataMember]
        public string PartitionConsumerGroup { get; set; }

        [DataMember]
        public string[] ClientHubs { get; set; }

        [DataMember]
        public string ClientConsumerGroup { get; set; }

        [DataMember]
        public long[] StartPositions { get; set; }
    }

}
