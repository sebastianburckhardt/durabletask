//  ----------------------------------------------------------------------------------
//  Copyright Microsoft Corporation
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  ----------------------------------------------------------------------------------

namespace DurableTask.Core
{
    using System.Runtime.Serialization;
    using DurableTask.Core.History;

    /// <summary>
    /// Wire level transport object for task messages containing events and orchestration instance information
    /// </summary>
    [DataContract]
    public class TaskMessage : IExtensibleDataObject
    {
        /// <summary>
        /// Event information for this task message
        /// </summary>
        [DataMember]
        public HistoryEvent Event { get; set; }

        /// <summary>
        /// Sequence number for ordering of messages in history tracking
        /// </summary>
        [DataMember]
        public long SequenceNumber { get; set; }

        /// <summary>
        /// The orchestration instance information
        /// </summary>
        [DataMember]
        public OrchestrationInstance OrchestrationInstance { get; set; }


        // This represents messages that were received from other partitions 
        // and should contain the origin partition and the origin position. 
        // The partion and position can be used to track inter-partition dependencies
        // when recovering from a crash. 
        // 
        // The partition that is recovering can let the other partitions know the commitLogPosition
        // from which it is recovering. The other partitions then have to recover from the latest 
        // commit log position where all dependencies (originPosition) are smaller or equal to the commitLogPosition.

        /// <summary>
        /// 
        /// </summary>
        [DataMember]
        public uint OriginPartition { get; set; }

        /// <summary>
        /// 
        /// </summary>
        [DataMember]
        public long OriginPosition { get; set; }


        /// <summary>
        /// Implementation for <see cref="IExtensibleDataObject.ExtensionData"/>.
        /// </summary>
        public ExtensionDataObject ExtensionData { get; set; }
    }
}