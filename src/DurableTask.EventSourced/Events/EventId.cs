﻿//  ----------------------------------------------------------------------------------
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

using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using DurableTask.Core;
using Dynamitey;

namespace DurableTask.EventSourced
{
    /// <summary>
    /// A unique identifier for an event.
    /// </summary>
    public struct EventId
    {
        /// <summary>
        /// The category of an event.
        /// </summary>
        public enum EventCategory
        {
            /// <summary>
            /// An event that is sent from a client to a partition.
            /// </summary>
            ClientRequest,

            /// <summary>
            /// An event that is sent from a partition back to a client, as a response.
            /// </summary>
            ClientResponse,

            /// <summary>
            /// An event that is sent by a partition to itself.
            /// </summary>
            PartitionInternal,

            /// <summary>
            /// An event that is sent from a partition to another partition.
            /// </summary>
            PartitionToPartition,
        }

        /// <summary>
        /// The category of this event
        /// </summary>
        public EventCategory Category { get; set; }

        /// <summary>
        /// For events originating on a client, the client id. 
        /// </summary>
        public Guid ClientId { get; set; }

        /// <summary>
        /// For events originating on a partition, the partition id.
        /// </summary>
        public uint PartitionId { get; set; }

        /// <summary>
        /// For events originating on a client, a sequence number
        /// </summary>
        public long Number { get; set; }

        /// <summary>
        /// For sub-events, the index
        /// </summary>
        public int SubIndex { get; set; }

        /// <summary>
        /// For events originating on a partition, a string for correlating this event
        /// </summary>
        public string WorkItemId { get; set; }

        /// <summary>
        /// For fragmented events, or internal dependent reads, the fragment number or subindex.
        /// </summary>
        public int? Index { get; set; }

        internal static EventId MakeClientRequestEventId(Guid ClientId, long RequestId) => new EventId()
        {
            ClientId = ClientId,
            Number = RequestId,
            Category = EventCategory.ClientRequest
        };

        internal static EventId MakeClientResponseEventId(Guid ClientId, long RequestId) => new EventId()
        {
            ClientId = ClientId,
            Number = RequestId,
            Category = EventCategory.ClientResponse
        };

        internal static EventId MakePartitionInternalEventId(string workItemId) => new EventId()
        {
            WorkItemId = workItemId,
            Category = EventCategory.PartitionInternal
        };

        internal static EventId MakePartitionToPartitionEventId(string workItemId, uint destinationPartition) => new EventId()
        {
            WorkItemId = workItemId,
            PartitionId = destinationPartition,
            Category = EventCategory.PartitionToPartition
        };

        internal static EventId MakeSubEventId(EventId id, int fragment)
        {
            id.Index = fragment;
            return id;
        }

        /// <inheritdoc/>
        public override string ToString()
        {
            switch (this.Category)
            {
                case EventCategory.ClientRequest:
                    return $"{Client.GetShortId(this.ClientId)}-{this.Number}{this.IndexSuffix}";

                case EventCategory.ClientResponse:
                    return $"{Client.GetShortId(this.ClientId)}-{this.Number}R{this.IndexSuffix}";

                case EventCategory.PartitionInternal:
                    return $"{this.WorkItemId}{this.IndexSuffix}";

                case EventCategory.PartitionToPartition:
                    return $"{this.WorkItemId}-{this.PartitionId:D2}{this.IndexSuffix}";

                default:
                    throw new InvalidOperationException();
            }
        }

        private string IndexSuffix => this.Index.HasValue ? $"-{this.Index.Value}" : string.Empty;
    }
}