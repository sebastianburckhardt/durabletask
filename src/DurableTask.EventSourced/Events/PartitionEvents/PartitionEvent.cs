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

using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace DurableTask.EventSourced
{
    /// <summary>
    /// An event that is processed by a partition
    /// </summary>
    [DataContract]
    internal abstract class PartitionEvent : Event
    {
        [DataMember]
        public uint PartitionId { get; set; }

        [IgnoreDataMember]
        public ArraySegment<byte> Serialized;

        /// <summary>
        /// For events coming from the input queue, the next input queue position after this event. For internal events, zero.
        /// </summary>
        [DataMember]
        public long NextInputQueuePosition { get; set; }

        [IgnoreDataMember]
        public double ReceivedTimestamp { get; set; }

        [IgnoreDataMember]
        public double ReadyToSendTimestamp { get; set; }

        [IgnoreDataMember]
        public double SentTimestamp { get; set; }

        /// <summary>
        /// This is set (using Unix time) just before sending the event on EventHubs and is used to measure the time that it takes for an event to pass through eventHubs.
        /// It is compared with the local unix time at the receiver's end.
        /// Warning: If there is clock skew, the result might not be very accurate.
        /// </summary>
        [DataMember]
        public long SentTimestampUnixMs { get; set; } = 0;

        [IgnoreDataMember]
        public long ReceivedTimestampUnixMs { get; set; }

        [IgnoreDataMember]
        public double IssuedTimestamp { get; set; }
    }
}
