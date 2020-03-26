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
    [DataContract]
    [KnownTypeAttribute("KnownTypes")]
    internal abstract class Event
    {
        private string eventIdString;

        /// <summary>
        /// A unique identifier for this event.
        /// </summary>
        [IgnoreDataMember]
        public abstract EventId EventId { get; }

        /// <summary>
        /// Listeners to be notified when this event is durably persisted or sent.
        /// </summary>
        [IgnoreDataMember]
        public DurabilityListeners DurabilityListeners;

        /// <summary>
        /// A string identifiying this event, suitable for tracing (cached to avoid excessive formatting)
        /// </summary>
        [IgnoreDataMember]
        public string EventIdString => this.eventIdString ?? (this.eventIdString = this.EventId.ToString());

        public override string ToString()
        {
            var s = new StringBuilder();
            s.Append(this.GetType().Name);
            this.ExtraTraceInformation(s);
            return s.ToString();
        }

        protected virtual void ExtraTraceInformation(StringBuilder s)
        {
            // subclasses can override this to add extra information to the trace
        }

        public static IEnumerable<Type> KnownTypes()
        {
            yield return typeof(ClientEventFragment);
            yield return typeof(CreationResponseReceived);
            yield return typeof(QueryResponseReceived);
            yield return typeof(StateResponseReceived);
            yield return typeof(WaitResponseReceived);
            yield return typeof(ClientTaskMessagesReceived);
            yield return typeof(CreationRequestReceived);
            yield return typeof(InstanceQueryReceived);
            yield return typeof(StateRequestReceived);
            yield return typeof(WaitRequestReceived);
            yield return typeof(ActivityCompleted);
            yield return typeof(BatchProcessed);
            yield return typeof(CreationRequestProcessed);
            yield return typeof(SendConfirmed);
            yield return typeof(TimerFired);
            yield return typeof(ActivityOffloadReceived);
            yield return typeof(RemoteActivityResultReceived);
            yield return typeof(TaskMessagesReceived);
            yield return typeof(OffloadDecision);
            yield return typeof(PartitionEventFragment);
        }

        public bool SafeToDuplicateInTransport()
        {
            if (this is ClientUpdateRequestEvent)
            {
                return false;
            }
            else
            {
                // all others are safe to duplicate:
                // -   duplicate responses sent to clients are simply ignored
                // -   duplicate read requests cause no harm (other than redundant work)
                // -   duplicate partition events are deduplicated by Dedup

                return true;
            }
        }
    }
}