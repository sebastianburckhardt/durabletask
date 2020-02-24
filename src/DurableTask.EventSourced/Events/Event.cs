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
using DurableTask.Core;

namespace DurableTask.EventSourced
{
    [DataContract]
    [KnownTypeAttribute("KnownTypes")]
    internal abstract class Event
    {
        /// <summary>
        /// For events entering the commit log, the position at which this event committed
        /// </summary>
        [IgnoreDataMember]
        public ulong? CommitLogPosition { get; set; }

        /// <summary>
        /// For events coming from the input queue, the input queue position.
        /// </summary>
        [DataMember]
        public ulong? InputQueuePosition { get; set; }

        [IgnoreDataMember]
        public AckListeners AckListeners;

        public override string ToString()
        {
            var s = new StringBuilder();
            s.Append(this.GetType().Name);
            this.TraceInformation(s);
            return s.ToString();
        }

        protected virtual void TraceInformation(StringBuilder s)
        {
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
            if (this is ClientEvent)
            {
                // duplicate responses sent to clients are simply ignored
                return true; 
            }
            else if (this is ClientRequestEvent)
            {
                // requests from clients are safe to duplicate if and only if they are read-only
                return this is IReadonlyPartitionEvent;
            }
            else
            {
                // all partition events are safe to duplicate because they are perfectly deduplicated by Dedup
                return true; 
            }
        }

        #region ETW trace properties

        [IgnoreDataMember]
        public virtual string WorkItem => string.Empty;

        #endregion
    }
}