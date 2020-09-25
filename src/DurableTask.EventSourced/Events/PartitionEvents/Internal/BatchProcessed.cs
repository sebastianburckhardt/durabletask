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
using DurableTask.Core.History;

namespace DurableTask.EventSourced
{
    [DataContract]
    internal class BatchProcessed : PartitionUpdateEvent
    {
        [DataMember]
        public long SessionId { get; set; }

        [DataMember]
        public string InstanceId { get; set; }

        [DataMember]
        public long BatchStartPosition { get; set; }

        [DataMember]
        public int BatchLength { get; set; }

        [DataMember]
        public List<HistoryEvent> NewEvents { get; set; }

        [DataMember]
        public OrchestrationState State { get; set; }

        [DataMember]
        public List<TaskMessage> ActivityMessages { get; set; }

        [DataMember]
        public List<TaskMessage> LocalMessages { get; set; }

        [DataMember]
        public List<TaskMessage> RemoteMessages { get; set; }

        [DataMember]
        public List<TaskMessage> TimerMessages { get; set; }

        [DataMember]
        public DateTime Timestamp { get; set; }

        [DataMember]
        public bool IsPersisted { get; set; }

        [IgnoreDataMember]
        public OrchestrationWorkItem WorkItemForReuse { get; set; }

        [IgnoreDataMember]
        public string WorkItemId => SessionsState.GetWorkItemId(this.PartitionId, this.SessionId, this.BatchStartPosition);

        [IgnoreDataMember]
        public override EventId EventId => EventId.MakePartitionInternalEventId(this.IsPersisted ? this.WorkItemId + "P" : this.WorkItemId);

        [IgnoreDataMember]
        public override IEnumerable<TaskMessage> TracedTaskMessages 
        { 
            get
            {
                if (this.ActivityMessages != null)
                    foreach (var a in this.ActivityMessages)
                        yield return a;
                if (this.LocalMessages != null)
                    foreach (var l in this.LocalMessages)
                        yield return l;
                if (this.RemoteMessages != null)
                    foreach (var r in this.RemoteMessages)
                        yield return r;
                // we are not including the timer messages because they are considered "sent" at the time the timer fires, not when it is scheduled
            }
        }

        public override void DetermineEffects(EffectTracker effects)
        {
            // start on the sessions object; further effects are determined from there
            effects.Add(TrackedObjectKey.Sessions);
        }

        protected override void ExtraTraceInformation(StringBuilder s)
        {
            base.ExtraTraceInformation(s);

            if (State != null)
            {
                s.Append(' ');
                s.Append(State.OrchestrationStatus);
            }

            s.Append(' ');
            s.Append(this.InstanceId);
        }
    }
}
