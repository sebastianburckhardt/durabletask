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
using DurableTask.Core.History;

namespace DurableTask.EventSourced
{
    [DataContract]
    internal class TimerFired : PartitionUpdateEvent
    {
        [DataMember]
        public long TimerId { get; set; }
        
        [DataMember]
        public DateTime Due { get; set; }

        [DataMember]
        public TaskMessage TaskMessage { get; set; }

        [IgnoreDataMember]
        public string WorkItemId => $"{this.PartitionId:D2}-T{TimerId}";

        [IgnoreDataMember]
        public override EventId EventId => EventId.MakePartitionInternalEventId(this.WorkItemId);

        [IgnoreDataMember]
        public override IEnumerable<TaskMessage> TracedTaskMessages { get { yield return TaskMessage; } }

        public override void DetermineEffects(EffectTracker effects)
        {
            effects.Add(TrackedObjectKey.Sessions);
            effects.Add(TrackedObjectKey.Timers);
        }
    }
}
