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
using DurableTask.Core.Exceptions;
using DurableTask.Core.History;

namespace DurableTask.EventSourced
{
    [DataContract]
    internal class CreationRequestReceived : ClientUpdateRequestEvent
    {
        [DataMember]
        public OrchestrationStatus[] DedupeStatuses { get; set; }

        [DataMember]
        public DateTime Timestamp { get; set; }

        [DataMember]
        public TaskMessage TaskMessage { get; set; }

        [IgnoreDataMember]
        public ExecutionStartedEvent ExecutionStartedEvent => this.TaskMessage.Event as ExecutionStartedEvent;

        [IgnoreDataMember]
        public string InstanceId => ExecutionStartedEvent.OrchestrationInstance.InstanceId;

        [IgnoreDataMember]
        public override IEnumerable<TaskMessage> TracedTaskMessages { get { yield return this.TaskMessage; } }

        public override void DetermineEffects(EffectTracker effects)
        {
            // the creation request is first buffered in the prefetch state while the instance info is loaded
            effects.Add(TrackedObjectKey.Creation);
        }
    }

}