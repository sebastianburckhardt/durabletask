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
using System.Threading.Tasks;
using DurableTask.Core;
using DurableTask.Core.History;

namespace DurableTask.EventSourced
{
    [DataContract]
    internal class DedupState : TrackedObject
    {
        [DataMember]
        public Dictionary<uint, long> ProcessedOrigins { get; set; } = new Dictionary<uint, long>();

        [IgnoreDataMember]
        public override TrackedObjectKey Key => new TrackedObjectKey(TrackedObjectKey.TrackedObjectType.Dedup);
        
        // TaskMessageReceived 
        // filters any messages that originated on a partition, and whose origin is marked as processed

        public void Process(TaskMessageReceived evt, EffectList effects)
        {
            long alreadyProcessed = -1;
            this.ProcessedOrigins.TryGetValue(evt.OriginPartition, out alreadyProcessed);
            if (evt.OriginPosition > alreadyProcessed)
            {
                effects.Add(TrackedObjectKey.Sessions);
                this.ProcessedOrigins[evt.OriginPartition] = evt.OriginPosition;
            }
        }
    }
}
