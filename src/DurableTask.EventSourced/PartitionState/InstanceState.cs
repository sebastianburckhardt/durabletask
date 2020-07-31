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
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using DurableTask.Core;
using DurableTask.Core.Exceptions;
using DurableTask.Core.History;

namespace DurableTask.EventSourced
{
    [DataContract]
    internal class InstanceState : TrackedObject
    {
        [DataMember]
        public string InstanceId { get; set; }

        [DataMember]
        public OrchestrationState OrchestrationState { get; set; }

        [DataMember]
        public List<WaitRequestProcessed> Waiters { get; set; }

        [IgnoreDataMember]
        public override TrackedObjectKey Key => new TrackedObjectKey(TrackedObjectKey.TrackedObjectType.Instance, this.InstanceId);

        public override string ToString()
        {
            return $"History InstanceId={this.InstanceId} Status={this.OrchestrationState?.OrchestrationStatus}";
        }

        public void Process(CreationRequestProcessed evt, EffectTracker effects)
        {
            effects.Partition.Assert(!evt.FilteredDuplicate);

            var ee = evt.ExecutionStartedEvent;

            // set the orchestration state now (before processing the creation in the history)
            // so that this new instance is "on record" immediately - it is guaranteed to replace whatever is in flight
            this.OrchestrationState = new OrchestrationState
            {
                Name = ee.Name,
                Version = ee.Version,
                OrchestrationInstance = ee.OrchestrationInstance,
                OrchestrationStatus = OrchestrationStatus.Pending,
                Input = ee.Input,
                Tags = ee.Tags,
                CreatedTime = ee.Timestamp,
                LastUpdatedTime = evt.Timestamp,
                CompletedTime = Core.Common.DateTimeUtils.MinDateTime
            };
        }


        public void Process(BatchProcessed evt, EffectTracker effects)
        {
            // update the state of an orchestration
            this.OrchestrationState = evt.State;

            // if the orchestration is complete, notify clients that are waiting for it
            if (this.Waiters != null && WaitRequestProcessed.SatisfiesWaitCondition(this.OrchestrationState))
            {
                if (!effects.IsReplaying)
                {
                    foreach (var request in this.Waiters)
                    {
                        this.Partition.Send(request.CreateResponse(this.OrchestrationState));
                    }
                }

                this.Waiters = null;
            }
        }

        public void Process(WaitRequestProcessed evt, EffectTracker effects)
        {
            if (WaitRequestProcessed.SatisfiesWaitCondition(this.OrchestrationState))
            {
                if (!effects.IsReplaying)
                {
                    this.Partition.Send(evt.CreateResponse(this.OrchestrationState));
                }
            }
            else
            {
                if (this.Waiters == null)
                {
                    this.Waiters = new List<WaitRequestProcessed>();
                }
                else
                {
                    // cull the list of waiters to remove requests that have already timed out
                    this.Waiters = this.Waiters
                        .Where(request => request.TimeoutUtc > DateTime.UtcNow)
                        .ToList();
                }
                
                this.Waiters.Add(evt);
            }
        }
    }
}