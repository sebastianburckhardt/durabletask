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

        [IgnoreDataMember]
        public override TrackedObjectKey Key => new TrackedObjectKey(TrackedObjectKey.TrackedObjectType.Instance, this.InstanceId);

        public override string ToString()
        {
            return $"History InstanceId={InstanceId} Status={OrchestrationState?.OrchestrationStatus}";
        }

        // CreationRequestReceived
        // can create or replace an instance and return a success response, or 
        // return an error response

        public void Process(CreationRequestReceived evt, EffectTracker effects)
        {
            if (this.OrchestrationState != null
                && evt.DedupeStatuses != null
                && evt.DedupeStatuses.Contains(this.OrchestrationState.OrchestrationStatus))
            {
                // An instance already exists and has a state for which we want to dedup.
                // Therefore, we do nothing other than responding to the client so they
                // know that creation failed.
                if (!effects.IsReplaying)
                {
                    this.Partition.Send(new CreationResponseReceived()
                    {
                        ClientId = evt.ClientId,
                        RequestId = evt.RequestId,
                        Succeeded = false,
                    });
                }
            }
            else
            {
                var ee = evt.ExecutionStartedEvent;

                // set the orchestration state now (before processing the creation in the history)
                // so that this instance is "on record" immediately
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

                // update the index also
                effects.Add(TrackedObjectKey.Index);

                // process the creation message on the sessions object
                // so it can be added to the queue for workitems
                effects.Add(TrackedObjectKey.Sessions);

                if (!effects.IsReplaying)
                {
                    this.Partition.Send(new CreationResponseReceived()
                    {
                        ClientId = evt.ClientId,
                        RequestId = evt.RequestId,
                        Succeeded = true,
                    });

                }
            }
        }

        // BatchProcessed
        // updates the state of an orchestration and notifies observers

        public void Process(BatchProcessed evt, EffectTracker effects)
        {
            this.OrchestrationState = evt.State;

            // update the index also
            effects.Add(TrackedObjectKey.Index);

            // notify observers that this orchestration state has changed
            if (!effects.IsReplaying)
            {
                this.Partition.InstanceStatePubSub.Notify(InstanceId, OrchestrationState);
            }
        }
    }
}