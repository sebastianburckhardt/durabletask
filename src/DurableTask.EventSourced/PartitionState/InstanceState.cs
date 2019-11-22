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
        private static DataContractSerializer serializer = new DataContractSerializer(typeof(InstanceState));
        protected override DataContractSerializer Serializer => serializer;

        [IgnoreDataMember]
        public string InstanceId => Key.InstanceId;

        [DataMember]
        public OrchestrationState OrchestrationState { get; set; }

        public static OrchestrationState GetOrchestrationState(InstanceState state)
        {
            return state.OrchestrationState;
        }

        // CreationRequestReceived

        public void Process(CreationRequestReceived evt, EffectTracker effect)
        {
            if (this.OrchestrationState != null
                && evt.DedupeStatuses != null
                && evt.DedupeStatuses.Contains(this.OrchestrationState.OrchestrationStatus))
            {
                // An instance in this state already exists. do nothing but respond to client.
                this.Partition.Send(new CreationResponseReceived()
                {
                    ClientId = evt.ClientId,
                    RequestId = evt.RequestId,
                    Succeeded = false,
                });
            }
            else
            {
                effect.ApplyTo(TrackedObjectKey.Sessions);
                effect.ApplyTo(this.Key);

                this.Partition.Send(new CreationResponseReceived()
                {
                    ClientId = evt.ClientId,
                    RequestId = evt.RequestId,
                    Succeeded = true,
                });
            }
        }

        public void Apply(CreationRequestReceived evt)
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
        }

        // BatchProcessed
    
        public void Apply(BatchProcessed evt)
        {
            this.OrchestrationState = evt.State;

            this.Partition.InstanceStatePubSub.Notify(InstanceId, OrchestrationState);
        }
    }
}