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
        public List<WaitRequestReceived> Waiters { get; set; }

        [IgnoreDataMember]
        public override TrackedObjectKey Key => new TrackedObjectKey(TrackedObjectKey.TrackedObjectType.Instance, this.InstanceId);

        public override string ToString()
        {
            return $"History InstanceId={this.InstanceId} Status={this.OrchestrationState?.OrchestrationStatus}";
        }

        public void Process(CreationRequestReceived creationRequestReceived, EffectTracker effects)
        {
            bool filterDuplicate = this.OrchestrationState != null
                && creationRequestReceived.DedupeStatuses != null
                && creationRequestReceived.DedupeStatuses.Contains(this.OrchestrationState.OrchestrationStatus);

            // Use this moment of time as the creation timestamp, replacing the original timestamp taken on the client.
            // This is preferrable because it avoids clock synchronization issues (which can result in negative orchestration durations)
            // and means the timestamp is consistently ordered with respect to timestamps of other events on this partition.
            ((ExecutionStartedEvent)creationRequestReceived.TaskMessage.Event).Timestamp = DateTime.UtcNow;

            if (!filterDuplicate)
            {
                var ee = creationRequestReceived.ExecutionStartedEvent;

                // set the orchestration state now (before processing the creation in the history)
                // so that this new instance is "on record" immediately - it is guaranteed to replace whatever is in flight
                this.OrchestrationState = new OrchestrationState
                {
                    Name = ee.Name,
                    Version = ee.Version,
                    OrchestrationInstance = ee.OrchestrationInstance,
                    OrchestrationStatus = OrchestrationStatus.Pending,
                    ParentInstance = ee.ParentInstance,
                    Input = ee.Input,
                    Tags = ee.Tags,
                    CreatedTime = ee.Timestamp,
                    LastUpdatedTime = DateTime.UtcNow,
                    CompletedTime = Core.Common.DateTimeUtils.MinDateTime,
                    ScheduledStartTime = ee.ScheduledStartTime
                };

                // queue the message in the session, or start a timer if delayed
                if (!ee.ScheduledStartTime.HasValue)
                {
                    effects.Add(TrackedObjectKey.Sessions);
                }
                else
                {
                    effects.Add(TrackedObjectKey.Timers);
                }
            }

            if (!effects.IsReplaying)
            {
                // send response to client
                effects.Partition.Send(new CreationResponseReceived()
                {
                    ClientId = creationRequestReceived.ClientId,
                    RequestId = creationRequestReceived.RequestId,
                    Succeeded = !filterDuplicate,
                    ExistingInstanceOrchestrationStatus = this.OrchestrationState?.OrchestrationStatus,
                });
            }
        }


        public void Process(BatchProcessed evt, EffectTracker effects)
        {
            // update the state of an orchestration
            this.OrchestrationState = evt.State;

            // if the orchestration is complete, notify clients that are waiting for it
            if (this.Waiters != null && WaitRequestReceived.SatisfiesWaitCondition(this.OrchestrationState))
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

        public void Process(WaitRequestReceived evt, EffectTracker effects)
        {
            if (WaitRequestReceived.SatisfiesWaitCondition(this.OrchestrationState))
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
                    this.Waiters = new List<WaitRequestReceived>();
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