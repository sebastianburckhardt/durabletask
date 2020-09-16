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

using DurableTask.Core.History;
using DurableTask.EventSourced.Scaling;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;

namespace DurableTask.EventSourced
{
    [DataContract]
    internal class CreationState : TrackedObject
    {
        [DataMember]
        public Dictionary<string, CreationRequestReceived> PendingCreations { get; private set; } = new Dictionary<string, CreationRequestReceived>();

        [IgnoreDataMember]
        public override TrackedObjectKey Key => new TrackedObjectKey(TrackedObjectKey.TrackedObjectType.Creation);

        public override void OnRecoveryCompleted()
        {
            // reissue creation prefetch tasks for what did not complete prior to crash/recovery
            foreach (var kvp in PendingCreations)
            {
                this.Partition.SubmitInternalEvent(new InstanceLoopkup(kvp.Value, this.Partition));
            }
        }

        public override void UpdateLoadInfo(PartitionLoadInfo info)
        {
            info.WorkItems += this.PendingCreations.Count;
        }

        public override string ToString()
        {
            return $"Creation ({PendingCreations.Count} pending)";
        }

        public void Process(CreationRequestReceived creationRequestReceived, EffectTracker effects)
        {
            this.Partition.Assert(!this.PendingCreations.ContainsKey(creationRequestReceived.EventIdString));

            // Issue a read request that fetches the instance state.
            // We have to buffer this request in the pending list so we can recover it.

            this.PendingCreations.Add(creationRequestReceived.EventIdString, creationRequestReceived);

            if (!effects.IsReplaying)
            {
                this.Partition.SubmitInternalEvent(new InstanceLoopkup(creationRequestReceived, this.Partition));
            }
        }

        internal class InstanceLoopkup : InternalReadEvent
        {
            private readonly CreationRequestReceived request;

            public InstanceLoopkup(CreationRequestReceived creationRequestReceived, Partition partition)
            {
                this.request = creationRequestReceived;
            }

            public override TrackedObjectKey ReadTarget => TrackedObjectKey.Instance(request.InstanceId);         

            public override EventId EventId => EventId.MakeSubEventId(this.request.EventId, 0);

            public override void OnReadComplete(TrackedObject target, Partition partition)
            {
                var instanceState = (InstanceState)target;

                bool filterDuplicate = instanceState?.OrchestrationState != null
                    && this.request.DedupeStatuses != null
                    && this.request.DedupeStatuses.Contains(instanceState.OrchestrationState.OrchestrationStatus);

                // Use this moment of time as the creation timestamp, replacing the original timestamp taken on the client.
                // This is preferrable because it avoids clock synchronization issues (which can result in negative orchestration durations)
                // and means the timestamp is consistently ordered with respect to timestamps of other events on this partition.
                ((ExecutionStartedEvent)this.request.TaskMessage.Event).Timestamp = DateTime.UtcNow;

                partition.SubmitInternalEvent(new CreationRequestProcessed()
                {
                    PartitionId = partition.PartitionId,
                    ClientId = this.request.ClientId,
                    RequestId = this.request.RequestId,
                    TaskMessage = this.request.TaskMessage,
                    CreationRequestEventId = this.request.EventIdString,
                    FilteredDuplicate = filterDuplicate,
                });
            }
        }

        public void Process(CreationRequestProcessed creationRequestProcessed, EffectTracker effects)
        {
            if (this.PendingCreations.Remove(creationRequestProcessed.CreationRequestEventId))
            {
                if (!creationRequestProcessed.FilteredDuplicate)
                {
                    // update the instance state to "Pending" immediately
                    effects.Add(TrackedObjectKey.Instance(creationRequestProcessed.InstanceId));

                    // queue the message in the session, or start a timer if delayed
                    if (!creationRequestProcessed.ExecutionStartedEvent.ScheduledStartTime.HasValue)
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
                        ClientId = creationRequestProcessed.ClientId,
                        RequestId = creationRequestProcessed.RequestId,
                        Succeeded = !creationRequestProcessed.FilteredDuplicate,
                    });
                }
            }
        }
    }
}
