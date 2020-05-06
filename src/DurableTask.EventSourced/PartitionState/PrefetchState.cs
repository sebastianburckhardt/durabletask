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

using DurableTask.EventSourced.Scaling;
using System;
using System.Collections.Generic;
using System.Runtime.Serialization;

namespace DurableTask.EventSourced
{
    [DataContract]
    internal class PrefetchState : TrackedObject
    {
        [DataMember]
        public Dictionary<string, CreationRequestReceived> Pending { get; private set; } = new Dictionary<string, CreationRequestReceived>();

        [IgnoreDataMember]
        public override TrackedObjectKey Key => new TrackedObjectKey(TrackedObjectKey.TrackedObjectType.Prefetch);

        public override void OnRecoveryCompleted()
        {
            // issue prefetch tasks for all prefetches that did not complete prior to crash/recovery
            foreach (var kvp in Pending)
            {
                this.Partition.SubmitInternalEvent(new Prefetch(kvp.Value, this.Partition));
            }
        }

        public override void UpdateLoadInfo(PartitionLoadInfo info)
        {
            info.WorkItems += this.Pending.Count;
        }

        public override string ToString()
        {
            return $"Prefetch ({Pending.Count} pending)";
        }

        public void Process(CreationRequestReceived creationRequestReceived, EffectTracker effects)
        {
            this.Partition.Assert(!this.Pending.ContainsKey(creationRequestReceived.EventIdString));
            this.Pending.Add(creationRequestReceived.EventIdString, creationRequestReceived);

            if (!effects.IsReplaying)
            {
                this.Partition.SubmitInternalEvent(new Prefetch(creationRequestReceived, this.Partition));
            }
        }

        internal class Prefetch : InternalReadEvent
        {
            private readonly CreationRequestReceived creationRequestReceived;

            public Prefetch(CreationRequestReceived creationRequestReceived, Partition partition)
            {
                this.creationRequestReceived = creationRequestReceived;
            }

            public override TrackedObjectKey ReadTarget => TrackedObjectKey.Instance(creationRequestReceived.InstanceId);         

            public override EventId EventId => EventId.MakeSubEventId(this.creationRequestReceived.EventId, 0);

            public override void OnReadComplete(TrackedObject target, Partition partition)
            {
                partition.SubmitInternalEvent(new CreationRequestProcessed()
                {
                    PartitionId = partition.PartitionId,
                    ClientId = this.creationRequestReceived.ClientId,
                    RequestId = this.creationRequestReceived.RequestId,
                    TaskMessage = this.creationRequestReceived.TaskMessage,
                    CreationRequestEventId = this.creationRequestReceived.EventIdString,
                    DedupeStatuses = this.creationRequestReceived.DedupeStatuses,                   
                    Timestamp = DateTime.UtcNow,
                });
            }
        }
               
        public void Process(CreationRequestProcessed creationRequestProcessed, EffectTracker effects)
        {
            this.Pending.Remove(creationRequestProcessed.CreationRequestEventId);
        }
    }
}
