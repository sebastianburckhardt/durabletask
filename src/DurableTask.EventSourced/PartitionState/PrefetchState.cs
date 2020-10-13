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
    internal class PrefetchState : TrackedObject
    {
        [DataMember]
        public Dictionary<string, ClientRequestEventWithPrefetch> PendingRequests { get; private set; } = new Dictionary<string, ClientRequestEventWithPrefetch>();

        [IgnoreDataMember]
        public override TrackedObjectKey Key => new TrackedObjectKey(TrackedObjectKey.TrackedObjectType.Prefetch);

        public override void OnRecoveryCompleted()
        {
            // reissue prefetch tasks for what did not complete prior to crash/recovery
            foreach (var kvp in PendingRequests)
            {
                this.Partition.SubmitInternalEvent(new InstanceLoopkup(kvp.Value));
            }
        }

        public override void UpdateLoadInfo(PartitionLoadInfo info)
        {
            info.WorkItems += this.PendingRequests.Count;
        }

        public override string ToString()
        {
            return $"Prefetch ({this.PendingRequests.Count} pending)";
        }

        public void Process(ClientRequestEventWithPrefetch clientRequestEvent, EffectTracker effects)
        {
            if (clientRequestEvent.Phase == ClientRequestEventWithPrefetch.ProcessingPhase.Read)
            {           
                this.Partition.Assert(!this.PendingRequests.ContainsKey(clientRequestEvent.EventIdString));

                // Issue a read request that fetches the instance state.
                // We have to buffer this request in the pending list so we can recover it.

                this.PendingRequests.Add(clientRequestEvent.EventIdString, clientRequestEvent);

                if (!effects.IsReplaying)
                {
                    this.Partition.SubmitInternalEvent(new InstanceLoopkup(clientRequestEvent));
                }
            }
            else 
            {
                if (this.PendingRequests.Remove(clientRequestEvent.EventIdString))
                {
                    if (clientRequestEvent.Phase == ClientRequestEventWithPrefetch.ProcessingPhase.ConfirmAndProcess)
                    {
                        effects.Add(clientRequestEvent.Target);
                    }
                }
            }
        }

        internal class InstanceLoopkup : InternalReadEvent
        {
            private readonly ClientRequestEventWithPrefetch request;

            public InstanceLoopkup(ClientRequestEventWithPrefetch clientRequest)
            {
                this.request = clientRequest;
            }
            
            public override TrackedObjectKey ReadTarget => this.request.Target;

            public override TrackedObjectKey? Prefetch => this.request.Prefetch;

            public override EventId EventId => this.request.EventId;

            public override void OnReadComplete(TrackedObject target, Partition partition)
            {
                partition.Assert(this.request.Phase == ClientRequestEventWithPrefetch.ProcessingPhase.Read);

                bool requiresProcessing = this.request.OnReadComplete(target, partition);

                this.request.Phase = requiresProcessing ?
                    ClientRequestEventWithPrefetch.ProcessingPhase.ConfirmAndProcess : ClientRequestEventWithPrefetch.ProcessingPhase.Confirm;
                 
                partition.SubmitInternalEvent(this.request);
            }
        }
    }
}
