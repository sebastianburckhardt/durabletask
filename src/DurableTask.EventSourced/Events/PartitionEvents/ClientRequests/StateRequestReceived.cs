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
using System.Diagnostics;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using DurableTask.Core;
using DurableTask.Core.Exceptions;
using DurableTask.Core.History;

namespace DurableTask.EventSourced
{
    [DataContract]
    internal class StateRequestReceived : ClientRequestEvent
    {
        [DataMember]
        public string InstanceId { get; set; }

        [IgnoreDataMember]
        public override bool AtMostOnce => false;

        [IgnoreDataMember]
        public override bool PersistInLog => false;

        public override void DetermineEffects(TrackedObject.EffectList effects)
        {
            effects.Partition.Assert(!effects.InRecovery);
            effects.Partition.State.ScheduleRead(new Waiter(effects.Partition, this.InstanceId, this.ClientId, this.RequestId));
        }

        private class Waiter : StorageAbstraction.IReadContinuation
        {
            private readonly Partition partition;
            private readonly Guid clientId;
            private readonly long requestId;

            public Waiter(Partition partition, string instanceId, Guid clientId, long requestId)
            {
                this.partition = partition;
                this.clientId = clientId;
                this.requestId = requestId;
                this.ReadTarget = TrackedObjectKey.Instance(instanceId);
            }

            public TrackedObjectKey ReadTarget { get; }

            public void OnReadComplete(TrackedObject target)
            {
                var orchestrationState = ((InstanceState)target)?.OrchestrationState;

                var response = new StateResponseReceived()
                {
                    ClientId = this.clientId,
                    RequestId = this.requestId,
                    OrchestrationState = orchestrationState,
                };

                this.partition.Send(response);
            }
        }
    }
}