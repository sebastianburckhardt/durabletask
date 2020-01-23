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
        public override bool AtLeastOnceDelivery => true;

        [IgnoreDataMember]
        public override bool PersistInLog => false;

        public override void DetermineEffects(TrackedObject.EffectList effects)
        {
            Debug.Assert(!effects.InRecovery);
            var task = ReadAsync(effects.Partition);
        }

        public async Task ReadAsync(Partition partition)
        {
            try
            {
                var orchestrationState = await partition.State.ReadAsync<InstanceState, OrchestrationState>(
                     TrackedObjectKey.Instance(this.InstanceId),
                     InstanceState.GetOrchestrationState);

                var response = new StateResponseReceived()
                {
                    ClientId = this.ClientId,
                    RequestId = this.RequestId,
                    OrchestrationState = orchestrationState,
                };

                partition.Send(response);
            }
            catch (TaskCanceledException)
            {
            }
            catch (Exception e)
            {
                partition.ReportError($"{nameof(StateRequestReceived)}.{nameof(ReadAsync)}({this.GetType().Name})", e);
            }
        }
    }
}