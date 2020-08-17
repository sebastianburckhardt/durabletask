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
using System.Threading.Tasks;
using DurableTask.Core;

namespace DurableTask.EventSourced
{
    [DataContract]
    internal class InstanceQueryReceived : PartitionQueryEvent, IClientRequestEvent
    {
        [DataMember]
        public Guid ClientId { get; set; }

        [DataMember]
        public long RequestId { get; set; }

        [DataMember]
        public DateTime TimeoutUtc { get; set; }

        [IgnoreDataMember]
        public override EventId EventId => EventId.MakeClientRequestEventId(ClientId, RequestId);

        public async override Task OnQueryCompleteAsync(IAsyncEnumerable<TrackedObject> instances, Partition partition)
        {
            var response = new QueryResponseReceived
            {
                ClientId = this.ClientId,
                RequestId = this.RequestId,
                OrchestrationStates = new List<OrchestrationState>()
            };

            await foreach (var trackedObject in instances)
            {
                var instanceState = trackedObject as InstanceState;
                partition.Assert(instanceState != null);

                if (this.Matches(instanceState.OrchestrationState))
                {
                    response.OrchestrationStates.Add(instanceState.OrchestrationState);
                }
            }

            partition.Send(response);
        }
    }
}
