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
using DurableTask.Core;

namespace DurableTask.EventSourced
{
    [DataContract]
    internal class InstanceQueryReceived : ClientReadRequestEvent
    {   
        /// <summary>
        /// The subset of runtime statuses to return, or null if all
        /// </summary>
        [DataMember]
        public IEnumerable<OrchestrationStatus> RuntimeStatus { get; set; }

        /// <summary>
        /// The lowest creation time to return, or null if no lower bound
        /// </summary>
        [DataMember]
        public DateTime? CreatedTimeFrom { get; set; }

        /// <summary>
        /// The latest creation time to return, or null if no upper bound
        /// </summary>
        [DataMember]
        public DateTime? CreatedTimeTo { get; set; }

        /// <summary>
        /// A prefix of the instance ids to return, or null if no specific prefix
        /// </summary>
        [DataMember]
        public string InstanceIdPrefix { get; set; }

        internal bool Matches(OrchestrationState targetState) 
            => (this.RuntimeStatus is null || this.RuntimeStatus.Contains(targetState.OrchestrationStatus))
                 && (string.IsNullOrWhiteSpace(this.InstanceIdPrefix) || targetState.OrchestrationInstance.InstanceId.StartsWith(this.InstanceIdPrefix))
                 && (this.CreatedTimeFrom is null || targetState.CreatedTime >= this.CreatedTimeFrom)
                 && (this.CreatedTimeTo is null || targetState.CreatedTime <= this.CreatedTimeTo);

        public override void OnReadIssued(Partition partition)
        {
            partition.Assert(partition.RecoveryIsComplete);
        }

        [IgnoreDataMember]
        public override TrackedObjectKey ReadTarget => TrackedObjectKey.Index;

        public override void OnReadComplete(TrackedObject target, Partition partition)
        {
            var response = new QueryResponseReceived
            {
                ClientId = this.ClientId,
                RequestId = this.RequestId
            };

            if (target is null)
            {
                // Short-circuit if none
               partition.Send(response);
            }

            if (!(target is IndexState indexState))
            {
                throw new ArgumentException(nameof(target));
            }

            if (indexState.InstanceIds.Count == 0)
            {
                // Short-circuit if empty
                partition.Send(response);
            }

            // We're all on one partition here--the IndexState has only the InstanceIds for its partition.
            response.ExpectedCount = indexState.InstanceIds.Count;

            int counter = 0;

            foreach (var instanceId in indexState.InstanceIds)
            {
                // For now the query is implemented as an enumeration over the singleton IndexState's InstanceIds;
                // read the InstanceState and compare state fields.
                partition.SubmitInternalEvent(new ResponseWaiter(partition, this, instanceId, counter++, response));
            }
        }

        private class ResponseWaiter : InternalReadEvent
        {
            private readonly InstanceQueryReceived query;
            private readonly QueryResponseReceived response;
            private readonly int index;

            public ResponseWaiter(Partition partition, InstanceQueryReceived query, string instanceId, int index, QueryResponseReceived response)
            {
                this.query = query;
                this.response = response;
                this.index = index;
                this.ReadTarget = TrackedObjectKey.Instance(instanceId);
            }

            public override EventId EventId => EventId.MakeSubEventId(query.EventId, index);

            public override TrackedObjectKey ReadTarget { get; }

            public override void OnReadComplete(TrackedObject target, Partition partition)
            {
                if (target is null)
                {
                    throw new ArgumentNullException(nameof(target));
                }

                if (!(target is InstanceState instanceState))
                {
                    throw new ArgumentException(nameof(target));
                }

                if (!query.Matches(instanceState.OrchestrationState))
                {
                    this.response.DiscardState();
                }
                else
                {
                    this.response.OrchestrationStates.Add(instanceState.OrchestrationState);
                }

                if (this.response.IsDone)
                {
                    partition.Send(response);
                }
            }
        }
    }
}
