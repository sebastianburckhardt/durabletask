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
    internal class InstanceQueryReceived : ClientRequestEvent, IPartitionEventWithSideEffects
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

        public void DetermineEffects(EffectTracker effects)
        {
            effects.Partition.Assert(!effects.IsReplaying);
            var response = new QueryResponseReceived
            {
                ClientId = this.ClientId,
                RequestId = this.RequestId
            };
            effects.Partition.State.ScheduleRead(new QueryWaiter(effects.Partition, this, response));
        }

        private class QueryWaiter : StorageAbstraction.IReadContinuation
        {
            private readonly Partition partition;
            private readonly InstanceQueryReceived query;
            private readonly QueryResponseReceived response;

            public QueryWaiter(Partition partition, InstanceQueryReceived query, QueryResponseReceived response)
            { 
                this.partition = partition;
                this.query = query;
                this.response = response;
            }

            public TrackedObjectKey ReadTarget => TrackedObjectKey.Index;

            public void OnReadComplete(TrackedObject target)
            {
                if (target is null)
                {
                    // Short-circuit if none
                    this.partition.Send(this.response);
                }

                if (!(target is IndexState indexState))
                {
                    throw new ArgumentException(nameof(target));
                }

                if (indexState.InstanceIds.Count == 0)
                {
                    // Short-circuit if empty
                    this.partition.Send(this.response);
                }

                // We're all on one partition here--the IndexState has only the InstanceIds for its partition.
                response.ExpectedCount = indexState.InstanceIds.Count;
                foreach (var instanceId in indexState.InstanceIds)
                {
                    // For now the query is implemented as an enumeration over the singleton IndexState's InstanceIds;
                    // read the InstanceState and compare state fields.
                    this.partition.State.ScheduleRead(new ResponseWaiter(this.partition, this.query, instanceId, this.response));
                }
            }
        }

        private class ResponseWaiter : StorageAbstraction.IReadContinuation
        {
            private readonly Partition partition;
            private readonly InstanceQueryReceived query;
            private readonly QueryResponseReceived response;

            public ResponseWaiter(Partition partition, InstanceQueryReceived query, string instanceId, QueryResponseReceived response)
            {
                this.partition = partition;
                this.query = query;
                this.response = response;
                this.ReadTarget = TrackedObjectKey.Instance(instanceId);
            }

            public TrackedObjectKey ReadTarget { get; private set; }

            public void OnReadComplete(TrackedObject target)
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
                    this.partition.Send(response);
                }
            }
        }
    }
}
