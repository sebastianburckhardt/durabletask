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
        public OrchestrationStatus[] RuntimeStatus { get; set; }

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

        internal bool HasRuntimeStatus => !(this.RuntimeStatus is null) && this.RuntimeStatus.Length > 0;

        internal bool IsSet => this.HasRuntimeStatus || !string.IsNullOrWhiteSpace(this.InstanceIdPrefix) 
                                || !(this.CreatedTimeFrom is null) || !(this.CreatedTimeTo is null);

        internal bool Matches(OrchestrationState targetState) 
            => (!this.HasRuntimeStatus || this.RuntimeStatus.Contains(targetState.OrchestrationStatus))
                 && (string.IsNullOrWhiteSpace(this.InstanceIdPrefix) || targetState.OrchestrationInstance.InstanceId.StartsWith(this.InstanceIdPrefix))
                 && (this.CreatedTimeFrom is null || targetState.CreatedTime >= this.CreatedTimeFrom)
                 && (this.CreatedTimeTo is null || targetState.CreatedTime <= this.CreatedTimeTo);

        private QueryResponseReceived response;

        public override void OnReadIssued(Partition partition)
        {
            partition.Assert(partition.RecoveryIsComplete);
            this.response = new QueryResponseReceived
            {
                ClientId = this.ClientId,
                RequestId = this.RequestId
            };
        }

        [IgnoreDataMember]
        public override TrackedObjectKey ReadTarget => this.readTarget;

        // There's no "set" accessor to override so we must add a setter function as we overwrite this with the actual instances.
        internal void SetReadTarget(TrackedObjectKey rt) => this.readTarget = rt;
        TrackedObjectKey readTarget = TrackedObjectKey.Index;

        public override void OnReadComplete(TrackedObject target, Partition partition)
        {
            if (target is null)
                throw new ArgumentNullException(nameof(target));

            if (!(target is InstanceState instanceState))
                throw new ArgumentException(nameof(target));

            if (Matches(instanceState.OrchestrationState))
                this.response.OrchestrationStates.Add(instanceState.OrchestrationState);
        }

        internal void OnPSFComplete(Partition partition) => partition.Send(response);
    }
}
