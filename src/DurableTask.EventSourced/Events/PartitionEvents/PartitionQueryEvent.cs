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
using System.Text;
using System.Threading.Tasks;
using DurableTask.Core;

namespace DurableTask.EventSourced
{
    [DataContract]
    internal abstract class PartitionQueryEvent : PartitionEvent
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

        /// <summary>
        /// The continuation for the query operation.
        /// </summary>
        /// <param name="result">The tracked objects returned by this query</param>
        /// <param name="partition">The partition</param>
        public abstract Task OnQueryCompleteAsync(IAsyncEnumerable<TrackedObject> result, Partition partition);
    }
}
