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
using System.Threading;
using System.Threading.Tasks;
using DurableTask.Core;
using DurableTask.Core.Exceptions;
using DurableTask.Core.History;

namespace DurableTask.EventSourced
{
    [DataContract]
    internal class HistoryState : TrackedObject
    {
        [DataMember]
        public string InstanceId { get; set; }

        [DataMember]
        public string ExecutionId { get; set; }

        [DataMember]
        public List<HistoryEvent> History { get; set; }

        /// <summary>
        /// We cache this so we can resume the execution at the execution cursor.
        /// </summary>
        [IgnoreDataMember]
        public OrchestrationWorkItem CachedOrchestrationWorkItem { get; set; } 

        [IgnoreDataMember]
        public override TrackedObjectKey Key => new TrackedObjectKey(TrackedObjectKey.TrackedObjectType.History, this.InstanceId);


        public override string ToString()
        {
            return $"History InstanceId={InstanceId} ExecutionId={ExecutionId} Events={History.Count}";
        }

        // BatchProcessed
        // can add events to the history, or replace it with a new history

        public void Process(BatchProcessed evt, EffectTracker effects)
        {
            // update the stored history
            if (this.History == null || evt.State.OrchestrationInstance.ExecutionId != this.ExecutionId)
            {
                this.History = new List<HistoryEvent>();
                this.ExecutionId = evt.State.OrchestrationInstance.ExecutionId;
            }

            // TODO add some checking here

            if (evt.NewEvents != null)
            {
                this.History.AddRange(evt.NewEvents);
            }

            // cache the work item for reuse, if supplied
            this.CachedOrchestrationWorkItem = evt.CachedWorkItem;
        }

    }
}