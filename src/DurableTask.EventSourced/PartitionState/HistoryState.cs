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
using DurableTask.Core;
using DurableTask.Core.Exceptions;
using DurableTask.Core.History;

namespace DurableTask.EventSourced
{
    [DataContract]
    internal class HistoryState : TrackedObject
    {
        private static DataContractSerializer serializer = new DataContractSerializer(typeof(HistoryState));
        protected override DataContractSerializer Serializer => serializer;

        [IgnoreDataMember]
        public string InstanceId => Key.InstanceId;

        [DataMember]
        public string ExecutionId { get; set; }

        [DataMember]
        public List<HistoryEvent> History { get; set; }

        [IgnoreDataMember]
        private OrchestrationRuntimeState cachedRuntimeState;

        public static OrchestrationRuntimeState GetRuntimeState(HistoryState state)
        {
            return state.cachedRuntimeState ?? (state.cachedRuntimeState = new OrchestrationRuntimeState(state.History));
        }

        // BatchProcessed

        public void Apply(BatchProcessed evt)
        {
            if (this.History == null)
            {
                this.History = new List<HistoryEvent>();
                this.ExecutionId = evt.State.OrchestrationInstance.ExecutionId;
            }

            else if (evt.State.OrchestrationInstance.ExecutionId != this.ExecutionId)
            {
                this.History.Clear();
                this.ExecutionId = evt.State.OrchestrationInstance.ExecutionId;
            }

            if (this.cachedRuntimeState != null)
            {
                if (this.cachedRuntimeState.OrchestrationInstance.ExecutionId == this.ExecutionId)
                {
                    this.cachedRuntimeState?.NewEvents.Clear();
                }
                else
                {
                    this.cachedRuntimeState = null;
                }
            }
            
            if (evt.NewEvents != null)
            {
                this.History.AddRange(evt.NewEvents);
            }
        }
    }
}