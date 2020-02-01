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
using System.Runtime.Serialization;
using System.Text;
using DurableTask.Core;
using DurableTask.Core.Exceptions;
using DurableTask.Core.History;

namespace DurableTask.EventSourced
{
    [DataContract]
    internal class InstanceQueryReceived : ClientRequestEvent
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

        [IgnoreDataMember]
        public override bool AtMostOnce => false;

        [IgnoreDataMember]
        public override bool PersistInLog => false;

        public override void DetermineEffects(TrackedObject.EffectList effects)
        {
            effects.Partition.Assert(!effects.InRecovery);
            effects.Partition.State.ScheduleRead(new Waiter(effects.Partition, this));
        }

        private class Waiter : StorageAbstraction.IReadContinuation
        {
            private readonly Partition partition;
            private readonly InstanceQueryReceived query;

            public Waiter(Partition partition, InstanceQueryReceived query)
            { 
                this.partition = partition;
                this.query = query;
            }

            public TrackedObjectKey ReadTarget => TrackedObjectKey.Index;

            public void OnReadComplete(TrackedObject target)
            {
                var indexState = (IndexState) target;

                // here we have to implement the query
                // possibly issuing more reads in the process 
                

                var response = new QueryResponseReceived()
                {
                    // put result here
                };

                this.partition.Send(response);
            }
        }
    }
}
