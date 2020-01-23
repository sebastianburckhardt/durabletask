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
using System.Threading.Tasks;
using DurableTask.Core;
using DurableTask.Core.History;

namespace DurableTask.EventSourced
{
    [DataContract]
    internal class ReassemblyState : TrackedObject
    {
        [DataMember]
        public Dictionary<Guid, List<PartitionEventFragment>> Fragments { get; private set; } = new Dictionary<Guid, List<PartitionEventFragment>>();

        [IgnoreDataMember]
        public override TrackedObjectKey Key => new TrackedObjectKey(TrackedObjectKey.TrackedObjectType.Reassembly);


        // PartitionEventFragment 
        // stores fragments until the last one is received

        public override void Process(PartitionEventFragment evt, EffectList effects)
        {
            if (evt.IsLast)
            {
                evt.ReassembledEvent = (PartitionEvent) FragmentationAndReassembly.Reassemble(this.Fragments[evt.CohortId], evt);
                
                if (EtwSource.EmitDiagnosticsTrace)
                {
                    this.Partition.DiagnosticsTrace($"Reassembled {evt.ReassembledEvent}");
                }

                this.Fragments.Remove(evt.CohortId);

                evt.ReassembledEvent.DetermineEffects(effects);
            }
            else
            {
                if (!this.Fragments.TryGetValue(evt.CohortId, out var list))
                {
                    this.Fragments[evt.CohortId] = list = new List<PartitionEventFragment>();
                }
                list.Add(evt);
            }
        } 
    }
}
