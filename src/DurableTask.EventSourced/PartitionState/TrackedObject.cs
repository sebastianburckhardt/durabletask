﻿//  ----------------------------------------------------------------------------------
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

using Dynamitey;
using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization;
using System.Threading.Tasks;

namespace DurableTask.EventSourced
{
    [DataContract]
    [KnownTypeAttribute("KnownTypes")]
    internal abstract class TrackedObject
    {
        [DataMember]
        internal long LastProcessed { get; set; } = -1;

        [IgnoreDataMember]
        protected Partition Partition;

        [IgnoreDataMember]
        public abstract TrackedObjectKey Key { get; }

        [IgnoreDataMember]
        internal byte[] SerializedSnapshot { get; set; }

        // used by the state storage backend to protect from conflicts
        [IgnoreDataMember]
        internal object AccessLock => this;

        // call after deserialization, or after simulating a recovery
        public void Restore(Partition Partition)
        {
            if (this.Partition != Partition)
            {
                this.Partition = Partition;
                this.Restore();
            }
        }

        private static IEnumerable<Type> KnownTypes()
        {
            foreach (var t in Core.History.HistoryEvent.KnownTypes())
            {
                yield return t;
            }
            foreach (var t in DurableTask.EventSourced.Event.KnownTypes())
            {
                yield return t;
            }
            foreach (var t in TrackedObjectKey.TypeMap.Values)
            {
                yield return t;
            }
        }

        protected virtual void Restore()
        {
            // subclasses override this if there is work they need to do here
        }

        public void Apply(PartitionEvent e)
        {
            // the default apply for an object does not update any state.
        }

        public virtual void Process(PartitionEventFragment e, EffectTracker effect)
        {
            // the default scope for a reassembled event applies that event
            dynamic dynamicThis = this;
            dynamic dynamicPartitionEvent = e.ReassembledEvent;
            dynamicThis.Process(dynamicPartitionEvent, effect);
        }

        public virtual void Apply(PartitionEventFragment e)
        {
            // the default apply for a reassembled event applies that event
            dynamic dynamicThis = this;
            dynamic dynamicPartitionEvent = e.ReassembledEvent;
            dynamicThis.Apply(dynamicPartitionEvent);
        }

        public class EffectTracker
        {
            public List<TrackedObjectKey> ObjectsToProcessOn = new List<TrackedObjectKey>();
            public List<TrackedObjectKey> ObjectsToApplyTo = new List<TrackedObjectKey>();

            public void ProcessOn(TrackedObjectKey o)
            {
                ObjectsToProcessOn.Add(o);
            }

            public void ApplyTo(TrackedObjectKey o)
            {
                ObjectsToApplyTo.Add(o);
            }

            public void Clear()
            {
                ObjectsToProcessOn.Clear();
                ObjectsToApplyTo.Clear();
            }
        }
     
    }
}
