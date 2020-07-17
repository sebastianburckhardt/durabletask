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

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace DurableTask.EventSourced
{
    /// <summary>
    /// Represents a key used to identify <see cref="TrackedObject"/> instances.
    /// </summary>
    internal struct TrackedObjectKey 
    {
        public TrackedObjectType ObjectType;
        public string InstanceId;

        public TrackedObjectKey(TrackedObjectType objectType) { this.ObjectType = objectType; this.InstanceId = null; }
        public TrackedObjectKey(TrackedObjectType objectType, string instanceId) { this.ObjectType = objectType; this.InstanceId = instanceId; }

        public enum TrackedObjectType
        {
            // singletons
            Activities,
            Dedup,
            Outbox,
            Reassembly,
            Sessions,
            Timers,
            Creation,

            // non-singletons
            History,
            Instance,
        }

        public static Dictionary<TrackedObjectType, Type> TypeMap = new Dictionary<TrackedObjectType, Type>()
        {
            { TrackedObjectType.Activities, typeof(ActivitiesState) },
            { TrackedObjectType.Dedup, typeof(DedupState) },
            { TrackedObjectType.Outbox, typeof(OutboxState) },
            { TrackedObjectType.Reassembly, typeof(ReassemblyState) },
            { TrackedObjectType.Sessions, typeof(SessionsState) },
            { TrackedObjectType.Timers, typeof(TimersState) },
            { TrackedObjectType.Creation, typeof(CreationState) },
            
            // non-singletons
            { TrackedObjectType.History, typeof(HistoryState) },
            { TrackedObjectType.Instance, typeof(InstanceState) },
        };

        public static bool IsSingletonType(TrackedObjectType t) => (int) t < (int) TrackedObjectType.History;

        public bool IsSingleton => IsSingletonType(this.ObjectType);

        public static int Compare(ref TrackedObjectKey key1, ref TrackedObjectKey key2)
        {
            int result = key1.ObjectType.CompareTo(key2.ObjectType);
            return result == 0 ? key1.InstanceId.CompareTo(key2.InstanceId) : result;
        }

        public class Comparer : IComparer<TrackedObjectKey>
        {
            public int Compare(TrackedObjectKey x, TrackedObjectKey y) => TrackedObjectKey.Compare(ref x, ref y); 
        }

        // convenient constructors for singletons

        public static TrackedObjectKey Activities = new TrackedObjectKey() { ObjectType = TrackedObjectType.Activities };
        public static TrackedObjectKey Dedup = new TrackedObjectKey() { ObjectType = TrackedObjectType.Dedup };
        public static TrackedObjectKey Outbox = new TrackedObjectKey() { ObjectType = TrackedObjectType.Outbox };
        public static TrackedObjectKey Reassembly = new TrackedObjectKey() { ObjectType = TrackedObjectType.Reassembly };
        public static TrackedObjectKey Sessions = new TrackedObjectKey() { ObjectType = TrackedObjectType.Sessions };
        public static TrackedObjectKey Timers = new TrackedObjectKey() { ObjectType = TrackedObjectType.Timers };
        public static TrackedObjectKey Creation = new TrackedObjectKey() { ObjectType = TrackedObjectType.Creation };

        // convenient constructors for non-singletons

        public static TrackedObjectKey History(string id) => new TrackedObjectKey() 
            { 
                ObjectType = TrackedObjectType.History,
                InstanceId = id,
            };
        public static TrackedObjectKey Instance(string id) => new TrackedObjectKey() 
            {
                ObjectType = TrackedObjectType.Instance,
                InstanceId = id,
            };

        public static TrackedObject Factory(TrackedObjectKey key) => key.ObjectType switch
            {
                TrackedObjectType.Activities => new ActivitiesState(),
                TrackedObjectType.Dedup => new DedupState(),
                TrackedObjectType.Outbox => new OutboxState(),
                TrackedObjectType.Reassembly => new ReassemblyState(),
                TrackedObjectType.Sessions => new SessionsState(),
                TrackedObjectType.Timers => new TimersState(),
                TrackedObjectType.Creation => new CreationState(),
                TrackedObjectType.History => new HistoryState() { InstanceId = key.InstanceId },
                TrackedObjectType.Instance => new InstanceState() { InstanceId = key.InstanceId },
                _ => throw new ArgumentException("invalid key", nameof(key)),
            };

        public static IEnumerable<TrackedObjectKey> GetSingletons() 
            => Enum.GetValues(typeof(TrackedObjectType)).Cast<TrackedObjectType>().Where(t => IsSingletonType(t)).Select(t => new TrackedObjectKey() { ObjectType = t });

        public override string ToString() 
            => this.InstanceId == null ? this.ObjectType.ToString() : $"{this.ObjectType}-{this.InstanceId}";

        public void Deserialize(BinaryReader reader)
        {
            this.ObjectType = (TrackedObjectType) reader.ReadByte();
            if (!IsSingletonType(this.ObjectType))
            {
                this.InstanceId = reader.ReadString();
            }
        }

        public void Serialize(BinaryWriter writer)
        {
            writer.Write((byte) this.ObjectType);
            if (!IsSingletonType(this.ObjectType))
            {
                writer.Write(this.InstanceId);
            }
        }
    }
}
