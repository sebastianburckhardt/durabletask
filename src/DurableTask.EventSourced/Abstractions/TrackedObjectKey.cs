﻿using Dynamitey;
using FASTER.core;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

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
            Index,
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
            { TrackedObjectType.Index, typeof(IndexState) },
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
            if (result == 0)
            {
                result = key1.InstanceId.CompareTo(key2.InstanceId);
            }
            return result;
        }

        public class Comparer : IComparer<TrackedObjectKey>
        {
            public int Compare(TrackedObjectKey x, TrackedObjectKey y) => Compare(x, y); 
        }

        // convenient constructors for singletons

        public static TrackedObjectKey Activities = new TrackedObjectKey() { ObjectType = TrackedObjectType.Activities };
        public static TrackedObjectKey Dedup = new TrackedObjectKey() { ObjectType = TrackedObjectType.Dedup };
        public static TrackedObjectKey Index = new TrackedObjectKey() { ObjectType = TrackedObjectType.Index };
        public static TrackedObjectKey Outbox = new TrackedObjectKey() { ObjectType = TrackedObjectType.Outbox };
        public static TrackedObjectKey Reassembly = new TrackedObjectKey() { ObjectType = TrackedObjectType.Reassembly };
        public static TrackedObjectKey Sessions = new TrackedObjectKey() { ObjectType = TrackedObjectType.Sessions };
        public static TrackedObjectKey Timers = new TrackedObjectKey() { ObjectType = TrackedObjectType.Timers };
        public static TrackedObjectKey Prefetch = new TrackedObjectKey() { ObjectType = TrackedObjectType.Creation };

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

        public static TrackedObject Factory(TrackedObjectKey key)
        {
            switch (key.ObjectType)
            {
                case TrackedObjectKey.TrackedObjectType.Activities:
                    return new ActivitiesState();
                case TrackedObjectKey.TrackedObjectType.Dedup:
                    return new DedupState();
                case TrackedObjectKey.TrackedObjectType.Index:
                    return new IndexState();
                case TrackedObjectKey.TrackedObjectType.Outbox:
                    return new OutboxState();
                case TrackedObjectKey.TrackedObjectType.Reassembly:
                    return new ReassemblyState();
                case TrackedObjectKey.TrackedObjectType.Sessions:
                    return new SessionsState();
                case TrackedObjectKey.TrackedObjectType.Timers:
                    return new TimersState();
                case TrackedObjectKey.TrackedObjectType.Creation:
                    return new CreationState();
                case TrackedObjectKey.TrackedObjectType.History:
                    return new HistoryState() { InstanceId = key.InstanceId };
                case TrackedObjectKey.TrackedObjectType.Instance:
                    return new InstanceState() { InstanceId = key.InstanceId };
                default:
                    throw new ArgumentException("invalid key", nameof(key));
            }
        }

        public static IEnumerable<TrackedObjectKey> GetSingletons()
        {
            foreach (var t in (TrackedObjectType[]) Enum.GetValues(typeof(TrackedObjectType)))
            {
                if (IsSingletonType(t))
                {
                    yield return new TrackedObjectKey() { ObjectType = t };
                }
            }
        }

        public override string ToString()
        {
            return this.InstanceId == null ? this.ObjectType.ToString() : $"{this.ObjectType}-{this.InstanceId}";
        }

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
