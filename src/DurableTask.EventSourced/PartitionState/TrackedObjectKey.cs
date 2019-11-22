using Dynamitey;
using FASTER.core;
using Microsoft.WindowsAzure.Storage.Blob.Protocol;
using System;
using System.Collections.Generic;
using System.Text;

namespace DurableTask.EventSourced
{
    internal struct TrackedObjectKey : IFasterEqualityComparer<TrackedObjectKey>
    {
        public TrackedObjectType ObjectType;
        public string InstanceId;

        public enum TrackedObjectType
        {
            Activities,
            Clients,
            Dedup,
            History,
            Instance,
            Outbox,
            Reassembly,
            Recovery,
            Sessions,
            Timers
        }

        public static bool IsSingletonType(TrackedObjectType t) => t != TrackedObjectType.Instance && t != TrackedObjectType.History;

        // singleton objects

        public static TrackedObjectKey Activities = new TrackedObjectKey() { ObjectType = TrackedObjectType.Activities };
        public static TrackedObjectKey Clients = new TrackedObjectKey() { ObjectType = TrackedObjectType.Clients };
        public static TrackedObjectKey Dedup = new TrackedObjectKey() { ObjectType = TrackedObjectType.Dedup };
        public static TrackedObjectKey Outbox = new TrackedObjectKey() { ObjectType = TrackedObjectType.Outbox };
        public static TrackedObjectKey Reassembly = new TrackedObjectKey() { ObjectType = TrackedObjectType.Reassembly };
        public static TrackedObjectKey Recovery = new TrackedObjectKey() { ObjectType = TrackedObjectType.Recovery };
        public static TrackedObjectKey Sessions = new TrackedObjectKey() { ObjectType = TrackedObjectType.Sessions };
        public static TrackedObjectKey Timers = new TrackedObjectKey() { ObjectType = TrackedObjectType.Timers };

        // per-instance objects

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

        public static TrackedObject TrackedObjectFactory(TrackedObjectKey key)
        {
            switch (key.ObjectType)
            {
                case TrackedObjectKey.TrackedObjectType.Activities:
                    return new ActivitiesState() { Key = key };
                case TrackedObjectKey.TrackedObjectType.Clients:
                    return new ClientsState() { Key = key };
                case TrackedObjectKey.TrackedObjectType.Dedup:
                    return new DedupState() { Key = key };
                case TrackedObjectKey.TrackedObjectType.Outbox:
                    return new OutboxState() { Key = key };
                case TrackedObjectKey.TrackedObjectType.Reassembly:
                    return new ReassemblyState() { Key = key };
                case TrackedObjectKey.TrackedObjectType.Recovery:
                    return new RecoveryState() { Key = key };
                case TrackedObjectKey.TrackedObjectType.Sessions:
                    return new SessionsState() { Key = key };
                case TrackedObjectKey.TrackedObjectType.Timers:
                    return new TimersState() { Key = key };
                case TrackedObjectKey.TrackedObjectType.History:
                    return new HistoryState() { Key = key };
                case TrackedObjectKey.TrackedObjectType.Instance:
                    return new InstanceState() { Key = key };
                default:
                    throw new ArgumentException("invalid key", nameof(key));
            }
        }

        public override string ToString()
        {
            return this.InstanceId == null ? this.ObjectType.ToString() : $"{this.ObjectType}-{this.InstanceId}";
        }

        public long GetHashCode64(ref TrackedObjectKey k)
        {
            unchecked
            {
                // Compute an FNV hash
                var hash = 0xcbf29ce484222325ul; // FNV_offset_basis
                var prime = 0x100000001b3ul; // FNV_prime

                // hash the kind
                hash ^= (byte)ObjectType;
                hash *= prime;

                // hash the instance id, if applicable
                if (InstanceId != null)
                {
                    for (int i = 0; i < InstanceId.Length; i++)
                    {
                        hash ^= InstanceId[i];
                        hash *= prime;
                    }
                }

                return (long)hash;
            }
        }

        public bool Equals(ref TrackedObjectKey k1, ref TrackedObjectKey k2)
        {
            return k1.ObjectType == k2.ObjectType && k1.InstanceId == k2.InstanceId;
        }

        public class TrackedObjectKeySerializer : BinaryObjectSerializer<TrackedObjectKey>
        {
            public override void Deserialize(ref TrackedObjectKey obj)
            {
                obj.ObjectType = (TrackedObjectType)reader.ReadByte();
                if (!IsSingletonType(obj.ObjectType))
                {
                    obj.InstanceId = reader.ReadString();
                }
            }

            public override void Serialize(ref TrackedObjectKey obj)
            {
                writer.Write((byte) obj.ObjectType);
                if (!IsSingletonType(obj.ObjectType))
                {
                    writer.Write(obj.InstanceId);
                }
            }
        }
    }
}
