using Dynamitey;
using FASTER.core;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace DurableTask.EventSourced
{
    internal struct TrackedObjectKey
    {
        public TrackedObjectType ObjectType;
        public string InstanceId;

        public TrackedObjectKey(TrackedObjectType objectType) { this.ObjectType = objectType; this.InstanceId = null; }
        public TrackedObjectKey(TrackedObjectType objectType, string instanceId) { this.ObjectType = objectType; this.InstanceId = instanceId; }

        public enum TrackedObjectType
        {
            Activities,
            Clients,
            Dedup,
            History,
            Instance,
            Outbox,
            Reassembly,
            Sessions,
            Timers
        }

        public static Dictionary<TrackedObjectType, Type> TypeMap = new Dictionary<TrackedObjectType, Type>()
        {
            { TrackedObjectType.Activities, typeof(ActivitiesState) },
            { TrackedObjectType.Clients, typeof(ClientsState) },
            { TrackedObjectType.Dedup, typeof(DedupState) },
            { TrackedObjectType.History, typeof(HistoryState) },
            { TrackedObjectType.Instance, typeof(InstanceState) },
            { TrackedObjectType.Outbox, typeof(OutboxState) },
            { TrackedObjectType.Reassembly, typeof(ReassemblyState) },
            { TrackedObjectType.Sessions, typeof(SessionsState) },
            { TrackedObjectType.Timers, typeof(TimersState) },
        };

        public static bool IsSingletonType(TrackedObjectType t) => 
            t != TrackedObjectType.Instance && t != TrackedObjectType.History;

        // convenient constructors for singletons

        public static TrackedObjectKey Activities = new TrackedObjectKey() { ObjectType = TrackedObjectType.Activities };
        public static TrackedObjectKey Clients = new TrackedObjectKey() { ObjectType = TrackedObjectType.Clients };
        public static TrackedObjectKey Dedup = new TrackedObjectKey() { ObjectType = TrackedObjectType.Dedup };
        public static TrackedObjectKey Outbox = new TrackedObjectKey() { ObjectType = TrackedObjectType.Outbox };
        public static TrackedObjectKey Reassembly = new TrackedObjectKey() { ObjectType = TrackedObjectType.Reassembly };
        public static TrackedObjectKey Sessions = new TrackedObjectKey() { ObjectType = TrackedObjectType.Sessions };
        public static TrackedObjectKey Timers = new TrackedObjectKey() { ObjectType = TrackedObjectType.Timers };

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
                case TrackedObjectKey.TrackedObjectType.Clients:
                    return new ClientsState();
                case TrackedObjectKey.TrackedObjectType.Dedup:
                    return new DedupState();
                case TrackedObjectKey.TrackedObjectType.Outbox:
                    return new OutboxState();
                case TrackedObjectKey.TrackedObjectType.Reassembly:
                    return new ReassemblyState();
                case TrackedObjectKey.TrackedObjectType.Sessions:
                    return new SessionsState();
                case TrackedObjectKey.TrackedObjectType.Timers:
                    return new TimersState();
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
