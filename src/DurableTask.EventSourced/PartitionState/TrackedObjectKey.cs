using System;
using System.Collections.Generic;
using System.Text;

namespace DurableTask.EventSourced
{
    internal struct TrackedObjectKey
    {
        public string Key; // we'll optimize later

        public const string ActivitiesName = "Activities";
        public const string ClientsName = "Clients";
        public const string DedupName = "Dedup";
        public const string HistoryName = "History";
        public const string InstanceName = "Instance";
        public const string OutboxName = "Outbox";
        public const string ReassemblyName = "Reassembly";
        public const string RecoveryName = "Recovery";
        public const string SessionsName = "Sessions";
        public const string TimersName = "Timers";

        // keep this alphabetic
        public static TrackedObjectKey ActivitiesKey = new TrackedObjectKey() { Key = ActivitiesName };
        public static TrackedObjectKey ClientsKey = new TrackedObjectKey() { Key = ClientsName };
        public static TrackedObjectKey DedupKey = new TrackedObjectKey() { Key = DedupName };
        public static TrackedObjectKey HistoryKey(string id) => new TrackedObjectKey() { Key = $"{HistoryName}-{id}" };
        public static TrackedObjectKey InstanceKey(string id) => new TrackedObjectKey() { Key = $"{InstanceName}-{id}" };
        public static TrackedObjectKey OutboxKey = new TrackedObjectKey() { Key = OutboxName };
        public static TrackedObjectKey ReassemblyKey = new TrackedObjectKey() { Key = ReassemblyName };
        public static TrackedObjectKey RecoveryKey = new TrackedObjectKey() { Key = RecoveryName };
        public static TrackedObjectKey SessionsKey = new TrackedObjectKey() { Key = SessionsName };
        public static TrackedObjectKey TimersKey = new TrackedObjectKey() { Key = TimersName };


        public override string ToString()
        {
            return this.Key;
        }

    }
}
