using System;
using System.Collections.Generic;
using System.Text;

namespace DurableTask.EventSourced.PartitionState
{
    internal static class TrackedObjectFactory
    {
        public static TrackedObject Create(TrackedObjectKey key)
        {
            switch (key.Key)
            {
                case TrackedObjectKey.ActivitiesName:
                    return new ActivitiesState();
                case TrackedObjectKey.ClientsName:
                    return new ClientsState();
                case TrackedObjectKey.DedupName:
                    return new DedupState();
                case TrackedObjectKey.OutboxName:
                    return new OutboxState();
                case TrackedObjectKey.ReassemblyName:
                    return new ReassemblyState();
                case TrackedObjectKey.RecoveryName:
                    return new RecoveryState();
                case TrackedObjectKey.SessionsName:
                    return new SessionsState();
                case TrackedObjectKey.TimersName:
                    return new TimersState();

                default:
                    {
                        if (key.Key.StartsWith(TrackedObjectKey.HistoryName))
                        {
                            return new HistoryState() { InstanceId = key.Key.Substring(TrackedObjectKey.HistoryName.Length + 1) };
                        }
                        else if (key.Key.StartsWith(TrackedObjectKey.InstanceName))
                        {
                            return new InstanceState() { InstanceId = key.Key.Substring(TrackedObjectKey.InstanceName.Length + 1) };
                        }
                        else
                        {
                            throw new ArgumentException("not a valid key", nameof(key));
                        }
                    }
            }
        }
    }
}
