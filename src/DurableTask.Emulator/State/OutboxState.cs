using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using DurableTask.Core;

namespace DurableTask.Emulator
{
    [DataContract]
    internal class OutboxState : TrackedObject
    {
        [DataMember]
        public SortedList<long, List<TaskMessage>> Outbox { get; private set; } = new SortedList<long, List<TaskMessage>>();

        [DataMember]
        public long LastAckedQueuePosition { get; set; } = -1;

        [IgnoreDataMember]
        public override string Key => "@@outbox";

        public long GetLastAckedQueuePosition() { return LastAckedQueuePosition; }

        public Batch? TryGetBatch(long sendPosition)
        {
            if (Outbox.Count == 0 || Outbox.Last().Key < sendPosition)
            {
                return null;
            }
            else
            {
                var batch = new Batch()
                {
                    Messages = new List<PartitionEvent>(),
                };

                foreach (var kvp in this.Outbox)
                {
                    if (kvp.Key < sendPosition) continue;

                    foreach (var message in kvp.Value)
                    {
                        batch.Messages.Add(new TaskMessageReceived()
                        {
                            // TODO vector clock
                            TaskMessage = message
                        });
                    }

                    batch.LastQueuePosition = kvp.Key;
                }

                return batch;
            }
        }

        public void Apply(BatchProcessed evt)
        {
            Outbox.Add(evt.QueuePosition, evt.RemoteOrchestratorMessages);

            LocalPartition.BatchSender.Notify();
        }

        public void Scope(OutgoingMessagesAcked evt, List<TrackedObject> scope, List<TrackedObject> apply)
        {
            if (Outbox.Count > 0 && Outbox.First().Key < evt.LastAckedQueuePosition)
            {
                apply.Add(this);
            }
        }

        public void Apply(OutgoingMessagesAcked evt)
        {
            while (Outbox.Count > 0)
            {
                var first = Outbox.First();

                if (first.Key < evt.LastAckedQueuePosition)
                {
                    Outbox.Remove(first.Key);
                }
            }

            LastAckedQueuePosition = evt.LastAckedQueuePosition;
        }

        public struct Batch
        {
            public List<PartitionEvent> Messages;

            public long LastQueuePosition;
        }

    }
}
