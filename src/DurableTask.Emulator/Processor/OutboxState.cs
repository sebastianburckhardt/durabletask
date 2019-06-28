using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using DurableTask.Core;

namespace DurableTask.Emulator
{
    [DataContract]
    internal class OutboxState : TrackedObject
    {
        [DataMember]
        public List<ProcessorEvent> Outbox { get; set; }

        [DataMember]
        public long LastAckedQueuePosition { get; set; } = -1;

        public bool Apply(BatchProcessed evt)
        {
            if (!AlreadyApplied(evt))
            {
                foreach (var msg in evt.OrchestratorMessages)
                {
                    if (!LocalPartition.Handles(msg))
                    {
                        Outbox.Add(new TaskMessageReceived()
                        {
                            TaskMessage = msg,
                            QueuePosition = evt.QueuePosition
                        });
                    }
                }
            }

            return true;
        }
         
    }
}
