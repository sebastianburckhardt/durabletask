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
using DurableTask.Core;
using DurableTask.Core.History;

namespace DurableTask.EventSourced
{
    [DataContract]
    internal class TimersState : TrackedObject
    {
        [DataMember]
        public Dictionary<long, TaskMessage> PendingTimers { get; private set; } = new Dictionary<long, TaskMessage>();

        [DataMember]
        public long SequenceNumber { get; set; }

        [IgnoreDataMember]
        public override TrackedObjectKey Key => new TrackedObjectKey(TrackedObjectKey.TrackedObjectType.Timers);

        public override void OnRecoveryCompleted()
        {
            // restore the pending timers
            foreach (var kvp in PendingTimers)
            {
                var expirationEvent = new TimerFired()
                {
                    PartitionId = this.Partition.PartitionId,
                    TimerId = kvp.Key,
                    TimerFiredMessage = kvp.Value,
                };

                Partition.TraceDetail($"Rescheduled {kvp.Value}");
                Partition.PendingTimers.Schedule(expirationEvent.TimerFiredEvent.FireAt, expirationEvent);
            }
        }

        public override string ToString()
        {
            return $"Timers ({PendingTimers.Count} pending) next={SequenceNumber:D6}";
        }


        // TimerFired
        // removes the entry for the pending timer, and then adds it to the sessions queue

        public void Process(TimerFired evt, EffectTracker effects)
        {
            PendingTimers.Remove(evt.TimerId);
        }

        // BatchProcessed
        // starts new timers as specified by the batch

        public void Process(BatchProcessed evt, EffectTracker effects)
        {
            foreach (var t in evt.TimerMessages)
            {
                var timerId = SequenceNumber++;
                PendingTimers.Add(timerId, t);

                if (!effects.InRecovery)
                {
                    var expirationEvent = new TimerFired()
                    {
                        PartitionId = this.Partition.PartitionId,
                        TimerId = timerId,
                        TimerFiredMessage = t,
                    };

                    Partition.PendingTimers.Schedule(expirationEvent.TimerFiredEvent.FireAt, expirationEvent);
                }
            }
        }
    }
}
