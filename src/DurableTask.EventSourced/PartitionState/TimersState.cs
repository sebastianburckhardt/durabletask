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
using DurableTask.Core.Common;
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

        public override string ToString()
        {
            return $"Timers ({PendingTimers.Count} pending) next={SequenceNumber:D6}";
        }

        public override void OnRecoveryCompleted()
        {
            // restore the pending timers
            foreach (var kvp in PendingTimers)
            {
                this.Schedule(kvp.Key, kvp.Value);
            }
        }

        private void Schedule(long timerId, TaskMessage message)
        {
            TimerFired expirationEvent = new TimerFired()
            {
                PartitionId = this.Partition.PartitionId,
                TimerId = timerId,
                TaskMessage = message,
            };
            if (message.Event is TimerFiredEvent timerFiredEvent)
            {
                expirationEvent.Due = timerFiredEvent.FireAt;
            }
            else if (Entities.IsDelayedEntityMessage(message, out DateTime due))
            {
                expirationEvent.Due = due;
            }
            else
            {
                throw new ArgumentException(nameof(message), "unhandled event type");
            }

            Partition.DetailTracer?.TraceDetail($"Scheduled {message} due at {expirationEvent.Due:o}, id={expirationEvent.EventIdString}");
            Partition.PendingTimers.Schedule(expirationEvent.Due, expirationEvent);
        }

        public void Process(TimerFired evt, EffectTracker effects)
        {
            // removes the entry for the pending timer, and then adds it to the sessions queue
            this.PendingTimers.Remove(evt.TimerId);
        }

        public void Process(BatchProcessed evt, EffectTracker effects)
        {
            // starts new timers as specified by the batch
            foreach (var t in evt.TimerMessages)
            {
                var timerId = this.SequenceNumber++;
                this.PendingTimers.Add(timerId, t);

                if (!effects.IsReplaying)
                {
                    this.Schedule(timerId, t);
                }
            }
        }

        public void Process(TaskMessagesReceived evt, EffectTracker effects)
        {
            // starts new timers as specified by the batch
            foreach (var t in evt.DelayedTaskMessages)
            {
                var timerId = this.SequenceNumber++;
                this.PendingTimers.Add(timerId, t);

                if (!effects.IsReplaying)
                {
                    this.Schedule(timerId, t);
                }
            }
        }
    }
}
