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
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using DurableTask.Core;

namespace DurableTask.EventHubs
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

        [IgnoreDataMember]
        private readonly List<PartitionEvent> outlist = new List<PartitionEvent>();

        public long GetLastAckedQueuePosition() { return LastAckedQueuePosition; }

        protected override void Restore()
        {
            foreach(var kvp in Outbox)
            {
                if (kvp.Key > this.LastAckedQueuePosition)
                {
                    this.Send(kvp.Key, kvp.Value);
                }
            }
        }

        public void Apply(BatchProcessed evt)
        {
            Outbox.Add(evt.QueuePosition, evt.RemoteOrchestratorMessages);

            this.Send(evt.QueuePosition, evt.RemoteOrchestratorMessages);
        }

        private void Send(long queuePosition, List<TaskMessage> messages)
        {
            foreach (var message in messages)
            {
                outlist.Add(new TaskMessageReceived()
                {
                    TaskMessage = message,
                });
            }

            outlist.Add(new OutgoingMessagesAcked()
            {
                LastAckedQueuePosition = queuePosition, 
            });

            LocalPartition.BatchSender.AddRange(outlist);

            outlist.Clear();
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
    }
}
