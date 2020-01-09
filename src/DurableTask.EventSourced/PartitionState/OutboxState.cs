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
using System.Threading;
using System.Threading.Tasks;
using DurableTask.Core;

namespace DurableTask.EventSourced
{
    [DataContract]
    internal class OutboxState : TrackedObject, Backend.IAckListener
    {
        [DataMember]
        public Dictionary<uint, List<TaskMessageReceived>> Outbox { get; private set; } = new Dictionary<uint, List<TaskMessageReceived>>();

        [IgnoreDataMember]
        public long LastKnownPersistedPosition { get; set; } = -1;

        [IgnoreDataMember]
        public override TrackedObjectKey Key => new TrackedObjectKey(TrackedObjectKey.TrackedObjectType.Outbox);

        protected override void Restore()
        {
            long lastPersistedPosition = -1;
            foreach(var kvp in this.Outbox)
            {
                if (kvp.Value.Count > 0)
                {
                    lastPersistedPosition = Math.Max(lastPersistedPosition, kvp.Value.Last().OriginPosition);
                }
            }

            // submit to self so the LastKnownPersistedPosition gets updated and messages are resent
            if (lastPersistedPosition > -1)
            {
                this.Partition.Submit(new SentOrPersisted()
                {
                    PartitionId = this.Partition.PartitionId,
                    DurablyPersistedPosition = lastPersistedPosition,
                });
            }
        }

        private void Send(long queuePosition, List<TaskMessage> messages)
        {
            var toSend = new Dictionary<uint, TaskMessageReceived>();

            foreach (var message in messages)
            {
                var instanceId = message.OrchestrationInstance.InstanceId;
                var partitionId = this.Partition.PartitionFunction(instanceId);

                if (!toSend.TryGetValue(partitionId, out var outmessage))
                {
                    toSend[partitionId] = outmessage = new TaskMessageReceived()
                    {
                        PartitionId = partitionId,
                        OriginPartition = this.Partition.PartitionId,
                        OriginPosition = queuePosition,
                        TaskMessages = new List<TaskMessage>(),
                    };
                }
                outmessage.TaskMessages.Add(message);
            }

            foreach (var outmessage in toSend.Values)
            {
                outmessage.AckListener = this;
                Partition.Send(outmessage);
            }
        }


        // BatchProcessed

        public void Apply(BatchProcessed evt)
        {
            var toSend = new Dictionary<uint, TaskMessageReceived>();

            foreach (var message in evt.OrchestratorMessages)
            {
                var instanceId = message.OrchestrationInstance.InstanceId;
                var partitionId = this.Partition.PartitionFunction(instanceId);

                if (partitionId == this.Partition.PartitionId)
                {
                    continue; // message is submitted to this partition directly, not sent via outbox
                }

                if (!toSend.TryGetValue(partitionId, out var outmessage))
                {
                    toSend[partitionId] = outmessage = new TaskMessageReceived()
                    {
                        PartitionId = partitionId,
                        OriginPartition = this.Partition.PartitionId,
                        OriginPosition = evt.CommitPosition,
                        TaskMessages = new List<TaskMessage>(),
                    };
                }
                outmessage.TaskMessages.Add(message);
            }

            foreach (var kvp in toSend)
            {
                if (!this.Outbox.TryGetValue(kvp.Key, out var list))
                {
                    this.Outbox.Add(kvp.Key, list = new List<TaskMessageReceived>());
                }

                list.Add(kvp.Value);
            }

            evt.AckListener = this;
        }

        public void Acknowledge(Event evt)
        {
            if (evt is TaskMessageReceived taskMessageReceived)
            {
                this.Partition.Submit(new SentOrPersisted()
                {
                    PartitionId = this.Partition.PartitionId,
                    DurablySentMessages = (taskMessageReceived.PartitionId, taskMessageReceived.OriginPosition)
                });
            }
            else if (evt is BatchProcessed batchProcessed)
            {
                this.Partition.Submit(new SentOrPersisted()
                {
                    PartitionId = this.Partition.PartitionId,
                    DurablyPersistedPosition = batchProcessed.CommitPosition,
                });
            }
        }


        // SentOrPersisted

        public void Process(SentOrPersisted evt, EffectTracker effect)
        {
            effect.ApplyTo(this.Key);
        }

        public void Apply(SentOrPersisted evt)
        {
            // send messages whose batch has been persisted
            if (evt.DurablyPersistedPosition.HasValue)
            {
                foreach (var kvp in this.Outbox)
                {
                    foreach (var message in kvp.Value)
                    {
                        if (message.OriginPosition <= this.LastKnownPersistedPosition)
                        {
                            continue;
                        }
                        if (message.OriginPosition > evt.DurablyPersistedPosition.Value)
                        {
                            break;
                        }

                        message.AckListener = this;
                        Partition.Send(message);
                    }
                }

                this.LastKnownPersistedPosition = evt.DurablyPersistedPosition.Value;
            }

            // remove messages whose sending has been confirmed
            if (evt.DurablySentMessages.HasValue)
            {
                var (destination, sentPosition) = evt.DurablySentMessages.Value;
            
                if (this.Outbox.TryGetValue(destination, out var messages))
                {
                    int count = 0;

                    while (count < messages.Count && messages[count].OriginPosition <= sentPosition)
                    {
                        count++;
                    }

                    if (count > 0)
                    {
                        messages.RemoveRange(0, count);
                    }
                }
            }
        }
    }
}
