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
    internal class OutboxState : TrackedObject, TransportAbstraction.IAckListener
    {
        [DataMember]
        public SortedDictionary<long, List<TaskMessage>> Outbox { get; private set; } = new SortedDictionary<long, List<TaskMessage>>();

        public override TrackedObjectKey Key => new TrackedObjectKey(TrackedObjectKey.TrackedObjectType.Outbox);

        protected override void Restore()
        {
            // resend all pending
            foreach (var kvp in Outbox)
            {
                Send(kvp.Key, kvp.Value);
            }
        }

        private void Send(long position, List<TaskMessage> messages)
        {
            var batch = new Batch() { Position = position, Partition = this.Partition } ;
            
            foreach (var message in messages)
            {
                var instanceId = message.OrchestrationInstance.InstanceId;
                var partitionId = this.Partition.PartitionFunction(instanceId);

                if (!batch.TryGetValue(partitionId, out var outmessage))
                {
                    batch[partitionId] = outmessage = new TaskMessageReceived()
                    {
                        PartitionId = partitionId,
                        OriginPartition = this.Partition.PartitionId,
                        OriginPosition = position,
                        TaskMessages = new List<TaskMessage>(),
                        AckListener = batch,
                    };
                }
                outmessage.TaskMessages.Add(message);
            }

            foreach (var outmessage in batch.Values)
            {
                Partition.Send(outmessage);
            }
        }

        private class Batch : Dictionary<uint, TaskMessageReceived>, TransportAbstraction.IAckListener
        {
            public long Position { get; set; }
            public Partition Partition { get; set; }

            private int numAcks = 0;

            public void Acknowledge(Event evt)
            {
                if (++numAcks == this.Count)
                {
                    Partition.Submit(new SendConfirmed()
                    {
                        PartitionId = this.Partition.PartitionId,
                        Position = Position,
                    });
                }
            }
        }


        // BatchProcessed

        public void Process(BatchProcessed evt, EffectList effects)
        {
            this.Outbox[evt.CommitPosition] = evt.RemoteMessages;
            evt.AckListener = this; // we need to notified when this event is durable
        }

        public void Acknowledge(Event evt)
        {
            // now that the event is durable we can send the messages
            var batchProcessedEvent = (BatchProcessed)evt;
            this.Send(batchProcessedEvent.CommitPosition, batchProcessedEvent.RemoteMessages);
        }

        // SendConfirmed

        public void Process(SendConfirmed evt, EffectList effects)
        {
            // we no longer need to keep these events around
            this.Outbox.Remove(evt.Position);
        }
    }
}
