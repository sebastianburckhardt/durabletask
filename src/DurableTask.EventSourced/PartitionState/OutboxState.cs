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
    internal class OutboxState : TrackedObject
    {
        [DataMember]
        long SendCursor { get; set; } = 0;

        protected override void Restore()
        {
            this.Partition.State.StartIterator(SendCursor, this.SendBatch);
        }

        [IgnoreDataMember]
        public override TrackedObjectKey Key => new TrackedObjectKey(TrackedObjectKey.TrackedObjectType.Outbox);

        private async Task SendBatch(CancellationToken token, IList<PartitionEvent> partitionEvents)
        {
            var batch = new Batch();

            foreach(var partitionEvent in partitionEvents)
            {
                var toSend = new Dictionary<uint, TaskMessageReceived>();
             
                if (partitionEvent is BatchProcessed evt && evt.RemoteMessages != null)
                {
                    foreach (var message in evt.RemoteMessages)
                    {
                        var instanceId = message.OrchestrationInstance.InstanceId;
                        var partitionId = this.Partition.PartitionFunction(instanceId);

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

                    batch.AddRange(toSend.Values);

                    batch.Position = evt.CommitPosition;
                }
            }

            await batch.SubmitAndWait(this.Partition);

            this.Partition.Submit(new SendConfirmed()
            {
                PartitionId = this.Partition.PartitionId,
                CommitPosition = batch.Position,
            });
        }

        private class Batch : List<TaskMessageReceived>, TransportAbstraction.IAckListener
        {
            private int numAcks = 0;
            private TaskCompletionSource<object> Tcs = new TaskCompletionSource<object>();

            public long Position { get; set; }

            public void Acknowledge(Event evt)
            {
                if (--numAcks == 0)
                {
                    Tcs.TrySetResult(null);
                }
            }

            public Task SubmitAndWait(TransportAbstraction.IPartition partition)
            {
                foreach(var evt in this)
                {
                    evt.AckListener = this;
                }
                
                partition.SubmitRange(this);

                return this.Tcs.Task;
            }
        }

        // SendConfirmed
        // indicates that the event was reliably sent, so we can advance the send cursor

        public void Process(SendConfirmed evt, EffectList effect)
        {
            this.SendCursor = evt.CommitPosition;
        }
    }
}
