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
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Runtime.Serialization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DurableTask.Core;
using DurableTask.Core.Common;

namespace DurableTask.EventSourced
{
    [DataContract]
    internal class OutboxState : TrackedObject, TransportAbstraction.IAckListener
    {
        [DataMember]
        public SortedDictionary<long, Batch> Outbox { get; private set; } = new SortedDictionary<long, Batch>();

        public override TrackedObjectKey Key => new TrackedObjectKey(TrackedObjectKey.TrackedObjectType.Outbox);

        public override void OnRecoveryCompleted()
        {
            // resend all pending
            foreach (var kvp in Outbox)
            {
                // recover non-persisted fields
                kvp.Value.Position = kvp.Key;
                kvp.Value.Partition = this.Partition;

                // resend (anything we have recovered is of course persisted)
                Partition.DetailTracer?.TraceDetail($"Resent {kvp.Key:D10} ({kvp.Value} messages)");
                this.Send(kvp.Value);
            }
        }

        public override string ToString()
        {
            return $"Outbox ({Outbox.Count} pending)";
        }

        private void SendBatchOnceEventIsPersisted(PartitionEvent evt, EffectTracker effects, Batch batch)
        {
            // put the messages in the outbox where they are kept until actually sent
            var commitPosition = evt.NextCommitLogPosition;
            this.Outbox[commitPosition] = batch;
            batch.Position = commitPosition;
            batch.Partition = this.Partition;

            if (!effects.IsReplaying)
            {
                AckListeners.Register(evt, this); // we need to continue the send after this event is durable
            }
        }

        public void Acknowledge(Event evt)
        {
            this.Partition.DetailTracer?.TraceDetail($"store has persisted outbound event {evt} id={evt.EventIdString}");
            long commitPosition = evt.NextCommitLogPosition;
            this.Send(this.Outbox[commitPosition]);
        }

        private void Send(Batch batch)
        {
            // now that we know the sending event is persisted, we can send the messages
            foreach (var outmessage in batch)
            {
                AckListeners.Register(outmessage, batch);
                outmessage.OriginPartition = this.Partition.PartitionId;
                outmessage.OriginPosition = batch.Position;
                Partition.Send(outmessage);
            }
        }

        [DataContract]
        public class Batch : List<PartitionMessageReceived>, TransportAbstraction.IAckListener
        {
            [IgnoreDataMember]
            public long Position { get; set; }

            [IgnoreDataMember]
            public Partition Partition { get; set; }

            [IgnoreDataMember]
            private int numAcks = 0;

            public void Acknowledge(Event evt)
            {
                this.Partition.DetailTracer?.TraceDetail($"transport has confirmed event {evt} id={evt.EventIdString}");

                if (++numAcks == Count)
                {
                    Partition.Submit(new SendConfirmed()
                    {
                        PartitionId = this.Partition.PartitionId,
                        Position = Position,
                    });
                }
            }
        }

        public void Process(SendConfirmed evt, EffectTracker _)
        {
            // we no longer need to keep these events around
            this.Outbox.Remove(evt.Position);
        }

        public void Process(ActivityCompleted evt, EffectTracker effects)
        {
            var batch = new Batch();
            batch.Add(new RemoteActivityResultReceived()
            {
                PartitionId = evt.OriginPartitionId,
                Result = evt.Response,
                ActivityId = evt.ActivityId,
                ActivitiesQueueSize = evt.ReportedLoad,
            });
            this.SendBatchOnceEventIsPersisted(evt, effects, batch);
        }

        public void Process(BatchProcessed evt, EffectTracker effects)
        {
            var sorted = new Dictionary<uint, TaskMessagesReceived>();
            foreach (var message in evt.RemoteMessages)
            {   
                var instanceId = message.OrchestrationInstance.InstanceId;
                var destination = this.Partition.PartitionFunction(instanceId);          
                if (!sorted.TryGetValue(destination, out var outmessage))
                {
                    sorted[destination] = outmessage = new TaskMessagesReceived()
                    {
                        PartitionId = destination,
                        OriginCorrelationId = evt.CorrelationId,
                    };
                }
                if (Entities.IsDelayedEntityMessage(message, out _))
                {
                    (outmessage.DelayedTaskMessages ?? (outmessage.DelayedTaskMessages = new List<TaskMessage>())).Add(message);
                }
                else
                {
                    (outmessage.TaskMessages ?? (outmessage.TaskMessages = new List<TaskMessage>())).Add(message);
                }
            }
            var batch = new Batch();
            batch.AddRange(sorted.Values);
            this.SendBatchOnceEventIsPersisted(evt, effects, batch);
        }

        public void Process(OffloadDecision evt, EffectTracker effects)
        {
            var batch = new Batch();
            batch.Add(new ActivityOffloadReceived()
            {
                PartitionId = evt.DestinationPartitionId,
                OffloadedActivities = evt.OffloadedActivities,
                Timestamp = evt.Timestamp,
            });
            this.SendBatchOnceEventIsPersisted(evt, effects, batch);
        }
    }
}
