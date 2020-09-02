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
using DurableTask.Core.History;
using DurableTask.EventSourced.Scaling;

namespace DurableTask.EventSourced
{
    [DataContract]
    internal class OutboxState : TrackedObject, TransportAbstraction.IDurabilityListener
    {
        [DataMember]
        public SortedDictionary<long, Batch> Outbox { get; private set; } = new SortedDictionary<long, Batch>();

        // Contains the partition identifiers that we need to inform when events have been persisted
        [DataMember]
        public SortedDictionary<long, HashSet<uint>> WaitingForConfirmation { get; private set; } = new SortedDictionary<long, HashSet<uint>>();

        public override TrackedObjectKey Key => new TrackedObjectKey(TrackedObjectKey.TrackedObjectType.Outbox);

        public override void OnRecoveryCompleted()
        {
            // resend all pending
            foreach (var kvp in this.Outbox)
            {
                // recover non-persisted fields
                kvp.Value.Position = kvp.Key;
                kvp.Value.Partition = this.Partition;

                // resend (anything we have recovered is of course persisted)
                Partition.EventDetailTracer?.TraceEventProcessingDetail($"Resent {kvp.Key:D10} ({kvp.Value} messages)");
                this.Send(kvp.Value);

                // resend the persistence confirmation events (since ConfirmDurable won't be called)
                // TODO: Make sure that it is safe to remove this now that we send PersistenceConfirmation events
                //       at the end of recovery in FasterStorage
                SendPersistenceConfirmation(kvp.Key);
            }
        }

        public override void UpdateLoadInfo(PartitionLoadInfo info)
        {
            info.Outbox = this.Outbox.Count;
        }

        public override string ToString()
        {
            return $"Outbox ({this.Outbox.Count} pending)";
        }

        private void SendBatchAndSetupConfirmation(PartitionUpdateEvent evt, EffectTracker effects, Batch batch)
        {
            // Put the messages in the outbox to be able to send a confirmation afterwards.
            var commitPosition = evt.NextCommitLogPosition;
            this.Outbox[commitPosition] = batch;
            batch.Position = commitPosition;
            batch.Partition = this.Partition;

            if (!effects.IsReplaying)
            {
                this.Send(this.Outbox[commitPosition]);
                DurabilityListeners.Register(evt, this); // we need to send a persistence confirmation after this event is durable
            }
        }


        private void SendPersistenceConfirmation(long commitPosition)
        {
            var destinationPartitionIds = WaitingForConfirmation[commitPosition];
            foreach (var destinationPartitionId in destinationPartitionIds)
            {
                var persistenceConfirmationEvent = new PersistenceConfirmationEvent
                {
                    PartitionId = destinationPartitionId,
                    OriginPartition = this.Partition.PartitionId,
                    OriginPosition = commitPosition

                };

                this.Partition.Send(persistenceConfirmationEvent);
            }
            WaitingForConfirmation.Remove(commitPosition);
        }

        public void ConfirmDurable(Event evt)
        {
            this.Partition.EventDetailTracer?.TraceEventProcessingDetail($"Log has persisted event {evt} id={evt.EventIdString}, now sending confirmation messages");
            SendPersistenceConfirmation(((PartitionUpdateEvent)evt).NextCommitLogPosition);
        }

        private void Send(Batch batch)
        {
            // Gather all destination partitions in a list
            var destinationPartitionIds = new HashSet<uint>();

            // now that we know the sending event is persisted, we can send the messages
            foreach (var outmessage in batch.OutgoingMessages)
            {
                DurabilityListeners.Register(outmessage, batch);
                outmessage.OriginPartition = this.Partition.PartitionId;
                outmessage.OriginPosition = batch.Position;
                destinationPartitionIds.Add(outmessage.PartitionId);
                outmessage.SentTimestampUnixMs = DateTimeOffset.Now.ToUnixTimeMilliseconds();
                Partition.Send(outmessage);
            }
            // Get the identifier of the update event that caused this batch to be sent
            var nextCommitLogAddress = batch.Position;
            WaitingForConfirmation.Add(nextCommitLogAddress, destinationPartitionIds);

        }

        [DataContract]
        public class Batch : TransportAbstraction.IDurabilityListener
        {
            [DataMember]
            public List<PartitionMessageEvent> OutgoingMessages { get; set; } = new List<PartitionMessageEvent>();

            [IgnoreDataMember]
            public long Position { get; set; }

            [IgnoreDataMember]
            public Partition Partition { get; set; }

            [IgnoreDataMember]
            private int numAcks = 0;

            // Q: Maybe we should be able to serialize this? Is this necessary? Probably not
            [IgnoreDataMember]
            public PartitionUpdateEvent Event { get; set; }

            public void ConfirmDurable(Event evt)
            {
                this.Partition.EventDetailTracer?.TraceEventProcessingDetail($"Transport has confirmed event {evt} id={evt.EventIdString}");

                if (++numAcks == this.OutgoingMessages.Count)
                {
                    this.Partition.SubmitInternalEvent(new SendConfirmed()
                    {
                        PartitionId = this.Partition.PartitionId,
                        Position = Position,
                    });
                }
            }
        }

        public void Process(SendConfirmed evt, EffectTracker _)
        {
            this.Partition.EventDetailTracer?.TraceEventProcessingDetail($"Store has sent all outbound messages by event {evt} id={evt.EventIdString}");

            // we no longer need to keep these events around
            this.Outbox.Remove(evt.Position);
        }

        public void Process(ActivityCompleted evt, EffectTracker effects)
        {
            var batch = new Batch();
            batch.OutgoingMessages.Add(new RemoteActivityResultReceived()
            {
                PartitionId = evt.OriginPartitionId,
                Result = evt.Response,
                ActivityId = evt.ActivityId,
                ActivitiesQueueSize = evt.ReportedLoad,
            });
            this.SendBatchAndSetupConfirmation(evt, effects, batch);
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
                        WorkItemId = evt.WorkItemId,
                    };
                }
                if (Entities.IsDelayedEntityMessage(message, out _))
                {
                    (outmessage.DelayedTaskMessages ?? (outmessage.DelayedTaskMessages = new List<TaskMessage>())).Add(message);
                }
                else if (message.Event is ExecutionStartedEvent executionStartedEvent && executionStartedEvent.ScheduledStartTime.HasValue)
                {
                    (outmessage.DelayedTaskMessages ?? (outmessage.DelayedTaskMessages = new List<TaskMessage>())).Add(message);
                }
                else
                {
                    (outmessage.TaskMessages ?? (outmessage.TaskMessages = new List<TaskMessage>())).Add(message);
                }
            }
            var batch = new Batch();
            batch.OutgoingMessages.AddRange(sorted.Values);
            this.SendBatchAndSetupConfirmation(evt, effects, batch);
        }

        public void Process(OffloadDecision evt, EffectTracker effects)
        {
            var batch = new Batch();
            batch.OutgoingMessages.Add(new ActivityOffloadReceived()
            {
                PartitionId = evt.DestinationPartitionId,
                OffloadedActivities = evt.OffloadedActivities,
                Timestamp = evt.Timestamp,
            });
            this.SendBatchAndSetupConfirmation(evt, effects, batch);
        }
    }
}
