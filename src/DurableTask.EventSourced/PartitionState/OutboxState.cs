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
            // TODO: We might need to resend persistence confirmations to all partitions if we recover 
            //       (and also clean up the WaitingForConfirmation based on that)

            // resend all pending
            foreach (var kvp in Outbox)
            {
                // recover non-persisted fields
                kvp.Value.Position = kvp.Key;
                kvp.Value.Partition = this.Partition;

                // resend (anything we have recovered is of course persisted)
                Partition.EventDetailTracer?.TraceEventProcessingDetail($"Resent {kvp.Key:D10} ({kvp.Value} messages)");
                this.Send(kvp.Value);
            }
        }

        public override void UpdateLoadInfo(PartitionLoadInfo info)
        {
            info.Outbox = this.Outbox.Count;
        }

        public override string ToString()
        {
            return $"Outbox ({Outbox.Count} pending)";
        }

        private void SendBatchAndSetupConfirmation(PartitionUpdateEvent evt, EffectTracker effects, Batch batch)
        {
            // Keep the event with the batch so that we can confirm it in the snapshot
            // TODO: There should be a better way to do this.
            batch.Event = evt;
            // Put the messages in the outbox to be able to send a confirmation afterwards.
            // TODO: We might actually not need to store the batch here
            var commitPosition = evt.NextCommitLogPosition;
            this.Outbox[commitPosition] = batch;
            batch.Position = commitPosition;
            batch.Partition = this.Partition;

            // If we are replaying we don't need to resend the messages 
            // since they will be replayed in the other partitions from their commit logs
            // Q: Is that assumption correct?

            if (!effects.IsReplaying)
            {
                this.Send(this.Outbox[commitPosition]);
                DurabilityListeners.Register(evt, this); // we need to send a persistence confirmation after this event is durable
            }
        }

        //private void SendBatchOnceEventIsPersisted(PartitionUpdateEvent evt, EffectTracker effects, Batch batch)
        //{
        //    // put the messages in the outbox where they are kept until actually sent
        //    var commitPosition = evt.NextCommitLogPosition;
        //    this.Outbox[commitPosition] = batch;
        //    batch.Position = commitPosition;
        //    batch.Partition = this.Partition;

        //    if (!effects.IsReplaying)
        //    {
        //        DurabilityListeners.Register(evt, this); // we need to continue the send after this event is durable
        //    }
        //}

        public void ConfirmDurable(Event evt)
        {
            var commitPosition = ((PartitionUpdateEvent)evt).NextCommitLogPosition;
            var destinationPartitionIds = WaitingForConfirmation[commitPosition];
            this.Partition.EventDetailTracer?.TraceEventProcessingDetail($"Log has persisted event {evt} id={evt.EventIdString}, now sending messages");

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

        //public void OldConfirmDurable(Event evt)
        //{
        //    long commitPosition = ((PartitionUpdateEvent)evt).NextCommitLogPosition;
        //    this.Partition.EventDetailTracer?.TraceEventProcessingDetail($"Store has persisted event {evt} id={evt.EventIdString}, now sending messages");
        //    this.Send(this.Outbox[commitPosition]);
        //}

        private void Send(Batch batch)
        {
            // Gather all destination partitions in a list
            var destinationPartitionIds = new HashSet<uint>();

            // now that we know the sending event is persisted, we can send the messages
            foreach (var outmessage in batch)
            {
                DurabilityListeners.Register(outmessage, batch);
                outmessage.OriginPartition = this.Partition.PartitionId;
                outmessage.OriginPosition = batch.Position;
                destinationPartitionIds.Add(outmessage.PartitionId);
                Partition.Send(outmessage);
            }
            // Get the identifier of the update event that caused this batch to be sent
            var nextCommitLogAddress = batch.Event.NextCommitLogPosition;
            WaitingForConfirmation.Add(nextCommitLogAddress, destinationPartitionIds);

        }

        [DataContract]
        public class Batch : List<PartitionMessageEvent>, TransportAbstraction.IDurabilityListener
        {
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

                if (++numAcks == Count)
                {
                    Partition.SubmitInternalEvent(new SendConfirmed()
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
            batch.Add(new RemoteActivityResultReceived()
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
                else
                {
                    (outmessage.TaskMessages ?? (outmessage.TaskMessages = new List<TaskMessage>())).Add(message);
                }
            }
            var batch = new Batch();
            batch.AddRange(sorted.Values);
            this.SendBatchAndSetupConfirmation(evt, effects, batch);
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
            this.SendBatchAndSetupConfirmation(evt, effects, batch);
        }
    }
}
