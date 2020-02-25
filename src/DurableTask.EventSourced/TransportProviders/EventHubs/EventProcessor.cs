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
using System.IO;
using System.Linq;
using System.Net.Mime;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DurableTask.EventSourced.Emulated;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;
using Microsoft.Extensions.Logging;

namespace DurableTask.EventSourced.EventHubs
{
    internal class EventProcessor : IEventProcessor, TransportAbstraction.IAckListener
    {
        private readonly TransportAbstraction.IHost host;
        private readonly TransportAbstraction.ISender sender;
        private readonly Guid processorId;
        private readonly EventSourcedOrchestrationServiceSettings settings;
        private readonly EventHubsTransport.TaskhubParameters parameters;
        private readonly ILogger logger;

        private StorageAbstraction.IPartitionState partitionState;
        private TransportAbstraction.IPartition partition;

        private Stopwatch timeSinceLastCheckpoint = new Stopwatch();
        private Checkpoint pendingCheckpoint;
        private Checkpoint completedCheckpoint;
        private ulong inputQueuePosition;

        private Dictionary<string, MemoryStream> reassembly = new Dictionary<string, MemoryStream>();

        public EventProcessor(
            TransportAbstraction.IHost host, 
            TransportAbstraction.ISender sender, 
            EventSourcedOrchestrationServiceSettings settings,
            EventHubsTransport.TaskhubParameters parameters)
        {
            this.host = host;
            this.logger = host.TransportLogger;
            this.sender = sender;
            this.processorId = Guid.NewGuid();
            this.settings = settings;
            this.parameters = parameters;
        }

        async Task IEventProcessor.OpenAsync(PartitionContext context)
        {
            this.logger.LogInformation("Starting EventProcessor for partition {partition}", context.PartitionId);

            uint partitionId = uint.Parse(context.PartitionId);
            this.partitionState = this.host.StorageProvider.CreatePartitionState();
            this.partition = host.AddPartition(partitionId, this.partitionState, this.sender);
            
            this.inputQueuePosition = await this.partition.StartAsync(CancellationToken.None);
            if (this.inputQueuePosition == 0)
            {
                // this taskhub started with a queue already having some start position, so adjust it accordingly
                this.inputQueuePosition = (ulong) this.parameters.StartPositions[partitionId];
            }
 
            timeSinceLastCheckpoint.Start();
        }

        async Task IEventProcessor.CloseAsync(PartitionContext context, CloseReason reason)
        {
            this.logger.LogInformation("Stopping EventProcessor for partition {partition}", context.PartitionId);

            try
            {
                await this.partition.StopAsync();

                if (this.completedCheckpoint != null
                    && !this.partitionState.OwnershipCancellationToken.IsCancellationRequested)
                {
                    await context.CheckpointAsync(this.completedCheckpoint);
                    completedCheckpoint = null;
                }

                this.logger.LogInformation("Successfully stopped EventProcessor for partition {partition}", context.PartitionId);
            }
            catch (Exception e)
            {
                this.logger.LogError(e, "Error while shutting down EventHubs partition {partition}", context.PartitionId);

                throw;
            }
        }

        Task IEventProcessor.ProcessErrorAsync(PartitionContext context, Exception error)
        {
            this.logger.LogError(error, "Error in EventProcessor for partition {partition}", context.PartitionId);

            return Task.FromResult<object>(null);
        }

        Task IEventProcessor.ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
        {
            this.partitionState.OwnershipCancellationToken.ThrowIfCancellationRequested();

            try
            {
                var batch = new List<PartitionEvent>();

                foreach (var eventData in messages)
                {
                    var seqno = (ulong)eventData.SystemProperties.SequenceNumber;
                    if (seqno >= this.inputQueuePosition)
                    {
                        var evt = (PartitionEvent)Serializer.DeserializeEvent(eventData.Body);
                        evt.Serialized = eventData.Body; // we'll reuse this for writing to the event log
                        this.inputQueuePosition = seqno + 1;
                        evt.InputQueuePosition = this.inputQueuePosition;
                        batch.Add(evt);

                        this.CheckpointAfterThisEventIfAppropriate(context.PartitionId, evt, eventData);
                    }
                }

                this.partitionState.OwnershipCancellationToken.ThrowIfCancellationRequested();

                if (batch.Count > 0)
                {
                    partition.SubmitRange(batch);
                }

                if (this.completedCheckpoint != null)
                {
                    var checkpoint = this.completedCheckpoint;
                    this.completedCheckpoint = null;
                    this.logger.LogInformation("EventProcessor for partition {partition} is checkpointing input position {position}", int.Parse(context.PartitionId), checkpoint.SequenceNumber + 1);
                    return context.CheckpointAsync(checkpoint);
                }

                return Task.CompletedTask;
            }
            catch (Exception e)
            {
                this.logger.LogError(e, "Error while processing events in EventProcessor for partition {partition}", context.PartitionId);
                throw;
            }
        }

        private void CheckpointAfterThisEventIfAppropriate(string partitionId, PartitionEvent evt, EventData eventData)
        {
            if (this.pendingCheckpoint != null || this.completedCheckpoint != null)
            {
                return; // there is a checkpoint in progress already
            }

            if (this.timeSinceLastCheckpoint.ElapsedMilliseconds < 30000)
            {
                return; // it has not been long enough since last checkpoint
            }

            if (!(evt is IPartitionEventWithSideEffects))
            {
                return; // the event is not executed in order and can thus not be used for batch ack
            }

            // register for an ack (when the event is durable) at which point we can checkpoint the receipt
            this.pendingCheckpoint = new Checkpoint(partitionId, eventData.SystemProperties.Offset, eventData.SystemProperties.SequenceNumber);
            AckListeners.Register(evt, this);

            timeSinceLastCheckpoint.Restart();
        }

        public void Acknowledge(Event evt)
        {
            this.completedCheckpoint = this.pendingCheckpoint;
            this.pendingCheckpoint = null;
        }
    }
}
