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
using System.Threading.Tasks;
using DurableTask.EventSourced.Emulated;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;

namespace DurableTask.EventSourced.EventHubs
{
    internal class EventProcessor : IEventProcessor, TransportAbstraction.IAckListener
    {
        private readonly TransportAbstraction.IHost host;
        private readonly TransportAbstraction.ISender sender;
        private readonly Guid processorId;
        private readonly EventSourcedOrchestrationServiceSettings settings;

        private StorageAbstraction.IPartitionState partitionState;
        private TransportAbstraction.IPartition partition;

        private Stopwatch timeSinceLastCheckpoint = new Stopwatch();
        private int eventsSinceLastCheckpoint;
        private Checkpoint pendingCheckpoint;
        private Checkpoint completedCheckpoint;
        private long inputQueuePosition;

        private Dictionary<string, MemoryStream> reassembly = new Dictionary<string, MemoryStream>();

        public EventProcessor(TransportAbstraction.IHost host, TransportAbstraction.ISender sender, EventSourcedOrchestrationServiceSettings settings)
        {
            this.host = host;
            this.sender = sender;
            this.processorId = Guid.NewGuid();
            this.settings = settings;
        }

        async Task IEventProcessor.OpenAsync(PartitionContext context)
        {
            uint partitionId = uint.Parse(context.PartitionId);
            this.partitionState = this.host.CreatePartitionState();
            this.partition = host.AddPartition(partitionId, this.partitionState, this.sender);
            this.inputQueuePosition = await this.partition.StartAsync();
            timeSinceLastCheckpoint.Start();
            eventsSinceLastCheckpoint = 0;
        }

        async Task IEventProcessor.CloseAsync(PartitionContext context, CloseReason reason)
        {         
            await this.partition.StopAsync();

            if (this.completedCheckpoint != null
                && !this.partitionState.OwnershipCancellationToken.IsCancellationRequested)
            {
                await context.CheckpointAsync(this.completedCheckpoint);
                completedCheckpoint = null;
            }
        }

        Task IEventProcessor.ProcessErrorAsync(PartitionContext context, Exception error)
        {
            partition.ReportError("EventHubs", error);
            return Task.FromResult<object>(null);
        }

        Task IEventProcessor.ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
        {
            this.partitionState.OwnershipCancellationToken.ThrowIfCancellationRequested();

            var batch = new List<PartitionEvent>();
            EventData last = null;

            foreach(var eventData in messages)
            {
                var seqno = eventData.SystemProperties.SequenceNumber;
                if (seqno > this.inputQueuePosition)
                {
                    var evt = (PartitionEvent)Serializer.DeserializeEvent(eventData.Body);
                    evt.Serialized = eventData.Body; // we'll reuse this for writing to the event log
                    evt.InputQueuePosition = seqno;
                    batch.Add(evt);
                    this.inputQueuePosition = seqno;
                }
                last = eventData;
            }

            this.eventsSinceLastCheckpoint += batch.Count;

            if (this.pendingCheckpoint == null && this.completedCheckpoint == null
                && (this.timeSinceLastCheckpoint.ElapsedMilliseconds > 30000 || this.eventsSinceLastCheckpoint > 3000))
            {
                this.eventsSinceLastCheckpoint = 0;
                this.timeSinceLastCheckpoint.Restart();
                this.pendingCheckpoint = new Checkpoint(context.PartitionId, last.SystemProperties.Offset, last.SystemProperties.SequenceNumber);
                var lastEventInBatch = batch[batch.Count - 1];
                AckListeners.Register(lastEventInBatch, this); 
            }

            this.partitionState.OwnershipCancellationToken.ThrowIfCancellationRequested();

            partition.SubmitRange(batch);

            if (this.completedCheckpoint != null)
            {
                var checkpoint = this.completedCheckpoint;
                this.completedCheckpoint = null;
                return context.CheckpointAsync(checkpoint);
            }

            return Task.CompletedTask;
        }

        public void Acknowledge(Event evt)
        {
            this.completedCheckpoint = this.pendingCheckpoint;
            this.pendingCheckpoint = null;
        }
    }
}
