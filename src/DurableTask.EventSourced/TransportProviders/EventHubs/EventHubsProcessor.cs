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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Mime;
using System.Net.NetworkInformation;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DurableTask.EventSourced.Emulated;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace DurableTask.EventSourced.EventHubs
{
    internal class EventHubsProcessor : IEventProcessor, TransportAbstraction.IAckListener
    {
        private readonly TransportAbstraction.IHost host;
        private readonly TransportAbstraction.ISender sender;
        private readonly EventHubsTransport.TaskhubParameters parameters;
        private readonly ILogger logger;
        private readonly PartitionContext partitionContext;
        private readonly string eventHubName;
        private readonly string eventHubPartition;
        private uint partitionId;

        //private uint partitionId;
        private CancellationTokenSource eventProcessorShutdown;

        // we occasionally checkpoint received packets with eventhubs. It is not required for correctness
        // as we filter duplicates anyway, but it will help startup time.
        private Stopwatch timeSinceLastCheckpoint = new Stopwatch();
        private volatile Checkpoint pendingCheckpoint;

        // since EventProcessorHost does not redeliver packets, we need to keep them around until we are sure
        // they are processed durably, so we can redeliver them when recycling/recovering a partition
        // we make this a concurrent queue so we can remove confirmed events concurrently with receiving new ones
        private ConcurrentQueue<(PartitionEvent evt, string offset, long seqno)> packetDeliveryBackup;

        // this points to the latest incarnation of this partition; it gets
        // updated as we recycle partitions (create new incarnations after failures)
        private volatile Task<CurrentPartition> currentPartition;

        /// <summary>
        /// The event processor can recover after exceptions, so we encapsulate
        /// the currently active partition
        /// </summary>
        private class CurrentPartition
        {
            public int Incarnation;
            public IPartitionErrorHandler ErrorHandler;
            public TransportAbstraction.IPartition Partition;
            public Task<CurrentPartition> Next;
            public ulong NextPacketToReceive;
        }

        private Dictionary<string, MemoryStream> reassembly = new Dictionary<string, MemoryStream>();

        public EventHubsProcessor(
            TransportAbstraction.IHost host,
            TransportAbstraction.ISender sender,
            EventHubsTransport.TaskhubParameters parameters,
            PartitionContext partitionContext)
        {
            this.host = host;
            this.logger = host.TransportLogger;
            this.sender = sender;
            this.parameters = parameters;
            this.packetDeliveryBackup = new ConcurrentQueue<(PartitionEvent evt, string offset, long seqno)>();
            this.partitionContext = partitionContext;
            this.eventHubName = this.partitionContext.EventHubPath;
            this.eventHubPartition = this.partitionContext.PartitionId;
            this.partitionId = uint.Parse(this.eventHubPartition);
        }

        Task IEventProcessor.OpenAsync(PartitionContext context)
        {
            this.logger.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} Starting", this.eventHubName, this.eventHubPartition);
            this.eventProcessorShutdown = new CancellationTokenSource();

            // we kick off the start-and-retry mechanism for the partition, but don't wait for it to be fully started.
            // instead, we save the task and wait for it when we need it
            this.currentPartition = this.StartPartitionAsync();

            return Task.CompletedTask;
        }

        public void Acknowledge(Event evt)
        {
            // this is called after an event has committed (i.e. has been durably persisted in the recovery log).
            // so we know we will never need to deliver it again. We remove it from the local buffer, and also checkpoint
            // with EventHubs occasionally.
            while (this.packetDeliveryBackup.TryPeek(out var front) && front.evt.NextInputQueuePosition.Value <= evt.NextInputQueuePosition.Value)
            {
                if (this.packetDeliveryBackup.TryDequeue(out var candidate))
                {
                    if (this.timeSinceLastCheckpoint.ElapsedMilliseconds > 30000)
                    {
                        this.pendingCheckpoint = new Checkpoint(this.partitionId.ToString(), candidate.offset, candidate.seqno);
                        timeSinceLastCheckpoint.Restart();
                    }
                }
            }
        }

        private async Task<CurrentPartition> StartPartitionAsync(CurrentPartition prior = null)
        {
            int incarnation = 1;

            // if this is not the first incarnation of this partition, wait for the previous incarnation to be terminated.
            if (prior != null)
            {
                incarnation = prior.Incarnation + 1;
                await TaskHelpers.WaitForCancellationAsync(prior.ErrorHandler.Token);
                this.currentPartition = prior.Next;
                this.logger.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition}  Restarting EventProcessor (incarnation {incarnation}) soon", this.eventHubName, this.eventHubPartition, incarnation);
                await Task.Delay(TimeSpan.FromSeconds(12), this.eventProcessorShutdown.Token);
            }

            // check that we are not already shutting down before even starting this
            this.eventProcessorShutdown.Token.ThrowIfCancellationRequested();

            // create the record for the this incarnation, and start the next one also.
            var c = new CurrentPartition();
            c.Incarnation = incarnation;
            c.ErrorHandler = this.host.CreateErrorHandler(this.partitionId);
            c.Next = this.StartPartitionAsync(c);
            c.Partition = host.AddPartition(this.partitionId, this.sender);
            c.NextPacketToReceive = await c.Partition.StartAsync(c.ErrorHandler, (ulong)this.parameters.StartPositions[this.partitionId]);

            this.logger.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} Successfully started EventProcessor (incarnation {incarnation}), next expected packet is #{nextSeqno}", this.eventHubName, this.eventHubPartition, incarnation, c.NextPacketToReceive);

            // receive packets already sitting in the buffer; use lock to prevent race with new packets being delivered
            lock (this.packetDeliveryBackup)
            {
                var batch = packetDeliveryBackup.Select(triple => triple.Item1).Where(evt => evt.NextInputQueuePosition > c.NextPacketToReceive).ToList();
                if (batch.Count > 0)
                {
                    c.NextPacketToReceive = batch[batch.Count - 1].NextInputQueuePosition.Value;
                    c.Partition.SubmitInputEvents(batch);
                    this.logger.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} Received {batchsize} packets, starting with #{seqno}, next expected packet is #{nextSeqno}", this.eventHubName, this.eventHubPartition, batch.Count, batch[0].NextInputQueuePosition - 1, c.NextPacketToReceive);
                }
            }

            timeSinceLastCheckpoint.Start();
            return c;
        }

        async Task IEventProcessor.CloseAsync(PartitionContext context, CloseReason reason)
        {
            this.logger.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} Stopping EventProcessor for partition", this.eventHubName, this.eventHubPartition);

            this.eventProcessorShutdown.Cancel(); // stops the automatic partition restart, but does not stop the current partition

            try
            {
                CurrentPartition current = await this.currentPartition;

                while (current.ErrorHandler.IsTerminated)
                {
                    current = await current.Next;
                }

                await current.Partition.StopAsync();

                this.logger.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} cleanly stopped partition (incarnation {incarnation})", this.eventHubName, this.eventHubPartition, current.Incarnation);
            }
            catch (OperationCanceledException)
            {
                this.logger.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} terminated partition", this.eventHubName, this.eventHubPartition);
            }

            await SaveEventHubsReceiverCheckpoint(context);

            this.logger.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} for partition", this.eventHubName, this.eventHubPartition);
        }

        private async ValueTask SaveEventHubsReceiverCheckpoint(PartitionContext context)
        {
            var checkpoint = this.pendingCheckpoint;
            if (checkpoint != null)
            {
                this.pendingCheckpoint = null;
                this.logger.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} Checkpointing packets received through #{seqno}", this.eventHubName, this.eventHubPartition, checkpoint.SequenceNumber);
                try
                {
                    await context.CheckpointAsync(checkpoint);
                }
                catch (Exception e)
                {
                    // updating EventHubs checkpoints has been known to fail occasionally due to leases shifting around; since it is optional anyway
                    // we don't want this exception to cause havoc
                    this.logger.LogWarning("EventHubsProcessor {eventHubName}/{eventHubPartition} could not checkpoint packets received: {e}", this.eventHubName, this.eventHubPartition, e);
                }
            }
        } 

        Task IEventProcessor.ProcessErrorAsync(PartitionContext context, Exception exception)
        {
            this.logger.LogWarning("EventHubsProcessor {eventHubName}/{eventHubPartition} encountered an exception: {exception}", this.eventHubName, this.eventHubPartition, exception);

            return Task.FromResult<object>(null);
        }

        async Task IEventProcessor.ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> packets)
        {
            CurrentPartition current = null;

            try
            {
                current = await this.currentPartition;

                while (current.ErrorHandler.IsTerminated)
                {
                    current = await current.Next;
                }
                
                var batch = new List<PartitionEvent>();

                lock (this.packetDeliveryBackup) // must prevent race with a partition that is restarting in the background
                {
                    foreach (var eventData in packets)
                    {
                        var seqno = (ulong)eventData.SystemProperties.SequenceNumber;
                        if (seqno == current.NextPacketToReceive)
                        {
                            string eventId = null;
                            PartitionEvent evt = null;
                            try
                            {
                                Packet.Deserialize<PartitionEvent>(eventData.Body, out eventId, out evt);
                            }
                            catch (Exception)
                            {
                                this.logger.LogError("EventHubsProcessor {eventHubName}/{eventHubPartition} could not deserialize packet #{seqno} ({size} bytes) eventId={eventId}", this.eventHubName, this.eventHubPartition, seqno, eventData.Body.Count, eventId);
                                throw;
                            }
                            this.logger.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} received packet #{seqno} ({size} bytes) {evt} id={eventId}", this.eventHubName, this.eventHubPartition, seqno, eventData.Body.Count, evt, eventId);
                            current.NextPacketToReceive = seqno + 1;
                            evt.NextInputQueuePosition = current.NextPacketToReceive;
                            batch.Add(evt);
                            packetDeliveryBackup.Enqueue((evt, eventData.SystemProperties.Offset, eventData.SystemProperties.SequenceNumber));
                            AckListeners.Register(evt, this);
                        }
                        else if (seqno > current.NextPacketToReceive)
                        {
                            this.logger.LogError("EventHubsProcessor {eventHubName}/{eventHubPartition} received wrong packet, #{seqno} instead of #{expected}", this.eventHubName, this.eventHubPartition, seqno, current.NextPacketToReceive);
                            // this should never happen, as EventHubs guarantees in-order delivery of packets
                            throw new InvalidOperationException("EventHubs Out-Of-Order Packet");
                        }
                        else
                        {
                            this.logger.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} discarded packet #{seqno} because it is already processed", this.eventHubName, this.eventHubPartition, seqno);
                        }
                    }
                }

                if (batch.Count > 0)
                {
                    this.logger.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} received batch of {batchsize} packets, starting with #{seqno}, next expected packet is #{nextSeqno}", this.eventHubName, this.eventHubPartition, batch.Count, batch[0].NextInputQueuePosition - 1, current.NextPacketToReceive);
                    current.Partition.SubmitInputEvents(batch);
                }

                await this.SaveEventHubsReceiverCheckpoint(context);

                // can use this for testing: terminates partition after every one packet received, but
                // that packet is then processed once the partition recovers, so in the end there is progress
                // throw new InvalidOperationException("error injection");
            }
            catch (OperationCanceledException)
            {
                this.logger.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} was terminated", this.eventHubName, this.eventHubPartition);
            }
            catch (Exception exception)
            {
                this.logger.LogError("EventHubsProcessor {eventHubName}/{eventHubPartition} Error while processing packets : {exception}", this.eventHubName, this.eventHubPartition, exception);
                current?.ErrorHandler.HandleError("IEventProcessor.ProcessEventsAsync", "unexpected error", exception, true, false);
            }
        }
    }
}
