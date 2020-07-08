using DurableTask.Core.Common;
using Microsoft.Azure.EventHubs;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventSourced.TransportProviders.EventHubs
{
    /// <summary>
    /// This is a controller that starts, stops, and kills partitions, and manages their eventHubs receivers. 
    /// It is a cleaner interface for EventProcessor and is used by the CustomEventProcessor hosts.
    /// </summary>
    class CustomPartitionController
    {
        private uint partitionId;
        private CustomConstantEventProcessorHost eventProcessorHost;
        private TransportAbstraction.IPartition partition;
        private Task partitionEventLoop;
        private long nextPacketToReceive;
        private CancellationTokenSource shutdownSource;

        // Just copied from EventHubsTransport
        private const int MaxReceiveBatchSize = 10000; // actual batches will always be much smaller

        public CustomPartitionController(uint partitionId, CustomConstantEventProcessorHost eventProcessorHost)
        {
            this.partitionId = partitionId;
            this.eventProcessorHost = eventProcessorHost;
        }

        // TODO: Handle errors
        public async Task StartPartitionAndLoop()
        {
            this.shutdownSource = new CancellationTokenSource();

            this.eventProcessorHost.logger.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} is starting partition", this.eventProcessorHost.eventHubPath, partitionId);

            // start this partition (which may include waiting for the lease to become available)
            this.partition = this.eventProcessorHost.host.AddPartition(partitionId, this.eventProcessorHost.sender);
            
            var errorHandler = this.eventProcessorHost.host.CreateErrorHandler(partitionId);

            this.nextPacketToReceive = await partition.CreateOrRestoreAsync(errorHandler, this.eventProcessorHost.parameters.StartPositions[Convert.ToInt32(partitionId)]).ConfigureAwait(false);
            this.eventProcessorHost.logger.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} started partition, next expected packet is #{nextSeqno}", this.eventProcessorHost.eventHubPath, partitionId, nextPacketToReceive);

            this.eventProcessorHost.logger.LogDebug("Starting Event Processing loop for partition{partitionIndex}", partitionId);
            this.partitionEventLoop = Task.Run(() => this.PartitionEventLoop());
        }

        // TODO: Handle errors
        public async Task StopPartitionAndLoop()
        {
            // First stop the partition. We need to wait until it shutdowns before closing the receiver, since it needs to receive confirmation events.
            this.eventProcessorHost.logger.LogDebug("PartitionController {eventHubName}/{eventHubPartition} stopping partition)", this.eventProcessorHost.eventHubPath, partitionId);
            await partition.StopAsync().ConfigureAwait(false);
            this.eventProcessorHost.logger.LogDebug("PartitionController {eventHubName}/{eventHubPartition} stopped partition", this.eventProcessorHost.eventHubPath, partitionId);

            // Stop the receiver loop
            this.shutdownSource.Cancel();

            // In a previous version we were waiting for partitionEventLoop to finish before progressing, but that
            // introduces unnecessary delays. So now we don't wait. However, if we restart, this.shutdownSource will be reinitialized.
            //
            // TODO: Make sure that this can not lead to old partitioneventloops not being stopped properly.   
            //await this.partitionEventLoop;
        }

        // TODO: Update all the logging messages
        private async Task PartitionEventLoop()
        {
            this.eventProcessorHost.logger.LogDebug("Receiver Loop for Partition{partitionId} started", partitionId.ToString());

            // This could be wrong
            var partitionReceiver = this.eventProcessorHost.connections.CreatePartitionReceiver(partitionId, this.eventProcessorHost.consumerGroupName, this.nextPacketToReceive);

            // Save a reference to the current shutdownSource so that it exits even if a new shutdownSource is started together with a new loop.
            var loopShutdownSource = this.shutdownSource;

            while (!loopShutdownSource.IsCancellationRequested)
            {
                // TODO: Catch errors around this
                // TODO: Is there a way to cancel the receive async if there is a requested cancellation?
                IEnumerable<EventData> eventData = await partitionReceiver.ReceiveAsync(MaxReceiveBatchSize, TimeSpan.FromSeconds(10)).ConfigureAwait(false);
                this.eventProcessorHost.logger.LogTrace("EventProcessor for Partition{partitionId} tried to receive eventdata from position {position}", partitionId.ToString(), nextPacketToReceive);
                if (eventData != null)
                {
                    this.eventProcessorHost.logger.LogDebug("EventProcessor for Partition{partitionId} received eventdata from position {position}", partitionId.ToString(), nextPacketToReceive);
                    try
                    {
                        var batch = new List<PartitionEvent>();
                        var receivedTimestamp = partition.CurrentTimeMs;

                        // I am not sure if this will be necessary at some point.
                        //lock (this.pendingDelivery) // must prevent race with a partition that is currently restarting

                        foreach (var eventDatum in eventData)
                        {
                            var seqno = eventDatum.SystemProperties.SequenceNumber;
                            if (seqno == nextPacketToReceive)
                            {
                                string eventId = null;
                                PartitionEvent partitionEvent = null;
                                try
                                {
                                    Packet.Deserialize(eventDatum.Body, out eventId, out partitionEvent);
                                }
                                catch (Exception)
                                {
                                    this.eventProcessorHost.logger.LogError("EventHubsProcessor {eventHubName}/{eventHubPartition} could not deserialize packet #{seqno} ({size} bytes) eventId={eventId}", this.eventProcessorHost.eventHubPath, partitionId, seqno, eventDatum.Body.Count, eventId);
                                    throw;
                                }
                                this.eventProcessorHost.logger.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} received packet #{seqno} ({size} bytes) {event} id={eventId}", this.eventProcessorHost.eventHubPath, partitionId, seqno, eventDatum.Body.Count, partitionEvent, eventId);
                                nextPacketToReceive = seqno + 1;
                                partitionEvent.NextInputQueuePosition = nextPacketToReceive;
                                batch.Add(partitionEvent);

                                // Q: Could these be of any use?
                                //pendingDelivery.Enqueue((partitionEvent, eventData.SystemProperties.Offset, eventData.SystemProperties.SequenceNumber));
                                //DurabilityListeners.Register(partitionEvent, this);
                                partitionEvent.ReceivedTimestamp = partition.CurrentTimeMs;
                            }
                            else if (seqno > nextPacketToReceive)
                            {
                                this.eventProcessorHost.logger.LogError("EventHubsProcessor {eventHubName}/{eventHubPartition} received wrong packet, #{seqno} instead of #{expected}", this.eventProcessorHost.eventHubPath, partitionId, seqno, nextPacketToReceive);
                                // this should never happen, as EventHubs guarantees in-order delivery of packets
                                throw new InvalidOperationException("EventHubs Out-Of-Order Packet");
                            }
                            else
                            {
                                this.eventProcessorHost.logger.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} discarded packet #{seqno} because it is already processed", this.eventProcessorHost.eventHubPath, partitionId, seqno);
                            }
                        }


                        if (batch.Count > 0)
                        {
                            this.eventProcessorHost.logger.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} received batch of {batchsize} packets, starting with #{seqno}, next expected packet is #{nextSeqno}", this.eventProcessorHost.eventHubPath, partitionId, batch.Count, batch[0].NextInputQueuePosition - 1, nextPacketToReceive);
                            partition.SubmitExternalEvents(batch);
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        this.eventProcessorHost.logger.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} was terminated", this.eventProcessorHost.eventHubPath, partitionId);
                    }
                    catch (Exception exception)
                    {
                        this.eventProcessorHost.logger.LogError("EventHubsProcessor {eventHubName}/{eventHubPartition} encountered an exception while processing packets : {exception}", this.eventProcessorHost.eventHubPath, partitionId, exception);
                        partition.ErrorHandler.HandleError("IEventProcessor.ProcessEventsAsync", "Encountered exception while processing events", exception, true, false);
                    }
                }
            }
            this.eventProcessorHost.logger.LogInformation("EventProcessor ReceiverLoop for Partition{partitionId} exits", partitionId.ToString());
        }
    }
}
