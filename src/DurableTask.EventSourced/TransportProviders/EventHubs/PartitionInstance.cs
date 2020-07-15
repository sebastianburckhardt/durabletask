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
    /// Represents a particular instance of a partition that is being managed by a CustomEventProcessor host.
    /// </summary>
    class PartitionInstance
    {
        private readonly uint partitionId;
        private readonly CustomConstantEventProcessorHost eventProcessorHost;

        private TransportAbstraction.IPartition partition;
        private Task partitionEventLoop;
        private PartitionReceiver partitionReceiver;
        private CancellationTokenSource shutdownSource;

        // Just copied from EventHubsTransport
        private const int MaxReceiveBatchSize = 10000; // actual batches will always be much smaller

        public PartitionInstance(uint partitionId, int incarnation, CustomConstantEventProcessorHost eventProcessorHost)
        {
            this.partitionId = partitionId;
            this.Incarnation = incarnation;
            this.eventProcessorHost = eventProcessorHost;
        }

        public int Incarnation { get; }

        // TODO: Handle errors
        public async Task StartAsync()
        {
            this.shutdownSource = new CancellationTokenSource();

            this.eventProcessorHost.logger.LogDebug("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) is starting partition", this.eventProcessorHost.eventHubPath, partitionId, this.Incarnation);

            // start this partition (which may include waiting for the lease to become available)
            this.partition = this.eventProcessorHost.host.AddPartition(partitionId, this.eventProcessorHost.sender);

            var errorHandler = this.eventProcessorHost.host.CreateErrorHandler(partitionId);

            var nextPacketToReceive = await partition.CreateOrRestoreAsync(errorHandler, this.eventProcessorHost.parameters.StartPositions[Convert.ToInt32(partitionId)]).ConfigureAwait(false);
            this.eventProcessorHost.logger.LogInformation("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) started partition, next expected packet is #{nextSeqno}", this.eventProcessorHost.eventHubPath, partitionId, this.Incarnation, nextPacketToReceive);

            this.partitionEventLoop = Task.Run(() => this.PartitionEventLoop(nextPacketToReceive));
        }

        // TODO: Handle errors
        public async Task StopAsync()
        {
            // First stop the partition. We need to wait until it shutdowns before closing the receiver, since it needs to receive confirmation events.
            this.eventProcessorHost.logger.LogDebug("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) stopping partition)", this.eventProcessorHost.eventHubPath, partitionId, this.Incarnation);
            await partition.StopAsync().ConfigureAwait(false);
            this.eventProcessorHost.logger.LogDebug("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) stopped partition", this.eventProcessorHost.eventHubPath, partitionId, this.Incarnation);

            // Stop the receiver loop
            this.shutdownSource.Cancel();

            // wait for the receiver loop to terminate
            await this.partitionEventLoop;

            // shut down the partition receiver (eventHubs complains if more than 5 of these are active per partition)
            await this.partitionReceiver.CloseAsync();

            this.eventProcessorHost.logger.LogDebug("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) stopped receive loop", this.eventProcessorHost.eventHubPath, partitionId, this.Incarnation);
        }

        // TODO: Update all the logging messages
        private async Task PartitionEventLoop(long nextPacketToReceive)
        {
            this.eventProcessorHost.logger.LogDebug("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) starting receive loop", this.eventProcessorHost.eventHubPath, partitionId, this.Incarnation);
            try
            {
                this.partitionReceiver = this.eventProcessorHost.connections.CreatePartitionReceiver(partitionId, this.eventProcessorHost.consumerGroupName, nextPacketToReceive);

                while (!this.shutdownSource.IsCancellationRequested)
                {
                    this.eventProcessorHost.logger.LogTrace("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) trying to receive eventdata from position {position}", this.eventProcessorHost.eventHubPath, partitionId, this.Incarnation, nextPacketToReceive);

                    // TODO: Catch errors around this
                    // TODO: Is there a way to cancel the receive async if there is a requested cancellation?
                    IEnumerable<EventData> eventData = await this.partitionReceiver.ReceiveAsync(MaxReceiveBatchSize, TimeSpan.FromSeconds(10)).ConfigureAwait(false);

                    this.shutdownSource.Token.ThrowIfCancellationRequested();

                    if (eventData != null)
                    {
                        this.eventProcessorHost.logger.LogDebug("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) received eventdata from position {position}", this.eventProcessorHost.eventHubPath, partitionId, this.Incarnation, nextPacketToReceive);

                        var batch = new List<PartitionEvent>();
                        var receivedTimestamp = partition.CurrentTimeMs;

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
                                    this.eventProcessorHost.logger.LogError("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) could not deserialize packet #{seqno} ({size} bytes) eventId={eventId}", this.eventProcessorHost.eventHubPath, partitionId, this.Incarnation, seqno, eventDatum.Body.Count, eventId);
                                    throw;
                                }
                                this.eventProcessorHost.logger.LogDebug("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) received packet #{seqno} ({size} bytes) {event} id={eventId}", this.eventProcessorHost.eventHubPath, partitionId, this.Incarnation, seqno, eventDatum.Body.Count, partitionEvent, eventId);
                                nextPacketToReceive = seqno + 1;
                                partitionEvent.NextInputQueuePosition = nextPacketToReceive;
                                batch.Add(partitionEvent);
                                partitionEvent.ReceivedTimestamp = partition.CurrentTimeMs;
                            }
                            else if (seqno > nextPacketToReceive)
                            {
                                this.eventProcessorHost.logger.LogError("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) received wrong packet, #{seqno} instead of #{expected}", this.eventProcessorHost.eventHubPath, partitionId, this.Incarnation, seqno, nextPacketToReceive);
                                // this should never happen, as EventHubs guarantees in-order delivery of packets
                                throw new InvalidOperationException("EventHubs Out-Of-Order Packet");
                            }
                            else
                            {
                                this.eventProcessorHost.logger.LogDebug("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) discarded packet #{seqno} because it is already processed", this.eventProcessorHost.eventHubPath, partitionId, this.Incarnation, seqno);
                            }
                        }

                        if (batch.Count > 0 && !this.shutdownSource.IsCancellationRequested)
                        {
                            this.eventProcessorHost.logger.LogDebug("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) received batch of {batchsize} packets, starting with #{seqno}, next expected packet is #{nextSeqno}", this.eventProcessorHost.eventHubPath, partitionId, this.Incarnation, batch.Count, batch[0].NextInputQueuePosition - 1, nextPacketToReceive);
                            partition.SubmitExternalEvents(batch);
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
                this.eventProcessorHost.logger.LogInformation("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) was terminated", this.eventProcessorHost.eventHubPath, partitionId, this.Incarnation);
            }
            catch (Exception exception)
            {
                this.eventProcessorHost.logger.LogError("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) encountered an exception while processing packets : {exception}", this.eventProcessorHost.eventHubPath, partitionId, this.Incarnation, exception);
                partition.ErrorHandler.HandleError("IEventProcessor.ProcessEventsAsync", "Encountered exception while processing events", exception, true, false);
            }

            this.eventProcessorHost.logger.LogInformation("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) ReceiverLoop exits", this.eventProcessorHost.eventHubPath, partitionId, this.Incarnation);
        }
    }
}
