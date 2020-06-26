using DurableTask.EventSourced.EventHubs;
using Microsoft.Azure.Documents;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventSourced.TransportProviders.EventHubs
{
    class CustomConstantEventProcessorHost
    {
        private string eventHubPath;
        private string consumerGroupName;
        private string eventHubConnectionString;
        private string storageConnectionString;
        private string leaseContainerName;
        private TransportAbstraction.IHost host;
        private TransportAbstraction.ISender sender;
        private readonly EventHubsConnections connections;
        private EventHubsTransport.TaskhubParameters parameters;
        private EventHubsTraceHelper logger;
        private int numberOfPartitions;
        private CancellationTokenSource shutdownSource;

        // Just copied from EventHubsTransport
        private const int MaxReceiveBatchSize = 10000; // actual batches will always be much smaller

        public CustomConstantEventProcessorHost(
            string eventHubPath,
            string consumerGroupName,
            string eventHubConnectionString,
            string storageConnectionString,
            string leaseContainerName,
            TransportAbstraction.IHost host,
            TransportAbstraction.ISender sender,
            EventHubsConnections connections,
            EventHubsTransport.TaskhubParameters parameters,
            EventHubsTraceHelper logger)
        {
            this.eventHubPath = eventHubPath;
            this.consumerGroupName = consumerGroupName;
            this.eventHubConnectionString = eventHubConnectionString;
            this.storageConnectionString = storageConnectionString;
            this.leaseContainerName = leaseContainerName;
            this.host = host;
            this.sender = sender;
            this.connections = connections;
            this.parameters = parameters;
            this.logger = logger;
        }


        // TODO: This has to have some restarting logic and management
        public async Task StartEventProcessing()
        {
            this.logger.LogInformation("Custom EventProcessorHost {eventHubPath}--{consumerGroupName} is starting", this.eventHubPath, this.consumerGroupName);
            this.shutdownSource = new CancellationTokenSource();

            this.numberOfPartitions = this.parameters.StartPositions.Length;

            var partitions = new List<TransportAbstraction.IPartition>();
            var partitionEventLoops = new List<Task>();
            for (var partitionIndex = 0; partitionIndex < this.numberOfPartitions; partitionIndex++)
            {
                var partitionId = Convert.ToUInt32(partitionIndex);

                // Q: Do we need a new cancellation token?
                this.logger.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} is starting partition", this.eventHubPath, partitionId);

                // start this partition (which may include waiting for the lease to become available)
                var partition = this.host.AddPartition(partitionId, this.sender);
                partitions.Add(partition);
                var errorHandler = this.host.CreateErrorHandler(partitionId);

                // TODO: What if this fails???
                var nextPacketToReceive = await partition.CreateOrRestoreAsync(errorHandler, this.parameters.StartPositions[partitionIndex]).ConfigureAwait(false);

                this.logger.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} started partition, next expected packet is #{nextSeqno}", this.eventHubPath, partitionId, nextPacketToReceive);

                this.logger.LogDebug("Starting Event Processing loop for partition{partitionIndex}", partitionIndex);
                var partitionEventLoop = Task.Run(() => PartitionEventLoop(partitionId, nextPacketToReceive, partition));
                partitionEventLoops.Add(partitionEventLoop);
            }
            this.logger.LogDebug("Custom EventProcessorHost successfully started the ReceiverLoops.");
            await Task.WhenAll(partitionEventLoops);

            foreach (var loopTask in partitionEventLoops)
            {
                if(loopTask.IsFaulted)
                {
                    // Make this more informed
                    throw new Exception();
                }
            }
        }

        // TODO: Finish this method. Make sure that events are indeed received by the partition
        private async Task PartitionEventLoop(uint partitionId, 
                                              long initialPacketToReceive, 
                                              TransportAbstraction.IPartition partition)
        {
            this.logger.LogDebug("Receiver Loop for Partition{partitionId} started", partitionId.ToString());

            // This could be wrong
            var partitionReceiver = this.connections.GetPartitionReceiver(partitionId, this.consumerGroupName, initialPacketToReceive);
            var receivedEvents = new List<PartitionEvent>();
            var nextPacketToReceive = initialPacketToReceive;

            while (!this.shutdownSource.IsCancellationRequested)
            {
                // Catch errors around this
                IEnumerable<EventData> eventData = await partitionReceiver.ReceiveAsync(MaxReceiveBatchSize, TimeSpan.FromMinutes(1)).ConfigureAwait(false);
                this.logger.LogTrace("EventProcessor for Partition{partitionId} tried to receive eventdata from position {position}", partitionId.ToString(), nextPacketToReceive);
                if (eventData != null)
                {
                    this.logger.LogDebug("EventProcessor for Partition{partitionId} received eventdata from position {position}", partitionId.ToString(), nextPacketToReceive);
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
                                    this.logger.LogError("EventHubsProcessor {eventHubName}/{eventHubPartition} could not deserialize packet #{seqno} ({size} bytes) eventId={eventId}", this.eventHubPath, partitionId, seqno, eventDatum.Body.Count, eventId);
                                    throw;
                                }
                                this.logger.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} received packet #{seqno} ({size} bytes) {event} id={eventId}", this.eventHubPath, partitionId, seqno, eventDatum.Body.Count, partitionEvent, eventId);
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
                                this.logger.LogError("EventHubsProcessor {eventHubName}/{eventHubPartition} received wrong packet, #{seqno} instead of #{expected}", this.eventHubPath, partitionId, seqno, nextPacketToReceive);
                                // this should never happen, as EventHubs guarantees in-order delivery of packets
                                throw new InvalidOperationException("EventHubs Out-Of-Order Packet");
                            }
                            else
                            {
                                this.logger.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} discarded packet #{seqno} because it is already processed", this.eventHubPath, partitionId, seqno);
                            }
                        }
                        

                        if (batch.Count > 0)
                        {
                            this.logger.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} received batch of {batchsize} packets, starting with #{seqno}, next expected packet is #{nextSeqno}", this.eventHubPath, partitionId, batch.Count, batch[0].NextInputQueuePosition - 1, nextPacketToReceive);
                            partition.SubmitExternalEvents(batch);
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        this.logger.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} was terminated", this.eventHubPath, partitionId);
                    }
                    catch (Exception exception)
                    {
                        this.logger.LogError("EventHubsProcessor {eventHubName}/{eventHubPartition} encountered an exception while processing packets : {exception}", this.eventHubPath, partitionId, exception);
                        partition.ErrorHandler.HandleError("IEventProcessor.ProcessEventsAsync", "Encountered exception while processing events", exception, true, false);
                    }
                }
            }
            this.logger.LogDebug("EventProcessor for Partition{partitionId} exits", partitionId.ToString());
        }
    }
}
