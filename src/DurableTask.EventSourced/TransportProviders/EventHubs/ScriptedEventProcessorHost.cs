using DurableTask.Core.Common;
using DurableTask.EventSourced.EventHubs;
using ImpromptuInterface;
using Microsoft.Azure.Documents;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventSourced.TransportProviders.EventHubs
{
    class ScriptedEventProcessorHost
    {
        private readonly string eventHubPath;
        private readonly string consumerGroupName;
        private readonly string eventHubConnectionString;
        private readonly string storageConnectionString;
        private readonly string leaseContainerName;
        private readonly string workerId;
        private readonly TransportAbstraction.IHost host;
        private readonly TransportAbstraction.ISender sender;
        private readonly EventHubsConnections connections;
        private readonly EventHubsTransport.TaskhubParameters parameters;
        private readonly EventHubsTraceHelper logger;
        private readonly List<PartitionInstance> partitionInstances = new List<PartitionInstance>();

        private int numberOfPartitions;

        public ScriptedEventProcessorHost(
            string eventHubPath,
            string consumerGroupName,
            string eventHubConnectionString,
            string storageConnectionString,
            string leaseContainerName,
            TransportAbstraction.IHost host,
            TransportAbstraction.ISender sender,
            EventHubsConnections connections,
            EventHubsTransport.TaskhubParameters parameters,
            EventHubsTraceHelper logger,
            string workerId)
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
            this.workerId = workerId;
        }

        public void StartEventProcessing(EventSourcedOrchestrationServiceSettings settings, CloudBlockBlob partitionScript)
        {
            if (!partitionScript.Exists())
            {
                this.logger.LogInformation("ScriptedEventProcessorHost workerId={workerId} is waiting for script", this.workerId);
                while (! partitionScript.Exists())
                {
                    Thread.Sleep(TimeSpan.FromSeconds(1));
                }
            }

            // we use the UTC modification timestamp on the script as the scenario start time
            DateTime scenarioStartTimeUtc = partitionScript.Properties.LastModified.Value.UtcDateTime;

            // the number of partitions matters only if the script contains wildcards
            this.numberOfPartitions = this.parameters.StartPositions.Length;
            for (var partitionIndex = 0; partitionIndex < this.numberOfPartitions; partitionIndex++)
            {
                this.partitionInstances.Add(null);
            }

            List<PartitionScript.ProcessorHostEvent> timesteps;

            using (var memoryStream = new System.IO.MemoryStream())
            {
                partitionScript.DownloadRangeToStream(memoryStream, null, null);
                memoryStream.Seek(0, System.IO.SeekOrigin.Begin);
                timesteps = PartitionScript.ParseEvents(scenarioStartTimeUtc, settings.WorkerId, this.numberOfPartitions, memoryStream).ToList();
            }

            this.logger.LogInformation("ScriptedEventProcessorHost workerId={workerId} started.", this.workerId);
      
            int nextTime = 0;
            List<PartitionScript.ProcessorHostEvent> nextGroup = new List<PartitionScript.ProcessorHostEvent>();

            foreach (var timestep in timesteps)
            {
                if (nextTime == timestep.TimeSeconds)
                {
                    nextGroup.Add(timestep);
                }
                else
                {
                    this.Process(nextGroup);
                    nextGroup.Clear();
                    nextGroup.Add(timestep);
                    nextTime = timestep.TimeSeconds;
                }
            }

            this.Process(nextGroup);
        }

        private void Process(List<PartitionScript.ProcessorHostEvent> ready)
        {
            if (ready.Count > 0)
            {
                int delay = (int)(ready[0].TimeUtc - DateTime.UtcNow).TotalMilliseconds;
                if (delay > 0)
                {
                    this.logger.LogInformation("ScriptedEventProcessorHost workerId={workerId} is waiting for {delay} ms until next hostEvent", this.workerId, delay);
                    Thread.Sleep(delay);
                }

                Stopwatch stopwatch = new Stopwatch();
                stopwatch.Start();

                bool parallel = true;

                var tasks = new List<Task>();
                int lasttime = 0;
                foreach (var timestep in ready)
                {
                    this.logger.LogWarning("ScriptedEventProcessorHost workerId={workerId} performs action={action} partition={partition} time={time}.", this.workerId, timestep.Action, timestep.PartitionId, timestep.TimeSeconds);
                    lasttime = timestep.TimeSeconds;
                }
                foreach (var timestep in ready)
                {
                    if (parallel)
                    {
                        tasks.Add(ProcessHostEvent(timestep));
                    }
                    else
                    {
                        ProcessHostEvent(timestep).GetAwaiter().GetResult();
                    }
                }
                Task.WhenAll(tasks).GetAwaiter().GetResult();
                this.logger.LogWarning("ScriptedEventProcessorHost workerId={workerId} finished all actions for time={time} in {elapsedSeconds}s.", this.workerId, lasttime, stopwatch.Elapsed.TotalSeconds);
            }
        }

        private async Task ProcessHostEvent(PartitionScript.ProcessorHostEvent timestep)
        {
            try
            {
                int partitionId = timestep.PartitionId;
                if (timestep.Action == "restart")
                {
                    var oldPartitionInstance = this.partitionInstances[partitionId];
                    var newPartitionInstance = new PartitionInstance((uint) partitionId, oldPartitionInstance.Incarnation + 1, this);
                    this.partitionInstances[partitionId] = newPartitionInstance;
                    await Task.WhenAll(newPartitionInstance.StartAsync(), oldPartitionInstance.StopAsync());
                }
                else if (timestep.Action == "start")
                {
                    var oldPartitionInstance = this.partitionInstances[partitionId];
                    var newPartitionInstance = new PartitionInstance((uint)partitionId, (oldPartitionInstance?.Incarnation ?? 0) + 1, this);
                    this.partitionInstances[partitionId] = newPartitionInstance;
                    await newPartitionInstance.StartAsync();
                }
                else if (timestep.Action == "stop")
                {
                    var oldPartitionInstance = this.partitionInstances[partitionId];
                    await oldPartitionInstance.StopAsync();
                }
                else
                {
                    throw new InvalidOperationException($"Unknown action: {timestep.Action}");
                }

                this.logger.LogWarning("ScriptedEventProcessorHost workerId={workerId} successfully performed action={action} partition={partition} time={time}.", this.workerId, timestep.Action, timestep.PartitionId, timestep.TimeSeconds);
            }
            catch (Exception e) when (!Utils.IsFatal(e))
            {
                // TODO: Maybe in the future we would like to actually do something in case of failure. 
                //       For now it is fine to ignore them.
                this.logger.LogError("ScriptedEventProcessorHost workerId={workerId} failed on action={action} partition={partition} time={time} exception={exception}", this.workerId, timestep.Action, timestep.PartitionId, timestep.TimeSeconds, e);
            }
        }

        /// <summary>
        /// Represents a particular instance of a partition that is being managed by a CustomEventProcessor host.
        /// </summary>
        class PartitionInstance
        {
            private readonly uint partitionId;
            private readonly ScriptedEventProcessorHost host;

            private TransportAbstraction.IPartition partition;
            private Task partitionEventLoop;
            private PartitionReceiver partitionReceiver;
            private CancellationTokenSource shutdownSource;

            // Just copied from EventHubsTransport
            private const int MaxReceiveBatchSize = 10000; // actual batches will always be much smaller

            public PartitionInstance(uint partitionId, int incarnation, ScriptedEventProcessorHost eventProcessorHost)
            {
                this.partitionId = partitionId;
                this.Incarnation = incarnation;
                this.host = eventProcessorHost;
            }

            public int Incarnation { get; }

            // TODO: Handle errors
            public async Task StartAsync()
            {
                this.shutdownSource = new CancellationTokenSource();

                this.host.logger.LogDebug("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) is starting partition", this.host.eventHubPath, partitionId, this.Incarnation);

                // start this partition (which may include waiting for the lease to become available)
                this.partition = this.host.host.AddPartition(partitionId, this.host.sender);

                var errorHandler = this.host.host.CreateErrorHandler(partitionId);

                var nextPacketToReceive = await partition.CreateOrRestoreAsync(errorHandler, this.host.parameters.StartPositions[Convert.ToInt32(partitionId)]).ConfigureAwait(false);
                this.host.logger.LogInformation("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) started partition, next expected packet is #{nextSeqno}", this.host.eventHubPath, partitionId, this.Incarnation, nextPacketToReceive);

                this.partitionEventLoop = Task.Run(() => this.PartitionEventLoop(nextPacketToReceive));
            }

            // TODO: Handle errors
            public async Task StopAsync()
            {
                // First stop the partition. We need to wait until it shutdowns before closing the receiver, since it needs to receive confirmation events.
                this.host.logger.LogDebug("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) stopping partition)", this.host.eventHubPath, partitionId, this.Incarnation);
                await partition.StopAsync().ConfigureAwait(false);
                this.host.logger.LogDebug("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) stopped partition", this.host.eventHubPath, partitionId, this.Incarnation);

                // Stop the receiver loop
                this.shutdownSource.Cancel();

                // wait for the receiver loop to terminate
                await this.partitionEventLoop;

                // shut down the partition receiver (eventHubs complains if more than 5 of these are active per partition)
                await this.partitionReceiver.CloseAsync();

                this.host.logger.LogDebug("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) stopped receive loop", this.host.eventHubPath, partitionId, this.Incarnation);
            }

            // TODO: Update all the logging messages
            private async Task PartitionEventLoop(long nextPacketToReceive)
            {
                this.host.logger.LogDebug("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) starting receive loop", this.host.eventHubPath, partitionId, this.Incarnation);
                try
                {
                    this.partitionReceiver = this.host.connections.CreatePartitionReceiver(partitionId, this.host.consumerGroupName, nextPacketToReceive);

                    while (!this.shutdownSource.IsCancellationRequested)
                    {
                        this.host.logger.LogTrace("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) trying to receive eventdata from position {position}", this.host.eventHubPath, partitionId, this.Incarnation, nextPacketToReceive);

                        // TODO: Catch errors around this
                        // TODO: Is there a way to cancel the receive async if there is a requested cancellation?
                        IEnumerable<EventData> eventData = await this.partitionReceiver.ReceiveAsync(MaxReceiveBatchSize, TimeSpan.FromSeconds(10)).ConfigureAwait(false);

                        this.shutdownSource.Token.ThrowIfCancellationRequested();

                        if (eventData != null)
                        {
                            this.host.logger.LogDebug("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) received eventdata from position {position}", this.host.eventHubPath, partitionId, this.Incarnation, nextPacketToReceive);

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
                                        this.host.logger.LogError("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) could not deserialize packet #{seqno} ({size} bytes) eventId={eventId}", this.host.eventHubPath, partitionId, this.Incarnation, seqno, eventDatum.Body.Count, eventId);
                                        throw;
                                    }
                                    this.host.logger.LogDebug("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) received packet #{seqno} ({size} bytes) {event} id={eventId}", this.host.eventHubPath, partitionId, this.Incarnation, seqno, eventDatum.Body.Count, partitionEvent, eventId);
                                    nextPacketToReceive = seqno + 1;
                                    partitionEvent.NextInputQueuePosition = nextPacketToReceive;
                                    batch.Add(partitionEvent);
                                    partitionEvent.ReceivedTimestamp = partition.CurrentTimeMs;
                                }
                                else if (seqno > nextPacketToReceive)
                                {
                                    this.host.logger.LogError("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) received wrong packet, #{seqno} instead of #{expected}", this.host.eventHubPath, partitionId, this.Incarnation, seqno, nextPacketToReceive);
                                    // this should never happen, as EventHubs guarantees in-order delivery of packets
                                    throw new InvalidOperationException("EventHubs Out-Of-Order Packet");
                                }
                                else
                                {
                                    this.host.logger.LogDebug("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) discarded packet #{seqno} because it is already processed", this.host.eventHubPath, partitionId, this.Incarnation, seqno);
                                }
                            }

                            if (batch.Count > 0 && !this.shutdownSource.IsCancellationRequested)
                            {
                                this.host.logger.LogDebug("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) received batch of {batchsize} packets, starting with #{seqno}, next expected packet is #{nextSeqno}", this.host.eventHubPath, partitionId, this.Incarnation, batch.Count, batch[0].NextInputQueuePosition - 1, nextPacketToReceive);
                                partition.SubmitExternalEvents(batch);
                            }
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    this.host.logger.LogInformation("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) was terminated", this.host.eventHubPath, partitionId, this.Incarnation);
                }
                catch (Exception exception)
                {
                    this.host.logger.LogError("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) encountered an exception while processing packets : {exception}", this.host.eventHubPath, partitionId, this.Incarnation, exception);
                    partition.ErrorHandler.HandleError("IEventProcessor.ProcessEventsAsync", "Encountered exception while processing events", exception, true, false);
                }

                this.host.logger.LogInformation("PartitionInstance {eventHubName}/{eventHubPartition}({incarnation}) ReceiverLoop exits", this.host.eventHubPath, partitionId, this.Incarnation);
            }
        }
    }
}
