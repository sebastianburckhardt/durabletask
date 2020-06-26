using DurableTask.EventSourced.EventHubs;
using Microsoft.Azure.Documents;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventSourced.TransportProviders.EventHubs
{
    class CustomConstantEventProcessorHost
    {
        public string eventHubPath;
        public string consumerGroupName;
        public string eventHubConnectionString;
        public string storageConnectionString;
        public string leaseContainerName;
        public TransportAbstraction.IHost host;
        public TransportAbstraction.ISender sender;
        public readonly EventHubsConnections connections;
        public EventHubsTransport.TaskhubParameters parameters;
        public EventHubsTraceHelper logger;
        private int numberOfPartitions;
        private List<CustomPartitionController> partitionControllers = new List<CustomPartitionController>(); 


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
        public async Task StartEventProcessing(List<Tuple<long, uint, string>> hostEvents)
        {
            Stopwatch stopwatch = new Stopwatch();
            this.logger.LogInformation("Custom EventProcessorHost {eventHubPath}--{consumerGroupName} is starting", this.eventHubPath, this.consumerGroupName);
            
            // TODO: This must also be customizable, based on how many partitions to spawn in this node
            this.numberOfPartitions = this.parameters.StartPositions.Length;

            for (var partitionIndex = 0; partitionIndex < this.numberOfPartitions; partitionIndex++)
            {
                var partitionId = Convert.ToUInt32(partitionIndex);

                // Q: Can we start all of them in parallel?
                var partitionController = new CustomPartitionController(partitionId, this);
                this.partitionControllers.Add(partitionController);
                await partitionController.StartPartitionAndLoop();

            }
            this.logger.LogDebug("Custom EventProcessorHost successfully started the ReceiverLoops.");

            // For each of the events in the HostEvents list, wait until it is time and then perform the event
            foreach (var hostEvent in hostEvents)
            {
                var waitTime = Math.Max(hostEvent.Item1 - stopwatch.ElapsedMilliseconds, 0);
                System.Threading.Thread.Sleep(Convert.ToInt32(waitTime));
                this.logger.LogWarning("Custom EventProcessorHost performs {action} for partition{partitionId} at time:{time}. Real time: {realTime}", hostEvent.Item3, hostEvent.Item2, hostEvent.Item1, stopwatch.ElapsedMilliseconds);
            }
        }

        
    }
}
