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
        private Stopwatch stopwatch;


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


        // TODO: Refactor for host events to be a class and be parse-able from a string or json
        public async Task StartEventProcessing(List<Tuple<long, List<Tuple<uint, string>>>> timesteps)
        {
            this.stopwatch = new Stopwatch();
            this.stopwatch.Start();
            this.logger.LogInformation("Custom EventProcessorHost {eventHubPath}--{consumerGroupName} is starting", this.eventHubPath, this.consumerGroupName);
            
            // TODO: This must also be customizable, based on how many partitions to spawn in this node
            this.numberOfPartitions = this.parameters.StartPositions.Length;

            for (var partitionIndex = 0; partitionIndex < this.numberOfPartitions; partitionIndex++)
            {
                var partitionId = Convert.ToUInt32(partitionIndex);
                var partitionController = new CustomPartitionController(partitionId, this);
                this.partitionControllers.Add(partitionController);
            }
            this.logger.LogDebug("Custom EventProcessorHost successfully initialized.");

            // For each of the events in the HostEvents list, wait until it is time and then perform the event
            foreach (var timestep in timesteps)
            {
                var timestepTime = timestep.Item1;
                var waitTime = Math.Max(timestepTime - this.stopwatch.ElapsedMilliseconds, 0);
                this.logger.LogDebug("Custom EventProcessorHost will wait for {milliseconds} ms until next hostEvent", waitTime);
                System.Threading.Thread.Sleep(Convert.ToInt32(waitTime));
                await ProcessHostEvents(timestepTime, timestep.Item2);
            }
        }

        private async Task ProcessHostEvents(long timestepTime, List<Tuple<uint, string>> hostEvents)
        {
            this.logger.LogWarning("Custom EventProcessorHost performs actions at time:{time}. Real time: {realTime}", timestepTime, this.stopwatch.ElapsedMilliseconds);

            var tasks = new List<Task>();
            foreach (var hostEvent in hostEvents)
            {
                // Assumption: All the events in the same timestep need to refer to different partitionIds
                var task = ProcessHostEvent(timestepTime, hostEvent);
                tasks.Add(task);
            }
            await Task.WhenAll(tasks);
            // TODO: Catch any errors
            this.logger.LogWarning("Custom EventProcessorHost successfully performed actions at time:{time}. Real time: {realTime}", timestepTime, this.stopwatch.ElapsedMilliseconds);

        }

        private async Task ProcessHostEvent(long timestepTime, Tuple<uint, string> hostEvent)
        {
            var action = hostEvent.Item2;
            var partitionId = hostEvent.Item1;
            this.logger.LogWarning("Custom EventProcessorHost performs {action} for partition{partitionId} at time:{time}. Real time: {realTime}", action, partitionId, timestepTime, this.stopwatch.ElapsedMilliseconds);
            if (action == "restart")
            {
                var partitionController = this.partitionControllers[Convert.ToInt32(partitionId)];
                await partitionController.StopPartitionAndLoop();
                await partitionController.StartPartitionAndLoop();
            }
            else if (action == "start")
            {
                var partitionController = this.partitionControllers[Convert.ToInt32(partitionId)];
                await partitionController.StartPartitionAndLoop();
            }
            else
            {
                this.logger.LogError("Custom EventProcessorHost failed on action: {action} for partition{partitionId} at time:{time}. Real time: {realTime}", action, partitionId, timestepTime, this.stopwatch.ElapsedMilliseconds);
                throw new InvalidOperationException($"Custom EventProcessorHost cannot perform hostEvent with action: {action}");
            }
            this.logger.LogWarning("Custom EventProcessorHost successfully performed {action} for partition{partitionId} at time:{time}. Real time: {realTime}", action, partitionId, timestepTime, this.stopwatch.ElapsedMilliseconds);
        }
    }
}
