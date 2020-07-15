using DurableTask.Core.Common;
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
        private List<PartitionInstance> partitionInstances = new List<PartitionInstance>();
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

        public List<ProcessorHostTimestep> ParseInput(String inputString)
        {
            // TODO: Make this automatic
            // TODO: Make this be a string(or json) here, and the parsing to happen in the custom host
            var timesteps = new List<ProcessorHostTimestep>();
            var startingEvents = new List<ProcessorHostEvent>();
            var partitionNumber = this.parameters.StartPositions.Length;
            for (var partitionIndex = 0; partitionIndex < partitionNumber; partitionIndex++)
            {
                startingEvents.Add(new ProcessorHostEvent(Convert.ToUInt32(partitionIndex), "start"));
            }
            timesteps.Add(new ProcessorHostTimestep(0, startingEvents));
            var restartingEvent = new List<ProcessorHostEvent>();
            restartingEvent.Add(new ProcessorHostEvent(0, "restart"));
            timesteps.Add(new ProcessorHostTimestep(30000, restartingEvent));
            timesteps.Add(new ProcessorHostTimestep(60000, restartingEvent));
            timesteps.Add(new ProcessorHostTimestep(90000, restartingEvent));
            timesteps.Add(new ProcessorHostTimestep(120000, restartingEvent));
            timesteps.Add(new ProcessorHostTimestep(150000, restartingEvent));
            timesteps.Add(new ProcessorHostTimestep(180000, restartingEvent));
            timesteps.Add(new ProcessorHostTimestep(210000, restartingEvent));
            timesteps.Add(new ProcessorHostTimestep(240000, restartingEvent));
            return timesteps;
        }

        /// <summary>
        /// This represents events that the CustomProcessorHost can handle, e.g. starting, stopping, restarting a partition.
        /// </summary>
        public class ProcessorHostEvent
        {
            public uint partitionId;
            
            // TODO: Make this an enum
            public string action;

            public ProcessorHostEvent(uint partitionId, string action)
            {
                this.partitionId = partitionId;
                this.action = action;
            }
        }

        public class ProcessorHostTimestep
        {
            public long time;
            public List<ProcessorHostEvent> events;

            public ProcessorHostTimestep(long time, List<ProcessorHostEvent> events)
            {
                this.time = time;
                this.events = events;
            }
        }

        // TODO: Refactor for host events to be a class and be parse-able from a string or json
        public async Task StartEventProcessing(String inputString)
        {
            List<ProcessorHostTimestep> timesteps = this.ParseInput(inputString);

            this.stopwatch = new Stopwatch();
            this.stopwatch.Start();
            this.logger.LogInformation("Custom EventProcessorHost {eventHubPath}--{consumerGroupName} is starting", this.eventHubPath, this.consumerGroupName);
            
            // TODO: This must also be customizable, based on how many partitions to spawn in this node
            this.numberOfPartitions = this.parameters.StartPositions.Length;

            for (var partitionIndex = 0; partitionIndex < this.numberOfPartitions; partitionIndex++)
            {
                var partitionId = Convert.ToUInt32(partitionIndex);
                var partitionController = new PartitionInstance(partitionId, 0, this);
                this.partitionInstances.Add(partitionController);
            }
            this.logger.LogDebug("Custom EventProcessorHost successfully initialized.");

            // For each of the events in the HostEvents list, wait until it is time and then perform the event
            foreach (var timestep in timesteps)
            {
                var timestepTime = timestep.time;
                var waitTime = Math.Max(timestepTime - this.stopwatch.ElapsedMilliseconds, 0);
                this.logger.LogDebug("Custom EventProcessorHost will wait for {milliseconds} ms until next hostEvent", waitTime);
                System.Threading.Thread.Sleep(Convert.ToInt32(waitTime));
                await ProcessHostEvents(timestep);
            }
        }

        private async Task ProcessHostEvents(ProcessorHostTimestep timestep)
        {
            var timestepTime = timestep.time;
            var hostEvents = timestep.events;
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

        private async Task ProcessHostEvent(long timestepTime, ProcessorHostEvent hostEvent)
        {
            var action = hostEvent.action;
            int partitionId = (int) hostEvent.partitionId;
            this.logger.LogWarning("Custom EventProcessorHost performs {action} for partition{partitionId} at time:{time}. Real time: {realTime}", action, partitionId, timestepTime, this.stopwatch.ElapsedMilliseconds);
            try
            {
                if (action == "restart")
                {
                    var oldPartitionInstance = this.partitionInstances[partitionId];
                    var newPartitionInstance = new PartitionInstance(hostEvent.partitionId, oldPartitionInstance.Incarnation + 1, this);
                    this.partitionInstances[partitionId] = newPartitionInstance;
                    await Task.WhenAll(newPartitionInstance.StartAsync(), oldPartitionInstance.StopAsync());
                }
                else if (action == "start")
                {
                    var partitionInstance = this.partitionInstances[partitionId];
                    await partitionInstance.StartAsync();
                }
                else
                {
                    this.logger.LogError("Custom EventProcessorHost cannot perform action:{action} for partition{partitionId} at time:{time}. Real time: {realTime}", action, partitionId, timestepTime, this.stopwatch.ElapsedMilliseconds);
                    //throw new InvalidOperationException($"Custom EventProcessorHost cannot perform hostEvent with action: {action}");
                }
                this.logger.LogWarning("Custom EventProcessorHost successfully performed {action} for partition{partitionId} at time:{time}. Real time: {realTime}", action, partitionId, timestepTime, this.stopwatch.ElapsedMilliseconds);
            }
            catch (Exception e) when (!Utils.IsFatal(e))
            {
                // TODO: Maybe in the future we would like to actually do something in case of failure. 
                //       For now it is fine to ignore them.
                this.logger.LogError("Custom EventProcessorHost failed on action: {action} for partition{partitionId} at time:{time}. Real time: {realTime}", action, partitionId, timestepTime, this.stopwatch.ElapsedMilliseconds);
            }
        }
    }
}
