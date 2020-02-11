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
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;
using Microsoft.Azure.Storage.Blob.Protocol;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace DurableTask.EventSourced.EventHubs
{
    internal class EventHubsTransport :
        TransportAbstraction.ITaskHub,
        IEventProcessorFactory,
        TransportAbstraction.ISender
    {
        private readonly TransportAbstraction.IHost host;
        private readonly string HostId;
        private readonly EventSourcedOrchestrationServiceSettings settings;
        private readonly EventHubsConnections connections;

        private EventProcessorHost eventProcessorHost;
        private TransportAbstraction.IClient client;

        private TaskhubParameters parameters;

        private CancellationTokenSource shutdownSource;

        private CloudBlobContainer cloudBlobContainer;
        private CloudBlockBlob taskhubParameters;

        private const int MaxReceiveBatchSize = 10000; // actual batches will always be much smaller

        public Guid ClientId { get; private set; }

        public EventHubsTransport(TransportAbstraction.IHost host, EventSourcedOrchestrationServiceSettings settings)
        {
            this.host = host;
            this.settings = settings;
            this.HostId = $"{Environment.MachineName}-{DateTime.UtcNow:o}";
            this.ClientId = Guid.NewGuid();
            this.connections = new EventHubsConnections(host, settings.EventHubsConnectionString);
            var blobContainerName = $"{settings.EventHubsNamespaceName}-processors";
            var cloudStorageAccount = CloudStorageAccount.Parse(this.settings.StorageConnectionString);
            var cloudBlobClient = cloudStorageAccount.CreateCloudBlobClient();
            this.cloudBlobContainer = cloudBlobClient.GetContainerReference(blobContainerName);
            this.taskhubParameters = cloudBlobContainer.GetBlockBlobReference("taskhubparameters.json");
        }

        private async Task<TaskhubParameters> TryLoadExistingTaskhubAsync()
        {
            // try load the taskhub parameters
            try
            {
                var jsonText = await this.taskhubParameters.DownloadTextAsync();
                return  JsonConvert.DeserializeObject<TaskhubParameters>(jsonText);
            }
            catch (StorageException ex) when (ex.RequestInformation.HttpStatusCode == 404)
            {
                return null;
            }
        }

        async Task<bool> TransportAbstraction.ITaskHub.ExistsAsync()
        {
            var parameters = await TryLoadExistingTaskhubAsync();
            return (parameters != null && parameters.TaskhubName == this.settings.TaskHubName);
        }

        async Task TransportAbstraction.ITaskHub.CreateAsync()
        {
            await this.cloudBlobContainer.CreateIfNotExistsAsync();

            if (await TryLoadExistingTaskhubAsync() != null)
            {
                throw new InvalidOperationException("Cannot create TaskHub: Only one TaskHub is allowed per EventHub");
            }

            // get runtime information from the eventhubs
            var partitionEventHubsClient = this.connections.GetPartitionEventHubsClient();
            var info = await partitionEventHubsClient.GetRuntimeInformationAsync();
            var numberPartitions = info.PartitionCount;

            // check the current queue position in all the partitions 
            var infoTasks = new Task<EventHubPartitionRuntimeInformation>[numberPartitions];
            var startPositions = new long[numberPartitions];
            for (uint i = 0; i < numberPartitions; i++)
            {
                infoTasks[i] = partitionEventHubsClient.GetPartitionRuntimeInformationAsync(i.ToString());
            }
            for (uint i = 0; i < numberPartitions; i++)
            {
                var queueInfo = await infoTasks[i];
                startPositions[i] = queueInfo.LastEnqueuedSequenceNumber + 1;
            }

            var taskHubParameters = new
            {
                TaskhubName = settings.TaskHubName,
                CreationTimestamp = DateTime.UtcNow,
                StartPositions = startPositions
            };

            // save the taskhub parameters in a blob
            var jsonText = JsonConvert.SerializeObject(
                taskHubParameters,
                Formatting.Indented,
                new JsonSerializerSettings() { TypeNameHandling = TypeNameHandling.None });
            await this.taskhubParameters.UploadTextAsync(jsonText);
        }

        [DataContract]
        public class TaskhubParameters
        {
            [DataMember]
            public string TaskhubName { get; set; }

            [DataMember]
            public DateTime CreationTimestamp { get; set; }

            [DataMember]
            public long[] StartPositions { get; set; }
        }

        async Task TransportAbstraction.ITaskHub.DeleteAsync()
        {
            await this.taskhubParameters.DeleteIfExistsAsync();
            // todo delete consumption checkpoints
            await this.host.StorageProvider.DeleteAllPartitionStatesAsync();
        }

        async Task TransportAbstraction.ITaskHub.StartAsync()
        {
            this.shutdownSource = new CancellationTokenSource();

            // load the taskhub parameters
            var jsonText = await this.taskhubParameters.DownloadTextAsync();
            this.parameters = JsonConvert.DeserializeObject<TaskhubParameters>(jsonText);

            // check that we are the correct taskhub!
            if (this.parameters.TaskhubName != this.settings.TaskHubName)
            {
                throw new InvalidOperationException("Only one taskhub is allowed per EventHub");
            }

            this.host.NumberPartitions = (uint) this.parameters.StartPositions.Length;

            this.client = host.AddClient(this.ClientId, this);

            var clientEventLoopTask = this.ClientEventLoop();

            this.eventProcessorHost = new EventProcessorHost(
                 EventHubsConnections.PartitionsPath,
                 EventHubsConnections.PartitionsConsumerGroup,
                 settings.EventHubsConnectionString,
                 settings.StorageConnectionString,
                 cloudBlobContainer.Name);

            var processorOptions = new EventProcessorOptions()
            {
                InitialOffsetProvider = (s) => EventPosition.FromSequenceNumber(this.parameters.StartPositions[int.Parse(s)] - 1),
                MaxBatchSize = MaxReceiveBatchSize,
            };

            await eventProcessorHost.RegisterEventProcessorFactoryAsync(this, processorOptions);
        }

        async Task TransportAbstraction.ITaskHub.StopAsync()
        {
            System.Diagnostics.Trace.TraceInformation("Shutting down EventHubsBackend");
            await client.StopAsync();
            this.shutdownSource.Cancel();
            await this.eventProcessorHost.UnregisterEventProcessorAsync();
            await this.connections.Close();
        }

        IEventProcessor IEventProcessorFactory.CreateEventProcessor(PartitionContext context)
        {
            var processor = new EventProcessor(this.host, this, this.settings, this.parameters);
            return processor;
        }

        void TransportAbstraction.ISender.Submit(Event evt)
        {
            if (evt is ClientEvent clientEvent)
            {
                var clientId = clientEvent.ClientId;
                var sender = this.connections.GetClientSender(clientEvent.ClientId);
                sender.Submit(clientEvent);
            }
            else if (evt is PartitionEvent partitionEvent)
            {
                var partitionId = partitionEvent.PartitionId;
                var sender = this.connections.GetPartitionSender(partitionId);
                sender.Submit(partitionEvent);
            }
        }

        private async Task ClientEventLoop()
        {
            var clientReceiver = this.connections.GetClientReceiver(this.ClientId);
            var receivedEvents = new List<ClientEvent>();

            while (!this.shutdownSource.IsCancellationRequested)
            {
                IEnumerable<EventData> eventData = await clientReceiver.ReceiveAsync(MaxReceiveBatchSize, TimeSpan.FromMinutes(1));

                if (eventData != null)
                {
                    foreach (var ed in eventData)
                    {
                        var clientEvent = (ClientEvent)Serializer.DeserializeEvent(ed.Body);

                        if (clientEvent.ClientId == this.ClientId)
                        {
                            receivedEvents.Add(clientEvent);
                        }
                    }

                    if (receivedEvents.Count > 0)
                    {
                        foreach (var evt in receivedEvents)
                        {
                            client.Process(evt);
                        }
                        receivedEvents.Clear();
                    }
                }
            }
        }
    }
}
