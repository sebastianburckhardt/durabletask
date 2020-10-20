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
using Microsoft.Extensions.Logging;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using DurableTask.EventSourced.TransportProviders.EventHubs;
using System.Xml;
using Microsoft.OData.UriParser;

namespace DurableTask.EventSourced.EventHubs
{
    internal class EventHubsTransport :
        TransportAbstraction.ITaskHub,
        IEventProcessorFactory,
        TransportAbstraction.ISender
    {
        private readonly TransportAbstraction.IHost host;
        private readonly EventSourcedOrchestrationServiceSettings settings;
        private readonly EventHubsConnections connections;
        private readonly CloudStorageAccount cloudStorageAccount;
        private readonly EventHubsTraceHelper traceHelper;

        private EventProcessorHost eventProcessorHost;
        private TransportAbstraction.IClient client;

        private TaskhubParameters parameters;

        private Task clientEventLoopTask = Task.CompletedTask;
        private CancellationTokenSource shutdownSource;

        private CloudBlobContainer cloudBlobContainer;
        private CloudBlockBlob taskhubParameters;
        private CloudBlockBlob partitionScript;
        private ScriptedEventProcessorHost customEventProcessorHost;

        public Guid ClientId { get; private set; }

        public EventHubsTransport(TransportAbstraction.IHost host, EventSourcedOrchestrationServiceSettings settings, ILoggerFactory loggerFactory)
        {
            this.host = host;
            this.settings = settings;
            this.cloudStorageAccount = CloudStorageAccount.Parse(this.settings.StorageConnectionString);
            string namespaceName = TransportConnectionString.EventHubsNamespaceName(settings.EventHubsConnectionString);
            this.traceHelper = new EventHubsTraceHelper(loggerFactory, settings.TransportLogLevelLimit, this.cloudStorageAccount.Credentials.AccountName, settings.HubName, namespaceName);
            this.ClientId = Guid.NewGuid();
            this.connections = new EventHubsConnections(settings.EventHubsConnectionString, EventHubsTransport.PartitionHubs, EventHubsTransport.ClientHubs)
            {
                Host = host,
                TraceHelper = this.traceHelper,
                UseJsonPackets = settings.UseJsonPackets,
            };
            var blobContainerName = GetContainerName(settings.HubName);
            var cloudBlobClient = cloudStorageAccount.CreateCloudBlobClient();
            this.cloudBlobContainer = cloudBlobClient.GetContainerReference(blobContainerName);
            this.taskhubParameters = cloudBlobContainer.GetBlockBlobReference("taskhubparameters.json");
            this.partitionScript = cloudBlobContainer.GetBlockBlobReference("partitionscript.json");
        }

        // these are hardcoded now but we may turn them into settings
        public static string[] PartitionHubs = { "partitions" };
        public static string[] ClientHubs = { "clients0", "clients1", "clients2", "clients3" };
        public static string PartitionConsumerGroup = "$Default";
        public static string ClientConsumerGroup = "$Default";

        private static string GetContainerName(string taskHubName) => taskHubName.ToLowerInvariant() + "-storage";

        private async Task<TaskhubParameters> TryLoadExistingTaskhubAsync()
        {
            // try load the taskhub parameters
            try
            {
                var jsonText = await this.taskhubParameters.DownloadTextAsync().ConfigureAwait(false);
                return  JsonConvert.DeserializeObject<TaskhubParameters>(jsonText);
            }
            catch (StorageException ex) when (ex.RequestInformation.HttpStatusCode == 404)
            {
                return null;
            }
        }

        async Task<bool> TransportAbstraction.ITaskHub.ExistsAsync()
        {
            var parameters = await TryLoadExistingTaskhubAsync().ConfigureAwait(false);
            return (parameters != null && parameters.TaskhubName == this.settings.HubName);
        }

        async Task TransportAbstraction.ITaskHub.CreateAsync()
        {
            await this.cloudBlobContainer.CreateIfNotExistsAsync().ConfigureAwait(false);

            if (await TryLoadExistingTaskhubAsync().ConfigureAwait(false) != null)
            {
                throw new InvalidOperationException("Cannot create TaskHub: TaskHub already exists");
            }

            long[] startPositions = await EventHubsConnections.GetQueuePositions(this.settings.EventHubsConnectionString, EventHubsTransport.PartitionHubs);

            var taskHubParameters = new TaskhubParameters()
            {
                TaskhubName = settings.HubName,
                TaskhubGuid = Guid.NewGuid(),
                CreationTimestamp = DateTime.UtcNow,
                PartitionHubs = EventHubsTransport.PartitionHubs,
                ClientHubs = EventHubsTransport.ClientHubs,
                PartitionConsumerGroup = EventHubsTransport.PartitionConsumerGroup,
                ClientConsumerGroup = EventHubsTransport.ClientConsumerGroup,
                StartPositions = startPositions
            };

            // save the taskhub parameters in a blob
            var jsonText = JsonConvert.SerializeObject(
                taskHubParameters,
                Newtonsoft.Json.Formatting.Indented,
                new JsonSerializerSettings() { TypeNameHandling = TypeNameHandling.None });
            await this.taskhubParameters.UploadTextAsync(jsonText).ConfigureAwait(false);
        }
      
        async Task TransportAbstraction.ITaskHub.DeleteAsync()
        {
            if (await this.taskhubParameters.ExistsAsync().ConfigureAwait(false))
            {
                await BlobUtils.ForceDeleteAsync(this.taskhubParameters).ConfigureAwait(false);
            }

            // todo delete consumption checkpoints
            await this.host.StorageProvider.DeleteAllPartitionStatesAsync().ConfigureAwait(false);
        }

        async Task TransportAbstraction.ITaskHub.StartAsync()
        {
            this.shutdownSource = new CancellationTokenSource();

            // load the taskhub parameters
            var jsonText = await this.taskhubParameters.DownloadTextAsync().ConfigureAwait(false);
            this.parameters = JsonConvert.DeserializeObject<TaskhubParameters>(jsonText);

            // check that we are the correct taskhub!
            if (this.parameters.TaskhubName != this.settings.HubName)
            {
                throw new InvalidOperationException($"The specified taskhub name does not match the task hub name in {taskhubParameters.Name}");
            }
     
            this.host.NumberPartitions = (uint) this.parameters.StartPositions.Length;

            await this.connections.StartAsync();

            this.client = host.AddClient(this.ClientId, this.parameters.TaskhubGuid, this);

            this.clientEventLoopTask = Task.Run(this.ClientEventLoop);

            if (PartitionHubs.Length > 1)
            {
                throw new NotSupportedException("Using multiple eventhubs for partitions is not yet supported.");
            }

            string partitionsHub = PartitionHubs[0];

            // Use standard eventProcessor offered by EventHubs or a custom one
            if (this.settings.EventProcessorManagement == "EventHubs")
            {

                this.eventProcessorHost = new EventProcessorHost(
                        partitionsHub,
                        EventHubsTransport.PartitionConsumerGroup,
                        settings.EventHubsConnectionString,
                        settings.StorageConnectionString,
                        cloudBlobContainer.Name);

                var processorOptions = new EventProcessorOptions()
                {
                    InitialOffsetProvider = (s) => EventPosition.FromSequenceNumber(this.parameters.StartPositions[int.Parse(s)] - 1),
                    MaxBatchSize = 300,
                    PrefetchCount = 500,
                };

                await eventProcessorHost.RegisterEventProcessorFactoryAsync(this, processorOptions).ConfigureAwait(false);
            }
            else if (this.settings.EventProcessorManagement.StartsWith("Custom"))
            {
                this.traceHelper.LogWarning($"EventProcessorManagement: {this.settings.EventProcessorManagement}");
                this.customEventProcessorHost = new ScriptedEventProcessorHost(
                        partitionsHub,
                        EventHubsTransport.PartitionConsumerGroup,
                        settings.EventHubsConnectionString,
                        settings.StorageConnectionString,
                        cloudBlobContainer.Name,
                        this.host, 
                        this, 
                        this.connections,
                        this.parameters, 
                        this.settings,
                        this.traceHelper,
                        settings.WorkerId);

                var thread = new Thread(() => this.customEventProcessorHost.StartEventProcessing(settings, this.partitionScript));
                thread.Name = "ScriptedEventProcessorHost";
                thread.Start();
            }
            else
            {
                throw new InvalidOperationException("Unknown EventProcessorManagement setting!");
            }
        }

        async Task TransportAbstraction.ITaskHub.StopAsync()
        {
            this.parameters = null;
            this.traceHelper.LogInformation("Shutting down EventHubsBackend");
            this.traceHelper.LogDebug("Stopping client event loop");
            this.shutdownSource.Cancel();
            this.traceHelper.LogDebug("Stopping client");
            await client.StopAsync().ConfigureAwait(false);
            this.traceHelper.LogDebug("Unregistering event processor");
            if (this.settings.EventProcessorManagement == "EventHubs")
                await this.eventProcessorHost.UnregisterEventProcessorAsync().ConfigureAwait(false);
            else if (this.settings.EventProcessorManagement.StartsWith("Custom"))
                throw new NotImplementedException("Custom eventhubs stopping not yet implemented");
            else
                throw new InvalidOperationException("Unknown EventProcessorManagement setting!");
            this.traceHelper.LogDebug("Closing connections");
            await this.connections.StopAsync().ConfigureAwait(false);
            this.traceHelper.LogInformation("EventHubsBackend shutdown completed");
        }

        IEventProcessor IEventProcessorFactory.CreateEventProcessor(PartitionContext partitionContext)
        {
            var processor = new EventHubsProcessor(this.host, this, this.parameters, partitionContext, this.settings, this.traceHelper);
            return processor;
        }

        void TransportAbstraction.ISender.Submit(Event evt)
        {
            switch (evt)
            {
                case ClientEvent clientEvent:
                    var clientId = clientEvent.ClientId;
                    var clientSender = this.connections.GetClientSender(clientEvent.ClientId, this.parameters.TaskhubGuid.ToByteArray());
                    clientSender.Submit(clientEvent);
                    break;

                case PartitionEvent partitionEvent:
                    var partitionId = partitionEvent.PartitionId;
                    var partitionSender = this.connections.GetPartitionSender((int) partitionId, this.parameters.TaskhubGuid.ToByteArray());
                    partitionSender.Submit(partitionEvent);
                    break;

                default:
                    throw new InvalidCastException("could not cast to neither PartitionReadEvent nor PartitionUpdateEvent");
            }
        }

        private async Task ClientEventLoop()
        {
            var clientReceiver = this.connections.CreateClientReceiver(this.ClientId, EventHubsTransport.ClientConsumerGroup);
            var receivedEvents = new List<ClientEvent>();

            byte[] taskHubGuid = this.parameters.TaskhubGuid.ToByteArray();

            while (!this.shutdownSource.IsCancellationRequested)
            {
                IEnumerable<EventData> eventData = await clientReceiver.ReceiveAsync(1000, TimeSpan.FromMinutes(1)).ConfigureAwait(false);

                if (eventData != null)
                {
                    foreach (var ed in eventData)
                    {
                        ClientEvent clientEvent = null;

                        try
                        {
                            Packet.Deserialize(ed.Body, out clientEvent, taskHubGuid);

                            if (clientEvent != null && clientEvent.ClientId == this.ClientId)
                            {
                                receivedEvents.Add(clientEvent);
                            }
                        }
                        catch (Exception)
                        {
                            this.traceHelper.LogError("EventProcessor for Client{clientId} could not deserialize packet #{seqno} ({size} bytes)", Client.GetShortId(this.ClientId), ed.SystemProperties.SequenceNumber, ed.Body.Count);
                            throw;
                        }
                        this.traceHelper.LogDebug("EventProcessor for Client{clientId} received packet #{seqno} ({size} bytes)", Client.GetShortId(this.ClientId), ed.SystemProperties.SequenceNumber, ed.Body.Count);

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
