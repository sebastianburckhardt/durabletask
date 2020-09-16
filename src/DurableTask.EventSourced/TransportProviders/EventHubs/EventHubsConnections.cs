//  ----------------------------------------------------------------------------------
//  Copyright Microsoft Corporation
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express oTrace.TraceInformationr implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  ----------------------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Extensions.Logging;

namespace DurableTask.EventSourced.EventHubs
{
    internal class EventHubsConnections
    {
        private readonly TransportAbstraction.IHost host;
        private readonly string connectionString;
        private readonly EventHubsTraceHelper traceHelper;
        private readonly EventSourcedOrchestrationServiceSettings.JsonPacketUse useJsonPackets;

        public EventHubClient _partitionEventHubsClient;
        public ConcurrentDictionary<uint, EventHubClient> _clientEventHubsClients = new ConcurrentDictionary<uint, EventHubClient>();
        public ConcurrentDictionary<uint, EventHubsSender<PartitionUpdateEvent>> _partitionSenders = new ConcurrentDictionary<uint, EventHubsSender<PartitionUpdateEvent>>();
        public ConcurrentDictionary<Guid, EventHubsSender<ClientEvent>> _clientSenders = new ConcurrentDictionary<Guid, EventHubsSender<ClientEvent>>();

        public PartitionReceiver ClientReceiver { get; private set; }

        public EventHubsConnections(TransportAbstraction.IHost host, string connectionString, EventHubsTraceHelper traceHelper, EventSourcedOrchestrationServiceSettings.JsonPacketUse useJsonPackets)
        {
            this.host = host;
            this.connectionString = connectionString;
            this.traceHelper = traceHelper;
            this.useJsonPackets = useJsonPackets;
        }

        public Task StartAsync()
        {
            this._partitionEventHubsClient = GetEventHubClient(this.connectionString);
            traceHelper.LogDebug("Created Partitions Client {clientId}", _partitionEventHubsClient.ClientId);

            return Task.CompletedTask;
        }

        public const string PartitionsPath = "partitions";

        public string GetClientsPath(uint instance) { return $"clients{instance}"; }

        public const uint NumClientBuckets = 128;
        public const uint NumPartitionsPerClientPath = 32;
        public uint NumClientPaths => NumClientBuckets / NumPartitionsPerClientPath;

        public const string PartitionsConsumerGroup = "$Default";
        public const string ClientsConsumerGroup = "$Default";

        public static EventHubClient GetEventHubClient(string connectionString)
        {
            var connectionStringBuilder = new EventHubsConnectionStringBuilder(connectionString)
            {
                EntityPath = PartitionsPath
            };
            return EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());
        }

        public static async Task<long[]> GetQueuePositions(string connectionString)
        {
            // get runtime information from the eventhubs
            var partitionEventHubsClient = GetEventHubClient(connectionString);
            var info = await partitionEventHubsClient.GetRuntimeInformationAsync().ConfigureAwait(false);
            var numberPartitions = info.PartitionCount;

            // get the queue position in all the partitions 
            var infoTasks = new Task<EventHubPartitionRuntimeInformation>[numberPartitions];
            var positions = new long[numberPartitions];
            for (uint i = 0; i < numberPartitions; i++)
            {
                infoTasks[i] = partitionEventHubsClient.GetPartitionRuntimeInformationAsync(i.ToString());
            }
            for (uint i = 0; i < numberPartitions; i++)
            {
                var queueInfo = await infoTasks[i].ConfigureAwait(false);
                positions[i] = queueInfo.LastEnqueuedSequenceNumber + 1;
            }

            return positions;
        }

        public EventHubClient GetClientBucketEventHubsClient(uint clientBucket)
        {
            var clientPath = clientBucket / NumPartitionsPerClientPath;
            return _clientEventHubsClients.GetOrAdd(clientPath, (uint key) => {
                var connectionStringBuilder = new EventHubsConnectionStringBuilder(connectionString)
                {
                    EntityPath = GetClientsPath(clientPath)
                };
                var client = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());
                traceHelper.LogDebug("Created EventHub Client {clientId}", client.ClientId);
                return client;
            });
        }

        // This is to be used when EventProcessorHost is not used.
        public PartitionReceiver CreatePartitionReceiver(uint partitionId, string consumerGroupName, long nextPacketToReceive)
        {
            var client = this._partitionEventHubsClient;
            // To create a receiver we need to give it the last! packet number and not the next to receive 
            var eventPosition = EventPosition.FromSequenceNumber(nextPacketToReceive - 1);
            var partitionReceiver = client.CreateReceiver(consumerGroupName, partitionId.ToString(), eventPosition);
            traceHelper.LogDebug("Created PartitionReceiver {receiver} from {clientId} to read at {position}", partitionReceiver.ClientId, client.ClientId, nextPacketToReceive);
            return partitionReceiver;
        }

        public PartitionReceiver GetClientReceiver(Guid clientId)
        {
            uint clientBucket = Fnv1aHashHelper.ComputeHash(clientId.ToByteArray()) % NumClientBuckets;
            var client = GetClientBucketEventHubsClient(clientBucket);
            return ClientReceiver = client.CreateReceiver(ClientsConsumerGroup, (clientBucket % NumPartitionsPerClientPath).ToString(), EventPosition.FromEnd());
        }

        public EventHubsSender<PartitionUpdateEvent> GetPartitionSender(uint partitionId)
        {
            return _partitionSenders.GetOrAdd(partitionId, (key) => {
                var client = this._partitionEventHubsClient;
                var partitionSender = client.CreatePartitionSender(partitionId.ToString());
                var sender = new EventHubsSender<PartitionUpdateEvent>(
                    host,
                    partitionSender,
                    this.traceHelper,
                    this.useJsonPackets >= EventSourcedOrchestrationServiceSettings.JsonPacketUse.ForAll);
                traceHelper.LogDebug("Created PartitionSender {sender} from {clientId}", partitionSender.ClientId, client.ClientId);
                return sender;
            });
        }

        public EventHubsSender<ClientEvent> GetClientSender(Guid clientId)
        {
            return _clientSenders.GetOrAdd(clientId, (key) =>
            {
                uint clientBucket = Fnv1aHashHelper.ComputeHash(clientId.ToByteArray()) % NumClientBuckets;
                var client = GetClientBucketEventHubsClient(clientBucket);
                var partitionSender = client.CreatePartitionSender((clientBucket % NumPartitionsPerClientPath).ToString());
                var sender = new EventHubsSender<ClientEvent>(
                    host,
                    partitionSender,
                    this.traceHelper,
                    this.useJsonPackets >= EventSourcedOrchestrationServiceSettings.JsonPacketUse.ForClients);
                traceHelper.LogDebug("Created ResponseSender {sender} from {clientId}", partitionSender.ClientId, client.ClientId);
                return sender;
            });
        }

        public async Task Close()
        {
            if (ClientReceiver != null)
            {
                traceHelper.LogDebug("Closing Client Receiver");
                await ClientReceiver.CloseAsync().ConfigureAwait(false);
            }

            traceHelper.LogDebug($"Closing Client Bucket Clients");
            await Task.WhenAll(_clientEventHubsClients.Values.Select(s => s.CloseAsync()).ToList()).ConfigureAwait(false);

            traceHelper.LogDebug("Closing Partitions Client {clientId}", _partitionEventHubsClient.ClientId);
            await _partitionEventHubsClient.CloseAsync().ConfigureAwait(false);
        }
     }
}
