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

        private object thisLock = new object();

        public EventHubClient _partitionEventHubsClient;
        public Dictionary<uint, EventHubClient> _clientEventHubsClients = new Dictionary<uint, EventHubClient>();
        public Dictionary<uint, EventHubsSender<PartitionUpdateEvent>> _partitionSenders = new Dictionary<uint, EventHubsSender<PartitionUpdateEvent>>();
        public Dictionary<uint, PartitionReceiver> _partitionReceivers = new Dictionary<uint, PartitionReceiver>();
        public Dictionary<Guid, EventHubsSender<ClientEvent>> _clientSenders = new Dictionary<Guid, EventHubsSender<ClientEvent>>();

        public PartitionReceiver ClientReceiver { get; private set; }

        public EventHubsConnections(TransportAbstraction.IHost host, string connectionString, EventHubsTraceHelper traceHelper)
        {
            this.host = host;
            this.connectionString = connectionString;
            this.traceHelper = traceHelper;
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

        public EventHubClient GetPartitionEventHubsClient()
        {
            lock (thisLock)
            {
                if (_partitionEventHubsClient == null)
                {
                    _partitionEventHubsClient = GetEventHubClient(this.connectionString);
                    traceHelper.LogDebug("Created Partitions Client {clientId}", _partitionEventHubsClient.ClientId);
                }
                return _partitionEventHubsClient;
            }
        }

        public EventHubClient GetClientBucketEventHubsClient(uint clientBucket)
        {
            lock (_clientEventHubsClients)
            {
                var clientPath = clientBucket / NumPartitionsPerClientPath;
                if (!_clientEventHubsClients.TryGetValue(clientPath, out var client))
                {
                    var connectionStringBuilder = new EventHubsConnectionStringBuilder(connectionString)
                    {
                        EntityPath = GetClientsPath(clientPath)
                    };
                    _clientEventHubsClients[clientPath] = client = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());
                    traceHelper.LogDebug("Created EventHub Client {clientId}", client.ClientId);

                }
                return client;
            }
        }

        // This is to be used when EventProcessorHost is not used.
        public PartitionReceiver CreatePartitionReceiver(uint partitionId, string consumerGroupName, long nextPacketToReceive)
        {
            var client = GetPartitionEventHubsClient();
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
            lock (_partitionSenders) // TODO optimize using array, and lock on slow path only
            {
                if (!_partitionSenders.TryGetValue(partitionId, out var sender))
                {
                    var client = GetPartitionEventHubsClient();
                    var partitionSender = client.CreatePartitionSender(partitionId.ToString());
                    _partitionSenders[partitionId] = sender = new EventHubsSender<PartitionUpdateEvent>(host, partitionSender, this.traceHelper);
                    traceHelper.LogDebug("Created PartitionSender {sender} from {clientId}", partitionSender.ClientId, client.ClientId);
                }
                return sender;
            }
        }

        public EventHubsSender<ClientEvent> GetClientSender(Guid clientId)
        {
            lock (_clientSenders) // TODO optimize using array, and lock on slow path only
            {
                if (!_clientSenders.TryGetValue(clientId, out var sender))
                {
                    uint clientBucket = Fnv1aHashHelper.ComputeHash(clientId.ToByteArray()) % NumClientBuckets;
                    var client = GetClientBucketEventHubsClient(clientBucket);
                    var partitionSender = client.CreatePartitionSender((clientBucket % NumPartitionsPerClientPath).ToString());
                    _clientSenders[clientId] = sender = new EventHubsSender<ClientEvent>(host, partitionSender, this.traceHelper);
                    traceHelper.LogDebug("Created ResponseSender {sender} from {clientId}", partitionSender.ClientId, client.ClientId);
                }
                return sender;
            }
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

            if (_partitionEventHubsClient != null)
            {
                traceHelper.LogDebug("Closing Partitions Client {clientId}", _partitionEventHubsClient.ClientId);
                await _partitionEventHubsClient.CloseAsync().ConfigureAwait(false);
            }
        }


     }
}
