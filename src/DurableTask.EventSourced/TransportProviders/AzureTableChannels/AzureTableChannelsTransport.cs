//  ----------------------------------------------------------------------------------
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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.Cosmos.Table;

namespace DurableTask.EventSourced.AzureTableChannels
{
    internal class AzureTableChannelsTransport : TransportAbstraction.ITaskHub
    {
        private readonly TransportAbstraction.IHost host;
        private readonly EventSourcedOrchestrationServiceSettings settings;
        private readonly uint numberPartitions;

        private TransportAbstraction.IClient client;
        private Transport[] partitionTransports;
        private StorageAbstraction.IPartitionState[] partitionStates;
        private TransportAbstraction.IPartition[] partitions;
        private CancellationTokenSource shutdownTokenSource;
        private CloudStorageAccount account;

        private static readonly TimeSpan simulatedDelay = TimeSpan.FromMilliseconds(1);

        public AzureTableChannelsTransport(TransportAbstraction.IHost host, EventSourcedOrchestrationServiceSettings settings, ILoggerFactory loggerFactory)
        {
            this.host = host;
            TransportConnectionString.Parse(settings.EventHubsConnectionString, out _, out _, out var numberPartitions);
            this.settings = settings;
            this.numberPartitions = (uint)numberPartitions.Value;
            this.account = CloudStorageAccount.Parse(this.settings.StorageConnectionString);
        }

        async Task TransportAbstraction.ITaskHub.CreateAsync()
        {
            await Task.Delay(simulatedDelay).ConfigureAwait(false);
            this.partitionTransports = new Transport[this.numberPartitions]; // TODO use number hosts instead
            this.partitionStates = new StorageAbstraction.IPartitionState[this.numberPartitions];
            this.partitions = new TransportAbstraction.IPartition[this.numberPartitions];
        }

        async Task TransportAbstraction.ITaskHub.DeleteAsync()
        {
            await Task.Delay(simulatedDelay).ConfigureAwait(false);
            this.partitionTransports = null;
        }

        async Task<bool> TransportAbstraction.ITaskHub.ExistsAsync()
        {
            await Task.Delay(simulatedDelay).ConfigureAwait(false);
            return this.partitionTransports != null;
        }

        async Task TransportAbstraction.ITaskHub.StartAsync()
        {
            this.shutdownTokenSource = new CancellationTokenSource();

            this.host.NumberPartitions = this.numberPartitions;
            var creationTimestamp = DateTime.UtcNow;
            var startPositions = new long[numberPartitions];

            var clientId = Guid.NewGuid();

            var tableClient = this.account.CreateCloudTableClient();

            // create all partitions
            for (uint i = 0; i < this.numberPartitions; i++)
            {
                var partitionState = partitionStates[i] = this.host.StorageProvider.CreatePartitionState();

                var partitionTransport = this.partitionTransports[i] = new Transport(
                    this.settings,
                    this.shutdownTokenSource.Token,
                    "TaskHub",
                    i,
                    partitionState,
                    clientId,
                    tableClient);

                if (i == 0)
                {
                    await partitionTransport.CreateTableIfNotExistAsync().ConfigureAwait(false);
                }

                var partition = partitions[i] = this.host.AddPartition(i, partitionTransport.PartitionSender);

                await partition.CreateOrRestoreAsync(this.host.CreateErrorHandler(i), 0).ConfigureAwait(false);
            }

            // create a client
            this.client = this.host.AddClient(clientId, partitionTransports[0].ClientSender);

            // start the receive loops
            for (uint i = 0; i < numberPartitions; i++)
            {
                // TODO recovery sets last received

                var transport = partitionTransports[i];
                var partition = this.partitions[i];
                var receiveLoop = Task.Run(() => transport.ReceiveLoopAsync(partition, this.client));
            }
        }

        async Task TransportAbstraction.ITaskHub.StopAsync()
        {
            if (this.shutdownTokenSource != null)
            {
                this.shutdownTokenSource.Cancel();
                this.shutdownTokenSource = null;

                await this.client.StopAsync().ConfigureAwait(false);
                await Task.WhenAll(this.partitionStates.Select(partitionState => partitionState.CleanShutdown(this.settings.TakeStateCheckpointWhenStoppingPartition))).ConfigureAwait(false);
            }
        }

        public static Task<long[]> GetQueuePositions(string storageConnectionString)
        {
            throw new NotImplementedException();
        }
    }
}