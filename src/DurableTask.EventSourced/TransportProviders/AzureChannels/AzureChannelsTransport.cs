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
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DurableTask.EventSourced.Faster;
using Microsoft.WindowsAzure.Storage;

namespace DurableTask.EventSourced.AzureChannels
{
    internal class AzureChannelsTransport : TransportAbstraction.ITaskHub
    {
        private readonly TransportAbstraction.IHost host;
        private readonly EventSourcedOrchestrationServiceSettings settings;

        private TransportAbstraction.IClient client;
        private Transport[] partitionTransports;
        private StorageAbstraction.IPartitionState[] partitionStates;
        private TransportAbstraction.IPartition[] partitions;
        private CancellationTokenSource shutdownTokenSource;
        private CloudStorageAccount account;

        private static readonly TimeSpan simulatedDelay = TimeSpan.FromMilliseconds(1);

        public AzureChannelsTransport(TransportAbstraction.IHost host, EventSourcedOrchestrationServiceSettings settings)
        {
            this.host = host;
            this.settings = settings;
            this.account = CloudStorageAccount.Parse(this.settings.StorageConnectionString);
        }

        async Task TransportAbstraction.ITaskHub.CreateAsync()
        {
            await Task.Delay(simulatedDelay);
            this.partitionTransports = new Transport[this.settings.MemoryPartitions]; // TODO use number hosts instead
            this.partitionStates = new StorageAbstraction.IPartitionState[this.settings.MemoryPartitions];
            this.partitions = new TransportAbstraction.IPartition[this.settings.MemoryPartitions];
        }

        async Task TransportAbstraction.ITaskHub.DeleteAsync()
        {
            await Task.Delay(simulatedDelay);
            this.partitionTransports = null;
        }

        async Task<bool> TransportAbstraction.ITaskHub.ExistsAsync()
        {
            await Task.Delay(simulatedDelay);
            return this.partitionTransports != null;
        }

        async Task TransportAbstraction.ITaskHub.StartAsync()
        {
            this.shutdownTokenSource = new CancellationTokenSource();

            var numberPartitions = this.settings.MemoryPartitions;
            this.host.NumberPartitions = numberPartitions;
            var creationTimestamp = DateTime.UtcNow;
            var startPositions = new long[numberPartitions];

            var clientId = Guid.NewGuid();

            var tableClient = this.account.CreateCloudTableClient();

            // create all partitions
            for (uint i = 0; i < this.settings.MemoryPartitions; i++)
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
                    await partitionTransport.CreateTableIfNotExistAsync();
                }

                var partition = partitions[i] = this.host.AddPartition(i, partitionStates[i], partitionTransport.PartitionSender);
                
                await partition.StartAsync(CancellationToken.None);
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

                await this.client.StopAsync();
                await Task.WhenAll(this.partitionStates.Select(partitionState => partitionState.PersistAndShutdownAsync(this.settings.TakeStateCheckpointWhenStoppingPartition)));
            }
        }
    }
}
