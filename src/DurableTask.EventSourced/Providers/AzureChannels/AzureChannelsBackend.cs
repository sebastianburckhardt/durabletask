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
    internal class AzureChannelsBackend : Backend.ITaskHub
    {
        private readonly Backend.IHost host;
        private readonly EventSourcedOrchestrationServiceSettings settings;

        private Backend.IClient client;
        private Transport[] partitionTransports;
        private Storage.IPartitionState[] partitionStates;
        private Backend.IPartition[] partitions;
        private CancellationTokenSource shutdownTokenSource;
        private CloudStorageAccount account;

        private static readonly TimeSpan simulatedDelay = TimeSpan.FromMilliseconds(1);

        public AzureChannelsBackend(Backend.IHost host, EventSourcedOrchestrationServiceSettings settings)
        {
            this.host = host;
            this.settings = settings;
            this.account = CloudStorageAccount.Parse(this.settings.StorageConnectionString);
        }


        async Task Backend.ITaskHub.CreateAsync()
        {
            await Task.Delay(simulatedDelay);
            this.partitionTransports = new Transport[this.settings.EmulatedPartitions]; // TODO use number hosts instead
            this.partitionStates = new Storage.IPartitionState[this.settings.EmulatedPartitions];
            this.partitions = new Backend.IPartition[this.settings.EmulatedPartitions];
        }

        async Task Backend.ITaskHub.DeleteAsync()
        {
            await Task.Delay(simulatedDelay);
            this.partitionTransports = null;
        }

        async Task<bool> Backend.ITaskHub.ExistsAsync()
        {
            await Task.Delay(simulatedDelay);
            return this.partitionTransports != null;
        }

        async Task Backend.ITaskHub.StartAsync()
        {
            this.shutdownTokenSource = new CancellationTokenSource();

            var numberPartitions = this.settings.EmulatedPartitions;
            this.host.NumberPartitions = numberPartitions;
            var creationTimestamp = DateTime.UtcNow;
            var startPositions = new long[numberPartitions];

            var clientId = Guid.NewGuid();

            var tableClient = this.account.CreateCloudTableClient();

            // create all partitions
            for (uint i = 0; i < this.settings.EmulatedPartitions; i++)
            {
                var partitionState = partitionStates[i] = new FasterStorage(settings.StorageConnectionString);
                //var partitionState = partitionStates[i] = new DurableTask.EventSourced.Emulated.EmulatedStorage();

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
                
                await partition.StartAsync();

                // TODO this only makes sense in test
                partition.Submit(new TaskhubCreated()
                {
                    PartitionId = i,
                    CreationTimestamp = creationTimestamp,
                    StartPositions = startPositions,
                });
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

        async Task Backend.ITaskHub.StopAsync()
        {
            if (this.shutdownTokenSource != null)
            {
                this.shutdownTokenSource.Cancel();
                this.shutdownTokenSource = null;

                await this.client.StopAsync();
                await Task.WhenAll(this.partitionStates.Select(partitionState => partitionState.WaitForTerminationAsync()));
            }
        }
    }
}
