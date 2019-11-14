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
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventSourced.Emulated
{
    internal class EmulatedBackend : Backend.ITaskHub
    {
        private readonly Backend.IHost host;
        private readonly EventSourcedOrchestrationServiceSettings settings;

        private Dictionary<Guid, IEmulatedQueue<ClientEvent>> clientQueues;
        private IEmulatedQueue<PartitionEvent>[] partitionQueues;
        private CancellationTokenSource shutdownTokenSource;

        private static readonly TimeSpan simulatedDelay = TimeSpan.FromMilliseconds(1);

        public EmulatedBackend(Backend.IHost host, EventSourcedOrchestrationServiceSettings settings)
        {
            this.host = host;
            this.settings = settings;
        }

        async Task Backend.ITaskHub.CreateAsync()
        {
            var numberPartitions = settings.EmulatedPartitions;
            await Task.Delay(simulatedDelay);
            this.clientQueues = new Dictionary<Guid, IEmulatedQueue<ClientEvent>>();
            this.partitionQueues = new IEmulatedQueue<PartitionEvent>[numberPartitions];
        }

        async Task Backend.ITaskHub.DeleteAsync()
        {
            await Task.Delay(simulatedDelay);
            this.clientQueues = null;
            this.partitionQueues = null;
        }

        async Task<bool> Backend.ITaskHub.ExistsAsync()
        {
            await Task.Delay(simulatedDelay);
            return this.partitionQueues != null;
        }

        async Task Backend.ITaskHub.StartAsync()
        {
            this.shutdownTokenSource = new CancellationTokenSource();

            var numberPartitions = this.settings.EmulatedPartitions;
            this.host.NumberPartitions = numberPartitions;
            var creationTimestamp = DateTime.UtcNow;
            var startPositions = new long[numberPartitions];

            // create a client
            var clientId = Guid.NewGuid();
            var clientSender = new SendWorker(this.shutdownTokenSource.Token);
            var client = this.host.AddClient(clientId, clientSender);
            var clientQueue = this.settings.SerializeInEmulator
                ? (IEmulatedQueue<ClientEvent>) new EmulatedSerializingClientQueue(client, this.shutdownTokenSource.Token)
                : (IEmulatedQueue<ClientEvent>) new EmulatedClientQueue(client, this.shutdownTokenSource.Token);
            this.clientQueues[clientId] = clientQueue;
            clientSender.SetHandler(list => SendEvents(client, list));

            // create all partitions
            for (uint i = 0; i < this.settings.EmulatedPartitions; i++)
            {
                uint partitionId = i;
                var partitionSender = new SendWorker(this.shutdownTokenSource.Token);
                var partition = this.host.AddPartition(i, new MemoryStorage(), partitionSender);
                partitionSender.SetHandler(list => SendEvents(partition, list));
                var partitionQueue = this.settings.SerializeInEmulator
                    ? (IEmulatedQueue<PartitionEvent>)new EmulatedSerializingPartitionQueue(partition, this.shutdownTokenSource.Token)
                    : (IEmulatedQueue<PartitionEvent>)new EmulatedPartitionQueue(partition, this.shutdownTokenSource.Token);
                this.partitionQueues[i] = partitionQueue;
                await partition.StartAsync();
            }

            for (uint i = 0; i < numberPartitions; i++)
            {
                var evt = new TaskhubCreated()
                {
                    PartitionId = i,
                    CreationTimestamp = creationTimestamp,
                    StartPositions = startPositions,
                };

                this.partitionQueues[i].Send(evt);
            }
        }

        async Task Backend.ITaskHub.StopAsync()
        {
            await Task.Delay(simulatedDelay);

            if (this.shutdownTokenSource != null)
            {
                this.shutdownTokenSource.Cancel();
                this.shutdownTokenSource = null;
            }
        }

        private Task SendEvents(Backend.IClient client, List<Event> events)
        {
            try
            {
                SendEvents(events);
            }
            catch (TaskCanceledException)
            {
                // this is normal during shutdown
            }
            catch (Exception e)
            {
                client.ReportError(nameof(SendEvents), e);
                throw e;
            }
            return Task.CompletedTask;
        }

        private Task SendEvents(Backend.IPartition partition, List<Event> events)
        {
            try
            {
                SendEvents(events);
            }
            catch (TaskCanceledException)
            {
                // this is normal during shutdown
            }
            catch (Exception e)
            {
                partition.ReportError(nameof(SendEvents), e);
                throw e;
            }
            return Task.CompletedTask;
        }

        private void SendEvents(List<Event> events)
        {
            foreach (var evt in events)
            {
                if (evt is ClientEvent clientEvent)
                {
                    this.clientQueues[clientEvent.ClientId].Send(clientEvent);
                }
                else if (evt is PartitionEvent partitionEvent)
                {
                    this.partitionQueues[partitionEvent.PartitionId].Send(partitionEvent);
                }
            }
        }
    }
}
