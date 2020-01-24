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

using DurableTask.EventSourced.Faster;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventSourced.Emulated
{
    internal class MemoryTransport : TransportAbstraction.ITaskHub
    {
        private readonly TransportAbstraction.IHost host;
        private readonly EventSourcedOrchestrationServiceSettings settings;

        private Dictionary<Guid, IMemoryQueue<ClientEvent>> clientQueues;
        private IMemoryQueue<PartitionEvent>[] partitionQueues;
        private TransportAbstraction.IClient client;
        private StorageAbstraction.IPartitionState[] partitionStates;
        private CancellationTokenSource shutdownTokenSource;

        private static readonly TimeSpan simulatedDelay = TimeSpan.FromMilliseconds(1);

        public MemoryTransport(TransportAbstraction.IHost host, EventSourcedOrchestrationServiceSettings settings)
        {
            this.host = host;
            this.settings = settings;
        }

        async Task TransportAbstraction.ITaskHub.CreateAsync()
        {
            var numberPartitions = settings.MemoryPartitions;
            await Task.Delay(simulatedDelay);
            this.clientQueues = new Dictionary<Guid, IMemoryQueue<ClientEvent>>();
            this.partitionQueues = new IMemoryQueue<PartitionEvent>[numberPartitions];
            this.partitionStates = new StorageAbstraction.IPartitionState[numberPartitions];
        }

        async Task TransportAbstraction.ITaskHub.DeleteAsync()
        {
            await Task.Delay(simulatedDelay);
            this.clientQueues = null;
            this.partitionQueues = null;
        }

        async Task<bool> TransportAbstraction.ITaskHub.ExistsAsync()
        {
            await Task.Delay(simulatedDelay);
            return this.partitionQueues != null;
        }

        async Task TransportAbstraction.ITaskHub.StartAsync()
        {
            this.shutdownTokenSource = new CancellationTokenSource();

            var numberPartitions = this.settings.MemoryPartitions;
            this.host.NumberPartitions = numberPartitions;
            var creationTimestamp = DateTime.UtcNow;
            var startPositions = new long[numberPartitions];

            // create a client
            var clientId = Guid.NewGuid();
            var clientSender = new SendWorker(this.shutdownTokenSource.Token);
            this.client = this.host.AddClient(clientId, clientSender);
            var clientQueue = new MemorySerializingClientQueue(this.client, this.shutdownTokenSource.Token);
            this.clientQueues[clientId] = clientQueue;
            clientSender.SetHandler(list => SendEvents(this.client, list));

            // create all partitions
            for (uint i = 0; i < this.settings.MemoryPartitions; i++)
            {
                uint partitionId = i;
                var partitionSender = new SendWorker(this.shutdownTokenSource.Token);
                var partitionState = partitionStates[i] = this.host.CreatePartitionState();
                var partition = this.host.AddPartition(i, partitionState, partitionSender);
                partitionSender.SetHandler(list => SendEvents(partition, list));
                var partitionQueue = new MemorySerializingPartitionQueue(partition, this.shutdownTokenSource.Token);
                this.partitionQueues[i] = partitionQueue;
                await partition.StartAsync();
            }

            // start all the emulated queues
            foreach (var partitionQueue in this.partitionQueues)
            {
                partitionQueue.Resume();
            }
            clientQueue.Resume();
        }

        async Task TransportAbstraction.ITaskHub.StopAsync()
        {
            if (this.shutdownTokenSource != null)
            {
                this.shutdownTokenSource.Cancel();
                this.shutdownTokenSource = null;

                await this.client.StopAsync();
                await Task.WhenAll(this.partitionStates.Select(partitionState => partitionState.PersistAndShutdownAsync()));
            }
        }

        private void SendEvents(TransportAbstraction.IClient client, IEnumerable<Event> events)
        {
            try
            {
                SendEvents(events, null);
            }
            catch (TaskCanceledException)
            {
                // this is normal during shutdown
            }
            catch (Exception e)
            {
                client.ReportError(nameof(SendEvents), e);
            }
        }

        private void SendEvents(TransportAbstraction.IPartition partition, IEnumerable<Event> events)
        {
            try
            {
                SendEvents(events, partition.PartitionId);
            }
            catch (TaskCanceledException)
            {
                // this is normal during shutdown
            }
            catch (Exception e)
            {
                partition.ReportError(nameof(SendEvents), e);
            }
        }

        private void SendEvents(IEnumerable<Event> events, uint? sendingPartition)
        {
            foreach (var evt in events)
            {
                if (evt is ClientEvent clientEvent)
                {
                    this.clientQueues[clientEvent.ClientId].Send(clientEvent);
                }
                else if (evt is PartitionEvent partitionEvent)
                {
                    if (partitionEvent.PartitionId == sendingPartition)
                    {
                        // a loop-back message (impulse) can be committed immediately
                        this.partitionStates[sendingPartition.Value].Submit(partitionEvent);
                    }
                    else
                    {
                        // enqueue this event
                        this.partitionQueues[partitionEvent.PartitionId].Send(partitionEvent);
                    }
                }
            }
        }
    }
}
