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
using Microsoft.Extensions.Logging;
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
        private TransportAbstraction.IPartition[] partitions;
        private CancellationTokenSource shutdownTokenSource;

        private static readonly TimeSpan simulatedDelay = TimeSpan.FromMilliseconds(1);

        public MemoryTransport(TransportAbstraction.IHost host, EventSourcedOrchestrationServiceSettings settings, ILoggerFactory loggerFactory)
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
            this.partitions = new TransportAbstraction.IPartition[numberPartitions];
        }

        Task TransportAbstraction.ITaskHub.DeleteAsync()
        {
            this.clientQueues = null;
            this.partitionQueues = null;

            return this.host.StorageProvider.DeleteAllPartitionStatesAsync();
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
            var clientQueue = new MemoryClientQueue(this.client, this.shutdownTokenSource.Token);
            this.clientQueues[clientId] = clientQueue;
            clientSender.SetHandler(list => SendEvents(this.client, list));

            // create all partitions
            Parallel.For(0, this.settings.MemoryPartitions, (iteration) =>
            {
                int i = (int) iteration;
                uint partitionId = (uint) iteration;
                var partitionSender = new SendWorker(this.shutdownTokenSource.Token);
                var partition = this.host.AddPartition(partitionId, partitionSender);
                partitionSender.SetHandler(list => SendEvents(partition, list));
                this.partitionQueues[i] = new MemoryPartitionQueue(partition, this.shutdownTokenSource.Token);
                this.partitions[i] = partition;
            });

            // create or recover all the partitions
            for (uint i = 0; i < this.settings.MemoryPartitions; i++)
            {
                var nextInputQueuePosition = await partitions[i].CreateOrRestoreAsync(this.host.CreateErrorHandler(i), 0);
                partitionQueues[i].FirstInputQueuePosition = nextInputQueuePosition;
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

                var tasks = new List<Task>();
                foreach(var p in this.partitions)
                {
                    tasks.Add(p.StopAsync());
                }
                await Task.WhenAll(tasks);
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
                client.ReportTransportError(nameof(SendEvents), e);
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
                partition.ErrorHandler.HandleError(nameof(SendEvents), "Encountered exception while trying to send events", e, true, false);
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
                    this.partitionQueues[partitionEvent.PartitionId].Send(partitionEvent);
                }
            }
        }
    }
}
