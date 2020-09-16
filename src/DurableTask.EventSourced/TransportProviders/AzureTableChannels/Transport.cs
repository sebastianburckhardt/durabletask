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
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos.Table;

namespace DurableTask.EventSourced.AzureTableChannels
{
    internal class Transport : AzureTableHub<Event>
    {
        private readonly string taskHubId;
        private readonly uint partitionId;
        private readonly Guid clientId;
        private readonly StorageAbstraction.IPartitionState partitionState;

        private TransportAbstraction.IPartition partition;
        private TransportAbstraction.IClient client;

        public TransportAbstraction.ISender PartitionSender { get; private set; }
        public TransportAbstraction.ISender ClientSender { get; private set; }

        public Transport(
            EventSourcedOrchestrationServiceSettings settings,
            CancellationToken token,
            string taskHubId,
            uint partitionId,
            StorageAbstraction.IPartitionState partitionState,
            Guid clientId,
            CloudTableClient tableClient)
            : base(token, taskHubId, $"Host{partitionId:D2}", tableClient)
        {
            this.taskHubId = taskHubId;
            this.partitionId = partitionId;
            this.clientId = clientId;
            this.partitionState = partitionState;
            this.PartitionSender = new Transport.PartitionSenderWrap(this);
            this.ClientSender = new Transport.ClientSenderWrap(this);
        }

        public void SetLastReceived(uint partition, long lastReceived)
        {
            this.LastReceived[$"Host{partition:D2}"] = lastReceived;
        }

        public void SetLastReceived(Guid client, long lastReceived)
        {
            this.LastReceived[client.ToString("N")] = lastReceived;
        }

        public async Task ReceiveLoopAsync(TransportAbstraction.IPartition partition, TransportAbstraction.IClient client)
        {
            List<PartitionEvent> partitionBatch = new List<PartitionEvent>();
            List<ClientEvent> clientBatch = new List<ClientEvent>();

            SemaphoreSlim credits = new SemaphoreSlim(0); // no parallelism
            
            this.partition = partition;
            this.client = client;

            while (!this.cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var received = await this.Receive().ConfigureAwait(false);

                    foreach (var entity in received)
                    {
                        var evt = (Event)Serializer.DeserializeEvent(entity.Content);

                        if (evt is PartitionUpdateEvent partitionEvent)
                        {
                            partitionEvent.Serialized = new ArraySegment<byte>(entity.Content);
                            partitionBatch.Add(partitionEvent);
                        }
                        else
                        {
                            clientBatch.Add((ClientEvent)evt);
                        }
                    }
                    if (clientBatch.Count > 0)
                    {
                        foreach (var clientEvent in clientBatch)
                        {
                            client.Process(clientEvent);
                        }

                        clientBatch.Clear();
                    }
                    if (partitionBatch.Count > 0)
                    {
                        var lastEventInBatch = partitionBatch[partitionBatch.Count - 1];

                        partition.SubmitExternalEvents(partitionBatch, credits);

                        await credits.WaitAsync(this.cancellationToken);

                        partitionBatch.Clear();
                    }

                    this.DeleteRange(received);
                }
                catch (TaskCanceledException)
                {
                    // this is normal during shutdown
                }
                catch (Exception e)
                {
                    partition.ErrorHandler.HandleError(nameof(ReceiveLoopAsync), "Encountered exception while processing events", e, true, false);
                }
            }
        }

        protected override void HandleSuccessfulSend(Event evt)
        {
            try
            {
                DurabilityListeners.ConfirmDurable(evt);
            }
            catch (Exception exception) when (!(exception is OutOfMemoryException))
            {
                // for robustness, swallow exceptions, but report them
                this.partition.ErrorHandler.HandleError("LogWorker.Process", $"Encountered exception while notifying persistence listeners for {evt} id={evt.EventIdString}", exception, false, false);
            }
        }

        protected override void HandleFailedSend(Event evt, Exception exception, out bool requeue)
        {
            this.partition.ErrorHandler.HandleError(nameof(HandleFailedSend), $"Encountered exception while trying to send {evt}", exception, false, false);

            if (evt.SafeToRetryFailedSend())
            {
                requeue = true;
            }
            else
            {
                // the event may have been sent or maybe not, report problem to listener
                // this is used by clients who can give the exception back to the caller
                DurabilityListeners.ReportException(evt, exception);

                requeue = false;
            }
        }

        private class ClientSenderWrap : TransportAbstraction.ISender
        {
            private readonly Transport transport;
            private long position = 0;

            public ClientSenderWrap(Transport transport)
            {
                this.transport = transport;
            }

            public void Submit(Event evt)
            {
                var content = Serializer.SerializeEvent(evt);

                string destination = evt is PartitionUpdateEvent p ? $"Host{p.PartitionId:D2}" : "Host00";

                var pos = Interlocked.Increment(ref position);

                transport.Send(evt, transport.clientId.ToString("N"), destination, pos, content, evt.ToString());
            }
        }

        private class PartitionSenderWrap : TransportAbstraction.ISender
        {
            private readonly Transport transport;
            private long position = 0;

            public PartitionSenderWrap(Transport transport)
            {
                this.transport = transport;
            }

            public void Submit(Event evt)
            {
                var content = Serializer.SerializeEvent(evt);

                long nextCommitLogPosition = (evt is PartitionUpdateEvent e) ? e.NextCommitLogPosition : 0;
                string source = nextCommitLogPosition > 0 ? $"Host{this.transport.partitionId:D2}U" : $"Host{this.transport.partitionId:D2}";
                string destination = evt is PartitionUpdateEvent p ? $"Host{p.PartitionId:D2}" : "Host00";
                long pos = nextCommitLogPosition > 0 ? nextCommitLogPosition : Interlocked.Increment(ref position);
                transport.Send(evt, source, destination, pos, content, evt.ToString());
            }
        }
    }
}
