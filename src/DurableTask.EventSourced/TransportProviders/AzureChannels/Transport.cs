using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventSourced.AzureChannels
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
            PartitionBatch partitionBatch = new PartitionBatch();
            List<ClientEvent> clientBatch = new List<ClientEvent>();

            this.partition = partition;
            this.client = client;

            while (!this.cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var received = await this.Receive();

                    foreach (var entity in received)
                    {
                        var evt = (Event)Serializer.DeserializeEvent(entity.Content);

                        if (evt is PartitionEvent partitionEvent)
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

                        AckListeners.Register(lastEventInBatch, partitionBatch);

                        partition.SubmitInputEvents(partitionBatch);

                        await partitionBatch.Tcs.Task; // TODO add cancellation token

                        partitionBatch.Clear();
                    }

                    this.DeleteRange(received);
                }
                catch (TaskCanceledException)
                {
                    // this is normal during shutdown
                }
                catch(Exception e)
                {
                    partition.ReportError("Failure in receive loop", e);
                }
            }
        }

        protected override void HandleSuccessfulSend(Event evt)
        {
            AckListeners.Acknowledge(evt);          
        }

        protected override bool HandleFailedSend(Event evt, Exception exception)
        {
            this.partition.ReportError($"could not send {evt}", exception);

            if (evt.SafeToDuplicateInTransport())
            {
                return true;
            }
            else
            {
                // the event may have been sent or maybe not, report problem to listener
                // this is used by clients who can give the exception back to the caller
                AckListeners.ReportException(evt, exception);
            }

            return false;
        }

        private class PartitionBatch : List<PartitionEvent>, TransportAbstraction.IAckListener
        {
            public TaskCompletionSource<object> Tcs = new TaskCompletionSource<object>();

            public void Acknowledge(Event evt)
            {
                Tcs.TrySetResult(null);
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

                string destination = evt is PartitionEvent p ? $"Host{p.PartitionId:D2}" : "Host00";

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
                if (evt is PartitionEvent partitionEvent
                    && partitionEvent.PartitionId == transport.partitionId)
                {
                    // a loop-back message (impulse) can be committed immediately
                    transport.partitionState.Submit(partitionEvent);
                }
                else
                {
                    var content = Serializer.SerializeEvent(evt);

                    string source = !evt.CommitLogPosition.HasValue ? $"Host{this.transport.partitionId:D2}U" : $"Host{this.transport.partitionId:D2}";
                    string destination = evt is PartitionEvent p ? $"Host{p.PartitionId:D2}" : "Host00";
                    long pos = evt.CommitLogPosition.HasValue ? (long) evt.CommitLogPosition.Value : Interlocked.Increment(ref position);
                    transport.Send(evt, source, destination, pos, content, evt.ToString());
                }
            }
        }
    }
}
