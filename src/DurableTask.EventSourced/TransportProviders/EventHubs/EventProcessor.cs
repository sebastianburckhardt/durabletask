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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Mime;
using System.Net.NetworkInformation;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DurableTask.EventSourced.Emulated;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace DurableTask.EventSourced.EventHubs
{
    internal class EventProcessor : IEventProcessor, TransportAbstraction.IAckListener
    {
        private readonly TransportAbstraction.IHost host;
        private readonly TransportAbstraction.ISender sender;
        private readonly EventHubsTransport.TaskhubParameters parameters;
        private readonly ILogger logger;

        private uint partitionId;
        private CancellationTokenSource forcefulTermination;

        private Stopwatch timeSinceLastCheckpoint = new Stopwatch();
        private volatile Checkpoint pendingCheckpoint;

        private ConcurrentQueue<(PartitionEvent evt, string offset, long seqno)> pending;

        private volatile Task<CurrentPartition> currentPartition;
     
        /// <summary>
        /// The event processor can recover after exceptions, so we encapsulate
        /// the currently active partition
        /// </summary>
        private class CurrentPartition
        {
            public Termination Termination;
            public TransportAbstraction.IPartition Partition;
            public Task<CurrentPartition> Next;
            public ulong NextMessageToReceive;
        }

        private Dictionary<string, MemoryStream> reassembly = new Dictionary<string, MemoryStream>();

        public EventProcessor(
            TransportAbstraction.IHost host,
            TransportAbstraction.ISender sender,
            EventHubsTransport.TaskhubParameters parameters)
        {
            this.host = host;
            this.logger = host.TransportLogger;
            this.sender = sender;
            this.parameters = parameters;
            this.pending = new ConcurrentQueue<(PartitionEvent evt, string offset, long seqno)>();
        }

        Task IEventProcessor.OpenAsync(PartitionContext context)
        {
            this.logger.LogInformation("Part{partition:D2} Starting EventProcessor", context.PartitionId);
            this.forcefulTermination = new CancellationTokenSource();
            this.partitionId = uint.Parse(context.PartitionId);
            this.currentPartition = this.StartPartitionAsync();
            return Task.CompletedTask;
        }

        public void Acknowledge(Event evt)
        {
            while (this.pending.TryPeek(out var front) && front.evt.NextInputQueuePosition.Value <= evt.NextInputQueuePosition.Value)
            {
                if (this.pending.TryDequeue(out var candidate))
                {
                    if (this.timeSinceLastCheckpoint.ElapsedMilliseconds > 30000)
                    {
                        this.pendingCheckpoint = new Checkpoint(this.partitionId.ToString(), candidate.offset, candidate.seqno);
                        timeSinceLastCheckpoint.Restart();
                    }
                }
            }
        }

        private async Task<CurrentPartition> StartPartitionAsync(CurrentPartition prior = null)
        {
            if (prior != null)
            {
                await TaskHelpers.WaitForCancellationAsync(prior.Termination.Token);
                this.currentPartition = prior.Next;
                this.logger.LogDebug("Part{partition:D2} Waiting before restarting EventProcessor", this.partitionId);
                await Task.Delay(TimeSpan.FromSeconds(12));
            }

            this.forcefulTermination.Token.ThrowIfCancellationRequested();

            var c = new CurrentPartition();
            c.Termination = new Termination(this.forcefulTermination.Token);
            c.Next = this.StartPartitionAsync(c);
            c.Partition = host.AddPartition(this.partitionId, this.sender);
            c.NextMessageToReceive = await c.Partition.StartAsync(c.Termination, (ulong)this.parameters.StartPositions[this.partitionId]);

            var next = this.StartPartitionAsync(c);

            this.logger.LogInformation("Part{partition:D2} Successfully started EventProcessor, next expected message is #{nextSeqno}", this.partitionId, c.NextMessageToReceive);

            // receive messages already sitting in the buffer
            var batch = pending.Select(triple => triple.Item1).Where(evt => evt.NextInputQueuePosition > c.NextMessageToReceive).ToList();
            if (batch.Count > 0)
            {
                c.NextMessageToReceive = batch[batch.Count - 1].NextInputQueuePosition.Value;
                c.Partition.SubmitInputEvents(batch);
                this.logger.LogDebug("Part{partition:D2} EventProcessor received {batchsize} messages, starting with #{seqno}, next expected message is #{nextSeqno}", this.partitionId, batch.Count, batch[0].NextInputQueuePosition - 1, c.NextMessageToReceive);
            }

            timeSinceLastCheckpoint.Start();
            return c;
        }

        async Task IEventProcessor.CloseAsync(PartitionContext context, CloseReason reason)
        {
            this.logger.LogInformation("Part{partition:D2} Stopping EventProcessor for partition", this.partitionId);

            var currentTask = this.currentPartition;

            if (! currentTask.IsCompleted)
            {
                this.forcefulTermination.Cancel();
            }

            try
            {
                CurrentPartition current = await this.currentPartition;

                while (current.Termination.IsTerminated)
                {
                    current = await current.Next;               
                }

                await current.Partition.StopAsync();

                this.logger.LogInformation("Part{partition:D2} Cleanly stopped EventProcessor", this.partitionId);
            }
            catch(OperationCanceledException)
            {
                this.logger.LogInformation("Part{partition:D2} Terminated EventProcessor", this.partitionId);
            }

            await WriteLastCompletedCheckpoint(context);

            this.logger.LogInformation("Part{partition:D2} Stopped EventProcessor for partition", this.partitionId);
        }

        private async ValueTask WriteLastCompletedCheckpoint(PartitionContext context)
        {
            var checkpoint = this.pendingCheckpoint;
            if (checkpoint != null)
            {
                this.pendingCheckpoint = null;
                this.logger.LogInformation("Part{partition:D2} EventProcessor is checkpointing messages received through #{seqno}", this.partitionId, checkpoint.SequenceNumber);
                await context.CheckpointAsync(checkpoint);
            }
        }

        Task IEventProcessor.ProcessErrorAsync(PartitionContext context, Exception exception)
        {
            this.logger.LogError("Part{partition:D2} Error in EventProcessor: {exception}", this.partitionId, exception);

            return Task.FromResult<object>(null);
        }

        async Task IEventProcessor.ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
        {
            CurrentPartition current = null;

            try
            {
                current = await this.currentPartition;

                while (current.Termination.IsTerminated)
                {
                    current = await current.Next;
                }
                
                var batch = new List<PartitionEvent>();

                foreach (var eventData in messages)
                {
                    var seqno = (ulong)eventData.SystemProperties.SequenceNumber;
                    if (seqno == current.NextMessageToReceive)
                    {
                        this.logger.LogTrace("Part{partition:D2} EventProcessor received message #{seqno}", this.partitionId, seqno);
                        var evt = (PartitionEvent)Serializer.DeserializeEvent(eventData.Body);
                        current.NextMessageToReceive = seqno + 1;
                        evt.NextInputQueuePosition = current.NextMessageToReceive;
                        batch.Add(evt);
                        pending.Enqueue((evt, eventData.SystemProperties.Offset, eventData.SystemProperties.SequenceNumber));
                        AckListeners.Register(evt, this);
                    }
                    else if (seqno > current.NextMessageToReceive)
                    {
                        this.logger.LogError("Part{partition:D2} EventProcessor received wrong message, #{seqno} instead of #{expected}", this.partitionId, seqno, current.NextMessageToReceive);
                        throw new InvalidOperationException("EventHubs Out-Of-Order Message");
                    }
                    else
                    {
                        this.logger.LogTrace("Part{partition:D2} EventProcessor discarded message #{seqno} because it is already processed", this.partitionId, seqno);
                    }
                }

                if (batch.Count > 0)
                {
                    this.logger.LogDebug("Part{partition:D2} EventProcessor received {batchsize} messages, starting with #{seqno}, next expected message is #{nextSeqno}", this.partitionId, batch.Count, batch[0].NextInputQueuePosition - 1, current.NextMessageToReceive);
                    current.Partition.SubmitInputEvents(batch);
                }

                await this.WriteLastCompletedCheckpoint(context);

                return;
            }
            catch (OperationCanceledException)
            {
                this.logger.LogInformation("Part{partition:D2} EventProcessor was cancelled or lost lease", context.PartitionId);
                throw;
            }
            catch (Exception exception)
            {
                this.logger.LogError("Part{partition:D2} Error while processing messages : {exception}", context.PartitionId, exception);
                current?.Termination.Terminate("error in event processor");
                throw;
            }
        }
    }
}
