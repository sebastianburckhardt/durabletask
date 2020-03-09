////  Copyright Microsoft Corporation
////  Licensed under the Apache License, Version 2.0 (the "License");
////  you may not use this file except in compliance with the License.
////  You may obtain a copy of the License at
////  http://www.apache.org/licenses/LICENSE-2.0
////  Unless required by applicable law or agreed to in writing, software
////  distributed under the License is distributed on an "AS IS" BASIS,
////  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
////  See the License for the specific language governing permissions and
////  limitations under the License.
////  ----------------------------------------------------------------------------------

//using System;
//using System.Collections.Concurrent;
//using System.Collections.Generic;
//using System.Diagnostics;
//using System.IO;
//using System.Linq;
//using System.Net.Mime;
//using System.Net.NetworkInformation;
//using System.Security.Cryptography;
//using System.Text;
//using System.Threading;
//using System.Threading.Tasks;
//using DurableTask.EventSourced.Emulated;
//using Microsoft.Azure.EventHubs;
//using Microsoft.Azure.EventHubs.Processor;
//using Microsoft.Extensions.Logging;

//namespace DurableTask.EventSourced.EventHubs
//{
//    internal class RestartingPartition : TransportAbstraction.IPartition
//    {
//        private readonly TransportAbstraction.IHost host;
//        private readonly ILogger logger;
//        private readonly ConcurrentQueue<(PartitionEvent evt, string offset, long seqno)> pending;

//        private uint partitionId;

//        public RestartingPartition(TransportAbstraction.IHost host, ILogger logger)
//        {
//            this.host = host;
//            this.logger = logger;
//            this.pending = new ConcurrentQueue<(PartitionEvent evt, string offset, long seqno)>();

//        }



//        Task IEventProcessor.OpenAsync(PartitionContext context)
//        {
//            this.logger.LogInformation("Part{partition:D2} Starting EventProcessor", context.PartitionId);
//            this.eventProcessorShutdown = new CancellationTokenSource();
//            this.partitionId = uint.Parse(context.PartitionId);
//            this.startupTask = this.StartPartition(TimeSpan.Zero);
//            return Task.CompletedTask;
//        }

//        /// <summary>
//        /// The event processor can recover after exceptions, so we encapsulate
//        /// the currently active partition
//        /// </summary>
//        private class CurrentPartition
//        {
//            public StorageAbstraction.IPartitionState partitionState;
//            public TransportAbstraction.IPartition partition;
//            public ulong nextMessageToReceive;
//        }
//        private CancellationTokenSource eventProcessorShutdown;
//        private Task<CurrentPartition> startupTask;
//        private Stopwatch timeSinceLastCheckpoint = new Stopwatch();

      

//        /// <summary>
//        /// The event processor can recover after exceptions, so we encapsulate
//        /// the currently active partition
//        /// </summary>
//        private class CurrentPartition
//        {
//            public StorageAbstraction.IPartitionState partitionState;
//            public TransportAbstraction.IPartition partition;
//            public ulong nextMessageToReceive;
//        }

 


//        public void Acknowledge(Event evt)
//        {
//            while (this.pending.TryPeek(out var front) && front.evt.NextInputQueuePosition.Value <= evt.NextInputQueuePosition.Value)
//            {
//                if (this.pending.TryDequeue(out var candidate))
//                {
//                    if (this.timeSinceLastCheckpoint.ElapsedMilliseconds > 30000)
//                    {
//                        this.pendingCheckpoint = new Checkpoint(this.partitionId.ToString(), candidate.offset, candidate.seqno);
//                        timeSinceLastCheckpoint.Restart();
//                    }
//                }
//            }
//        }

//        private async Task<CurrentPartition> StartPartition(TimeSpan delay)
//        {
//            CurrentPartition current = new CurrentPartition();

//            if (delay > TimeSpan.Zero)
//            {
//                this.logger.LogDebug("Part{partition:D2} Waiting before restarting EventProcessor", this.partitionId);
//                await Task.Delay(delay, this.eventProcessorShutdown.Token);
//            }

//            current.partitionState = this.host.StorageProvider.CreatePartitionState();
//            current.partition = host.AddPartition(this.partitionId, current.partitionState, this.sender);

//            current.nextMessageToReceive = await current.partition.StartAsync(this.eventProcessorShutdown.Token, (ulong)this.parameters.StartPositions[this.partitionId]);
            
//            if (delay > TimeSpan.Zero)
//            {
//                this.logger.LogInformation("Part{partition:D2} Successfully started EventProcessor, next expected message is #{nextSeqno}", this.partitionId, current.nextMessageToReceive);
//            }

//            // if this is a recovery, i.e. we failed prior to starting this partition, re-receive all pending batches
//            var batch = pending.Select(triple => triple.Item1).Where(evt => evt.NextInputQueuePosition > current.nextMessageToReceive).ToList();

//            if (batch.Count > 0)
//            {
//                current.nextMessageToReceive = batch[batch.Count - 1].NextInputQueuePosition.Value;
//                current.partition.SubmitInputEvents(batch);
//                this.logger.LogDebug("Part{partition:D2} EventProcessor received {batchsize} messages, starting with #{seqno}, next expected message is #{nextSeqno}", this.partitionId, batch.Count, batch[0].NextInputQueuePosition - 1, current.nextMessageToReceive);
//            }

//            timeSinceLastCheckpoint.Start();

//            return current;
//        }

//        async Task IEventProcessor.CloseAsync(PartitionContext context, CloseReason reason)
//        {
//            this.logger.LogInformation("Part{partition:D2} Stopping EventProcessor for partition", this.partitionId);
//            this.eventProcessorShutdown.Cancel();

//            CurrentPartition current = null;

//            try
//            {
//                current = await this.startupTask; // must wait for startup to complete (or fault, or be canceled)

//                await current.partition.StopAsync();

//                var checkpoint = this.pendingCheckpoint;
//                if (checkpoint != null)
//                {
//                    this.pendingCheckpoint = null;
//                    this.logger.LogInformation("Part{partition:D2} EventProcessor is checkpointing messages received through #{seqno}", this.partitionId, checkpoint.SequenceNumber);
//                    await context.CheckpointAsync(checkpoint);
//                }

//                this.logger.LogInformation("Part{partition:D2} Successfully stopped EventProcessor", this.partitionId);
//            }
//            catch (OperationCanceledException) when (this.eventProcessorShutdown.IsCancellationRequested)
//            {
//                this.logger.LogInformation("Part{partition:D2} EventProcessor was cancelled or lost lease", context.PartitionId);
//            }
//            catch (Exception e)
//            {
//                this.logger.LogError(e, "Part{partition:D2} Error while stopping EventProcessor", this.partitionId);

//                throw;
//            }
//        }

//        Task IEventProcessor.ProcessErrorAsync(PartitionContext context, Exception exception)
//        {
//            this.logger.LogError("Part{partition:D2} Error in EventProcessor: {exception}", this.partitionId, exception);

//            return Task.FromResult<object>(null);
//        }


//        async Task IEventProcessor.ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
//        {
//            try
//            {
//                var current = await this.startupTask; // must wait for startup to complete (or fault, or be canceled)

//                var batch = new List<PartitionEvent>();

//                foreach (var eventData in messages)
//                {
//                    var seqno = (ulong)eventData.SystemProperties.SequenceNumber;
//                    if (seqno == current.nextMessageToReceive)
//                    {
//                        this.logger.LogTrace("Part{partition:D2} EventProcessor received message #{seqno}", this.partitionId, seqno);
//                        var evt = (PartitionEvent)Serializer.DeserializeEvent(eventData.Body);
//                        current.nextMessageToReceive = seqno + 1;
//                        evt.NextInputQueuePosition = current.nextMessageToReceive;
//                        batch.Add(evt);
//                        pending.Enqueue((evt, eventData.SystemProperties.Offset, eventData.SystemProperties.SequenceNumber));
//                        AckListeners.Register(evt, this);
//                    }
//                    else if (seqno > current.nextMessageToReceive)
//                    {
//                        this.logger.LogError("Part{partition:D2} EventProcessor received wrong message, #{seqno} instead of #{expected}", this.partitionId, seqno, current.nextMessageToReceive);
//                        throw new InvalidOperationException("EventHubs Out-Of-Order Message");
//                    }
//                    else
//                    {
//                        this.logger.LogTrace("Part{partition:D2} EventProcessor discarded message #{seqno} because it is already processed", this.partitionId, seqno);
//                    }
//                }

//                if (batch.Count > 0)
//                {
//                    this.logger.LogDebug("Part{partition:D2} EventProcessor received {batchsize} messages, starting with #{seqno}, next expected message is #{nextSeqno}", this.partitionId, batch.Count, batch[0].NextInputQueuePosition - 1, current.nextMessageToReceive);
//                    current.partition.SubmitInputEvents(batch);
//                }

//                current.partitionState.CancellationToken.ThrowIfCancellationRequested();

//                var checkpoint = this.pendingCheckpoint;
//                if (checkpoint != null)
//                {
//                    this.pendingCheckpoint = null;
//                    this.logger.LogInformation("Part{partition:D2} EventProcessor is checkpointing messages received through #{seqno}", this.partitionId, checkpoint.SequenceNumber);
//                    await context.CheckpointAsync(checkpoint);
//                }

//                return;
//            }
//            catch (OperationCanceledException)
//            {
//                this.logger.LogInformation("Part{partition:D2} EventProcessor was cancelled or lost lease", context.PartitionId);
//            }
//            catch (Exception exception)
//            {
//                this.logger.LogError("Part{partition:D2} Error while processing messages : {exception}", context.PartitionId, exception);
//            }

//            // retry in a bit
//            if (!this.eventProcessorShutdown.IsCancellationRequested)
//            {
//                this.startupTask = StartPartition(TimeSpan.FromSeconds(10));
//            }
//        }
//    }
//}
