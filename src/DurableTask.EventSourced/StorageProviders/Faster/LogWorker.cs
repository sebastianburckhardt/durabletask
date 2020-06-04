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

using DurableTask.Core.Common;
using FASTER.core;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventSourced.Faster
{
    internal class LogWorker : BatchWorker<PartitionUpdateEvent>
    {
        private readonly BlobManager blobManager;
        private readonly FasterLog log;
        private readonly Partition partition;
        private readonly StoreWorker storeWorker;
        private readonly FasterTraceHelper traceHelper;
        private bool isShuttingDown;
        private long ProspectiveTailAddress;

        public LogWorker(BlobManager blobManager, FasterLog log, Partition partition, StoreWorker storeWorker, FasterTraceHelper traceHelper, CancellationToken cancellationToken)
            : base(cancellationToken)
        {
            this.blobManager = blobManager;
            this.log = log;
            this.partition = partition;
            this.storeWorker = storeWorker;
            this.traceHelper = traceHelper;
            this.ProspectiveTailAddress = this.log.TailAddress;

            this.maxFragmentSize = (1 << this.blobManager.EventLogSettings.PageSizeBits) - 64; // faster needs some room for header, 64 bytes is conservative
        }

        public const byte first = 0x1;
        public const byte last = 0x2;
        public const byte none = 0x0;

        private int maxFragmentSize;

        public override void Submit(PartitionUpdateEvent evt)
        {
            byte[] bytes = Serializer.SerializeEvent(evt, first | last);

            if (!this.isShuttingDown || this.cancellationToken.IsCancellationRequested)
            {
                lock (this.thisLock)
                {
                    // Ideally, we would like to enqueue everything here as before, but only commit events when
                    // their dependencies are persisted. However, there is no "commitUntil" in FASTERLog API,
                    // so we can instead do enqueue, commit at the same time to ensure that this works.
                    //
                    // Instead of enqueuing the bytes, we just "predict" how much will they take
                    //Enqueue(bytes);
                    //evt.NextCommitLogPosition = this.log.TailAddress;

                    FakeEnqueue(bytes);
                    evt.NextCommitLogPosition = this.ProspectiveTailAddress;

                    base.Submit(evt);

                    // add to store worker (under lock for consistent ordering)
                    this.storeWorker.Submit(evt);
                }
            }
        }

        public override void SubmitIncomingBatch(IEnumerable<PartitionUpdateEvent> events)
        {
            // TODO optimization: use batching and reference data in EH queue instead of duplicating it          
            foreach (var evt in events)
            {
                // Before submitting external update events, we need to 
                // configure them to wait for external dependency confirmation
                // TODO: Find a way to do this somewhere else. This is not the right place
                evt.EventHasNoUnconfirmeDependencies = new TaskCompletionSource<object>();
                if (!(evt is PartitionMessageEvent))
                { 
                    // Bad code
                    evt.EventHasNoUnconfirmeDependencies.SetResult(null);
                }
                this.Submit(evt);
            }
        }

        private void FakeEnqueue(byte[] bytes)
        {
            // TODO: Ask FASTER about this size
            this.ProspectiveTailAddress = this.ProspectiveTailAddress + bytes.Length;
        }

        private void Enqueue(byte[] bytes)
        {
            if (bytes.Length <= maxFragmentSize)
            {
                this.log.Enqueue(bytes);
            }
            else
            {
                // the message is too big. Break it into fragments. 
                int pos = 1;
                while (pos < bytes.Length)
                {
                    bool isLastFragment = 1 + bytes.Length - pos <= maxFragmentSize;
                    int packetSize = isLastFragment ? 1 + bytes.Length - pos : maxFragmentSize;
                    bytes[pos - 1] = (byte)(((pos == 1) ? first : none) | (isLastFragment ? last : none));
                    this.log.Enqueue(new ReadOnlySpan<byte>(bytes, pos - 1, packetSize));
                    pos += packetSize - 1;
                }
            }
        }

        public async Task PersistAndShutdownAsync()
        {
            this.traceHelper.FasterProgress($"Stopping LogWorker");

            this.isShuttingDown = true;

            await this.WaitForCompletionAsync().ConfigureAwait(false);

            this.traceHelper.FasterProgress($"Stopped LogWorker");
        }

        //public async Task SetupUnconfirmedDependenciesListener()
        //{
        //    // We want to ensure that a checkpoint is only completed if events that 
        //    // it depends on have already been persisted in other partitions.
        //    DedupState dedupState = (DedupState)(await this.storeWorker.store.ReadAsync(TrackedObjectKey.Dedup, this.storeWorker.effectTracker));


        //    // Q: Could LastProcessed be faster than the real events that we are waiting for?
        //    //    If we can't use dedupState.LastProcessed, we might be able to keep this information
        //    //    by tracking every PartitionUpdateEvent that we process
        //    Dictionary<uint, long> waitingFor = dedupState.LastProcessed;
        //    Dictionary<uint, long> confirmed = dedupState.LastConfirmed;
        //    if (dedupState.KeepWaitingForPersistenceConfirmation(waitingFor, confirmed))
        //    {
        //        dedupState.ConfirmationListener = (lastConfirmed) =>
        //        {
        //            if (!dedupState.KeepWaitingForPersistenceConfirmation(waitingFor, lastConfirmed))
        //                this.store.CheckpointHasNoUnconfirmeDependencies.TrySetResult(null);
        //        };
        //    }
        //    else
        //    {
        //        this.store.CheckpointHasNoUnconfirmeDependencies.TrySetResult(null);
        //    }
        //}

        private async Task CheckpointLog(int count)
        {
            var stopwatch = new System.Diagnostics.Stopwatch();
            stopwatch.Start();
            long previous = log.CommittedUntilAddress;

            await log.CommitAsync().ConfigureAwait(false); // may commit more events than just the ones in the batch, but that is o.k.

            this.traceHelper.FasterLogPersisted(log.CommittedUntilAddress, count, (log.CommittedUntilAddress - previous), stopwatch.ElapsedMilliseconds);
        }

        private void EnqueueEvents(IList<PartitionUpdateEvent> batch, int from, int to)
        {
            for (var j = from; j < to; j++)
            {
                var currEvt = batch[j];
                byte[] bytes = Serializer.SerializeEvent(currEvt, first | last);
                Enqueue(bytes);
            }
        }

        private async Task EnqueueEventsAndCheckpoint(IList<PartitionUpdateEvent> batch, int from, int to)
        {
            var count = to - from;
            if (count > 0)
            {
                EnqueueEvents(batch, from, to);

                await this.CheckpointLog(count);

                // Now that the log is commited, we can send persistence confirmation events for
                // the commited events.
                for (var j = from; j < to; j++)
                {
                    var currEvt = batch[j];
                    try
                    {
                        DurabilityListeners.ConfirmDurable(currEvt);
                    }
                    catch (Exception exception) when (!(exception is OutOfMemoryException))
                    {
                        // for robustness, swallow exceptions, but report them
                        this.partition.ErrorHandler.HandleError("LogWorker.Process", $"Encountered exception while notifying persistence listeners for event {currEvt} id={currEvt.EventIdString}", exception, false, false);
                    }
                }
            }
        }

        protected override async Task Process(IList<PartitionUpdateEvent> batch)
        {
            try
            {
                // Q: Could this be a problem that this here takes a long time possibly blocking

                // Iteratively
                // - Find the next event that has a dependency (by checking if their Task is set)
                // - The ones before it can be safely commited.
                // - For event that is commited we also inform its durability listener
                // - Wait until the waiting for dependence is complete.
                // - go back to step 1
                var lastEnqueuedCommited = 0;
                for (var i=0; i < batch.Count; i++)
                {
                    var evt = batch[i];
                    if (!evt.EventHasNoUnconfirmeDependencies.Task.IsCompleted)
                    {
                        await EnqueueEventsAndCheckpoint(batch, lastEnqueuedCommited, i);

                        // Progress the last commited index
                        lastEnqueuedCommited = i;
                        // Before continuing, wait for the dependencies of this update to be done, so that we can continue
                        await evt.EventHasNoUnconfirmeDependencies.Task;
                    }
                }
                await EnqueueEventsAndCheckpoint(batch, lastEnqueuedCommited, batch.Count);

            }
            catch (OperationCanceledException) when (this.cancellationToken.IsCancellationRequested)
            {
                // o.k. during shutdown
            }
            catch (Exception e) when (!(e is OutOfMemoryException))
            {
                this.partition.ErrorHandler.HandleError("LogWorker.Process", "Encountered exception while working on commit log", e, true, false);
            }
        }


        //protected override async Task Process(IList<PartitionUpdateEvent> batch)
        //{
        //    try
        //    {
        //        //  checkpoint the log
        //        var stopwatch = new System.Diagnostics.Stopwatch();
        //        stopwatch.Start();
        //        long previous = log.CommittedUntilAddress;

        //        await log.CommitAsync().ConfigureAwait(false); // may commit more events than just the ones in the batch, but that is o.k.

        //        this.traceHelper.FasterLogPersisted(log.CommittedUntilAddress, batch.Count, (log.CommittedUntilAddress - previous), stopwatch.ElapsedMilliseconds);

        //        foreach (var evt in batch)
        //        {
        //            if (! (this.isShuttingDown || this.cancellationToken.IsCancellationRequested))
        //            {
        //                try
        //                {
        //                    DurabilityListeners.ConfirmDurable(evt);
        //                }
        //                catch (Exception exception) when (!(exception is OutOfMemoryException))
        //                {
        //                    // for robustness, swallow exceptions, but report them
        //                    this.partition.ErrorHandler.HandleError("LogWorker.Process", $"Encountered exception while notifying persistence listeners for event {evt} id={evt.EventIdString}", exception, false, false);
        //                }
        //            }
        //        }
        //    }
        //    catch (OperationCanceledException) when (this.cancellationToken.IsCancellationRequested)
        //    {
        //        // o.k. during shutdown
        //    }
        //    catch (Exception e) when (!(e is OutOfMemoryException))
        //    {
        //        this.partition.ErrorHandler.HandleError("LogWorker.Process", "Encountered exception while working on commit log", e, true, false);
        //    }        
        //}
    

        public async Task ReplayCommitLog(long from, StoreWorker worker)
        {
            // this procedure is called by StoreWorker during recovery. It replays all the events
            // that were committed to the log but are not reflected in the loaded store checkpoint.
            try
            {
                var to = this.log.TailAddress;

                using (var iter = log.Scan((long)from, to))
                {
                    byte[] result;
                    int entryLength;
                    long currentAddress;
                    MemoryStream reassembly = null;

                    while (!this.cancellationToken.IsCancellationRequested)
                    {
                        PartitionUpdateEvent partitionEvent = null;

                        while (!iter.GetNext(out result, out entryLength, out currentAddress))
                        {
                            if (currentAddress >= to)
                            {
                                return;
                            }
                            await iter.WaitAsync(this.cancellationToken).ConfigureAwait(false);
                        }

                        if ((result[0] & first) != none)
                        {
                            if ((result[0] & last) != none)
                            {
                                partitionEvent = (PartitionUpdateEvent)Serializer.DeserializeEvent(new ArraySegment<byte>(result, 1, entryLength - 1));
                            }
                            else
                            {
                                reassembly = new MemoryStream();
                                reassembly.Write(result, 1, entryLength - 1);
                            }
                        }
                        else
                        {
                            reassembly.Write(result, 1, entryLength - 1);

                            if ((result[0] & last) != none)
                            {
                                reassembly.Position = 0;
                                partitionEvent = (PartitionUpdateEvent)Serializer.DeserializeEvent(reassembly);
                                reassembly = null;
                            }
                        }

                        if (partitionEvent != null)
                        {
                            //// If the event depends on a later commit position that the one that is persisted
                            //// we have to stop replaying the commit log
                            //PartitionMessageEvent partitionMessageEvent = partitionEvent as PartitionMessageEvent;
                            //if (partitionMessageEvent != null)
                            //{
                            //    // Q: What is a good maybe type as an argument for this function to hold
                            //    //    maybe a dictionary from partitionIds to commitLogPositions
                            //    if (partitionMessageEvent.OriginPosition > beforePositions[partitionMessageEvent.OriginPartition])
                            //    {
                            //        // TODO: Stop the replay
                            //    }
                            //}
                            partitionEvent.NextCommitLogPosition = iter.NextAddress;
                            await worker.ProcessUpdate(partitionEvent).ConfigureAwait(false);
                        }
                    }
                }
            }
            catch (Exception exception)
                when (this.cancellationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.partition.ErrorHandler.Token);
            }
        }
    }
}
