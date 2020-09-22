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

        private readonly IntakeWorker intakeWorker;

        public LogWorker(BlobManager blobManager, FasterLog log, Partition partition, StoreWorker storeWorker, FasterTraceHelper traceHelper, CancellationToken cancellationToken)
            : base(nameof(LogWorker), cancellationToken)
        {
            partition.ErrorHandler.Token.ThrowIfCancellationRequested();

            this.blobManager = blobManager;
            this.log = log;
            this.partition = partition;
            this.storeWorker = storeWorker;
            this.traceHelper = traceHelper;
            this.intakeWorker = new IntakeWorker(cancellationToken, this);

            this.maxFragmentSize = (1 << this.blobManager.EventLogSettings(partition.Settings.UsePremiumStorage).PageSizeBits) - 64; // faster needs some room for header, 64 bytes is conservative
        }

        public const byte first = 0x1;
        public const byte last = 0x2;
        public const byte none = 0x0;

        private int maxFragmentSize;

        public long LastCommittedInputQueuePosition { get; private set; }

        private class IntakeWorker : BatchWorker<PartitionEvent>
        {
            private readonly LogWorker logWorker;
            private readonly List<PartitionUpdateEvent> updateEvents;
            private readonly SemaphoreSlim logWorkerCredits = new SemaphoreSlim(10000);
            private readonly SemaphoreSlim storeWorkerCredits = new SemaphoreSlim(10000);

            // I assume that this list contains pointers to the events
            public Dictionary<uint, List<Tuple<long, PartitionUpdateEvent>>> WaitingForConfirmation = new Dictionary<uint, List<Tuple<long, PartitionUpdateEvent>>>();

            public IntakeWorker(CancellationToken token, LogWorker logWorker) : base(nameof(IntakeWorker), token)
            {
                this.logWorker = logWorker;
                this.updateEvents = new List<PartitionUpdateEvent>();
            }

            protected override async Task Process(IList<PartitionEvent> batch)
            {
                if (batch.Count > 0 && !this.logWorker.isShuttingDown)
                {
                    // before processing any update events they need to be serialized
                    // and assigned a commit log position
                    foreach (var evt in batch)
                    {

                        // We don't need to submit PersistenceConfirmationEvents further down, since they don't need to be actually committed.
                        // Commiting the events implies that persistence of their dependencies was confirmed.
                        if (evt is PersistenceConfirmationEvent persistenceConfirmationEvent)
                        {
                            // PersistenceConfirmationEvents have no dependencies
                            // This is probably unnecessary since we don't submit them
                            // TODO: Delete if that is the case
                            //persistenceConfirmationEvent.EventHasNoUnconfirmeDependencies.SetResult(null);
                            this.ConfirmDependencyPersistence(persistenceConfirmationEvent);
                        }
                        else if (evt is PartitionUpdateEvent partitionUpdateEvent)
                        {
                            // TODO: 
                            // Q: Is there a reason we deleted this? Shouldn't we drop events if we are shutting down?
                            // if (!this.isShuttingDown || this.cancellationToken.IsCancellationRequested || (evt is PersistenceConfirmationEvent))
                            // {
                            //     ...
                            // }
                            // else
                            // {
                            //     this.traceHelper.FasterProgress($"Dropped event: " + evt.ToString());
                            // }

                            // Before submitting external update events, we need to 
                            // configure them to wait for external dependency confirmation
                            partitionUpdateEvent.EventHasNoUnconfirmeDependencies = new TaskCompletionSource<object>();
                            if (partitionUpdateEvent is PartitionMessageEvent partitionMessageEvent)
                            {
                                // It is actually fine keeping the dependencies of events in the internal worker, since
                                // if the partition crashes, all uncommited messages (that are the only ones that have unconfirmed dependencies)
                                // will be re-received and re-submitted. Since every event will be followed by its confirmation, this cannot lead
                                // to a deadlock.
                                SetConfirmationWaiter(partitionMessageEvent);
                            }
                            else
                            {
                                partitionUpdateEvent.EventHasNoUnconfirmeDependencies.SetResult(null);
                            }
                            var bytes = Serializer.SerializeEvent(evt, first | last);
                            this.logWorker.AddToFasterLog(bytes);
                            partitionUpdateEvent.NextCommitLogPosition = this.logWorker.log.TailAddress;

                            updateEvents.Add(partitionUpdateEvent);
                        }
                    }

                    // the store worker and the log worker can now process these events in parallel
                    // Persistence Confirmation events are not submitted to any of the workers.
                    this.logWorker.storeWorker.SubmitBatch(batch.Where(e => !(e is PersistenceConfirmationEvent)).ToList(), this.storeWorkerCredits);
                    this.logWorker.SubmitBatch(updateEvents, this.logWorkerCredits);

                    this.updateEvents.Clear();

                    // do not continue the processing loop until we have enough credits
                    await this.storeWorkerCredits.WaitAsync(this.cancellationToken);
                    await this.logWorkerCredits.WaitAsync(this.cancellationToken);
                }
            }

            private void SetConfirmationWaiter(PartitionMessageEvent evt)
            {
                var originPartition = evt.OriginPartition;
                var originPosition = evt.OriginPosition;
                var tuple = new Tuple<long, PartitionUpdateEvent>(originPosition, evt);

                if (!WaitingForConfirmation.TryGetValue(originPartition, out List<Tuple<long, PartitionUpdateEvent>> waitingList))
                {
                    WaitingForConfirmation[originPartition] = waitingList = new List<Tuple<long, PartitionUpdateEvent>>();
                }
                
                waitingList.Add(tuple);
            }

            public void ConfirmDependencyPersistence(PersistenceConfirmationEvent evt)
            {
                var originPartition = evt.OriginPartition;
                var originPosition = evt.OriginPosition;
                this.logWorker.traceHelper.FasterProgress($"Received PersistenceConfirmation message: (partition: {originPartition}, position: {originPosition})");

                // It must be the case that there exists an entry for this partition (except if we failed)
                if (this.WaitingForConfirmation.TryGetValue(originPartition, out List<Tuple<long, PartitionUpdateEvent>> waitingList))
                {
                    // TODO: Do this in a more elegant way. (Using filter?)
                    // TODO: If we do this with a forward pass and break early (assuming that list is increasing,
                    //       cost is amortized.
                    for (int i = waitingList.Count - 1; i >= 0; --i)
                    {
                        var tuple = waitingList[i];
                        if (tuple.Item1 <= originPosition)
                        {
                            tuple.Item2.EventHasNoUnconfirmeDependencies.SetResult(null);
                            waitingList.RemoveAt(i);
                        }
                    }
                }
            }

            protected override void WorkLoopCompleted(int batchSize, double elapsedMilliseconds, int? nextBatch)
            {
                this.logWorker.traceHelper.FasterProgress($"IntakeWorker completed batch: batchSize={batchSize} elapsedMilliseconds={elapsedMilliseconds} nextBatch={nextBatch}");
            }

        }

        public void SubmitInternalEvent(PartitionEvent evt)
        {
            this.intakeWorker.Submit(evt);
        }

        public void SubmitExternalEvents(IList<PartitionEvent> events, SemaphoreSlim credits)
        {
            this.intakeWorker.SubmitBatch(events, credits);
        }

      
        private void AddToFasterLog(byte[] bytes)
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

            // By turning this on here, we don't allow further events to be processed.
            // This means that persistence confirmations will not be sent to other partitions
            // from this partition, even though it might persist some events. 
            // The latest PersistenceConfirmation events will be sent once the 
            // partition recovers.
            this.isShuttingDown = true;

            await this.intakeWorker.WaitForCompletionAsync().ConfigureAwait(false);

            await this.WaitForCompletionAsync().ConfigureAwait(false);

            this.traceHelper.FasterProgress($"Stopped LogWorker");
        }

        private async Task CheckpointLog(int count, long latestConsistentAddress)
        {
            var stopwatch = new System.Diagnostics.Stopwatch();
            stopwatch.Start();
            long previous = log.CommittedUntilAddress;
            
            this.blobManager.LatestConsistentCommitLogPosition = latestConsistentAddress;
            await log.CommitAndWaitUntil(latestConsistentAddress);

            // Since the commit not been called (if it was already flushed by FASTER in a previous call)
            // we need to ensure that the metadata reflect the latest causally consistent commit.
            //
            // TODO: Could this lead to a race condition (if both FASTER and this call the TrySave concurrently)
            var newCommitMetadata = blobManager.ModifyCommitMetadataUntilAddress();
            blobManager.TrySaveCommitMetadata(newCommitMetadata);

            this.traceHelper.FasterLogPersisted(log.CommittedUntilAddress, count, (log.CommittedUntilAddress - previous), stopwatch.ElapsedMilliseconds);
        }


        private async Task CommitUntil(IList<PartitionUpdateEvent> batch, int from, int to)
        {
            var count = to - from;
            if (count > 0)
            {
                var latestConsistentAddress = batch[to - 1].NextCommitLogPosition;

                await this.CheckpointLog(count, latestConsistentAddress);

                // Now that the log is commited, we can send persistence confirmation events for
                // the commited events.
                //
                // TODO: (Optimization) Can we group and only send aggregate persistence confirmations?
                //       This could relieve the pressure on sending/receiving from Eventhubs
                for (var j = from; j < to; j++)
                {
                    // Q: Is this the right place to check for that, or should we do it earlier?
                    if (!(this.isShuttingDown || this.cancellationToken.IsCancellationRequested))
                    {
                        var currEvt = batch[j];

                        this.LastCommittedInputQueuePosition = Math.Max(this.LastCommittedInputQueuePosition, currEvt.NextInputQueuePosition);

                        try
                        {
                            DurabilityListeners.ConfirmDurable(currEvt);
                            // Possible optimization: Move persistence confirmation logic to the lower level and out of the application layer
                        }
                        catch (Exception exception) when (!(exception is OutOfMemoryException))
                        {
                            // for robustness, swallow exceptions, but report them
                            this.partition.ErrorHandler.HandleError("LogWorker.Process", $"Encountered exception while notifying persistence listeners for event {currEvt} id={currEvt.EventIdString}", exception, false, false);
                        }
                    }
                }
            }
        }

        protected override async Task Process(IList<PartitionUpdateEvent> batch)
        {
            try
            {
                if (batch.Count > 0)
                {
                    // Q: Could this be a problem that this here takes a long time possibly blocking

                    // Iteratively
                    // - Find the next event that has a dependency (by checking if their Task is set)
                    // - The ones before it can be safely commited.
                    // - For event that is commited we also inform its durability listener
                    // - Wait until the waiting for dependence is complete.
                    // - go back to step 1
                    var lastEnqueuedCommited = 0;
                    for (var i = 0; i < batch.Count; i++)
                    {
                        var evt = batch[i];
                        // TODO: Optimize by not allocating an object
                        if (!evt.EventHasNoUnconfirmeDependencies.Task.IsCompleted)
                        {
                            // we must commit our log (and send associated confirmations) BEFORE waiting for
                            // dependencies, otherwise we can get stuck in a circular wait-for pattern
                            await CommitUntil(batch, lastEnqueuedCommited, i);

                            // Progress the last commited index
                            lastEnqueuedCommited = i;

                            // Before continuing, wait for the dependencies of this update to be done, so that we can continue
                            await evt.EventHasNoUnconfirmeDependencies.Task;
                        }
                    }
                    await CommitUntil(batch, lastEnqueuedCommited, batch.Count);
                }
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

        protected override void WorkLoopCompleted(int batchSize, double elapsedMilliseconds, int? nextBatch)
        {
            this.traceHelper.FasterProgress($"LogWorker completed batch: batchSize={batchSize} elapsedMilliseconds={elapsedMilliseconds} nextBatch={nextBatch}");
        }

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
                            partitionEvent.NextCommitLogPosition = iter.NextAddress;
                            await worker.ReplayUpdate(partitionEvent).ConfigureAwait(false);
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
