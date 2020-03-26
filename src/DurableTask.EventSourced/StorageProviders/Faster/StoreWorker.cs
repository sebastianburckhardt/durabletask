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

using FASTER.core;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventSourced.Faster
{
    internal class StoreWorker : BatchWorker<PartitionEvent>
    {
        private readonly FasterKV store;
        private readonly Partition partition;
        private readonly FasterTraceHelper traceHelper;

        private readonly EffectTracker effectTracker;

        private volatile TaskCompletionSource<bool> shutdownWaiter;

        private bool IsShuttingDown => this.shutdownWaiter != null || this.cancellationToken.IsCancellationRequested;

        public long InputQueuePosition { get; private set; }
        public long CommitLogPosition { get; private set; }

        private Task pendingIndexCheckpoint;
        private Task<long> pendingStoreCheckpoint;
        private long lastCheckpointedPosition;
        private long numberEventsSinceLastCheckpoint;

        public StoreWorker(FasterKV store, Partition partition, FasterTraceHelper traceHelper, CancellationToken cancellationToken) 
            : base(cancellationToken)
        {
            this.store = store;
            this.partition = partition;
            this.traceHelper = traceHelper;

            // construct an effect tracker that we use to apply effects to the store
            this.effectTracker = new EffectTracker(
                this.partition,
                (key, tracker) => store.ProcessEffectOnTrackedObject(key, tracker),
                () => (this.CommitLogPosition, this.InputQueuePosition),
                (c, i) => { this.CommitLogPosition = c; this.InputQueuePosition = i; }
            );
        }

        public async Task Initialize(long initialCommitLogPosition, long initialInputQueuePosition)
        {
            this.InputQueuePosition = initialInputQueuePosition;
            this.CommitLogPosition = initialCommitLogPosition;

            foreach (var key in TrackedObjectKey.GetSingletons())
            {
                var target = await store.GetOrCreate(key);
                target.OnFirstInitialization();
            }

            this.lastCheckpointedPosition = this.CommitLogPosition;
            this.numberEventsSinceLastCheckpoint = initialCommitLogPosition;
        }

        public void ReadCheckpointPositions(BlobManager blobManager)
        {
            this.InputQueuePosition = blobManager.CheckpointInputQueuePosition;
            this.CommitLogPosition = blobManager.CheckpointCommitLogPosition;

            this.lastCheckpointedPosition = this.CommitLogPosition;
            this.numberEventsSinceLastCheckpoint = 0;
        }    

        public async Task CancelAndShutdown()
        {
            this.traceHelper.FasterProgress("stopping StoreWorker");

            lock (this.thisLock)
            {
                this.shutdownWaiter = new TaskCompletionSource<bool>();
                this.Notify();
            }

            // waits for the currently processing entry to finish processing
            await this.shutdownWaiter.Task;

            this.traceHelper.FasterProgress("stopped StoreWorker");
        }

        protected override async Task Process(IList<PartitionEvent> batch)
        {
            try
            {
                foreach (var partitionEvent in batch)
                {
                    if (this.IsShuttingDown)
                    {
                        // immediately stop processing requests and exit
                        return;
                    }

                    // if there are IO responses ready to process, do that first
                    this.store.CompletePending();

                    // record the current time, for measuring latency in the event processing pipeline
                    partitionEvent.IssuedTimestamp = this.partition.Stopwatch.Elapsed.TotalMilliseconds;

                    // now process the read or update
                    switch (partitionEvent)
                    {
                        case PartitionReadEvent readEvent:
                            readEvent.OnReadIssued(this.partition);
                            this.store.Read(readEvent, this.effectTracker);
                            // we don't await async reads, they complete when CompletePending() is called
                            break;

                        case PartitionUpdateEvent updateEvent:
                            await this.ProcessUpdate(updateEvent);
                            break;

                        default:
                            throw new InvalidCastException("could not cast to neither PartitionReadEvent nor PartitionUpdateEvent");
                    }
                }

                if (this.IsShuttingDown)
                {
                    return;
                }

                // handle progression of checkpointing state machine (none -> index pending -> store pending -> none)
                if (this.pendingStoreCheckpoint != null)
                {
                    if (this.pendingStoreCheckpoint.IsCompleted == true)
                    {
                        this.lastCheckpointedPosition = await this.pendingStoreCheckpoint; // observe exceptions here
                        this.pendingStoreCheckpoint = null;
                    }
                }
                else if (this.pendingIndexCheckpoint != null)
                {
                    if (this.pendingIndexCheckpoint.IsCompleted == true)
                    {
                        await this.pendingIndexCheckpoint; // observe exceptions here
                        this.pendingIndexCheckpoint = null;
                        var token = this.store.StartStoreCheckpoint(this.CommitLogPosition, this.InputQueuePosition);
                        this.pendingStoreCheckpoint = this.WaitForCheckpointAsync("store checkpoint", token);
                        this.numberEventsSinceLastCheckpoint = 0;
                    }
                }
                else if (this.lastCheckpointedPosition + this.partition.Settings.MaxNumberBytesBetweenCheckpoints <= this.CommitLogPosition
                    || this.numberEventsSinceLastCheckpoint > this.partition.Settings.MaxNumberEventsBetweenCheckpoints)
                {
                    var token = this.store.StartIndexCheckpoint();
                    this.pendingIndexCheckpoint = this.WaitForCheckpointAsync("index checkpoint", token);
                }
            }
            catch (OperationCanceledException) when (this.cancellationToken.IsCancellationRequested)
            {
                // o.k during termination
            }
            catch (Exception exception)
            {
                this.partition.ErrorHandler.HandleError("StoreWorker.Process", "Encountered exception while working on store", exception, true, false);
            }
            finally
            {
                this.shutdownWaiter?.TrySetResult(true);
            }
        }     

        public async Task<long> WaitForCheckpointAsync(string description, Guid checkpointToken)
        {
            var stopwatch = new System.Diagnostics.Stopwatch();
            stopwatch.Start();
            var commitLogPosition = this.CommitLogPosition;
            var inputQueuePosition = this.InputQueuePosition;
            this.traceHelper.FasterCheckpointStarted(checkpointToken, description, commitLogPosition, inputQueuePosition);
            await store.CompleteCheckpointAsync();
            this.traceHelper.FasterCheckpointPersisted(checkpointToken, description, commitLogPosition, inputQueuePosition, stopwatch.ElapsedMilliseconds);
            this.Notify();
            return commitLogPosition;
        }

        public async Task ReplayCommitLog(LogWorker logWorker)
        {
            this.traceHelper.FasterProgress("replaying log");

            var stopwatch = new System.Diagnostics.Stopwatch();
            stopwatch.Start();

            var startPosition = this.CommitLogPosition;
            this.effectTracker.IsReplaying = true;
            await logWorker.ReplayCommitLog(startPosition, this);
            stopwatch.Stop();
            this.effectTracker.IsReplaying = false;

            this.traceHelper.FasterLogReplayed(this.CommitLogPosition, this.InputQueuePosition, this.numberEventsSinceLastCheckpoint, this.CommitLogPosition - startPosition, stopwatch.ElapsedMilliseconds);
        }

        public async ValueTask ProcessUpdate(PartitionUpdateEvent partitionEvent)
        {
            // the transport layer should always deliver a fresh event; if it repeats itself that's a bug
            // (note that it may not be the very next in the sequence since readonly events are not persisted in the store)
            if (partitionEvent.NextInputQueuePosition > 0 && partitionEvent.NextInputQueuePosition <= this.InputQueuePosition)
            {
                this.partition.ErrorHandler.HandleError(nameof(ProcessUpdate), "Duplicate event detected", null, false, false);
                return;
            }

            await this.effectTracker.ProcessUpdate(partitionEvent);
            this.numberEventsSinceLastCheckpoint++;
        }
    }
}
