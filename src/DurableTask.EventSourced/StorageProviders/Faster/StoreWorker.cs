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
    internal class StoreWorker : BatchWorker<object>
    {
        private readonly FasterKV store;
        private readonly Partition partition;
        private readonly FasterTraceHelper traceHelper;

        private readonly EffectTracker effects;

        private volatile TaskCompletionSource<bool> shutdownWaiter;

        private bool IsShuttingDown => this.shutdownWaiter != null || this.cancellationToken.IsCancellationRequested;

        public ulong InputQueuePosition { get; private set; }
        public ulong CommitLogPosition { get; private set; }

        private Task pendingIndexCheckpoint;
        private Task<ulong> pendingStoreCheckpoint;
        private ulong lastCheckpointedPosition;

        public StoreWorker(FasterKV store, Partition partition, FasterTraceHelper traceHelper, CancellationToken cancellationToken) 
            : base(cancellationToken)
        {
            this.store = store;
            this.partition = partition;
            this.traceHelper = traceHelper;

            // we are reusing the same effect tracker for all calls to reduce allocations
            this.effects = new EffectTracker(this.partition);
        }

        public async Task Initialize(ulong initialInputQueuePosition)
        {
            foreach (var key in TrackedObjectKey.GetSingletons())
            {
                var target = await store.GetOrCreate(key);

                target.OnFirstInitialization();

                if (target is DedupState dedupState)
                {
                    this.InputQueuePosition = dedupState.InputQueuePosition = initialInputQueuePosition;
                    this.CommitLogPosition = dedupState.CommitLogPosition = 0;
                }
            }

            this.lastCheckpointedPosition = this.CommitLogPosition;
        }

        public async Task Recover()
        {
            var dedupState = (DedupState) await store.GetOrCreate(TrackedObjectKey.Dedup);

            this.InputQueuePosition = dedupState.InputQueuePosition;
            this.CommitLogPosition = dedupState.CommitLogPosition;

            this.lastCheckpointedPosition = this.CommitLogPosition;
        }

        public async Task CancelAndShutdown()
        {
            this.traceHelper.FasterProgress("stopping StoreWorker");

            lock (this.thisLock)
            {
                this.shutdownWaiter = new TaskCompletionSource<bool>();
                this.Notify();
            }

            // waits for the currently processing entry to finish processing, or for termination
            await Task.WhenAny(this.shutdownWaiter.Task, Task.Delay(-1, this.cancellationToken));

            await this.SaveCurrentPositions();

            this.traceHelper.FasterProgress("stopped StoreWorker");
        }

        private ValueTask SaveCurrentPositions()
        {
            // write back the queue and log positions
            this.effects.Effect = this;
            return store.ProcessEffectOnTrackedObject(TrackedObjectKey.Dedup, this.effects);
        }

        protected override async Task Process(IList<object> batch)
        {
            try
            {
                foreach (var o in batch)
                {
                    if (this.IsShuttingDown)
                    {
                        // immediately stop processing requests and exit
                        this.shutdownWaiter?.TrySetResult(true);
                        return;
                    }

                    // if there are IO responses ready to process, do that first
                    this.store.CompletePending();

                    // now process the read or update
                    if (o is StorageAbstraction.IReadContinuation readContinuation)
                    {
                        try
                        {
                            this.store.Read(readContinuation, this.partition);
                        }
                        catch (Exception readException)
                        {
                            this.partition.HandleError($"Processing Read", readException, true);
                        }
                    }
                    else
                    {
                        partition.Assert(o is IPartitionEventWithSideEffects);
                        await this.ProcessEvent((PartitionEvent)o);
                    }
                }

                if (this.IsShuttingDown)
                {
                    this.shutdownWaiter?.TrySetResult(true);
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
                        await this.SaveCurrentPositions(); // must be stored back to dedup before taking store checkpoint
                        var token = this.store.StartStoreCheckpoint();
                        this.pendingStoreCheckpoint = this.WaitForCheckpointAsync("store checkpoint", token);
                    }
                }
                else if (this.lastCheckpointedPosition + this.partition.Settings.MaxLogDistanceBetweenCheckpointsInBytes <= this.CommitLogPosition)
                {
                    var token = this.store.StartIndexCheckpoint();
                    this.pendingIndexCheckpoint = this.WaitForCheckpointAsync("index checkpoint", token);
                }
            }
            catch (Exception e)
            {
                this.traceHelper.FasterStorageError("StoreWorker.Process", e);

                // if this happens during shutdown let the waiter know
                this.shutdownWaiter?.TrySetException(e);
            }
        }     

        public async Task<ulong> WaitForCheckpointAsync(string description, Guid checkpointToken)
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
            var stopwatch = new System.Diagnostics.Stopwatch();
            stopwatch.Start();

            var startPosition = this.CommitLogPosition;
            this.effects.IsReplaying = true;
            await logWorker.ReplayCommitLog(startPosition, this);
            stopwatch.Stop();
            this.partition.DetailTracer?.TraceDetail($"Event log replayed ({(this.CommitLogPosition - startPosition)/1024}kB) in {stopwatch.Elapsed.TotalSeconds}s");
            this.effects.IsReplaying = false;
        }

        public async ValueTask ProcessEvent(PartitionEvent partitionEvent)
        {
            if (partitionEvent.NextInputQueuePosition.HasValue && partitionEvent.NextInputQueuePosition.Value <= this.InputQueuePosition)
            {
                partition.DetailTracer?.TraceDetail($"Skipping duplicate input {partitionEvent.NextInputQueuePosition}");
                return;
            }

            try
            {
                this.partition.TraceProcess(partitionEvent, this.effects.IsReplaying);
                this.effects.Effect = partitionEvent;

                // collect the initial list of targets
                ((IPartitionEventWithSideEffects)partitionEvent).DetermineEffects(this.effects);

                // process until there are no more targets
                while (this.effects.Count > 0)
                {
                    await this.ProcessRecursively(partitionEvent);
                }

                // update the commit log and input queue positions
                if (partitionEvent.NextCommitLogPosition.HasValue)
                {
                    this.partition.Assert(partitionEvent.NextCommitLogPosition.Value > this.CommitLogPosition);
                    this.CommitLogPosition = partitionEvent.NextCommitLogPosition.Value;
                }
                if (partitionEvent.NextInputQueuePosition.HasValue)
                {
                    this.partition.Assert(partitionEvent.NextInputQueuePosition.Value > this.InputQueuePosition);
                    this.InputQueuePosition = partitionEvent.NextInputQueuePosition.Value;
                }

                partition.DetailTracer?.TraceDetail("finished processing event");
                this.effects.Effect = null;
                Partition.ClearTraceContext();
            }
            catch (Exception updateException)
            {
                this.partition.HandleError($"Processing Update", updateException, false);
                throw;
            }
        }
    
        public async ValueTask ProcessRecursively(PartitionEvent evt)
        {
            var startPos = this.effects.Count - 1;
            var key = this.effects[startPos];

            this.partition.DetailTracer?.TraceDetail($"Process on [{key}]");

            // start with processing the event on this object 
            await store.ProcessEffectOnTrackedObject(key, this.effects);
             
            // recursively process all additional objects to process
            while (effects.Count - 1 > startPos)
            {
                await this.ProcessRecursively(evt);
            }

            // pop this object as we are done processing
            effects.RemoveAt(startPos);
        }
    }
}
