﻿//  ----------------------------------------------------------------------------------
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

using DurableTask.EventSourced.Scaling;
using FASTER.core;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventSourced.Faster
{
    internal class StoreWorker : BatchWorker<PartitionEvent>
    {
        private readonly TrackedObjectStore store;
        private readonly Partition partition;
        private readonly FasterTraceHelper traceHelper;
        private readonly BlobManager blobManager;

        private readonly EffectTracker effectTracker;
        
        private bool isShuttingDown;

        public long InputQueuePosition { get; private set; }
        public long CommitLogPosition { get; private set; }

        public LogWorker LogWorker { get; set; }

        // periodic index and store checkpointing
        private CheckpointTrigger pendingCheckpointTrigger;
        private Task pendingIndexCheckpoint;
        private Task<(long,long)> pendingStoreCheckpoint;
        private long lastCheckpointedInputQueuePosition;
        private long lastCheckpointedCommitLogPosition;
        private long numberEventsSinceLastCheckpoint;
        private double timeOfLastCheckpoint;
        private double maxTimeOfNextCheckpoint;

        // periodic load publishing
        private long lastPublishedCommitLogPosition = 0;
        private long lastPublishedInputQueuePosition = 0;
        private string lastPublishedLatencyTrend = "";
        private DateTime lastPublishedTime = DateTime.MinValue;
        public static TimeSpan PublishInterval = TimeSpan.FromSeconds(10);
        public static TimeSpan IdlingPeriod = TimeSpan.FromSeconds(2);

        private bool isInputQueuePositionPersisted => 
            this.InputQueuePosition <= Math.Max(this.lastCheckpointedInputQueuePosition, this.LogWorker.LastCommittedInputQueuePosition);

        private double ComputeMaxTimeOfNextCheckpoint => 
            // we randomize this so that partitions don't all decide to checkpoint at the exact same time
            this.timeOfLastCheckpoint + this.partition.Settings.MaxTimeMsBetweenCheckpoints * (0.8 + 0.2 * new Random().NextDouble());


        public StoreWorker(TrackedObjectStore store, Partition partition, FasterTraceHelper traceHelper, BlobManager blobManager, CancellationToken cancellationToken) 
            : base(cancellationToken)
        {
            partition.ErrorHandler.Token.ThrowIfCancellationRequested();

            this.store = store;
            this.partition = partition;
            this.traceHelper = traceHelper;
            this.blobManager = blobManager;

            // construct an effect tracker that we use to apply effects to the store
            this.effectTracker = new EffectTracker(
                this.partition,
                (key, tracker) => store.ProcessEffectOnTrackedObject(key, tracker),
                () => (this.CommitLogPosition, this.InputQueuePosition)
            );
        }

        public async Task Initialize(long initialCommitLogPosition, long initialInputQueuePosition)
        {
            this.partition.ErrorHandler.Token.ThrowIfCancellationRequested();

            this.InputQueuePosition = initialInputQueuePosition;
            this.CommitLogPosition = initialCommitLogPosition;
           
            this.store.InitMainSession();

            foreach (var key in TrackedObjectKey.GetSingletons())
            {
                var target = await this.store.CreateAsync(key).ConfigureAwait(false);
                target.OnFirstInitialization();
            }

            this.lastCheckpointedCommitLogPosition = this.CommitLogPosition;
            this.lastCheckpointedInputQueuePosition = this.InputQueuePosition;
            this.numberEventsSinceLastCheckpoint = initialCommitLogPosition;
        }

        public void SetCheckpointPositionsAfterRecovery(long commitLogPosition, long inputQueuePosition)
        {
            this.CommitLogPosition = commitLogPosition;
            this.InputQueuePosition = inputQueuePosition;

            this.lastCheckpointedCommitLogPosition = this.CommitLogPosition;
            this.lastCheckpointedInputQueuePosition = this.InputQueuePosition;
            this.numberEventsSinceLastCheckpoint = 0;
            this.timeOfLastCheckpoint = this.partition.CurrentTimeMs;
            this.maxTimeOfNextCheckpoint = this.ComputeMaxTimeOfNextCheckpoint;
        }

        internal async ValueTask TakeFullCheckpointAsync(string reason)
        {
            var stopwatch = new System.Diagnostics.Stopwatch();
            stopwatch.Start();

            if (this.store.TakeFullCheckpoint(this.CommitLogPosition, this.InputQueuePosition, out var checkpointGuid))
            {
                this.traceHelper.FasterCheckpointStarted(checkpointGuid, reason, this.store.StoreStats.Get(), this.CommitLogPosition, this.InputQueuePosition);

                // do the faster full checkpoint and then finalize it
                await this.store.CompleteCheckpointAsync().ConfigureAwait(false);
                await this.store.FinalizeCheckpointCompletedAsync(checkpointGuid).ConfigureAwait(false);

                this.lastCheckpointedCommitLogPosition = this.CommitLogPosition;
                this.lastCheckpointedInputQueuePosition = this.InputQueuePosition;
                this.numberEventsSinceLastCheckpoint = 0;

                this.traceHelper.FasterCheckpointPersisted(checkpointGuid, reason, this.CommitLogPosition, this.InputQueuePosition, stopwatch.ElapsedMilliseconds);
            }
            else
            {
                this.traceHelper.FasterProgress($"Checkpoint skipped: {reason}");
            }

            this.timeOfLastCheckpoint = this.partition.CurrentTimeMs;
            this.maxTimeOfNextCheckpoint = this.ComputeMaxTimeOfNextCheckpoint;
        }

        public async Task CancelAndShutdown()
        {
            this.traceHelper.FasterProgress("Stopping StoreWorker");

            this.isShuttingDown = true;

            await this.WaitForCompletionAsync().ConfigureAwait(false);

            // wait for any in-flight checkpoints. It is unlikely but not impossible.
            if (this.pendingIndexCheckpoint != null)
            {
                await this.pendingIndexCheckpoint.ConfigureAwait(false);
            }
            if (this.pendingStoreCheckpoint != null)
            {
                await this.pendingStoreCheckpoint.ConfigureAwait(false);
            }

            this.traceHelper.FasterProgress("Stopped StoreWorker");
        }
       
        private async Task PublishPartitionLoad()
        {
            var info = new PartitionLoadInfo()
            {
                CommitLogPosition = this.CommitLogPosition,
                InputQueuePosition = this.InputQueuePosition,
                WorkerId = this.partition.Settings.WorkerId,
                LatencyTrend = this.lastPublishedLatencyTrend,
                MissRate = this.store.StoreStats.GetMissRate(),
            };
            foreach (var k in TrackedObjectKey.GetSingletons())
            {
                (await this.store.ReadAsync(k, this.effectTracker).ConfigureAwait(false))?.UpdateLoadInfo(info);
            }

            this.UpdateLatencyTrend(info);
             
            // to avoid unnecessary traffic for statically provisioned deployments,
            // suppress load publishing if the state is not changing
            if (info.CommitLogPosition == this.lastPublishedCommitLogPosition 
                && info.InputQueuePosition == this.lastPublishedInputQueuePosition
                && info.LatencyTrend == this.lastPublishedLatencyTrend)
            {
                return;
            }

            // to avoid publishing not-yet committed state, publish
            // only after the current log is persisted.
            var task = this.LogWorker.WaitForCompletionAsync()
                .ContinueWith((t) => partition.LoadPublisher?.Submit((this.partition.PartitionId, info)));

            this.lastPublishedCommitLogPosition = this.CommitLogPosition;
            this.lastPublishedInputQueuePosition = this.InputQueuePosition;
            this.lastPublishedLatencyTrend = info.LatencyTrend;
            this.lastPublishedTime = DateTime.UtcNow;

            this.partition.TraceHelper.TracePartitionLoad(info);
        }

        private void UpdateLatencyTrend(PartitionLoadInfo info)
        {
            int activityLatencyCategory;

            if (info.Activities == 0)
            {
                activityLatencyCategory = 0;
            }
            else if (info.ActivityLatencyMs < 100)
            {
                activityLatencyCategory = 1;
            }
            else if (info.ActivityLatencyMs < 1000)
            {
                activityLatencyCategory = 2;
            }
            else
            {
                activityLatencyCategory = 3;
            }

            int workItemLatencyCategory;

            if (info.WorkItems == 0)
            {
                workItemLatencyCategory = 0;
            }
            else if (info.WorkItemLatencyMs < 100)
            {
                workItemLatencyCategory = 1;
            }
            else if (info.WorkItemLatencyMs < 1000)
            {
                workItemLatencyCategory = 2;
            }
            else
            {
                workItemLatencyCategory = 3;
            }

            if (info.LatencyTrend.Length == PartitionLoadInfo.LatencyTrendLength)
            {
                info.LatencyTrend = info.LatencyTrend.Substring(1, PartitionLoadInfo.LatencyTrendLength - 1);
            }

            info.LatencyTrend = info.LatencyTrend
                + PartitionLoadInfo.LatencyCategories[Math.Max(activityLatencyCategory, workItemLatencyCategory)];         
        }

        private enum CheckpointTrigger
        {
            None,
            CommitLogBytes,
            EventCount,
            TimeElapsed
        }

        private bool CheckpointDue(out CheckpointTrigger trigger)
        {
            trigger = CheckpointTrigger.None;

            if (this.lastCheckpointedCommitLogPosition + this.partition.Settings.MaxNumberBytesBetweenCheckpoints <= this.CommitLogPosition)
            {
                trigger = CheckpointTrigger.CommitLogBytes;
            }
            else if (this.numberEventsSinceLastCheckpoint > this.partition.Settings.MaxNumberEventsBetweenCheckpoints)
            {
                trigger = CheckpointTrigger.EventCount;
            }
            else if ((this.numberEventsSinceLastCheckpoint > 0 || !this.isInputQueuePositionPersisted)
                && this.partition.CurrentTimeMs > this.maxTimeOfNextCheckpoint)
            {
                trigger = CheckpointTrigger.TimeElapsed;
            }
             
            return trigger != CheckpointTrigger.None;
        }

        protected override async Task Process(IList<PartitionEvent> batch)
        {
            try
            {
                foreach (var partitionEvent in batch)
                {
                    if (this.isShuttingDown || this.cancellationToken.IsCancellationRequested)
                    {
                        return;
                    }

                    // if there are IO responses ready to process, do that first
                    this.store.CompletePending();

                    // record the current time, for measuring latency in the event processing pipeline
                    partitionEvent.IssuedTimestamp = this.partition.CurrentTimeMs;

                    // now process the read or update
                    switch (partitionEvent)
                    {
                        case PartitionUpdateEvent updateEvent:
                            await this.ProcessUpdate(updateEvent).ConfigureAwait(false);

                            if (updateEvent.NextCommitLogPosition > 0)
                            {
                                this.partition.Assert(updateEvent.NextCommitLogPosition > this.CommitLogPosition);
                                this.CommitLogPosition = updateEvent.NextCommitLogPosition;
                            }
                            break;

                        case PartitionReadEvent readEvent:
                            readEvent.OnReadIssued(this.partition);
                            // async reads may either complete immediately (on cache hit) or later (on cache miss) when CompletePending() is called
                            this.store.ReadAsync(readEvent, this.effectTracker);
                            break;

                        case PartitionQueryEvent queryEvent:
                            // async queries execute on their own task and their own session
                            Task ignored = Task.Run(() => this.store.QueryAsync(queryEvent, this.effectTracker));
                            break;

                        default:
                            throw new InvalidCastException("could not cast to neither PartitionReadEvent nor PartitionUpdateEvent");
                    }
                    
                    if (partitionEvent.NextInputQueuePosition > 0)
                    {
                        this.partition.Assert(partitionEvent.NextInputQueuePosition > this.InputQueuePosition);
                        this.InputQueuePosition = partitionEvent.NextInputQueuePosition;
                    }
                }

                if (this.isShuttingDown || this.cancellationToken.IsCancellationRequested)
                {
                    return;
                }

                // handle progression of checkpointing state machine (none -> index pending -> store pending -> none)
                if (this.pendingStoreCheckpoint != null)
                {
                    if (this.pendingStoreCheckpoint.IsCompleted == true)
                    {
                        (this.lastCheckpointedCommitLogPosition, this.lastCheckpointedInputQueuePosition)
                            = await this.pendingStoreCheckpoint.ConfigureAwait(false); // observe exceptions here
                        this.pendingStoreCheckpoint = null;
                        this.pendingCheckpointTrigger = CheckpointTrigger.None;
                        this.timeOfLastCheckpoint = this.partition.CurrentTimeMs;
                        this.maxTimeOfNextCheckpoint = this.ComputeMaxTimeOfNextCheckpoint;
                    }
                }
                else if (this.pendingIndexCheckpoint != null)
                {
                    if (this.pendingIndexCheckpoint.IsCompleted == true)
                    {
                        await this.pendingIndexCheckpoint.ConfigureAwait(false); // observe exceptions here
                        this.pendingIndexCheckpoint = null;
                        var token = this.store.StartStoreCheckpoint(this.CommitLogPosition, this.InputQueuePosition);
                        this.pendingStoreCheckpoint = this.WaitForCheckpointAsync(false, token);
                        this.numberEventsSinceLastCheckpoint = 0;
                    }
                }
                else if (this.CheckpointDue(out var trigger))
                {
                    var token = this.store.StartIndexCheckpoint();
                    this.pendingCheckpointTrigger = trigger;
                    this.pendingIndexCheckpoint = this.WaitForCheckpointAsync(true, token);
                }
                
                if (this.lastPublishedTime + PublishInterval < DateTime.UtcNow)
                {
                    await this.PublishPartitionLoad().ConfigureAwait(false);
                }

                if (this.lastCheckpointedCommitLogPosition == this.CommitLogPosition 
                    && this.lastCheckpointedInputQueuePosition == this.InputQueuePosition
                    && this.LogWorker.LastCommittedInputQueuePosition <= this.InputQueuePosition)
                {
                    this.timeOfLastCheckpoint = this.partition.CurrentTimeMs; // nothing has changed since the last checkpoint
                    this.maxTimeOfNextCheckpoint = this.ComputeMaxTimeOfNextCheckpoint;
                }

                // make sure to complete ready read requests, or notify this worker
                // if any read requests become ready to process at some point
                var t = this.store.ReadyToCompletePendingAsync();
                if (t.IsCompleted)
                {
                    store.CompletePending();
                }
                else
                {
                    var ignoredTask = t.AsTask().ContinueWith(x => this.Notify());
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
        }     

        public async Task<(long,long)> WaitForCheckpointAsync(bool isIndexCheckpoint, Guid checkpointToken)
        {
            var stopwatch = new System.Diagnostics.Stopwatch();
            stopwatch.Start();
            var commitLogPosition = this.CommitLogPosition;
            var inputQueuePosition = this.InputQueuePosition;
            string description = $"{(isIndexCheckpoint ? "index" : "store")} checkpoint triggered by {this.pendingCheckpointTrigger}";
            this.traceHelper.FasterCheckpointStarted(checkpointToken, description, this.store.StoreStats.Get(), commitLogPosition, inputQueuePosition);

            // first do the faster checkpoint
            await store.CompleteCheckpointAsync().ConfigureAwait(false);

            if (!isIndexCheckpoint)
            {
                // wait for the commit log so it is never behind the checkpoint
                await this.LogWorker.WaitForCompletionAsync().ConfigureAwait(false);

                // finally we write the checkpoint info file
                await this.store.FinalizeCheckpointCompletedAsync(checkpointToken).ConfigureAwait(false);
            }
 
            this.traceHelper.FasterCheckpointPersisted(checkpointToken, description, commitLogPosition, inputQueuePosition, stopwatch.ElapsedMilliseconds);

            this.Notify();
            return (commitLogPosition, inputQueuePosition);
        }

        public async Task ReplayCommitLog(LogWorker logWorker)
        {
            this.traceHelper.FasterProgress("Replaying log");

            var stopwatch = new System.Diagnostics.Stopwatch();
            stopwatch.Start();

            var startPosition = this.CommitLogPosition;
            this.effectTracker.IsReplaying = true;
            await logWorker.ReplayCommitLog(startPosition, this).ConfigureAwait(false);
            stopwatch.Stop();
            this.effectTracker.IsReplaying = false;

            this.traceHelper.FasterLogReplayed(this.CommitLogPosition, this.InputQueuePosition, this.numberEventsSinceLastCheckpoint, this.CommitLogPosition - startPosition, this.store.StoreStats.Get(), stopwatch.ElapsedMilliseconds);
        }

        public async Task RestartThingsAtEndOfRecovery()
        {
            using (EventTraceContext.MakeContext(this.CommitLogPosition, string.Empty))
            {
                foreach (var key in TrackedObjectKey.GetSingletons())
                {
                    var target = (TrackedObject)await store.ReadAsync(key, this.effectTracker).ConfigureAwait(false);
                    target.OnRecoveryCompleted();
                }
            }
        }

        public async ValueTask ProcessUpdate(PartitionUpdateEvent partitionEvent)
        {
            // the transport layer should always deliver a fresh event; if it repeats itself that's a bug
            // (note that it may not be the very next in the sequence since readonly events are not persisted in the log)
            if (partitionEvent.NextInputQueuePosition > 0 && partitionEvent.NextInputQueuePosition <= this.InputQueuePosition)
            {
                this.partition.ErrorHandler.HandleError(nameof(ProcessUpdate), "Duplicate event detected", null, false, false);
                return;
            }

            await this.effectTracker.ProcessUpdate(partitionEvent).ConfigureAwait(false);
            this.numberEventsSinceLastCheckpoint++;
        }
    }
}
