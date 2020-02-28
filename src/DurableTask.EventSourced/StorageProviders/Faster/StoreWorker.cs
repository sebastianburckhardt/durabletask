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
        private readonly TraceHelper traceHelper;

        private readonly EffectTracker effects;

        private volatile TaskCompletionSource<bool> cancellationWaiter;

        private bool IsShuttingDown => this.cancellationWaiter != null;

        public ulong InputQueuePosition { get; private set; }
        public ulong CommitLogPosition { get; private set; }

        public StoreWorker(FasterKV store, Partition partition, TraceHelper traceHelper)
        {
            this.store = store;
            this.partition = partition;
            this.traceHelper = traceHelper;

            // we are reusing the same effect tracker for all calls to reduce allocations
            this.effects = new EffectTracker(this.partition);
        }

        public async Task Initialize()
        {
            foreach (var key in TrackedObjectKey.GetSingletons())
            {
                var target = await store.GetOrCreate(key);

                target.OnFirstInitialization();

                if (target is DedupState dedupState)
                {
                    this.InputQueuePosition = dedupState.InputQueuePosition = 0;
                    this.CommitLogPosition = dedupState.CommitLogPosition = 0;
                }
            }
        }

        public async Task Recover()
        {
            var dedupState = (DedupState) await store.GetOrCreate(TrackedObjectKey.Dedup);

            this.InputQueuePosition = dedupState.InputQueuePosition;
            this.CommitLogPosition = dedupState.CommitLogPosition;

            //var dump = await this.store.DumpCurrentState();
        }

        public async Task CancelAndShutdown()
        {
            this.traceHelper.FasterProgress("stopping StoreWorker");

            lock (this.thisLock)
            {
                this.cancellationWaiter = new TaskCompletionSource<bool>();
                this.Notify();
            }

            // waits for the currently processing entry to finish processing
            await this.cancellationWaiter.Task;

            // write back the queue and log positions
            this.effects.Effect = this;
            await store.ProcessEffectOnTrackedObject(TrackedObjectKey.Dedup, this.effects);

            //var dump = await this.store.DumpCurrentState();

            this.traceHelper.FasterProgress("stopped StoreWorker");
        }

        protected override async Task Process(IList<object> batch)
        {
            foreach (var o in batch)
            {
                if (this.IsShuttingDown)
                {
                    break; // stop processing sooner rather than later
                }

                // if there are IO responses ready to process, do that first
                this.store.CompletePending();

                if (o is StorageAbstraction.IReadContinuation readContinuation)
                {
                    try
                    {
                        store.Read(readContinuation, this.partition);
                    }
                    catch (Exception readException)
                    {
                        partition.ReportError($"Processing Read", readException);
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
                // at this point we know we will not process any more reads or updates
                this.cancellationWaiter.TrySetResult(true);
            }
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
            if (partitionEvent.InputQueuePosition.HasValue && partitionEvent.InputQueuePosition.Value <= this.InputQueuePosition)
            {
                partition.DetailTracer?.TraceDetail($"Skipping duplicate input {partitionEvent.InputQueuePosition}");
                return;
            }

            try
            {
                this.partition.TraceProcess(partitionEvent);
                this.effects.Effect = partitionEvent;

                // collect the initial list of targets
                ((IPartitionEventWithSideEffects)partitionEvent).DetermineEffects(this.effects);

                // process until there are no more targets
                while (this.effects.Count > 0)
                {
                    await this.ProcessRecursively(partitionEvent);
                }

                // update the commit log and input queue positions
                if (partitionEvent.CommitLogPosition.HasValue)
                {
                    this.partition.Assert(partitionEvent.CommitLogPosition.Value > this.CommitLogPosition);
                    this.CommitLogPosition = partitionEvent.CommitLogPosition.Value;
                }
                if (partitionEvent.InputQueuePosition.HasValue)
                {
                    this.partition.Assert(partitionEvent.InputQueuePosition.Value > this.InputQueuePosition);
                    this.InputQueuePosition = partitionEvent.InputQueuePosition.Value;
                }

                partition.DetailTracer?.TraceDetail("finished processing event");
                this.effects.Effect = null;
                Partition.ClearTraceContext();
            }
            catch (Exception updateException)
            {
                this.partition.ReportError($"Processing Update", updateException);
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
