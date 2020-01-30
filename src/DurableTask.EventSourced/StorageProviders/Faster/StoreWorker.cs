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

        private readonly TrackedObject.EffectList effects;

        private volatile TaskCompletionSource<bool> cancellationWaiter;

        private bool IsShuttingDown => this.cancellationWaiter != null;

        private long inputQueuePosition;
        private long commitLogPosition;

        public StoreWorker(FasterKV store, Partition partition)
        {
            this.store = store;
            this.partition = partition;

            // we are reusing the same effect list for all calls to reduce allocations
            this.effects = new TrackedObject.EffectList(this.partition);
        }

        public async Task Initialize()
        {
            foreach (var key in TrackedObjectKey.GetSingletons())
            {
                var target = await store.GetOrCreate(key);

                if (target is DedupState dedupState)
                {
                    this.inputQueuePosition = dedupState.InputQueuePosition;
                    this.commitLogPosition = dedupState.CommitLogPosition;
                }
            }

            if (this.commitLogPosition == -1)
            {
                this.commitLogPosition = 0; // the first position in FasterLog is larger than 0, so 0 is our before-the-first value
            }
        }

        public async Task CancelAndShutdown()
        {
            lock (this.thisLock)
            {
                this.cancellationWaiter = new TaskCompletionSource<bool>();
                this.Notify();
            }

            // waits for the currently processing entry to finish processing
            await this.cancellationWaiter.Task; 

            // write back the queue and log positions
            var dedup = (DedupState)await store.GetOrCreate(TrackedObjectKey.Dedup);
            dedup.InputQueuePosition = this.inputQueuePosition;
            dedup.CommitLogPosition = this.commitLogPosition;
            await store.MarkWritten(TrackedObjectKey.Dedup);
        }

        protected override async Task Process(IList<object> batch)
        {
            foreach (var o in batch)
            {
                if (this.IsShuttingDown)
                {
                    break; // stop processing sooner rather than later
                }

                if (o is PartitionEvent partitionEvent)
                {
                    await this.ProcessEvent(partitionEvent);
                }
                else
                {
                    try
                    {
                        store.Read((StorageAbstraction.IReadContinuation)o, this.partition);
                    }
                    catch (Exception readException)
                    {
                        partition.ReportError($"Processing Read", readException);
                    }
                }
            }

            if (this.IsShuttingDown)
            {
                // at this point we know we will not process any more reads or updates
                this.cancellationWaiter.TrySetResult(true);
            }
        }

        public async Task ReplayCommitLog(FasterLog log)
        {
            this.effects.InRecovery = true;
            await ReplayCommitLog(this.commitLogPosition, log.TailAddress);
            this.partition.DiagnosticsTrace($"Commit log replay complete");
            this.effects.InRecovery = false;

            async Task ReplayCommitLog(long from, long to)
            {
                using (var iter = log.Scan(from, to))
                {
                    byte[] result;
                    int entryLength;
                    long currentAddress;

                    while (true)
                    {
                        while (!iter.GetNext(out result, out entryLength, out currentAddress))
                        {
                            if (currentAddress >= to)
                            {
                                return;
                            }
                            await iter.WaitAsync();
                        }

                        var partitionEvent = (PartitionEvent)Serializer.DeserializeEvent(new ArraySegment<byte>(result, 0, entryLength));
                        partitionEvent.CommitLogPosition = currentAddress;
                        await this.ProcessEvent(partitionEvent);
                    }
                }
            }
        }

        public async Task<long> FinishRecovery()
        {
            foreach (var key in TrackedObjectKey.GetSingletons())
            {
                var target = (TrackedObject)await store.GetOrCreate(key);
                target.OnRecoveryCompleted();
            }

            this.partition.DiagnosticsTrace($"Recovery complete {this.inputQueuePosition}");

            return this.inputQueuePosition;
        }

        public async ValueTask ProcessEvent(PartitionEvent partitionEvent)
        {
            if (partitionEvent.InputQueuePosition.HasValue && partitionEvent.InputQueuePosition.Value <= this.inputQueuePosition)
            {
                partition.DiagnosticsTrace($"Skipping duplicate input {partitionEvent.InputQueuePosition}");
            }

            try
            {
                this.partition.TraceProcess(partitionEvent);
                partitionEvent.DetermineEffects(this.effects);
                while (this.effects.Count > 0)
                {
                    await this.ProcessRecursively(partitionEvent);
                }
                if (partitionEvent.CommitLogPosition.HasValue)
                {
                    this.partition.Assert(partitionEvent.CommitLogPosition.Value > this.commitLogPosition);
                    this.commitLogPosition = partitionEvent.CommitLogPosition.Value;
                }
                if (partitionEvent.InputQueuePosition.HasValue)
                {
                    partition.Assert(partitionEvent.InputQueuePosition.Value > this.inputQueuePosition);
                    this.inputQueuePosition = partitionEvent.InputQueuePosition.Value;
                }
                partition.DiagnosticsTrace($"Processing complete {partitionEvent.InputQueuePosition}");
                Partition.TraceContext = null;
            }
            catch (Exception updateException)
            {
                partition.ReportError($"Processing Update", updateException);
                throw;
            }
        }
    
        public async ValueTask ProcessRecursively(PartitionEvent evt)
        {
            var startPos = this.effects.Count - 1;
            var thisKey = this.effects[startPos];
            var thisObject = await store.GetOrCreate(thisKey);

            if (EtwSource.EmitDiagnosticsTrace)
            {
                partition.DiagnosticsTrace($"Process on [{thisObject.Key}]");
            }

            // start with processing the event on this object, which
            // updates its state and can flag more objects to process on
            dynamic dynamicThis = thisObject;
            dynamic dynamicPartitionEvent = evt;
            dynamicThis.Process(dynamicPartitionEvent, this.effects);

            // tell Faster that this object was modified
            await store.MarkWritten(thisKey);

            // recursively process all additional objects to process
            while (effects.Count - 1 > startPos)
            {
                await this.ProcessRecursively(evt);
            }

            effects.RemoveAt(startPos);
        }
    }
}
