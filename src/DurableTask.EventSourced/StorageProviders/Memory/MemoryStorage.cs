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

using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventSourced
{
    internal class MemoryStorage : BatchWorker<object>, StorageAbstraction.IPartitionState
    {
        private Partition partition;
        private long nextSubmitPosition = 0;
        private long commitPosition = 0;
        private long inputQueuePosition = 0;

        private ConcurrentDictionary<TrackedObjectKey, TrackedObject> trackedObjects
            = new ConcurrentDictionary<TrackedObjectKey, TrackedObject>();

        public MemoryStorage()
        {
            this.GetOrAdd(TrackedObjectKey.Activities);
            this.GetOrAdd(TrackedObjectKey.Dedup);
            this.GetOrAdd(TrackedObjectKey.Outbox);
            this.GetOrAdd(TrackedObjectKey.Reassembly);
            this.GetOrAdd(TrackedObjectKey.Sessions);
            this.GetOrAdd(TrackedObjectKey.Timers);
        }
        public CancellationToken Termination => CancellationToken.None;

        public void SubmitEvent(PartitionEvent entry)
        {
            entry.NextCommitLogPosition = ++nextSubmitPosition;
            base.Submit(entry);
        }

        public void SubmitExternalEvents(IEnumerable<PartitionEvent> entries)
        {
            foreach (var entry in entries)
            {
                entry.NextCommitLogPosition = ++nextSubmitPosition;
            }

            base.SubmitIncomingBatch(entries);
        }

        public void SubmitInternalReadonlyEvent(StorageAbstraction.IInternalReadonlyEvent readContinuation)
        {
            this.Submit(readContinuation);
        }

        public Task<long> CreateOrRestoreAsync(Partition partition, IPartitionErrorHandler termination, long initialInputQueuePosition)
        {
            this.partition = partition;

            foreach (var trackedObject in this.trackedObjects.Values)
            {
                trackedObject.Partition = partition;

                if (trackedObject.Key.IsSingleton)
                {
                    trackedObject.OnFirstInitialization();
                }
            }

            return Task.FromResult(0L);
        }

        public async Task CleanShutdown(bool takeFinalStateCheckpoint)
        {
            await Task.Delay(10);
            
            this.partition.ErrorHandler.TerminateNormally();
        }

        private TrackedObject GetOrAdd(TrackedObjectKey key)
        {
            var result = trackedObjects.GetOrAdd(key, TrackedObjectKey.Factory);
            result.Partition = this.partition;
            return result;
        }

        protected override async Task Process(IList<object> batch)
        {
            var effects = new EffectTracker(
                this.partition, 
                this.ApplyToStore,
                () => (this.commitPosition, this.inputQueuePosition),
                (c, i) => { this.commitPosition = c; this.inputQueuePosition = i; }
            );

            if (batch.Count != 0)
            {
                foreach (var o in batch)
                {
                    try
                    {
                        if (o is StorageAbstraction.IInternalReadonlyEvent readContinuation)
                        {
                            var readTarget = this.GetOrAdd(readContinuation.ReadTarget);
                            effects.ProcessRead(readContinuation, readTarget);
                        }
                        else
                        {
                            var partitionEvent = (PartitionEvent)o;
                            partitionEvent.NextCommitLogPosition = commitPosition + 1;
                            await effects.ProcessUpdate(partitionEvent);
                            AckListeners.Acknowledge(partitionEvent);
                        }
                    }
                    catch(Exception e)
                    {
                        partition.ErrorHandler.HandleError(nameof(Process), $"Encountered exception while processing event {o}", e, false, false);
                    }
                }
            }
        }

        public ValueTask ApplyToStore(TrackedObjectKey key, EffectTracker tracker)
        {
            tracker.ProcessEffectOn(this.GetOrAdd(key));
            return default;
        }
    }
}