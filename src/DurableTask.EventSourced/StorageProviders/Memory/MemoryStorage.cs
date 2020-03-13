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
        private ulong nextSubmitPosition = 0;
        private ulong nextCommitPosition = 0;
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

        public void Submit(PartitionEvent entry)
        {
            entry.NextCommitLogPosition = nextSubmitPosition++;
            base.Submit(entry);
        }

        public void SubmitInputEvents(IEnumerable<PartitionEvent> entries)
        {
            foreach (var entry in entries)
            {
                entry.NextCommitLogPosition = nextSubmitPosition++;
            }

            base.SubmitIncomingBatch(entries);
        }

        public void ScheduleRead(StorageAbstraction.IReadContinuation readContinuation)
        {
            this.Submit(readContinuation);
        }

        public Task<ulong> CreateOrRestoreAsync(Partition partition, IPartitionErrorHandler termination, ulong initialInputQueuePosition)
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

            return Task.FromResult(0UL);
        }

        public Task CleanShutdown(bool takeFinalStateCheckpoint)
        {
            return Task.Delay(10);
        }

        private TrackedObject GetOrAdd(TrackedObjectKey key)
        {
            var result = trackedObjects.GetOrAdd(key, TrackedObjectKey.Factory);
            result.Partition = this.partition;
            return result;
        }

        protected override Task Process(IList<object> batch)
        {
            var effects = new EffectTracker(this.partition);

            if (batch.Count != 0)
            {
                foreach (var o in batch)
                {
                    try
                    {
                        if (o is StorageAbstraction.IReadContinuation readContinuation)
                        {
                            var readTarget = this.GetOrAdd(readContinuation.ReadTarget);
                            
                            var partitionEvent = readContinuation as PartitionEvent;

                            if (partitionEvent != null)
                            {
                                partition.TraceProcess(partitionEvent, false);
                            }

                            readContinuation.OnReadComplete(readTarget);

                            if (partitionEvent != null)
                            {
                                Partition.ClearTraceContext();
                            }
                        }
                        else
                        {
                            partition.Assert(o is IPartitionEventWithSideEffects);
                            var partitionEvent = (PartitionEvent)o;

                            partitionEvent.NextCommitLogPosition = nextCommitPosition++;
                            partition.TraceProcess(partitionEvent, false);
                            effects.Effect = partitionEvent;

                            // determine the effects and apply all the updates
                            ((IPartitionEventWithSideEffects)partitionEvent).DetermineEffects(effects);
                            while (effects.Count > 0)
                            {
                                this.ProcessRecursively(partitionEvent, effects);
                            }

                            partition.DetailTracer?.TraceDetail("finished processing event");
                            effects.Effect = null;
                            Partition.ClearTraceContext();

                            AckListeners.Acknowledge(partitionEvent);
                        }
                    }
                    catch(Exception e)
                    {
                        partition.ErrorHandler.HandleError(nameof(Process), $"error while processing {o}", e, false, false);
                    }
                }
            }

            return Task.CompletedTask;
        }

        public void ProcessRecursively(PartitionEvent evt, EffectTracker effects)
        {
            // process the last effect in the list, and recursively any effects it adds
            var startPos = effects.Count - 1;
            var key = effects[startPos];

            this.partition.DetailTracer?.TraceDetail($"Process on [{key}]");

            // start with processing the event on this object, which
            // updates its state and can flag more objects to process on
            effects.ProcessEffectOn(this.GetOrAdd(key));

            // recursively process all additional objects to process
            while (effects.Count - 1 > startPos)
            {
                this.ProcessRecursively(evt, effects);
            }

            effects.RemoveAt(startPos);
        }
    }
}