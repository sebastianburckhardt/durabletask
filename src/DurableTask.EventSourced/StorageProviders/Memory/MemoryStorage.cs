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
        private long nextCommitPosition = 0;
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
        public CancellationToken OwnershipCancellationToken => CancellationToken.None;

        public void Submit(PartitionEvent entry)
        {
            entry.CommitLogPosition = nextSubmitPosition++;
            this.partition.TraceSubmit(entry);
            base.Submit(entry);
        }

        public void SubmitRange(IEnumerable<PartitionEvent> entries)
        {
            foreach (var entry in entries)
            {
                entry.CommitLogPosition = nextSubmitPosition++;
                this.partition.TraceSubmit(entry);
            }

            base.SubmitRange(entries);
        }

        public void ScheduleRead(StorageAbstraction.IReadContinuation readContinuation)
        {
            this.Submit(readContinuation);
        }

        public Task<long> RestoreAsync(Partition partition)
        {
            this.partition = partition;

            foreach (var trackedObject in this.trackedObjects.Values)
            {
                trackedObject.Partition = partition;
            }

            return Task.FromResult(-1L);
        }

        public Task PersistAndShutdownAsync()
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
            var effects = new TrackedObject.EffectList(this.partition);

            if (batch.Count != 0)
            {
                foreach (var o in batch)
                {
                    if (o is PartitionEvent partitionEvent)
                    {
                        partitionEvent.CommitLogPosition = nextCommitPosition++;
                        partition.TraceProcess(partitionEvent);

                        // determine the effects and apply all the updates
                        partitionEvent.DetermineEffects(effects);
                        while (effects.Count > 0)
                        {
                            this.ProcessRecursively(partitionEvent, effects);
                        }

                        partition.DiagnosticsTrace("Processing complete");
                        Partition.TraceContext = null;

                        AckListeners.Acknowledge(partitionEvent);
                    }
                    else
                    {
                        var readContinuation = (StorageAbstraction.IReadContinuation)o;
                        var readTarget = this.GetOrAdd(readContinuation.ReadTarget);
                        readContinuation.OnReadComplete(readTarget);
                    }
                }
            }

            return Task.CompletedTask;
        }

        public void ProcessRecursively(PartitionEvent evt, TrackedObject.EffectList effects)
        {
            // process the last effect in the list, and recursively any effects it adds
            var startPos = effects.Count - 1;
            var thisKey = effects[startPos];
            var thisObject = this.GetOrAdd(thisKey);

            if (EtwSource.EmitDiagnosticsTrace)
            {
                partition.DiagnosticsTrace($"Process on [{thisObject.Key}]");
            }

            // start with processing the event on this object, which
            // updates its state and can flag more objects to process on
            dynamic dynamicThis = thisObject;
            dynamic dynamicPartitionEvent = evt;
            dynamicThis.Process(dynamicPartitionEvent, effects);

            // recursively process all additional objects to process
            while (effects.Count - 1 > startPos)
            {
                this.ProcessRecursively(evt, effects);
            }

            effects.RemoveAt(startPos);
        }
    }
}