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

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventSourced.Emulated
{
    internal class EmulatedStorage : BatchWorker<PartitionEvent>, Storage.IPartitionState
    {
        private Partition partition;
        private ConcurrentDictionary<TrackedObjectKey, TrackedObject> trackedObjects
            = new ConcurrentDictionary<TrackedObjectKey, TrackedObject>();

        public EmulatedStorage()
        {
            this.GetOrAdd(TrackedObjectKey.Activities);
            this.GetOrAdd(TrackedObjectKey.Clients);
            this.GetOrAdd(TrackedObjectKey.Dedup);
            this.GetOrAdd(TrackedObjectKey.Outbox);
            this.GetOrAdd(TrackedObjectKey.Reassembly);
            this.GetOrAdd(TrackedObjectKey.Recovery);
            this.GetOrAdd(TrackedObjectKey.Sessions);
            this.GetOrAdd(TrackedObjectKey.Timers);
        }

        public TrackedObject GetOrAdd(TrackedObjectKey key)
        {
            var result = trackedObjects.GetOrAdd(key, TrackedObjectKey.Factory);
            result.Restore(this.partition);
            return result;
        }

        public Task RestoreAsync(Partition partition)
        {
            this.partition = partition;

            // first, connect objects to partition and restart all in-flight tasks
            foreach (var trackedObject in this.trackedObjects.Values)
            {
                trackedObject.Restore(partition);
            }

            // then, finish any potentially incomplete commit batches
            var recoveryState = (RecoveryState)this.GetOrAdd(TrackedObjectKey.Recovery);

            if (recoveryState.Pending != null)
            {
                this.Process(recoveryState.Pending);
            }

            return Task.CompletedTask;
        }

        public Task WaitForTerminationAsync()
        {
            return Task.Delay(10);
        }

        public Task<TResult> ReadAsync<TObject, TResult>(TrackedObjectKey key, Func<TObject, TResult> read) where TObject : TrackedObject
        {
            var target = (TObject)this.GetOrAdd(key);

            lock (target.AccessLock) // prevent conflict with writers
            {
                return Task.FromResult(read(target));
            }
        }

        protected override Task Process(IReadOnlyList<PartitionEvent> batch)
        {
            var recoveryState = (RecoveryState)this.GetOrAdd(TrackedObjectKey.Recovery);

            recoveryState.Pending = batch.ToList();

            this.ProcessBatch(batch, recoveryState.LastProcessed + 1);

            recoveryState.LastProcessed = recoveryState.LastProcessed + batch.Count;
            recoveryState.Pending = null;

            foreach(var evt in batch)
            {
                evt.ConfirmationListener?.Confirm(evt);
            }

            return Task.CompletedTask;
        }

        private void ProcessBatch(IReadOnlyList<PartitionEvent> batch, long nextCommitPosition)
        {
            for (int i = 0; i < batch.Count; i++)
            {
                var partitionEvent = batch[i];
                partitionEvent.CommitPosition = nextCommitPosition + i;
                partition.TraceProcess(partitionEvent);
                var targetKey = partitionEvent.StartProcessingOnObject;
                var target = this.GetOrAdd(targetKey);
                this.ProcessRecursively(target, partitionEvent, tracker);
                tracker.Clear();
            }
        }

        // reuse these collection objects between updates (note that updates are never concurrent by design)
        TrackedObject.EffectTracker tracker = new TrackedObject.EffectTracker();

        public void ProcessRecursively(TrackedObject thisObject, PartitionEvent evt, TrackedObject.EffectTracker effect)
        {
            if (evt.CommitPosition > thisObject.LastProcessed)
            {
                if (EtwSource.EmitDiagnosticsTrace)
                {
                    partition.DiagnosticsTrace($"Process on [{thisObject.Key}]");
                }

                // remember the initial position of the lists so we can tell
                // which elements were added by this frame, and remove them at the end.

                var processOnStartPos = effect.ObjectsToProcessOn.Count;
                var applyToStartPos = effect.ObjectsToApplyTo.Count;

                // start with processing the event on this object, determining effect
                dynamic dynamicThis = thisObject;
                dynamic dynamicPartitionEvent = evt;
                dynamicThis.Process(dynamicPartitionEvent, effect);

                var numObjectsToProcessOn = effect.ObjectsToProcessOn.Count - processOnStartPos;
                var numObjectsToApplyTo = effect.ObjectsToApplyTo.Count - applyToStartPos;

                // recursively process all objects as determined by effect tracker
                if (numObjectsToProcessOn > 0)
                {
                    for (int i = 0; i < numObjectsToProcessOn; i++)
                    {
                        var t = this.GetOrAdd(effect.ObjectsToProcessOn[processOnStartPos + i]);
                        this.ProcessRecursively(t, evt, effect);
                    }
                }

                // apply all objects as determined by effect tracker
                if (numObjectsToApplyTo > 0)
                {
                    for (int i = 0; i < numObjectsToApplyTo; i++)
                    {
                        var targetKey = effect.ObjectsToApplyTo[applyToStartPos + i];
                        var target = this.GetOrAdd(targetKey);
                        if (target.LastProcessed < evt.CommitPosition)
                        {
                            if (EtwSource.EmitDiagnosticsTrace)
                            {
                                this.partition.DiagnosticsTrace($"Apply to [{target.Key}]");
                            }

                            lock (target.AccessLock) // prevent conflict with readers
                            {
                                dynamic dynamicTarget = target;
                                dynamicTarget.Apply(dynamicPartitionEvent);

                                target.LastProcessed = evt.CommitPosition;
                            }
                        }
                    }
                }

                // remove the elements that were added in this frame
                effect.ObjectsToProcessOn.RemoveRange(processOnStartPos, numObjectsToProcessOn);
                effect.ObjectsToApplyTo.RemoveRange(applyToStartPos, numObjectsToApplyTo);
            }
        }
    }
}