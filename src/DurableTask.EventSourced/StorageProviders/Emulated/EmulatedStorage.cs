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

namespace DurableTask.EventSourced.Emulated
{
    internal class EmulatedStorage : BatchWorker<PartitionEvent>, StorageAbstraction.IPartitionState
    {
        private Partition partition;
        private long nextCommitPosition = 0;
        private ConcurrentDictionary<TrackedObjectKey, TrackedObject> trackedObjects
            = new ConcurrentDictionary<TrackedObjectKey, TrackedObject>();
        private List<Func<CancellationToken, IList<PartitionEvent>, Task>> activeIterators 
            = new List<Func<CancellationToken, IList<PartitionEvent>, Task>>();

        public EmulatedStorage()
        {
            this.GetOrAdd(TrackedObjectKey.Activities);
            this.GetOrAdd(TrackedObjectKey.Clients);
            this.GetOrAdd(TrackedObjectKey.Dedup);
            this.GetOrAdd(TrackedObjectKey.Outbox);
            this.GetOrAdd(TrackedObjectKey.Reassembly);
            this.GetOrAdd(TrackedObjectKey.Sessions);
            this.GetOrAdd(TrackedObjectKey.Timers);
        }

        public override void Submit(PartitionEvent entry)
        {
            this.partition.TraceSubmit(entry);
            base.Submit(entry);
        }

        public override void SubmitRange(IEnumerable<PartitionEvent> entries)
        {
            foreach(var entry in entries)
            {
                this.partition.TraceSubmit(entry);
            }

            base.SubmitRange(entries);
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

            foreach (var trackedObject in this.trackedObjects.Values)
            {
                trackedObject.Restore(partition);
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

        protected override Task Process(IList<PartitionEvent> batch)
        {
            this.ProcessBatch(batch, this.nextCommitPosition);

            this.nextCommitPosition += batch.Count;

            foreach (var evt in batch)
            {
                evt.AckListener?.Acknowledge(evt);
            }

            foreach(var iterator in this.activeIterators)
            {
                iterator.Invoke(this.cancellationToken, batch);
            }

            return Task.CompletedTask;
        }

        private void ProcessBatch(IList<PartitionEvent> batch, long nextCommitPosition)
        {
            for (int i = 0; i < batch.Count; i++)
            {
                var partitionEvent = batch[i];
                partitionEvent.CommitPosition = nextCommitPosition + i;
                partition.TraceProcess(partitionEvent);
                tracker.ObjectsToProcess.Add(partitionEvent.StartProcessingOnObject);
                this.ProcessRecursively(partitionEvent, tracker);
                tracker.Clear();
            }
        }

        public void StartIterator(long StartPosition, Func<CancellationToken, IList<PartitionEvent>, Task> body)
        {
            if (StartPosition != nextCommitPosition)
            {
                throw new InvalidOperationException("Emulator can start iterators only at present moment");
            }

            activeIterators.Add(body);
        }

        // reuse these collection objects between updates 
        // (note that updates are never concurrent by design)
        TrackedObject.EffectTracker tracker = new TrackedObject.EffectTracker();

        public void ProcessRecursively(PartitionEvent evt, TrackedObject.EffectTracker effect)
        {
            var startPos = effect.ObjectsToProcess.Count - 1;
            var thisKey = effect.ObjectsToProcess[startPos];
            var thisObject = this.GetOrAdd(thisKey);

            if (EtwSource.EmitDiagnosticsTrace)
            {
                partition.DiagnosticsTrace($"Process on [{thisObject.Key}]");
            }

            // start with processing the event on this object, which
            // updates its state and can flag more objects to process on
            lock (thisObject)
            {
                dynamic dynamicThis = thisObject;
                dynamic dynamicPartitionEvent = evt;
                dynamicThis.Process(dynamicPartitionEvent, effect);
            }

            // recursively process all additional objects to process
            while (effect.ObjectsToProcess.Count - 1 > startPos)
            {
                this.ProcessRecursively(evt, effect);
            }

            effect.ObjectsToProcess.RemoveAt(startPos);
        }
    }
}