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
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventSourced.Faster
{
    internal class StoreWorker
    {
        private readonly FasterKV store;
        private readonly FasterLog log;
        private readonly Partition partition;
        private readonly object thisLock = new object();
        private readonly CancellationToken cancellationToken;
        private List<PartitionEvent> commitQueue;
        private List<Task> readQueue;
        private Task commitLoopTask;

        public StoreWorker(FasterKV store, FasterLog log, Partition partition, CancellationToken token)
        {
            this.store = store;
            this.log = log;
            this.partition = partition;
            this.cancellationToken = token;
            this.commitQueue = new List<PartitionEvent>();
            this.readQueue = new List<Task>();
            this.thisLock = new object();
            this.commitLoopTask = new Task(CommitLoop);
            cancellationToken.Register(Release);

            var thread = new Thread(() => commitLoopTask.RunSynchronously());
            thread.Name = $"CommitWorker{partition.PartitionId:D2}";
            thread.Start();
        }

        private void Release()
        {
            lock (this.thisLock)
            {
                Monitor.Pulse(this.thisLock);
            }
        }

        public void Process(PartitionEvent evt)
        {
            lock (this.thisLock)
            {
                if (this.commitQueue.Count == 0 && this.readQueue.Count == 0)
                {
                    Monitor.Pulse(this.thisLock);
                }

                this.commitQueue.Add(evt);
            }
        }

        public void Process(IEnumerable<PartitionEvent> evts)
        {
            lock (this.thisLock)
            {
                if (this.commitQueue.Count == 0 && this.readQueue.Count == 0)
                {
                    Monitor.Pulse(this.thisLock);
                }

                this.commitQueue.AddRange(evts);
            }
        }

        public void Process(Task readtask)
        {
            lock (this.thisLock)
            {
                if (this.commitQueue.Count == 0 && this.readQueue.Count == 0)
                {
                    Monitor.Pulse(this.thisLock);
                }

                this.readQueue.Add(readtask);
            }
        }

        public Task JoinAsync()
        {
            return this.commitLoopTask;
        }


        private void CommitLoop()
        {
            try
            {
                this.store.StartSession();

                foreach (var k in TrackedObjectKey.GetSingletons())
                {
                    store.GetOrCreate(k);
                }

                var tracker = new TrackedObject.EffectTracker();
                var readBatch = new List<Task>();
                var commitBatch = new List<PartitionEvent>();

                while (!this.cancellationToken.IsCancellationRequested)
                {
                    lock (this.thisLock)
                    {
                        while (this.commitQueue.Count == 0
                            && this.readQueue.Count == 0
                            && !this.cancellationToken.IsCancellationRequested)
                        {
                            Monitor.Wait(this.thisLock);
                        }

                        if (this.readQueue.Count > 0)
                        {
                            var tmp = readBatch;
                            readBatch = this.readQueue;
                            this.readQueue = tmp;
                        }
                        if (this.commitQueue.Count > 0)
                        {
                            var tmp = commitBatch;
                            commitBatch = this.commitQueue;
                            this.commitQueue = tmp;
                        }
                    }

                    if (readBatch.Count > 0)
                    {
                        foreach (Task t in readBatch)
                        {
                            t.RunSynchronously();
                        }

                        readBatch.Clear();
                    }

                    if (commitBatch.Count > 0)
                    {
                        for (int i = 0; i < commitBatch.Count; i++)
                        {
                            var partitionEvent = commitBatch[i];
                            partition.TraceProcess(partitionEvent);
                            var target = store.GetOrCreate(partitionEvent.StartProcessingOnObject);
                            this.ProcessRecursively(target, partitionEvent, tracker);
                            tracker.Clear();
                        }

                        commitBatch.Clear();
                    }
                }
                store.TakeFullCheckpoint(out _);
                store.CompleteCheckpoint(true);
                store.StopSession();
                store.Dispose();
            }
            catch (Exception e)
            {
                partition.ReportError(nameof(CommitLoop), e);
                throw e;
            }
        }

        public void ProcessRecursively(TrackedObject thisObject, PartitionEvent evt, TrackedObject.EffectTracker effect)
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
                    var t = store.GetOrCreate(effect.ObjectsToProcessOn[processOnStartPos + i]);
                    this.ProcessRecursively(t, evt, effect);
                }
            }

            // apply all objects as determined by effect tracker
            if (numObjectsToApplyTo > 0)
            {
                for (int i = 0; i < numObjectsToApplyTo; i++)
                {
                    var targetKey = effect.ObjectsToApplyTo[applyToStartPos + i];
                    var target = store.GetOrCreate(targetKey);

                    if (EtwSource.EmitDiagnosticsTrace)
                    {
                        this.partition.DiagnosticsTrace($"Apply to [{target.Key}]");
                    }

                    dynamic dynamicTarget = target;
                    dynamicTarget.Apply(dynamicPartitionEvent);
                }
            }

            // remove the elements that were added in this frame
            effect.ObjectsToProcessOn.RemoveRange(processOnStartPos, numObjectsToProcessOn);
            effect.ObjectsToApplyTo.RemoveRange(applyToStartPos, numObjectsToApplyTo);
        }
    }
}
