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

                var effectsList = new TrackedObject.EffectList();
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
                            partitionEvent.DetermineEffects(effectsList);
                            while (effectsList.Count > 0)
                            {
                                this.ProcessRecursively(partitionEvent, effectsList);
                            }
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

        public void ProcessRecursively(PartitionEvent evt, TrackedObject.EffectList effects)
        {
            var startPos = effects.Count - 1;
            var thisKey = effects[startPos];
            var thisObject = store.GetOrCreate(thisKey);

            if (EtwSource.EmitDiagnosticsTrace)
            {
                partition.DiagnosticsTrace($"Process on [{thisObject.Key}]");
            }

            // start with processing the event on this object, which
            // updates its state and can flag more objects to process on
            dynamic dynamicThis = thisObject;
            dynamic dynamicPartitionEvent = evt;
            dynamicThis.Process(dynamicPartitionEvent, effects);

            // tell Faster that this object was modified
            store.MarkWritten(thisKey, evt);

            // recursively process all additional objects to process
            while (effects.Count - 1 > startPos)
            {
                this.ProcessRecursively(evt, effects);
            }

            effects.RemoveAt(startPos);
        }
    }
}
