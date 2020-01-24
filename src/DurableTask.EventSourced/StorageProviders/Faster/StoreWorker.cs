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
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventSourced.Faster
{
    internal class StoreWorker : BatchWorker<object>
    {
        private readonly FasterKV store;
        private readonly Partition partition;
        private readonly BlobManager blobManager;

        private readonly TrackedObject.EffectList effects;

        private volatile TaskCompletionSource<bool> shutdownWaiter;

        private bool IsShuttingDown => this.shutdownWaiter != null;

        public StoreWorker(Partition partition, BlobManager blobManager)
        {
            this.store = new FasterKV(partition, blobManager);
            this.partition = partition;
            this.blobManager = blobManager;

            this.effects = new TrackedObject.EffectList(this.partition);
        }

        public async Task StartAsync()
        {
            var tasks = new List<Task>();
            foreach (var k in TrackedObjectKey.GetSingletons())
            {
                tasks.Add(store.GetOrCreateAsync(k));
            }
            await Task.WhenAll(tasks);
        }


        public async Task PersistAndShutdownAsync()
        {
            lock (this.lockable)
            {
                this.shutdownWaiter = new TaskCompletionSource<bool>();
                this.Notify();
            }

            await this.shutdownWaiter.Task; // waits for processor to stop processing reads and updates

            // take a full checkpoint
            store.TakeFullCheckpoint(out _);
            await store.CompleteCheckpointAsync();

            // we are done with the store
            store.Dispose();
        }

        public void Process(PartitionEvent evt)
        {
            this.Submit(evt);
        }

        public void Process(IEnumerable<PartitionEvent> evts)
        {
            this.SubmitRange(evts);
        }

        public Task<TResult> ProcessRead<TObject, TResult>(TrackedObjectKey key, Func<TObject, TResult> read) where TObject : TrackedObject
        {
            var tcs = new TaskCompletionSource<TResult>();
            var readtask = new Task(async () =>               
            {
                try
                {
                    var target = await store.GetOrCreateAsync(key);
                    TResult result;
                    result = read((TObject)target);
                    tcs.TrySetResult(result);
                }
                catch (Exception e)
                {
                    tcs.TrySetException(e);
                }
            });  

            this.Submit(readtask);
            return tcs.Task;
        }

        protected override async ValueTask ProcessAsync(IList<object> batch)
        {
            try
            {
                foreach (object o in batch)
                {
                    if (this.IsShuttingDown)
                    {
                        break; // stop processing sooner rather than later
                    }

                    switch (o)
                    {
                        case Task task:
                            {
                                task.RunSynchronously(); // todo handle async reads
                                break;
                            }

                        case PartitionEvent partitionEvent:
                            {
                                this.partition.TraceProcess(partitionEvent);
                                partitionEvent.DetermineEffects(this.effects);
                                while (this.effects.Count > 0)
                                {
                                    await this.ProcessRecursively(partitionEvent, this.effects);
                                }
                                partition.DiagnosticsTrace("Processing complete");
                                Partition.TraceContext = null;
                                break;
                            }
                    }
                }
            }
            catch (Exception e)
            {
                partition.ReportError(nameof(StoreWorker), e);
                throw e;
            }

            if (this.IsShuttingDown)
            {
                // at this point we know we will not process any more reads or updates
                this.shutdownWaiter.TrySetResult(true);
            }
        }

    
        public async ValueTask ProcessRecursively(PartitionEvent evt, TrackedObject.EffectList effects)
        {
            var startPos = effects.Count - 1;
            var thisKey = effects[startPos];
            var thisObject = await store.GetOrCreateAsync(thisKey);

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
                await this.ProcessRecursively(evt, effects);
            }

            effects.RemoveAt(startPos);
        }

    }
}
