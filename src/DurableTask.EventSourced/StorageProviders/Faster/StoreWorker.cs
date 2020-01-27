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
    internal class StoreWorker : BatchWorker<object>
    {
        private readonly FasterKV store;
        private readonly Partition partition;

        private readonly TrackedObject.EffectList effects;

        private volatile TaskCompletionSource<bool> shutdownWaiter;

        private bool IsShuttingDown => this.shutdownWaiter != null;

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
                await store.GetOrCreate(key);
            }
        }

        public async Task ShutdownAsync()
        {
            lock (this.thisLock)
            {
                this.shutdownWaiter = new TaskCompletionSource<bool>();
                this.Notify();
            }

            await this.shutdownWaiter.Task; // waits for processor to stop processing reads and updates
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
                    try
                    {
                        this.partition.TraceProcess(partitionEvent);
                        partitionEvent.DetermineEffects(this.effects);
                        while (this.effects.Count > 0)
                        {
                            await this.ProcessRecursively(partitionEvent, this.effects);
                        }
                        partition.DiagnosticsTrace("Processing complete");
                        Partition.TraceContext = null;
                    }
                    catch (Exception updateException)
                    {
                        partition.ReportError($"Processing Update", updateException);
                    }
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
                this.shutdownWaiter.TrySetResult(true);
            }
        }
    
        public async ValueTask ProcessRecursively(PartitionEvent evt, TrackedObject.EffectList effects)
        {
            var startPos = effects.Count - 1;
            var thisKey = effects[startPos];
            var thisObject = await store.GetOrCreate(thisKey);

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
            await store.MarkWritten(thisKey, evt);

            // recursively process all additional objects to process
            while (effects.Count - 1 > startPos)
            {
                await this.ProcessRecursively(evt, effects);
            }

            effects.RemoveAt(startPos);
        }
    }
}
