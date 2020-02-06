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
using System.Linq;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventSourced.Faster
{
    internal class LogWorker : BatchWorker<PartitionEvent>
    {
        private readonly FasterLog log;
        private readonly Partition partition;
        private readonly StoreWorker storeWorker;

        private volatile TaskCompletionSource<bool> shutdownWaiter;

        private bool IsShuttingDown => this.shutdownWaiter != null;

        public LogWorker(FasterLog log, Partition partition, StoreWorker storeWorker)
            : base(CancellationToken.None)
        {
            this.log = log;
            this.partition = partition;
            this.storeWorker = storeWorker;
        }

        public void EnsureSerialized(PartitionEvent evt)
        {
            if (evt.Serialized.Count == 0)
            {
                byte[] bytes = Serializer.SerializeEvent(evt);
                evt.Serialized = new ArraySegment<byte>(bytes, 0, bytes.Length);
            }
        }

        public override void Submit(PartitionEvent evt)
        {
            partition.Assert(evt is IPartitionEventWithSideEffects);

            this.EnsureSerialized(evt);

            lock (this.thisLock)
            {
                // append to faster log
                this.log.Enqueue(evt.Serialized.AsSpan<byte>());
                evt.CommitLogPosition = (ulong)this.log.TailAddress;

                // add to store worker (under lock for consistent ordering)
                this.storeWorker.Submit(evt);

                base.Submit(evt);
            }
        }      

        public override void SubmitRange(IEnumerable<PartitionEvent> events)
        {
            foreach (var evt in events)
            {
                partition.Assert(evt is IPartitionEventWithSideEffects);
                this.EnsureSerialized(evt);
            }

            lock (this.thisLock)
            {
                foreach (var evt in events)
                {
                    // append to faster log
                    this.log.Enqueue(evt.Serialized.AsSpan<byte>());
                    evt.CommitLogPosition = (ulong)this.log.TailAddress;
                }

                // add to store worker (under lock for consistent ordering)
                this.storeWorker.SubmitRange(events);

                base.SubmitRange(events);
            }
        }

        public async Task PersistAndShutdownAsync()
        {
            EtwSource.Log.FasterProgress((int)this.partition.PartitionId, "stopping LogWorker");

            lock (this.thisLock)
            {
                this.shutdownWaiter = new TaskCompletionSource<bool>();
                base.Submit(null);
            }

            await this.shutdownWaiter.Task; // waits for all the enqueued entries to be persisted

            EtwSource.Log.FasterProgress((int)this.partition.PartitionId, "stopped LogWorker");
        }

        protected override async Task Process(IList<PartitionEvent> batch)
        {
            try
            {
                //  checkpoint the log
                EtwSource.Log.FasterProgress((int)partition.PartitionId, "persisting log");
                var stopwatch = new System.Diagnostics.Stopwatch();
                stopwatch.Start();
                long previous = log.CommittedUntilAddress;

                try
                {
                    await log.CommitAsync();
                    EtwSource.Log.FasterLogPersisted((int)partition.PartitionId, log.CommittedUntilAddress, log.CommittedUntilAddress - previous, stopwatch.ElapsedMilliseconds);
                }
                catch(Exception e)
                {
                    EtwSource.Log.FasterStorageError((int)partition.PartitionId, "persisting log", e.ToString());
                    throw;
                }

                foreach (var evt in batch)
                {
                    if (evt == null)
                    {
                        this.shutdownWaiter.TrySetResult(true);
                        return;
                    }

                    if (!this.IsShuttingDown)
                    {
                        AckListeners.Acknowledge(evt);
                    }
                }
            }
            catch (Exception e)
            {
                if (this.IsShuttingDown)
                {
                    // lets the caller know that shutdown did not successfully persist the latest log
                    this.shutdownWaiter.TrySetException(e);
                }
            }      
        }

    }
}
