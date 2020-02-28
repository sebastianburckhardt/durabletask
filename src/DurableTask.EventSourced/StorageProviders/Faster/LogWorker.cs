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
using System.IO;
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
        private readonly TraceHelper traceHelper;

        private volatile TaskCompletionSource<bool> shutdownWaiter;

        private bool IsShuttingDown => this.shutdownWaiter != null;

        public LogWorker(FasterLog log, Partition partition, StoreWorker storeWorker, TraceHelper traceHelper)
            : base(CancellationToken.None)
        {
            this.log = log;
            this.partition = partition;
            this.storeWorker = storeWorker;
            this.traceHelper = traceHelper;
        }

        public const byte completePacket = 0;
        public const byte partialPacket = 1;
        public const byte transportPacket = 2;

        private const int maxFragmentSize = 50000; // TODO think about this, and sync it to Faster parameter
        
        public override void Submit(PartitionEvent evt)
        {
            partition.Assert(evt is IPartitionEventWithSideEffects);

            byte[] bytes = Serializer.SerializeEvent(evt, completePacket);

            lock (this.thisLock)
            {
                Enqueue(bytes);

                evt.CommitLogPosition = (ulong)this.log.TailAddress;

                // add to store worker (under lock for consistent ordering)
                this.storeWorker.Submit(evt);

                base.Submit(evt);
            }
        }

        public override void SubmitIncomingBatch(IEnumerable<PartitionEvent> events)
        {
            // TODO optimization: use batching and reference data in EH queue instead of duplicating it          
            foreach (var evt in events)
            {
                Submit(evt);
            }
        }

        private void Enqueue(byte[] bytes)
        {
            // append to faster log
            try
            {
                this.log.Enqueue(bytes);
            }
            catch (FASTER.core.FasterException e) when (e.Message == "Entry does not fit on page")
            {
                // the message is too big. Break it into fragments. 
                int pos = 1;
                while (pos < bytes.Length)
                {

                    bool isLastFragment = 1 + bytes.Length - pos <= maxFragmentSize;
                    int packetSize = isLastFragment ? 1 + bytes.Length - pos : maxFragmentSize;
                    bytes[pos - 1] = isLastFragment ? completePacket : partialPacket;
                    this.log.Enqueue(new ReadOnlySpan<byte>(bytes, pos - 1, packetSize));
                    pos += packetSize - 1;
                }
            }
        }

        public async Task PersistAndShutdownAsync()
        {
            this.traceHelper.FasterProgress("stopping LogWorker");

            lock (this.thisLock)
            {
                this.shutdownWaiter = new TaskCompletionSource<bool>();
                base.Submit(null);
            }

            await this.shutdownWaiter.Task; // waits for all the enqueued entries to be persisted

            this.traceHelper.FasterProgress("stopped LogWorker");
        }

        protected override async Task Process(IList<PartitionEvent> batch)
        {
            try
            {
                //  checkpoint the log
                this.traceHelper.FasterProgress("persisting log");
                var stopwatch = new System.Diagnostics.Stopwatch();
                stopwatch.Start();
                long previous = log.CommittedUntilAddress;

                try
                {
                    await log.CommitAsync();
                    this.traceHelper.FasterLogPersisted(log.CommittedUntilAddress, log.CommittedUntilAddress - previous, stopwatch.ElapsedMilliseconds);
                }
                catch(Exception e)
                {
                    this.traceHelper.FasterStorageError("persisting log", e);
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

        public async Task ReplayCommitLog(ulong from, StoreWorker worker)
        {
            var to = this.log.TailAddress;

            using (var iter = log.Scan((long)from, to))
            {
                byte[] result;
                int entryLength;
                long currentAddress;
                MemoryStream reassembly = null;

                while (true)
                {
                    PartitionEvent partitionEvent = null;

                    while (!iter.GetNext(out result, out entryLength, out currentAddress))
                    {
                        if (currentAddress >= to)
                        {
                            return;
                        }
                        await iter.WaitAsync();
                    }

                    if (result[0] == completePacket)
                    {
                        if (reassembly == null)
                        {
                            partitionEvent = (PartitionEvent)Serializer.DeserializeEvent(new ArraySegment<byte>(result, 1, entryLength - 1));
                        }
                        else
                        {
                            reassembly.Write(result, 1, entryLength - 1);
                            reassembly.Position = 0;
                            partitionEvent = (PartitionEvent)Serializer.DeserializeEvent(reassembly);
                            reassembly = null;
                        }
                    }
                    else
                    {
                        if (reassembly == null)
                        {
                            reassembly = new MemoryStream();
                        }
                        reassembly.Write(result, 1, entryLength - 1);
                    }

                    if (partitionEvent != null)
                    {
                        partitionEvent.CommitLogPosition = (ulong)iter.NextAddress;
                        await worker.ProcessEvent(partitionEvent);
                    }
                }
            }
        }
    }
}
