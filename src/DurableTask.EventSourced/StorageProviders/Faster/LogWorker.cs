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

using DurableTask.Core.Common;
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
    internal class LogWorker : BatchWorker<PartitionUpdateEvent>
    {
        private readonly BlobManager blobManager;
        private readonly FasterLog log;
        private readonly Partition partition;
        private readonly StoreWorker storeWorker;
        private readonly FasterTraceHelper traceHelper;

        private volatile TaskCompletionSource<bool> shutdownWaiter;

        private ulong numberEventsProcessed;
        private ulong numberEventsQueued;

        private bool IsShuttingDown => this.shutdownWaiter != null || this.cancellationToken.IsCancellationRequested;

        public LogWorker(BlobManager blobManager, FasterLog log, Partition partition, StoreWorker storeWorker, FasterTraceHelper traceHelper, CancellationToken cancellationToken)
            : base(cancellationToken)
        {
            this.blobManager = blobManager;
            this.log = log;
            this.partition = partition;
            this.storeWorker = storeWorker;
            this.traceHelper = traceHelper;

            this.maxFragmentSize = (1 << this.blobManager.EventLogSettings.PageSizeBits) - 64; // faster needs some room for header, 64 bytes is conservative
        }

        public const byte first = 0x1;
        public const byte last = 0x2;
        public const byte none = 0x0;

        private int maxFragmentSize;

        public override void Submit(PartitionUpdateEvent evt)
        {
            byte[] bytes = Serializer.SerializeEvent(evt, first | last);

            if (!this.IsShuttingDown)
            {
                lock (this.thisLock)
                {
                    Enqueue(bytes);

                    evt.NextCommitLogPosition = this.log.TailAddress;

                    base.Submit(evt);

                    // add to store worker (under lock for consistent ordering)
                    this.storeWorker.Submit(evt);

                    this.numberEventsQueued++;
                }
            }
        }

        public override void SubmitIncomingBatch(IEnumerable<PartitionUpdateEvent> events)
        {
            // TODO optimization: use batching and reference data in EH queue instead of duplicating it          
            foreach (var evt in events)
            {
                this.Submit(evt);
            }
        }

        private void Enqueue(byte[] bytes)
        {
            if (bytes.Length <= maxFragmentSize)
            {
                this.log.Enqueue(bytes);
            }
            else
            {
                // the message is too big. Break it into fragments. 
                int pos = 1;
                while (pos < bytes.Length)
                {
                    bool isLastFragment = 1 + bytes.Length - pos <= maxFragmentSize;
                    int packetSize = isLastFragment ? 1 + bytes.Length - pos : maxFragmentSize;
                    bytes[pos - 1] = (byte)(((pos == 1) ? first : none) | (isLastFragment ? last : none));
                    this.log.Enqueue(new ReadOnlySpan<byte>(bytes, pos - 1, packetSize));
                    pos += packetSize - 1;
                }
            }
        }

        public async Task PersistAndShutdownAsync()
        {
            this.traceHelper.FasterProgress($"Stopping LogWorker");

            lock (this.thisLock)
            {
                this.shutdownWaiter = new TaskCompletionSource<bool>();
                this.Notify();
            }

            // wait for all the enqueued entries to be persisted
            await TaskHelpers.WaitForTaskOrCancellation(this.shutdownWaiter.Task, this.cancellationToken); 

            this.traceHelper.FasterProgress($"Stopped LogWorker");
        }

        protected override async Task Process(IList<PartitionUpdateEvent> batch)
        {
            if (batch.Count > 0)
            {
                try
                {
                    //  checkpoint the log
                    this.traceHelper.FasterProgress("Persisting log");
                    var stopwatch = new System.Diagnostics.Stopwatch();
                    stopwatch.Start();
                    long previous = log.CommittedUntilAddress;

                    await log.CommitAsync();

                    this.numberEventsProcessed += (uint) batch.Count;

                    this.traceHelper.FasterLogPersisted(log.CommittedUntilAddress, batch.Count, (log.CommittedUntilAddress - previous), stopwatch.ElapsedMilliseconds);

                    foreach (var evt in batch)
                    {
                        if (!this.IsShuttingDown)
                        {
                            try
                            {
                                DurabilityListeners.ConfirmDurable(evt);
                            }
                            catch (Exception exception) when (!(exception is OutOfMemoryException))
                            {
                                // for robustness, swallow exceptions, but report them
                                this.partition.ErrorHandler.HandleError("LogWorker.Process", $"Encountered exception while notifying persistence listeners for event {evt} id={evt.EventIdString}", exception, false, false);
                            }
                        }
                    }

                    this.traceHelper.FasterProgress("Log persistence acked");
                }
                catch (OperationCanceledException) when (this.cancellationToken.IsCancellationRequested)
                {
                    // o.k. during shutdown
                }
                catch (Exception e) when (!(e is OutOfMemoryException))
                {
                    this.partition.ErrorHandler.HandleError("LogWorker.Process", "Encountered exception while working on commit log", e, true, false);
                }
            }

            if (this.IsShuttingDown && this.numberEventsQueued == this.numberEventsProcessed)
            {
                this.shutdownWaiter?.TrySetResult(true);
            }
        }

        public async Task ReplayCommitLog(long from, StoreWorker worker)
        {
            // this procedure is called by StoreWorker during recovery. It replays all the events
            // that were committed to the log but are not reflected in the loaded store checkpoint.
            try
            {
                var to = this.log.TailAddress;

                using (var iter = log.Scan((long)from, to))
                {
                    byte[] result;
                    int entryLength;
                    long currentAddress;
                    MemoryStream reassembly = null;

                    while (!this.cancellationToken.IsCancellationRequested)
                    {
                        PartitionUpdateEvent partitionEvent = null;

                        while (!iter.GetNext(out result, out entryLength, out currentAddress))
                        {
                            if (currentAddress >= to)
                            {
                                return;
                            }
                            await iter.WaitAsync(this.cancellationToken);
                        }

                        if ((result[0] & first) != none)
                        {
                            if ((result[0] & last) != none)
                            {
                                partitionEvent = (PartitionUpdateEvent)Serializer.DeserializeEvent(new ArraySegment<byte>(result, 1, entryLength - 1));
                            }
                            else
                            {
                                reassembly = new MemoryStream();
                                reassembly.Write(result, 1, entryLength - 1);
                            }
                        }
                        else
                        {
                            reassembly.Write(result, 1, entryLength - 1);

                            if ((result[0] & last) != none)
                            {
                                reassembly.Position = 0;
                                partitionEvent = (PartitionUpdateEvent)Serializer.DeserializeEvent(reassembly);
                                reassembly = null;
                            }
                        }

                        if (partitionEvent != null)
                        {
                            partitionEvent.NextCommitLogPosition = iter.NextAddress;
                            await worker.ProcessUpdate(partitionEvent);
                        }
                    }
                }
            }
            catch (Exception exception)
                when (this.cancellationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.partition.ErrorHandler.Token);
            }
        }
    }
}
