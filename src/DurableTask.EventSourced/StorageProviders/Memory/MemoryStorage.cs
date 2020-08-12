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
using Microsoft.Azure.Cosmos.Table;
using Microsoft.Extensions.Logging;

namespace DurableTask.EventSourced
{
    internal class MemoryStorage : BatchWorker<PartitionEvent>, StorageAbstraction.IPartitionState
    {
        private readonly ILogger logger;
        private Partition partition;
        private long nextSubmitPosition = 0;
        private long commitPosition = 0;
        private long inputQueuePosition = 0;

        private ConcurrentDictionary<TrackedObjectKey, TrackedObject> trackedObjects
            = new ConcurrentDictionary<TrackedObjectKey, TrackedObject>();

        public MemoryStorage(ILogger logger)
        {
            this.logger = logger;
            this.GetOrAdd(TrackedObjectKey.Activities);
            this.GetOrAdd(TrackedObjectKey.Dedup);
            this.GetOrAdd(TrackedObjectKey.Outbox);
            this.GetOrAdd(TrackedObjectKey.Reassembly);
            this.GetOrAdd(TrackedObjectKey.Sessions);
            this.GetOrAdd(TrackedObjectKey.Timers);
        }
        public CancellationToken Termination => CancellationToken.None;

        public void SubmitInternalEvent(PartitionEvent entry)
        {
            if (entry is PartitionUpdateEvent updateEvent)
            {
                updateEvent.NextCommitLogPosition = ++nextSubmitPosition;
            }

            base.Submit(entry);
        }

        public void SubmitExternalEvents(IEnumerable<PartitionEvent> entries)
        {
            foreach (var entry in entries)
            {
                if (entry is PartitionUpdateEvent updateEvent)
                {
                    updateEvent.NextCommitLogPosition = ++nextSubmitPosition;
                }
            }

            base.SubmitBatch(entries);
        }

        public Task<long> CreateOrRestoreAsync(Partition partition, IPartitionErrorHandler termination, long initialInputQueuePosition)
        {
            this.partition = partition;

            foreach (var trackedObject in this.trackedObjects.Values)
            {
                trackedObject.Partition = partition;

                if (trackedObject.Key.IsSingleton)
                {
                    trackedObject.OnFirstInitialization();
                }
            }

            this.commitPosition = 1;
            return Task.FromResult(1L);
        }

        public async Task CleanShutdown(bool takeFinalStateCheckpoint)
        {
            await Task.Delay(10).ConfigureAwait(false);
            
            this.partition.ErrorHandler.TerminateNormally();
        }

        private TrackedObject GetOrAdd(TrackedObjectKey key)
        {
            var result = trackedObjects.GetOrAdd(key, TrackedObjectKey.Factory);
            result.Partition = this.partition;
            return result;
        }

        private IAsyncEnumerable<InstanceState> GetAllInstances()
        {
            return trackedObjects
                .Values
                .Select(trackedObject => trackedObject as InstanceState)
                .Where(instanceState => instanceState != null).ToList().ToAsyncEnumerable();
        }

        protected override async Task Process(IList<PartitionEvent> batch)
        {
            try
            {
                var effects = new EffectTracker(
                    this.partition,
                    this.ApplyToStore,
                    () => (this.commitPosition, this.inputQueuePosition)
                );

                if (batch.Count != 0)
                {
                    foreach (var partitionEvent in batch)
                    {
                        // record the current time, for measuring latency in the event processing pipeline
                        partitionEvent.IssuedTimestamp = this.partition.CurrentTimeMs;

                        try
                        {
                            switch (partitionEvent)
                            {
                                case PartitionUpdateEvent updateEvent:
                                    updateEvent.NextCommitLogPosition = commitPosition + 1;
                                    await effects.ProcessUpdate(updateEvent).ConfigureAwait(false);
                                    DurabilityListeners.ConfirmDurable(updateEvent);
                                    if (updateEvent.NextCommitLogPosition > 0)
                                    {
                                        this.partition.Assert(updateEvent.NextCommitLogPosition > this.commitPosition);
                                        this.commitPosition = updateEvent.NextCommitLogPosition;
                                    }
                                    break;

                                case PartitionReadEvent readEvent:
                                    readEvent.OnReadIssued(this.partition);
                                    if (readEvent.Prefetch.HasValue)
                                    {
                                        var prefetchTarget = this.GetOrAdd(readEvent.Prefetch.Value);
                                        effects.ProcessReadResult(readEvent, readEvent.Prefetch.Value, prefetchTarget);
                                    }
                                    var readTarget = this.GetOrAdd(readEvent.ReadTarget);
                                    effects.ProcessReadResult(readEvent, readEvent.ReadTarget, readTarget);
                                    break;

                                case PartitionQueryEvent queryEvent:
                                    await effects.ProcessQueryResultAsync(queryEvent, this.GetAllInstances());
                                    break;

                                default:
                                    throw new InvalidCastException("could not cast to neither PartitionReadEvent nor PartitionUpdateEvent");
                            }

                            if (partitionEvent.NextInputQueuePosition > 0)
                            {
                                this.partition.Assert(partitionEvent.NextInputQueuePosition > this.inputQueuePosition);
                                this.inputQueuePosition = partitionEvent.NextInputQueuePosition;
                            }
                        }
                        catch (Exception e)
                        {
                            partition.ErrorHandler.HandleError(nameof(Process), $"Encountered exception while processing event {partitionEvent}", e, false, false);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                this.logger.LogError("Exception in MemoryQueue BatchWorker: {exception}", e);
            }
        }

        public ValueTask ApplyToStore(TrackedObjectKey key, EffectTracker tracker)
        {
            tracker.ProcessEffectOn(this.GetOrAdd(key));
            return default;
        }
    }
}