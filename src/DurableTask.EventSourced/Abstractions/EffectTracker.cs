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

using Dynamitey.DynamicObjects;
using FASTER.core;
using Microsoft.Extensions.Logging;
using Microsoft.IdentityModel.Tokens;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DurableTask.EventSourced
{
    /// <summary>
    /// Is used while applying an effect to a partition state, to carry
    /// information about the context, and to enumerate the objects on which the effect
    /// is being processed.
    /// </summary>
    internal class EffectTracker : List<TrackedObjectKey>
    {
        private readonly Func<TrackedObjectKey, EffectTracker, ValueTask> applyToStore;
        private readonly Func<(ulong, ulong)> getPositions;
        private readonly Action<ulong, ulong> setPositions;

        public EffectTracker(Partition partition, Func<TrackedObjectKey, EffectTracker, ValueTask> applyToStore, Func<(ulong, ulong)> getPositions, Action<ulong, ulong> setPositions)
        {
            this.Partition = partition;
            this.applyToStore = applyToStore;
            this.getPositions = getPositions;
            this.setPositions = setPositions;
        }

        /// <summary>
        /// The current partition.
        /// </summary>
        public Partition Partition { get; }

        /// <summary>
        /// The effect currently being applied.
        /// </summary>
        public dynamic Effect { get; set; }

        /// <summary>
        /// True if we are replaying this effect during recovery.
        /// Typically, external side effects (such as launching tasks, sending responses, etc.)
        /// are suppressed during replay.
        /// </summary>
        public bool IsReplaying { get; set; }

        /// <summary>
        /// Applies the event to the given tracked object, using dynamic dispatch to 
        /// select the correct Process method overload for the event. 
        /// </summary>
        /// <param name="trackedObject"></param>
        public void ProcessEffectOn(dynamic trackedObject)
        {
            trackedObject.Process(Effect, this);
        }

        public async Task ProcessUpdate(PartitionEvent partitionEvent)
        {
            (ulong commitLogPosition, ulong inputQueuePosition) = this.getPositions();

            using (EventTraceHelper.TraceContext(commitLogPosition, partitionEvent.EventIdString))
            {
                try
                {
                    this.Partition.Assert(partitionEvent is IPartitionEventWithSideEffects);
                    this.Partition.EventTraceHelper.TraceEvent(commitLogPosition, partitionEvent, this.IsReplaying);

                    this.Effect = partitionEvent;

                    // collect the initial list of targets
                    ((IPartitionEventWithSideEffects)partitionEvent).DetermineEffects(this);

                    // process until there are no more targets
                    while (this.Count > 0)
                    {
                        await ProcessRecursively();
                    }

                    async ValueTask ProcessRecursively()
                    {
                        var startPos = this.Count - 1;
                        var key = this[startPos];

                        this.Partition.DetailTracer?.TraceDetail($"Process on [{key}]");

                        // start with processing the event on this object 
                        await this.applyToStore(key, this);

                        // recursively process all additional objects to process
                        while (this.Count - 1 > startPos)
                        {
                            await ProcessRecursively();
                        }

                        // pop this object now since we are done processing
                        this.RemoveAt(startPos);
                    }

                    // update the commit log and input queue positions
                    if (partitionEvent.NextCommitLogPosition.HasValue)
                    {
                        this.Partition.Assert(partitionEvent.NextCommitLogPosition.Value > commitLogPosition);
                        commitLogPosition = partitionEvent.NextCommitLogPosition.Value;
                    }
                    if (partitionEvent.NextInputQueuePosition.HasValue)
                    {
                        this.Partition.Assert(partitionEvent.NextInputQueuePosition.Value > inputQueuePosition);
                        inputQueuePosition = partitionEvent.NextInputQueuePosition.Value;
                    }
                    this.setPositions(commitLogPosition, inputQueuePosition);

                    this.Effect = null;
                    this.Partition.DetailTracer?.TraceDetail("finished processing event");
                }
                catch (OperationCanceledException)
                {
                    // o.k. during termination
                }
                catch (Exception exception) when (!(exception is OutOfMemoryException))
                {
                    // for robustness, swallow exceptions, but report them
                    this.Partition.ErrorHandler.HandleError(nameof(ProcessUpdate), $"Processing Update {partitionEvent}", exception, false, false);
                }
            }
        }

        public void ProcessRead(StorageAbstraction.IInternalReadonlyEvent readContinuation, TrackedObject target)
        {
            (ulong commitLogPosition, ulong inputQueuePosition) = this.getPositions();

            using (EventTraceHelper.TraceContext(commitLogPosition, readContinuation.EventIdString))
            {
                try
                {
                    this.Partition.EventTraceHelper.TraceEvent(commitLogPosition, readContinuation);
                    this.Partition.Assert(!this.IsReplaying); // read events are never part of the replay

                    readContinuation.OnReadComplete(target);

                    this.Partition.DetailTracer?.TraceDetail("finished processing read event");
                }
                catch (OperationCanceledException)
                {
                    // o.k. during termination
                }
                catch (Exception exception) when (!(exception is OutOfMemoryException))
                {
                    // for robustness, swallow exceptions, but report them
                    this.Partition.ErrorHandler.HandleError(nameof(ProcessRead), $"Processing Read {readContinuation.ToString()}", exception, false, false);
                }
            }
        }
    }
}
