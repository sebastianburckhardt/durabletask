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
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using DurableTask.Core;
using DurableTask.Core.History;
using DurableTask.EventSourced.Faster;

namespace DurableTask.EventSourced
{
    [DataContract]
    internal class DedupState : TrackedObject
    {
        [DataMember]
        public Dictionary<uint, long> LastProcessed { get; set; } = new Dictionary<uint, long>();

        // Q: Should this be serialized or not?
        public Dictionary<uint, long> LastConfirmed { get; set; } = new Dictionary<uint, long>();

        [IgnoreDataMember]
        public Action<Dictionary<uint, long>> ConfirmationListener;

        [IgnoreDataMember]
        public override TrackedObjectKey Key => new TrackedObjectKey(TrackedObjectKey.TrackedObjectType.Dedup);

        private bool IsNotDuplicate(PartitionMessageEvent evt)
        {
            // detect duplicates of incoming partition-to-partition events by comparing commit log position of this event against last processed event from same partition
            this.LastProcessed.TryGetValue(evt.OriginPartition, out long lastProcessed);
            if (evt.OriginPosition > lastProcessed)
            {
                this.LastProcessed[evt.OriginPartition] = evt.OriginPosition;
                return true;
            }
            else
            {
                return false;
            }
        }

        public bool KeepWaitingForPersistenceConfirmation(Dictionary<uint, long> waitingFor, Dictionary<uint, long> lastConfirmed)
        {
            var keepWaiting = false;
            foreach (var partitionPosition in waitingFor)
            {
                // Q: Is this an eager or? Is the following check ok?
                if (!lastConfirmed.ContainsKey(partitionPosition.Key)
                    || lastConfirmed[partitionPosition.Key] < partitionPosition.Value)
                {
                    keepWaiting = true;
                    break;
                }
            }
            return keepWaiting;
        }

        public void Process(PersistenceConfirmationEvent evt, EffectTracker effects)
        {
            // Q: Is this check necessary, or is it expected that confirmed are non-decreasing?
            long previousConfirmed;
            long newConfirmed;
            if (this.LastConfirmed.TryGetValue(evt.OriginPartition, out previousConfirmed))
            {
                newConfirmed = Math.Max(previousConfirmed, evt.OriginPosition);
            }
            else 
            {
                newConfirmed = evt.OriginPosition;
            }
            this.LastConfirmed[evt.OriginPartition] = newConfirmed;

            // If the Confirmation listener is set, it means that a checkpoint is waiting for 
            // persistence confirmation to complete
            this.ConfirmationListener?.Invoke(this.LastConfirmed);
        }
        public void Process(ActivityOffloadReceived evt, EffectTracker effects)
        {
            // queues activities originating from a remote partition to execute on this partition
            if (this.IsNotDuplicate(evt))
            {
                effects.Add(TrackedObjectKey.Activities);
            }
        }

        public void Process(RemoteActivityResultReceived evt, EffectTracker effects)
        {
            // returns a response to an ongoing orchestration, and reports load data to the offload logic
            if (this.IsNotDuplicate(evt))
            {
                effects.Add(TrackedObjectKey.Sessions);
                effects.Add(TrackedObjectKey.Activities);
            }
        }

        public void Process(TaskMessagesReceived evt, EffectTracker effects)
        {
            // contains messages to be processed by sessions and/or to be scheduled by timer
            if (this.IsNotDuplicate(evt))
            {
                if (evt.TaskMessages != null)
                {
                    effects.Add(TrackedObjectKey.Sessions);
                }
                if (evt.DelayedTaskMessages != null)
                {
                    effects.Add(TrackedObjectKey.Timers);
                }
            }
        }
    }
}
