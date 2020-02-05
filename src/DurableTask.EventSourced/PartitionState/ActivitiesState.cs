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
using System.Diagnostics;
using System.Runtime.Serialization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DurableTask.Core;
using DurableTask.Core.History;
using Dynamitey;

namespace DurableTask.EventSourced
{
    [DataContract]
    internal class ActivitiesState : TrackedObject
    {
        [DataMember]
        public Dictionary<long, TaskMessage> PendingActivities { get; private set; } = new Dictionary<long, TaskMessage>();

        [DataMember]
        public long SequenceNumber { get; set; }

        [IgnoreDataMember]
        public override TrackedObjectKey Key => new TrackedObjectKey(TrackedObjectKey.TrackedObjectType.Activities);

        public override void OnRecoveryCompleted()
        {
            // reschedule work items
            foreach (var pending in PendingActivities)
            {
                Partition.EnqueueActivityWorkItem(new ActivityWorkItem(this.Partition, pending.Key, pending.Value));
            }
        }

        // ActivityCompleted
        // records the result of a finished activity

        public void Process(ActivityCompleted evt, EffectTracker effects)
        {
            PendingActivities.Remove(evt.ActivityId);
        }

        // BatchProcessed
        // may launch new activities

        public void Process(BatchProcessed evt, EffectTracker effect)
        {
            foreach (var msg in evt.ActivityMessages)
            {
                var activityId = SequenceNumber++;
                PendingActivities.Add(activityId, msg);

                if (!effect.InRecovery)
                {
                    Partition.EnqueueActivityWorkItem(new ActivityWorkItem(this.Partition, activityId, msg));
                }
            }
        }
    }
}
