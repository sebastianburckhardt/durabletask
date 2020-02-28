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
using System.Linq;
using System.Runtime.Serialization;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using DurableTask.Core;
using DurableTask.Core.History;

namespace DurableTask.EventSourced
{
    [DataContract]
    internal class SessionsState : TrackedObject
    {
        [DataMember]
        public Dictionary<string, Session> Sessions { get; private set; } = new Dictionary<string, Session>();

        [DataMember]
        public long SequenceNumber { get; set; }

        [DataContract]
        internal class Session
        {
            [DataMember]
            public long SessionId { get; set; }

            [DataMember]
            public long BatchStartPosition { get; set; }

            [DataMember]
            public List<TaskMessage> Batch { get; set; }

            [DataMember]
            public bool ForceNewExecution { get; set; }
        }

        [IgnoreDataMember]
        public override TrackedObjectKey Key => new TrackedObjectKey(TrackedObjectKey.TrackedObjectType.Sessions);


        public override void OnRecoveryCompleted()
        {
            // create work items for all sessions
            foreach(var kvp in Sessions)
            {
                new OrchestrationMessageBatch(kvp.Key, kvp.Value).ScheduleWorkItem(this.Partition);
            }
        }

        public override string ToString()
        {
            return $"Sessions ({Sessions.Count} pending) next={SequenceNumber:D6}";
        }

        private void AddMessageToSession(TaskMessage message, bool forceNewExecution, bool isReplaying)
        {
            var instanceId = message.OrchestrationInstance.InstanceId;

            if (this.Sessions.TryGetValue(instanceId, out var session) && !forceNewExecution)
            {
                // A session for this instance already exists, so a work item is in progress already.
                // We don't need to schedule a work item because we'll notice the new messages when it completes.
                session.Batch.Add(message);
            }
            else
            {
                this.Sessions[instanceId] = session = new Session()
                {
                    SessionId = SequenceNumber++,
                    Batch = new List<TaskMessage>() { message },
                    BatchStartPosition = 0,
                    ForceNewExecution = forceNewExecution,
                };

                if (!isReplaying) // we don't start work items until end of recovery
                {
                    new OrchestrationMessageBatch(instanceId, session).ScheduleWorkItem(this.Partition);
                }
            }
        }

        private void AddMessagesToSession(string instanceId, IEnumerable<TaskMessage> messages, bool isReplaying)
        {
            if (this.Sessions.TryGetValue(instanceId, out var session))
            {
                // A session for this instance already exists, so a work item is in progress already.
                // We don't need to schedule a work item because we'll notice the new messages when it completes.
                session.Batch.AddRange(messages);
            }
            else
            {
                // Create a new session
                this.Sessions[instanceId] = session = new Session()
                {
                    SessionId = SequenceNumber++,
                    Batch = messages.ToList(),
                    BatchStartPosition = 0
                };

                if (!isReplaying) // we don't start work items until end of recovery
                {
                    new OrchestrationMessageBatch(instanceId, session).ScheduleWorkItem(this.Partition);
                }
            }
        }

        // TaskMessagesReceived
        // queues task message (from another partition) in a new or existing session

        public void Process(TaskMessagesReceived evt, EffectTracker effects)
        {
            foreach (var group in evt.TaskMessages
                .GroupBy(tm => tm.OrchestrationInstance.InstanceId))
            {
                this.AddMessagesToSession(group.Key, group, effects.IsReplaying);
            }
        }

        // ActivityResultReceived
        // queues task message (from another partition) in a new or existing session

        public void Process(RemoteActivityResultReceived evt, EffectTracker effects)
        {
            this.AddMessageToSession(evt.Result, false, effects.IsReplaying);
            effects.Add(TrackedObjectKey.Activities);
        }

        // ActivityOffloadReceived
        // does not operate on sessions but on activities

        public void Process(ActivityOffloadReceived evt, EffectTracker effects)
        {
            effects.Add(TrackedObjectKey.Activities);
        }

        // ClientTaskMessagesReceived
        // queues task message (from a client) in a new or existing session

        public void Process(ClientTaskMessagesReceived evt, EffectTracker effects)
        {
            var instanceId = evt.TaskMessages[0].OrchestrationInstance.InstanceId;
            this.AddMessagesToSession(instanceId, evt.TaskMessages, effects.IsReplaying);
        }

        // CreationMessageReceived
        // queues a creation task message in a new or existing session

        public void Process(CreationRequestReceived creationRequestReceived, EffectTracker effects)
        {
            this.AddMessageToSession(creationRequestReceived.TaskMessage, true, effects.IsReplaying);
        }

        // TimerFired
        // queues a timer fired message in a session

        public void Process(TimerFired timerFired, EffectTracker effects)
        {
            this.AddMessageToSession(timerFired.TimerFiredMessage, false, effects.IsReplaying);
        }

        // ActivityCompleted
        // queues an activity-completed message in a session

        public void Process(ActivityCompleted activityCompleted, EffectTracker effects)
        {
            this.AddMessageToSession(activityCompleted.Response, false, effects.IsReplaying);
        }

        // BatchProcessed
        // updates the session and other state

        public void Process(BatchProcessed evt, EffectTracker effects)
        {
            var session = this.Sessions[evt.InstanceId];

            // the session may have been forcefully replaced by a new one
            // (if the user replaced a running instance)
            // we can recognize this situation because the session id will not match
            // in that case, ignore the results of the processed batch
            if (session.SessionId != evt.SessionId)
            {
                return;
            }

            if (evt.ActivityMessages?.Count > 0)
            {
                effects.Add(TrackedObjectKey.Activities);
            }

            if (evt.TimerMessages?.Count > 0)
            {
                effects.Add(TrackedObjectKey.Timers);
            }

            if (evt.RemoteMessages?.Count > 0)
            {
                effects.Add(TrackedObjectKey.Outbox);
            }

            // deliver orchestrator messages destined for this partition directly to the relevant session(s)
            if (evt.LocalMessages?.Count > 0)
            {
                foreach (var group in evt.LocalMessages.GroupBy(tm => tm.OrchestrationInstance.InstanceId))
                {
                    this.AddMessagesToSession(group.Key, group, effects.IsReplaying);
                }
            }

            if (evt.State != null)
            {
                effects.Add(TrackedObjectKey.Instance(evt.InstanceId));
                effects.Add(TrackedObjectKey.History(evt.InstanceId));
            }

            // remove processed messages from this batch
            effects.Partition.Assert(session != null);
            effects.Partition.Assert(session.SessionId == evt.SessionId);
            effects.Partition.Assert(session.BatchStartPosition == evt.BatchStartPosition);
            session.Batch.RemoveRange(0, evt.BatchLength);
            session.BatchStartPosition += evt.BatchLength;

            this.StartNewBatchIfNeeded(session, effects, evt.InstanceId, effects.IsReplaying);
        }

        private void StartNewBatchIfNeeded(Session session, EffectTracker effects, string instanceId, bool inRecovery)
        {
            if (session.Batch.Count == 0)
            {
                // no more pending messages for this instance, so we delete the session.
                this.Sessions.Remove(instanceId);
            }
            else
            {
                if (!inRecovery) // we don't start work items until end of recovery
                {
                    // there are more messages. Prepare another work item.
                    new OrchestrationMessageBatch(instanceId, session).ScheduleWorkItem(this.Partition);
                }
            }
        }
    }
}
