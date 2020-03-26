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
using Dynamitey;

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

        public static string GetWorkItemId(uint partition, long session, long position, int length) => $"{partition:D2}-S{session}:{position}[{length}]";


        public override void OnRecoveryCompleted()
        {
            // create work items for all sessions
            foreach(var kvp in Sessions)
            {
                new OrchestrationMessageBatch(kvp.Key, kvp.Value, this.Partition);
            }
        }

        public override string ToString()
        {
            return $"Sessions ({Sessions.Count} pending) next={SequenceNumber:D6}";
        }

        private string GetSessionPosition(Session session) => $"{this.Partition.PartitionId:D2}-S{session.SessionId}:{session.BatchStartPosition + session.Batch.Count}";
      

        private void AddMessageToSession(TaskMessage message, bool forceNewExecution, bool isReplaying)
        {
            var instanceId = message.OrchestrationInstance.InstanceId;

            if (this.Sessions.TryGetValue(instanceId, out var session) && !forceNewExecution)
            {
                // A session for this instance already exists, so a work item is in progress already.
                // We don't need to schedule a work item because we'll notice the new messages when it completes.
                this.Partition.EventTraceHelper.TraceTaskMessageReceived(message, GetSessionPosition(session));
                session.Batch.Add(message);
            }
            else
            {
                this.Sessions[instanceId] = session = new Session()
                {
                    SessionId = SequenceNumber++,
                    Batch = new List<TaskMessage>(),
                    BatchStartPosition = 0,
                    ForceNewExecution = forceNewExecution,
                };

                this.Partition.EventTraceHelper.TraceTaskMessageReceived(message, GetSessionPosition(session));
                session.Batch.Add(message);

                if (!isReplaying) // we don't start work items until end of recovery
                {
                    new OrchestrationMessageBatch(instanceId, session, this.Partition);
                }
            }
        }

        private void AddMessagesToSession(string instanceId, IEnumerable<TaskMessage> messages, bool isReplaying)
        {
            if (this.Sessions.TryGetValue(instanceId, out var session))
            {
                // A session for this instance already exists, so a work item is in progress already.
                // We don't need to schedule a work item because we'll notice the new messages 
                // when the previous work item completes.
                foreach(var message in messages)
                {
                    this.Partition.EventTraceHelper.TraceTaskMessageReceived(message, GetSessionPosition(session));                  
                    session.Batch.Add(message);
                }
            }
            else
            {
                // Create a new session
                this.Sessions[instanceId] = session = new Session()
                {
                    SessionId = SequenceNumber++,
                    Batch = new List<TaskMessage>(),
                    BatchStartPosition = 0
                };

                foreach (var message in messages)
                {
                    this.Partition.EventTraceHelper.TraceTaskMessageReceived(message, GetSessionPosition(session));
                    session.Batch.Add(message);
                }

                if (!isReplaying) // we don't start work items until end of recovery
                {
                    new OrchestrationMessageBatch(instanceId, session, this.Partition);
                }
            }
        }

        public void Process(TaskMessagesReceived evt, EffectTracker effects)
        {
             // queues task message (from another partition) in a new or existing session
           foreach (var group in evt.TaskMessages
                .GroupBy(tm => tm.OrchestrationInstance.InstanceId))
            {
                this.AddMessagesToSession(group.Key, group, effects.IsReplaying);
            }
        }

        public void Process(RemoteActivityResultReceived evt, EffectTracker effects)
        {
            // queues task message (from another partition) in a new or existing session
            this.AddMessageToSession(evt.Result, false, effects.IsReplaying);
        }

        public void Process(ClientTaskMessagesReceived evt, EffectTracker effects)
        {
            // queues task message (from a client) in a new or existing session
            var instanceId = evt.TaskMessages[0].OrchestrationInstance.InstanceId;
            this.AddMessagesToSession(instanceId, evt.TaskMessages, effects.IsReplaying);
        }

        public void Process(TimerFired timerFired, EffectTracker effects)
        {
            // queues a timer fired message in a session
            this.AddMessageToSession(timerFired.TaskMessage, false, effects.IsReplaying);
        }

        public void Process(ActivityCompleted activityCompleted, EffectTracker effects)
        {
            // queues an activity-completed message in a session
            this.AddMessageToSession(activityCompleted.Response, false, effects.IsReplaying);
        }

        public void Process(CreationRequestProcessed creationRequestProcessed, EffectTracker effects)
        {
            // queues the execution started message
            this.AddMessageToSession(creationRequestProcessed.TaskMessage, true, effects.IsReplaying);
        }
        
        public void Process(BatchProcessed evt, EffectTracker effects)
        {
            // updates the session and other state

            // our instance may already be obsolete if it has been forcefully replaced.
            // This can manifest as the instance having disappeared, or as the current instance having
            // a different session id
            if (!this.Sessions.TryGetValue(evt.InstanceId, out var session) || session.SessionId != evt.SessionId)
            {
                return;           
            };

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
                    new OrchestrationMessageBatch(instanceId, session, this.Partition);
                }
            }
        }
    }
}
