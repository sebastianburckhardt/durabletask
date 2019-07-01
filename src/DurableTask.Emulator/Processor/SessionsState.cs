using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using DurableTask.Core;
using DurableTask.Core.History;

namespace DurableTask.Emulator
{
    [DataContract]
    internal class SessionsState : TrackedObject
    {
        [DataMember]
        public Dictionary<string, Session> Sessions { get; set; }

        [DataMember]
        public long SequenceNumber { get; set; }

        [DataContract]
        internal class Session : CircularLinkedList<Session>.Node
        {
            [DataMember]
            public long SessionId { get; set; }

            [DataMember]
            public long BatchStart { get; set; }

            [DataMember]
            public List<TaskMessage> Batch { get; set; }

            [IgnoreDataMember]
            public string InstanceId { get; set; }

            [IgnoreDataMember]
            public OrchestrationRuntimeState RuntimeState { get; set; } // gets added when loaded

            [IgnoreDataMember]
            public TaskOrchestrationWorkItem WorkItem { get; set; } // gets added when locked
        }

        [IgnoreDataMember]
        public override string Key => "@@outbox";

        protected override void Restore()
        {
            foreach(var kvp in Sessions)
            {
                kvp.Value.InstanceId = kvp.Key;

                // when recovering, all sessions are unlocked to begin with
                LocalPartition.AvailableSessions.Add(kvp.Value);
            }
        }

        private void AddMessageToSession(TaskMessage message)
        {
            var instanceId = message.OrchestrationInstance.InstanceId;

            if (!this.Sessions.TryGetValue(instanceId, out var session))
            {
                this.Sessions[instanceId] = session = new Session()
                {
                    SessionId = SequenceNumber++,
                    Batch = new List<TaskMessage>(),
                    BatchStart = 0
                };
            }

            session.Batch.Add(message);
        }

        public void Apply(TaskMessageReceived taskMessageReceived)
        {
            this.AddMessageToSession(taskMessageReceived.TaskMessage);
        }

        public void Apply(TimerFired timerFired)
        {
            this.AddMessageToSession(timerFired.TimerFiredMessage);
        }

        public void Apply(ActivityCompleted activityCompleted)
        {
            this.AddMessageToSession(activityCompleted.Response);
        }

        public void Scope(BatchProcessed evt, List<TrackedObject> scope, List<TrackedObject> apply)
        {
            if (this.Sessions.TryGetValue(evt.InstanceId, out var session)
                && session.SessionId != evt.SessionId
                && session.BatchStart != evt.BatchStart)
            {
                apply.Add(State.GetInstance(evt.InstanceId));

                if (evt.LocalOrchestratorMessages?.Count > 0)
                {
                    apply.Add(State.Outbox);
                }

                if (evt.ActivityMessages?.Count > 0)
                {
                    apply.Add(State.Activities);
                }

                if (evt.WorkItemTimerMessages?.Count > 0)
                {
                    apply.Add(State.Timers);
                }

                apply.Add(this);
            }
        }

        public void Apply(BatchProcessed evt)
        {
            var session = this.Sessions[evt.InstanceId];

            session.Batch.RemoveRange(0, evt.BatchLength);
            session.BatchStart += evt.BatchLength;

            // deliver messages handled by this partition
            foreach (var msg in evt.LocalOrchestratorMessages)
            {
                this.AddMessageToSession(msg);
            }

            if (session.Batch.Count == 0)
            {
                // no more pending messages for this instance, so we delete the session.
                this.Sessions.Remove(evt.InstanceId);
            }
        }
    }
}
