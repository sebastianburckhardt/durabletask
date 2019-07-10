using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using DurableTask.Core;
using DurableTask.Core.History;

namespace DurableTask.EventHubs
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
        }

        [IgnoreDataMember]
        public override string Key => "@@outbox";

        protected override void Restore()
        {
            // create work items for all sessions
            foreach(var kvp in Sessions)
            {
                OrchestrationWorkItem.EnqueueWorkItem(LocalPartition, kvp.Key, kvp.Value);
            }
        }

        private void AddMessageToSession(TaskMessage message)
        {
            var instanceId = message.OrchestrationInstance.InstanceId;

            if (this.Sessions.TryGetValue(instanceId, out var session))
            {
                session.Batch.Add(message);
            }
            else
            {
                this.Sessions[instanceId] = session = new Session()
                {
                    SessionId = SequenceNumber++,
                    Batch = new List<TaskMessage>() { message },
                    BatchStartPosition = 0
                };

                OrchestrationWorkItem.EnqueueWorkItem(LocalPartition, instanceId, session);
            }
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
                && session.SessionId == evt.SessionId
                && session.BatchStartPosition == evt.StartPosition)
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

                if (evt.TimerMessages?.Count > 0)
                {
                    apply.Add(State.Timers);
                }

                apply.Add(this);
            }
        }

        public void Apply(BatchProcessed evt)
        {
            var session = this.Sessions[evt.InstanceId];

            session.Batch.RemoveRange(0, evt.Length);
            session.BatchStartPosition += evt.Length;

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
            else
            {
                // there are more messages. Prepare another work item.
                OrchestrationWorkItem.EnqueueWorkItem(LocalPartition, evt.InstanceId, session);
            }
        }
    }
}
