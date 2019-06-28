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
        public Dictionary<string, SessionState> Sessions { get; set; }

        [DataMember]
        public long SequenceNumber { get; set; }

        [DataContract]
        internal class SessionState
        {
            [DataMember]
            public long SessionId { get; set; }

            [DataMember]
            public long BatchStart { get; set; }

            [DataMember]
            public List<TaskMessage> Batch { get; set; }
        }

        private void AddMessageToSession(TaskMessage message)
        {
            var instanceId = message.OrchestrationInstance.InstanceId;

            if (!this.Sessions.TryGetValue(instanceId, out var session))
            {
                this.Sessions[instanceId] = session = new SessionState()
                {
                    SessionId = SequenceNumber++,
                    Batch = new List<TaskMessage>(),
                    BatchStart = 0
                };
            }

            session.Batch.Add(message);
        }

        public bool Apply(TaskMessageReceived taskMessageReceived)
        {
            if (!AlreadyApplied(taskMessageReceived))
            {
                this.AddMessageToSession(taskMessageReceived.TaskMessage);
            }

            return true;
        }

        public bool Apply(TimerFired timerFired)
        {
            if (!AlreadyApplied(timerFired))
            {
                this.AddMessageToSession(timerFired.TimerFiredMessage);
            }

            return true;
        }

        public bool Apply(ActivityCompleted activityCompleted)
        {
            if (! AlreadyApplied(activityCompleted))
            {
                this.AddMessageToSession(activityCompleted.Response);
            }

            return true;
        }

        public bool Apply(BatchProcessed evt)
        {
            if (!this.Sessions.TryGetValue(evt.InstanceId, out var session)
                || session.SessionId != evt.SessionId
                || session.BatchStart != evt.BatchStart)
            {
                // ignore this event, as the indicated messages for this session have already been processed
                return false;
            }

            session.Batch.RemoveRange(0, evt.BatchLength);
            session.BatchStart += evt.BatchLength;

            // deliver messages handled by this partition
            foreach (var msg in evt.OrchestratorMessages)
            {
                if (this.LocalPartition.Handles(msg))
                {
                    this.AddMessageToSession(msg);
                }
            }

            if (evt.ContinuedAsNewMessage != null)
            {
                session.Batch.Add(evt.ContinuedAsNewMessage);
            }

            if (session.Batch.Count == 0)
            {
                // no more pending messages for this instance, so we delete the session.
                this.Sessions.Remove(evt.InstanceId);
            }
        }

    }
}
