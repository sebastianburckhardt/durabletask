using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using DurableTask.Core;
using DurableTask.Core.Exceptions;
using DurableTask.Core.History;

namespace DurableTask.Emulator
{
    [DataContract]
    internal class InstanceState : TrackedObject
    {
        [DataMember]
        public OrchestrationState OrchestrationState { get; set; }

        [DataMember]
        public List<HistoryEvent> History { get; set; }

        [IgnoreDataMember]
        public override string Key => OrchestrationState.OrchestrationInstance.InstanceId;

        [IgnoreDataMember]
        private OrchestrationRuntimeState cachedRuntimeState;

        public OrchestrationRuntimeState GetRuntimeState()
        {
            return cachedRuntimeState ?? (cachedRuntimeState = new OrchestrationRuntimeState(History));
        }

        public void Scope(OrchestrationCreationMessageReceived evt, List<TrackedObject> scope, List<TrackedObject> apply)
        {
            if (OrchestrationState != null
                && evt.DedupeStatuses != null
                && evt.DedupeStatuses.Contains(OrchestrationState.OrchestrationStatus))
            {
                // An instance in this state already exists.
                return;
            }

            apply.Add(State.Sessions);
            apply.Add(this);
        }

        public void Apply(OrchestrationCreationMessageReceived evt)
        {
            var ee = evt.ExecutionStartedEvent;

            OrchestrationState = new OrchestrationState
            {
                OrchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = evt.TaskMessage.OrchestrationInstance.InstanceId,
                    ExecutionId = evt.TaskMessage.OrchestrationInstance.ExecutionId,
                },
                CreatedTime = evt.Timestamp,
                LastUpdatedTime = evt.Timestamp,
                OrchestrationStatus = OrchestrationStatus.Pending,
                Version = ee.Version,
                Name = ee.Name,
                Input = ee.Input,
            };
        }

        public void Apply(BatchProcessed evt)
        {
            if (evt.State.OrchestrationInstance.ExecutionId != this.OrchestrationState.OrchestrationInstance.ExecutionId)
            {
                History.Clear();
                cachedRuntimeState = null;
            }

            History.AddRange(evt.NewEvents);

            cachedRuntimeState?.NewEvents.Clear();

            OrchestrationState = evt.State;
        }
    }
}