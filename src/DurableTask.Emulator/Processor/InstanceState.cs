﻿using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using DurableTask.Core;
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


        public override void Process(ClocksState processor)
        {
            base.Process(processor);

            var instanceId = TaskMessage.OrchestrationInstance.InstanceId;

            switch (TaskMessage.Event.EventType)
            {
                case EventType.ExecutionStarted:
                    {
                        if (processor.Instances.TryGetValue(instanceId, out var state)
                            && state.OrchestrationState.OrchestrationStatus != OrchestrationStatus.ContinuedAsNew)
                        {
                            throw new OrchestrationAlreadyExistsException($"An orchestration with id '{instanceId}' already exists. It is in state {state.OrchestrationState.OrchestrationStatus}");
                        }

                        var executionStartedEvent = (ExecutionStartedEvent)TaskMessage.Event;

                        var newState = new OrchestrationState
                        {
                            OrchestrationInstance = new OrchestrationInstance
                            {
                                InstanceId = instanceId,
                                ExecutionId = TaskMessage.OrchestrationInstance.ExecutionId,
                            },
                            CreatedTime = processor.Clock,
                            LastUpdatedTime = processor.Clock,
                            OrchestrationStatus = OrchestrationStatus.Pending,
                            Version = executionStartedEvent.Version,
                            Name = executionStartedEvent.Name,
                            Input = executionStartedEvent.Input,
                        };

                        processor.Instances[instanceId] = new InstanceState()
                        {
                            OrchestrationState = newState,
                            History = new List<HistoryEvent>(),
                        };

                        break;
                    }

                case EventType.



            }
        }
    }
}
