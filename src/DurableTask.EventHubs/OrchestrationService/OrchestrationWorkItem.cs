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
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DurableTask.Core;
using DurableTask.Core.History;

namespace DurableTask.EventHubs
{
    internal class OrchestrationWorkItem : TaskOrchestrationWorkItem
    {
        public long SessionId;

        public long BatchStartPosition;

        public int BatchLength;

        public EventHubsOrchestrationService LocalPartition;

        public static void EnqueueWorkItem(EventHubsOrchestrationService localPartition, string instanceId, SessionsState.Session session)
        {
            var workItem = new OrchestrationWorkItem()
            {
                SessionId = session.SessionId,
                BatchStartPosition = session.BatchStartPosition,
                BatchLength = session.Batch.Count,
                LocalPartition = localPartition,
                InstanceId = instanceId,
                LockedUntilUtc = DateTime.MaxValue,
                Session = null,
                NewMessages = session.Batch.ToList(), // make a copy
            };

            Task.Run(workItem.LoadAsync);
        }

        public async Task LoadAsync()
        {
            var r = (Func<OrchestrationRuntimeState>)LocalPartition.State.GetInstance(this.InstanceId).GetRuntimeState;

            // load the runtime state
            this.OrchestrationRuntimeState = await LocalPartition.State.ReadAsync(
                LocalPartition.State.GetInstance(this.InstanceId).GetRuntimeState);

            if (!this.IsExecutableInstance(out var warningMessage))
            {
                // discard the messages, by marking the batch as processed, without updating the state
                await this.LocalPartition.Queue.SendAsync(new BatchProcessed()
                {
                    SessionId = this.SessionId,
                    InstanceId = this.InstanceId,
                    BatchStartPosition = this.BatchStartPosition,
                    BatchLength = this.BatchLength,
                    NewEvents = null,
                    State = null,
                    ActivityMessages = null,
                    LocalOrchestratorMessages = null,
                    RemoteOrchestratorMessages = null,
                    TimerMessages = null,
                    Timestamp = DateTime.UtcNow,
                });
            }
            else
            {
                // the work item is ready to process
                LocalPartition.OrchestrationWorkItemQueue.Add(this);
            }
        }

        bool IsExecutableInstance(out string message)
        {
            if (this.OrchestrationRuntimeState.ExecutionStartedEvent == null && !this.NewMessages.Any(msg => msg.Event is ExecutionStartedEvent))
            {
                if (this.InstanceId.StartsWith("@"))
                {
                    // automatically start this instance
                    var orchestrationInstance = new OrchestrationInstance
                    {
                        InstanceId = this.InstanceId,
                        ExecutionId = Guid.NewGuid().ToString("N"),
                    };
                    var startedEvent = new ExecutionStartedEvent(-1, null)
                    {
                        Name = this.InstanceId,
                        Version = "",
                        OrchestrationInstance = orchestrationInstance
                    };
                    var taskMessage = new TaskMessage()
                    {
                        OrchestrationInstance = orchestrationInstance,
                        Event = startedEvent
                    };
                    this.NewMessages.Insert(0, taskMessage);
                }
                else
                {
                    message = this.OrchestrationRuntimeState.Events.Count == 0 ? "No such instance" : "Instance is corrupted";
                    return false;
                }
            }

            if (this.OrchestrationRuntimeState.ExecutionStartedEvent != null &&
                this.OrchestrationRuntimeState.OrchestrationStatus != OrchestrationStatus.Running &&
                this.OrchestrationRuntimeState.OrchestrationStatus != OrchestrationStatus.Pending)
            {
                message = $"Instance is {this.OrchestrationRuntimeState.OrchestrationStatus}";
                return false;
            }

            message = null;
            return true;
        }
    }
}
