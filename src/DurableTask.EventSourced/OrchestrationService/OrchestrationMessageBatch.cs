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

namespace DurableTask.EventSourced
{
    internal class OrchestrationMessageBatch : StorageAbstraction.IInternalReadonlyEvent
    {
        public Partition Partition;
        public string InstanceId;
        public long SessionId;
        public long BatchStartPosition;
        public int BatchLength;
        public bool ForceNewExecution;
        public List<TaskMessage> MessagesToProcess;

        public string CorrelationId => $"S{SessionId:D6}:{BatchStartPosition}[{BatchLength}]";

        public string EventIdString => $"{this.Partition.PartitionId:D2}-{CorrelationId}";

        public OrchestrationMessageBatch(string instanceId, SessionsState.Session session)
        {
            this.InstanceId = instanceId;
            this.SessionId = session.SessionId;
            this.BatchStartPosition = session.BatchStartPosition;
            this.BatchLength = session.Batch.Count;
            this.ForceNewExecution = session.ForceNewExecution && session.BatchStartPosition == 0;
            this.MessagesToProcess = session.Batch.ToList(); // make a copy, because it can be modified when new messages arrive
        }

        public void ScheduleWorkItem(Partition partition)
        {
            partition.DetailTracer?.TraceDetail($"Prefetching instance={this.InstanceId} batch={this.CorrelationId}");
            this.Partition = partition;

            // continue when we have the history state loaded, which gives us the latest state and/or cursor
            partition.State.SubmitInternalReadonlyEvent(this);
        }

        public TrackedObjectKey ReadTarget => TrackedObjectKey.History(this.InstanceId);

        public void OnReadComplete(TrackedObject s)
        {
            var historyState = (HistoryState)s;
            OrchestrationWorkItem workItem = null;

            if (this.ForceNewExecution || historyState == null)
            {
                // we either have no previous instance, or want to replace the previous instance
                this.Partition.DetailTracer?.TraceDetail($"Starting fresh orchestration instance={this.InstanceId} batch={this.CorrelationId}");
                workItem = new OrchestrationWorkItem(this);
            }
            else if (historyState.CachedOrchestrationWorkItem != null)
            {
                this.Partition.DetailTracer?.TraceDetail($"Continuing orchestration from cached cursor instance={this.InstanceId} batch={CorrelationId}");
                workItem = historyState.CachedOrchestrationWorkItem;
                workItem.SetNextMessageBatch(this);
            }
            else
            {
                this.Partition.DetailTracer?.TraceDetail($"Continuing orchestration from saved history instance={this.InstanceId} batch={CorrelationId}");
                workItem = new OrchestrationWorkItem(this, historyState.History);
            }

            if (!this.IsExecutableInstance(workItem, out var warningMessage))
            {
                this.Partition.DetailTracer?.TraceDetail($"Discarding messages for non-executable orchestration instance={this.InstanceId} batch={CorrelationId} reason={warningMessage}");

                // discard the messages, by marking the batch as processed without updating the state
                this.Partition.Submit(new BatchProcessed()
                {
                    PartitionId = this.Partition.PartitionId,
                    SessionId = this.SessionId,
                    InstanceId = this.InstanceId,
                    BatchStartPosition = this.BatchStartPosition,
                    BatchLength = this.BatchLength,
                    NewEvents = null,
                    State = null,
                    CachedWorkItem = null,
                    ActivityMessages = null,
                    LocalMessages = null,
                    RemoteMessages = null,
                    TimerMessages = null,
                    Timestamp = DateTime.UtcNow,
                });
            }
            else
            {
                // the work item is ready to process
                this.Partition.EnqueueOrchestrationWorkItem(workItem);
            }
        }

        bool IsExecutableInstance(TaskOrchestrationWorkItem workItem, out string message)
        {
            if (workItem.OrchestrationRuntimeState.ExecutionStartedEvent == null && !this.MessagesToProcess.Any(msg => msg.Event is ExecutionStartedEvent))
            {
                if (this.InstanceId.StartsWith("@")
                    && this.MessagesToProcess[0].Event.EventType == EventType.EventRaised
                    && this.MessagesToProcess[0].OrchestrationInstance.ExecutionId == null)
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
                    this.MessagesToProcess.Insert(0, taskMessage);
                }
                else
                {
                    message = workItem.NewMessages.Count == 0 ? "No such instance" : "Instance is corrupted";
                    return false;
                }
            }

            if (workItem.OrchestrationRuntimeState.ExecutionStartedEvent != null &&
                workItem.OrchestrationRuntimeState.OrchestrationStatus != OrchestrationStatus.Running &&
                workItem.OrchestrationRuntimeState.OrchestrationStatus != OrchestrationStatus.Pending)
            {
                message = $"Instance is {workItem.OrchestrationRuntimeState.OrchestrationStatus}";
                return false;
            }

            message = null;
            return true;
        }
    }
}
