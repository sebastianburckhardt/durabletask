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
    internal class OrchestrationMessageBatch : InternalReadEvent
    {
        public string InstanceId;
        public long SessionId;
        public long BatchStartPosition;
        public int BatchLength;
        public bool ForceNewExecution;
        public List<TaskMessage> MessagesToProcess;
        public string WorkItemId;

        public override EventId EventId => EventId.MakePartitionInternalEventId(this.WorkItemId);
            
        public OrchestrationMessageBatch(string instanceId, SessionsState.Session session, Partition partition)
        {
            this.InstanceId = instanceId;
            this.SessionId = session.SessionId;
            this.BatchStartPosition = session.BatchStartPosition;
            this.BatchLength = session.Batch.Count;
            this.ForceNewExecution = session.ForceNewExecution && session.BatchStartPosition == 0;
            this.MessagesToProcess = session.Batch.ToList(); // make a copy, because it can be modified when new messages arrive
            this.WorkItemId = SessionsState.GetWorkItemId(partition.PartitionId, SessionId, BatchStartPosition, BatchLength);
      
            partition.EventDetailTracer?.TraceDetail($"Prefetching instance={this.InstanceId} batch={this.WorkItemId}");

            // continue when we have the history state loaded, which gives us the latest state and/or cursor
            partition.SubmitInternalEvent(this);
        }

        public override TrackedObjectKey ReadTarget => TrackedObjectKey.History(this.InstanceId);

        public override void OnReadComplete(TrackedObject s, Partition partition)
        {
            var historyState = (HistoryState)s;
            OrchestrationWorkItem workItem = null;

            if (this.ForceNewExecution || historyState == null)
            {
                // we either have no previous instance, or want to replace the previous instance
                partition.EventDetailTracer?.TraceDetail($"Starting fresh orchestration instance={this.InstanceId} batch={this.WorkItemId}");
                workItem = new OrchestrationWorkItem(partition, this);
            }
            else if (historyState.CachedOrchestrationWorkItem != null)
            {
                partition.EventDetailTracer?.TraceDetail($"Continuing orchestration from cached cursor instance={this.InstanceId} batch={WorkItemId}");
                workItem = historyState.CachedOrchestrationWorkItem;
                workItem.SetNextMessageBatch(this);
            }
            else
            {
                partition.EventDetailTracer?.TraceDetail($"Continuing orchestration from saved history instance={this.InstanceId} batch={WorkItemId}");
                workItem = new OrchestrationWorkItem(partition, this, historyState.History);
            }

            if (!this.IsExecutableInstance(workItem, out var warningMessage))
            {
                partition.EventDetailTracer?.TraceDetail($"Discarding messages for non-executable orchestration instance={this.InstanceId} batch={WorkItemId} reason={warningMessage}");

                // discard the messages, by marking the batch as processed without updating the state
                partition.SubmitInternalEvent(new BatchProcessed()
                {
                    PartitionId = partition.PartitionId,
                    SessionId = this.SessionId,
                    InstanceId = this.InstanceId,
                    BatchStartPosition = this.BatchStartPosition,
                    BatchLength = this.BatchLength,
                    NewEvents = null,
                    State = null,
                    WorkItem = workItem,
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
                partition.EnqueueOrchestrationWorkItem(workItem);
            }
        }

        bool IsExecutableInstance(TaskOrchestrationWorkItem workItem, out string message)
        {
            if (workItem.OrchestrationRuntimeState.ExecutionStartedEvent == null && !this.MessagesToProcess.Any(msg => msg.Event is ExecutionStartedEvent))
            {
                if (DurableTask.Core.Common.Entities.AutoStart(this.InstanceId, this.MessagesToProcess))
                {
                    message = default;
                    return true;
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
