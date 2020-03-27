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
using System.Diagnostics.Tracing;
using System.Linq;
using System.Threading;
using DurableTask.Core;
using Microsoft.Extensions.Logging;

namespace DurableTask.EventSourced
{
    /// <summary>
    /// ETW Event Provider for the DurableTask.EventSourced provider extension.
    /// </summary>
    /// <remarks>
    /// The ETW Provider ID for this event source is {b3b94da0-1edd-53a7-435e-53129d278be4}.
    /// We list all events from the various layers (transport, storage) in this single file; however,
    /// we do have separate helper classes for each component.
    /// </remarks>
    [EventSource(Name = "DurableTask-EventSourced")]
    class EtwSource : EventSource
    {
        /// <summary>
        /// Singleton instance used for writing events.
        /// </summary>
        public static readonly EtwSource Log = new EtwSource();

        // we should always check if verbose is enabled before doing extensive string formatting for a verbose event
        public bool IsVerboseEnabled => this.IsEnabled(EventLevel.Verbose, EventKeywords.None);

        // ----- orchestration service lifecycle

        // we are grouping all events by this instance of EventSourcedOrchestrationService using a single activity id
        // and since there is only one of these per machine, we can save its id in this static field.
        private static Guid serviceInstanceId;

        [Event(10, Level = EventLevel.Informational, Opcode = EventOpcode.Start, Version = 1)]
        public void OrchestrationServiceStarted(Guid OrchestrationServiceInstanceId, string Account, string TaskHub, string WorkerName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(OrchestrationServiceInstanceId);
            this.WriteEvent(10, OrchestrationServiceInstanceId, Account, TaskHub, WorkerName, ExtensionVersion);
            EtwSource.serviceInstanceId = OrchestrationServiceInstanceId;
        }

        [Event(11, Level = EventLevel.Informational, Opcode = EventOpcode.Stop, Version = 1)]
        public void OrchestrationServiceStopped(Guid OrchestrationServiceInstanceId, string Account, string TaskHub, string WorkerName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(OrchestrationServiceInstanceId);
            this.WriteEvent(11, OrchestrationServiceInstanceId, Account, TaskHub, WorkerName, ExtensionVersion);
        }

        // ----- partition and client lifecycles

        [Event(20, Level = EventLevel.Informational, Version = 1)]
        public void PartitionProgress(string Account, string TaskHub, int PartitionId, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(20, Account, TaskHub, PartitionId, Details, ExtensionVersion);
        }

        [Event(21, Level = EventLevel.Warning, Version = 1)]
        public void PartitionWarning(string Account, string TaskHub, int PartitionId, string Context, bool TerminatesPartition, string Message, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(21, Account, TaskHub, PartitionId, Context, TerminatesPartition, Message, Details, ExtensionVersion);
        }

        [Event(22, Level = EventLevel.Error, Version = 1)]
        public void PartitionError(string Account, string TaskHub, int PartitionId, string Context, bool TerminatesPartition, string Message, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(22, Account, TaskHub, PartitionId, Context, TerminatesPartition, Message, Details, ExtensionVersion);
        }

        [Event(23, Level = EventLevel.Informational, Version = 1)]
        public void ClientProgress(string Account, string TaskHub, Guid ClientId, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(23, Account, TaskHub, ClientId, Details, ExtensionVersion);
        }

        [Event(24, Level = EventLevel.Warning, Version = 1)]
        public void ClientWarning(string Account, string TaskHub, Guid ClientId, string Context, string Message, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(24, Account, TaskHub, ClientId, Context, Message, Details, ExtensionVersion);
        }

        [Event(25, Level = EventLevel.Error, Version = 1)]
        public void ClientError(string Account, string TaskHub, Guid ClientId, string Context, string Message, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(25, Account, TaskHub, ClientId, Context, Message, Details, ExtensionVersion);
        }

        // ----- specific events relating to DurableTask concepts (TaskMessage, OrchestrationWorkItem, Instance)

        [Event(30, Level = EventLevel.Verbose, Version = 1)]
        public void TaskMessageReceived(string Account, string TaskHub, int PartitionId, long CommitLogPosition, string EventType, int TaskEventId, string InstanceId, string ExecutionId, string MessageId, string SessionPosition, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(30, Account, TaskHub, PartitionId, CommitLogPosition, EventType, TaskEventId, InstanceId, ExecutionId, MessageId, SessionPosition, ExtensionVersion);
        }

        [Event(31, Level = EventLevel.Verbose, Version = 1)]
        public void TaskMessageSent(string Account, string TaskHub, int PartitionId, string EventType, int TaskEventId, string InstanceId, string ExecutionId, string MessageId, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(31, Account, TaskHub, PartitionId, EventType, TaskEventId, InstanceId, ExecutionId, MessageId, ExtensionVersion);
        }

        [Event(32, Level = EventLevel.Verbose, Version = 1)]
        public void TaskMessageDiscarded(string Account, string TaskHub, int PartitionId, string Reason, string EventType, int TaskEventId, string InstanceId, string ExecutionId, string WorkItemId, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(32, Account, TaskHub, PartitionId, Reason, EventType, TaskEventId, InstanceId, ExecutionId, WorkItemId, ExtensionVersion);
        }

        [Event(33, Level = EventLevel.Verbose, Version = 1)]
        public void OrchestrationWorkItemQueued(string Account, string TaskHub, int PartitionId, string ExecutionType, string InstanceId, string WorkItemId, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(33, Account, TaskHub, PartitionId, ExecutionType, InstanceId, WorkItemId, ExtensionVersion);
        }

        [Event(34, Level = EventLevel.Verbose, Version = 1)]
        public void OrchestrationWorkItemDiscarded(string Account, string TaskHub, int PartitionId, string InstanceId, string WorkItemId, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(34, Account, TaskHub, PartitionId, InstanceId, WorkItemId, ExtensionVersion);
        }

        [Event(35, Level = EventLevel.Informational, Version = 1)]
        public void InstanceUpdated(string Account, string TaskHub, int PartitionId, string InstanceId, string ExecutionId, string WorkItemId, int NewEventCount, int TotalEventCount, string NewEvents, string EventType, int Episode, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(35, Account, TaskHub, PartitionId, InstanceId, ExecutionId, WorkItemId, NewEventCount, TotalEventCount, NewEvents, EventType, Episode, ExtensionVersion);
        }

        [Event(36, Level = EventLevel.Verbose, Version = 1)]
        public void InstanceStatusFetched(string Account, string TaskHub, int PartitionId, string InstanceId, string ExecutionId, string EventId, double LatencyMs, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(36, Account, TaskHub, PartitionId, InstanceId, ExecutionId, EventId, LatencyMs, ExtensionVersion);
        }

        [Event(37, Level = EventLevel.Verbose, Version = 1)]
        public void InstanceHistoryFetched(string Account, string TaskHub, int PartitionId, string InstanceId, string ExecutionId, int EventCount, int Episode, string EventId, double LatencyMs, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(37, Account, TaskHub, PartitionId, InstanceId, ExecutionId, EventCount, Episode, EventId, LatencyMs, ExtensionVersion);
        }

        // ----- general event processing

        [Event(50, Level = EventLevel.Informational, Version = 1)]
        public void PartitionEventProcessed(string Account, string TaskHub, int PartitionId, long CommitLogPosition, string MessageId, string EventInfo, long NextCommitLogPosition, long NextInputQueuePosition, double QueueLatencyMs, double FetchLatencyMs, double LatencyMs, bool IsReplaying, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(50, Account, TaskHub, PartitionId, CommitLogPosition, MessageId, EventInfo, NextCommitLogPosition, NextInputQueuePosition, QueueLatencyMs, FetchLatencyMs, LatencyMs, IsReplaying, ExtensionVersion);
        }

        [Event(51, Level = EventLevel.Verbose, Version = 1)]
        public void PartitionEventDetail(string Account, string TaskHub, int PartitionId, long CommitLogPosition, string MessageId, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(51, Account, TaskHub, PartitionId, CommitLogPosition, MessageId, Details, ExtensionVersion);
        }

        [Event(52, Level = EventLevel.Verbose, Version = 1)]
        public void ClientEventReceived(string Account, string TaskHub, Guid ClientId, string MessageId, string EventInfo, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(52, Account, TaskHub, ClientId, MessageId, EventInfo, ExtensionVersion);
        }

        [Event(53, Level = EventLevel.Verbose, Version = 1)]
        public void ClientEventSent(string Account, string TaskHub, Guid ClientId, string MessageId, string EventInfo, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(53, Account, TaskHub, ClientId, MessageId, EventInfo, ExtensionVersion);
        }

        // ----- statistics and heuristics

        [Event(60, Level = EventLevel.Informational, Version = 1)]
        public void PartitionOffloadDecision(string Account, string TaskHub, int PartitionId, long CommitLogPosition, string MessageId, int ReportedLocalLoad, int Pending, int Backlog, int Remotes, string ReportedRemoteLoad, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(38, Account, TaskHub, PartitionId, CommitLogPosition, MessageId, ReportedLocalLoad, Pending, Backlog, Remotes, ReportedRemoteLoad, ExtensionVersion);
        }

        // ----- Faster Storage

        [Event(70, Level = EventLevel.Informational, Version = 1)]
        public void FasterStoreCreated(string Account, string TaskHub, int PartitionId, long InputQueuePosition, long LatencyMs, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(70, Account, TaskHub, PartitionId, InputQueuePosition, LatencyMs, ExtensionVersion);
        }

        [Event(71, Level = EventLevel.Informational, Version = 1)]
        public void FasterCheckpointStarted(string Account, string TaskHub, int PartitionId, Guid CheckpointId, string Reason, long CommitLogPosition, long InputQueuePosition, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(71, Account, TaskHub, PartitionId, CheckpointId, Reason, CommitLogPosition, InputQueuePosition, ExtensionVersion);
        }

        [Event(72, Level = EventLevel.Informational, Version = 1)]
        public void FasterCheckpointPersisted(string Account, string TaskHub, int PartitionId, Guid CheckpointId, string Reason, long CommitLogPosition, long InputQueuePosition, long LatencyMs, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(72, Account, TaskHub, PartitionId, CheckpointId, Reason, CommitLogPosition, InputQueuePosition, LatencyMs, ExtensionVersion);
        }

        [Event(73, Level = EventLevel.Verbose, Version = 1)]
        public void FasterLogPersisted(string Account, string TaskHub, int PartitionId, long CommitLogPosition, long NumberEvents, long SizeInBytes, long LatencyMs, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(73, Account, TaskHub, PartitionId, CommitLogPosition, NumberEvents, SizeInBytes, LatencyMs, ExtensionVersion);
        }

        [Event(74, Level = EventLevel.Informational, Version = 1)]
        public void FasterCheckpointLoaded(string Account, string TaskHub, int PartitionId, long CommitLogPosition, long InputQueuePosition, long LatencyMs, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(74, Account, TaskHub, PartitionId, CommitLogPosition, InputQueuePosition, LatencyMs, ExtensionVersion);
        }

        [Event(75, Level = EventLevel.Informational, Version = 1)]
        public void FasterLogReplayed(string Account, string TaskHub, int PartitionId, long CommitLogPosition, long InputQueuePosition, long NumberEvents, long SizeInBytes, long LatencyMs, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(75, Account, TaskHub, PartitionId, CommitLogPosition, InputQueuePosition, NumberEvents, SizeInBytes, LatencyMs, ExtensionVersion);
        }

        [Event(76, Level = EventLevel.Error, Version = 1)]
        public void FasterStorageError(string Account, string TaskHub, int PartitionId, string Context, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(76, Account, TaskHub, PartitionId, Context, Details, ExtensionVersion);
        }

        [Event(77, Level = EventLevel.Error, Version = 1)]
        public void FasterBlobStorageError(string Account, string TaskHub, int PartitionId, string Context, string BlobName, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(77, Account, TaskHub, PartitionId, Context, BlobName, Details, ExtensionVersion);
        }

        [Event(78, Level = EventLevel.Warning, Version = 1)]
        public void FasterBlobStorageWarning(string Account, string TaskHub, int PartitionId, string Context, string BlobName, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(78, Account, TaskHub, PartitionId, Context, BlobName, Details, ExtensionVersion);
        }

        [Event(79, Level = EventLevel.Verbose, Version = 1)]
        public void FasterProgress(string Account, string TaskHub, int PartitionId, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(79, Account, TaskHub, PartitionId, Details, ExtensionVersion);
        }

        [Event(80, Level = EventLevel.Informational, Version = 1)]
        public void FasterLeaseAcquired(string Account, string TaskHub, int PartitionId, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(80, Account, TaskHub, PartitionId, ExtensionVersion);
        }

        [Event(81, Level = EventLevel.Informational, Version = 1)]
        public void FasterLeaseReleased(string Account, string TaskHub, int PartitionId, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(81, Account, TaskHub, PartitionId, ExtensionVersion);
        }

        [Event(82, Level = EventLevel.Warning, Version = 1)]
        public void FasterLeaseLost(string Account, string TaskHub, int PartitionId, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(82, Account, TaskHub, PartitionId, Details, ExtensionVersion);
        }

        [Event(83, Level = EventLevel.Verbose, Version = 1)]
        public void FasterLeaseProgress(string Account, string TaskHub, int PartitionId, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(83, Account, TaskHub, PartitionId, Details, ExtensionVersion);
        }

        // ----- EventHubs Transport

        [Event(90, Level = EventLevel.Informational, Version = 1)]
        public void EventHubsInformation(string Account, string TaskHub, string EventHubsNamespace, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(90, Account, TaskHub, EventHubsNamespace, Details, ExtensionVersion);
        }

        [Event(91, Level = EventLevel.Warning, Version = 1)]
        public void EventHubsWarning(string Account, string TaskHub, string EventHubsNamespace, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(91, Account, TaskHub, EventHubsNamespace, Details, ExtensionVersion);
        }

        [Event(92, Level = EventLevel.Error, Version = 1)]
        public void EventHubsError(string Account, string TaskHub, string EventHubsNamespace, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(92, Account, TaskHub, EventHubsNamespace, Details, ExtensionVersion);
        }

        [Event(93, Level = EventLevel.Verbose, Version = 1)]
        public void EventHubsDebug(string Account, string TaskHub, string EventHubsNamespace, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(93, Account, TaskHub, EventHubsNamespace, Details, ExtensionVersion);
        }

        [Event(94, Level = EventLevel.Verbose, Version = 1)]
        public void EventHubsTrace(string Account, string TaskHub, string EventHubsNamespace, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(94, Account, TaskHub, EventHubsNamespace, Details, ExtensionVersion);
        }
    }
}
