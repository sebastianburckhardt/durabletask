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

        // ----- starting/stopping of the orchestration service

        // we are grouping all events by this instance of EventSourcedOrchestrationService using a single activity id
        // and since there is only one of these per machine, we can save its id in this static field.
        private static Guid serviceInstanceId;

        [Event(10, Level = EventLevel.Informational, Opcode = EventOpcode.Start, Version = 1)]
        public void ServiceStarted(Guid ServiceInstanceId, string Account, string TaskHub, string WorkerName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(ServiceInstanceId);
            this.WriteEvent(10, ServiceInstanceId, Account, TaskHub, WorkerName, ExtensionVersion);
            EtwSource.serviceInstanceId = ServiceInstanceId;
        }

        [Event(11, Level = EventLevel.Informational, Opcode = EventOpcode.Stop, Version = 1)]
        public void ServiceStopped(Guid ServiceInstanceId, string Account, string TaskHub, string WorkerName, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(ServiceInstanceId);
            this.WriteEvent(11, ServiceInstanceId, Account, TaskHub, WorkerName, ExtensionVersion);
        }

        // ----- partition lifecycle, error handling, and periodic tasks

        [Event(20, Level = EventLevel.Informational, Version = 1)]
        public void PartitionStarted(string Account, string TaskHub, int PartitionId, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(20, Account, TaskHub, PartitionId, ExtensionVersion);
        }

        [Event(21, Level = EventLevel.Informational, Version = 1)]
        public void PartitionStopped(string Account, string TaskHub, int PartitionId, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(21, Account, TaskHub, PartitionId, ExtensionVersion);
        }

        [Event(22, Level = EventLevel.Error, Version = 1)]
        public void PartitionError(string Account, string TaskHub, int PartitionId, string Context, bool TerminatesPartition, string Message, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(22, Account, TaskHub, PartitionId, Context, TerminatesPartition, Message, Details, ExtensionVersion);
        }

        [Event(23, Level = EventLevel.Warning, Version = 1)]
        public void PartitionWarning(string Account, string TaskHub, int PartitionId, string Context, bool TerminatesPartition, string Message, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(23, Account, TaskHub, PartitionId, Context, TerminatesPartition, Message, Details, ExtensionVersion);
        }

        [Event(24, Level = EventLevel.Informational, Version = 1)]
        public void PartitionOffloadDecision(string Account, string TaskHub, int PartitionId, long CommitLogPosition, string MessageId, int ReportedLocalLoad, int Pending, int Backlog, int Remotes, string ReportedRemoteLoad, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(24, Account, TaskHub, PartitionId, CommitLogPosition, MessageId, ReportedLocalLoad, Pending, Backlog, Remotes, ReportedRemoteLoad, ExtensionVersion);
        }

        // ----- task message tracing

        [Event(30, Level = EventLevel.Informational, Version = 1)]
        public void TaskMessageReceived(string Account, string TaskHub, int PartitionId, long CommitLogPosition, string EventType, int TaskEventId, string InstanceId, string ExecutionId, string MessageId, string SessionPosition, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(30, Account, TaskHub, PartitionId, CommitLogPosition, EventType, TaskEventId, InstanceId, ExecutionId, MessageId, SessionPosition, ExtensionVersion);
        }

        [Event(31, Level = EventLevel.Informational, Version = 1)]
        public void TaskMessageSent(string Account, string TaskHub, int PartitionId, string EventType, int TaskEventId, string InstanceId, string ExecutionId, string MessageId, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(31, Account, TaskHub, PartitionId, EventType, TaskEventId, InstanceId, ExecutionId, MessageId, ExtensionVersion);
        }

        [Event(32, Level = EventLevel.Informational, Version = 1)]
        public void TraceInstanceUpdate(string Account, string TaskHub, string InstanceId, string ExecutionId, string WorkItemId, int NewEventCount, int TotalEventCount, string NewEvents, string EventType, int Episode, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(32, Account, TaskHub, InstanceId, ExecutionId, WorkItemId, NewEventCount, TotalEventCount, NewEvents, EventType, Episode, ExtensionVersion);
        }

        // ----- partition event processing

        [Event(40, Level = EventLevel.Verbose, Version = 1)]
        public void PartitionEventProcessed(string Account, string TaskHub, int PartitionId, long CommitLogPosition, string MessageId, string EventInfo, long NextCommitLogPosition, long NextInputQueuePosition, bool IsReplaying, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(40, Account, TaskHub, PartitionId, CommitLogPosition, MessageId, EventInfo, NextCommitLogPosition, NextInputQueuePosition, IsReplaying, ExtensionVersion);
        }

        [Event(41, Level = EventLevel.Verbose, Version = 1)]
        public void PartitionEventSent(string Account, string TaskHub, int PartitionId, long CommitLogPosition, string MessageId, string SentMessageId, string EventInfo, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(41, Account, TaskHub, PartitionId, CommitLogPosition, MessageId, SentMessageId, EventInfo, ExtensionVersion);
        }

        [Event(42, Level = EventLevel.Verbose, Version = 1)]
        public void PartitionEventDetail(string Account, string TaskHub, int PartitionId, long CommitLogPosition, string MessageId, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(42, Account, TaskHub, PartitionId, CommitLogPosition, MessageId, Details, ExtensionVersion);
        }

        // -----  client lifecycle, error handling, and periodic tasks

        [Event(50, Level = EventLevel.Informational, Version = 1)]
        public void ClientStarted(string Account, string TaskHub, Guid ClientId, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(50, Account, TaskHub, ClientId, ExtensionVersion);
        }

        [Event(51, Level = EventLevel.Informational, Version = 1)]
        public void ClientStopped(string Account, string TaskHub, Guid ClientId, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(51, Account, TaskHub, ClientId, ExtensionVersion);
        }

        [Event(52, Level = EventLevel.Error, Version = 1)]
        public void ClientErrorReported(string Account, string TaskHub, Guid ClientId, string Context, string Message, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(52, Account, TaskHub, ClientId, Context, Message, Details, ExtensionVersion);
        }

        // ----- client event processing

        [Event(60, Level = EventLevel.Verbose, Version = 1)]
        public void ClientEventReceived(string Account, string TaskHub, Guid ClientId, string MessageId, string EventInfo, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(60, Account, TaskHub, ClientId, MessageId, EventInfo, ExtensionVersion);
        }

        [Event(61, Level = EventLevel.Verbose, Version = 1)]
        public void ClientEventSent(string Account, string TaskHub, Guid ClientId, string MessageId, string EventInfo, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(61, Account, TaskHub, ClientId, MessageId, EventInfo, ExtensionVersion);
        }

        // ----- faster storage events

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

        // ----- lease management events

        [Event(90, Level = EventLevel.Informational, Version = 1)]
        public void LeaseAcquired(string Account, string TaskHub, int PartitionId, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(90, Account, TaskHub, PartitionId, ExtensionVersion);
        }

        [Event(91, Level = EventLevel.Informational, Version = 1)]
        public void LeaseReleased(string Account, string TaskHub, int PartitionId, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(91, Account, TaskHub, PartitionId, ExtensionVersion);
        }

        [Event(92, Level = EventLevel.Warning, Version = 1)]
        public void LeaseLost(string Account, string TaskHub, int PartitionId, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(92, Account, TaskHub, PartitionId, Details, ExtensionVersion);
        }

        [Event(93, Level = EventLevel.Verbose, Version = 1)]
        public void LeaseProgress(string Account, string TaskHub, int PartitionId, string Details, string ExtensionVersion)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(93, Account, TaskHub, PartitionId, Details, ExtensionVersion);
        }
    }
}
