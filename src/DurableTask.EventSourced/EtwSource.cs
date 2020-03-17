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

        // ----- starting/stopping of the host

        // we are grouping all events by this instance of EventSourcedOrchestrationService using a single activity id
        // and since there is only one of these per machine, we can save its id in this static field.
        private static Guid serviceInstanceId;

        [Event(10, Level = EventLevel.Informational, Opcode = EventOpcode.Start, Version = 1)]
        public void HostStarted(Guid hostId, string machineName)
        {
            SetCurrentThreadActivityId(hostId);
            this.WriteEvent(10, hostId, machineName);
            EtwSource.serviceInstanceId = hostId;
        }

        [Event(11, Level = EventLevel.Informational, Opcode = EventOpcode.Stop, Version = 1)]
        public void HostStopped(Guid hostId)
        {
            SetCurrentThreadActivityId(hostId);
            this.WriteEvent(11, hostId);
        }

        // ----- events observed on a partition processor

        [Event(20, Level = EventLevel.Informational, Version = 1)]
        public void PartitionStarted(int partitionId)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(20, partitionId);
        }

        [Event(21, Level = EventLevel.Informational, Version = 1)]
        public void PartitionStopped(int partitionId)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(21, partitionId);
        }

        [Event(22, Level = EventLevel.Error, Version = 1)]
        public void PartitionError(int partitionId, string where, bool isFatal, string message, string exception)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(22, partitionId, where, isFatal, message, exception);
        }

        [Event(23, Level = EventLevel.Warning, Version = 1)]
        public void PartitionWarning(int partitionId, string where, bool isFatal, string message, string exception)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(23, partitionId, where, isFatal, message, exception);
        }

        [Event(30, Level = EventLevel.Verbose, Version = 1)]
        public void PartitionEventProcessed(int partitionId, ulong commitLogPosition, string eventId, string eventInfo, ulong nextCommitLogPosition, ulong nextInputQueuePosition, bool replaying)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(30, partitionId, commitLogPosition, eventId, eventInfo, nextCommitLogPosition, nextInputQueuePosition, replaying);
        }

        [Event(31, Level = EventLevel.Verbose, Version = 1)]
        public void PartitionEventSent(int partitionId, ulong commitLogPosition, string contextEventId, string eventId, string eventInfo)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(31, partitionId, commitLogPosition, contextEventId, eventId, eventInfo);
        }

        [Event(32, Level = EventLevel.Verbose, Version = 1)]
        public void PartitionDetail(int partitionId, ulong commitLogPosition, string context, string details)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(32, partitionId, commitLogPosition, context, details);
        }

        [Event(33, Level = EventLevel.Verbose, Version = 1)]
        public void OffloadDecision(int partitionId, int reportedLocalLoad, int pending, int backlog, int remotes, string reportedRemoteLoad)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(33, partitionId, reportedLocalLoad, pending, backlog, remotes, reportedRemoteLoad);
        }

        // -----  events observed on a client

        [Event(50, Level = EventLevel.Informational, Version = 1)]
        public void ClientStarted(Guid clientId)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(50, clientId);
        }

        [Event(51, Level = EventLevel.Informational, Version = 1)]
        public void ClientStopped(Guid clientId)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(51, clientId);
        }

        [Event(52, Level = EventLevel.Error, Version = 1)]
        public void ClientErrorReported(Guid clientId, string where, string exceptionType, string message)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(52, clientId, where, exceptionType, message);
        }

        [Event(60, Level = EventLevel.Verbose, Version = 1)]
        public void ClientEventReceived(Guid clientId, string eventInfo)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(60, clientId, eventInfo);
        }

        [Event(61, Level = EventLevel.Verbose, Version = 1)]
        public void ClientEventSent(Guid clientId, string eventInfo)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(61, clientId, eventInfo);
        }

        // ----- faster storage events

        [Event(70, Level = EventLevel.Informational, Version = 1)]
        public void FasterStoreCreated(int partitionId, ulong inputPosition, long elapsedMilliseconds)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(70, partitionId, inputPosition, elapsedMilliseconds);
        }

        [Event(71, Level = EventLevel.Informational, Version = 1)]
        public void FasterCheckpointStarted(int partitionId, Guid checkpointGuid, string reason, ulong commitPosition, ulong inputPosition)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(71, partitionId, checkpointGuid, reason, commitPosition, inputPosition);
        }

        [Event(72, Level = EventLevel.Informational, Version = 1)]
        public void FasterCheckpointPersisted(int partitionId, Guid checkpointGuid, string reason, ulong commitPosition, ulong inputPosition, long elapsedMilliseconds)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(72, partitionId, checkpointGuid, reason, commitPosition, inputPosition, elapsedMilliseconds);
        }

        [Event(73, Level = EventLevel.Verbose, Version = 1)]
        public void FasterLogPersisted(int partitionId, ulong commitPosition, ulong numEvents, ulong numBytes, long elapsedMilliseconds)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(73, partitionId, commitPosition, numEvents, numBytes, elapsedMilliseconds);
        }

        [Event(74, Level = EventLevel.Informational, Version = 1)]
        public void FasterCheckpointLoaded(int partitionId, ulong commitPosition, ulong inputPosition, long elapsedMilliseconds)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(74, partitionId, commitPosition, inputPosition, elapsedMilliseconds);
        }

        [Event(75, Level = EventLevel.Informational, Version = 1)]
        public void FasterLogReplayed(int partitionId, ulong commitPosition, ulong inputPosition, ulong numEvents, ulong numBytes, long elapsedMilliseconds)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(75, partitionId, commitPosition, inputPosition, numEvents, numBytes, elapsedMilliseconds);
        }

        [Event(76, Level = EventLevel.Error, Version = 1)]
        public void FasterStorageError(int partitionId, string operation, string details)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(76, partitionId, operation, details);
        }

        [Event(77, Level = EventLevel.Error, Version = 1)]
        public void FasterBlobStorageError(int partitionId, string operation, string blobName, string details)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(77, partitionId, operation, blobName, details);
        }

        [Event(78, Level = EventLevel.Warning, Version = 1)]
        public void FasterBlobStorageWarning(int partitionId, string operation, string blobName, string details)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(78, partitionId, operation, blobName, details);
        }

        [Event(79, Level = EventLevel.Verbose, Version = 1)]
        public void FasterProgress(int partitionId, string operation)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(79, partitionId, operation);
        }

        // ----- lease management events

        [Event(90, Level = EventLevel.Informational, Version = 1)]
        public void LeaseAcquired(int partitionId)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(90, partitionId);
        }

        [Event(91, Level = EventLevel.Informational, Version = 1)]
        public void LeaseReleased(int partitionId)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(91, partitionId);
        }

        [Event(92, Level = EventLevel.Warning, Version = 1)]
        public void LeaseLost(int partitionId, string operation)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(92, partitionId, operation);
        }

        [Event(93, Level = EventLevel.Verbose, Version = 1)]
        public void LeaseProgress(int partitionId, string operation)
        {
            SetCurrentThreadActivityId(serviceInstanceId);
            this.WriteEvent(93, partitionId, operation);
        }

        // ----- transport events

        //[Event(30, Level = EventLevel.Verbose, Version = 1)]
        //public void PacketReceived(int partitionId, ulong commitPosition, ulong inputPosition, bool replaying, string workItem, string eventInfo)
        //{
        //    SetCurrentThreadActivityId(serviceInstanceId);
        //    this.WriteEvent(30, partitionId, commitPosition, inputPosition, replaying, workItem, eventInfo);
        //}

        //[Event(31, Level = EventLevel.Verbose, Version = 1)]
        //public void PartitionEventSent(int partitionId, ulong context, string workItem, string eventInfo)
        //{
        //    SetCurrentThreadActivityId(serviceInstanceId);
        //    this.WriteEvent(31, partitionId, context, workItem, eventInfo);
        //}

        //// ----- EventHubs EventProcessor events
        //[Event(100, Level = EventLevel.Informational, Version = 1)]
        //public void EventProcessorInfo(int partitionId, string Details)
        //{
        //    SetCurrentThreadActivityId(serviceInstanceId);
        //    this.WriteEvent(90, partitionId);
        //}

        //[Event(101, Level = EventLevel.Informational, Version = 1)]
        //public void LeaseReleased(int partitionId)
        //{
        //    SetCurrentThreadActivityId(serviceInstanceId);
        //    this.WriteEvent(91, partitionId);
        //}

        //[Event(012, Level = EventLevel.Warning, Version = 1)]
        //public void LeaseLost(int partitionId, string operation)
        //{
        //    SetCurrentThreadActivityId(serviceInstanceId);
        //    this.WriteEvent(92, partitionId, operation);
        //}

        //[Event(93, Level = EventLevel.Verbose, Version = 1)]
        //public void LeaseProgress(int partitionId, string operation)
        //{
        //    SetCurrentThreadActivityId(serviceInstanceId);
        //    this.WriteEvent(93, partitionId, operation);
        //}
    }
}
