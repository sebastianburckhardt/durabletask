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

using DurableTask.Core;
using DurableTask.Core.History;
using Dynamitey;
using Dynamitey.DynamicObjects;
using FASTER.core;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DurableTask.EventSourced
{
    internal class EventTraceHelper
    {
        private readonly ILogger logger;
        private readonly string account;
        private readonly string taskHub;
        private readonly int partitionId;

        public EventTraceHelper(ILogger logger, Partition partition)
        {
            this.logger = logger;
            this.account = partition.StorageAccountName;
            this.taskHub = partition.Settings.TaskHubName;
            this.partitionId = (int)partition.PartitionId;
        }

        public bool IsTracingDetails => (this.logger.IsEnabled(LogLevel.Debug) || EtwSource.Log.IsVerboseEnabled);

        public void TraceEvent(long commitLogPosition, PartitionEvent evt, bool replaying)
        {
            if (this.logger.IsEnabled(LogLevel.Debug))
            {
                var details = string.Format($"{(replaying ? "Replaying" : "Processing")} {(evt.NextInputQueuePosition > 0 ? "external" : "internal")}{(evt.NextCommitLogPosition > 0 ? "" : " readonly")} event");
                this.logger.LogDebug("Part{partition:D2}.{commitLogPosition:D10} {details} {event} id={eventId} pos=({nextCommitLogPosition},{nextInputQueuePosition})", this.partitionId, commitLogPosition, details, evt, evt.EventIdString, evt.NextCommitLogPosition, evt.NextInputQueuePosition);
            }
            if (EtwSource.Log.IsVerboseEnabled)
            {
                EtwSource.Log.PartitionEventProcessed(this.account, this.taskHub, this.partitionId, commitLogPosition, evt.EventIdString, evt.ToString(), evt.NextCommitLogPosition, evt.NextInputQueuePosition, replaying, TraceUtils.ExtensionVersion);
            }
        }

        public void TraceEvent(long commitLogPosition, StorageAbstraction.IInternalReadonlyEvent evt)
        {
            if (this.logger.IsEnabled(LogLevel.Debug))
            {
                var details = string.Format($"Processing internal readonly event");
                this.logger.LogDebug("Part{partition:D2}.{commitLogPosition:D10} {details} {event} id={eventId}", this.partitionId, commitLogPosition, details, evt, evt.EventIdString);
            }
            if (EtwSource.Log.IsVerboseEnabled)
            {
                EtwSource.Log.PartitionEventProcessed(this.account, this.taskHub, this.partitionId, commitLogPosition, evt.EventIdString, evt.ToString(), 0L, 0L, false, TraceUtils.ExtensionVersion);
            }
        }

        // The trace context correlates the processing of an event with the effects of that event
        [ThreadStatic]
        private static (long commitLogPosition, string eventId) traceContext;

        internal const string RestartAfterRecoveryEventId = "RestartAfterRecovery"; // a pseudo eventId used as the context during restart

        private static readonly TraceContextClear traceContextClear = new TraceContextClear();

        public static IDisposable TraceContext(long commitLogPosition, string eventId)
        {
            EventTraceHelper.traceContext = (commitLogPosition, eventId);
            return traceContextClear;
        }

        private class TraceContextClear : IDisposable
        {
            public void Dispose()
            {
                EventTraceHelper.traceContext = (0L, null);
            }
        }

        public static void ClearTraceContext()
        {
            EventTraceHelper.traceContext = (0L, null);
        }

        public void TraceSend(Event evt)
        {
            (long commitLogPosition, string eventId) = EventTraceHelper.traceContext;

            if (this.logger.IsEnabled(LogLevel.Debug))
            {
                string prefix = commitLogPosition > 0 ? $".{commitLogPosition:D10}   " : "";
                this.logger.LogDebug("Part{partition:D2}{prefix} Sending event {eventId} {event}", this.partitionId, prefix, evt.EventIdString, evt);
            }
            if (EtwSource.Log.IsVerboseEnabled)
            {
                EtwSource.Log.PartitionEventSent(this.account, this.taskHub, this.partitionId, commitLogPosition, eventId ?? "", evt.EventIdString, evt.ToString(), TraceUtils.ExtensionVersion);
            }
        }

        public void TraceDetail(string details)
        {
            (long commitLogPosition, string eventId) = EventTraceHelper.traceContext;

            if (this.logger.IsEnabled(LogLevel.Debug))
            {
                string prefix = commitLogPosition > 0 ? $".{commitLogPosition:D10}   " : "";
                this.logger.LogDebug("Part{partition:D2}{prefix} {details}", this.partitionId, prefix, details);
            }
            if (EtwSource.Log.IsVerboseEnabled)
            {
                EtwSource.Log.PartitionEventDetail(this.account, this.taskHub, this.partitionId, commitLogPosition, eventId ?? "", details, TraceUtils.ExtensionVersion);
            }
        }

        public void TracePartitionOffloadDecision(int reportedLocalLoad, int pending, int backlog, int remotes, string reportedRemotes)
        {
            (long commitLogPosition, string eventId) = EventTraceHelper.traceContext;

            string prefix = commitLogPosition > 0 ? $".{commitLogPosition:D10}   " : "";
            this.logger.LogInformation("Part{partition:D2}{prefix} Offload decision reportedLocalLoad={reportedLocalLoad} pending={pending} backlog={backlog} remotes={remotes} reportedRemotes={reportedRemotes}",
                this.partitionId, prefix, reportedLocalLoad, pending, backlog, remotes, reportedRemotes);

            EtwSource.Log.PartitionOffloadDecision(this.account, this.taskHub, this.partitionId, commitLogPosition, eventId, reportedLocalLoad, pending, backlog, remotes, reportedRemotes, TraceUtils.ExtensionVersion);
        }

        public void TraceTaskMessageReceived(TaskMessage message, string sessionPosition)
        {
            (long commitLogPosition, string eventId) = EventTraceHelper.traceContext;

            if (eventId != RestartAfterRecoveryEventId)
            {
                if (this.logger.IsEnabled(LogLevel.Debug))
                {
                    string prefix = commitLogPosition > 0 ? $".{commitLogPosition:D10}   " : "";
                    this.logger.LogDebug("Part{partition:D2}{prefix} received TaskMessage eventType={eventType} taskEventId={taskEventId} instanceId={instanceId} executionId={executionId} messageId={messageId} sessionPosition={SessionPosition}",
                        this.partitionId, prefix, message.Event.EventType.ToString(), TraceUtils.GetTaskEventId(message.Event), message.OrchestrationInstance.InstanceId, message.OrchestrationInstance.ExecutionId, eventId, sessionPosition);
                }

                EtwSource.Log.TaskMessageReceived(this.account, this.taskHub, this.partitionId, commitLogPosition, message.Event.EventType.ToString(), TraceUtils.GetTaskEventId(message.Event), message.OrchestrationInstance.InstanceId, message.OrchestrationInstance.ExecutionId ?? "", eventId, sessionPosition, TraceUtils.ExtensionVersion);
            }
        }

        public void TraceTaskMessageSent(TaskMessage message, string sentEventId)
        {
            (long commitLogPosition, string eventId) = EventTraceHelper.traceContext;

            if (this.logger.IsEnabled(LogLevel.Debug))
            {
                string prefix = commitLogPosition > 0 ? $".{commitLogPosition:D10}   " : "";
                this.logger.LogDebug("Part{partition:D2}{prefix} sent TaskMessage eventType={eventType} taskEventId={taskEventId} instanceId={instanceId} executionId={executionId} messageId={messageId}",
                    this.partitionId, prefix, message.Event.EventType.ToString(), TraceUtils.GetTaskEventId(message.Event), message.OrchestrationInstance.InstanceId, message.OrchestrationInstance.ExecutionId, sentEventId);
            }

            EtwSource.Log.TaskMessageSent(this.account, this.taskHub, this.partitionId, message.Event.EventType.ToString(), TraceUtils.GetTaskEventId(message.Event), message.OrchestrationInstance.InstanceId, message.OrchestrationInstance.ExecutionId ?? "", sentEventId, TraceUtils.ExtensionVersion);
        }

        public void TraceInstanceUpdate(string workItemId, string instanceId, string executionId, int totalEventCount, List<HistoryEvent> newEvents, int episode)
        {
            string eventNames = string.Empty;
            string eventType = string.Empty;
            int numNewEvents = 0;

            if (newEvents != null)
            {
                HistoryEvent orchestratorEvent = null;
                var eventNamesBuilder = new StringBuilder();
                numNewEvents = newEvents.Count;

                if (numNewEvents > 20)
                {
                    eventNamesBuilder.Append("...,");
                }

                for (int i = 0; i < numNewEvents; i++)
                {
                    var historyEvent = newEvents[i];
                    switch (historyEvent.EventType)
                    {
                        case EventType.ExecutionStarted:
                        case EventType.ExecutionCompleted:
                        case EventType.ExecutionTerminated:
                        case EventType.ContinueAsNew:
                            orchestratorEvent = historyEvent;
                            break;
                        default:
                            break;
                    }
                    if (i >= newEvents.Count - 20)
                    {
                        eventNamesBuilder.Append(historyEvent.EventType.ToString());
                        eventNamesBuilder.Append(",");
                    }
                }
                eventNames = eventNamesBuilder.ToString(0, eventNamesBuilder.Length - 1); // remove trailing comma
                if (orchestratorEvent != null)
                {
                    eventType = orchestratorEvent.ToString();
                }
            }

            (long commitLogPosition, string eventId) = EventTraceHelper.traceContext;

            if (this.logger.IsEnabled(LogLevel.Debug))
            {
                string prefix = commitLogPosition > 0 ? $".{commitLogPosition:D10}   " : "";
                this.logger.LogDebug("Part{partition:D2}{prefix} Updated history instanceId={instanceId} executionId={executionId} workItemId={workItemId} numNewEvents={numNewEvents} totalEventCount={totalEventCount} eventNames={eventNames} eventType={eventType} episode={episode}",
                    this.partitionId, prefix, instanceId, executionId, workItemId, numNewEvents, totalEventCount, eventNames, eventType, episode);
            }

            EtwSource.Log.TraceInstanceUpdate(this.account, this.taskHub, instanceId, executionId, workItemId, numNewEvents, totalEventCount, eventNames, eventType, episode, TraceUtils.ExtensionVersion);
        }

        public void TraceInstanceFetch(string workItemId, string instanceId, string executionId, bool history, bool state)
        { }

             public void FetchedInstanceHistory(
            string Account,
            string TaskHub,
            string InstanceId,
            string ExecutionId,
            int EventCount,
            int Episode,
            int RequestCount,
            long LatencyMs,
            string ETag,
            DateTime LastCheckpointTime,
            string ExtensionVersion)
        { }
        public void FetchedInstanceStatus(
           string Account,
           string TaskHub,
           string InstanceId,
           string ExecutionId,
           long LatencyMs,
           string ExtensionVersion)
        { }
    }
}