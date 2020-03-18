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

using Dynamitey;
using FASTER.core;
using Microsoft.Extensions.Logging;
using System;
using System.Diagnostics;

namespace DurableTask.EventSourced
{
    internal class EventTraceHelper
    {
        private readonly ILogger logger;
        private readonly Partition partition;
        private int partitionId;

        public EventTraceHelper(ILogger logger, Partition partition)
        {
            this.logger = logger;
            this.partition = partition;
            this.partitionId = (int) partition.PartitionId;
        }

        public bool IsTracingDetails => (this.logger.IsEnabled(LogLevel.Debug) || EtwSource.Log.IsVerboseEnabled);

        public void TraceEvent(ulong commitLogPosition, PartitionEvent evt, bool replaying)
        {
            if (this.logger.IsEnabled(LogLevel.Debug))
            {
                var details = string.Format($"{(replaying ? "Replaying" : "Processing")} {(evt.NextInputQueuePosition.HasValue ? "external" : "internal")}{(evt.NextCommitLogPosition.HasValue ? "" : " readonly")} event");
                this.logger.LogDebug("Part{partition:D2}.{commitLogPosition:D10} {details} {event} id={eventId} pos=({nextCommitLogPosition},{nextInputQueuePosition})", this.partitionId, commitLogPosition, details, evt, evt.EventIdString, evt.NextCommitLogPosition?.ToString() ?? "_", evt.NextInputQueuePosition?.ToString() ?? "_");
            }
            if (EtwSource.Log.IsVerboseEnabled)
            {
                EtwSource.Log.PartitionEventProcessed(this.partitionId, commitLogPosition, evt.EventIdString, evt.ToString(), evt.NextCommitLogPosition ?? 0UL, evt.NextInputQueuePosition ?? 0UL, replaying);
            }
        }

        public void TraceEvent(ulong commitLogPosition, StorageAbstraction.IInternalReadonlyEvent evt)
        {
            if (this.logger.IsEnabled(LogLevel.Debug))
            {
                var details = string.Format($"Processing internal readonly event");
                this.logger.LogDebug("Part{partition:D2}.{commitLogPosition:D10} {details} {event} id={eventId}", this.partitionId, commitLogPosition, details, evt, evt.EventIdString);
            }
            if (EtwSource.Log.IsVerboseEnabled)
            {
                EtwSource.Log.PartitionEventProcessed(this.partitionId, commitLogPosition, evt.EventIdString, evt.ToString(), 0UL, 0UL, false);
            }
        }

        // The trace context correlates the processing of an event with the effects of that event
        [ThreadStatic]
        private static (ulong?, string) traceContext;

        private static readonly TraceContextClear traceContextClear = new TraceContextClear();

        public static IDisposable TraceContext(ulong? commitLogPosition, string context)
        {
            EventTraceHelper.traceContext = (commitLogPosition, context);
            return traceContextClear;
        }

        private class TraceContextClear: IDisposable
        {
            public void Dispose()
            {
                EventTraceHelper.traceContext = (null, null);
            }
        }
 
        public static void ClearTraceContext()
        {
            EventTraceHelper.traceContext = (null, null);
        }

        public void TraceSend(Event evt)
        {
            (ulong? commitLogPosition, string context) = EventTraceHelper.traceContext;

            if (this.logger.IsEnabled(LogLevel.Debug))
            {
                string prefix = commitLogPosition.HasValue ? $".{commitLogPosition.Value:D10}   " : "";
                this.logger.LogDebug("Part{partition:D2}{prefix} Sending event {eventId} {event}", this.partitionId, prefix, evt.EventIdString, evt);
            }
            if (EtwSource.Log.IsVerboseEnabled)
            {
                EtwSource.Log.PartitionEventSent(this.partitionId, commitLogPosition ?? 0UL, context ?? "", evt.EventIdString, evt.ToString());
            }
        }

        public void TraceDetail(string details)
        {
            (ulong? commitLogPosition, string context) = EventTraceHelper.traceContext;

            if (this.logger.IsEnabled(LogLevel.Debug))
            {
                string prefix = commitLogPosition.HasValue ? $".{commitLogPosition.Value:D10}   " : "";
                this.logger.LogDebug("Part{partition:D2}{prefix} {details}", this.partitionId, prefix, details);
            }
            if (EtwSource.Log.IsVerboseEnabled)
            {
                EtwSource.Log.PartitionEventDetail(this.partitionId, commitLogPosition ?? 0UL, context ?? "", details);
            }
        }
    }
}
