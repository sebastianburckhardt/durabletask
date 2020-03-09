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
    internal partial class Partition : TransportAbstraction.IPartition
    {
        private readonly ILogger logger;

        [ThreadStatic]
        public static (ulong?,string) TraceContext = (null, string.Empty);

        public void TraceError(string context, Exception exception, bool isFatal)
        {
            if (this.logger.IsEnabled(LogLevel.Error))
            {
                this.logger.LogError("Part{partition:D2} !!! Error in {context}: {exception} isFatal={isFatal}", this.PartitionId, context, exception, isFatal);
            }
            if (EtwSource.Log.IsEnabled())
            {
                EtwSource.Log.PartitionErrorReported((int) this.PartitionId, context, isFatal, exception.Message, exception.ToString());
            }
        }

        public void TraceError(string context, string message, bool isFatal)
        {
            if (this.logger.IsEnabled(LogLevel.Error))
            {
                this.logger.LogError("Part{partition:D2} !!! Error in {context}: {message} isFatal={isFatal}", this.PartitionId, context, message, isFatal);
            }
            if (EtwSource.Log.IsEnabled())
            {
                EtwSource.Log.PartitionErrorReported((int)this.PartitionId, context, isFatal, message, string.Empty);
            }
        }

        public Partition DetailTracer => (this.logger.IsEnabled(LogLevel.Debug)) ? this : null;

        public static void ClearTraceContext()
        {
            TraceContext = (null, string.Empty);
        }

        public void TraceProcess(PartitionEvent evt, bool replaying)
        {
            if (this.logger.IsEnabled(LogLevel.Debug))
            {
                var context = evt.NextCommitLogPosition.HasValue ? $".{evt.NextCommitLogPosition.Value:D10}" : "";
                var verb = replaying ? "Replaying" : "Processing";
                if (evt.NextInputQueuePosition.HasValue)
                {
                    this.logger.LogDebug("Part{partition:D2}{context} {verb} external event {event} {inputQueuePosition} {workItem}", this.PartitionId, context, verb, evt, evt.NextInputQueuePosition, evt.WorkItem);
                }
                else
                {
                    this.logger.LogDebug("Part{partition:D2}{context} {verb} internal event {event} {workItem}", this.PartitionId, context, verb, evt, evt.WorkItem);
                }

                // the events following this will be processed with the same prefix and additional indentation
                Partition.TraceContext = (evt.NextCommitLogPosition, $"{context}   ");
            }
            if (EtwSource.Log.IsVerboseEnabled)
            {
                EtwSource.Log.PartitionEventReceived((int)this.PartitionId, evt.NextCommitLogPosition ?? 0UL, evt.NextInputQueuePosition ?? 0UL, replaying, evt.WorkItem, evt.ToString());
            }

        }

        public void TraceSend(Event evt)
        {
            if (this.logger.IsEnabled(LogLevel.Debug))
            {
                this.logger.LogDebug("Part{partition:D2}{context} Sending {event} {workItem}", this.PartitionId, Partition.TraceContext.Item2, evt, evt.WorkItem);
            }
            if (EtwSource.Log.IsVerboseEnabled)
            {
                EtwSource.Log.PartitionEventSent((int)this.PartitionId, Partition.TraceContext.Item1 ?? 0, evt.WorkItem, evt.ToString());
            }
        }

        public void TraceDetail(string message)
        {
            this.logger.LogDebug("Part{partition:D2}{context} {message}", this.PartitionId, Partition.TraceContext.Item2, message);

            EtwSource.Log.PartitionDetail((int)this.PartitionId, Partition.TraceContext.Item1 ?? 0, message);
        }
    }
}
