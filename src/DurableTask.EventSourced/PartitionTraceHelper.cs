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

        private string GetTracePrefix()
        {
            return $"Part{this.PartitionId:D2}";
        }

        public void ReportError(string context, Exception exception)
        {
            if (this.logger.IsEnabled(LogLevel.Error))
            {
                this.logger.LogError("{partition} !!! Exception in {context}: {exception}", this.TracePrefix, context, exception);
            }
            if (EtwSource.Log.IsEnabled())
            {
                EtwSource.Log.PartitionErrorReported((int)this.PartitionId, context, exception.GetType().Name, exception.Message);
            }
        }

        public void TraceProcess(PartitionEvent evt)
        {
            if (this.logger.IsEnabled(LogLevel.Debug))
            {
                var context = evt.CommitLogPosition.HasValue ? $".{evt.CommitLogPosition.Value:D10}" : "";
                if (evt.InputQueuePosition.HasValue)
                {
                    this.logger.LogDebug("{partition}{context} Processing external event {event} {inputQueuePosition} {workItem}", this.TracePrefix, context, evt, evt.InputQueuePosition, evt.WorkItem);
                }
                else
                {
                    this.logger.LogDebug("{partition}{context} Processing internal event {event} {workItem}", this.TracePrefix, context, evt, evt.WorkItem);
                }

                // the events following this will be processed with the same prefix and additional indentation
                Partition.TraceContext = (evt.CommitLogPosition, $"{context}   ");
            }
            if (EtwSource.Log.IsVerboseEnabled)
            {
                EtwSource.Log.PartitionEventReceived((int)this.PartitionId, evt.CommitLogPosition ?? 0UL, evt.InputQueuePosition ?? 0UL, evt.WorkItem, evt.ToString());
            }

        }

        public void TraceSend(Event evt)
        {
            if (this.logger.IsEnabled(LogLevel.Debug))
            {
                this.logger.LogDebug("{partition}{context} Sending {event} {workItem}", this.TracePrefix, Partition.TraceContext.Item2, evt, evt.WorkItem);
            }
            if (EtwSource.Log.IsVerboseEnabled)
            {
                EtwSource.Log.PartitionEventSent((int)this.PartitionId, Partition.TraceContext.Item1 ?? 0, evt.WorkItem, evt.ToString());
            }
        }

        public Partition DetailTracer => (this.logger.IsEnabled(LogLevel.Debug)) ? this : null;

        public void TraceDetail(string message)
        {
            this.logger.LogDebug("{partition}{context} {message}", this.TracePrefix, Partition.TraceContext.Item2, message);

            EtwSource.Log.PartitionDetail((int)this.PartitionId, Partition.TraceContext.Item1 ?? 0, message);
        }

        public static void ClearTraceContext()
        {
            TraceContext = (null, string.Empty);
        }

        [Conditional("DEBUG")]
        public void Assert(bool condition)
        {
            if (!condition)
            {
                if (System.Diagnostics.Debugger.IsAttached)
                {
                    System.Diagnostics.Debugger.Break();
                }

                var stacktrace = System.Environment.StackTrace;

                if (this.logger.IsEnabled(LogLevel.Error))
                {
                    this.logger.LogError("{partition} !!! Assertion failed {stacktrace}", this.TracePrefix, stacktrace);
                }
            }
        }
    }
}
