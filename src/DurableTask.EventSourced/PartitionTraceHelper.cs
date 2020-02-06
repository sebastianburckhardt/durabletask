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
using System.Diagnostics;

namespace DurableTask.EventSourced
{
    internal partial class Partition : TransportAbstraction.IPartition
    {
        public void ReportError(string where, Exception e)
        {
            if (EtwSource.EmitDiagnosticsTrace)
            {
                System.Diagnostics.Trace.TraceError($"Part{this.PartitionId:D2} !!! Exception in {where}: {e}");
            }
            if (EtwSource.EmitEtwTrace)
            {
                EtwSource.Log.PartitionErrorReported((int)this.PartitionId, where, e.GetType().Name, e.Message);
            }
        }

        public void TraceProcess(PartitionEvent evt)
        {
            Partition.TraceContext = $"{evt.CommitLogPosition:D10}   ";

            if (EtwSource.EmitDiagnosticsTrace)
            {
                System.Diagnostics.Trace.TraceInformation($"Part{this.PartitionId:D2}.{evt.CommitLogPosition:D10} Processing {evt} {evt.WorkItem}");
            }
            if (EtwSource.Log.IsVerboseEnabled)
            {
                EtwSource.Log.PartitionEventReceived((int)this.PartitionId, Partition.TraceContext ?? "", evt.WorkItem, evt.ToString());
            }
        }

        public void TraceSend(Event evt)
        {
            if (EtwSource.EmitDiagnosticsTrace)
            {
                this.DiagnosticsTrace($"Sending {evt} {evt.WorkItem}");
            }
            if (EtwSource.Log.IsVerboseEnabled)
            {
                EtwSource.Log.PartitionEventSent((int)this.PartitionId, Partition.TraceContext ?? "", evt.WorkItem, evt.ToString());
            }
        }

        public void TraceSubmit(Event evt)
        {
            if (EtwSource.EmitDiagnosticsTrace)
            {
                this.DiagnosticsTrace($"Submitting {evt} {evt.CommitLogPosition:D10} {evt.WorkItem}");
            }
            if (EtwSource.Log.IsVerboseEnabled)
            {
                EtwSource.Log.PartitionEventSent((int)this.PartitionId, Partition.TraceContext ?? "", evt.WorkItem, evt.ToString());
            }
        }

        public void DiagnosticsTrace(string msg)
        {
            var context = Partition.TraceContext;
            if (string.IsNullOrEmpty(context))
            {
                System.Diagnostics.Trace.TraceInformation($"Part{this.PartitionId:D2} {msg}");
            }
            else
            {
                System.Diagnostics.Trace.TraceInformation($"Part{this.PartitionId:D2}.{context} {msg}");
            }
        }

        [Conditional("DEBUG")]
        public void Assert(bool condition)
        {
            if (!condition)
            {
                var stacktrace = System.Environment.StackTrace;

                if (EtwSource.EmitDiagnosticsTrace)
                {
                    System.Diagnostics.Trace.TraceError($"Part{this.PartitionId:D2} !!! Assertion failed {stacktrace}");
                }

                System.Diagnostics.Debugger.Break();
            }
        }

    }
}
