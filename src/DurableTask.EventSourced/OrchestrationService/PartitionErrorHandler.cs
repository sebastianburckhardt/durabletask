﻿//  ----------------------------------------------------------------------------------
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
using System.Threading;
using Microsoft.Extensions.Logging;

namespace DurableTask.EventSourced
{
    // For indicating and initiating termination, and for tracing errors and warnings relating to a partition.
    // Is is basically a wrapper around CancellationTokenSource with features for diagnostics.
    internal class PartitionErrorHandler : IPartitionErrorHandler
    {
        private readonly CancellationTokenSource cts = new CancellationTokenSource();
        private readonly int partitionId;
        private readonly ILogger logger;
        private readonly LogLevel logLevelLimit;
        private readonly string account;
        private readonly string taskHub;

        public CancellationToken Token => cts.Token;
        public bool IsTerminated => cts.Token.IsCancellationRequested;

        public PartitionErrorHandler(int partitionId, ILogger logger, LogLevel logLevelLimit, string storageAccountName, string taskHubName)
        {
            this.cts = new CancellationTokenSource();
            this.partitionId = partitionId;
            this.logger = logger;
            this.logLevelLimit = logLevelLimit;
            this.account = storageAccountName;
            this.taskHub = taskHubName;
        }

        public void HandleError(string context, string message, Exception exception, bool terminatePartition, bool isWarning)
        {
            this.TraceError(isWarning, context, message, exception, terminatePartition);

            // terminate this partition in response to the error
            if (terminatePartition && !cts.IsCancellationRequested)
            {
                this.Terminate();
            }
        }

        private void TraceError(bool isWarning, string context, string message, Exception exception, bool terminatePartition)
        {
            var logLevel = isWarning ? LogLevel.Warning : LogLevel.Error;
            if (this.logLevelLimit <= logLevel)
            {
                this.logger?.Log(logLevel, "Part{partition:D2} !!! {message} in {context}: {exception} terminatePartition={terminatePartition}", this.partitionId, message, context, exception, terminatePartition);

                if (isWarning)
                {
                    EtwSource.Log.PartitionWarning(this.account, this.taskHub, this.partitionId, context, terminatePartition, message, exception?.ToString() ?? string.Empty, TraceUtils.ExtensionVersion);
                }
                else
                {
                    EtwSource.Log.PartitionError(this.account, this.taskHub, this.partitionId, context, terminatePartition, message, exception?.ToString() ?? string.Empty, TraceUtils.ExtensionVersion);
                }
            }
        }

        public void TerminateNormally()
        {
            this.Terminate();
        }

        private void Terminate()
        {
            try
            {
                this.cts.Cancel();
            }
            catch (AggregateException aggregate)
            {
                foreach (var e in aggregate.InnerExceptions)
                {
                    this.HandleError("PartitionErrorHandler.Terminate", "Encountered exeption while canceling token", e, false, true);
                }
            }
            catch (Exception e)
            {
                this.HandleError("PartitionErrorHandler.Terminate", "Encountered exeption while canceling token", e, false, true);
            }
        }

    }
}
