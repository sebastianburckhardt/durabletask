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
using Microsoft.Extensions.Logging;

namespace DurableTask.EventSourced.Faster
{
    class FasterTraceHelper
    {
        private readonly ILogger logger;
        private readonly LogLevel logLevelLimit;
        private readonly string account;
        private readonly string taskHub;
        private readonly int partitionId;

        public FasterTraceHelper(ILogger logger, LogLevel logLevelLimit, uint partitionId, string storageAccountName, string taskHubName)
        {
            this.logger = logger;
            this.logLevelLimit = logLevelLimit;
            this.account = storageAccountName;
            this.taskHub = taskHubName;
            this.partitionId = (int) partitionId;
        }

        private EtwSource etwLogTrace => (this.logLevelLimit <= LogLevel.Trace) ? EtwSource.Log : null;
        private EtwSource etwLogDebug => (this.logLevelLimit <= LogLevel.Debug) ? EtwSource.Log : null;
        private EtwSource etwLogInformation => (this.logLevelLimit <= LogLevel.Information) ? EtwSource.Log : null;
        private EtwSource etwLogWarning => (this.logLevelLimit <= LogLevel.Warning) ? EtwSource.Log : null;
        private EtwSource etwLogError => (this.logLevelLimit <= LogLevel.Error) ? EtwSource.Log : null;

        public bool IsTracingAtMostDetailedLevel => this.logLevelLimit == LogLevel.Trace;

        // ----- faster storage provider events

        public void FasterStoreCreated(long inputQueuePosition, long latencyMs)
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                logger.LogInformation("Part{partition:D2} Created Store, inputQueuePosition={inputQueuePosition} latencyMs={latencyMs}", partitionId, inputQueuePosition, latencyMs);
                this.etwLogInformation?.FasterStoreCreated(this.account, this.taskHub, partitionId, inputQueuePosition, latencyMs, TraceUtils.ExtensionVersion);
            }
        }
        public void FasterCheckpointStarted(Guid checkpointId, string reason, string storeStats, long commitLogPosition, long inputQueuePosition)
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                logger.LogInformation("Part{partition:D2} Started Checkpoint {checkpointId}, reason={reason}, storeStats={storeStats}, commitLogPosition={commitLogPosition} inputQueuePosition={inputQueuePosition}", partitionId, checkpointId, reason, storeStats, commitLogPosition, inputQueuePosition);
                this.etwLogInformation?.FasterCheckpointStarted(this.account, this.taskHub, partitionId, checkpointId, reason, storeStats, commitLogPosition, inputQueuePosition, TraceUtils.ExtensionVersion);
            }
        }

        public void FasterCheckpointPersisted(Guid checkpointId, string reason, long commitLogPosition, long inputQueuePosition, long latencyMs)
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                logger.LogInformation("Part{partition:D2} Persisted Checkpoint {checkpointId}, reason={reason}, commitLogPosition={commitLogPosition} inputQueuePosition={inputQueuePosition} latencyMs={latencyMs}", partitionId, checkpointId, reason, commitLogPosition, inputQueuePosition, latencyMs);
                this.etwLogInformation?.FasterCheckpointPersisted(this.account, this.taskHub, partitionId, checkpointId, reason, commitLogPosition, inputQueuePosition, latencyMs, TraceUtils.ExtensionVersion);
            }

            if (latencyMs > 10000)
            {
                this.FasterPerfWarning($"Persisting the checkpoint {checkpointId} took {(double)latencyMs / 1000}s, which is excessive; checkpointId={checkpointId} commitLogPosition={commitLogPosition} inputQueuePosition={inputQueuePosition}");
            }
        }

        public void FasterLogPersisted(long commitLogPosition, long numberEvents, long sizeInBytes, long latencyMs)
        {
            if (this.logLevelLimit <= LogLevel.Debug)
            {
                logger.LogDebug("Part{partition:D2} Persisted Log, commitLogPosition={commitLogPosition} numberEvents={numberEvents} sizeInBytes={sizeInBytes} latencyMs={latencyMs}", partitionId, commitLogPosition, numberEvents, sizeInBytes, latencyMs);
                this.etwLogDebug?.FasterLogPersisted(this.account, this.taskHub, partitionId, commitLogPosition, numberEvents, sizeInBytes, latencyMs, TraceUtils.ExtensionVersion);
            }

            if (latencyMs > 10000)
            {
                this.FasterPerfWarning($"Persisting the log took {(double)latencyMs / 1000}s, which is excessive; commitLogPosition={commitLogPosition} numberEvents={numberEvents} sizeInBytes={sizeInBytes}");
            }
        }

        public void FasterPerfWarning(string details)
        {
            if (this.logLevelLimit <= LogLevel.Warning)
            {
                logger.LogWarning("Part{partition:D2} Performance issue detected: {details}", partitionId, details);
                this.etwLogDebug?.FasterPerfWarning(this.account, this.taskHub, partitionId, details, TraceUtils.ExtensionVersion);
            }
        }

        public void FasterCheckpointLoaded(long commitLogPosition, long inputQueuePosition, string storeStats, long latencyMs)
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                logger.LogInformation("Part{partition:D2} Loaded Checkpoint, commitLogPosition={commitLogPosition} inputQueuePosition={inputQueuePosition}  storeStats={storeStats} latencyMs={latencyMs}", partitionId, commitLogPosition, inputQueuePosition, storeStats, latencyMs);
                this.etwLogInformation?.FasterCheckpointLoaded(this.account, this.taskHub, partitionId, commitLogPosition, inputQueuePosition, storeStats, latencyMs, TraceUtils.ExtensionVersion);
            }
        }

        public void FasterLogReplayed(long commitLogPosition, long inputQueuePosition, long numberEvents, long sizeInBytes, string storeStats, long latencyMs)
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                logger.LogInformation("Part{partition:D2} Replayed CommitLog, commitLogPosition={commitLogPosition} inputQueuePosition={inputQueuePosition} numberEvents={numberEvents} sizeInBytes={sizeInBytes} storeStats={storeStats} latencyMs={latencyMs}", partitionId, commitLogPosition, inputQueuePosition, numberEvents, sizeInBytes, storeStats, latencyMs);
                this.etwLogInformation?.FasterLogReplayed(this.account, this.taskHub, partitionId, commitLogPosition, inputQueuePosition, numberEvents, sizeInBytes, storeStats, latencyMs, TraceUtils.ExtensionVersion);
            }
        }

        public void FasterStorageError(string context, Exception exception)
        {
            if (this.logLevelLimit <= LogLevel.Error)
            {
                logger.LogError("Part{partition:D2} !!! Faster Storage Error : {context} : {exception}", partitionId, context, exception);
                this.etwLogError?.FasterStorageError(this.account, this.taskHub, partitionId, context, exception.ToString(), TraceUtils.ExtensionVersion);
            }
        }

        public void FasterBlobStorageWarning(string context, string blobName, Exception exception)
        {
            if (this.logLevelLimit <= LogLevel.Error)
            {
                logger.LogError(exception, "Part{partition:D2} !!! Faster Blob Storage error : {context} blobName={blobName} {exception}", partitionId, context, blobName, exception);
                this.etwLogError?.FasterBlobStorageWarning(this.account, this.taskHub, partitionId, context, blobName ?? string.Empty, exception?.ToString() ?? string.Empty, TraceUtils.ExtensionVersion);
            }
        }

        public void FasterBlobStorageError(string context, string blobName, Exception exception)
        {
            if (this.logLevelLimit <= LogLevel.Error)
            {
                logger.LogError(exception, "Part{partition:D2} !!! Faster Blob Storage error : {context} blobName={blobName} {exception}", partitionId, context, blobName, exception);
                this.etwLogError?.FasterBlobStorageError(this.account, this.taskHub, partitionId, context, blobName ?? string.Empty, exception?.ToString() ?? string.Empty, TraceUtils.ExtensionVersion);
            }
        }

        public void FasterProgress(string details)
        {
            if (this.logLevelLimit <= LogLevel.Debug)
            {
                logger.LogDebug("Part{partition:D2} {details}", partitionId, details);
                this.etwLogDebug?.FasterProgress(this.account, this.taskHub, partitionId, details, TraceUtils.ExtensionVersion);
            }
        }

        public void FasterStorageProgress(string details)
        {
            if (this.logLevelLimit <= LogLevel.Trace)
            {
                logger.LogTrace("Part{partition:D2} {details}", partitionId, details);
                this.etwLogTrace?.FasterStorageProgress(this.account, this.taskHub, partitionId, details, TraceUtils.ExtensionVersion);
            }
        }

        // ----- lease management events

        public void LeaseAcquired()
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                logger.LogInformation("Part{partition:D2} Acquired lease", partitionId);
                this.etwLogInformation?.FasterLeaseAcquired(this.account, this.taskHub, partitionId, TraceUtils.ExtensionVersion);
            }
        }

        public void LeaseReleased()
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                logger.LogInformation("Part{partition:D2} Released lease", partitionId);
                this.etwLogInformation?.FasterLeaseReleased(this.account, this.taskHub, partitionId, TraceUtils.ExtensionVersion);
            }
        }

        public void LeaseLost(string operation)
        {
            if (this.logLevelLimit <= LogLevel.Warning)
            {
                logger.LogWarning("Part{partition:D2} Lease lost in {operation}", partitionId, operation);
                this.etwLogWarning?.FasterLeaseLost(this.account, this.taskHub, partitionId, operation, TraceUtils.ExtensionVersion);
            }
        }

        public void LeaseProgress(string operation)
        {
            if (this.logLevelLimit <= LogLevel.Debug)
            {
                logger.LogDebug("Part{partition:D2} Lease progress: {operation}", partitionId, operation);
                this.etwLogDebug?.FasterLeaseProgress(this.account, this.taskHub, partitionId, operation, TraceUtils.ExtensionVersion);
            }
        }
    }
}
