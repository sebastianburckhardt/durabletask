using Dynamitey;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;

namespace DurableTask.EventSourced.Faster
{
    class FasterTraceHelper
    {
        private readonly ILogger logger;
        private readonly LogLevel etwLogLevel;
        private readonly string account;
        private readonly string taskHub;
        private readonly int partitionId;

        public FasterTraceHelper(ILogger logger, LogLevel etwLogLevel, uint partitionId, string storageAccountName, string taskHubName)
        {
            this.logger = logger;
            this.etwLogLevel = etwLogLevel;
            this.account = storageAccountName;
            this.taskHub = taskHubName;
            this.partitionId = (int) partitionId;
        }

        private EtwSource etwLogTrace => (this.etwLogLevel <= LogLevel.Trace) ? EtwSource.Log : null;
        private EtwSource etwLogDebug => (this.etwLogLevel <= LogLevel.Debug) ? EtwSource.Log : null;
        private EtwSource etwLogInformation => (this.etwLogLevel <= LogLevel.Information) ? EtwSource.Log : null;
        private EtwSource etwLogWarning => (this.etwLogLevel <= LogLevel.Warning) ? EtwSource.Log : null;
        private EtwSource etwLogError => (this.etwLogLevel <= LogLevel.Error) ? EtwSource.Log : null;

        // ----- faster storage provider events

        public void FasterStoreCreated(long inputQueuePosition, long latencyMs)
        {
            logger.LogInformation("Part{partition:D2} Created Store, inputQueuePosition={inputQueuePosition} latencyMs={latencyMs}", partitionId, inputQueuePosition, latencyMs);
            etwLogInformation?.FasterStoreCreated(this.account, this.taskHub, partitionId, inputQueuePosition, latencyMs, TraceUtils.ExtensionVersion);
        }

        public void FasterCheckpointStarted(Guid checkpointId, string reason, long commitLogPosition, long inputQueuePosition)
        {
            logger.LogInformation("Part{partition:D2} Started Checkpoint {checkpointId}, reason={reason}, commitLogPosition={commitLogPosition} inputQueuePosition={inputQueuePosition}", partitionId, checkpointId, reason, commitLogPosition, inputQueuePosition);
            etwLogInformation?.FasterCheckpointStarted(this.account, this.taskHub, partitionId, checkpointId, reason, commitLogPosition, inputQueuePosition, TraceUtils.ExtensionVersion);
        }

        public void FasterCheckpointPersisted(Guid checkpointId, string reason, long commitLogPosition, long inputQueuePosition, long latencyMs)
        {
            logger.LogInformation("Part{partition:D2} Persisted Checkpoint {checkpointId}, reason={reason}, commitLogPosition={commitLogPosition} inputQueuePosition={inputQueuePosition} latencyMs={latencyMs}", partitionId, checkpointId, reason, commitLogPosition, inputQueuePosition, latencyMs);
            etwLogInformation?.FasterCheckpointPersisted(this.account, this.taskHub, partitionId, checkpointId, reason, commitLogPosition, inputQueuePosition, latencyMs, TraceUtils.ExtensionVersion);
        }

        public void FasterLogPersisted(long commitLogPosition, long numberEvents, long sizeInBytes, long latencyMs)
        {
            logger.LogTrace("Part{partition:D2} Persisted Log, commitLogPosition={commitLogPosition} numberEvents={numberEvents} sizeInBytes={sizeInBytes} latencyMs={latencyMs}", partitionId, commitLogPosition, numberEvents, sizeInBytes, latencyMs);
            etwLogTrace?.FasterLogPersisted(this.account, this.taskHub, partitionId, commitLogPosition, numberEvents, sizeInBytes, latencyMs, TraceUtils.ExtensionVersion);
        }

         public void FasterCheckpointLoaded(long commitLogPosition, long inputQueuePosition, long latencyMs)
        {
            logger.LogInformation("Part{partition:D2} Loaded Checkpoint, commitLogPosition={commitLogPosition} inputQueuePosition={inputQueuePosition} latencyMs={latencyMs}", partitionId, commitLogPosition, inputQueuePosition, latencyMs);
            etwLogInformation?.FasterCheckpointLoaded(this.account, this.taskHub, partitionId, commitLogPosition, inputQueuePosition, latencyMs, TraceUtils.ExtensionVersion);
        }

        public void FasterLogReplayed(long commitLogPosition, long inputQueuePosition, long numberEvents, long sizeInBytes, long latencyMs)
        {
            logger.LogInformation("Part{partition:D2} Replayed CommitLog, commitLogPosition={commitLogPosition} inputQueuePosition={inputQueuePosition} numberEvents={numberEvents} sizeInBytes={sizeInBytes} latencyMs={latencyMs}", partitionId, commitLogPosition, inputQueuePosition, numberEvents, sizeInBytes, latencyMs);
            etwLogInformation?.FasterLogReplayed(this.account, this.taskHub, partitionId, commitLogPosition, inputQueuePosition, numberEvents, sizeInBytes, latencyMs, TraceUtils.ExtensionVersion);
        }

        public void FasterStorageError(string context, Exception exception)
        {
            logger.LogError("Part{partition:D2} !!! Faster Storage Error : {context} : {exception}", partitionId, context, exception);
            etwLogError?.FasterStorageError(this.account, this.taskHub, partitionId, context, exception.ToString(), TraceUtils.ExtensionVersion);
        }

        public void FasterBlobStorageError(string context, CloudBlob blob, Exception exception)
        {
            logger.LogError(exception, "Part{partition:D2} !!! Faster Blob Storage error : {context} blobName={blobName} {exception}", partitionId, context, blob?.Name, exception);
            etwLogError?.FasterBlobStorageError(this.account, this.taskHub, partitionId, context, blob?.Name ?? string.Empty, exception?.ToString() ?? string.Empty, TraceUtils.ExtensionVersion);
        }

        public void FasterBlobStorageWarning(string context, CloudBlob blob, Exception exception)
        {
            logger.LogError(exception, "Part{partition:D2} !!! Faster Blob Storage error : {context} blobName={blobName} {exception}", partitionId, context, blob?.Name, exception);
            etwLogError?.FasterBlobStorageWarning(this.account, this.taskHub, partitionId, context, blob?.Name ?? string.Empty, exception?.ToString() ?? string.Empty, TraceUtils.ExtensionVersion);
        }

        public void FasterProgress(string details)
        {
            logger.LogDebug("Part{partition:D2} {details}", partitionId, details);
            etwLogDebug?.FasterProgress(this.account, this.taskHub, partitionId, details, TraceUtils.ExtensionVersion);
        }

        // ----- lease management events

        public void LeaseAcquired()
        {
            logger.LogInformation("Part{partition:D2} Acquired lease", partitionId);
            etwLogInformation?.FasterLeaseAcquired(this.account, this.taskHub, partitionId, TraceUtils.ExtensionVersion);
        }

        public void LeaseReleased()
        {
            logger.LogInformation("Part{partition:D2} Released lease", partitionId);
            etwLogInformation?.FasterLeaseReleased(this.account, this.taskHub, partitionId, TraceUtils.ExtensionVersion);
        }

        public void LeaseLost(string operation)
        {
            logger.LogWarning("Part{partition:D2} Lease lost in {operation}", partitionId, operation);
            etwLogWarning?.FasterLeaseLost(this.account, this.taskHub, partitionId, operation, TraceUtils.ExtensionVersion);
        }

        public void LeaseProgress(string operation)
        {
            logger.LogDebug("Part{partition:D2} Lease progress: {operation}", partitionId, operation);
            etwLogDebug?.FasterLeaseProgress(this.account, this.taskHub, partitionId, operation, TraceUtils.ExtensionVersion);
        }
    }
}
