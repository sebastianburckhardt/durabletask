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
        private readonly string account;
        private readonly string taskHub;
        private readonly int partitionId;

        public FasterTraceHelper(ILogger logger, uint partitionId, string storageAccountName, string taskHubName)
        {
            this.logger = logger;
            this.account = storageAccountName;
            this.taskHub = taskHubName;
            this.partitionId = (int) partitionId;
        }

        // ----- faster storage provider events

        public void FasterStoreCreated(long inputQueuePosition, long latencyMs)
        {
            logger.LogInformation("Part{partition:D2} Created Store, inputQueuePosition={inputQueuePosition} latencyMs={latencyMs}", partitionId, inputQueuePosition, latencyMs);
            EtwSource.Log.FasterStoreCreated(this.account, this.taskHub, partitionId, inputQueuePosition, latencyMs, TraceUtils.ExtensionVersion);
        }

        public void FasterCheckpointStarted(Guid checkpointId, string reason, long commitLogPosition, long inputQueuePosition)
        {
            logger.LogInformation("Part{partition:D2} Started Checkpoint {checkpointId}, reason={reason}, commitLogPosition={commitLogPosition} inputQueuePosition={inputQueuePosition}", partitionId, checkpointId, reason, commitLogPosition, inputQueuePosition);
            EtwSource.Log.FasterCheckpointStarted(this.account, this.taskHub, partitionId, checkpointId, reason, commitLogPosition, inputQueuePosition, TraceUtils.ExtensionVersion);
        }

        public void FasterCheckpointPersisted(Guid checkpointId, string reason, long commitLogPosition, long inputQueuePosition, long latencyMs)
        {
            logger.LogInformation("Part{partition:D2} Persisted Checkpoint {checkpointId}, reason={reason}, commitLogPosition={commitLogPosition} inputQueuePosition={inputQueuePosition} latencyMs={latencyMs}", partitionId, checkpointId, reason, commitLogPosition, inputQueuePosition, latencyMs);
            EtwSource.Log.FasterCheckpointPersisted(this.account, this.taskHub, partitionId, checkpointId, reason, commitLogPosition, inputQueuePosition, latencyMs, TraceUtils.ExtensionVersion);
        }

        public void FasterLogPersisted(long commitLogPosition, long numberEvents, long sizeInBytes, long latencyMs)
        {
            logger.LogDebug("Part{partition:D2} Persisted Log, commitLogPosition={commitLogPosition} numberEvents={numberEvents} sizeInBytes={sizeInBytes} latencyMs={latencyMs}", partitionId, commitLogPosition, numberEvents, sizeInBytes, latencyMs);
            EtwSource.Log.FasterLogPersisted(this.account, this.taskHub, partitionId, commitLogPosition, numberEvents, sizeInBytes, latencyMs, TraceUtils.ExtensionVersion);
        }

         public void FasterCheckpointLoaded(long commitLogPosition, long inputQueuePosition, long latencyMs)
        {
            logger.LogInformation("Part{partition:D2} Loaded Checkpoint, commitLogPosition={commitLogPosition} inputQueuePosition={inputQueuePosition} latencyMs={latencyMs}", partitionId, commitLogPosition, inputQueuePosition, latencyMs);
            EtwSource.Log.FasterCheckpointLoaded(this.account, this.taskHub, partitionId, commitLogPosition, inputQueuePosition, latencyMs, TraceUtils.ExtensionVersion);
        }

        public void FasterLogReplayed(long commitLogPosition, long inputQueuePosition, long numberEvents, long sizeInBytes, long latencyMs)
        {
            logger.LogInformation("Part{partition:D2} Replayed CommitLog, commitLogPosition={commitLogPosition} inputQueuePosition={inputQueuePosition} numberEvents={numberEvents} sizeInBytes={sizeInBytes} latencyMs={latencyMs}", partitionId, commitLogPosition, inputQueuePosition, numberEvents, sizeInBytes, latencyMs);
            EtwSource.Log.FasterLogReplayed(this.account, this.taskHub, partitionId, commitLogPosition, inputQueuePosition, numberEvents, sizeInBytes, latencyMs, TraceUtils.ExtensionVersion);
        }

        public void FasterStorageError(string context, Exception exception)
        {
            logger.LogError("Part{partition:D2} !!! Faster Storage Error : {context} : {exception}", partitionId, context, exception);
            EtwSource.Log.FasterStorageError(this.account, this.taskHub, partitionId, context, exception.ToString(), TraceUtils.ExtensionVersion);
        }

        public void FasterBlobStorageError(string context, CloudBlob blob, Exception exception)
        {
            logger.LogError(exception, "Part{partition:D2} !!! Faster Blob Storage error : {context} blobName={blobName} {exception}", partitionId, context, blob?.Name, exception);
            EtwSource.Log.FasterBlobStorageError(this.account, this.taskHub, partitionId, context, blob?.Name ?? string.Empty, exception?.ToString() ?? string.Empty, TraceUtils.ExtensionVersion);
        }

        public void FasterBlobStorageWarning(string context, CloudBlob blob, Exception exception)
        {
            logger.LogError(exception, "Part{partition:D2} !!! Faster Blob Storage error : {context} blobName={blobName} {exception}", partitionId, context, blob?.Name, exception);
            EtwSource.Log.FasterBlobStorageWarning(this.account, this.taskHub, partitionId, context, blob?.Name ?? string.Empty, exception?.ToString() ?? string.Empty, TraceUtils.ExtensionVersion);
        }

        public void FasterProgress(string details)
        {
            logger.LogDebug("Part{partition:D2} {details}", partitionId, details);
            EtwSource.Log.FasterProgress(this.account, this.taskHub, partitionId, details, TraceUtils.ExtensionVersion);
        }

        // ----- lease management events

        public void LeaseAcquired()
        {
            logger.LogInformation("Part{partition:D2} acquired lease", partitionId);
            EtwSource.Log.LeaseAcquired(this.account, this.taskHub, partitionId, TraceUtils.ExtensionVersion);
        }

        public void LeaseReleased()
        {
            logger.LogInformation("Part{partition:D2} released lease", partitionId);
            EtwSource.Log.LeaseReleased(this.account, this.taskHub, partitionId, TraceUtils.ExtensionVersion);
        }

        public void LeaseLost(string operation)
        {
            logger.LogWarning("Part{partition:D2} lease lost in {operation}", partitionId, operation);
            EtwSource.Log.LeaseLost(this.account, this.taskHub, partitionId, operation, TraceUtils.ExtensionVersion);
        }

        public void LeaseProgress(string operation)
        {
            logger.LogDebug("Part{partition:D2} lease progress: {operation}", partitionId, operation);
            EtwSource.Log.LeaseProgress(this.account, this.taskHub, partitionId, operation, TraceUtils.ExtensionVersion);
        }
    }
}
