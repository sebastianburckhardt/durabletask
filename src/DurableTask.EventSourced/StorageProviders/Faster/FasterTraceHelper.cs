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
        private readonly int partitionId;

        public FasterTraceHelper(ILogger logger, int partitionId)
        {
            this.logger = logger;
            this.partitionId = partitionId;
        }

        // ----- faster storage provider events

        public void FasterStoreCreated(ulong inputPosition, long elapsedMs)
        {
            logger.LogInformation("Part{partition:D2} Created Store, inputPosition={inputPosition} elapsedMs={elapsedMs}", partitionId, inputPosition, elapsedMs);
            EtwSource.Log.FasterStoreCreated(partitionId, inputPosition, elapsedMs);
        }

        public void FasterCheckpointStarted(Guid checkpointGuid, string reason, ulong commitPosition, ulong inputPosition)
        {
            logger.LogInformation("Part{partition:D2} Started Checkpoint {checkpointGuid}, reason={reason}, commitPosition={commitPosition} inputPosition={inputPosition}", partitionId, checkpointGuid, reason, commitPosition, inputPosition);
            EtwSource.Log.FasterCheckpointStarted(partitionId, checkpointGuid, reason, commitPosition, inputPosition);
        }

        public void FasterCheckpointPersisted(Guid checkpointGuid, string reason, ulong commitPosition, ulong inputPosition, long elapsedMs)
        {
            logger.LogInformation("Part{partition:D2} Persisted Checkpoint {checkpointGuid}, reason={reason}, commitPosition={commitPosition} inputPosition={inputPosition} elapsedMs={elapsedMs}", partitionId, checkpointGuid, reason, commitPosition, inputPosition, elapsedMs);
            EtwSource.Log.FasterCheckpointPersisted(partitionId, checkpointGuid, reason, commitPosition, inputPosition, elapsedMs);
        }

        public void FasterLogPersisted(ulong commitPosition, ulong numEvents, ulong numBytes, long elapsedMs)
        {
            logger.LogDebug("Part{partition:D2} Persisted Log, commitPosition={commitPosition} numEvents={numEvents} numBytes={numBytes} elapsedMs={elapsedMs}", partitionId, commitPosition, numEvents, numBytes, elapsedMs);
            EtwSource.Log.FasterLogPersisted(partitionId, commitPosition, numEvents, numBytes, elapsedMs);
        }

         public void FasterCheckpointLoaded(ulong commitPosition, ulong inputPosition, long elapsedMs)
        {
            logger.LogInformation("Part{partition:D2} Loaded Checkpoint, commitPosition={commitPosition} inputPosition={inputPosition} elapsedMs={elapsedMs}", partitionId, commitPosition, inputPosition, elapsedMs);
            EtwSource.Log.FasterCheckpointLoaded(partitionId, commitPosition, inputPosition, elapsedMs);
        }

        public void FasterLogReplayed(ulong commitPosition, ulong inputPosition, ulong numEvents, ulong numBytes, long elapsedMs)
        {
            logger.LogInformation("Part{partition:D2} Replayed CommitLog, commitPosition={commitPosition} inputPosition={inputPosition} numEvents={numEvents} numBytes={numBytes} elapsedMs={elapsedMs}", partitionId, commitPosition, inputPosition, numEvents, numBytes, elapsedMs);
            EtwSource.Log.FasterLogReplayed(partitionId, commitPosition, inputPosition, numEvents, numBytes, elapsedMs);
        }

        public void FasterStorageError(string operation, Exception exception)
        {
            logger.LogError("Part{partition:D2} !!! Faster Storage Error : {operation} : {exception}", partitionId, operation, exception);
            EtwSource.Log.FasterStorageError(partitionId, operation, exception.ToString());
        }

        public void FasterBlobStorageError(string operation, CloudBlob blob, Exception exception)
        {
            logger.LogError(exception, "Part{partition:D2} !!! Faster Blob Storage error : {operation} blobName={blobName} {exception}", partitionId, operation, blob?.Name, exception);
            EtwSource.Log.FasterBlobStorageError(partitionId, operation, blob?.Name ?? string.Empty, exception?.ToString() ?? string.Empty);
        }

        public void FasterBlobStorageWarning(string operation, CloudBlob blob, Exception exception)
        {
            logger.LogError(exception, "Part{partition:D2} !!! Faster Blob Storage error : {operation} blobName={blobName} {exception}", partitionId, operation, blob?.Name, exception);
            EtwSource.Log.FasterBlobStorageWarning(partitionId, operation, blob?.Name ?? string.Empty, exception?.ToString() ?? string.Empty);
        }

        public void FasterProgress(string operation)
        {
            logger.LogDebug("Part{partition:D2} {message}", partitionId, operation);
            EtwSource.Log.FasterProgress(partitionId, operation);
        }

        // ----- lease management events

        public void LeaseAcquired()
        {
            logger.LogInformation("Part{partition:D2} acquired lease", partitionId);
            EtwSource.Log.LeaseAcquired(partitionId);
        }

        public void LeaseReleased()
        {
            logger.LogInformation("Part{partition:D2} released lease", partitionId);
            EtwSource.Log.LeaseReleased(partitionId);
        }

        public void LeaseLost(string operation)
        {
            logger.LogWarning("Part{partition:D2} lease lost in {operation}", partitionId, operation);
            EtwSource.Log.LeaseLost(partitionId, operation);
        }

        public void LeaseProgress(string operation)
        {
            logger.LogDebug("Part{partition:D2} lease progress: {operation}", partitionId, operation);
            EtwSource.Log.LeaseProgress(partitionId, operation);
        }
    }
}
