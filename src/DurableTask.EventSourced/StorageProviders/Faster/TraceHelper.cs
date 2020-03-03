using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;

namespace DurableTask.EventSourced.Faster
{
    class TraceHelper
    {
        private readonly ILogger logger;
        private readonly int partitionId;

        public TraceHelper(ILogger logger, int partitionId)
        {
            this.logger = logger;
            this.partitionId = partitionId;
        }

        // ----- faster storage provider events

        public void FasterCheckpointSaved(ulong commitPosition, ulong inputPosition, long elapsedMs)
        {
            logger.LogInformation("Part{partition:D2} Saved Checkpoint, commitPosition={commitPosition} inputPosition={inputPosition} elapsedMs={elapsedMs}", partitionId, commitPosition, inputPosition, elapsedMs);
            EtwSource.Log.FasterCheckpointSaved(partitionId, commitPosition, inputPosition, elapsedMs);
        }

        public void FasterLogPersisted(long commitPosition, long numBytes, long elapsedMs)
        {
            logger.LogDebug("Part{partition:D2} Persisted Log, commitPosition={commitPosition} numBytes={numBytes} elapsedMs={elapsedMs}", partitionId, commitPosition, numBytes, elapsedMs);
            EtwSource.Log.FasterLogPersisted(partitionId, commitPosition, numBytes, elapsedMs);
        }

        public void FasterStoreCreated(ulong inputPosition, long elapsedMs)
        {
            logger.LogInformation("Part{partition:D2} Created Store, inputPosition={inputPosition} elapsedMs={elapsedMs}", partitionId, inputPosition, elapsedMs);
            EtwSource.Log.FasterStoreCreated(partitionId, inputPosition, elapsedMs);
        }

        public void FasterCheckpointLoaded(ulong commitPosition, ulong inputPosition, long elapsedMs)
        {
            logger.LogInformation("Part{partition:D2} Loaded Checkpoint, commitPosition={commitPosition} inputPosition={inputPosition} elapsedMs={elapsedMs}", partitionId, commitPosition, inputPosition, elapsedMs);
            EtwSource.Log.FasterCheckpointLoaded(partitionId, commitPosition, inputPosition, elapsedMs);
        }

        public void FasterLogReplayed(ulong commitPosition, ulong inputPosition, long elapsedMs)
        {
            logger.LogInformation("Part{partition:D2} Replayed CommitLog, commitPosition={commitPosition} inputPosition={inputPosition} elapsedMs={elapsedMs}", partitionId, commitPosition, inputPosition, elapsedMs);
            EtwSource.Log.FasterLogReplayed(partitionId, commitPosition, inputPosition, elapsedMs);
        }

        public void FasterStorageError(string operation, Exception exception)
        {
            logger.LogError("Part{partition:D2} !!! Faster Storage Error : {operation} {exception}", partitionId, operation, exception);
            EtwSource.Log.FasterStorageError(partitionId, operation, exception.ToString());
        }

        public void FasterBlobStorageError(string operation, Exception exception)
        {
            logger.LogError(exception, "Part{partition:D2} !!! Faster Blob Storage error : {operation} : {exception}", partitionId, operation, exception);
            EtwSource.Log.FasterBlobStorageError(partitionId, operation, exception.ToString());
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
            logger.LogError("Part{partition:D2} lease lost in {operation}", partitionId, operation);
            EtwSource.Log.LeaseLost(partitionId, operation);
        }

        public void LeaseProgress(string operation)
        {
            logger.LogDebug("Part{partition:D2} lease progress: {operation}", partitionId, operation);
            EtwSource.Log.LeaseProgress(partitionId, operation);
        }
    }
}
