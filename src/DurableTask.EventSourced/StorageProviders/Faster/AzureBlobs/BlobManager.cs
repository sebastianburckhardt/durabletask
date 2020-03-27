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


using Dynamitey.DynamicObjects;
using FASTER.core;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.Storage.Blob.Protocol;
using Microsoft.Azure.Storage.RetryPolicies;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventSourced.Faster
{
    /// <summary>
    /// Provides management of blobs and blob names associated with a partition, and logic for partition lease maintenance and termination.
    /// </summary>
    internal class BlobManager : ICheckpointManager, ILogCommitManager
    {
        private readonly string containerName;
        private readonly uint partitionId;
        private readonly CancellationTokenSource shutDownOrTermination;
        private readonly CloudStorageAccount cloudStorageAccount;

        private CloudBlobContainer blobContainer;
        private CloudBlockBlob eventLogCommitBlob;
        private CloudBlobDirectory partitionDirectory;

        private string LeaseId;

        private TimeSpan LeaseDuration = TimeSpan.FromSeconds(45); // max time the lease stays after unclean shutdown
        private TimeSpan LeaseRenewal = TimeSpan.FromSeconds(30); // how often we renew the lease
        private TimeSpan LeaseSafetyBuffer = TimeSpan.FromSeconds(10); // how much time we want left on the lease before issuing a protected access

        internal FasterTraceHelper TraceHelper { get; private set; }

        internal bool UseLocalFilesForTestingAndDebugging { get; private set; }

        public IDevice EventLogDevice { get; private set; }
        public IDevice HybridLogDevice { get; private set; }
        public IDevice ObjectLogDevice { get; private set; }

        public IPartitionErrorHandler PartitionErrorHandler { get; private set; }

        public long CheckpointCommitLogPosition { get; set; }
        public long CheckpointInputQueuePosition { get; set; }

        private volatile System.Diagnostics.Stopwatch leaseTimer;

        public FasterLogSettings EventLogSettings => new FasterLogSettings
        {
            LogDevice = this.EventLogDevice,
            LogCommitManager = this.UseLocalFilesForTestingAndDebugging ?
                new LocalLogCommitManager($"{this.LocalDirectoryPath}\\{this.PartitionFolder}\\{CommitBlobName}") : (ILogCommitManager)this,
            PageSizeBits = 18, // 128k since we are just writing and often small portions
            SegmentSizeBits = 28,
            MemorySizeBits = 22, // 2MB because 16 pages are the minimum
        };

        public LogSettings StoreLogSettings => new LogSettings
        {
            LogDevice = this.HybridLogDevice,
            ObjectLogDevice = this.ObjectLogDevice,
            PageSizeBits = 22, // 4MB since page blobs can't write more than that in a single op
            MutableFraction = 0.9,
            SegmentSizeBits = 28, // 256 MB
            CopyReadsToTail = true,
            MemorySizeBits = 27, // 128 MB
        };

        public CheckpointSettings StoreCheckpointSettings => new CheckpointSettings
        {
            CheckpointManager = this.UseLocalFilesForTestingAndDebugging ?
                new LocalCheckpointManager($"{LocalDirectoryPath}\\chkpts{this.partitionId:D2}") : (ICheckpointManager)this,
            CheckPointType = CheckpointType.FoldOver,
        };

        public BlobRequestOptions BlobRequestOptionsUnderLease => new BlobRequestOptions()
        {
            RetryPolicy = new LinearRetry(TimeSpan.FromSeconds(2), 2),
        };

        public BlobRequestOptions BlobRequestOptionsNotUnderLease => new BlobRequestOptions()
        {
            RetryPolicy = new ExponentialRetry(TimeSpan.FromSeconds(4), 4),
        };

        /// <summary>
        /// Create a blob manager.
        /// </summary>
        /// <param name="storageAccount">The cloud storage account, or null if using local file paths</param>
        /// <param name="taskHubName">The name of the taskhub</param>
        /// <param name="logger">A logger for logging</param>
        /// <param name="etwLogLevel">A limit on log event level emitted to ETW</param>
        /// <param name="partitionId">The partition id</param>
        /// <param name="errorHandler">A handler for errors encountered in this partition</param>
        public BlobManager(CloudStorageAccount storageAccount, string taskHubName, ILogger logger, Microsoft.Extensions.Logging.LogLevel etwLogLevel, uint partitionId, IPartitionErrorHandler errorHandler)
        {
            this.cloudStorageAccount = storageAccount;
            this.UseLocalFilesForTestingAndDebugging = (storageAccount == null);
            this.containerName = GetContainerName(taskHubName);
            this.partitionId = partitionId;

            if (!this.UseLocalFilesForTestingAndDebugging)
            {
                CloudBlobClient serviceClient = this.cloudStorageAccount.CreateCloudBlobClient();
                this.blobContainer = serviceClient.GetContainerReference(containerName);
            }

            this.TraceHelper = new FasterTraceHelper(logger, etwLogLevel, this.partitionId, this.UseLocalFilesForTestingAndDebugging ? "none" : this.cloudStorageAccount.Credentials.AccountName, taskHubName);
            this.PartitionErrorHandler = errorHandler;
            this.shutDownOrTermination = CancellationTokenSource.CreateLinkedTokenSource(errorHandler.Token);

            if (!this.UseLocalFilesForTestingAndDebugging)
            {
                CloudBlobClient serviceClient = this.cloudStorageAccount.CreateCloudBlobClient();
                this.blobContainer = serviceClient.GetContainerReference(containerName);
            }
        }

        // For testing and debugging with local files
        internal static string LocalFileDirectoryForTestingAndDebugging { get; set; } = @"E:\Faster";

        private string LocalDirectoryPath => $"{LocalFileDirectoryForTestingAndDebugging}\\{this.containerName}";

        private string PartitionFolder => $"p{this.partitionId:D2}";
        private const string EventLogBlobName = "evts";
        private const string CommitBlobName = "evts.commit";
        private const string HybridLogBlobName = "store";
        private const string ObjectLogBlobName = "store.obj";

        private Task LeaseRenewalLoopTask = Task.CompletedTask;

        private volatile Task NextLeaseRenewalTask = Task.CompletedTask;

        private static string GetContainerName(string taskHubName)
        {
            return taskHubName.ToLowerInvariant() + "-storage";
        }

        public async Task StartAsync()
        {
            if (this.UseLocalFilesForTestingAndDebugging)
            {
                Directory.CreateDirectory($"{LocalDirectoryPath}\\{PartitionFolder}");

                this.EventLogDevice = Devices.CreateLogDevice($"{LocalDirectoryPath}\\{PartitionFolder}\\{EventLogBlobName}");
                this.HybridLogDevice = Devices.CreateLogDevice($"{LocalDirectoryPath}\\{PartitionFolder}\\{HybridLogBlobName}");
                this.ObjectLogDevice = Devices.CreateLogDevice($"{LocalDirectoryPath}\\{PartitionFolder}\\{ObjectLogBlobName}");
            }
            else
            {
                await this.blobContainer.CreateIfNotExistsAsync();
                this.partitionDirectory = this.blobContainer.GetDirectoryReference(this.PartitionFolder);

                this.eventLogCommitBlob = this.partitionDirectory.GetBlockBlobReference(CommitBlobName);

                var eventLogDevice = new AzureStorageDevice(EventLogBlobName, this.partitionDirectory.GetDirectoryReference(EventLogBlobName), this, true);
                var hybridLogDevice = new AzureStorageDevice(HybridLogBlobName, this.partitionDirectory.GetDirectoryReference(HybridLogBlobName), this, true);
                var objectLogDevice = new AzureStorageDevice(ObjectLogBlobName, this.partitionDirectory.GetDirectoryReference(ObjectLogBlobName), this, true);

                await this.AcquireOwnership();
                await Task.WhenAll(eventLogDevice.StartAsync(), hybridLogDevice.StartAsync(), objectLogDevice.StartAsync());

                this.EventLogDevice = eventLogDevice;
                this.HybridLogDevice = hybridLogDevice;
                this.ObjectLogDevice = objectLogDevice;
            }
        }

        public void HandleBlobError(string where, string message, CloudBlob blob, Exception e, bool isFatal, bool isWarning)
        {
            if (isWarning)
            {
                this.TraceHelper.FasterBlobStorageWarning(message, blob, e);
            }
            else
            {
                this.TraceHelper.FasterBlobStorageError(message, blob, e);
            }
            this.PartitionErrorHandler.HandleError(where, $"Encountered storage exception for blob {blob?.Name ?? ""}", e, isFatal, isWarning);
        }

        // clean shutdown, wait for everything, then terminate
        public async Task StopAsync()
        {
            this.shutDownOrTermination.Cancel(); // has no effect if already cancelled

            await this.LeaseRenewalLoopTask;
        }

        public static async Task DeleteTaskhubStorageAsync(CloudStorageAccount account, string taskHubName)
        {
            var containerName = GetContainerName(taskHubName);

            if (account == null)
            {
                System.IO.DirectoryInfo di = new DirectoryInfo($"{LocalFileDirectoryForTestingAndDebugging}\\{containerName}");
                if (di.Exists)
                {
                    di.Delete(true);
                }
            }
            else
            {
                CloudBlobClient serviceClient = account.CreateCloudBlobClient();
                var blobContainer = serviceClient.GetContainerReference(containerName);

                if (await blobContainer.ExistsAsync())
                {
                    // do a complete deletion of all contents of this directory
                    var allBlobsInContainer = blobContainer.ListBlobs(null, true).ToList();
                    var tasks = new List<Task>();
                    foreach (IListBlobItem blob in allBlobsInContainer)
                    {
                        if (blob.GetType() == typeof(CloudBlob) || blob.GetType().BaseType == typeof(CloudBlob))
                        {
                            tasks.Add(BlobUtils.ForceDeleteAsync((CloudBlob)blob));
                        }
                    };

                    await Task.WhenAll(tasks);
                }

                // we are not deleting the container itself because it creates problems
                // when trying to recreate the same container soon afterwards
                // so we leave an empty container behind. Oh well.
            }
        }

        public ValueTask ConfirmLeaseIsGoodForAWhileAsync()
        {
            if (this.leaseTimer?.Elapsed < this.LeaseDuration - this.LeaseSafetyBuffer)
            {
                return default;
            }
            else
            {
                this.TraceHelper.LeaseProgress($"Access is waiting for fresh lease");
                return new ValueTask(this.NextLeaseRenewalTask);
            }
        }

        public void ConfirmLeaseIsGoodForAWhile()
        {
            if (this.leaseTimer?.Elapsed < this.LeaseDuration - this.LeaseSafetyBuffer)
            {
                return;
            }
            else
            {
                this.TraceHelper.LeaseProgress($"Access is waiting for fresh lease");
                this.NextLeaseRenewalTask.Wait();
            }
        }

        private async Task AcquireOwnership()
        {
            var newLeaseTimer = new System.Diagnostics.Stopwatch();

            while (!this.PartitionErrorHandler.IsTerminated)
            {
                try
                {
                    newLeaseTimer.Restart();

                    this.LeaseId = await this.eventLogCommitBlob.AcquireLeaseAsync(LeaseDuration, null,
                        accessCondition: null, options: this.BlobRequestOptionsUnderLease, operationContext: null, cancellationToken: this.PartitionErrorHandler.Token);

                    this.leaseTimer = newLeaseTimer;

                    break;
                }
                catch (StorageException ex) when (BlobUtils.LeaseConflictOrExpired(ex))
                {
                    this.TraceHelper.LeaseProgress("Waiting for lease");

                    // the previous owner has not released the lease yet, 
                    // try again until it becomes available, should be relatively soon
                    // as the transport layer is supposed to shut down the previous owner when starting this
                    await Task.Delay(TimeSpan.FromSeconds(1), this.PartitionErrorHandler.Token);

                    continue;
                }
                catch (StorageException ex) when (BlobUtils.BlobDoesNotExist(ex))
                {
                    try
                    {
                        // Create blob with empty content, then try again
                        this.TraceHelper.LeaseProgress("Creating commit blob");
                        await this.eventLogCommitBlob.UploadFromByteArrayAsync(Array.Empty<byte>(), 0, 0);
                        continue;
                    }
                    catch (StorageException ex2) when (BlobUtils.LeaseConflictOrExpired(ex2))
                    {
                        // creation race, try from top
                        this.TraceHelper.LeaseProgress("Creation race observed, retrying");
                        continue;
                    }
                }
                catch (Exception e)
                {
                    this.PartitionErrorHandler.HandleError(nameof(AcquireOwnership), "Could not acquire partition lease", e, true, false);
                    throw;
                }
            }

            this.TraceHelper.LeaseAcquired();

            this.LeaseRenewalLoopTask = this.LeaseRenewalLoopAsync();
        }

        public async Task RenewLeaseTask()
        {
            await Task.Delay(this.LeaseRenewal, this.shutDownOrTermination.Token);
            AccessCondition acc = new AccessCondition() { LeaseId = this.LeaseId };
            var nextLeaseTimer = new System.Diagnostics.Stopwatch();
            this.TraceHelper.LeaseProgress("Renewing lease");
            nextLeaseTimer.Start();
            await this.eventLogCommitBlob.RenewLeaseAsync(acc, this.PartitionErrorHandler.Token);
            this.leaseTimer = nextLeaseTimer;
            this.TraceHelper.LeaseProgress("Renewed lease");
        }

        public async Task LeaseRenewalLoopAsync()
        {
            try
            {
                while (true)
                {
                    // save the task so storage accesses can wait for it
                    this.NextLeaseRenewalTask = this.RenewLeaseTask();

                    // wait for successful renewal, or exit the loop as this throws
                    await this.NextLeaseRenewalTask; 
                }
            }
            catch (OperationCanceledException)
            {
                // it's o.k. to cancel while waiting
            }
            catch (StorageException e) when (e.InnerException != null && e.InnerException is OperationCanceledException)
            {
                // it's o.k. to cancel a lease renewal
            }
            catch (StorageException ex) when (BlobUtils.LeaseConflict(ex))
            {
                // We lost the lease to someone else. Terminate ownership immediately.
                this.PartitionErrorHandler.HandleError(nameof(LeaseRenewalLoopAsync), "Lost partition lease", ex, true, true);
            }
            catch (Exception e)
            {
                this.PartitionErrorHandler.HandleError(nameof(LeaseRenewalLoopAsync), "Could not maintain partition lease", e, true, false);
            }

            // if this is a clean shutdown try to release the lease
            // otherwise leave it be and let it expire to protect straggling storage accesses
            if (!this.PartitionErrorHandler.IsTerminated)
            {
                try
                {
                    this.TraceHelper.LeaseProgress("Releasing lease");

                    AccessCondition acc = new AccessCondition() { LeaseId = this.LeaseId };

                    await this.eventLogCommitBlob.ReleaseLeaseAsync(accessCondition: acc,
                        options: this.BlobRequestOptionsUnderLease, operationContext: null, cancellationToken: this.PartitionErrorHandler.Token);

                    this.TraceHelper.LeaseReleased();
                }
                catch (OperationCanceledException)
                {
                    // it's o.k. if termination is triggered while waiting
                }
                catch (StorageException e) when (e.InnerException != null && e.InnerException is OperationCanceledException)
                {
                    // it's o.k. if termination is triggered while we are releasing the lease
                }
                catch (Exception e)
                {
                    this.TraceHelper.FasterBlobStorageWarning("could not release lease", this.eventLogCommitBlob, e);
                    // swallow exceptions when releasing a lease
                }
            }

            this.PartitionErrorHandler.TerminateNormally();

            this.TraceHelper.LeaseProgress("Blob manager stopped");
        }

        #region ILogCommitManager

        void ILogCommitManager.Commit(long beginAddress, long untilAddress, byte[] commitMetadata)
        {
            while (true)
            {
                AccessCondition acc = new AccessCondition() { LeaseId = this.LeaseId };
                try
                {
                    this.eventLogCommitBlob.UploadFromByteArray(commitMetadata, 0, commitMetadata.Length, acc, this.BlobRequestOptionsUnderLease);
                    return;
                }
                catch (StorageException ex) when (BlobUtils.LeaseExpired(ex))
                {
                    // if we get here, the lease renewal task did not complete in time
                    // wait for it to complete or throw
                    this.NextLeaseRenewalTask.Wait();
                    continue;
                }
                catch (StorageException ex) when (BlobUtils.LeaseConflict(ex))
                {
                    // We lost the lease to someone else. Terminate ownership immediately.
                    this.TraceHelper.LeaseLost(nameof(ILogCommitManager.Commit));
                    this.HandleBlobError(nameof(ILogCommitManager.Commit), "lease lost", this.eventLogCommitBlob, ex, true, this.PartitionErrorHandler.IsTerminated);
                    throw;
                }
                catch (Exception e)
                {
                    this.TraceHelper.FasterBlobStorageError(nameof(ILogCommitManager.Commit), this.eventLogCommitBlob, e);
                    throw;
                }
            }
        }

        byte[] ILogCommitManager.GetCommitMetadata()
        {
            while (true)
            {
                AccessCondition acc = new AccessCondition() { LeaseId = this.LeaseId };
                try
                {
                    using (var stream = new MemoryStream())
                    {
                        this.eventLogCommitBlob.DownloadToStream(stream, acc, this.BlobRequestOptionsUnderLease);
                        var bytes = stream.ToArray();
                        return bytes.Length == 0 ? null : bytes;
                    }
                }
                catch (StorageException ex) when (BlobUtils.LeaseExpired(ex))
                {
                    // if we get here, the lease renewal task did not complete in time
                    // wait for it to complete or throw
                    this.NextLeaseRenewalTask.Wait();
                    continue;
                }
                catch (StorageException ex) when (BlobUtils.LeaseConflict(ex))
                {
                    // We lost the lease to someone else. Terminate ownership immediately.
                    this.TraceHelper.LeaseLost(nameof(ILogCommitManager.GetCommitMetadata));
                    this.HandleBlobError(nameof(ILogCommitManager.Commit), "lease lost", this.eventLogCommitBlob, ex, true, this.PartitionErrorHandler.IsTerminated);
                    throw;
                }
                catch (Exception e)
                {
                    this.TraceHelper.FasterBlobStorageError(nameof(ILogCommitManager.GetCommitMetadata), this.eventLogCommitBlob, e);
                    throw;
                }
            }
        }

        #endregion

        #region ICheckpointManager

        void ICheckpointManager.InitializeIndexCheckpoint(Guid indexToken)
        {
            // there is no need to create empty directories in a blob container
        }

        void ICheckpointManager.InitializeLogCheckpoint(Guid logToken)
        {
            // there is no need to create empty directories in a blob container
        }

        void ICheckpointManager.CommitIndexCheckpoint(Guid indexToken, byte[] commitMetadata)
        {
            CloudBlockBlob target = null;
            try
            {
                var metaFileBlob = target = this.partitionDirectory.GetBlockBlobReference(this.GetIndexCheckpointMetaBlob(indexToken));
                // we don't need a lease for the checkpoint data since the checkpoint token provides isolation
                using (var blobStream = metaFileBlob.OpenWrite())
                {
                    using (var writer = new BinaryWriter(blobStream))
                    {
                        writer.Write(commitMetadata.Length);
                        writer.Write(commitMetadata);
                        writer.Flush();
                    }
                }

                var completedFileBlob = this.partitionDirectory.GetBlockBlobReference(this.GetIndexCheckpointCompletedBlob());
                this.ConfirmLeaseIsGoodForAWhile(); // we need the lease for the checkpoint completed file
                completedFileBlob.UploadText(indexToken.ToString());
            }
            catch (Exception e)
            {
                this.TraceHelper.FasterBlobStorageError(nameof(ICheckpointManager.CommitIndexCheckpoint), target, e);
                throw;
            }
        }

        void ICheckpointManager.CommitLogCheckpoint(Guid logToken, byte[] commitMetadata)
        {
            CloudBlockBlob target = null;
            try
            {
                var metaFileBlob = target = this.partitionDirectory.GetBlockBlobReference(this.GetHybridLogCheckpointMetaBlob(logToken));
                // we don't need a lease for the checkpoint data since the checkpoint token provides isolation
                using (var blobStream = metaFileBlob.OpenWrite())
                {
                    using (var writer = new BinaryWriter(blobStream))
                    {
                        writer.Write(this.CheckpointCommitLogPosition);
                        writer.Write(this.CheckpointInputQueuePosition);
                        writer.Write(commitMetadata.Length);
                        writer.Write(commitMetadata);
                        writer.Flush();
                    }
                }

                var completedFileBlob = target = this.partitionDirectory.GetBlockBlobReference(this.GetHybridLogCheckpointCompletedBlob());
                this.ConfirmLeaseIsGoodForAWhile(); // we need the lease for the checkpoint completed file
                completedFileBlob.UploadText(logToken.ToString());
            }
            catch (Exception e)
            {
                this.TraceHelper.FasterBlobStorageError(nameof(ICheckpointManager.CommitLogCheckpoint), target, e);
                throw;
            }
        }

        byte[] ICheckpointManager.GetIndexCommitMetadata(Guid indexToken)
        {
            CloudBlockBlob target = null;
            try
            {
                var metaFileBlob = target = this.partitionDirectory.GetBlockBlobReference(this.GetIndexCheckpointMetaBlob(indexToken));
                // we don't need a lease for the checkpoint data since the checkpoint token provides isolation
                using (var blobstream = metaFileBlob.OpenRead())
                {
                    using (var reader = new BinaryReader(blobstream))
                    {
                        var len = reader.ReadInt32();
                        return reader.ReadBytes(len);
                    }
                }
            }
            catch (Exception e)
            {
                this.TraceHelper.FasterBlobStorageError(nameof(ICheckpointManager.GetIndexCommitMetadata), target, e);
                throw;
            }
        }

        byte[] ICheckpointManager.GetLogCommitMetadata(Guid logToken)
        {
            CloudBlockBlob target = null;
            try
            {
                var metaFileBlob = target = this.partitionDirectory.GetBlockBlobReference(this.GetHybridLogCheckpointMetaBlob(logToken));
                // we don't need a lease for the checkpoint data since the checkpoint token provides isolation
                using (var blobstream = metaFileBlob.OpenRead())
                {
                    using (var reader = new BinaryReader(blobstream))
                    {
                        this.CheckpointCommitLogPosition = reader.ReadInt64();
                        this.CheckpointInputQueuePosition = reader.ReadInt64();
                        var len = reader.ReadInt32();
                        return reader.ReadBytes(len);
                    }
                }
            }
            catch (Exception e)
            {
                this.TraceHelper.FasterBlobStorageError(nameof(ICheckpointManager.GetLogCommitMetadata), target, e);
                throw;
            }
        }

        IDevice ICheckpointManager.GetIndexDevice(Guid indexToken)
        {
            try
            {
                var (path, blobName) = this.GetPrimaryHashTableBlob(indexToken);
                var blobDirectory = this.partitionDirectory.GetDirectoryReference(path);
                var device = new AzureStorageDevice(blobName, blobDirectory, this, false); // we don't need a lease since the token provides isolation
                device.StartAsync().Wait();
                return device;
            }
            catch (Exception e)
            {
                this.TraceHelper.FasterBlobStorageError(nameof(ICheckpointManager.GetIndexDevice), null, e);
                throw;
            }
        }

        IDevice ICheckpointManager.GetSnapshotLogDevice(Guid token)
        {
            try
            {
                var (path, blobName) = this.GetLogSnapshotBlob(token);
                var blobDirectory = this.partitionDirectory.GetDirectoryReference(path);
                var device = new AzureStorageDevice(blobName, blobDirectory, this, false); // we don't need a lease since the token provides isolation
                device.StartAsync().Wait();
                return device;
            }
            catch (Exception e)
            {
                this.TraceHelper.FasterBlobStorageError(nameof(ICheckpointManager.GetSnapshotLogDevice), null, e);
                throw;
            }
        }

        IDevice ICheckpointManager.GetSnapshotObjectLogDevice(Guid token)
        {
            try
            {
                var (path, blobName) = this.GetObjectLogSnapshotBlob(token);
                var blobDirectory = this.partitionDirectory.GetDirectoryReference(path);
                var device = new AzureStorageDevice(blobName, blobDirectory, this, false); // we don't need a lease since the token provides isolation
                device.StartAsync().Wait();
                return device;
            }
            catch (Exception e)
            {
                this.TraceHelper.FasterBlobStorageError(nameof(ICheckpointManager.GetSnapshotObjectLogDevice), null, e);
                throw;
            }
        }

        bool ICheckpointManager.GetLatestCheckpoint(out Guid indexToken, out Guid logToken)
        {
            CloudBlockBlob target = null;
            try
            {
                var indexCompletedFileBlob = this.partitionDirectory.GetBlockBlobReference(this.GetIndexCheckpointCompletedBlob());
                var logCompletedFileBlob = this.partitionDirectory.GetBlockBlobReference(this.GetHybridLogCheckpointCompletedBlob());

                if (indexCompletedFileBlob.Exists() && logCompletedFileBlob.Exists())
                {
                    target = indexCompletedFileBlob;
                    this.ConfirmLeaseIsGoodForAWhile();
                    var lastIndexCheckpoint = indexCompletedFileBlob.DownloadText();
                    indexToken = Guid.Parse(lastIndexCheckpoint);

                    target = logCompletedFileBlob;
                    this.ConfirmLeaseIsGoodForAWhile();
                    var lastLogCheckpoint = logCompletedFileBlob.DownloadText();
                    logToken = Guid.Parse(lastLogCheckpoint);

                    return true;
                }
                else
                {
                    return false;
                }
            }
            catch (Exception e)
            {
                this.TraceHelper.FasterBlobStorageError(nameof(ICheckpointManager.GetLatestCheckpoint), target, e);
                throw;
            }
        }

        #endregion

        #region Blob Name Management

        private string GetIndexCheckpointMetaBlob(Guid token)
        {
            return $"index-checkpoints/{token}/info.dat";
        }

        private string GetIndexCheckpointCompletedBlob()
        {
            return $"index-checkpoints/last.txt";
        }

        private (string, string) GetPrimaryHashTableBlob(Guid token)
        {
            return ($"index-checkpoints/{token}", "ht.dat");
        }

        private string GetHybridLogCheckpointMetaBlob(Guid token)
        {
            return $"cpr-checkpoints/{token}/info.dat";
        }

        private string GetHybridLogCheckpointCompletedBlob()
        {
            return $"cpr-checkpoints/last.txt";
        }

        private (string, string) GetLogSnapshotBlob(Guid token)
        {
            return ($"cpr-checkpoints/{token}", "snapshot.dat");
        }

        private (string, string) GetObjectLogSnapshotBlob(Guid token)
        {
            return ($"cpr-checkpoints/{token}", "snapshot.obj.dat");
        }

        #endregion
    }
}