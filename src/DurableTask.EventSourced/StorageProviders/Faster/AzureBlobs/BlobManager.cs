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

using DurableTask.Core.Common;
using Dynamitey.DynamicObjects;
using FASTER.core;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.Storage.Blob.Protocol;
using Microsoft.Azure.Storage.RetryPolicies;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
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
        private readonly uint partitionId;
        private readonly CancellationTokenSource shutDownOrTermination;
        private readonly CloudStorageAccount cloudStorageAccount;

        private CloudBlobContainer blobContainer;
        private CloudBlockBlob eventLogCommitBlob;
        private CloudBlobDirectory partitionDirectory;

        private string leaseId;

        private TimeSpan LeaseDuration = TimeSpan.FromSeconds(45); // max time the lease stays after unclean shutdown
        private TimeSpan LeaseRenewal = TimeSpan.FromSeconds(30); // how often we renew the lease
        private TimeSpan LeaseSafetyBuffer = TimeSpan.FromSeconds(10); // how much time we want left on the lease before issuing a protected access

        internal CheckpointInfo CheckpointInfo { get; private set; } = new CheckpointInfo();

        internal FasterTraceHelper TraceHelper { get; private set; }
        internal FasterTraceHelper StorageTracer => this.TraceHelper.IsTracingAtMostDetailedLevel ? this.TraceHelper : null;

        internal bool UseLocalFilesForTestingAndDebugging { get; private set; }

        public IDevice EventLogDevice { get; private set; }
        public IDevice HybridLogDevice { get; private set; }
        public IDevice ObjectLogDevice { get; private set; }

        public string ContainerName { get; }

        public IPartitionErrorHandler PartitionErrorHandler { get; private set; }

        private volatile System.Diagnostics.Stopwatch leaseTimer;

        public FasterLogSettings EventLogSettings => new FasterLogSettings
        {
            LogDevice = this.EventLogDevice,
            LogCommitManager = this.UseLocalFilesForTestingAndDebugging ?
                new LocalLogCommitManager($"{this.LocalDirectoryPath}\\{this.PartitionFolder}\\{CommitBlobName}") : (ILogCommitManager)this,
            PageSizeBits = 18, // 256k
            SegmentSizeBits = 28, // 256 MB
            MemorySizeBits = 22, // 4MB
        };

        public LogSettings StoreLogSettings => new LogSettings
        {
            LogDevice = this.HybridLogDevice,
            ObjectLogDevice = this.ObjectLogDevice,
            PageSizeBits = 20, // 1MB
            MutableFraction = 0.9,
            SegmentSizeBits = 28, // 256 MB
            CopyReadsToTail = true,
            MemorySizeBits = 24, // 16MB
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
            NetworkTimeout = TimeSpan.FromSeconds(50),
        };

        public BlobRequestOptions BlobRequestOptionsNotUnderLease => new BlobRequestOptions()
        {
            RetryPolicy = new ExponentialRetry(TimeSpan.FromSeconds(4), 4),
            NetworkTimeout = TimeSpan.FromSeconds(50),
        };

        /// <summary>
        /// Create a blob manager.
        /// </summary>
        /// <param name="storageAccount">The cloud storage account, or null if using local file paths</param>
        /// <param name="taskHubName">The name of the taskhub</param>
        /// <param name="logger">A logger for logging</param>
        /// <param name="logLevelLimit">A limit on log event level emitted</param>
        /// <param name="partitionId">The partition id</param>
        /// <param name="errorHandler">A handler for errors encountered in this partition</param>
        public BlobManager(CloudStorageAccount storageAccount, string taskHubName, ILogger logger, Microsoft.Extensions.Logging.LogLevel logLevelLimit, uint partitionId, IPartitionErrorHandler errorHandler)
        {
            this.cloudStorageAccount = storageAccount;
            this.UseLocalFilesForTestingAndDebugging = (storageAccount == null);
            this.ContainerName = GetContainerName(taskHubName);
            this.partitionId = partitionId;

            if (!this.UseLocalFilesForTestingAndDebugging)
            {
                CloudBlobClient serviceClient = this.cloudStorageAccount.CreateCloudBlobClient();
                this.blobContainer = serviceClient.GetContainerReference(this.ContainerName);
            }

            this.TraceHelper = new FasterTraceHelper(logger, logLevelLimit, this.partitionId, this.UseLocalFilesForTestingAndDebugging ? "none" : this.cloudStorageAccount.Credentials.AccountName, taskHubName);
            this.PartitionErrorHandler = errorHandler;
            this.shutDownOrTermination = CancellationTokenSource.CreateLinkedTokenSource(errorHandler.Token);
        }

        // For testing and debugging with local files
        internal static string LocalFileDirectoryForTestingAndDebugging { get; set; } = @"E:\Faster";

        private string LocalDirectoryPath => $"{LocalFileDirectoryForTestingAndDebugging}\\{this.ContainerName}";

        private string PartitionFolder => $"p{this.partitionId:D2}";
        private const string EventLogBlobName = "commit-log";
        private const string CommitBlobName = "commit-lease";
        private const string HybridLogBlobName = "store";
        private const string ObjectLogBlobName = "store.obj";

        private Task LeaseMaintenanceLoopTask = Task.CompletedTask;

        private volatile Task NextLeaseRenewalTask = Task.CompletedTask;

        private static string GetContainerName(string taskHubName)
        {
            return taskHubName.ToLowerInvariant() + "-storage";
        }

        public async Task StartAsync()
        {
            if (this.UseLocalFilesForTestingAndDebugging)
            {
                Directory.CreateDirectory($"{this.LocalDirectoryPath}\\{PartitionFolder}");

                this.EventLogDevice = Devices.CreateLogDevice($"{this.LocalDirectoryPath}\\{PartitionFolder}\\{EventLogBlobName}");
                this.HybridLogDevice = Devices.CreateLogDevice($"{this.LocalDirectoryPath}\\{PartitionFolder}\\{HybridLogBlobName}");
                this.ObjectLogDevice = Devices.CreateLogDevice($"{this.LocalDirectoryPath}\\{PartitionFolder}\\{ObjectLogBlobName}");
            }
            else
            {
                await this.blobContainer.CreateIfNotExistsAsync().ConfigureAwait(false);
                this.partitionDirectory = this.blobContainer.GetDirectoryReference(this.PartitionFolder);

                this.eventLogCommitBlob = this.partitionDirectory.GetBlockBlobReference(CommitBlobName);

                var eventLogDevice = new AzureStorageDevice(EventLogBlobName, this.partitionDirectory.GetDirectoryReference(EventLogBlobName), this, true);
                var hybridLogDevice = new AzureStorageDevice(HybridLogBlobName, this.partitionDirectory.GetDirectoryReference(HybridLogBlobName), this, true);
                var objectLogDevice = new AzureStorageDevice(ObjectLogBlobName, this.partitionDirectory.GetDirectoryReference(ObjectLogBlobName), this, true);

                await this.AcquireOwnership().ConfigureAwait(false);

                this.TraceHelper.FasterProgress("Starting Faster Devices");
                await Task.WhenAll(eventLogDevice.StartAsync(), hybridLogDevice.StartAsync(), objectLogDevice.StartAsync()).ConfigureAwait(false);
                this.TraceHelper.FasterProgress("Started Faster Devices");

                this.EventLogDevice = eventLogDevice;
                this.HybridLogDevice = hybridLogDevice;
                this.ObjectLogDevice = objectLogDevice;
            }
        }

        public void HandleBlobError(string where, string message, string blobName, Exception e, bool isFatal, bool isWarning)
        {
            if (isWarning)
            {
                this.TraceHelper.FasterBlobStorageWarning(message, blobName, e);
            }
            else
            {
                this.TraceHelper.FasterBlobStorageError(message, blobName, e);
            }
            this.PartitionErrorHandler.HandleError(where, $"Encountered storage exception for blob {blobName}", e, isFatal, isWarning);
        }

        // clean shutdown, wait for everything, then terminate
        public async Task StopAsync()
        {
            this.shutDownOrTermination.Cancel(); // has no effect if already cancelled

            await this.LeaseMaintenanceLoopTask.ConfigureAwait(false); // wait for loop to terminate cleanly
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

                if (await blobContainer.ExistsAsync().ConfigureAwait(false))
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

                    await Task.WhenAll(tasks).ConfigureAwait(false);
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
                this.TraceHelper.LeaseProgress("Access is waiting for fresh lease");
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
                this.TraceHelper.LeaseProgress("Access is waiting for fresh lease");
                this.NextLeaseRenewalTask.Wait();
                this.TraceHelper.LeaseProgress("Access has fresh lease now");
            }
        }

        private async Task AcquireOwnership()
        {
            var newLeaseTimer = new System.Diagnostics.Stopwatch();

            while (true)
            {
                this.PartitionErrorHandler.Token.ThrowIfCancellationRequested();

                try
                {
                    newLeaseTimer.Restart();

                    this.leaseId = await this.eventLogCommitBlob.AcquireLeaseAsync(LeaseDuration, null,
                        accessCondition: null, options: this.BlobRequestOptionsUnderLease, operationContext: null, cancellationToken: this.PartitionErrorHandler.Token).ConfigureAwait(false);

                    this.leaseTimer = newLeaseTimer;

                    this.TraceHelper.LeaseAcquired();

                    this.LeaseMaintenanceLoopTask = Task.Run(() => this.MaintenanceLoopAsync());

                    return;
                }
                catch (StorageException ex) when (BlobUtils.LeaseConflictOrExpired(ex))
                {
                    this.TraceHelper.LeaseProgress("Waiting for lease");

                    // the previous owner has not released the lease yet, 
                    // try again until it becomes available, should be relatively soon
                    // as the transport layer is supposed to shut down the previous owner when starting this
                    await Task.Delay(TimeSpan.FromSeconds(1), this.PartitionErrorHandler.Token).ConfigureAwait(false);

                    continue;
                }
                catch (StorageException ex) when (BlobUtils.BlobDoesNotExist(ex))
                {
                    try
                    {
                        // Create blob with empty content, then try again
                        this.TraceHelper.LeaseProgress("Creating commit blob");
                        await this.eventLogCommitBlob.UploadFromByteArrayAsync(Array.Empty<byte>(), 0, 0).ConfigureAwait(false);
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
        }

        public async Task RenewLeaseTask()
        {
            try
            {
                await Task.Delay(this.LeaseRenewal, this.shutDownOrTermination.Token).ConfigureAwait(false);
                AccessCondition acc = new AccessCondition() { LeaseId = this.leaseId };
                var nextLeaseTimer = new System.Diagnostics.Stopwatch();
                nextLeaseTimer.Start();

                this.TraceHelper.LeaseProgress("Renewing lease");
                await this.eventLogCommitBlob.RenewLeaseAsync(acc, this.PartitionErrorHandler.Token).ConfigureAwait(false);
                this.TraceHelper.LeaseProgress("Renewed lease");

                this.leaseTimer = nextLeaseTimer;
            }
            catch (Exception)
            {
                this.TraceHelper.LeaseProgress("Failed to renew lease");
                throw;
            }
        }

        public async Task MaintenanceLoopAsync()
        {
            this.TraceHelper.LeaseProgress("Started lease maintenance loop");
            try
            {
                while (true)
                {
                    // save the task so storage accesses can wait for it
                    this.NextLeaseRenewalTask = this.RenewLeaseTask();

                    // wait for successful renewal, or exit the loop as this throws
                    await this.NextLeaseRenewalTask.ConfigureAwait(false); 
                }
            }
            catch (OperationCanceledException)
            {
                // it's o.k. to cancel while waiting
                this.TraceHelper.LeaseProgress("Lease renewal loop cleanly canceled");
            }
            catch (StorageException e) when (e.InnerException != null && e.InnerException is OperationCanceledException)
            {
                // it's o.k. to cancel a lease renewal
                this.TraceHelper.LeaseProgress("Lease renewal storage operation canceled");
            }
            catch (StorageException ex) when (BlobUtils.LeaseConflict(ex))
            {
                // We lost the lease to someone else. Terminate ownership immediately.
                this.PartitionErrorHandler.HandleError(nameof(MaintenanceLoopAsync), "Lost partition lease", ex, true, true);
            }
            catch (Exception e) when (!Utils.IsFatal(e))
            {
                this.PartitionErrorHandler.HandleError(nameof(MaintenanceLoopAsync), "Could not maintain partition lease", e, true, false);
            }

            this.TraceHelper.LeaseProgress("Exited lease maintenance loop");

            if (this.PartitionErrorHandler.IsTerminated)
            {
                // this is an unclean shutdown, so we let the lease expire to protect straggling storage accesses
                this.TraceHelper.LeaseProgress("Leaving lease to expire on its own");
            }
            else 
            {
                try
                {
                    this.TraceHelper.LeaseProgress("Releasing lease");

                    AccessCondition acc = new AccessCondition() { LeaseId = this.leaseId };

                    await this.eventLogCommitBlob.ReleaseLeaseAsync(accessCondition: acc,
                        options: this.BlobRequestOptionsUnderLease, operationContext: null, cancellationToken: this.PartitionErrorHandler.Token).ConfigureAwait(false);

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
                    this.TraceHelper.FasterBlobStorageWarning("could not release lease", this.eventLogCommitBlob.Name, e);
                    // swallow exceptions when releasing a lease
                }
            }

            this.PartitionErrorHandler.TerminateNormally();

            this.TraceHelper.LeaseProgress("Blob manager stopped");
        }

        #region Blob Name Management

        private string GetCheckpointCompletedBlob()
        {
            return $"last-checkpoint.json";
        }

        private string GetIndexCheckpointMetaBlob(Guid token)
        {
            return $"index-checkpoints/{token}/info.dat";
        }

        private (string, string) GetPrimaryHashTableBlob(Guid token)
        {
            return ($"index-checkpoints/{token}", "ht.dat");
        }

        private string GetHybridLogCheckpointMetaBlob(Guid token)
        {
            return $"cpr-checkpoints/{token}/info.dat";
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

        #region ILogCommitManager

        void ILogCommitManager.Commit(long beginAddress, long untilAddress, byte[] commitMetadata)
        {
            this.StorageTracer?.FasterStorageProgress($"ILogCommitManager.Commit Called beginAddress={beginAddress} untilAddress={untilAddress}");

            while (true)
            {
                AccessCondition acc = new AccessCondition() { LeaseId = this.leaseId };
                try
                {
                    this.eventLogCommitBlob.UploadFromByteArray(commitMetadata, 0, commitMetadata.Length, acc, this.BlobRequestOptionsUnderLease);
                    this.StorageTracer?.FasterStorageProgress("ILogCommitManager.Commit Returned");
                    return;
                }
                catch (StorageException ex) when (BlobUtils.LeaseExpired(ex))
                {
                    // if we get here, the lease renewal task did not complete in time
                    // wait for it to complete or throw
                    this.TraceHelper.LeaseProgress("ILogCommitManager.Commit: wait for next renewal");
                    this.NextLeaseRenewalTask.Wait();
                    this.TraceHelper.LeaseProgress("ILogCommitManager.Commit: renewal complete");
                    continue;
                }
                catch (StorageException ex) when (BlobUtils.LeaseConflict(ex))
                {
                    // We lost the lease to someone else. Terminate ownership immediately.
                    this.TraceHelper.LeaseLost(nameof(ILogCommitManager.Commit));
                    this.HandleBlobError(nameof(ILogCommitManager.Commit), "lease lost", this.eventLogCommitBlob?.Name, ex, true, this.PartitionErrorHandler.IsTerminated);
                    throw;
                }
                catch (Exception e)
                {
                    this.TraceHelper.FasterBlobStorageError(nameof(ILogCommitManager.Commit), this.eventLogCommitBlob.Name, e);
                    throw;
                }
            }
        }

        byte[] ILogCommitManager.GetCommitMetadata()
        {
            this.StorageTracer?.FasterStorageProgress("ILogCommitManager.GetCommitMetadata Called");

            while (true)
            {
                AccessCondition acc = new AccessCondition() { LeaseId = this.leaseId };
                try
                {
                    using (var stream = new MemoryStream())
                    {
                        this.eventLogCommitBlob.DownloadToStream(stream, acc, this.BlobRequestOptionsUnderLease);
                        var bytes = stream.ToArray();
                        this.StorageTracer?.FasterStorageProgress($"ILogCommitManager.GetCommitMetadata Returned {bytes?.Length ?? null} bytes");
                        return bytes.Length == 0 ? null : bytes;
                    }
                }
                catch (StorageException ex) when (BlobUtils.LeaseExpired(ex))
                {
                    // if we get here, the lease renewal task did not complete in time
                    // wait for it to complete or throw
                    this.TraceHelper.LeaseProgress("ILogCommitManager.GetCommitMetadata: wait for next renewal");
                    this.NextLeaseRenewalTask.Wait();
                    this.TraceHelper.LeaseProgress("ILogCommitManager.GetCommitMetadata: renewal complete");
                    continue;
                }
                catch (StorageException ex) when (BlobUtils.LeaseConflict(ex))
                {
                    // We lost the lease to someone else. Terminate ownership immediately.
                    this.TraceHelper.LeaseLost(nameof(ILogCommitManager.GetCommitMetadata));
                    this.HandleBlobError(nameof(ILogCommitManager.Commit), "lease lost", this.eventLogCommitBlob?.Name, ex, true, this.PartitionErrorHandler.IsTerminated);
                    throw;
                }
                catch (Exception e)
                {
                    this.TraceHelper.FasterBlobStorageError(nameof(ILogCommitManager.GetCommitMetadata), this.eventLogCommitBlob.Name, e);
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
            this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.CommitIndexCheckpoint Called indexToken={indexToken}");
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

                this.CheckpointInfo.IndexToken = indexToken;

                this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.CommitIndexCheckpoint Returned target={target.Name}");
            }
            catch (Exception e)
            {
                this.TraceHelper.FasterBlobStorageError(nameof(ICheckpointManager.CommitIndexCheckpoint), target.Name, e);
                throw;
            }
        }

        void ICheckpointManager.CommitLogCheckpoint(Guid logToken, byte[] commitMetadata)
        {
            this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.CommitLogCheckpoint Called logToken={logToken}");
            CloudBlockBlob target = null;
            try
            {
                var metaFileBlob = target = this.partitionDirectory.GetBlockBlobReference(this.GetHybridLogCheckpointMetaBlob(logToken));
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

                this.CheckpointInfo.LogToken = logToken;
                this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.CommitLogCheckpoint Returned target={target.Name}");
            }
            catch (Exception e)
            {
                this.TraceHelper.FasterBlobStorageError(nameof(ICheckpointManager.CommitLogCheckpoint), target?.Name, e);
                throw;
            }
        }

        byte[] ICheckpointManager.GetIndexCommitMetadata(Guid indexToken)
        {
            this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.GetIndexCommitMetadata Called indexToken={indexToken}");
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
                        var result = reader.ReadBytes(len);
                        this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.GetIndexCommitMetadata Returned {result?.Length ?? null} bytes target={target.Name}");
                        return result;
                    }
                }
            }
            catch (Exception e)
            {
                this.TraceHelper.FasterBlobStorageError(nameof(ICheckpointManager.GetIndexCommitMetadata), target?.Name, e);
                throw;
            }
        }

        byte[] ICheckpointManager.GetLogCommitMetadata(Guid logToken)
        {
            this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.GetIndexCommitMetadata Called logToken={logToken}");
            CloudBlockBlob target = null;
            try
            {
                var metaFileBlob = target = this.partitionDirectory.GetBlockBlobReference(this.GetHybridLogCheckpointMetaBlob(logToken));
                // we don't need a lease for the checkpoint data since the checkpoint token provides isolation
                using (var blobstream = metaFileBlob.OpenRead())
                {
                    using (var reader = new BinaryReader(blobstream))
                    {
                        var len = reader.ReadInt32();
                        var result = reader.ReadBytes(len);
                        this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.GetIndexCommitMetadata Returned {result?.Length ?? null} bytes target={target.Name}");
                        return result;
                    }
                }
            }
            catch (Exception e)
            {
                this.TraceHelper.FasterBlobStorageError(nameof(ICheckpointManager.GetLogCommitMetadata), target?.Name, e);
                throw;
            }
        }

        IDevice ICheckpointManager.GetIndexDevice(Guid indexToken)
        {
            this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.GetIndexDevice Called indexToken={indexToken}");
            try
            {
                var (path, blobName) = this.GetPrimaryHashTableBlob(indexToken);
                var blobDirectory = this.partitionDirectory.GetDirectoryReference(path);
                var device = new AzureStorageDevice(blobName, blobDirectory, this, false); // we don't need a lease since the token provides isolation
                device.StartAsync().Wait();
                this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.GetIndexDevice Returned blobDirectory={blobDirectory} blobName={blobName}");
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
            this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.GetSnapshotLogDevice Called token={token}");
            try
            {
                var (path, blobName) = this.GetLogSnapshotBlob(token);
                var blobDirectory = this.partitionDirectory.GetDirectoryReference(path);
                var device = new AzureStorageDevice(blobName, blobDirectory, this, false); // we don't need a lease since the token provides isolation
                device.StartAsync().Wait();
                this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.GetSnapshotLogDevice Returned blobDirectory={blobDirectory} blobName={blobName}");
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
            this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.GetSnapshotObjectLogDevice Called token={token}");
            try
            {
                var (path, blobName) = this.GetObjectLogSnapshotBlob(token);
                var blobDirectory = this.partitionDirectory.GetDirectoryReference(path);
                var device = new AzureStorageDevice(blobName, blobDirectory, this, false); // we don't need a lease since the token provides isolation
                device.StartAsync().Wait();
                this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.GetSnapshotObjectLogDevice Returned blobDirectory={blobDirectory} blobName={blobName}");
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
            this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.GetLatestCheckpoint Called");
            CloudBlockBlob target = null;
            try
            {
                var checkpointCompletedBlob = this.partitionDirectory.GetBlockBlobReference(this.GetCheckpointCompletedBlob());

                if (checkpointCompletedBlob.Exists())
                {
                    target = checkpointCompletedBlob;
                    this.ConfirmLeaseIsGoodForAWhile();
                    var jsonString = checkpointCompletedBlob.DownloadText();
                    this.CheckpointInfo = JsonConvert.DeserializeObject<CheckpointInfo>(jsonString);

                    indexToken = this.CheckpointInfo.IndexToken;
                    logToken = this.CheckpointInfo.LogToken;

                    this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.GetSnapshotObjectLogDevice Returned indexToken={indexToken} logToken={logToken}");
                    return true;
                }
                else
                {
                    this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.GetSnapshotObjectLogDevice Returned false");
                    return false;
                }
            }
            catch (Exception e)
            {
                this.TraceHelper.FasterBlobStorageError(nameof(ICheckpointManager.GetLatestCheckpoint), target?.Name, e);
                throw;
            }
        }

        internal async Task WriteCheckpointCompletedAsync()
        {
            // write the final file that has all the checkpoint info
            var checkpointCompletedBlob = this.partitionDirectory.GetBlockBlobReference(this.GetCheckpointCompletedBlob());
            await this.ConfirmLeaseIsGoodForAWhileAsync().ConfigureAwait(false); // the lease protects the checkpoint completed file
            await checkpointCompletedBlob.UploadTextAsync(JsonConvert.SerializeObject(this.CheckpointInfo, Formatting.Indented)).ConfigureAwait(false);
        }

        #endregion
    }
}