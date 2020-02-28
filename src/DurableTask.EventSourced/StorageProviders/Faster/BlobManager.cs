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


using FASTER.core;
using FASTER.devices;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.Storage.Blob.Protocol;
using Microsoft.Extensions.Logging;
using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventSourced.Faster
{
    /// <summary>
    /// Provides management of blobs and blob names.
    /// </summary>
    internal class BlobManager : ICheckpointManager, ILogCommitManager
    {
        private readonly string connectionString;
        private readonly string containerName;
        private readonly uint partitionId;

        private CloudBlobContainer blobContainer;
        private CloudBlockBlob eventLogCommitBlob;
        private CloudBlobDirectory partitionDirectory;

        private CancellationTokenSource ownershipCancellation;
        private CancellationTokenSource shutdownCancellation;

        private string LeaseId;
        private TimeSpan LeaseDuration = System.Diagnostics.Debugger.IsAttached ? TimeSpan.FromSeconds(60) : TimeSpan.FromSeconds(35);
        private TimeSpan LeaseRenewal = System.Diagnostics.Debugger.IsAttached ? TimeSpan.FromSeconds(55) : TimeSpan.FromSeconds(30);

        internal TraceHelper TraceHelper { get; private set; }

        public IDevice EventLogDevice { get; private set; }
        public IDevice HybridLogDevice { get; private set; }
        public IDevice ObjectLogDevice { get; private set; }

        public FasterLogSettings EventLogSettings => new FasterLogSettings
        {
            LogDevice = this.EventLogDevice,
            LogCommitManager = UseLocalFilesForTestingAndDebugging ?
                new LocalLogCommitManager($"{this.LocalDirectoryPath}\\{this.PartitionFolder}\\{CommitBlobName}") : (ILogCommitManager)this,
            PageSizeBits = 18, // 128k since we are just writing and often small portions
            SegmentSizeBits = 28,
            MemorySizeBits = 22, // 2MB because 16 pages are the minimum
        };

        public LogSettings StoreLogSettings => new LogSettings
        {
            LogDevice = this.HybridLogDevice,
            ObjectLogDevice = this.ObjectLogDevice,
            PageSizeBits = 22, // 4MB since page blobs can't access more than that in a single op
            MutableFraction = 0.9,
            SegmentSizeBits = 28,
            CopyReadsToTail = true,    
            MemorySizeBits = 27, // 128 MB
        };

        public CheckpointSettings StoreCheckpointSettings => new CheckpointSettings
        {
            CheckpointManager = UseLocalFilesForTestingAndDebugging ?
                new LocalCheckpointManager($"{LocalDirectoryPath}\\chkpts{this.partitionId:D2}") : (ICheckpointManager)this,
            CheckPointType = CheckpointType.FoldOver,
        };

        /// <summary>
        /// Create new instance of local checkpoint manager at given base directory
        /// </summary>
        /// <param name="connectionString">The connection string for the Azure storage account</param>
        /// <param name="taskHubName">The name of the taskhub</param>
        /// <param name="logger">A logger for logging</param>
        /// <param name="partitionId">The partition id</param>
        public BlobManager(string connectionString, string taskHubName, ILogger logger, uint partitionId)
        {
            this.connectionString = connectionString;
            this.containerName = GetContainerName(taskHubName);
            this.partitionId = partitionId;
            this.TraceHelper = new TraceHelper(logger, (int)partitionId);

            if (!UseLocalFilesForTestingAndDebugging)
            {
                CloudStorageAccount account = CloudStorageAccount.Parse(connectionString);
                CloudBlobClient serviceClient = account.CreateCloudBlobClient();
                this.blobContainer = serviceClient.GetContainerReference(containerName);
            }
        }

        // For testing and debugging with local files, this is the single place the directory name is set.
        // This must be called before any BlobManager instances are instantiated.
        internal static void SetLocalFileDirectoryForTestingAndDebugging(bool useLocal) => LocalFileDirectoryForTestingAndDebugging = useLocal ? @"E:\Faster" : null;
        internal static string LocalFileDirectoryForTestingAndDebugging { get; private set; } = null;
        private static bool UseLocalFilesForTestingAndDebugging => !string.IsNullOrEmpty(LocalFileDirectoryForTestingAndDebugging);


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
            if (UseLocalFilesForTestingAndDebugging)
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
                
                var eventLogDevice = new AzureStorageDevice(EventLogBlobName, this.partitionDirectory);
                var hybridLogDevice = new AzureStorageDevice(HybridLogBlobName, this.partitionDirectory);
                var objectLogDevice = new AzureStorageDevice(ObjectLogBlobName, this.partitionDirectory);

                eventLogDevice.ExceptionTracer = (method, e) => this.TraceHelper.FasterBlobStorageError($"{EventLogBlobName}.{method}", e);
                hybridLogDevice.ExceptionTracer = (method, e) => this.TraceHelper.FasterBlobStorageError($"{HybridLogBlobName}.{method}", e);
                objectLogDevice.ExceptionTracer = (method, e) => this.TraceHelper.FasterBlobStorageError($"{ObjectLogBlobName}.{method}", e);

                this.EventLogDevice = eventLogDevice;
                this.HybridLogDevice = hybridLogDevice;
                this.ObjectLogDevice = objectLogDevice;
            }
        }

        public async Task StopAsync()
        {
            this.TraceHelper.LeaseProgress("stopping");
            this.shutdownCancellation?.Cancel();
            await this.LeaseRenewalLoopTask;
        }

        public static async Task DeleteTaskhubStorageAsync(string connectionString, string taskHubName)
        {
            var containerName = GetContainerName(taskHubName);

            if (UseLocalFilesForTestingAndDebugging)
            {
                System.IO.DirectoryInfo di = new DirectoryInfo($"{LocalFileDirectoryForTestingAndDebugging}\\{containerName}");
                if (di.Exists)
                {
                    di.Delete(true);
                }
            }
            else
            {
                CloudStorageAccount account = CloudStorageAccount.Parse(connectionString);
                CloudBlobClient serviceClient = account.CreateCloudBlobClient();
                var blobContainer = serviceClient.GetContainerReference(containerName);

                if (await blobContainer.ExistsAsync())
                {
                    var allBlobsInContainer = blobContainer.ListBlobs(null, true).ToList();
                    Parallel.ForEach(allBlobsInContainer, async (IListBlobItem blob) =>
                    {
                        if (blob.GetType() == typeof(CloudBlob) || blob.GetType().BaseType == typeof(CloudBlob))
                        {
                            await ((CloudBlob)blob).DeleteIfExistsAsync();
                        }
                    });
                }

                // we are not deleting the container itself because it creates problems
                // when trying to recreate the same container soon afterwards
                // so we leave an empty container behind. Oh well.
            }
        }

        public async Task<CancellationToken> AcquireOwnership(CancellationToken token)
        {
            // ownership is cancelled if a lease is lost and cannot be renewed due to conflict
            this.ownershipCancellation = new CancellationTokenSource();

            // shutdown can be triggered from three sources
            // - before this method even completes, via token
            // - implicitly if ownership is lost, after it is acquired
            // - if shutdown is called explicitly at the end
            this.shutdownCancellation = CancellationTokenSource.CreateLinkedTokenSource(this.ownershipCancellation.Token, token);

            if (UseLocalFilesForTestingAndDebugging)
            {
                // No-op for simple testing scenarios.
                return CancellationToken.None;
            }
            else
            {
                while (!this.shutdownCancellation.IsCancellationRequested)
                {
                    try
                    {
                        this.LeaseId = await this.eventLogCommitBlob.AcquireLeaseAsync(LeaseDuration, null);
                        break;
                    }
                    catch (StorageException ex) when (LeaseConflictOrExpired(ex))
                    {
                        // the previous owner has not released the lease yet, 
                        // try again until it becomes available, should be relatively soon
                        this.TraceHelper.LeaseProgress("waiting for lease");
                        await Task.Delay(TimeSpan.FromSeconds(1), this.shutdownCancellation.Token);
                        continue;
                    }
                    catch (StorageException ex) when (BlobDoesNotExist(ex))
                    {
                        try
                        {
                            // Create blob with empty content, then try again
                            this.TraceHelper.LeaseProgress("creating commit blob");
                            await this.eventLogCommitBlob.UploadFromByteArrayAsync(Array.Empty<byte>(), 0, 0);
                            continue;
                        }
                        catch (StorageException ex2) when (LeaseConflictOrExpired(ex2))
                        {
                            // creation race, try from top
                            this.TraceHelper.LeaseProgress("creation race, retrying");
                            continue;
                        }
                    }
                }

                // start background loop that renews the lease continuously
                this.LeaseRenewalLoopTask = this.LeaseRenewalLoopAsync();

                this.TraceHelper.LeaseAcquired();

                return this.ownershipCancellation.Token;
            }
        }

        public async Task LeaseRenewalLoopAsync()
        {
            try
            {
                while (!this.shutdownCancellation.Token.IsCancellationRequested)
                {
                    this.NextLeaseRenewalTask = RenewLeaseTask();
                    await this.NextLeaseRenewalTask;
                }
            }
            catch (OperationCanceledException)
            {
                // We get here as part of normal termination after shutdownCancellation was triggered. 
            }

            this.TraceHelper.LeaseProgress("shutting down");

            // if we haven't already lost ownership, try to cancel the lease now
            if (!ownershipCancellation.IsCancellationRequested)
            {
                AccessCondition acc = new AccessCondition() { LeaseId = this.LeaseId };
                try
                {
                    await this.eventLogCommitBlob.ReleaseLeaseAsync(acc);
                    this.TraceHelper.LeaseReleased();
                }
                catch (Exception e) // swallow exceptions when releasing a lease
                {
                    this.TraceHelper.FasterBlobStorageError("release", e);
                }
            }
        }

        public async Task RenewLeaseTask()
        {
            await Task.Delay(this.LeaseRenewal, this.shutdownCancellation.Token);

            AccessCondition acc = new AccessCondition() { LeaseId = this.LeaseId };

            this.TraceHelper.LeaseProgress("renewing lease");

            try
            {
                await this.eventLogCommitBlob.RenewLeaseAsync(acc, this.shutdownCancellation.Token);
                this.TraceHelper.LeaseProgress("renewed lease");
            }
            catch (OperationCanceledException)
            {
                // we are shutting down
            }
            catch (StorageException ex) when (LeaseConflict(ex))
            {
                // We lost the lease to someone else. Terminate ownership immediately.
                this.TraceHelper.LeaseLost("renew");
                this.ownershipCancellation.Cancel();
            }
            catch (Exception e)
            {
                this.TraceHelper.FasterBlobStorageError("renew", e);
                // continue trying to renew at normal intervals, to survive temporary storage unavailability
            }
        }

        private static bool LeaseConflictOrExpired(StorageException e)
        {
            return (e.RequestInformation.HttpStatusCode == 409) || (e.RequestInformation.HttpStatusCode == 412);
        }

        private static bool LeaseConflict(StorageException e)
        {
            return (e.RequestInformation.HttpStatusCode == 409);
        }

        private static bool LeaseExpired(StorageException e)
        {
            return (e.RequestInformation.HttpStatusCode == 412);
        }

        private static bool BlobDoesNotExist(StorageException e)
        {
            var information = e.RequestInformation.ExtendedErrorInformation;
            return (e.RequestInformation.HttpStatusCode == 404) && (information.ErrorCode.Equals(BlobErrorCodeStrings.BlobNotFound));
        }

        #region ILogCommitManager

        void ILogCommitManager.Commit(long beginAddress, long untilAddress, byte[] commitMetadata)
        {
            while (true)
            {
                AccessCondition acc = new AccessCondition() { LeaseId = this.LeaseId };
                try
                {
                    this.eventLogCommitBlob.UploadFromByteArray(commitMetadata, 0, commitMetadata.Length, acc);
                    return;
                }
                catch (StorageException ex) when (LeaseExpired(ex))
                {
                    // if we get here, the lease renewal task did not complete in time
                    // wait for it to complete or throw
                    this.NextLeaseRenewalTask.Wait();
                    continue;
                }
                catch (StorageException ex) when (LeaseConflict(ex))
                {
                    // We lost the lease to someone else. Terminate ownership immediately.
                    this.TraceHelper.LeaseLost(nameof(ILogCommitManager.Commit));
                    this.ownershipCancellation.Cancel();
                    throw;
                }
                catch (Exception e)
                {
                    this.TraceHelper.FasterBlobStorageError(nameof(ILogCommitManager.Commit), e);
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
                        this.eventLogCommitBlob.DownloadToStream(stream, acc);
                        var bytes = stream.ToArray();
                        return bytes.Length == 0 ? null : bytes;
                    }
                }
                catch (StorageException ex) when (LeaseExpired(ex))
                {
                    // if we get here, the lease renewal task did not complete in time
                    // wait for it to complete or throw
                    this.NextLeaseRenewalTask.Wait();
                    continue;
                }
                catch (StorageException ex) when (LeaseConflict(ex))
                {
                    // We lost the lease to someone else. Terminate ownership immediately.
                    this.TraceHelper.LeaseLost(nameof(ILogCommitManager.GetCommitMetadata));
                    this.ownershipCancellation.Cancel();
                    throw;
                }
                catch (Exception e)
                {
                    this.TraceHelper.FasterBlobStorageError(nameof(ILogCommitManager.GetCommitMetadata), e);
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
            try
            {
                var metaFileBlob = this.partitionDirectory.GetBlockBlobReference(this.GetIndexCheckpointMetaBlob(indexToken));
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
                completedFileBlob.UploadText(indexToken.ToString());
            }
            catch (Exception e)
            {
                this.TraceHelper.FasterBlobStorageError(nameof(ICheckpointManager.CommitIndexCheckpoint), e);
                throw;
            }
        }

        void ICheckpointManager.CommitLogCheckpoint(Guid logToken, byte[] commitMetadata)
        {
            try
            {
                var metaFileBlob = this.partitionDirectory.GetBlockBlobReference(this.GetHybridLogCheckpointMetaBlob(logToken));
                using (var blobStream = metaFileBlob.OpenWrite())
                {
                    using (var writer = new BinaryWriter(blobStream))
                    {
                        writer.Write(commitMetadata.Length);
                        writer.Write(commitMetadata);
                        writer.Flush();
                    }
                }

                var completedFileBlob = this.partitionDirectory.GetBlockBlobReference(this.GetHybridLogCheckpointCompletedBlob());
                completedFileBlob.UploadText(logToken.ToString());
            }
            catch (Exception e)
            {
                this.TraceHelper.FasterBlobStorageError(nameof(ICheckpointManager.CommitLogCheckpoint), e);
                throw;
            }
        }

        byte[] ICheckpointManager.GetIndexCommitMetadata(Guid indexToken)
        {
            try
            {
                var metaFileBlob = this.partitionDirectory.GetBlockBlobReference(this.GetIndexCheckpointMetaBlob(indexToken));
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
                this.TraceHelper.FasterBlobStorageError(nameof(ICheckpointManager.GetIndexCommitMetadata), e);
                throw;
            }
        }

        byte[] ICheckpointManager.GetLogCommitMetadata(Guid logToken)
        {
            try
            {
                var metaFileBlob = this.partitionDirectory.GetBlockBlobReference(this.GetHybridLogCheckpointMetaBlob(logToken));
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
                this.TraceHelper.FasterBlobStorageError(nameof(ICheckpointManager.GetLogCommitMetadata), e);
                throw;
            }
        }

        IDevice ICheckpointManager.GetIndexDevice(Guid indexToken)
        {
            try
            {
                var (path, blobName) = this.GetPrimaryHashTableBlob(indexToken);
                var blobDirectory = this.partitionDirectory.GetDirectoryReference(path);
                var device = new AzureStorageDevice(blobName, blobDirectory);
                device.ExceptionTracer = (method, e) => this.TraceHelper.FasterBlobStorageError($"indexDevice.{method}", e);
                return device;
            }
            catch (Exception e)
            {
                this.TraceHelper.FasterBlobStorageError(nameof(ICheckpointManager.GetIndexDevice), e);
                throw;
            }
        }

        IDevice ICheckpointManager.GetSnapshotLogDevice(Guid token)
        {
            try
            {
                var (path, blobName) = this.GetLogSnapshotBlob(token);
                var blobDirectory = this.partitionDirectory.GetDirectoryReference(path);
                var device = new AzureStorageDevice(blobName, blobDirectory);
                device.ExceptionTracer = (method, e) => this.TraceHelper.FasterBlobStorageError($"snapshotLogDevice.{method}", e);
                return device;
            }
            catch (Exception e)
            {
                this.TraceHelper.FasterBlobStorageError(nameof(ICheckpointManager.GetSnapshotLogDevice), e);
                throw;
            }
        }

        IDevice ICheckpointManager.GetSnapshotObjectLogDevice(Guid token)
        {
            try
            {
                var (path, blobName) = this.GetObjectLogSnapshotBlob(token);
                var blobDirectory = this.partitionDirectory.GetDirectoryReference(path);
                var device = new AzureStorageDevice(blobName, blobDirectory);
                device.ExceptionTracer = (method, e) => this.TraceHelper.FasterBlobStorageError($"snapshotObjectLogDevice.{method}", e);
                return device;
            }
            catch (Exception e)
            {
                this.TraceHelper.FasterBlobStorageError(nameof(ICheckpointManager.GetSnapshotObjectLogDevice), e);
                throw;
            }
        }

        bool ICheckpointManager.GetLatestCheckpoint(out Guid indexToken, out Guid logToken)
        {
            try
            {
                var indexCompletedFileBlob = this.partitionDirectory.GetBlockBlobReference(this.GetIndexCheckpointCompletedBlob());
                var logCompletedFileBlob = this.partitionDirectory.GetBlockBlobReference(this.GetHybridLogCheckpointCompletedBlob());

                if (indexCompletedFileBlob.Exists() && logCompletedFileBlob.Exists())
                {
                    var lastIndexCheckpoint = indexCompletedFileBlob.DownloadText();
                    indexToken = Guid.Parse(lastIndexCheckpoint);

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
                this.TraceHelper.FasterBlobStorageError(nameof(ICheckpointManager.GetLatestCheckpoint), e);
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