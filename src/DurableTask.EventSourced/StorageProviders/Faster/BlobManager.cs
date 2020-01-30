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
using Microsoft.Azure.Services.AppAuthentication;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.Storage.Blob.Protocol;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
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

        private CancellationTokenSource ownershipCancellation;
        private CancellationTokenSource shutdownCancellation;
        private CancellationToken cancellationToken;

        private string LeaseId;
        private TimeSpan LeaseDuration = System.Diagnostics.Debugger.IsAttached ? TimeSpan.FromSeconds(60) : TimeSpan.FromSeconds(20);
        private TimeSpan LeaseRenewal = System.Diagnostics.Debugger.IsAttached ? TimeSpan.FromSeconds(50) : TimeSpan.FromSeconds(15);

        public IDevice EventLogDevice { get; private set; }
        public IDevice HybridLogDevice { get; private set; }
        public IDevice ObjectLogDevice { get; private set; }

        /// <summary>
        /// Create new instance of local checkpoint manager at given base directory
        /// </summary>
        /// <param name="connectionString">The connection string for the Azure storage account</param>
        /// <param name="taskHubName">The name of the taskhub</param>
        /// <param name="partitionId">The partition id</param>
        public BlobManager(string connectionString, string taskHubName, uint partitionId)
        {
            this.connectionString = connectionString;
            this.containerName = taskHubName.ToLowerInvariant() + "-data";
            this.partitionId = partitionId;

            CloudStorageAccount account = CloudStorageAccount.Parse(connectionString);
            CloudBlobClient serviceClient = account.CreateCloudBlobClient();
            this.blobContainer = serviceClient.GetContainerReference(containerName);
        }

        internal bool UseLocalFilesForTestingAndDebugging { get; set; }

        private string LocalDirectoryPath => $"C:\\faster\\{this.containerName}";
        private string SnapshotFolder => $"partition{this.partitionId:D2}";
        private string EventLogBlobName => $"events{this.partitionId:D2}.log";
        private string HybridLogBlobName => $"store{this.partitionId:D2}.log";
        private string ObjectLogBlobName => $"store{this.partitionId:D2}.obj.log";

        private Task LeaseRenewalLoopTask = Task.CompletedTask;

        public async Task StartAsync()
        {
            if (!UseLocalFilesForTestingAndDebugging)
            {
                await this.blobContainer.CreateIfNotExistsAsync();
            }

            this.eventLogCommitBlob = this.blobContainer.GetBlockBlobReference(this.EventLogBlobName + ".commit");

            if (UseLocalFilesForTestingAndDebugging)
            {
                Directory.CreateDirectory(LocalDirectoryPath);
                this.EventLogDevice = Devices.CreateLogDevice($"{LocalDirectoryPath}\\{this.EventLogBlobName}");
                this.HybridLogDevice = Devices.CreateLogDevice($"{LocalDirectoryPath}\\{this.HybridLogBlobName}");
                this.ObjectLogDevice = Devices.CreateLogDevice($"{LocalDirectoryPath}\\{this.ObjectLogBlobName}");
            }
            else
            {
                this.EventLogDevice = new AzureStorageDevice(connectionString, containerName, this.EventLogBlobName);
                this.HybridLogDevice = new AzureStorageDevice(connectionString, containerName, this.HybridLogBlobName);
                this.ObjectLogDevice = new AzureStorageDevice(connectionString, containerName, this.ObjectLogBlobName);
            }
        }

        public async Task StopAsync()
        {
            this.shutdownCancellation.Cancel();
            await this.LeaseRenewalLoopTask;
        }

        private static bool ConflictOrExpiredLease(StorageException e)
        {
            return (e.RequestInformation.HttpStatusCode == 409) || (e.RequestInformation.HttpStatusCode == 412);
        }
    
        private static bool BlobDoesNotExist(StorageException e)
        {
            var information = e.RequestInformation.ExtendedErrorInformation;
            return (e.RequestInformation.HttpStatusCode == 404) && (information.ErrorCode.Equals(BlobErrorCodeStrings.BlobNotFound));
        }

        public async Task<CancellationToken> AcquireOwnership(CancellationToken token)
        {
            this.ownershipCancellation = new CancellationTokenSource();
            this.shutdownCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, ownershipCancellation.Token);
            this.cancellationToken = shutdownCancellation.Token;

            if (UseLocalFilesForTestingAndDebugging)
            {
                // no leases. Uses default commit manager.
                return CancellationToken.None;
            }

            while (!this.cancellationToken.IsCancellationRequested)
            {
                try
                {
                    this.LeaseId = await this.eventLogCommitBlob.AcquireLeaseAsync(LeaseDuration, null);
                    break;
                }
                catch (StorageException ex) when (ConflictOrExpiredLease(ex))
                {
                    // the previous owner has not released the lease yet, 
                    // try again until it becomes available, should be relatively soon
                    await Task.Delay(TimeSpan.FromSeconds(1));
                    continue;
                }
                catch (StorageException ex) when (BlobDoesNotExist(ex))
                {
                    try
                    {
                        // Create blob with empty content, then try again
                        await this.eventLogCommitBlob.UploadFromByteArrayAsync(Array.Empty<byte>(), 0, 0);
                        continue;
                    }
                    catch (StorageException ex2) when (ConflictOrExpiredLease(ex2))
                    {
                        // creation race, try from top
                        continue;
                    }
                }
            }

            // start background loop that renews the lease continuously
            this.LeaseRenewalLoopTask = this.LeaseRenewalLoopAsync();

            return this.ownershipCancellation.Token;
        }

        public async Task LeaseRenewalLoopAsync()
        {
            while (!this.cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(this.LeaseRenewal, this.cancellationToken);

                    AccessCondition acc = new AccessCondition() { LeaseId = this.LeaseId };

                    await this.eventLogCommitBlob.RenewLeaseAsync(acc, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch
                {
                    // for whatever reason we were unable to renew the lease
                    this.ownershipCancellation.Cancel();
                }
            }

            // if we haven't already lost the lease, release it now
            if (!ownershipCancellation.IsCancellationRequested)
            {
                AccessCondition acc = new AccessCondition() { LeaseId = this.LeaseId };
                try
                {
                    await this.eventLogCommitBlob.ReleaseLeaseAsync(acc);
                }
                catch // swallow exceptions when releasing a lease
                {
                }
            }
        }

        public async Task DeleteTaskhubDataAsync()
        {
            if (UseLocalFilesForTestingAndDebugging)
            {
                System.IO.DirectoryInfo di = new DirectoryInfo(LocalDirectoryPath);
                if (di.Exists)
                {
                    di.Delete(true);
                }
            }
            else
            {
                if (await this.blobContainer.ExistsAsync())
                {
                    foreach (IListBlobItem blob in this.blobContainer.ListBlobs())
                    {
                        if (blob.GetType() == typeof(CloudBlob) || blob.GetType().BaseType == typeof(CloudBlob))
                        {
                            await ((CloudBlob)blob).DeleteIfExistsAsync();
                        }
                    }
                }
                // we are not deleting the container itself because it creates problems
                // when trying to recreate the same container soon afterwards
            }
        }

        #region ILogCommitManager

        void ILogCommitManager.Commit(long beginAddress, long untilAddress, byte[] commitMetadata)
        {
            AccessCondition acc = new AccessCondition() { LeaseId = this.LeaseId };
            try
            {
                this.eventLogCommitBlob.UploadFromByteArray(commitMetadata, 0, commitMetadata.Length, acc);
            }
            catch (StorageException ex) when (ConflictOrExpiredLease(ex))
            {
                this.ownershipCancellation.Cancel();
                throw;
            }
        }

        byte[] ILogCommitManager.GetCommitMetadata()
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
            catch (StorageException ex) when (ConflictOrExpiredLease(ex))
            {
                this.ownershipCancellation.Cancel();
                throw;
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
            var metaFileBlob = this.blobContainer.GetBlockBlobReference(this.GetIndexCheckpointMetaFileName(indexToken));
            using (var blobStream = metaFileBlob.OpenWrite())
            {
                using (var writer = new BinaryWriter(blobStream))
                {
                    writer.Write(commitMetadata.Length);
                    writer.Write(commitMetadata);
                    writer.Flush();
                }
            }

            var completedFileBlob = this.blobContainer.GetBlockBlobReference(this.GetIndexCheckpointCompletedFileName());
            completedFileBlob.UploadText(indexToken.ToString());
        }

        void ICheckpointManager.CommitLogCheckpoint(Guid logToken, byte[] commitMetadata)
        {
            var metaFileBlob = this.blobContainer.GetBlockBlobReference(this.GetHybridLogCheckpointMetaFileName(logToken));
            using (var blobStream = metaFileBlob.OpenWrite())
            {
                using (var writer = new BinaryWriter(blobStream))
                {
                    writer.Write(commitMetadata.Length);
                    writer.Write(commitMetadata);
                    writer.Flush();
                }
            }

            var completedFileBlob = this.blobContainer.GetBlockBlobReference(this.GetHybridLogCheckpointCompletedFileName());
            completedFileBlob.UploadText(logToken.ToString());
        }

        byte[] ICheckpointManager.GetIndexCommitMetadata(Guid indexToken)
        {
            var metaFileBlob = this.blobContainer.GetBlockBlobReference(this.GetIndexCheckpointMetaFileName(indexToken));
            using (var blobstream = metaFileBlob.OpenRead())
            {
                using (var reader = new BinaryReader(blobstream))
                {
                    var len = reader.ReadInt32();
                    return reader.ReadBytes(len);
                }
            }
        }

        byte[] ICheckpointManager.GetLogCommitMetadata(Guid logToken)
        {
            var metaFileBlob = this.blobContainer.GetBlockBlobReference(this.GetHybridLogCheckpointMetaFileName(logToken));
            using (var blobstream = metaFileBlob.OpenRead())
            {
                using (var reader = new BinaryReader(blobstream))
                {
                    var len = reader.ReadInt32();
                    return reader.ReadBytes(len);
                }
            }
        }

        IDevice ICheckpointManager.GetIndexDevice(Guid indexToken)
        {
            return new AzureStorageDevice(this.connectionString, blobContainer.Name, this.GetPrimaryHashTableFileName(indexToken));
        }

        IDevice ICheckpointManager.GetSnapshotLogDevice(Guid token)
        {
            return new AzureStorageDevice(this.connectionString, blobContainer.Name, this.GetLogSnapshotFileName(token));
        }

        IDevice ICheckpointManager.GetSnapshotObjectLogDevice(Guid token)
        {
            return new AzureStorageDevice(this.connectionString, blobContainer.Name, this.GetObjectLogSnapshotFileName(token));
        }

        bool ICheckpointManager.GetLatestCheckpoint(out Guid indexToken, out Guid logToken)
        {
            return false;

            //var indexCompletedFileBlob = this.blobContainer.GetBlockBlobReference(this.GetIndexCheckpointCompletedFileName());
            //var logCompletedFileBlob = this.blobContainer.GetBlockBlobReference(this.GetHybridLogCheckpointCompletedFileName());

            //if (indexCompletedFileBlob.Exists() && logCompletedFileBlob.Exists())
            //{
            //    var lastIndexCheckpoint = indexCompletedFileBlob.DownloadText();
            //    indexToken = Guid.Parse(lastIndexCheckpoint);

            //    var lastLogCheckpoint = logCompletedFileBlob.DownloadText();
            //    logToken = Guid.Parse(lastLogCheckpoint);

            //    return true;
            //}
            //else
            //{
            //    return false;
            //}
        }

        #endregion

        #region Blob Name Management

        private const string index_base_folder = "index-checkpoints";
        private const string index_meta_file = "info";
        private const string index_completed_file = "index-last-checkpoint";
        private const string hash_table_file = "ht";
        private const string overflow_buckets_file = "ofb";
        private const string snapshot_file = "snapshot";

        private const string cpr_base_folder = "cpr-checkpoints";
        private const string cpr_meta_file = "info";
        private const string cpr_completed_file = "cpr-last-checkpoint";

        private string GetIndexCheckpointFolderName(Guid token)
        {
            return GetMergedFolderPath(this.SnapshotFolder,
                                    index_base_folder,
                                    token.ToString());
        }

        private string GetIndexCheckpointMetaFileName(Guid token)
        {
            return GetMergedFolderPath(this.SnapshotFolder,
                                    index_base_folder,
                                    token.ToString(),
                                    index_meta_file,
                                    ".dat");
        }

        private string GetIndexCheckpointCompletedFileName()
        {
            return GetMergedFolderPath(this.SnapshotFolder,
                                    index_base_folder,
                                    index_completed_file,
                                    ".txt");
        }

        private string GetPrimaryHashTableFileName(Guid token)
        {
            return GetMergedFolderPath(this.SnapshotFolder,
                                    index_base_folder,
                                    token.ToString(),
                                    hash_table_file,
                                    ".dat");
        }

        private string GetOverflowBucketsFileName(Guid token)
        {
            return GetMergedFolderPath(this.SnapshotFolder,
                                    index_base_folder,
                                    token.ToString(),
                                    overflow_buckets_file,
                                    ".dat");
        }

        private string GetHybridLogCheckpointMetaFileName(Guid token)
        {
            return GetMergedFolderPath(this.SnapshotFolder,
                                    cpr_base_folder,
                                    token.ToString(),
                                    cpr_meta_file,
                                    ".dat");
        }

        private string GetHybridLogCheckpointCompletedFileName()
        {
            return GetMergedFolderPath(this.SnapshotFolder,
                                    cpr_base_folder,
                                    cpr_completed_file,
                                    ".txt");
        }

        private string GetHybridLogCheckpointContextFileName(Guid checkpointToken, Guid sessionToken)
        {
            return GetMergedFolderPath(this.SnapshotFolder,
                                    cpr_base_folder,
                                    checkpointToken.ToString(),
                                    sessionToken.ToString(),
                                    ".dat");
        }

        private string GetLogSnapshotFileName(Guid token)
        {
            return GetMergedFolderPath(this.SnapshotFolder, cpr_base_folder, token.ToString(), snapshot_file, ".dat");
        }

        private string GetObjectLogSnapshotFileName(Guid token)
        {
            return GetMergedFolderPath(this.SnapshotFolder, cpr_base_folder, token.ToString(), snapshot_file, ".obj.dat");
        }

        private static string GetMergedFolderPath(params String[] paths)
        {
            String fullPath = paths[0];

            for (int i = 1; i < paths.Length; i++)
            {
                if (i == paths.Length - 1 && paths[i].Contains("."))
                {
                    fullPath += paths[i];
                }
                else
                {
                    fullPath += '/' + paths[i];
                }
            }

            return fullPath;
        }

        #endregion
    }
}