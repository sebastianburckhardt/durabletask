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
        private string snapshotsfolder;
        private CloudPageBlob eventLogPageBlob;

        public IDevice EventLogDevice { get; private set; }
        public IDevice HybridLogDevice { get; private set; }
        public IDevice ObjectLogDevice { get; private set; }

        /// <summary>
        /// Create new instance of local checkpoint manager at given base directory
        /// </summary>
        /// <param name="connectionString">The connection string for the Azure storage account</param>
        /// <param name="containerName">The blob container for storing the hybrid log and the checkpoints</param>
        /// <param name="partitionId">The partition id</param>
        public BlobManager(string connectionString, string containerName, uint partitionId)
        {
            this.connectionString = connectionString;
            this.containerName = containerName;
            this.partitionId = partitionId;
        }

        public async Task StartAsync()
        { 
            CloudStorageAccount account = CloudStorageAccount.Parse(connectionString);
            CloudBlobClient serviceClient = account.CreateCloudBlobClient();
            this.blobContainer = serviceClient.GetContainerReference(containerName);
            await this.blobContainer.CreateIfNotExistsAsync();

            this.snapshotsfolder = $"partition{partitionId:D2}";

            var eventLogBlobName = $"events{partitionId:D2}.log";
            var hybridLogBlobName = $"store{partitionId:D2}.log";
            var objectLogBlobName = $"store{partitionId:D2}.obj.log";

            this.eventLogPageBlob = this.blobContainer.GetPageBlobReference(eventLogBlobName + "0");

            this.EventLogDevice = new AzureStorageDevice(connectionString, containerName, eventLogBlobName);
            this.HybridLogDevice = new AzureStorageDevice(connectionString, containerName, hybridLogBlobName);
            this.ObjectLogDevice = new AzureStorageDevice(connectionString, containerName, objectLogBlobName);
        }


        #region ILogCommitManager

        void ILogCommitManager.Commit(long beginAddress, long untilAddress, byte[] commitMetadata)
        {
            int writeSize = sizeof(int) + commitMetadata.Length;
            // Writes to PageBlob must be aligned at 512 boundaries, we need to therefore pad up to the closest
            // multiple of 512 for the write buffer size.
            int mask = BlobUtil.PAGE_BLOB_SECTOR_SIZE - 1;
            byte[] alignedByteChunk = new byte[(writeSize + mask) & ~mask];

            Array.Copy(BitConverter.GetBytes(commitMetadata.Length), alignedByteChunk, sizeof(int));
            Array.Copy(commitMetadata, 0, alignedByteChunk, sizeof(int), commitMetadata.Length);

            // TODO(Tianyu): We assume this operation is atomic I guess?
            eventLogPageBlob.WritePages(new MemoryStream(alignedByteChunk), 0);
        }

        byte[] ILogCommitManager.GetCommitMetadata()
        {
            try
            {
                return BlobUtil.ReadMetadataFile(eventLogPageBlob);
            }
            catch(StorageException e) 
            {
                if (e.RequestInformation.HttpStatusCode == (int) HttpStatusCode.NotFound)
                {
                    // the page blob does not exist yet
                    return null; 
                }

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
            return GetMergedFolderPath(snapshotsfolder,
                                    index_base_folder,
                                    token.ToString());
        }

        private string GetIndexCheckpointMetaFileName(Guid token)
        {
            return GetMergedFolderPath(snapshotsfolder,
                                    index_base_folder,
                                    token.ToString(),
                                    index_meta_file,
                                    ".dat");
        }

        private string GetIndexCheckpointCompletedFileName()
        {
            return GetMergedFolderPath(snapshotsfolder,
                                    index_base_folder,
                                    index_completed_file,
                                    ".txt");
        }

        private string GetPrimaryHashTableFileName(Guid token)
        {
            return GetMergedFolderPath(snapshotsfolder,
                                    index_base_folder,
                                    token.ToString(),
                                    hash_table_file,
                                    ".dat");
        }

        private string GetOverflowBucketsFileName(Guid token)
        {
            return GetMergedFolderPath(snapshotsfolder,
                                    index_base_folder,
                                    token.ToString(),
                                    overflow_buckets_file,
                                    ".dat");
        }

        private string GetHybridLogCheckpointMetaFileName(Guid token)
        {
            return GetMergedFolderPath(snapshotsfolder,
                                    cpr_base_folder,
                                    token.ToString(),
                                    cpr_meta_file,
                                    ".dat");
        }

        private string GetHybridLogCheckpointCompletedFileName()
        {
            return GetMergedFolderPath(snapshotsfolder,
                                    cpr_base_folder,
                                    cpr_completed_file,
                                    ".txt");
        }

        private string GetHybridLogCheckpointContextFileName(Guid checkpointToken, Guid sessionToken)
        {
            return GetMergedFolderPath(snapshotsfolder,
                                    cpr_base_folder,
                                    checkpointToken.ToString(),
                                    sessionToken.ToString(),
                                    ".dat");
        }

        private string GetLogSnapshotFileName(Guid token)
        {
            return GetMergedFolderPath(snapshotsfolder, cpr_base_folder, token.ToString(), snapshot_file, ".dat");
        }

        private string GetObjectLogSnapshotFileName(Guid token)
        {
            return GetMergedFolderPath(snapshotsfolder, cpr_base_folder, token.ToString(), snapshot_file, ".obj.dat");
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