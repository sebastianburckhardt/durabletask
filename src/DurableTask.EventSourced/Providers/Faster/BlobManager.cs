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
using System.Runtime.CompilerServices;
using System.Threading;

namespace DurableTask.EventSourced.Faster
{
    /// <summary>
    /// Provides management of blobs and blob names.
    /// </summary>
    internal class BlobManager : ICheckpointManager
    {
        private string connectionString;
        private CloudBlobContainer blobContainer;
        private string baseFolder;

        public IDevice HybridLogDevice { get; }
        public IDevice ObjectLogDevice { get; }

        /// <summary>
        /// Create new instance of local checkpoint manager at given base directory
        /// </summary>
        /// <param name="connectionString">The connection string for the Azure storage account</param>
        /// <param name="containerName">The blob container for storing the hybrid log and the checkpoints</param>
        /// <param name="partitionId">The partition id</param>
        public BlobManager(string connectionString, string containerName, uint partitionId)
        {
            this.connectionString = connectionString;
            CloudStorageAccount account = CloudStorageAccount.Parse(connectionString);
            CloudBlobClient serviceClient = account.CreateCloudBlobClient();
            this.blobContainer = serviceClient.GetContainerReference(containerName);
            this.blobContainer.CreateIfNotExists();
            this.baseFolder = $"partition{partitionId:D2}";

            this.HybridLogDevice = new AzureStorageDevice(connectionString, containerName, $"partition{partitionId:D2}.log");
            this.ObjectLogDevice = new AzureStorageDevice(connectionString, containerName, $"partition{partitionId:D2}.obj.log");
        }

        /// <summary>
        /// Initialize index checkpoint
        /// </summary>
        /// <param name="indexToken"></param>
        public void InitializeIndexCheckpoint(Guid indexToken)
        {
            // there is no need to create an empty directory in a blob container
        }

        /// <summary>
        /// Initialize log checkpoint (snapshot and fold-over)
        /// </summary>
        /// <param name="logToken"></param>
        public void InitializeLogCheckpoint(Guid logToken)
        {
            // there is no need to create an empty directory in a blob container
        }

        /// <summary>
        /// Commit index checkpoint
        /// </summary>
        /// <param name="indexToken"></param>
        /// <param name="commitMetadata"></param>
        public void CommitIndexCheckpoint(Guid indexToken, byte[] commitMetadata)
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

        /// <summary>
        /// Commit log checkpoint (snapshot and fold-over)
        /// </summary>
        /// <param name="logToken"></param>
        /// <param name="commitMetadata"></param>
        public void CommitLogCheckpoint(Guid logToken, byte[] commitMetadata)
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

        /// <summary>
        /// Retrieve commit metadata for specified index checkpoint
        /// </summary>
        /// <param name="indexToken">Token</param>
        /// <returns>Metadata, or null if invalid</returns>
        public byte[] GetIndexCommitMetadata(Guid indexToken)
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

        /// <summary>
        /// Retrieve commit metadata for specified log checkpoint
        /// </summary>
        /// <param name="logToken">Token</param>
        /// <returns>Metadata, or null if invalid</returns>
        public byte[] GetLogCommitMetadata(Guid logToken)
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

        /// <summary>
        /// Provide device to store index checkpoint (including overflow buckets)
        /// </summary>
        /// <param name="indexToken"></param>
        /// <returns></returns>
        public IDevice GetIndexDevice(Guid indexToken)
        {
            return new AzureStorageDevice(this.connectionString, blobContainer.Name, this.GetPrimaryHashTableFileName(indexToken));
        }

        /// <summary>
        /// Provide device to store snapshot of log (required only for snapshot checkpoints)
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        public IDevice GetSnapshotLogDevice(Guid token)
        {
            return new AzureStorageDevice(this.connectionString, blobContainer.Name, this.GetLogSnapshotFileName(token));
        }

        /// <summary>
        /// Provide device to store snapshot of object log (required only for snapshot checkpoints)
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        public IDevice GetSnapshotObjectLogDevice(Guid token)
        {
            return new AzureStorageDevice(this.connectionString, blobContainer.Name, this.GetObjectLogSnapshotFileName(token));
        }

        /// <summary>
        /// Get latest valid checkpoint for recovery
        /// </summary>
        /// <param name="indexToken"></param>
        /// <param name="logToken"></param>
        /// <returns></returns>
        public bool GetLatestCheckpoint(out Guid indexToken, out Guid logToken)
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
            return GetMergedFolderPath(baseFolder,
                                    index_base_folder,
                                    token.ToString());
        }

        private string GetIndexCheckpointMetaFileName(Guid token)
        {
            return GetMergedFolderPath(baseFolder,
                                    index_base_folder,
                                    token.ToString(),
                                    index_meta_file,
                                    ".dat");
        }

        private string GetIndexCheckpointCompletedFileName()
        {
            return GetMergedFolderPath(baseFolder,
                                    index_base_folder,
                                    index_completed_file,
                                    ".txt");
        }

        private string GetPrimaryHashTableFileName(Guid token)
        {
            return GetMergedFolderPath(baseFolder,
                                    index_base_folder,
                                    token.ToString(),
                                    hash_table_file,
                                    ".dat");
        }

        private string GetOverflowBucketsFileName(Guid token)
        {
            return GetMergedFolderPath(baseFolder,
                                    index_base_folder,
                                    token.ToString(),
                                    overflow_buckets_file,
                                    ".dat");
        }

        private string GetHybridLogCheckpointMetaFileName(Guid token)
        {
            return GetMergedFolderPath(baseFolder,
                                    cpr_base_folder,
                                    token.ToString(),
                                    cpr_meta_file,
                                    ".dat");
        }

        private string GetHybridLogCheckpointCompletedFileName()
        {
            return GetMergedFolderPath(baseFolder,
                                    cpr_base_folder,
                                    cpr_completed_file,
                                    ".txt");
        }

        private string GetHybridLogCheckpointContextFileName(Guid checkpointToken, Guid sessionToken)
        {
            return GetMergedFolderPath(baseFolder,
                                    cpr_base_folder,
                                    checkpointToken.ToString(),
                                    sessionToken.ToString(),
                                    ".dat");
        }

        private string GetLogSnapshotFileName(Guid token)
        {
            return GetMergedFolderPath(baseFolder, cpr_base_folder, token.ToString(), snapshot_file, ".dat");
        }

        private string GetObjectLogSnapshotFileName(Guid token)
        {
            return GetMergedFolderPath(baseFolder, cpr_base_folder, token.ToString(), snapshot_file, ".obj.dat");
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
    }
}