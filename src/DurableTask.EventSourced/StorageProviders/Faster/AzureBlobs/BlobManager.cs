﻿//  ----------------------------------------------------------------------------------
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
using FASTER.core;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.Storage.RetryPolicies;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
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
        private readonly CloudStorageAccount secondaryStorageAccount;

        private readonly CloudBlobContainer blockBlobContainer;
        private readonly CloudBlobContainer pageBlobContainer;
        private CloudBlockBlob eventLogCommitBlob;
        private CloudBlobDirectory pageBlobPartitionDirectory;
        private CloudBlobDirectory blockBlobPartitionDirectory;

        private string leaseId;

        private TimeSpan LeaseDuration = TimeSpan.FromSeconds(45); // max time the lease stays after unclean shutdown
        private TimeSpan LeaseRenewal = TimeSpan.FromSeconds(30); // how often we renew the lease
        private TimeSpan LeaseSafetyBuffer = TimeSpan.FromSeconds(10); // how much time we want left on the lease before issuing a protected access

        public const bool CONFIGURE_AWAIT_FOR_STORAGE_CALLS = true;

        internal CheckpointInfo CheckpointInfo { get; private set; } = new CheckpointInfo();

        internal FasterTraceHelper TraceHelper { get; private set; }
        internal FasterTraceHelper StorageTracer => this.TraceHelper.IsTracingAtMostDetailedLevel ? this.TraceHelper : null;

        internal bool UseLocalFilesForTestingAndDebugging { get; private set; }

        public IDevice EventLogDevice { get; private set; }
        public IDevice HybridLogDevice { get; private set; }
        public IDevice ObjectLogDevice { get; private set; }

        private IDevice[] PsfLogDevices;
        private readonly CheckpointInfo[] PsfCheckpointInfos;
        private int PsfGroupCount => this.PsfCheckpointInfos.Length;
        private const int InvalidPsfGroupOrdinal = -1;

        public string ContainerName { get; }

        public CloudBlobContainer BlockBlobContainer => this.blockBlobContainer;
        public CloudBlobContainer PageBlobContainer => this.pageBlobContainer;

        public IPartitionErrorHandler PartitionErrorHandler { get; private set; }

        internal static SemaphoreSlim AsynchronousStorageReadMaxConcurrency = new SemaphoreSlim(Environment.ProcessorCount * 25);
        internal static SemaphoreSlim AsynchronousStorageWriteMaxConcurrency = new SemaphoreSlim(Environment.ProcessorCount * 25);
        internal static SemaphoreSlim SynchronousStorageAccessMaxConcurrency = new SemaphoreSlim(Environment.ProcessorCount * 3);

        private volatile System.Diagnostics.Stopwatch leaseTimer;

        public FasterLogSettings EventLogSettings => new FasterLogSettings
        {
            LogDevice = this.EventLogDevice,
            LogCommitManager = this.UseLocalFilesForTestingAndDebugging
                ? new LocalLogCommitManager($"{this.LocalDirectoryPath}\\{this.PartitionFolderName}\\{CommitBlobName}")
                : (ILogCommitManager)this,
            PageSizeBits = 21, // 2MB
            SegmentSizeBits = 30, // 1 GB
            MemorySizeBits = 23, // 8MB
        };

        //public LogSettings StoreLogSettings => new LogSettings
        //{
        //    LogDevice = this.HybridLogDevice,
        //    ObjectLogDevice = this.ObjectLogDevice,
        //    PageSizeBits = 20, // 1MB
        //    MutableFraction = 0.9,
        //    SegmentSizeBits = 28, // 256 MB
        //    CopyReadsToTail = true,
        //    MemorySizeBits = 24, // 16MB
        //};

        public LogSettings StoreLogSettings(uint numPartitions) => new LogSettings
        {
            LogDevice = this.HybridLogDevice,
            ObjectLogDevice = this.ObjectLogDevice,
            PageSizeBits = 13, // 8kB
            MutableFraction = 0.9,
            SegmentSizeBits = 28, // 256 MB
            CopyReadsToTail = true,
            MemorySizeBits =
                (numPartitions <=  1) ? 25 : // 32MB
                (numPartitions <=  2) ? 24 : // 16MB
                (numPartitions <=  4) ? 23 : // 8MB
                (numPartitions <=  8) ? 22 : // 4MB
                (numPartitions <= 16) ? 21 : // 2MB
                                        20 , // 1MB         
        };

        public void Dispose()
        {
            //TODO figure out what is supposed to go here
        }

        public void PurgeAll()
        {
            //TODO figure out what is supposed to go here
        }

        public CheckpointSettings StoreCheckpointSettings => new CheckpointSettings
        {
            CheckpointManager = this.UseLocalFilesForTestingAndDebugging 
                ? new LocalFileCheckpointManager(this.CheckpointInfo, this.LocalCheckpointDirectoryPath, this.GetCheckpointCompletedBlobName())
                : (ICheckpointManager)this,
            CheckPointType = CheckpointType.FoldOver
        };

        internal PSFRegistrationSettings<TKey> CreatePSFRegistrationSettings<TKey>(uint numberPartitions, int groupOrdinal)
        {
            var storeLogSettings = this.StoreLogSettings(numberPartitions);
            return new PSFRegistrationSettings<TKey>
            {
                HashTableSize = FasterKV.HashTableSize,
                LogSettings = new LogSettings()
                {
                    LogDevice = this.PsfLogDevices[groupOrdinal],
                    PageSizeBits = storeLogSettings.PageSizeBits,
                    SegmentSizeBits = storeLogSettings.SegmentSizeBits,
                    MemorySizeBits = storeLogSettings.MemorySizeBits,
                    CopyReadsToTail = false,
                    ReadCacheSettings = storeLogSettings.ReadCacheSettings
                },
                CheckpointSettings = new CheckpointSettings
                {
                    CheckpointManager = this.UseLocalFilesForTestingAndDebugging
                        ? new LocalFileCheckpointManager(this.PsfCheckpointInfos[groupOrdinal], this.LocalPsfCheckpointDirectoryPath(groupOrdinal), this.GetCheckpointCompletedBlobName())
                        : (ICheckpointManager)new PsfBlobCheckpointManager(this, groupOrdinal),
                    CheckPointType = CheckpointType.FoldOver
                }
            };
        }

        public const int MaxRetries = 5;

        public static BlobRequestOptions BlobRequestOptionsAggressiveTimeout = new BlobRequestOptions()
        {
            RetryPolicy = default, // no automatic retry
            NetworkTimeout = TimeSpan.FromSeconds(2),
            ServerTimeout = TimeSpan.FromSeconds(2),
            MaximumExecutionTime = TimeSpan.FromSeconds(2),
        };

        public static BlobRequestOptions BlobRequestOptionsDefault => new BlobRequestOptions()
        {
            RetryPolicy = default, // no automatic retry
            NetworkTimeout = TimeSpan.FromSeconds(15),
            ServerTimeout = TimeSpan.FromSeconds(15), 
            MaximumExecutionTime = TimeSpan.FromSeconds(15),
        };

        public static BlobRequestOptions BlobRequestOptionsWithRetry => new BlobRequestOptions()
        {
            RetryPolicy = new ExponentialRetry(TimeSpan.FromSeconds(2), MaxRetries),
            NetworkTimeout = TimeSpan.FromSeconds(15),
            ServerTimeout = TimeSpan.FromSeconds(15),
            MaximumExecutionTime = TimeSpan.FromSeconds(15),
        };

        public static TimeSpan GetDelayBetweenRetries(int numAttempts)
            => TimeSpan.FromSeconds(Math.Pow(2, (numAttempts - 1)));
       
        // For tests only; TODO consider adding PSFs
        internal BlobManager(CloudStorageAccount storageAccount, CloudStorageAccount secondaryStorageAccount, string taskHubName, ILogger logger, Microsoft.Extensions.Logging.LogLevel logLevelLimit, uint partitionId, IPartitionErrorHandler errorHandler)
            : this(storageAccount, secondaryStorageAccount, taskHubName, logger, logLevelLimit, partitionId, errorHandler, 0)
        {
        }

        /// <summary>
        /// Create a blob manager.
        /// </summary>
        /// <param name="storageAccount">The cloud storage account, or null if using local file paths</param>
        /// <param name="secondaryStorageAccount">Optionally, a secondary cloud storage accounts</param>
        /// <param name="taskHubName">The name of the taskhub</param>
        /// <param name="logger">A logger for logging</param>
        /// <param name="logLevelLimit">A limit on log event level emitted</param>
        /// <param name="partitionId">The partition id</param>
        /// <param name="errorHandler">A handler for errors encountered in this partition</param>
        /// <param name="psfGroupCount">Number of PSF groups to be created in FASTER</param>
        public BlobManager(CloudStorageAccount storageAccount, CloudStorageAccount secondaryStorageAccount, string taskHubName, ILogger logger, Microsoft.Extensions.Logging.LogLevel logLevelLimit, uint partitionId, IPartitionErrorHandler errorHandler, int psfGroupCount)
        {
            this.cloudStorageAccount = storageAccount;
            this.secondaryStorageAccount = secondaryStorageAccount;
            this.UseLocalFilesForTestingAndDebugging = (storageAccount == null);
            this.ContainerName = GetContainerName(taskHubName);
            this.partitionId = partitionId;
            this.PsfCheckpointInfos = Enumerable.Range(0, psfGroupCount).Select(ii => new CheckpointInfo()).ToArray();

            if (!this.UseLocalFilesForTestingAndDebugging)
            {
                CloudBlobClient serviceClient = this.cloudStorageAccount.CreateCloudBlobClient();
                this.blockBlobContainer = serviceClient.GetContainerReference(this.ContainerName);
                serviceClient = this.secondaryStorageAccount.CreateCloudBlobClient();
                this.pageBlobContainer = serviceClient.GetContainerReference(this.ContainerName);
            }

            this.TraceHelper = new FasterTraceHelper(logger, logLevelLimit, this.partitionId, this.UseLocalFilesForTestingAndDebugging ? "none" : this.cloudStorageAccount.Credentials.AccountName, taskHubName);
            this.PartitionErrorHandler = errorHandler;
            this.shutDownOrTermination = CancellationTokenSource.CreateLinkedTokenSource(errorHandler.Token);
        }

        // For testing and debugging with local files
        internal static string LocalFileDirectoryForTestingAndDebugging { get; set; } = $"{Environment.GetEnvironmentVariable("temp")}\\FasterTestStorage";

        private string LocalDirectoryPath => $"{LocalFileDirectoryForTestingAndDebugging}\\{this.ContainerName}";

        private string PartitionFolderName => $"p{this.partitionId:D2}";
        private string PsfGroupFolderName(int groupOrdinal) => $"psfgroup.{groupOrdinal:D3}";

        private string LocalCheckpointDirectoryPath => $"{LocalDirectoryPath}\\chkpts{this.partitionId:D2}";
        private string LocalPsfCheckpointDirectoryPath(int groupOrdinal) => $"{LocalDirectoryPath}\\chkpts{this.partitionId:D2}\\psfgroup.{groupOrdinal:D3}";

        private const string EventLogBlobName = "commit-log";
        private const string CommitBlobName = "commit-lease";
        private const string HybridLogBlobName = "store";
        private const string ObjectLogBlobName = "store.obj";

        // PSFs do not have an object log
        private const string PsfHybridLogBlobName = "store.psf";

        private Task LeaseMaintenanceLoopTask = Task.CompletedTask;
        private volatile Task NextLeaseRenewalTask = Task.CompletedTask;

        private static string GetContainerName(string taskHubName) => taskHubName.ToLowerInvariant() + "-storage";

        public async Task StartAsync()
        {
            if (this.UseLocalFilesForTestingAndDebugging)
            {
                Directory.CreateDirectory($"{this.LocalDirectoryPath}\\{PartitionFolderName}");

                this.EventLogDevice = Devices.CreateLogDevice($"{this.LocalDirectoryPath}\\{PartitionFolderName}\\{EventLogBlobName}");
                this.HybridLogDevice = Devices.CreateLogDevice($"{this.LocalDirectoryPath}\\{PartitionFolderName}\\{HybridLogBlobName}");
                this.ObjectLogDevice = Devices.CreateLogDevice($"{this.LocalDirectoryPath}\\{PartitionFolderName}\\{ObjectLogBlobName}");
                this.PsfLogDevices = (from groupOrdinal in Enumerable.Range(0, this.PsfGroupCount)
                                      let deviceName = $"{this.LocalDirectoryPath}\\{PartitionFolderName}\\{PsfGroupFolderName(groupOrdinal)}\\{PsfHybridLogBlobName}"
                                      select Devices.CreateLogDevice(deviceName)).ToArray();

                // This does not acquire any blob ownership, but is needed for the lease maintenance loop which calls PartitionErrorHandler.TerminateNormally() when done.
                await this.AcquireOwnership();
            }
            else
            {
                await this.blockBlobContainer.CreateIfNotExistsAsync();
                await this.pageBlobContainer.CreateIfNotExistsAsync();
                this.pageBlobPartitionDirectory = this.pageBlobContainer.GetDirectoryReference(this.PartitionFolderName);
                this.blockBlobPartitionDirectory = this.blockBlobContainer.GetDirectoryReference(this.PartitionFolderName);

                this.eventLogCommitBlob = this.blockBlobPartitionDirectory.GetBlockBlobReference(CommitBlobName);

                AzureStorageDevice createDevice(string name) =>
                    new AzureStorageDevice(name, this.blockBlobPartitionDirectory.GetDirectoryReference(name), this.pageBlobPartitionDirectory.GetDirectoryReference(name), this, true);

                var eventLogDevice = createDevice(EventLogBlobName);
                var hybridLogDevice = createDevice(HybridLogBlobName);
                var objectLogDevice = createDevice(ObjectLogBlobName);
                var psfLogDevices = (from groupOrdinal in Enumerable.Range(0, this.PsfGroupCount)
                                     let psfDirectoryBlob = this.blockBlobPartitionDirectory.GetDirectoryReference(this.PsfGroupFolderName(groupOrdinal))
                                     let psfDirectoryPage = this.pageBlobPartitionDirectory.GetDirectoryReference(this.PsfGroupFolderName(groupOrdinal))
                                     select new AzureStorageDevice(PsfHybridLogBlobName, psfDirectoryBlob.GetDirectoryReference(PsfHybridLogBlobName), psfDirectoryPage.GetDirectoryReference(PsfHybridLogBlobName), this, true)).ToArray();

                await this.AcquireOwnership();

                this.TraceHelper.FasterProgress("Starting Faster Devices");
                var startTasks = new List<Task>
                {
                    eventLogDevice.StartAsync(),
                    hybridLogDevice.StartAsync(),
                    objectLogDevice.StartAsync()
                };
                startTasks.AddRange(psfLogDevices.Select(psfLogDevice => psfLogDevice.StartAsync()));
                await Task.WhenAll(startTasks);
                this.TraceHelper.FasterProgress("Started Faster Devices");

                this.EventLogDevice = eventLogDevice;
                this.HybridLogDevice = hybridLogDevice;
                this.ObjectLogDevice = objectLogDevice;
                this.PsfLogDevices = psfLogDevices;
            }
        }

        internal void ClosePSFDevices() => Array.ForEach(this.PsfLogDevices, logDevice => logDevice.Dispose());

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

            await this.LeaseMaintenanceLoopTask; // wait for loop to terminate cleanly
        }

        public static async Task DeleteTaskhubStorageAsync(CloudStorageAccount account, string taskHubName)
        {
            var containerName = GetContainerName(taskHubName);

            if (account is null)
            {
                DirectoryInfo di = new DirectoryInfo($"{LocalFileDirectoryForTestingAndDebugging}\\{containerName}");
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
                    var tasks = blobContainer.ListBlobs(null, true)
                                             .Where(blob => blob.GetType() == typeof(CloudBlob) || blob.GetType().BaseType == typeof(CloudBlob))
                                             .Select(blob => BlobUtils.ForceDeleteAsync((CloudBlob)blob))
                                             .ToArray();
                    await Task.WhenAll(tasks);
                }

                // We are not deleting the container itself because it creates problems when trying to recreate
                // the same container soon afterwards so we leave an empty container behind. Oh well.
            }
        }

        public ValueTask ConfirmLeaseIsGoodForAWhileAsync()
        {
            if (this.leaseTimer?.Elapsed < this.LeaseDuration - this.LeaseSafetyBuffer)
            {
                return default;
            }
            this.TraceHelper.LeaseProgress("Access is waiting for fresh lease");
            return new ValueTask(this.NextLeaseRenewalTask);
        }

        public void ConfirmLeaseIsGoodForAWhile()
        {
            if (this.leaseTimer?.Elapsed < this.LeaseDuration - this.LeaseSafetyBuffer)
            {
                return;
            }
            this.TraceHelper.LeaseProgress("Access is waiting for fresh lease");
            this.NextLeaseRenewalTask.Wait();
            this.TraceHelper.LeaseProgress("Access has fresh lease now");
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

                    if (!this.UseLocalFilesForTestingAndDebugging)
                    {
                        this.leaseId = await this.eventLogCommitBlob.AcquireLeaseAsync(LeaseDuration, null, accessCondition: null, options: BlobManager.BlobRequestOptionsDefault, operationContext: null, cancellationToken: this.PartitionErrorHandler.Token)
                            .ConfigureAwait(BlobManager.CONFIGURE_AWAIT_FOR_STORAGE_CALLS);
                        this.TraceHelper.LeaseAcquired();
                    }

                    this.leaseTimer = newLeaseTimer;
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
                        await this.eventLogCommitBlob.UploadFromByteArrayAsync(Array.Empty<byte>(), 0, 0)
                            .ConfigureAwait(BlobManager.CONFIGURE_AWAIT_FOR_STORAGE_CALLS);
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
                this.shutDownOrTermination.Token.ThrowIfCancellationRequested();

                AccessCondition acc = new AccessCondition() { LeaseId = this.leaseId };
                var nextLeaseTimer = new System.Diagnostics.Stopwatch();
                nextLeaseTimer.Start();

                if (!this.UseLocalFilesForTestingAndDebugging)
                {
                    this.TraceHelper.LeaseProgress($"Renewing lease at {this.leaseTimer.Elapsed.TotalSeconds - this.LeaseDuration.TotalSeconds}s");
                    await this.eventLogCommitBlob.RenewLeaseAsync(acc, this.PartitionErrorHandler.Token)
                        .ConfigureAwait(BlobManager.CONFIGURE_AWAIT_FOR_STORAGE_CALLS);
                    this.TraceHelper.LeaseProgress($"Renewed lease at {this.leaseTimer.Elapsed.TotalSeconds - this.LeaseDuration.TotalSeconds}s");
                }

                this.leaseTimer = nextLeaseTimer;
            }
            catch(OperationCanceledException)
            {
                // o.k. during termination or shutdown
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
                    int timeLeft = (int) (this.LeaseRenewal - this.leaseTimer.Elapsed).TotalMilliseconds;

                    if (timeLeft <= 0)
                    {
                        this.NextLeaseRenewalTask = this.RenewLeaseTask();
                    }
                    else
                    {
                        this.NextLeaseRenewalTask = LeaseTimer.Instance.Schedule(timeLeft, this.RenewLeaseTask, this.shutDownOrTermination.Token);
                    }

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
                if (!this.UseLocalFilesForTestingAndDebugging)
                {
                    try
                    {
                        this.TraceHelper.LeaseProgress("Releasing lease");

                        AccessCondition acc = new AccessCondition() { LeaseId = this.leaseId };

                        await this.eventLogCommitBlob.ReleaseLeaseAsync(accessCondition: acc, options: BlobManager.BlobRequestOptionsDefault, operationContext: null, cancellationToken: this.PartitionErrorHandler.Token)
                            .ConfigureAwait(BlobManager.CONFIGURE_AWAIT_FOR_STORAGE_CALLS);

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
            }

            this.TraceHelper.LeaseProgress("Blob manager stopped");
        }

        #region Blob Name Management

        private string GetCheckpointCompletedBlobName() => $"last-checkpoint.json";

        private string GetIndexCheckpointMetaBlobName(Guid token) => $"index-checkpoints/{token}/info.dat";

        private (string, string) GetPrimaryHashTableBlobName(Guid token) => ($"index-checkpoints/{token}", "ht.dat");

        private string GetHybridLogCheckpointMetaBlobName(Guid token) => $"cpr-checkpoints/{token}/info.dat";

        private (string, string) GetLogSnapshotBlobName(Guid token) => ($"cpr-checkpoints/{token}", "snapshot.dat");

        private (string, string) GetObjectLogSnapshotBlobName(Guid token) => ($"cpr-checkpoints/{token}", "snapshot.obj.dat");

        #endregion

        #region ILogCommitManager

        void ILogCommitManager.Commit(long beginAddress, long untilAddress, byte[] commitMetadata)
        {
            this.StorageTracer?.FasterStorageProgress($"ILogCommitManager.Commit Called beginAddress={beginAddress} untilAddress={untilAddress}");
            var stopwatch = new Stopwatch();
            int numAttempts = 0;

            while (true)
            {
                numAttempts++;

                AccessCondition acc = new AccessCondition() { LeaseId = this.leaseId };
                try
                {
                    SynchronousStorageAccessMaxConcurrency.Wait();

                    this.PartitionErrorHandler.Token.ThrowIfCancellationRequested();
                    var blobRequestOptions = numAttempts > 1 ? BlobManager.BlobRequestOptionsDefault : BlobManager.BlobRequestOptionsAggressiveTimeout;
                    stopwatch.Restart();
                    this.eventLogCommitBlob.UploadFromByteArray(commitMetadata, 0, commitMetadata.Length, acc, blobRequestOptions);
                    stopwatch.Stop();
                    this.StorageTracer?.FasterStorageProgress($"ILogCommitManager.Commit Returned latencyMs={stopwatch.Elapsed.TotalMilliseconds:F1}");

                    if (stopwatch.ElapsedMilliseconds > 1000)
                    {
                        this.TraceHelper.FasterPerfWarning($"CloudBlockBlob.UploadFromByteArray took {stopwatch.ElapsedMilliseconds / 1000}s, which is excessive; target={eventLogCommitBlob.Name} length={commitMetadata.Length}");
                    }

                    return;
                }
                catch (StorageException ex) when (BlobUtils.LeaseConflict(ex))
                {
                    // We lost the lease to someone else. Terminate ownership immediately.
                    this.TraceHelper.LeaseLost(nameof(ILogCommitManager.Commit));
                    this.HandleBlobError(nameof(ILogCommitManager.Commit), "lease lost", this.eventLogCommitBlob?.Name, ex, true, this.PartitionErrorHandler.IsTerminated);
                    throw;
                }
                catch (StorageException ex) when (BlobUtils.LeaseExpired(ex) && numAttempts < BlobManager.MaxRetries)
                {
                    // if we get here, the lease renewal task did not complete in time
                    // wait for it to complete or throw
                    this.TraceHelper.LeaseProgress("ILogCommitManager.Commit: wait for next renewal");
                    this.NextLeaseRenewalTask.Wait();
                    this.TraceHelper.LeaseProgress("ILogCommitManager.Commit: renewal complete");
                    continue;
                }
                catch (StorageException e) when (BlobUtils.IsTransientStorageError(e) && numAttempts < BlobManager.MaxRetries)
                {
                    stopwatch.Stop();
                    if (BlobUtils.IsTimeout(e))
                    {
                        this.TraceHelper.FasterPerfWarning($"CloudBlockBlob.UploadFromByteArray timed out, retry after {stopwatch.ElapsedMilliseconds:f1}ms; target={eventLogCommitBlob.Name} length={commitMetadata.Length}");
                    }
                    else
                    {
                        TimeSpan nextRetryIn = BlobManager.GetDelayBetweenRetries(numAttempts);
                        this.HandleBlobError(nameof(ILogCommitManager.Commit), $"could not write to commit blob, will retry in {nextRetryIn}s", eventLogCommitBlob.Name, e, false, true);
                        Thread.Sleep(nextRetryIn);
                    }
                    continue;
                }
                catch (Exception e)
                {
                    this.TraceHelper.FasterBlobStorageError(nameof(ILogCommitManager.Commit), this.eventLogCommitBlob.Name, e);
                    throw;
                }
                finally
                {
                    SynchronousStorageAccessMaxConcurrency.Release();
                }
            }
        }

        IEnumerable<long> ILogCommitManager.ListCommits()
        {
            // we only use a single commit file in this implementation
            yield return 0;
        }

        byte[] ILogCommitManager.GetCommitMetadata(long commitNum)
        {
            this.StorageTracer?.FasterStorageProgress($"ILogCommitManager.GetCommitMetadata Called (thread={Thread.CurrentThread.ManagedThreadId})");
            var stopwatch = new Stopwatch();
            int numAttempts = 0;

            while (true)
            {
                numAttempts++;

                AccessCondition acc = new AccessCondition() { LeaseId = this.leaseId };
                try
                {
                    SynchronousStorageAccessMaxConcurrency.Wait();
                    this.PartitionErrorHandler.Token.ThrowIfCancellationRequested();
                    using var stream = new MemoryStream();
                    var blobRequestOptions = numAttempts > 1 ? BlobManager.BlobRequestOptionsDefault : BlobManager.BlobRequestOptionsAggressiveTimeout;
                    stopwatch.Restart();
                    this.eventLogCommitBlob.DownloadToStream(stream, acc, blobRequestOptions);
                    stopwatch.Stop();
                    if (stopwatch.ElapsedMilliseconds > 1000)
                    {
                        this.TraceHelper.FasterPerfWarning($"CloudBlockBlob.DownloadToStream took {stopwatch.ElapsedMilliseconds / 1000}s, which is excessive; target={eventLogCommitBlob.Name} length={stream.Position}");
                    }
                    var bytes = stream.ToArray();
                    this.StorageTracer?.FasterStorageProgress($"ILogCommitManager.GetCommitMetadata Returned {bytes?.Length ?? null} bytes, latencyMs={stopwatch.Elapsed.TotalMilliseconds:F1}");
                    return bytes.Length == 0 ? null : bytes;
                }
                catch (StorageException ex) when (BlobUtils.LeaseConflict(ex))
                {
                    // We lost the lease to someone else. Terminate ownership immediately.
                    this.TraceHelper.LeaseLost(nameof(ILogCommitManager.GetCommitMetadata));
                    this.HandleBlobError(nameof(ILogCommitManager.Commit), "lease lost", this.eventLogCommitBlob?.Name, ex, true, this.PartitionErrorHandler.IsTerminated);
                    throw;
                }
                catch (StorageException ex) when (BlobUtils.LeaseExpired(ex) && numAttempts < BlobManager.MaxRetries)
                {
                    // if we get here, the lease renewal task did not complete in time
                    // wait for it to complete or throw
                    this.TraceHelper.LeaseProgress("ILogCommitManager.GetCommitMetadata: wait for next renewal");
                    this.NextLeaseRenewalTask.Wait();
                    this.TraceHelper.LeaseProgress("ILogCommitManager.GetCommitMetadata: renewal complete");
                    continue;
                }
                catch (StorageException e) when (BlobUtils.IsTransientStorageError(e) && numAttempts < BlobManager.MaxRetries)
                {
                    stopwatch.Stop();
                    if (BlobUtils.IsTimeout(e))
                    {
                        this.TraceHelper.FasterPerfWarning($"CloudBlockBlob.DownloadToStream timed out, retry after {stopwatch.ElapsedMilliseconds:f1}ms; target={eventLogCommitBlob.Name}");
                    }
                    else
                    {
                        TimeSpan nextRetryIn = BlobManager.GetDelayBetweenRetries(numAttempts);
                        this.HandleBlobError(nameof(ILogCommitManager.Commit), $"could not read commit blob, will retry in {nextRetryIn}s", eventLogCommitBlob.Name, e, false, true);
                        Thread.Sleep(nextRetryIn);
                    }
                    continue;
                }
                catch (Exception e)
                {
                    this.TraceHelper.FasterBlobStorageError(nameof(ILogCommitManager.GetCommitMetadata), this.eventLogCommitBlob.Name, e);
                    throw;
                }
                finally
                {
                    SynchronousStorageAccessMaxConcurrency.Release();
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

        #region Call-throughs to actual implementation; separated for PSFs

        void ICheckpointManager.CommitIndexCheckpoint(Guid indexToken, byte[] commitMetadata)
            => this.CommitIndexCheckpoint(indexToken, commitMetadata, InvalidPsfGroupOrdinal);

        void ICheckpointManager.CommitLogCheckpoint(Guid logToken, byte[] commitMetadata)
            => this.CommitLogCheckpoint(logToken, commitMetadata, InvalidPsfGroupOrdinal);

        byte[] ICheckpointManager.GetIndexCheckpointMetadata(Guid indexToken)
            => this.GetIndexCheckpointMetadata(indexToken, InvalidPsfGroupOrdinal);

        byte[] ICheckpointManager.GetLogCheckpointMetadata(Guid logToken)
            => this.GetLogCheckpointMetadata(logToken, InvalidPsfGroupOrdinal);

        IDevice ICheckpointManager.GetIndexDevice(Guid indexToken)
            => this.GetIndexDevice(indexToken, InvalidPsfGroupOrdinal);

        IDevice ICheckpointManager.GetSnapshotLogDevice(Guid token)
            => this.GetSnapshotLogDevice(token, InvalidPsfGroupOrdinal);

        IDevice ICheckpointManager.GetSnapshotObjectLogDevice(Guid token)
            => this.GetSnapshotObjectLogDevice(token, InvalidPsfGroupOrdinal);

        IEnumerable<Guid> ICheckpointManager.GetIndexCheckpointTokens()
        {
            if (this.GetLatestCheckpoint(out Guid indexToken, out Guid _, InvalidPsfGroupOrdinal))
            {
                yield return indexToken;
            }
        }

        IEnumerable<Guid> ICheckpointManager.GetLogCheckpointTokens()
        {
            if (this.GetLatestCheckpoint(out Guid _, out Guid logToken, InvalidPsfGroupOrdinal))
            {
                yield return logToken;
            }
        }

        #endregion

        #region Actual implementation; separated for PSFs

        (bool, string) IsPsfOrPrimary(int psfGroupOrdinal)
        {
            var isPsf = psfGroupOrdinal != InvalidPsfGroupOrdinal;
            return (isPsf, isPsf ? $"PSF Group {psfGroupOrdinal}" : "Primary FKV");
        }

        internal void CommitIndexCheckpoint(Guid indexToken, byte[] commitMetadata, int psfGroupOrdinal)
        {
            var (isPsf, tag) = IsPsfOrPrimary(psfGroupOrdinal);
            this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.CommitIndexCheckpoint Called on {tag}, indexToken={indexToken}");
            CloudBlockBlob target = null;
            try
            {
                var partDir = isPsf ? this.blockBlobPartitionDirectory.GetDirectoryReference(PsfGroupFolderName(psfGroupOrdinal)) : this.blockBlobPartitionDirectory;
                var metaFileBlob = target = partDir.GetBlockBlobReference(this.GetIndexCheckpointMetaBlobName(indexToken));

                // we don't need a lease for the checkpoint data since the checkpoint token provides isolation
                using (var blobStream = metaFileBlob.OpenWrite())
                {
                    using var writer = new BinaryWriter(blobStream);
                    writer.Write(commitMetadata.Length);
                    writer.Write(commitMetadata);
                    writer.Flush();
                }

                (isPsf ? this.PsfCheckpointInfos[psfGroupOrdinal] : this.CheckpointInfo).IndexToken = indexToken;

                this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.CommitIndexCheckpoint Returned from {tag}, target={target.Name}");
            }
            catch (Exception e)
            {
                this.TraceHelper.FasterBlobStorageError(nameof(ICheckpointManager.CommitIndexCheckpoint), target.Name, e);
                throw;
            }
        }

        internal void CommitLogCheckpoint(Guid logToken, byte[] commitMetadata, int psfGroupOrdinal)
        {
            var (isPsf, tag) = IsPsfOrPrimary(psfGroupOrdinal);
            this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.CommitLogCheckpoint Called on {tag}, logToken={logToken}");
            CloudBlockBlob target = null;
            try
            {
                SynchronousStorageAccessMaxConcurrency.Wait();

                var partDir = isPsf ? this.blockBlobPartitionDirectory.GetDirectoryReference(PsfGroupFolderName(psfGroupOrdinal)) : this.blockBlobPartitionDirectory;
                var metaFileBlob = target = partDir.GetBlockBlobReference(this.GetHybridLogCheckpointMetaBlobName(logToken));
                
                // we don't need a lease for the checkpoint data since the checkpoint token provides isolation
                using (var blobStream = metaFileBlob.OpenWrite())
                {
                    using var writer = new BinaryWriter(blobStream);
                    writer.Write(commitMetadata.Length);
                    writer.Write(commitMetadata);
                    writer.Flush();
                }

                (isPsf ? this.PsfCheckpointInfos[psfGroupOrdinal] : this.CheckpointInfo).LogToken = logToken;

                this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.CommitLogCheckpoint Returned from {tag}, target={target.Name}");
            }
            catch (Exception e)
            {
                this.TraceHelper.FasterBlobStorageError(nameof(ICheckpointManager.CommitLogCheckpoint), target?.Name, e);
                throw;
            }
            finally
            {
                SynchronousStorageAccessMaxConcurrency.Release();
            }
        }

        internal byte[] GetIndexCheckpointMetadata(Guid indexToken, int psfGroupOrdinal)
        {
            var (isPsf, tag) = IsPsfOrPrimary(psfGroupOrdinal);
            this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.GetIndexCommitMetadata Called on {tag}, indexToken={indexToken}");
            CloudBlockBlob target = null;
            try
            {
                SynchronousStorageAccessMaxConcurrency.Wait();

                var partDir = isPsf ? this.blockBlobPartitionDirectory.GetDirectoryReference(PsfGroupFolderName(psfGroupOrdinal)) : this.blockBlobPartitionDirectory;
                var metaFileBlob = target = partDir.GetBlockBlobReference(this.GetIndexCheckpointMetaBlobName(indexToken));
                
                // we don't need a lease for the checkpoint data since the checkpoint token provides isolation
                using var blobstream = metaFileBlob.OpenRead();
                using var reader = new BinaryReader(blobstream);
                var len = reader.ReadInt32();
                var result = reader.ReadBytes(len);
                this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.GetIndexCommitMetadata Returned {result?.Length ?? null} bytes from {tag}, target={target.Name}");
                return result;
            }
            catch (Exception e)
            {
                this.TraceHelper.FasterBlobStorageError(nameof(ICheckpointManager.GetIndexCheckpointMetadata), target?.Name, e);
                throw;
            }
            finally
            {
                SynchronousStorageAccessMaxConcurrency.Release();
            }
        }

        internal byte[] GetLogCheckpointMetadata(Guid logToken, int psfGroupOrdinal)
        {
            var (isPsf, tag) = IsPsfOrPrimary(psfGroupOrdinal);
            this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.GetIndexCommitMetadata Called on {tag}, logToken={logToken}");
            CloudBlockBlob target = null;
            try
            {
                SynchronousStorageAccessMaxConcurrency.Wait();

                var partDir = isPsf ? this.blockBlobPartitionDirectory.GetDirectoryReference(PsfGroupFolderName(psfGroupOrdinal)) : this.blockBlobPartitionDirectory;
                var metaFileBlob = target = partDir.GetBlockBlobReference(this.GetHybridLogCheckpointMetaBlobName(logToken));
                
                // we don't need a lease for the checkpoint data since the checkpoint token provides isolation
                using var blobstream = metaFileBlob.OpenRead();
                using var reader = new BinaryReader(blobstream);
                var len = reader.ReadInt32();
                var result = reader.ReadBytes(len);
                this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.GetIndexCommitMetadata Returned {result?.Length ?? null} bytes from {tag}, target={target.Name}");
                return result;
            }
            catch (Exception e)
            {
                this.TraceHelper.FasterBlobStorageError(nameof(ICheckpointManager.GetLogCheckpointMetadata), target?.Name, e);
                throw;
            }
            finally
            {
                SynchronousStorageAccessMaxConcurrency.Release();
            }
        }

        internal IDevice GetIndexDevice(Guid indexToken, int psfGroupOrdinal)
        {
            var (isPsf, tag) = IsPsfOrPrimary(psfGroupOrdinal);
            this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.GetIndexDevice Called on {tag}, indexToken={indexToken}");
            try
            {
                SynchronousStorageAccessMaxConcurrency.Wait();

                var (path, blobName) = this.GetPrimaryHashTableBlobName(indexToken);
                var ppartDir = isPsf ? this.pageBlobPartitionDirectory.GetDirectoryReference(PsfGroupFolderName(psfGroupOrdinal)) : this.pageBlobPartitionDirectory;
                var pblobDirectory = ppartDir.GetDirectoryReference(path);
                var bpartDir = isPsf ? this.blockBlobPartitionDirectory.GetDirectoryReference(PsfGroupFolderName(psfGroupOrdinal)) : this.blockBlobPartitionDirectory;
                var bblobDirectory = bpartDir.GetDirectoryReference(path);
                var device = new AzureStorageDevice(blobName, bblobDirectory, pblobDirectory, this, false); // we don't need a lease since the token provides isolation
                device.StartAsync().Wait();
                this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.GetIndexDevice Returned from {tag}, blobDirectory={pblobDirectory.Prefix} blobName={blobName}");
                return device;
            }
            catch (Exception e)
            {
                this.TraceHelper.FasterBlobStorageError(nameof(ICheckpointManager.GetIndexDevice), null, e);
                throw;
            }
            finally
            {
                SynchronousStorageAccessMaxConcurrency.Release();
            }
        }

        internal IDevice GetSnapshotLogDevice(Guid token, int psfGroupOrdinal)
        {
            var (isPsf, tag) = IsPsfOrPrimary(psfGroupOrdinal);
            this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.GetSnapshotLogDevice Called on {tag}, token={token}");
            try
            {
                SynchronousStorageAccessMaxConcurrency.Wait();

                var (path, blobName) = this.GetLogSnapshotBlobName(token);
                var ppartDir = isPsf ? this.pageBlobPartitionDirectory.GetDirectoryReference(PsfGroupFolderName(psfGroupOrdinal)) : this.pageBlobPartitionDirectory;
                var pblobDirectory = ppartDir.GetDirectoryReference(path);
                var bpartDir = isPsf ? this.blockBlobPartitionDirectory.GetDirectoryReference(PsfGroupFolderName(psfGroupOrdinal)) : this.blockBlobPartitionDirectory;
                var bblobDirectory = bpartDir.GetDirectoryReference(path);
                var device = new AzureStorageDevice(blobName, bblobDirectory, pblobDirectory, this, false); // we don't need a lease since the token provides isolation

                device.StartAsync().Wait();
                this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.GetSnapshotLogDevice Returned from {tag}, blobDirectory={pblobDirectory} blobName={blobName}");
                return device;
            }
            catch (Exception e)
            {
                this.TraceHelper.FasterBlobStorageError(nameof(ICheckpointManager.GetSnapshotLogDevice), null, e);
                throw;
            }
            finally
            {
                SynchronousStorageAccessMaxConcurrency.Release();
            }
        }

        internal IDevice GetSnapshotObjectLogDevice(Guid token, int psfGroupOrdinal)
        {
            var (isPsf, tag) = IsPsfOrPrimary(psfGroupOrdinal);
            this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.GetSnapshotObjectLogDevice Called on {tag}, token={token}");
            try
            {
                SynchronousStorageAccessMaxConcurrency.Wait();

                var (path, blobName) = this.GetObjectLogSnapshotBlobName(token);
                var ppartDir = isPsf ? this.pageBlobPartitionDirectory.GetDirectoryReference(PsfGroupFolderName(psfGroupOrdinal)) : this.pageBlobPartitionDirectory;
                var pblobDirectory = ppartDir.GetDirectoryReference(path);
                var bpartDir = isPsf ? this.blockBlobPartitionDirectory.GetDirectoryReference(PsfGroupFolderName(psfGroupOrdinal)) : this.blockBlobPartitionDirectory;
                var bblobDirectory = bpartDir.GetDirectoryReference(path);
                var device = new AzureStorageDevice(blobName, bblobDirectory, pblobDirectory, this, false); // we don't need a lease since the token provides isolation
                device.StartAsync().Wait();
                this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.GetSnapshotObjectLogDevice Returned from {tag}, blobDirectory={pblobDirectory} blobName={blobName}");
                return device;
            }
            catch (Exception e)
            {
                this.TraceHelper.FasterBlobStorageError(nameof(ICheckpointManager.GetSnapshotObjectLogDevice), null, e);
                throw;
            }
            finally
            {
                SynchronousStorageAccessMaxConcurrency.Release();
            }
        }

        internal bool GetLatestCheckpoint(out Guid indexToken, out Guid logToken, int psfGroupOrdinal)
        {
            var (isPsf, tag) = IsPsfOrPrimary(psfGroupOrdinal);
            this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.GetLatestCheckpoint Called on {tag}");
            CloudBlockBlob target = null;
            try
            {
                SynchronousStorageAccessMaxConcurrency.Wait();

                var partDir = isPsf ? this.blockBlobPartitionDirectory.GetDirectoryReference(PsfGroupFolderName(psfGroupOrdinal)) : this.blockBlobPartitionDirectory;
                var checkpointCompletedBlob = partDir.GetBlockBlobReference(this.GetCheckpointCompletedBlobName());

                if (checkpointCompletedBlob.Exists())
                {

                    target = checkpointCompletedBlob;
                    this.ConfirmLeaseIsGoodForAWhile();
                    var jsonString = checkpointCompletedBlob.DownloadText();
                    var checkpointInfo = JsonConvert.DeserializeObject<CheckpointInfo>(jsonString);

                    if (isPsf)
                        this.PsfCheckpointInfos[psfGroupOrdinal] = checkpointInfo;
                    else
                        this.CheckpointInfo = checkpointInfo;

                    indexToken = checkpointInfo.IndexToken;
                    logToken = checkpointInfo.LogToken;

                    this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.GetSnapshotObjectLogDevice Returned from {tag}, indexToken={indexToken} logToken={logToken}");
                    return true;
                }
                else
                {
                    this.StorageTracer?.FasterStorageProgress($"ICheckpointManager.GetSnapshotObjectLogDevice Returned false from {tag}");
                    return false;
                }
            }
            catch (Exception e)
            {
                this.TraceHelper.FasterBlobStorageError(nameof(GetLatestCheckpoint), target?.Name, e);
                throw;
            }
            finally
            {
                SynchronousStorageAccessMaxConcurrency.Release();
            }
        }

        #endregion

        internal async Task FinalizeCheckpointCompletedAsync()
        {
            // write the final file that has all the checkpoint info
            void writeLocal(string path, string text)
                => File.WriteAllText(Path.Combine(path, this.GetCheckpointCompletedBlobName()), text);
            async Task writeBlob(CloudBlobDirectory partDir, string text)
            {
                var checkpointCompletedBlob = partDir.GetBlockBlobReference(this.GetCheckpointCompletedBlobName());
                await this.ConfirmLeaseIsGoodForAWhileAsync().ConfigureAwait(BlobManager.CONFIGURE_AWAIT_FOR_STORAGE_CALLS); // the lease protects the checkpoint completed file
                await checkpointCompletedBlob.UploadTextAsync(text).ConfigureAwait(BlobManager.CONFIGURE_AWAIT_FOR_STORAGE_CALLS);
            }

            // Primary FKV
            {
                var jsonText = JsonConvert.SerializeObject(this.CheckpointInfo, Formatting.Indented);
                if (this.UseLocalFilesForTestingAndDebugging)
                    writeLocal(this.LocalCheckpointDirectoryPath, jsonText);
                else
                    await writeBlob(this.blockBlobPartitionDirectory, jsonText);
            }

            // PSFs
            for (var ii = 0; ii < this.PsfLogDevices.Length; ++ii)
            {
                var jsonText = JsonConvert.SerializeObject(this.PsfCheckpointInfos[ii], Formatting.Indented);
                if (this.UseLocalFilesForTestingAndDebugging)
                    writeLocal(this.LocalPsfCheckpointDirectoryPath(ii), jsonText);
                else
                    await writeBlob(this.blockBlobPartitionDirectory.GetDirectoryReference(this.PsfGroupFolderName(ii)), jsonText);
            }
        }
 
        #endregion
    }
}
