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

using Microsoft.Azure.EventHubs.Processor;
using Microsoft.Azure.Storage;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventSourced.Faster
{
    internal class FasterStorage : StorageAbstraction.IPartitionState
    {
        // if used as a "azure storage connection string", causes Faster to use local file storage instead
        public const string LocalFileStorageConnectionString = "UseLocalFileStorage";

        private CloudStorageAccount storageAccount;
        private readonly string taskHubName;
        private readonly ILogger logger;

        private Partition partition;
        private BlobManager blobManager;
        private LogWorker logWorker;
        private StoreWorker storeWorker;
        private FasterLog log;
        private TrackedObjectStore store;

        private CancellationToken terminationToken;

        internal FasterTraceHelper TraceHelper { get; private set; }

        public FasterStorage(string connectionString, string taskHubName, ILoggerFactory loggerFactory)
        {
            if (connectionString != LocalFileStorageConnectionString)
            {
                this.storageAccount = CloudStorageAccount.Parse(connectionString);
            }
            this.taskHubName = taskHubName;
            this.logger = loggerFactory.CreateLogger($"{EventSourcedOrchestrationService.LoggerCategoryName}.FasterStorage");
        }

        public static Task DeleteTaskhubStorageAsync(string connectionString, string taskHubName)
        {
            var storageAccount = (connectionString != LocalFileStorageConnectionString) ? CloudStorageAccount.Parse(connectionString) : null;
            return BlobManager.DeleteTaskhubStorageAsync(storageAccount, taskHubName);
        }

        public async Task<long> CreateOrRestoreAsync(Partition partition, IPartitionErrorHandler errorHandler, long firstInputQueuePosition)
        {
            this.partition = partition;
            this.terminationToken = errorHandler.Token;


            this.blobManager = new BlobManager(this.storageAccount, this.taskHubName, this.logger, this.partition.Settings.StorageLogLevelLimit, partition.PartitionId, errorHandler, FasterKV.PSFCount);
            this.TraceHelper = blobManager.TraceHelper;

            this.TraceHelper.FasterProgress("Starting BlobManager");
            await blobManager.StartAsync().ConfigureAwait(false);

            var stopwatch = new System.Diagnostics.Stopwatch();
            stopwatch.Start();

            this.TraceHelper.FasterProgress("Creating FasterLog");
            this.log = new FasterLog(this.blobManager);

            if (partition.Settings.UseAlternateObjectStore)
            {
                this.TraceHelper.FasterProgress("Creating FasterAlt");
                this.store = new FasterAlt(this.partition, this.blobManager);
            }
            else
            {
                this.TraceHelper.FasterProgress("Creating FasterKV");
                this.store = new FasterKV(this.partition, this.blobManager);
            }

            this.TraceHelper.FasterProgress("Creating StoreWorker");
            this.storeWorker = new StoreWorker(store, this.partition, this.TraceHelper, this.blobManager, this.terminationToken);

            this.TraceHelper.FasterProgress("Creating LogWorker");
            this.logWorker = this.storeWorker.LogWorker = new LogWorker(this.blobManager, this.log, this.partition, this.storeWorker, this.TraceHelper, this.terminationToken);

            if (this.log.TailAddress == this.log.BeginAddress)
            {
                // take an (empty) checkpoint immediately to ensure the paths are working
                try
                {
                    this.TraceHelper.FasterProgress("Creating store");

                    // this is a fresh partition
                    await storeWorker.Initialize(this.log.BeginAddress, firstInputQueuePosition).ConfigureAwait(false);

                    await this.storeWorker.TakeFullCheckpointAsync("initial checkpoint").ConfigureAwait(false);
                    this.TraceHelper.FasterStoreCreated(storeWorker.InputQueuePosition, stopwatch.ElapsedMilliseconds);

                    this.partition.Assert(!FASTER.core.LightEpoch.AnyInstanceProtected());
                }
                catch (Exception e)
                {
                    this.TraceHelper.FasterStorageError(nameof(CreateOrRestoreAsync), e);
                    throw;
                }
            }
            else
            {
                this.TraceHelper.FasterProgress("Loading checkpoint");

                try
                {
                    // we are recovering the last checkpoint of the store
                    this.store.Recover(out long commitLogPosition, out long inputQueuePosition);
                    storeWorker.SetCheckpointPositionsAfterRecovery(commitLogPosition, inputQueuePosition);

                    this.TraceHelper.FasterCheckpointLoaded(storeWorker.CommitLogPosition, storeWorker.InputQueuePosition, store.StoreStats.Get(), stopwatch.ElapsedMilliseconds);
                }
                catch (Exception e)
                {
                    this.TraceHelper.FasterStorageError("loading checkpoint", e);
                    throw;
                }

                this.partition.Assert(!FASTER.core.LightEpoch.AnyInstanceProtected());

                try
                {
                    if (this.log.TailAddress > (long)storeWorker.CommitLogPosition)
                    {
                        // replay log as the store checkpoint lags behind the log
                        await this.storeWorker.ReplayCommitLog(this.logWorker).ConfigureAwait(false);
                    }
                }
                catch (Exception e)
                {
                    this.TraceHelper.FasterStorageError("replaying log", e);
                    throw;
                }

                // restart pending actitivities, timers, work items etc.
                await storeWorker.RestartThingsAtEndOfRecovery().ConfigureAwait(false);

                this.TraceHelper.FasterProgress("Recovery complete");
            }

            var ignoredTask = this.IdleLoop();

            return storeWorker.InputQueuePosition;
        }

        public async Task CleanShutdown(bool takeFinalCheckpoint)
        {
            this.TraceHelper.FasterProgress("Stopping workers");
            
            // in parallel, finish processing log requests and stop processing store requests
            Task t1 = this.logWorker.PersistAndShutdownAsync();
            Task t2 = this.storeWorker.CancelAndShutdown();

            // observe exceptions if the clean shutdown is not working correctly
            await t1.ConfigureAwait(false);
            await t2.ConfigureAwait(false);

            // if the the settings indicate we want to take a final checkpoint, do so now.
            if (takeFinalCheckpoint)
            {
                this.TraceHelper.FasterProgress("Writing final checkpoint");
                await this.storeWorker.TakeFullCheckpointAsync("final checkpoint").ConfigureAwait(false);
            }

            this.TraceHelper.FasterProgress("Stopping BlobManager");
            await this.blobManager.StopAsync().ConfigureAwait(false);
        }


        public void SubmitExternalEvents(IList<PartitionEvent> evts)
        {
            this.logWorker.SubmitExternalEvents(evts);
        }

        public void SubmitInternalEvent(PartitionEvent evt)
        {
            this.logWorker.SubmitInternalEvent(evt);
        }

        private async Task IdleLoop()
        {
            while (true)
            {
                await Task.Delay(StoreWorker.IdlingPeriod).ConfigureAwait(false);

                //await this.TestStorageLatency();

                if (this.terminationToken.IsCancellationRequested)
                {
                    break;
                }

                // periodically bump the store worker so it can check if enough time has elapsed for doing a checkpoint or a load publish
                this.storeWorker.Notify();
            }
        }

        private async Task TestStorageLatency()
        {
            Stopwatch stopwatch = new Stopwatch();
            var blob = this.blobManager.BlobContainer.GetBlockBlobReference("test");
            var text = DateTime.UtcNow.ToString("o");
            stopwatch.Start();
            await blob.UploadTextAsync(text).ConfigureAwait(BlobManager.CONFIGURE_AWAIT_FOR_STORAGE_CALLS);
            stopwatch.Stop();
            if (stopwatch.ElapsedMilliseconds > 1000)
            {
                this.TraceHelper.FasterPerfWarning($"CloudBlockBlob.UploadTextAsync took {stopwatch.ElapsedMilliseconds / 1000}s, which is excessive; target={blob} content={text}");
            }
        }
    }
}