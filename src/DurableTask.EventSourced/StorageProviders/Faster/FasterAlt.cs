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
using FASTER.core;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventSourced.Faster
{
    internal class FasterAlt : TrackedObjectStore
    {
        private readonly Partition partition;
        private readonly BlobManager blobManager;
        private readonly CancellationToken terminationToken;
        private readonly string prefix;
        private readonly FasterTraceHelper traceHelper;
        private readonly FasterTraceHelper detailTracer;

        private readonly Dictionary<TrackedObjectKey, CacheEntry> cache = new Dictionary<TrackedObjectKey, CacheEntry>();
        private Dictionary<TrackedObjectKey, PendingLoad> pendingLoads = new Dictionary<TrackedObjectKey, PendingLoad>();
        private List<CacheEntry> modified = new List<CacheEntry>();
        private HashSet<Guid> failedCheckpoints = new HashSet<Guid>();

        private Task checkpointTask;

        private class CacheEntry
        {
            public byte[] LastCheckpointed;
            public TrackedObject TrackedObject;
            public bool Modified;
        }

        public struct ToWrite
        {
            public TrackedObjectKey Key;
            public byte[] PreviousValue;
            public byte[] NewValue;
        }

        public struct ToRead
        {
            public byte[] PreviousValue;
            public byte[] NewValue;
            public Guid Guid;
        }

        public struct CompletedRead
        {
            public PartitionReadEvent ReadEvent;
            public EffectTracker EffectTracker;
            public TrackedObject TrackedObject;
        }

        public FasterAlt(Partition partition, BlobManager blobManager)
        {
            this.partition = partition;
            this.blobManager = blobManager;
            this.prefix = $"p{this.partition.PartitionId:D2}/store/";

            this.terminationToken = partition.ErrorHandler.Token;
            this.traceHelper = blobManager.TraceHelper;
            this.detailTracer = this.traceHelper.IsTracingAtMostDetailedLevel ? this.traceHelper : null;

            var _ = terminationToken.Register(
                () => {
                    // nothing so far
                },
                useSynchronizationContext: false);

            this.blobManager.TraceHelper.FasterProgress("Constructed FasterAlt");
        }

        public override void InitMainSession()
        {
        }

        public override void Recover(out long commitLogPosition, out long inputQueuePosition)
        {
            try
            {
                foreach (var guid in this.ReadCheckpointIntentions())
                {
                    this.failedCheckpoints.Add(guid);
                }

                var tasks = new List<Task>();

                // kick off loads for all singletons
                foreach (var key in TrackedObjectKey.GetSingletons())
                {
                    var loadTask = this.LoadAsync(key);
                    this.pendingLoads.Add(key, new PendingLoad()
                    {
                        EffectTracker = null,
                        ReadEvents = new List<PartitionReadEvent>(),
                        LoadTask = loadTask,
                    });
                    tasks.Add(loadTask);
                }

                Task.WhenAll(tasks).GetAwaiter().GetResult();

                this.CompletePending();

                var dedupState = (DedupState) this.cache[TrackedObjectKey.Dedup].TrackedObject;
                (commitLogPosition,inputQueuePosition) = dedupState.Positions;
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        public override void CompletePending()
        {
            try
            {
                var completed = this.pendingLoads.Where(p => p.Value.LoadTask.IsCompleted).ToList();

                foreach (var kvp in completed)
                {
                    this.ProcessCompletedLoad(kvp.Key, kvp.Value);
                }
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        public override ValueTask ReadyToCompletePendingAsync()
        {
            if (this.pendingLoads.Count == 0)
            {
                return default;
            }
            else
            {
                return new ValueTask(Task.WhenAny(this.pendingLoads.Select(kvp => kvp.Value.LoadTask)));
            }
        }

        public override bool TakeFullCheckpoint(long commitLogPosition, long inputQueuePosition, out Guid checkpointGuid)
        {
            try
            {
                checkpointGuid = Guid.NewGuid();
                StartStoreCheckpoint(commitLogPosition, inputQueuePosition, checkpointGuid);
                return true;
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        public async override ValueTask CompleteCheckpointAsync()
        {
            try
            {
                await this.checkpointTask.ConfigureAwait(false);
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        public override Guid StartIndexCheckpoint()
        {
            try
            {
                this.traceHelper.FasterProgress("FasterAlt.StartIndexCheckpoint Called");
                this.checkpointTask = Task.CompletedTask;
                return default;
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        public override Guid StartStoreCheckpoint(long commitLogPosition, long inputQueuePosition)
        {
            this.traceHelper.FasterProgress("FasterAlt.StartStoreCheckpoint Called");
            var guid = Guid.NewGuid();
            this.StartStoreCheckpoint(commitLogPosition, inputQueuePosition, guid);
            return guid;
        }

        internal void StartStoreCheckpoint(long commitLogPosition, long inputQueuePosition, Guid guid)
        {
            try
            {
                // update the positions
                var dedupState = cache[TrackedObjectKey.Dedup];
                dedupState.TrackedObject.SerializationCache = null;
                ((DedupState)dedupState.TrackedObject).Positions = (commitLogPosition, inputQueuePosition);
                if (!dedupState.Modified)
                {
                    dedupState.Modified = true;
                    this.modified.Add(dedupState);
                }

                // figure out which objects need to be written back
                var toWrite = new List<ToWrite>();
                foreach (var cacheEntry in this.modified)
                {
                    Serializer.SerializeTrackedObject(cacheEntry.TrackedObject);
                    toWrite.Add(new ToWrite()
                    {
                        Key = cacheEntry.TrackedObject.Key,
                        PreviousValue = cacheEntry.LastCheckpointed,
                        NewValue = cacheEntry.TrackedObject.SerializationCache,
                    });
                    cacheEntry.LastCheckpointed = cacheEntry.TrackedObject.SerializationCache;
                    cacheEntry.Modified = false;
                }
                this.modified.Clear();

                this.checkpointTask = Task.Run(() => WriteCheckpointAsync(toWrite, guid));
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        private async Task WriteCheckpointAsync(List<ToWrite> toWrite, Guid guid)
        {
            try
            {
                // the intention file instructs subsequent recoveries to ignore updates should we fail in the middle
                await this.WriteCheckpointIntention(guid).ConfigureAwait(false);

                var guidbytes = guid.ToByteArray();
                var tasks = new List<Task>();
                foreach (var entry in toWrite)
                {      
                    tasks.Add(this.StoreAsync(guidbytes, entry));
                }

                await Task.WhenAll(tasks).ConfigureAwait(false);
            }
            catch (Exception e) when (!Utils.IsFatal(e))
            {
                this.blobManager.PartitionErrorHandler.HandleError(nameof(WriteCheckpointAsync), "Failed to write checkpoint", e, true, this.blobManager.PartitionErrorHandler.IsTerminated);
                throw;
            }
        }

        public override Task FinalizeCheckpointCompletedAsync(Guid guid)
        {
            // we have finished the checkpoint; it is committed by removing the intention file
            return this.RemoveCheckpointIntention(guid);
        }

        // perform a query
        public override Task QueryAsync(PartitionQueryEvent queryEvent, EffectTracker effectTracker)
        {
            try
            {
                // TODO
                throw new NotImplementedException();
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        public class PendingLoad
        {
            public Task<ToRead> LoadTask;
            public EffectTracker EffectTracker;
            public List<PartitionReadEvent> ReadEvents;
        }

        // kick off a read of a tracked object, completing asynchronously if necessary
        public override void ReadAsync(PartitionReadEvent readEvent, EffectTracker effectTracker)
        {
            try
            {
                if (readEvent.Prefetch.HasValue)
                {
                    TryRead(readEvent.Prefetch.Value);
                }

                TryRead(readEvent.ReadTarget);

                void TryRead(TrackedObjectKey key)
                {
                    if (this.cache.TryGetValue(key, out var entry))
                    {
                        this.StoreStats.HitCount++;
                        effectTracker.ProcessReadResult(readEvent, key, entry.TrackedObject);
                    }
                    else if (this.pendingLoads.TryGetValue(key, out var pendingLoad))
                    {
                        this.StoreStats.HitCount++;
                        pendingLoad.EffectTracker = effectTracker;
                        pendingLoad.ReadEvents.Add(readEvent);
                    }
                    else
                    {
                        this.StoreStats.MissCount++;
                        this.pendingLoads.Add(key, new PendingLoad()
                        {
                            EffectTracker = effectTracker,
                            ReadEvents = new List<PartitionReadEvent>() { readEvent },
                            LoadTask = this.LoadAsync(key),
                        });
                    }
                }
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        // read a tracked object on the main session and wait for the response (only one of these is executing at a time)
        public override async ValueTask<TrackedObject> ReadAsync(FasterKV.Key key, EffectTracker effectTracker)
        {
            if (this.cache.TryGetValue(key.Val, out var entry))
            {
                this.StoreStats.HitCount++;
                return entry.TrackedObject;
            }
            else if (this.pendingLoads.TryGetValue(key, out var pendingLoad))
            {
                this.StoreStats.HitCount++;
                await pendingLoad.LoadTask.ConfigureAwait(false);
                return this.ProcessCompletedLoad(key, pendingLoad);
            }
            else
            {
                this.StoreStats.MissCount++;
                this.pendingLoads.Add(key, pendingLoad = new PendingLoad()
                {
                    EffectTracker = effectTracker,
                    ReadEvents = new List<PartitionReadEvent>(),
                    LoadTask = this.LoadAsync(key),
                });
                await pendingLoad.LoadTask.ConfigureAwait(false);
                return this.ProcessCompletedLoad(key, pendingLoad);
            }
        }

        private CacheEntry ProcessStorageRecord(TrackedObjectKey key, ToRead toRead)
        {
            byte[] bytes;
            if (!this.failedCheckpoints.Contains(toRead.Guid))
            {
                bytes = toRead.NewValue;
            }
            else
            {
                bytes = toRead.PreviousValue;
            }
            var trackedObject = (bytes == null) ? TrackedObjectKey.Factory(key) : Serializer.DeserializeTrackedObject(bytes);
            trackedObject.Partition = this.partition;

            return new CacheEntry()
            {
                LastCheckpointed = bytes,
                TrackedObject = trackedObject,
            };
        }

        private TrackedObject ProcessCompletedLoad(TrackedObjectKey key, PendingLoad pendingLoad)
        {
            var cacheEntry = this.ProcessStorageRecord(key, pendingLoad.LoadTask.Result);
        
            // install in cache
            this.cache.Add(key, cacheEntry);

            // process the read events that were waiting
            foreach (var evt in pendingLoad.ReadEvents)
            {
                pendingLoad.EffectTracker.ProcessReadResult(evt, key, cacheEntry.TrackedObject);
            }

            // remove from dictionary
            this.pendingLoads.Remove(key);

            return cacheEntry.TrackedObject;
        }

        // create a tracked object on the main session (only one of these is executing at a time)
        public override ValueTask<TrackedObject> CreateAsync(FasterKV.Key key)
        {
            try
            {
                var trackedObject = TrackedObjectKey.Factory(key.Val);
                trackedObject.Partition = this.partition;
                var cacheEntry = new CacheEntry()
                {
                    LastCheckpointed = null,
                    Modified = true,
                    TrackedObject = trackedObject,
                };
                this.cache.Add(key, cacheEntry);
                this.modified.Add(cacheEntry);
                return new ValueTask<TrackedObject>(trackedObject);
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        public override async ValueTask ProcessEffectOnTrackedObject(FasterKV.Key key, EffectTracker effectTracker)
        {
            try
            {
                if (!this.cache.TryGetValue(key, out var cacheEntry))
                {
                    this.partition.Assert(!this.pendingLoads.ContainsKey(key));
                    var storageRecord = await this.LoadAsync(key);
                    cacheEntry = this.ProcessStorageRecord(key, storageRecord);
                    this.cache.Add(key, cacheEntry);
                }
                var trackedObject = cacheEntry.TrackedObject;
                trackedObject.SerializationCache = null;
                effectTracker.ProcessEffectOn(trackedObject);
                if (!cacheEntry.Modified)
                {
                    cacheEntry.Modified = true;
                    this.modified.Add(cacheEntry);
                }
            }
            catch (Exception exception)
               when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        public override Task<SortedDictionary<TrackedObjectKey, TrackedObject>> EnumerateAllTrackedObjects(EffectTracker effectTracker, bool instanceOnly = false)
        {
            // TODO
            return default;
        }

        #region storage access operation

        private CloudBlockBlob GetBlob(TrackedObjectKey key)
        {
            StringBuilder blobName = new StringBuilder(this.prefix);
            blobName.Append(key.ObjectType.ToString());
            if (!key.IsSingleton)
            {
                blobName.Append('/');
                blobName.Append(WebUtility.UrlEncode(key.InstanceId));
            }
            return this.blobManager.BlobContainer.GetBlockBlobReference(blobName.ToString());
        }

        private async Task<ToRead> LoadAsync(TrackedObjectKey key)
        {
            try
            {
                this.detailTracer?.FasterProgress($"FasterAlt.LoadAsync Called {key}");
                var blob = GetBlob(key);
                using var stream = new MemoryStream();
                await this.blobManager.ConfirmLeaseIsGoodForAWhileAsync().ConfigureAwait(false);
                await blob.DownloadRangeToStreamAsync(stream, null, null, this.blobManager.PartitionErrorHandler.Token).ConfigureAwait(false);
                stream.Seek(0, SeekOrigin.Begin);
                using var reader = new BinaryReader(stream, Encoding.UTF8);
                var toRead = new ToRead();
                var previousLength = reader.ReadInt32();
                toRead.PreviousValue = previousLength > 0 ? reader.ReadBytes(previousLength) : null;
                var newLength = reader.ReadInt32();
                toRead.NewValue = newLength > 0 ? reader.ReadBytes(newLength) : null;
                toRead.Guid = new Guid(reader.ReadBytes(16));
                this.detailTracer?.FasterProgress($"FasterAlt.LoadAsync Returned {key}");
                return toRead;
            }
            catch (StorageException ex) when (BlobUtils.BlobDoesNotExist(ex))
            {
                this.detailTracer?.FasterProgress($"FasterAlt.LoadAsync Returned {key} 404");
                return default;
            }
            catch (Exception e) when (!Utils.IsFatal(e))
            {
                this.blobManager.PartitionErrorHandler.HandleError(nameof(LoadAsync), "Failed to read object from storage", e, true, this.blobManager.PartitionErrorHandler.IsTerminated);
                throw;
            }
        }

        private async Task StoreAsync(byte[] guid, ToWrite entry)
        {
            try
            {
                this.detailTracer?.FasterProgress($"FasterAlt.LoadAsync Called {entry.Key}");
                var blob = GetBlob(entry.Key);
                using var stream = new MemoryStream();
                using var writer = new BinaryWriter(stream, Encoding.UTF8);
                if (entry.PreviousValue == null)
                {
                    writer.Write(0);
                }
                else
                {
                    writer.Write(entry.PreviousValue.Length);
                    writer.Write(entry.PreviousValue);
                }
                if (entry.NewValue == null)
                {
                    writer.Write(0);
                }
                else
                {
                    writer.Write(entry.NewValue.Length);
                    writer.Write(entry.NewValue);
                }
                writer.Write(guid);
                writer.Flush();
                long length = stream.Position;
                stream.Seek(0, SeekOrigin.Begin);
                await this.blobManager.ConfirmLeaseIsGoodForAWhileAsync().ConfigureAwait(false);
                await blob.UploadFromStreamAsync(stream, this.blobManager.PartitionErrorHandler.Token).ConfigureAwait(false);
                this.detailTracer?.FasterProgress($"FasterAlt.LoadAsync Done {entry.Key} ({length} bytes)");
            }
            catch (Exception e) when (!Utils.IsFatal(e))
            {
                this.blobManager.PartitionErrorHandler.HandleError(nameof(StoreAsync), "Failed to write object to storage", e, true, this.blobManager.PartitionErrorHandler.IsTerminated);
                throw;
            }
        }

        private async Task WriteCheckpointIntention(Guid guid)
        {
            try
            {
                var blob = this.blobManager.BlobContainer.GetBlockBlobReference($"p{this.partition.PartitionId:D2}/incomplete-checkpoints/{guid}");
                await this.blobManager.ConfirmLeaseIsGoodForAWhileAsync().ConfigureAwait(false);
                await blob.UploadTextAsync("", this.blobManager.PartitionErrorHandler.Token);
            }
            catch (Exception e) when (!Utils.IsFatal(e))
            {
                this.blobManager.PartitionErrorHandler.HandleError(nameof(WriteCheckpointIntention), "Failed to write checkpoint intention to storage", e, true, this.blobManager.PartitionErrorHandler.IsTerminated);
                throw;
            }
        }

        private async Task RemoveCheckpointIntention(Guid guid)
        {
            try
            {
                var blob = this.blobManager.BlobContainer.GetBlockBlobReference($"p{this.partition.PartitionId:D2}/incomplete-checkpoints/{guid}");
                await this.blobManager.ConfirmLeaseIsGoodForAWhileAsync().ConfigureAwait(false);
                await blob.DeleteAsync(this.blobManager.PartitionErrorHandler.Token);
            }
            catch (Exception e) when (!Utils.IsFatal(e))
            {
                this.blobManager.PartitionErrorHandler.HandleError(nameof(RemoveCheckpointIntention), "Failed to remove checkpoint intention from storage", e, true, this.blobManager.PartitionErrorHandler.IsTerminated);
                throw;
            }
        }

        private IEnumerable<Guid> ReadCheckpointIntentions()
        {
            try
            {
                var directory = this.blobManager.BlobContainer.GetDirectoryReference($"p{this.partition.PartitionId:D2}/incomplete-checkpoints/");
                var checkPoints = directory.ListBlobs().ToList();
                this.blobManager.PartitionErrorHandler.Token.ThrowIfCancellationRequested();
                return checkPoints.Select((item) =>
                {
                    var segments = item.Uri.Segments;
                    var guid = Guid.Parse(segments[segments.Length - 1]);
                    return guid;
                });
            }
            catch (Exception e) when (!Utils.IsFatal(e))
            {
                this.blobManager.PartitionErrorHandler.HandleError(nameof(ReadCheckpointIntentions), "Failed to read checkpoint intentions from storage", e, true, this.blobManager.PartitionErrorHandler.IsTerminated);
                throw;
            }
        }

        #endregion
    }
}
