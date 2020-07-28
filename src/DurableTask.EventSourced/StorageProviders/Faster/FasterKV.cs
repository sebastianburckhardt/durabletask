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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventSourced.Faster
{
    internal class FasterKV : TrackedObjectStore
    {
        private FasterKV<Key, Value, EffectTracker, TrackedObject, PartitionReadEvent, Functions> fht;

        private readonly Partition partition;
        private readonly BlobManager blobManager;
        private readonly CancellationToken terminationToken;

        private ClientSession<Key, Value, EffectTracker, TrackedObject, PartitionReadEvent, Functions> mainSession;

        internal const long HashTableSize = 1L << 16;

        // We currently place all PSFs into a single group with a single TPSFKey type
        internal const int PSFCount = 1;
        internal IPSF RuntimeStatusPsf;
        internal IPSF CreatedTimePsf;
        internal IPSF InstanceIdPrefixPsf;

        public FasterKV(Partition partition, BlobManager blobManager)
        {
            this.partition = partition;
            this.blobManager = blobManager;

            partition.ErrorHandler.Token.ThrowIfCancellationRequested();

            this.fht = new FasterKV<Key, Value, EffectTracker, TrackedObject, PartitionReadEvent, Functions>(
                HashTableSize,
                new Functions(partition, this.StoreStats),
                blobManager.StoreLogSettings(partition.NumberPartitions()),
                blobManager.StoreCheckpointSettings,
                new SerializerSettings<Key, Value>
                {
                    keySerializer = () => new Key.Serializer(),
                    valueSerializer = () => new Value.Serializer(this.StoreStats),
                });

            if (partition.Settings.UsePSFQueries)
            {
                int groupOrdinal = 0;
                var psfs = fht.RegisterPSF(this.blobManager.CreatePSFRegistrationSettings<PSFKey>(partition.NumberPartitions(), groupOrdinal++),
                                           (nameof(this.RuntimeStatusPsf), (k, v) => v.Val is InstanceState state
                                                                                ? (PSFKey?)new PSFKey(state.OrchestrationState.OrchestrationStatus)
                                                                                : null),
                                           (nameof(this.CreatedTimePsf), (k, v) => v.Val is InstanceState state
                                                                                ? (PSFKey?)new PSFKey(state.OrchestrationState.CreatedTime)
                                                                                : null),
                                           (nameof(this.InstanceIdPrefixPsf), (k, v) => v.Val is InstanceState state
                                                                                ? (PSFKey?)new PSFKey(state.InstanceId)
                                                                                : null));

                this.RuntimeStatusPsf = psfs[0];
                this.CreatedTimePsf = psfs[1];
                this.InstanceIdPrefixPsf = psfs[2];
            }

            this.terminationToken = partition.ErrorHandler.Token;

            var _ = terminationToken.Register(
                () => {
                    try
                    {
                        this.mainSession?.Dispose();
                        fht.Dispose();
                        this.blobManager.HybridLogDevice.Close();
                        this.blobManager.ObjectLogDevice.Close();
                        this.blobManager.ClosePSFDevices();
                    }
                    catch(Exception e)
                    {
                        this.blobManager.TraceHelper.FasterStorageError("Disposing FasterKV", e);
                    }
                }, 
                useSynchronizationContext: false);

            this.blobManager.TraceHelper.FasterProgress("Constructed FasterKV");
        }

        public override void InitMainSession()
        {
            this.mainSession = fht.NewSession();
        }

        public override void Recover(out long commitLogPosition, out long inputQueuePosition)
        {
            try
            {
                this.fht.Recover();
                this.mainSession = fht.NewSession();
                commitLogPosition = this.blobManager.CheckpointInfo.CommitLogPosition;
                inputQueuePosition = this.blobManager.CheckpointInfo.InputQueuePosition;
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
                this.mainSession.CompletePending(false, false);
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        public override ValueTask ReadyToCompletePendingAsync()
        {
            return this.mainSession.ReadyToCompletePendingAsync(this.terminationToken);
        }

        public override bool TakeFullCheckpoint(long commitLogPosition, long inputQueuePosition, out Guid checkpointGuid)
        {
            try
            {
                this.blobManager.CheckpointInfo.CommitLogPosition = commitLogPosition;
                this.blobManager.CheckpointInfo.InputQueuePosition = inputQueuePosition;
                return this.fht.TakeFullCheckpoint(out checkpointGuid);
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        public override async ValueTask CompleteCheckpointAsync()
        {
            try
            {
                await this.fht.CompleteCheckpointAsync(this.terminationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        public override Task FinalizeCheckpointCompletedAsync(Guid guid)
        {
            return this.blobManager.FinalizeCheckpointCompletedAsync();
        }

        public override Guid StartIndexCheckpoint()
        {
            try
            {
                return this.fht.TakeIndexCheckpoint(out var token)
                    ? token
                    : throw new InvalidOperationException("Faster refused index checkpoint");
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        public override Guid StartStoreCheckpoint(long commitLogPosition, long inputQueuePosition)
        {
            try
            {
                this.blobManager.CheckpointInfo.CommitLogPosition = commitLogPosition;
                this.blobManager.CheckpointInfo.InputQueuePosition = inputQueuePosition;
                bool success = this.fht.TakeHybridLogCheckpoint(out var token);

                if (!success)
                    throw new InvalidOperationException("Faster refused store checkpoint");

                // according to Badrish this ensures proper fencing w.r.t. session
                this.mainSession.Refresh();

                return token;
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        // perform a query
        public override async Task QueryAsync(PartitionQueryEvent queryEvent, EffectTracker effectTracker)
        {
            try
            {
                // TODO: Resolve issue with File storage => breaks Azure stuff in EventSourcedOrchestrationService
                // TODO: Perf of EnumerateAllTrackedObjects

                IEnumerable<TrackedObject> queryPSFs()
                {
                    // Issue the PSF query. Note that pending operations will be completed before this returns.
                    var querySpec = new List<(IPSF, IEnumerable<PSFKey>)>();
                    if (queryEvent.HasRuntimeStatus)
                        querySpec.Add((this.RuntimeStatusPsf, queryEvent.RuntimeStatus.Select(s => new PSFKey(s))));
                    if (queryEvent.CreatedTimeFrom.HasValue || queryEvent.CreatedTimeTo.HasValue)
                    {
                        IEnumerable<PSFKey> enumerateDateBinKeys()
                        {
                            var to = queryEvent.CreatedTimeTo ?? DateTime.UtcNow;
                            var from = queryEvent.CreatedTimeFrom ?? to.AddDays(-7);   // TODO Some default so we don't have to iterate from the first possible date
                            for (var dt = from; dt <= to; dt += PSFKey.DateBinInterval)
                                yield return new PSFKey(dt);
                        }
                        querySpec.Add((this.CreatedTimePsf, enumerateDateBinKeys()));
                    }
                    if (!string.IsNullOrWhiteSpace(queryEvent.InstanceIdPrefix))
                        querySpec.Add((this.InstanceIdPrefixPsf, new[] { new PSFKey(queryEvent.InstanceIdPrefix) }));
                    var querySettings = new PSFQuerySettings
                    {
                        // This is a match-all-PSFs enumeration so do not continue after any PSF has hit EOS
                        OnStreamEnded = (unusedPsf, unusedIndex) => false
                    };
                    return this.mainSession.QueryPSF(querySpec, matches => matches.All(b => b), querySettings)
                                           .Select(providerData => (TrackedObject)providerData.GetValue());
                }

                // create a individual session for this query so the main session can be used
                // while the query is progressing.
                using (var session = this.fht.NewSession())
                {
                    var trackedObjects = (this.partition.Settings.UsePSFQueries && queryEvent.IsSet)
                    ? queryPSFs()
                    : (await this.EnumerateAllTrackedObjects(effectTracker, instanceOnly: true)).Values;

                    effectTracker.ProcessQueryResult(queryEvent, trackedObjects);
                }
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
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

                void TryRead(Key key)
                {
                    TrackedObject target = null;
                    var status = this.mainSession.Read(ref key, ref effectTracker, ref target, readEvent, 0);
                    switch (status)
                    {
                        case Status.NOTFOUND:
                        case Status.OK:
                            // fast path: we hit in the cache and complete the read
                            this.StoreStats.HitCount++;
                            effectTracker.ProcessReadResult(readEvent, key, target);
                            break;

                        case Status.PENDING:
                            // slow path: read continuation will be called when complete
                            this.StoreStats.MissCount++;
                            break;

                        case Status.ERROR:
                            throw new Exception("Faster"); //TODO
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
        public override async ValueTask<TrackedObject> ReadAsync(Key key, EffectTracker effectTracker)
        {
            try
            {
                var result = await this.mainSession.ReadAsync(ref key, ref effectTracker, context:null, this.terminationToken).ConfigureAwait(false);
                var (status, output) = result.CompleteRead();
                return output;
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        // create a tracked object on the main session (only one of these is executing at a time)
        public override async ValueTask<TrackedObject> CreateAsync(Key key)
        {
            try
            {              
                TrackedObject newObject = TrackedObjectKey.Factory(key);
                newObject.Partition = this.partition;
                Value newValue = newObject;
                await this.mainSession.UpsertAsync(ref key, ref newValue, null, false, this.terminationToken).ConfigureAwait(false);
                return newObject;
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        public override ValueTask ProcessEffectOnTrackedObject(Key k, EffectTracker tracker)
        {
            try
            {
                return this.mainSession.RMWAsync(ref k, ref tracker, null, false, this.terminationToken);
            }
            catch (Exception exception)
               when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        public override async Task<SortedDictionary<TrackedObjectKey, TrackedObject>> EnumerateAllTrackedObjects(EffectTracker effectTracker, bool instanceOnly = false)
        {
            // TODOperf: Performance of getting all tracked objects
            var results = new SortedDictionary<TrackedObjectKey, TrackedObject>(new TrackedObjectKey.Comparer());
            var keysToLookup = new HashSet<TrackedObjectKey>();

            // get the set of keys appearing in the log
            using (var iter1 = this.fht.Log.Scan(this.fht.Log.BeginAddress, this.fht.Log.TailAddress))
            {
                while (iter1.GetNext(out RecordInfo recordInfo) && !recordInfo.Tombstone)
                {
                    TrackedObjectKey key = iter1.GetKey().Val;
                    if (!instanceOnly || key.ObjectType == TrackedObjectKey.TrackedObjectType.Instance)
                        keysToLookup.Add(key);
                }
            }

            // read the current value of each
            foreach (var k in keysToLookup)
            {
                TrackedObject target = await this.ReadAsync(k, effectTracker).ConfigureAwait(false);
                if (target != null)
                {
                    results.Add(k, target);
                }
            }

            return results;
        }

        private async Task<string> DumpCurrentState(EffectTracker effectTracker)    // TODO unused
        {
            try
            {
                SortedDictionary<TrackedObjectKey, TrackedObject> results = await EnumerateAllTrackedObjects(effectTracker).ConfigureAwait(false);

                var stringBuilder = new StringBuilder();
                foreach (var trackedObject in results.Values)
                {
                    stringBuilder.Append(trackedObject.ToString());
                    stringBuilder.AppendLine();
                }
                return stringBuilder.ToString();
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        public struct Key : IFasterEqualityComparer<Key>
        {
            public TrackedObjectKey Val;

            public static implicit operator TrackedObjectKey(Key k) => k.Val;
            public static implicit operator Key(TrackedObjectKey k) => new Key() { Val = k };

            public long GetHashCode64(ref Key k)
            {
                unchecked
                {
                    // Compute an FNV hash
                    var hash = 0xcbf29ce484222325ul; // FNV_offset_basis
                    var prime = 0x100000001b3ul; // FNV_prime

                    // hash the kind
                    hash ^= (byte)k.Val.ObjectType;
                    hash *= prime;

                    // hash the instance id, if applicable
                    if (k.Val.InstanceId != null)
                    {
                        for (int i = 0; i < k.Val.InstanceId.Length; i++)
                        {
                            hash ^= k.Val.InstanceId[i];
                            hash *= prime;
                        }
                    }

                    return (long)hash;
                }
            }

            public override string ToString() => Val.ToString();

            public bool Equals(ref Key k1, ref Key k2) 
                => k1.Val.ObjectType == k2.Val.ObjectType && k1.Val.InstanceId == k2.Val.InstanceId;

            public class Serializer : BinaryObjectSerializer<Key>
            {
                public override void Deserialize(ref Key obj) => obj.Val.Deserialize(this.reader);

                public override void Serialize(ref Key obj) => obj.Val.Serialize(this.writer);
            }
        }

        public struct Value
        {
            public object Val;

            public static implicit operator TrackedObject(Value v) => (TrackedObject)v.Val;
            public static implicit operator Value(TrackedObject v) => new Value() { Val = v };

            public override string ToString() => Val.ToString();

            public class Serializer : BinaryObjectSerializer<Value>
            {
                private readonly StoreStatistics storeStats;

                public Serializer(StoreStatistics storeStats)
                {
                    this.storeStats = storeStats;
                }

                public override void Deserialize(ref Value obj)
                {
                    int count = this.reader.ReadInt32();
                    byte[] bytes = this.reader.ReadBytes(count);
                    var trackedObject = DurableTask.EventSourced.Serializer.DeserializeTrackedObject(bytes);
                    //if (trackedObject.Key.IsSingleton)
                    //{
                    //    this.storeStats.A++;
                    //    this.storeStats.B += bytes.Length;
                    //}
                    //else if (trackedObject is InstanceState i)
                    //{
                    //    this.storeStats.C++;
                    //    this.storeStats.D += bytes.Length;
                    //    this.storeStats.U.Add(i.InstanceId);
                    //}
                    //else if (trackedObject is HistoryState h)
                    //{
                    //    this.storeStats.E++;
                    //    this.storeStats.F += bytes.Length;
                    //    this.storeStats.UU.Add(h.InstanceId);
                    //}
                    obj.Val = trackedObject;
                    this.storeStats.Deserialize++;
                }

                public override void Serialize(ref Value obj)
                {
                    if (obj.Val is byte[] serialized)
                    {
                        writer.Write(serialized.Length);
                        writer.Write(serialized);
                    }
                    else
                    {
                        TrackedObject trackedObject = obj;
                        DurableTask.EventSourced.Serializer.SerializeTrackedObject(trackedObject);
                        this.storeStats.Serialize++;
                        writer.Write(trackedObject.SerializationCache.Length);
                        writer.Write(trackedObject.SerializationCache);
                    }
                }
            }
        }

        public class Functions : IFunctions<Key, Value, EffectTracker, TrackedObject, PartitionReadEvent>
        {
            private readonly Partition partition;
            private readonly StoreStatistics stats;

            public Functions(Partition partition, StoreStatistics stats)
            {
                this.partition = partition;
                this.stats = stats;
            }

            public void InitialUpdater(ref Key key, ref EffectTracker tracker, ref Value value)
            {
                var trackedObject = TrackedObjectKey.Factory(key.Val);
                stats.Create++;
                trackedObject.Partition = partition;
                value.Val = trackedObject;
                tracker.ProcessEffectOn(trackedObject);
                stats.Modify++;
            }

            public bool InPlaceUpdater(ref Key key, ref EffectTracker tracker, ref Value value)
            {
                partition.Assert(value.Val is TrackedObject);
                TrackedObject trackedObject = value;
                trackedObject.SerializationCache = null; // cache is invalidated
                trackedObject.Partition = partition;
                tracker.ProcessEffectOn(trackedObject);
                stats.Modify++;
                return true;
            }

            public void CopyUpdater(ref Key key, ref EffectTracker tracker, ref Value oldValue, ref Value newValue)
            {
                stats.Copy++;

                // replace old object with its serialized snapshot
                partition.Assert(oldValue.Val is TrackedObject);
                TrackedObject trackedObject = oldValue;
                DurableTask.EventSourced.Serializer.SerializeTrackedObject(trackedObject);
                stats.Serialize++;
                oldValue.Val = trackedObject.SerializationCache;

                // keep object as the new object, and apply effect
                newValue.Val = trackedObject;
                trackedObject.SerializationCache = null; // cache is invalidated
                trackedObject.Partition = partition;
                tracker.ProcessEffectOn(trackedObject);
                stats.Modify++;
            }

            public void SingleReader(ref Key key, ref EffectTracker _, ref Value value, ref TrackedObject dst)
            {
                var trackedObject = value.Val as TrackedObject;
                partition.Assert(trackedObject != null);
                trackedObject.Partition = partition;
                dst = value;
                stats.Read++;
            }

            public void ConcurrentReader(ref Key key, ref EffectTracker _, ref Value value, ref TrackedObject dst)
            {
                var trackedObject = value.Val as TrackedObject;
                partition.Assert(trackedObject != null);
                trackedObject.Partition = partition;
                dst = value;
                stats.Read++;
            }

            public void SingleWriter(ref Key key, ref Value src, ref Value dst)
            {
                dst.Val = src.Val;
            }

            public bool ConcurrentWriter(ref Key key, ref Value src, ref Value dst)
            {
                dst.Val = src.Val;
                return true;
            }

            public void ReadCompletionCallback(ref Key key, ref EffectTracker tracker, ref TrackedObject output, PartitionReadEvent evt, Status status)
            {
                // the result is passed on to the read event
                switch (status)
                {
                    case Status.NOTFOUND:
                        tracker.ProcessReadResult(evt, key, null);
                        break;

                    case Status.OK:
                        tracker.ProcessReadResult(evt, key, output);
                        break;

                    case Status.PENDING:
                    case Status.ERROR:
                        throw new Exception("Faster"); //TODO
                }
            }

            public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint) { }
            public void RMWCompletionCallback(ref Key key, ref EffectTracker input, PartitionReadEvent ctx, Status status) { }
            public void UpsertCompletionCallback(ref Key key, ref Value value, PartitionReadEvent ctx) { }
            public void DeleteCompletionCallback(ref Key key, PartitionReadEvent ctx) { }
        }
    }
}
