using DurableTask.Core.Common;
using FASTER.core;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventSourced.Faster
{
    internal class FasterKV
    {
        private FasterKV<Key, Value, EffectTracker, TrackedObject, PartitionReadEvent, Functions> fht;

        private readonly Partition partition;
        private readonly BlobManager blobManager;
        private readonly ClientSession<Key, Value, EffectTracker, TrackedObject, PartitionReadEvent, Functions> mainSession;
        private readonly CancellationToken terminationToken;

        public FasterKV(Partition partition, BlobManager blobManager)
        {
            this.partition = partition;
            this.blobManager = blobManager;
 
            this.fht = new FASTER.core.FasterKV<FasterKV.Key, FasterKV.Value, EffectTracker, TrackedObject, PartitionReadEvent, FasterKV.Functions>(
                1L << 16,
                new Functions(partition),
                blobManager.StoreLogSettings,
                blobManager.StoreCheckpointSettings,
                new SerializerSettings<FasterKV.Key, FasterKV.Value>
                {
                    keySerializer = () => new Key.Serializer(),
                    valueSerializer = () => new Value.Serializer(),
                });
            this.mainSession = fht.NewSession();

            this.terminationToken = partition.ErrorHandler.Token;

            var _ = terminationToken.Register(
                () => {
                    try
                    {
                        this.mainSession.Dispose();
                        fht.Dispose();
                        this.blobManager.HybridLogDevice.Close();
                        this.blobManager.ObjectLogDevice.Close();
                    }
                    catch(Exception e)
                    {
                        this.blobManager.TraceHelper.FasterStorageError("Disposing FasterKV", e);
                    }
                }, 
                useSynchronizationContext: false);

            this.blobManager.TraceHelper.FasterProgress("Constructed FasterKV.");
        }

        
        public void Recover()
        {
            try
            {
                this.fht.Recover();
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        public void CompletePending()
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

        public bool TakeFullCheckpoint(long commitLogPosition, long inputQueuePosition, out Guid checkpointGuid)
        {
            try
            {
                this.blobManager.CheckpointCommitLogPosition = commitLogPosition;
                this.blobManager.CheckpointInputQueuePosition = inputQueuePosition;
                return this.fht.TakeFullCheckpoint(out checkpointGuid);
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        public async ValueTask CompleteCheckpointAsync()
        {
            try
            {
                await this.fht.CompleteCheckpointAsync(this.terminationToken);
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        public Guid StartIndexCheckpoint()
        {
            try
            {
                bool success = this.fht.TakeIndexCheckpoint(out var token);

                if (!success)
                    throw new InvalidOperationException("Faster refused index checkpoint");

                return token;
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        public Guid StartStoreCheckpoint(long commitLogPosition, long inputQueuePosition)
        {
            try
            {
                this.blobManager.CheckpointCommitLogPosition = commitLogPosition;
                this.blobManager.CheckpointInputQueuePosition = inputQueuePosition;
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

        public EffectTracker NoInput = null;

        // fast path read, synchronous, on the main session
        public void Read(PartitionReadEvent readEvent, EffectTracker effectTracker)
        {
            try
            {
                FasterKV.Key key = readEvent.ReadTarget;
                TrackedObject target = null;

                // try to read directly (fast path)
                var status = this.mainSession.Read(ref key, ref effectTracker, ref target, readEvent, 0);

                switch (status)
                {
                    case Status.NOTFOUND:
                    case Status.OK:
                        effectTracker.ProcessRead(readEvent, target);
                        break;

                    case Status.PENDING:
                        // read continuation will be called when complete
                        break;

                    case Status.ERROR:
                        throw new Exception("Faster"); //TODO
                }
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

         // retrieve or create the tracked object, asynchronously if necessary, on the main session
        public async ValueTask<TrackedObject> GetOrCreate(Key key)
        {
            try
            {
                var (status, target) = await this.mainSession.ReadAsync(key, null, false, this.terminationToken);

                if (status == Status.NOTFOUND)
                {
                    target = TrackedObjectKey.Factory(key);
                    await this.mainSession.UpsertAsync(key, target, false, this.terminationToken);
                }
                else if (status != Status.OK)
                {
                    throw new Exception("Faster semantics?"); //TODO
                }

                target.Partition = this.partition;
                return target;
            }
            catch (Exception exception)
                when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        public ValueTask ProcessEffectOnTrackedObject(TrackedObjectKey k, EffectTracker tracker)
        {
            try
            {
                return this.mainSession.RMWAsync(k, tracker, false, this.terminationToken);
            }
            catch (Exception exception)
               when (this.terminationToken.IsCancellationRequested && !Utils.IsFatal(exception))
            {
                throw new OperationCanceledException("Partition was terminated.", exception, this.terminationToken);
            }
        }

        private async Task<StateDump> DumpCurrentState()
        {
            try
            {
                var result = new StateDump();
                var keysToLookup = new HashSet<TrackedObjectKey>();

                // get the set of keys appearing in the log
                using (var iter1 = this.fht.Log.Scan(this.fht.Log.BeginAddress, this.fht.Log.TailAddress))
                {
                    while (iter1.GetNext(out RecordInfo recordInfo))
                    {
                        keysToLookup.Add(iter1.GetKey().Val);
                    }
                }

                // read the current value of each
                foreach (var k in keysToLookup)
                {
                    (Status status, TrackedObject target) = await this.mainSession.ReadAsync(k, null, false, this.terminationToken);
                    if (status == Status.OK)
                    {
                        result.OnReadComplete(target, this.partition);
                    }
                }

                return result;
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

            public override string ToString()
            {
                return Val.ToString();
            }

            public bool Equals(ref Key k1, ref Key k2)
            {
                return k1.Val.ObjectType == k2.Val.ObjectType && k1.Val.InstanceId == k2.Val.InstanceId;
            }

            public class Serializer : BinaryObjectSerializer<Key>
            {
                public override void Deserialize(ref Key obj)
                {
                    obj.Val.Deserialize(this.reader);
                }

                public override void Serialize(ref Key obj)
                {
                    obj.Val.Serialize(this.writer);
                }
            }
        }

        public struct Value
        {
            public object Val;

            public static implicit operator TrackedObject(Value v) => (TrackedObject)v.Val;
            public static implicit operator Value(TrackedObject v) => new Value() { Val = v };

            public override string ToString()
            {
                return Val.ToString();
            }

            public class Serializer : BinaryObjectSerializer<Value>
            {
                public override void Deserialize(ref Value obj)
                {
                    int count = this.reader.ReadInt32();
                    byte[] bytes = this.reader.ReadBytes(count);
                    obj.Val = DurableTask.EventSourced.Serializer.DeserializeTrackedObject(bytes);
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
                        writer.Write(trackedObject.SerializationCache.Length);
                        writer.Write(trackedObject.SerializationCache);
                    }
                }
            }
        }

        public class Functions : IFunctions<Key, Value, EffectTracker, TrackedObject, PartitionReadEvent>
        {
            private readonly Partition partition;

            public Functions(Partition partition)
            {
                this.partition = partition;
            }

            public void InitialUpdater(ref Key key, ref EffectTracker tracker, ref Value value)
            {
                var trackedObject = TrackedObjectKey.Factory(key.Val);
                trackedObject.Partition = partition;
                value.Val = trackedObject;
                tracker.ProcessEffectOn(trackedObject);
            }

            public bool InPlaceUpdater(ref Key key, ref EffectTracker tracker, ref Value value)
            {
                partition.Assert(value.Val is TrackedObject);
                TrackedObject trackedObject = value;
                trackedObject.SerializationCache = null; // cache is invalidated
                trackedObject.Partition = partition;
                tracker.ProcessEffectOn(trackedObject);
                return true;
            }

            public void CopyUpdater(ref Key key, ref EffectTracker tracker, ref Value oldValue, ref Value newValue)
            {
                // replace old object with its serialized snapshot
                partition.Assert(oldValue.Val is TrackedObject);
                TrackedObject trackedObject = oldValue;
                DurableTask.EventSourced.Serializer.SerializeTrackedObject(trackedObject);
                oldValue.Val = trackedObject.SerializationCache;

                // keep object as the new object, and apply effect
                newValue.Val = trackedObject;
                trackedObject.SerializationCache = null; // cache is invalidated
                trackedObject.Partition = partition;
                tracker.ProcessEffectOn(trackedObject);
            }

            public void SingleReader(ref Key key, ref EffectTracker _, ref Value value, ref TrackedObject dst)
            {
                var trackedObject = value.Val as TrackedObject;
                partition.Assert(trackedObject != null);
                trackedObject.Partition = partition;
                dst = value;
            }

            public void ConcurrentReader(ref Key key, ref EffectTracker _, ref Value value, ref TrackedObject dst)
            {
                var trackedObject = value.Val as TrackedObject;
                partition.Assert(trackedObject != null);
                trackedObject.Partition = partition;
                dst = value;
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

            public void ReadCompletionCallback(ref Key key, ref EffectTracker input, ref TrackedObject output, PartitionReadEvent ctx, Status status)
            {
                partition.Assert(ctx != null);

                switch (status)
                {
                    case Status.NOTFOUND:
                        input.ProcessRead(ctx, null);
                        break;

                    case Status.OK:
                        input.ProcessRead(ctx, output);
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
