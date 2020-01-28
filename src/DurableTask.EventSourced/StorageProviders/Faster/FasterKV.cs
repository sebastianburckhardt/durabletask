using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DurableTask.Core;
using DurableTask.Core.Tracking;
using FASTER.core;
using Microsoft.Win32.SafeHandles;
using Mono.Posix;

namespace DurableTask.EventSourced.Faster
{
    internal class FasterKV : FasterKV<FasterKV.Key, FasterKV.Value, PartitionEvent, TrackedObject, Empty, FasterKV.Functions>, IDisposable
    {
        private readonly Partition partition;
        private readonly BlobManager blobManager;
        private readonly CancellationTokenSource shutdown;
        private readonly ClientSession<Key, Value, PartitionEvent, TrackedObject, Empty, Functions> mainSession;

        public FasterKV(Partition partition, BlobManager blobManager)
            : base(
                1L << 17,
                new Functions(partition),
                new LogSettings
                {
                    LogDevice = blobManager.HybridLogDevice,
                    ObjectLogDevice = blobManager.ObjectLogDevice,
                    MemorySizeBits = 29,
                },
                new CheckpointSettings
                {
                    CheckpointManager = blobManager,
                    CheckPointType = CheckpointType.FoldOver,
                },
                new SerializerSettings<FasterKV.Key, FasterKV.Value>
                {
                    keySerializer = () => new Key.Serializer(),
                    valueSerializer = () => new Value.Serializer(),
                })
        {
            this.partition = partition;
            this.blobManager = blobManager;
            this.shutdown = new CancellationTokenSource();
            this.mainSession = this.NewSession();
        }

        public new void Dispose()
        {
            shutdown.Cancel();
            this.mainSession.Dispose();
            base.Dispose();
            this.blobManager.HybridLogDevice.Close();
            this.blobManager.ObjectLogDevice.Close();
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
            public TrackedObject Val;

            public static implicit operator TrackedObject(Value v) => v.Val;
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
                    if (obj.Val.SerializedSnapshot == null)
                    {
                        DurableTask.EventSourced.Serializer.SerializeTrackedObject(obj.Val);
                    }
                    writer.Write(obj.Val.SerializedSnapshot.Length);
                    writer.Write(obj.Val.SerializedSnapshot);
                }
            }
        }

        public PartitionEvent NoInput = new TimerFired() { }; // just a dummy non-null object

        // fast path read, synchronous, on the main session
        public void Read(StorageAbstraction.IReadContinuation readContinuation, Partition partition)
        {
            FasterKV.Key key = readContinuation.ReadTarget;
            TrackedObject target = null;

            // try to read directly (fast path)
            var status = this.mainSession.Read(ref key, ref this.NoInput, ref target, Empty.Default, 0);

            switch (status)
            {
                case Status.NOTFOUND:
                    readContinuation.OnReadComplete(null);
                    break;

                case Status.OK:
                    readContinuation.OnReadComplete(target);
                    break;

                case Status.PENDING:
                    // we missed in memory. Go into the slow path, 
                    // which handles the request asynchronosly in a fresh session.
                    _ = this.AsynchronousReadTask(key, readContinuation, partition);
                    break;

                case Status.ERROR:
                    throw new Exception("Faster"); //TODO
            }
        }

        // slow path read (taken on miss), one its own session. This is not awaited.
        private async ValueTask AsynchronousReadTask(FasterKV.Key key, StorageAbstraction.IReadContinuation readContinuation, Partition partition)
        {
            try
            {
                using (var session = this.NewSession())
                {
                    var (status, target) = await session.ReadAsync(key, this.NoInput, false, this.shutdown.Token);

                    switch (status)
                    {
                        case Status.NOTFOUND:
                            readContinuation.OnReadComplete(null);
                            break;

                        case Status.OK:
                            // now that we have loaded the object into memory, resubmit
                            partition.State.ScheduleRead(readContinuation);
                            break;

                        default:
                            throw new Exception("Faster"); //TODO
                    }
                }
            } 
            catch(Exception e)
            {
                partition.ReportError(nameof(AsynchronousReadTask), e);
            }
        }

        // retrieve or create the tracked object, asynchronously if necessary, on the one session
        public async ValueTask<TrackedObject> GetOrCreate(Key key)
        {
            var (status, target) = await this.mainSession.ReadAsync(key, this.NoInput, false, this.shutdown.Token);
            if (status == Status.NOTFOUND)
            {
                target = TrackedObjectKey.Factory(key);
                await this.mainSession.UpsertAsync(key, target, false, this.shutdown.Token);
                target.Restore(this.partition);
            }
            else if (status != Status.OK)
            {
                throw new Exception("Faster"); //TODO
            }

            return target;
        }

        public ValueTask MarkWritten(TrackedObjectKey k, PartitionEvent evt)
        {
            return this.mainSession.RMWAsync(k, evt, false, this.shutdown.Token);
        }

        public class Functions : IFunctions<Key, Value, PartitionEvent, TrackedObject, Empty>
        {
            private readonly Partition partition;

            public Functions(Partition partition)
            {
                this.partition = partition;
            }

            private void ApplyCore(TrackedObject t, PartitionEvent e)
            {
                if (EtwSource.EmitDiagnosticsTrace)
                {
                    this.partition.DiagnosticsTrace($"Updated [{t.Key}]");
                }
                // effects were already applied during process.
            }

            public void InitialUpdater(ref Key key, ref PartitionEvent input, ref Value value)
            {
                value.Val = TrackedObjectKey.Factory(key.Val);
                value.Val.Restore(partition);
                this.ApplyCore(value, input);
            }

            public bool InPlaceUpdater(ref Key key, ref PartitionEvent input, ref Value value)
            {
                this.ApplyCore(value, input);
                return true;
            }

            public void CopyUpdater(ref Key key, ref PartitionEvent input, ref Value oldValue, ref Value newValue)
            {
                DurableTask.EventSourced.Serializer.SerializeTrackedObject(oldValue);
                newValue.Val = oldValue.Val;
                InPlaceUpdater(ref key, ref input, ref newValue);
            }

            public void SingleReader(ref Key key, ref PartitionEvent input, ref Value value, ref TrackedObject dst)
            {
                dst = value.Val;
            }
            public void ConcurrentReader(ref Key key, ref PartitionEvent input, ref Value value, ref TrackedObject dst)
            {
                dst = value.Val;
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


            public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint) { }
            public void ReadCompletionCallback(ref Key key, ref PartitionEvent input, ref TrackedObject output, Empty ctx, Status status) { }
            public void RMWCompletionCallback(ref Key key, ref PartitionEvent input, Empty ctx, Status status) { }
            public void UpsertCompletionCallback(ref Key key, ref Value value, Empty ctx) { }
            public void DeleteCompletionCallback(ref Key key, Empty ctx) { }
        }
    }
}
