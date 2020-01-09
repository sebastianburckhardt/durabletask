using System;
using System.Collections.Generic;
using System.Text;
using DurableTask.Core.Tracking;
using FASTER.core;

namespace DurableTask.EventSourced.Faster
{
    internal class FasterKV : FasterKV<FasterKV.Key, FasterKV.Value, PartitionEvent, TrackedObject, Empty, FasterKV.Functions>, IDisposable
    {
        private readonly Partition partition;
        private readonly BlobManager blobManager;
 
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
        }

        public new void Dispose()
        {
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

        public PartitionEvent NoInput = new TaskhubCreated() { }; // just a dummy non-null object

        public TrackedObject GetOrCreate(TrackedObjectKey k)
        {
            FasterKV.Key key = k;
            TrackedObject target = null;
            var status = this.Read(ref key, ref this.NoInput, ref target, Empty.Default, 0);

            if (status == Status.NOTFOUND)
            {
                FasterKV.Value newObject = TrackedObjectKey.Factory(k);
                var status2 = this.Upsert(ref key, ref newObject, Empty.Default, 0);
                if (status2 != Status.OK)
                {
                    throw new NotImplementedException("TODO");
                }
                newObject.Val.Restore(this.partition);
                return newObject.Val;
            }
            
            if (status != Status.OK)
            {
                throw new NotImplementedException("TODO");
            }

            return target;
        }

        public void Apply(TrackedObjectKey k, PartitionEvent evt)
        {
            FasterKV.Key key = k;
            var status = this.RMW(ref key, ref evt, Empty.Default, 0);
            
            if (status != Status.OK)
            {
                throw new NotImplementedException("TODO");
            }
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
                    this.partition.DiagnosticsTrace($"Apply to [{t.Key}]");
                }
                dynamic targetObject = t;
                dynamic evt = e;
                targetObject.Apply(evt);
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


            public void CheckpointCompletionCallback(Guid sessionId, long serialNum) { }
            public void ReadCompletionCallback(ref Key key, ref PartitionEvent input, ref TrackedObject output, Empty ctx, Status status) { }
            public void RMWCompletionCallback(ref Key key, ref PartitionEvent input, Empty ctx, Status status) { }
            public void UpsertCompletionCallback(ref Key key, ref Value value, Empty ctx) { }
            public void DeleteCompletionCallback(ref Key key, Empty ctx) { }
        }
    }
}
