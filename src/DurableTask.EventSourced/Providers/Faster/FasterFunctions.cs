//using DurableTask.EventSourced.PartitionState;
//using FASTER.core;
//using System;
//using System.Collections.Generic;
//using System.Text;

//namespace DurableTask.EventSourced.Faster
//{

//    internal class FasterFunctions : IFunctions<TrackedObjectKey, TrackedObject, PartitionEvent, long, Empty>
//    {

//        public void InitialUpdater(ref TrackedObjectKey key, ref PartitionEvent input, ref TrackedObject value)
//        {
//            value = TrackedObjectFactory.Create(key);
//            value.Apply(input);
//        }
//        public bool InPlaceUpdater(ref TrackedObjectKey key, ref PartitionEvent input, ref TrackedObject value)
//        {
//            if (value.LastProcessed < input.CommitPosition)
//            { 
//            lock (value.AccessLock) // prevent conflicts with readers
//            {
//                value.Apply(input);
//            }
//            value.LastProcessed = input.CommitPosition;
//            return true;
//        }

//        public void CopyUpdater(ref TrackedObjectKey key, ref PartitionEvent input, ref TrackedObject oldValue, ref TrackedObject newValue)
//        {
            
//            newValue = oldValue;

//        }

//        public void SingleReader(ref TrackedObjectKey key, ref PartitionEvent input, ref TrackedObject value, ref long dst)
//        {
//            throw new NotImplementedException();
//        }

//        public void SingleWriter(ref TrackedObjectKey key, ref TrackedObject src, ref TrackedObject dst)
//        {
//            throw new NotImplementedException();
//        }

//         public void ConcurrentReader(ref TrackedObjectKey key, ref PartitionEvent input, ref TrackedObject value, ref long dst)
//        {
//            throw new NotImplementedException();
//        }

//        public bool ConcurrentWriter(ref TrackedObjectKey key, ref TrackedObject src, ref TrackedObject dst)
//        {
//            throw new NotImplementedException();
//        }




//        public void CheckpointCompletionCallback(Guid sessionId, long serialNum) { }
//        public void ReadCompletionCallback(ref TrackedObjectKey key, ref PartitionEvent input, ref long output, Empty ctx, Status status) { }
//        public void RMWCompletionCallback(ref TrackedObjectKey key, ref PartitionEvent input, Empty ctx, Status status) { }
//        public void UpsertCompletionCallback(ref TrackedObjectKey key, ref TrackedObject value, Empty ctx) { }
//        public void DeleteCompletionCallback(ref TrackedObjectKey key, Empty ctx) { }
//    }

//}
