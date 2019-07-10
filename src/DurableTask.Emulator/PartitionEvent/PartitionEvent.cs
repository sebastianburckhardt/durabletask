using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using DurableTask.Core;

namespace DurableTask.EventHubs
{
    [DataContract]
    [KnownType(typeof(TaskMessageReceived))]
    [KnownType(typeof(BatchProcessed))]
    [KnownType(typeof(ActivityCompleted))]
    [KnownType(typeof(TimerFired))]
    internal abstract class PartitionEvent
    {
        /// <summary>
        /// For received events, this is the queue position at which the event was received.
        /// </summary>
        [IgnoreDataMember]
        public long QueuePosition { get; set; }

        public abstract TrackedObject Scope(IPartitionState state);
    }
}