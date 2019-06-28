using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using DurableTask.Core;

namespace DurableTask.Emulator
{
    [DataContract]
    [KnownType(typeof(TaskMessageReceived))]
    [KnownType(typeof(BatchProcessed))]
    [KnownType(typeof(ActivityCompleted))]
    internal abstract class ProcessorEvent
    {
        /// <summary>
        /// This field is not persisted with the event, but is used to label it during processing for convenience.
        /// For received events, this is the queue position at which the event was received.
        /// For sent events, this is the queue position of the event whose processing caused the send.
        /// </summary>
        [IgnoreDataMember]
        public long QueuePosition { get; set; }

        public abstract IEnumerable<TrackedObject> UpdateSequence(FasterState fasterState);  
    }
}