using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace DurableTask.Emulator
{
    [DataContract]
    internal abstract class TrackedObject
    {
        [IgnoreDataMember]
        protected LocalPartition LocalPartition;

        [DataMember]
        long LastProcessed { get; set; } = -1;

        public bool Process(ProcessorEvent processorEvent)
        {
            lock (this)
            {
                dynamic x = this;
                dynamic y = processorEvent;
                bool continueProcessing = x.Apply(y);

                if (processorEvent.QueuePosition > LastProcessed)
                {
                    LastProcessed = processorEvent.QueuePosition;
                }

                return continueProcessing;
            }
        }

        protected bool AlreadyApplied(ProcessorEvent processorEvent)
        {
            return processorEvent.QueuePosition <= LastProcessed;
        }

    }
}
