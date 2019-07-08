using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using System.Threading;

namespace DurableTask.Emulator
{
    [DataContract]
    internal abstract class TrackedObject
    {
        [IgnoreDataMember]
        protected LocalOrchestrationService LocalPartition;

        [DataMember]
        long LastProcessed { get; set; } = -1;

        [IgnoreDataMember]
        public abstract string Key { get; }

        [IgnoreDataMember]
        protected IPartitionState State => LocalPartition.State;

        // protects conflicts between the event processor and local tasks
        protected object thisLock = new object();

        // call after deserialization to fill in non-serialized fields
        public long Restore(LocalOrchestrationService LocalPartition)
        {
            this.LocalPartition = LocalPartition;
            this.Restore();
            return LastProcessed;
        }

        protected virtual void Restore()
        {
            // subclasses override this if there is work they need to do here
        }

        public void Process(PartitionEvent processorEvent, List<TrackedObject> scope, List<TrackedObject> apply)
        {
            // start with reading this object only, to determine the scope
            if (processorEvent.QueuePosition > this.LastProcessed)
            {
                var scopeStartPos = scope.Count;
                var applyStartPos = apply.Count;

                dynamic dynamicThis = this;
                dynamic dynamicProcessorEvent = processorEvent;
                dynamicThis.Scope(dynamicProcessorEvent, scope, apply);

                if (scope.Count > scopeStartPos)
                {
                    for (int i = scopeStartPos; i < scope.Count; i++)
                    {
                        scope[i].Process(processorEvent, scope, apply);
                    }
                }

                if (apply.Count > applyStartPos)
                {
                    for (int i = applyStartPos; i < apply.Count; i++)
                    {
                        var target = apply[i];
                        if (target.LastProcessed < processorEvent.QueuePosition)
                        {
                            lock (target.thisLock)
                            {
                                dynamic dynamicTarget = target;
                                dynamicTarget.Apply(dynamicProcessorEvent);
                                target.LastProcessed = processorEvent.QueuePosition;
                            }
                        }
                    }
                }
            }
        }
    }
}
