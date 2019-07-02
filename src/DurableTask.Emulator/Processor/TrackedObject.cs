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
        protected LocalPartition LocalPartition;

        [DataMember]
        long LastProcessed { get; set; } = -1;

        [IgnoreDataMember]
        public abstract string Key { get; }

        [IgnoreDataMember]
        protected IState State => LocalPartition.State;

        // protects conflicts between the event processor and local tasks
        protected object thisLock = new object();

        // call after deserialization to fill in non-serialized fields
        public virtual void Restore(LocalPartition LocalPartition)
        {
            this.LocalPartition = LocalPartition;
            this.Restore();
        }

        protected virtual void Restore()
        {
        }

        public void Process(ProcessorEvent processorEvent, List<TrackedObject> scope, List<TrackedObject> apply)
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
