using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace DurableTask.Emulator
{
    [DataContract]
    internal class State
    {
        [DataMember]
        public ClocksState Clocks { get; set; }

        [DataMember]
        public OutboxState Outbox { get; set; }

        [DataMember]
        public TimersState Timers { get; set; }

        [DataMember]
        public ActivitiesState Activities { get; set; }

        [DataMember]
        public SessionsState Sessions { get; set; }

        [DataMember]
        public Dictionary<string, InstanceState> Instances { get; set; }

        public InstanceState GetInstance(string instanceId)
        {
            if (! Instances.TryGetValue(instanceId, out var instance))
            {
                this.Instances[instanceId] = instance = new InstanceState();
            }
            return instance;
        }

        public void Process(ProcessorEvent evt, List<TrackedObject> scope, List<TrackedObject> apply)
        {
            var target = evt.Scope(this);
            target.Process(evt, scope, apply);
        }
    }
}
