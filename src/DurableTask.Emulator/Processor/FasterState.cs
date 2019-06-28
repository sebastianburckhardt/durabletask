using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace DurableTask.Emulator
{
    [DataContract]
    internal class FasterState
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


        public void Process(ProcessorEvent evt)
        {
            foreach (var trackedObject in evt.UpdateSequence(this))
            {
                bool continueProcessing = trackedObject.Process(evt);

                if (! continueProcessing)
                {
                    break;
                }
            }
        }

        public InstanceState GetInstance(string instanceId)
        {
            if (! Instances.TryGetValue(instanceId, out var instance))
            {
                this.Instances[instanceId] = instance = new InstanceState();
            }
            return instance;
        }
    }
}
