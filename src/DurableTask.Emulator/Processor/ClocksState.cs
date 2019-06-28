using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using DurableTask.Core;
using DurableTask.Core.History;

namespace DurableTask.Emulator
{
    [DataContract]
    internal class ClocksState : TrackedObject
    {     
        public bool Apply(TaskMessageReceived evt)
        {
            // todo: use vector clock to deduplicate
            //
            // if (already processed this remote request)
            //    return false;
            //


            return true;
        }
    }
}
