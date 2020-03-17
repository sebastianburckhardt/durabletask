using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace DurableTask.EventSourced.EventHubs
{
    internal abstract class Packet
    {
        public Event evt;

        public string PacketId;

        
        public abstract void Serialize(Stream stream, Event evt);

        public abstract void Deserialize(Stream stream);

    }
}
