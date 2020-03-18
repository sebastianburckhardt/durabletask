using DurableTask.Core.Common;
using DurableTask.Core.Exceptions;
using Dynamitey;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Reflection.Emit;
using System.Text;

namespace DurableTask.EventSourced
{
    /// <summary>
    /// Packets are the unit of transmission for external events, among clients and partitions.
    /// </summary>
    internal static class Packet
    {
        // we prefix packets with a version to facilitate changing formats in the future
        private static byte version = 0;

        public static void Serialize(Event evt, Stream stream)
        {
            var writer = new BinaryWriter(stream, Encoding.UTF8);

            // first entry is the version
            writer.Write(version);

            // next, we write the event id string. This helps tracing and debugging.
            writer.Write(evt.EventIdString);

            writer.Flush();

            // last is the actual event
            Serializer.SerializeEvent(evt, stream);
        }

        public static void Deserialize<TEvent>(Stream stream, out string eventId, out TEvent evt) where TEvent : Event
        {
            var reader = new BinaryReader(stream);
            var version = reader.ReadByte();
            if (version != Packet.version)
            {
                throw new VersionNotFoundException($"Received packet with version {version}, but code is version {Packet.version}");
            }
            eventId = reader.ReadString();
            evt = (TEvent)Serializer.DeserializeEvent(stream);
        }

        public static void Deserialize<TEvent>(ArraySegment<byte> arraySegment, out string eventId, out TEvent evt) where TEvent : Event
        {
            using (var stream = new MemoryStream(arraySegment.Array, arraySegment.Offset, arraySegment.Count, false))
            {
                Packet.Deserialize(stream, out eventId, out evt);
            }
        }
    }
}
