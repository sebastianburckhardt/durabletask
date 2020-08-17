using DurableTask.Core.Common;
using DurableTask.Core.Exceptions;
using Dynamitey;
using Newtonsoft.Json;
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
        // we prefix packets with a byte indicating the format
        // we use a flag (for json) or a version (for binary) to facilitate changing formats in the future
        private static byte jsonVersion = 0;
        private static byte binaryVersion = 1;

        private static JsonSerializerSettings serializerSettings 
            = new JsonSerializerSettings() { TypeNameHandling = TypeNameHandling.Auto };

        public static void Serialize(Event evt, Stream stream, bool useJson)
        {
            var writer = new BinaryWriter(stream, Encoding.UTF8);

            if (useJson)
            {
                // serialize the json
                string jsonContent = JsonConvert.SerializeObject(evt, typeof(Event), Packet.serializerSettings);

                // first entry is the version, followed by the json string
                writer.Write(Packet.jsonVersion);
                writer.Write(jsonContent);
            }
            else
            {
                // first entry is the version
                writer.Write(Packet.binaryVersion);
                writer.Flush();

                // write the binary serialization to the stream
                Serializer.SerializeEvent(evt, stream);
            }
        }

        public static void Deserialize<TEvent>(Stream stream, out TEvent evt) where TEvent : Event
        {
            var reader = new BinaryReader(stream);
            var format = reader.ReadByte();
            if (format == Packet.jsonVersion)
            {
                string jsonContent = reader.ReadString();
                evt = (TEvent) JsonConvert.DeserializeObject(jsonContent, Packet.serializerSettings);
            }
            else if (format == Packet.binaryVersion)
            {
                evt = (TEvent)Serializer.DeserializeEvent(stream);
            }
            else
            {
                throw new VersionNotFoundException($"Received packet with unhandled format indicator {format} - likely a versioning issue");
            }
        }

        public static void Deserialize<TEvent>(ArraySegment<byte> arraySegment, out TEvent evt) where TEvent : Event
        {
            using (var stream = new MemoryStream(arraySegment.Array, arraySegment.Offset, arraySegment.Count, false))
            {
                Packet.Deserialize(stream, out evt);
            }
        }
    }
}
