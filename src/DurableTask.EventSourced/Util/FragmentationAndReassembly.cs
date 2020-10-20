﻿//  Copyright Microsoft Corporation
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  ----------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace DurableTask.EventSourced
{
    internal static class FragmentationAndReassembly
    {
        public interface IEventFragment
        {
            EventId OriginalEventId { get; }

            byte[] Bytes { get; }

            bool IsLast { get; }
        }

        public static List<IEventFragment> Fragment(ArraySegment<byte> segment, Event original, int maxFragmentSize)
        {
            if (segment.Count <= maxFragmentSize)
                throw new ArgumentException(nameof(segment), "segment must be larger than max fragment size");

            var list = new List<IEventFragment>();
            int offset = segment.Offset;
            int length = segment.Count;
            int count = 0;
            while (length > 0)
            {
                int portion = Math.Min(length, maxFragmentSize);
                if (original is ClientEvent clientEvent)
                {
                    list.Add(new ClientEventFragment()
                    {
                        ClientId = clientEvent.ClientId,
                        RequestId = clientEvent.RequestId,
                        OriginalEventId = original.EventId,
                        Bytes = new ArraySegment<byte>(segment.Array, offset, portion).ToArray(),
                        Fragment = count++,
                        IsLast = (portion == length),
                    });
                }
                else if (original is PartitionUpdateEvent partitionEvent)
                {
                    list.Add(new PartitionEventFragment()
                    {
                        PartitionId = partitionEvent.PartitionId,
                        OriginalEventId = original.EventId,
                        Bytes = new ArraySegment<byte>(segment.Array, offset, portion).ToArray(),
                        Fragment = count++,
                        IsLast = (portion == length),
                    });
                }
                offset += portion;
                length -= portion;
            }
            return list;
        }

        public static TEvent Reassemble<TEvent>(MemoryStream stream, IEventFragment lastFragment) where TEvent : Event
        {
            stream.Write(lastFragment.Bytes, 0, lastFragment.Bytes.Length);
            stream.Seek(0, SeekOrigin.Begin);
            Packet.Deserialize(stream, out TEvent evt, null);
            stream.Dispose();
            return evt;
        }

        public static TEvent Reassemble<TEvent>(IEnumerable<IEventFragment> earlierFragments, IEventFragment lastFragment) where TEvent: Event
        {
            using (var stream = new MemoryStream())
            {
                foreach (var x in earlierFragments)
                {
                    stream.Write(x.Bytes, 0, x.Bytes.Length);
                }
                stream.Write(lastFragment.Bytes, 0, lastFragment.Bytes.Length);
                stream.Seek(0, SeekOrigin.Begin);
                Packet.Deserialize(stream, out TEvent evt, null);
                return evt;
            }
        }
    }
}