//  Copyright Microsoft Corporation
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

using Microsoft.Azure.EventHubs;
//using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace DurableTask.EventSourced.EventHubs
{
    internal class EventHubsSender<T> : BatchWorker<EventHubsSender<T>.Entry> where T: Event
    {
        private readonly PartitionSender sender;
        private readonly Backend.IHost host;

        public EventHubsSender(Backend.IHost host, PartitionSender sender)
        {
            this.host = host;
            this.sender = sender;
        }

        public struct Entry
        {
            public T Event;
            public Backend.ISendConfirmationListener Listener;
        }

        public void Add(T evt, Backend.ISendConfirmationListener listener)
        {
            this.Submit(new Entry() { Event = evt, Listener = listener });
        }
   
        // we reuse the same memory stream t
        private readonly MemoryStream stream = new MemoryStream();

        private TimeSpan backoff = TimeSpan.FromSeconds(5);

        protected override async Task Process(List<Entry> toSend)
        {
            // track progress in case of exception
            var sentSuccessfully = 0;
            var maybeSent = 0;
            Exception senderException = null;

            try
            {
                var batch = sender.CreateBatch();

                for (int i = 0; i < toSend.Count; i++)
                {
                    var startpos = (int)stream.Position;

                    Serializer.SerializeEvent(toSend[i].Event, stream);

                    var arraySegment = new ArraySegment<byte>(stream.GetBuffer(), startpos, (int)stream.Position - startpos);
                    var eventData = new EventData(arraySegment);

                    if (batch.TryAdd(eventData))
                    {
                        maybeSent = i;
                        continue;
                    }
                    else if (batch.Count > 0)
                    {
                        // send the batch we have so far, then create a new batch
                        await sender.SendAsync(batch);
                        sentSuccessfully = i;
                        batch = sender.CreateBatch();
                        stream.Seek(0, SeekOrigin.Begin);
                        // backtrack one so we try to send this same element again
                        i--;
                    }
                    else
                    {
                        // the message is too big. Break it into fragments, and send each individually.
                        var fragments = FragmentationAndReassembly.Fragment(arraySegment, toSend[i].Event);
                        maybeSent = i;
                        foreach (var fragment in fragments)
                        {
                            stream.Seek(0, SeekOrigin.Begin);
                            Serializer.SerializeEvent((Event)fragment, stream);
                            await sender.SendAsync(new EventData(new ArraySegment<byte>(stream.GetBuffer(), 0, (int)stream.Position)));
                        }
                        sentSuccessfully = i + 1;
                    }
                }

                if (batch.Count > 0)
                {
                    await sender.SendAsync(batch);
                }
                sentSuccessfully = toSend.Count;
            }
            catch (Exception e)
            {
                host.ReportError("Warning: failure in sender", e);
                senderException = e;
            }

            // Confirm all sent ones, and retry or report maybe-sent ones
            List<Entry> requeue = null;

            try
            {
                for (int i = 0; i < toSend.Count; i++)
                {
                    var entry = toSend[i];

                    if (i < sentSuccessfully)
                    {
                        // the event was definitely sent successfully
                        entry.Listener?.ConfirmDurablySent(entry.Event);
                    }
                    else if (i > maybeSent || entry.Event.AtLeastOnceDelivery)
                    {
                        // the event was definitely not sent, OR it was maybe sent but can be duplicated safely
                        (requeue ?? (requeue = new List<Entry>())).Add(entry);
                    }
                    else
                    {
                        // the event may have been sent or maybe not, report problem to listener
                        entry.Listener?.ReportSenderException(entry.Event, senderException);
                    }
                }

                if (requeue != null)
                {
                    // take a deep breath before trying again
                    await Task.Delay(backoff);

                    this.Requeue(requeue);
                }
            }
            catch(Exception e)
            {
                host.ReportError("Internal error: failure in requeue", e);
            }
        }

        private IEnumerable<EventData> Serialize(IEnumerable<Entry> entries)
        {
            using (var stream = new MemoryStream())
            {
                foreach (var entry in entries)
                {
                    stream.Seek(0, SeekOrigin.Begin);
                    Serializer.SerializeEvent(entry.Event, stream);
                    var length = (int)stream.Position;
                    yield return new EventData(new ArraySegment<byte>(stream.GetBuffer(), 0, length));
                }
            }
        }
    }
}
