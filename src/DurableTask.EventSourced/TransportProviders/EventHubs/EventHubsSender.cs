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
using Microsoft.Extensions.Logging;
//using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace DurableTask.EventSourced.EventHubs
{
    internal class EventHubsSender<T> : BatchWorker<Event> where T: Event
    {
        private readonly PartitionSender sender;
        private readonly TransportAbstraction.IHost host;
        private readonly ILogger logger;

        public EventHubsSender(TransportAbstraction.IHost host, PartitionSender sender)
        {
            this.host = host;
            this.sender = sender;
            this.logger = host.TransportLogger;
        }
   
        private TimeSpan backoff = TimeSpan.FromSeconds(5);
        private const int maxFragmentSize = 64 * 1024; // EH can handle more than this, but let's not go close to the limit
        private MemoryStream stream = new MemoryStream();

        protected override async Task Process(IList<Event> toSend)
        {
            if (toSend.Count == 0)
            {
                return;
            }

            // track progress in case of exception
            var sentSuccessfully = -1;
            var maybeSent = -1;
            Exception senderException = null;
            
            try
            {
                var batch = sender.CreateBatch();

                for (int i = 0; i < toSend.Count; i++)
                {
                    stream.Seek(0, SeekOrigin.Begin);
                    Serializer.SerializeEvent(toSend[i], stream);

                    var arraySegment = new ArraySegment<byte>(stream.GetBuffer(), 0, (int)stream.Position);
                    var eventData = new EventData(arraySegment);
                    bool tooBig = arraySegment.Count > maxFragmentSize;

                    if (!tooBig && batch.TryAdd(eventData))
                    {
                        continue;
                    }
                    else if (batch.Count > 0)
                    {
                        // backtrack one
                        i--;
                        // send the batch we have so far
                        maybeSent = i;
                        await sender.SendAsync(batch);
                        sentSuccessfully = i;

                        // create a fresh batch
                        batch = sender.CreateBatch();
                    }
                    else
                    {
                        // the message is too big. Break it into fragments, and send each individually.
                        var fragments = FragmentationAndReassembly.Fragment(arraySegment, toSend[i], maxFragmentSize);
                        maybeSent = i;
                        foreach (var fragment in fragments)
                        {
                            stream.Seek(0, SeekOrigin.Begin);
                            Serializer.SerializeEvent((Event)fragment, stream);
                            await sender.SendAsync(new EventData(new ArraySegment<byte>(stream.GetBuffer(), 0, (int)stream.Position)));
                        }
                        sentSuccessfully = i;
                    }
                }

                if (batch.Count > 0)
                {
                    maybeSent = toSend.Count - 1;
                    await sender.SendAsync(batch);
                    sentSuccessfully = toSend.Count - 1;
                }
            }
            catch (Exception e)
            {
                this.logger.LogWarning(e, "Could not send messages to EventHub {eventHubName}, partition {partitionId}", this.sender.EventHubClient.EventHubName, this.sender.PartitionId);
                senderException = e;
            }
            finally
            {
                // we don't need the contents of the stream anymore.
                stream.SetLength(0); 
            }

            // Confirm all sent events, and retry or report maybe-sent ones
            List<Event> requeue = null;

            try
            {
                int confirmed = 0;
                int requeued = 0;
                int dropped = 0;

                for (int i = 0; i < toSend.Count; i++)
                {
                    var evt = toSend[i];

                    if (i <= sentSuccessfully)
                    {
                        // the event was definitely sent successfully
                        AckListeners.Acknowledge(evt);
                        confirmed++;
                    }
                    else if (i > maybeSent || evt.SafeToDuplicateInTransport())
                    {
                        // the event was definitely not sent, OR it was maybe sent but can be duplicated safely
                        (requeue ?? (requeue = new List<Event>())).Add(evt);
                        requeued++;
                    }
                    else 
                    {
                        // the event may have been sent or maybe not, report problem to listener
                        // this is used by clients who can give the exception back to the caller
                        AckListeners.ReportException(evt, senderException);
                        dropped++;
                    }
                }

                if (requeue != null)
                {
                    // take a deep breath before trying again
                    await Task.Delay(backoff);

                    this.Requeue(requeue);
                }

                if (this.logger.IsEnabled(LogLevel.Debug))
                {
                    this.logger.LogDebug("Sender has confirmed {confirmed}, requeued {requeued}, dropped {dropped} messages for EventHub {eventHubName}, partition {partitionId}", confirmed, requeued, dropped, this.sender.EventHubClient.EventHubName, this.sender.PartitionId);
                }

            }
            catch (Exception exception)
            {
                this.logger.LogError("Internal error while trying to confirm messages for EventHub {eventHubName}, partition {partitionId}: {exception}", this.sender.EventHubClient.EventHubName, this.sender.PartitionId, exception);
            }
        }

        private IEnumerable<EventData> Serialize(IEnumerable<Event> events)
        {
            using (var stream = new MemoryStream())
            {
                foreach (var evt in events)
                {
                    stream.Seek(0, SeekOrigin.Begin);
                    Serializer.SerializeEvent(evt, stream);
                    var length = (int)stream.Position;
                    yield return new EventData(new ArraySegment<byte>(stream.GetBuffer(), 0, length));
                }
            }
        }
    }
}
