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
using System.Security.Cryptography;
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
        private readonly string eventHubName;
        private readonly string eventHubPartition;

        private TimeSpan backoff = TimeSpan.FromSeconds(5);
        private const int maxFragmentSize = 500 * 1024; // account for very non-optimal serialization of event
        private MemoryStream stream = new MemoryStream(); // reused for all packets

        public EventHubsSender(TransportAbstraction.IHost host, PartitionSender sender)
        {
            this.host = host;
            this.sender = sender;
            this.logger = host.TransportLogger;
            this.eventHubName = this.sender.EventHubClient.EventHubName;
            this.eventHubPartition = this.sender.PartitionId;
        }
   
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
                    long startPos = stream.Position;
                    var evt = toSend[i];
                    Packet.Serialize(evt, stream);
                    int length = (int)(stream.Position - startPos);
                    var arraySegment = new ArraySegment<byte>(stream.GetBuffer(), (int)startPos, length);
                    var eventData = new EventData(arraySegment);
                    bool tooBig = length > maxFragmentSize;

                    if (!tooBig && batch.TryAdd(eventData))
                    {
                        this.logger.LogDebug("EventHubsSender {eventHubName}/{partitionId} added packet to batch ({size} bytes) {evt} id={eventId}", this.eventHubName, this.eventHubPartition, eventData.Body.Count, evt, evt.EventIdString);
                        continue;
                    }
                    else
                    {
                        if (batch.Count > 0)
                        {
                            // send the batch we have so far
                            maybeSent = i - 1;
                            await sender.SendAsync(batch);
                            sentSuccessfully = i - 1;

                            this.logger.LogDebug("EventHubsSender {eventHubName}/{partitionId} sent batch", this.eventHubName, this.eventHubPartition);

                            // create a fresh batch
                            batch = sender.CreateBatch();
                        }

                        if (tooBig)
                        {
                            // the message is too big. Break it into fragments, and send each individually.
                            var fragments = FragmentationAndReassembly.Fragment(arraySegment, evt, maxFragmentSize);
                            maybeSent = i;
                            foreach (var fragment in fragments)
                            {
                                //TODO send bytes directly instead of as events (which causes significant space overhead)
                                stream.Seek(0, SeekOrigin.Begin);
                                Packet.Serialize((Event)fragment, stream);
                                length = (int)stream.Position;
                                await sender.SendAsync(new EventData(new ArraySegment<byte>(stream.GetBuffer(), 0, length)));
                                this.logger.LogDebug("EventHubsSender {eventHubName}/{partitionId} sent packet ({size} bytes) {evt} id={eventId}", this.eventHubName, this.eventHubPartition, length, fragment, ((Event)fragment).EventIdString);
                            }
                            sentSuccessfully = i;
                        }
                        else
                        {
                            // back up one
                            i--;
                        }

                        // the buffer can be reused now
                        stream.Seek(0, SeekOrigin.Begin);
                    }
                }

                if (batch.Count > 0)
                {
                    maybeSent = toSend.Count - 1;
                    await sender.SendAsync(batch);
                    sentSuccessfully = toSend.Count - 1;

                    this.logger.LogDebug("EventHubsSender {eventHubName}/{partitionId} sent batch", this.eventHubName, this.eventHubPartition);

                    // the buffer can be reused now
                    stream.Seek(0, SeekOrigin.Begin);
                }
            }
            catch (Exception e)
            {
                this.logger.LogWarning(e, "EventHubsSender {eventHubName}/{partitionId} failed to send", this.eventHubName, this.eventHubPartition, this.sender.EventHubClient.EventHubName, this.sender.PartitionId);
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

                this.logger.LogDebug("EventHubsSender {eventHubName}/{partitionId} has confirmed {confirmed}, requeued {requeued}, dropped {dropped} messages", this.eventHubName, this.eventHubPartition, confirmed, requeued, dropped, this.sender.EventHubClient.EventHubName, this.sender.PartitionId);
            }
            catch (Exception exception)
            {
                this.logger.LogError("EventHubsSender {eventHubName}/{partitionId} encountered an error while trying to confirm messages: {exception}", this.eventHubName, this.eventHubPartition, exception);
            }
        }
    }
}
