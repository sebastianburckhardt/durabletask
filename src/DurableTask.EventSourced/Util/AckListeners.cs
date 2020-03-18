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


using DurableTask.EventSourced.AzureChannels;
using Dynamitey.DynamicObjects;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventSourced
{
    internal struct AckListeners
    {
        private volatile object status;
        private static object MarkAsSuccessfullyCompleted = new object();

        public static void Register(Event evt, TransportAbstraction.IAckListener listener)
        {
            
            // fast path: status is null, replace it with the listener
            if (Interlocked.CompareExchange(ref evt.AckListeners.status, listener, null) == null)
            {
                return;
            }

            // slower path: there are some listener(s) already, or the event is acked already
            while (true)
            {
                var current = evt.AckListeners.status;

                // if the current status indicates the ack has happened already, notify the listener
                // right now

                if (current == MarkAsSuccessfullyCompleted)
                {
                    listener.Acknowledge(evt);
                    return;
                }

                if (current is Exception e && listener is TransportAbstraction.IAckOrExceptionListener exceptionListener)
                {
                    exceptionListener.ReportException(evt, e);
                    return;
                }

                // add the listener to the list of listeners

                List<TransportAbstraction.IAckListener> list;

                if (current is TransportAbstraction.IAckListener existing)
                {
                    list = new List<TransportAbstraction.IAckListener>() { existing, listener };
                }
                else
                {
                    list = (List<TransportAbstraction.IAckListener>) current;
                    list.Add(listener);
                }

                if (Interlocked.CompareExchange(ref evt.AckListeners.status, list, current) == current)
                {
                    return;
                }     
            }
        }

        public static void Acknowledge(Event evt)
        {
            var listeners = Interlocked.Exchange(ref evt.AckListeners.status, MarkAsSuccessfullyCompleted);

            if (listeners != null)
            {
                using (EventTraceHelper.TraceContext(null, evt.EventIdString))
                {
                    if (listeners is TransportAbstraction.IAckListener listener)
                    {
                        listener.Acknowledge(evt);
                    }
                    else if (listeners is List<TransportAbstraction.IAckListener> list)
                    {
                        foreach (var l in list)
                        {
                            l.Acknowledge(evt);
                        }
                    }
                }
            }       
        }

        public static void ReportException(Event evt, Exception e)
        {
            if (e == null)
            {
                throw new ArgumentNullException(nameof(e));
            }

            var listeners = Interlocked.Exchange(ref evt.AckListeners.status, e);

            if (listeners != null)
            {
                if (listeners is TransportAbstraction.IAckListener listener)
                {
                    listener.Acknowledge(evt);
                }
                else if (listeners is List<TransportAbstraction.IAckListener> list)
                {
                    foreach (var l in list)
                    {
                        l.Acknowledge(evt);
                    }
                }
            }
        }
    }
}
