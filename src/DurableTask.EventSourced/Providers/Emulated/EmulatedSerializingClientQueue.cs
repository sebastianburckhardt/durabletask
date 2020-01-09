//  ----------------------------------------------------------------------------------
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

using DurableTask.EventSourced;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventSourced.Emulated
{
    /// <summary>
    /// Simulates a in-memory queue for delivering events. Used for local testing and debugging.
    /// </summary>
    internal class EmulatedSerializingClientQueue : EmulatedQueue<ClientEvent, byte[]>, IEmulatedQueue<ClientEvent>
    {
        private readonly Backend.IClient client;

        public EmulatedSerializingClientQueue(Backend.IClient client, CancellationToken cancellationToken)
            : base(cancellationToken)
        {
            this.client = client;
        }

        protected override byte[] Serialize(ClientEvent evt)
        {
            return Serializer.SerializeEvent(evt);
        }

        protected override ClientEvent Deserialize(byte[] bytes)
        {
            return (ClientEvent)Serializer.DeserializeEvent(bytes);
        }

        protected override void Deliver(ClientEvent evt)
        {
            try
            {
                client.Process(evt);
            }
            catch (System.Threading.Tasks.TaskCanceledException)
            {
                // this is normal during shutdown
            }
            catch (Exception e)
            {
                client.ReportError(nameof(EmulatedClientQueue), e);
            }
        }
    }
}
