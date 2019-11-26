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

using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace DurableTask.EventSourced.Ambrosia
{
    [DataContract]
    internal class Immortal : Backend.ISender // IImmortal, Immortal<IServerProxy>
    {
        [IgnoreDataMember]
        private AmbrosiaBackend backend;

        [DataMember]
        private MemoryStorage state = new MemoryStorage();

        [IgnoreDataMember]
        public Storage.IPartitionState PartitionState => this.state;

        [IgnoreDataMember]
        private TaskCompletionSource<bool> startupComplete;

        [IgnoreDataMember]
        public Task StartupComplete => this.startupComplete.Task;

        //[DataMember]
        //private IImmortalProxy[] proxies;

        public Immortal(AmbrosiaBackend backend)
        {
            this.backend = backend;
            this.startupComplete = new TaskCompletionSource<bool>();
        }

        public static string GetImmortalName(uint partitionId)
        {
            return $"Partition{partitionId:D3}";
        }

        //protected override async Task<bool> OnFirstStart()
        //{
        //    for (int i = 0; i < backend.NumberPartitions; i++)
        //    {
        //        proxies[i] = GetProxy<IImmortalProxy>(GetImmortalName(i));
        //    }
        //    return true; // TODO find out what this return value means
        //}

        //protected override void BecomingPrimary()
        //{
        //    this.startupComplete.TrySetResult(true);
        //}

        public Task ProcessImpulseEvent(Event evt)
        {
            return backend.ProcessEvent(evt);
        }

        public Task ProcessOrderedEvent(Event evt)
        {
            return backend.ProcessEvent(evt);
        }

        void Backend.ISender.Submit(Event evt, Backend.ISendConfirmationListener listener)
        {
            if (evt is ClientEvent clientEvent)
            {
                var clientId = clientEvent.ClientId;
                var partitionId = backend.GetPartitionFromGuid(clientId);
                this.SendImpulseEvent(partitionId, evt);
            }
            else if (evt is TaskMessageReceived taskMessageReceivedEvent)
            {
                var partitionId = taskMessageReceivedEvent.PartitionId;
                this.SendOrderedEvent(partitionId, evt);
            }
            else if (evt is PartitionEvent partitionEvent)
            {
                var partitionId = partitionEvent.PartitionId;
                this.SendOrderedEvent(partitionId, evt);
            }
            listener?.ConfirmDurablySent(evt);
        }

        private void SendImpulseEvent(uint partitionId, Event evt)
        {
            //proxies[partitionId].ImpulseEventFork(evt);
        }

        private void SendOrderedEvent(uint partitionId, Event evt)
        {
            //proxies[partitionId].OrderedEventFork(evt);
        }

    
    }
}
