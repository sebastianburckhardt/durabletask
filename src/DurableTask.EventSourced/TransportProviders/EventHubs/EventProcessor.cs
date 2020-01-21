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
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DurableTask.EventSourced.Emulated;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;

namespace DurableTask.EventSourced.EventHubs
{
    internal class EventProcessor : IEventProcessor
    {
        private readonly TransportAbstraction.IHost host;
        private readonly TransportAbstraction.ISender sender;
        private readonly Guid processorId;
        private readonly EventSourcedOrchestrationServiceSettings settings;

        private TransportAbstraction.IPartition partition;

        private Dictionary<string, MemoryStream> reassembly = new Dictionary<string, MemoryStream>();

        public EventProcessor(TransportAbstraction.IHost host, TransportAbstraction.ISender sender, EventSourcedOrchestrationServiceSettings settings)
        {
            this.host = host;
            this.sender = sender;
            this.processorId = Guid.NewGuid();
            this.settings = settings;
        }

        Task IEventProcessor.OpenAsync(PartitionContext context)
        {
            uint partitionId = uint.Parse(context.PartitionId);
            var partitionState = this.host.CreatePartitionState();
            this.partition = host.AddPartition(partitionId, partitionState, this.sender);
            return this.partition.StartAsync();
        }

        async Task IEventProcessor.CloseAsync(PartitionContext context, CloseReason reason)
        {
            await this.partition.StopAsync();
        }

        Task IEventProcessor.ProcessErrorAsync(PartitionContext context, Exception error)
        {
            partition.ReportError("EventHubs", error);
            return Task.FromResult<object>(null);
        }

        Task IEventProcessor.ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
        {
            var batch = new Batch();

            foreach(var eventData in messages)
            {
                var evt = (PartitionEvent) Serializer.DeserializeEvent(eventData.Body);
                evt.Serialized = eventData.Body; // we'll reuse this for writing to the event log
                batch.Add(evt);
            }

            // attach the ack listener to the last event in the batch
            batch[batch.Count - 1].AckListener = batch;

            return batch.SubmitAndWait(partition);
        }

        private class Batch : List<PartitionEvent>, TransportAbstraction.IAckListener
        {
            private TaskCompletionSource<object> Tcs = new TaskCompletionSource<object>();

            public void Acknowledge(Event evt)
            {
                Tcs.TrySetResult(null);
            }

            public Task SubmitAndWait(TransportAbstraction.IPartition partition)
            {
                // attach the ack listener to the last event in the batch
                this[this.Count - 1].AckListener = this;

                partition.SubmitRange(this);

                return this.Tcs.Task;
            }
        }
    }
}
