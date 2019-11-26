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
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;

namespace DurableTask.EventSourced.EventHubs
{
    internal class EventProcessor : IEventProcessor
    {
        private readonly Backend.IHost host;
        private readonly Backend.ISender sender;
        private readonly Guid processorId;
        private readonly EventSourcedOrchestrationServiceSettings settings;

        private Backend.IPartition partition;

        private Dictionary<string, MemoryStream> reassembly = new Dictionary<string, MemoryStream>();

        public EventProcessor(Backend.IHost host, Backend.ISender sender, EventSourcedOrchestrationServiceSettings settings)
        {
            this.host = host;
            this.sender = sender;
            this.processorId = Guid.NewGuid();
            this.settings = settings;
        }

        Task IEventProcessor.OpenAsync(PartitionContext context)
        {
            uint partitionId = uint.Parse(context.PartitionId);
            this.partition = host.AddPartition(partitionId, new Faster.FasterStorage(settings.StorageConnectionString), this.sender);
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
                batch.Add(evt);
            }

            batch[batch.Count - 1].ConfirmationListener = batch;

            this.partition.SubmitRange(batch);

            return batch.Tcs.Task;
        }

        private class Batch : List<PartitionEvent>, Backend.IConfirmationListener
        {
            public TaskCompletionSource<object> Tcs = new TaskCompletionSource<object>();

            public void Confirm(Event evt)
            {
                Tcs.TrySetResult(null);
            }

            public void ReportException(Event evt, Exception e)
            {
                Tcs.TrySetException(e);
            }
        }
    }
}
