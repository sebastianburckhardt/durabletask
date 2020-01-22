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

using FASTER.core;
using System;
using System.Collections.Generic;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventSourced.Faster
{
    internal class LogWorker : BatchWorker<PartitionEvent>
    {
        private readonly FasterLog log;
        private readonly Partition partition;
        private readonly CancellationToken token;

        public LogWorker(Partition partition, BlobManager blobManager, CancellationToken token)
            : base(token, false)
        {
            this.log = new FasterLog(blobManager);
            this.partition = partition;
            this.token = token;
        }

        private void EnqueueEvent(PartitionEvent evt)
        {
            if (evt.Serialized.Count == 0)
            {
                byte[] bytes = Serializer.SerializeEvent(evt);
                evt.CommitPosition = this.log.Enqueue(bytes);
            }
            else
            {
                evt.CommitPosition = this.log.Enqueue(evt.Serialized.AsSpan<byte>());
            }

            this.partition.TraceSubmit(evt);
        }

        public void AddToLog(IEnumerable<PartitionEvent> evts)
        {
            foreach (var evt in evts)
            {
                EnqueueEvent(evt);
            }

            this.SubmitRange(evts);
        }

        public void AddToLog(PartitionEvent evt)
        {
            EnqueueEvent(evt);

            this.Submit(evt);
        }

        protected override async Task Process(IList<PartitionEvent> batch)
        {
            await log.CommitAsync(this.cancellationToken);

            foreach (var evt in batch)
            {
                evt.AckListener?.Acknowledge(evt);
            }
        }

        public async Task JoinAsync()
        {
            await log.CommitAsync(); //TODO
        }
    }
}
