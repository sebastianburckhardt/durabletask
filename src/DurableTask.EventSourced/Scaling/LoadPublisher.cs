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

using Dynamitey;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventSourced
{
    internal class LoadPublisher : BatchWorker<(uint, LoadMonitorAbstraction.PartitionInfo)>
    {
        private readonly LoadMonitorAbstraction.ILoadMonitorService service;

        public static TimeSpan PublishInterval = TimeSpan.FromSeconds(15);

        private CancellationTokenSource cancelWait = new CancellationTokenSource();

        public LoadPublisher(LoadMonitorAbstraction.ILoadMonitorService service)
        {
            this.service = service;
            this.cancelWait = new CancellationTokenSource();
        }

        public void Flush()
        {
            this.cancelWait.Cancel(); // so that we don't have to wait the whole delay
        }

        protected override async Task Process(IList<(uint, LoadMonitorAbstraction.PartitionInfo)> batch)
        {
            if (batch.Count != 0)
            {
                var latestForEachPartition = new Dictionary<uint, LoadMonitorAbstraction.PartitionInfo>();

                foreach (var (partitionId, info) in batch)
                {
                    latestForEachPartition[partitionId] = info;
                }

                try
                {
                    await this.service.PublishAsync(latestForEachPartition, this.cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    // o.k. during shutdown
                }
                catch
                {
                    // we swallow exceptions so we can tolerate temporary Azure storage errors
                    // TODO log
                }
            }

            try
            {
                await Task.Delay(PublishInterval, this.cancelWait.Token);
            }
            catch(OperationCanceledException)
            {
            }
        }
    }
}
 