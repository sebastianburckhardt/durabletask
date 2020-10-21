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
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventSourced
{
    internal class WorkItemQueue<T>
    {
        private readonly string name;
        private readonly Queue<T> work = new Queue<T>();
        private readonly SemaphoreSlim count = new SemaphoreSlim(0);

        public WorkItemQueue(string name)
        {
            this.name = name;
        }

        public int Load => count.CurrentCount;

        public void Add(T element)
        {
            this.work.Enqueue(element);
            this.count.Release();
        }

        public async Task<T> GetNext(TimeSpan timeout, CancellationToken cancellationToken)
        {
            bool success = await this.count.WaitAsync((int) timeout.TotalMilliseconds, cancellationToken);
            if (success)
            {
                return this.work.Dequeue();
            }
            else
            {
                throw new OperationCanceledException($"Could not get a workitem from {this.name} within {timeout}.");
            }
        }
    }
}
