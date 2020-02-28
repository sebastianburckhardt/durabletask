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
        private readonly CancellationToken cancellationToken;
        private readonly object thisLock = new object(); // TODO test and perhaps improve scalability of single lock

        private readonly Queue<T> work = new Queue<T>();
        private readonly Queue<(DateTime, CancellableCompletionSource<T>)> waiters = new Queue<(DateTime, CancellableCompletionSource<T>)>();

        public WorkItemQueue(CancellationToken token, Action<List<CancellableCompletionSource<T>>> onExpiration)
        {
            this.cancellationToken = token;
        }

        public int Load { get; private set; }

        public void Add(T element)
        {
            lock (this.thisLock)
            {
                // if there are waiters, hand it to the oldest one in line
                while (this.waiters.Count > 0)
                {
                    var next = this.waiters.Dequeue();
                    this.Load = -this.waiters.Count;
                    if (next.Item2.TrySetResult(element))
                    {
                        return;
                    }
                }

                this.work.Enqueue(element);
                this.Load = this.work.Count;
            }
        }

        public Task<T> GetNext(TimeSpan timeout, CancellationToken cancellationToken)
        {
            lock (this.thisLock)
            {
                if (this.work.Count > 0)
                {
                    var next = this.work.Dequeue();
                    this.Load = this.work.Count;
                    return Task.FromResult(next);
                }

                var tcs = new CancellableCompletionSource<T>(cancellationToken);

                this.waiters.Enqueue((DateTime.UtcNow + timeout, tcs));
                this.Load = -this.waiters.Count;

                return tcs.Task;
            }
        }

        public void CheckExpirations()
        {
            lock (this.thisLock)
            {
                while (waiters.Count > 0 && waiters.Peek().Item1 < DateTime.UtcNow)
                {
                    var expiredWaiter = waiters.Dequeue();
                    expiredWaiter.Item2.TrySetCanceled();
                }
            }
        }
    }
}
