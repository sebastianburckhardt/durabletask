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
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventSourced
{
    internal class BatchTimer<T>
    {
        private readonly object thisLock = new object();
        private readonly CancellationToken cancellationToken;
        private readonly Action<List<T>> handler;
        private readonly SortedList<(DateTime when, int id), T> schedule;
        private readonly SemaphoreSlim notify;

        private volatile int sequenceNumber;

        //TODO consider using a min heap for more efficient removal of entries

        public BatchTimer(CancellationToken token, Action<List<T>> handler)
        {
            this.cancellationToken = token;
            this.handler = handler;
            this.schedule = new SortedList<(DateTime when, int id), T>();
            this.notify = new SemaphoreSlim(0, int.MaxValue);

            token.Register(() => this.notify.Release());
        }

        public void Start(string name)
        {
            var thread = new Thread(ExpirationCheckLoop);
            thread.Name = name;
            thread.Start();
        }

        public int GetFreshId()
        {
           lock(this)
           {
               return sequenceNumber++;
           }
        }

        public void Schedule(DateTime due, T what, int? id = null)
        {
            lock (this.thisLock)
            {
                var key = (due, id.HasValue ? id.Value : sequenceNumber++);

                this.schedule.Add(key, what);

                // notify the expiration check loop
                if (key == this.schedule.First().Key)
                {
                    this.notify.Release();
                }
            }
        }

        public bool TryRemove((DateTime when, int id) key)
        {
            lock (this.thisLock)
            {
                return this.schedule.Remove(key);
            }
        }

        private void ExpirationCheckLoop()
        {
            List<T> batch = new List<T>();

            while (!cancellationToken.IsCancellationRequested)
            {
                // wait for the next expiration time or cleanup, but cut the wait short if notified
                if (this.RequiresDelay(out var delay))
                {
                    this.notify.Wait(delay); // blocks thread until delay is over, or until notified
                }

                lock (this.thisLock)
                {
                    var next = this.schedule.FirstOrDefault();

                    while (this.schedule.Count > 0
                        && next.Key.when <= DateTime.UtcNow
                        && !this.cancellationToken.IsCancellationRequested)
                    {
                        this.schedule.RemoveAt(0);
                        batch.Add(next.Value);

                        next = this.schedule.FirstOrDefault();
                    }
                }

                if (batch.Count > 0)
                {
                    // it is expected that the handler catches 
                    // all exceptions, since it has more meaningful ways to report errors
                    handler(batch);
                    batch.Clear();
                }
            }
        }

        private bool RequiresDelay(out TimeSpan delay)
        {
            lock (this.thisLock)
            {
                if (this.schedule.Count == 0)
                {
                    delay = TimeSpan.FromMilliseconds(-1); // represents infinite delay
                    return true;
                }

                var next = this.schedule.First();
                var now = DateTime.UtcNow;

                if (next.Key.when > now)
                {
                    delay = next.Key.when - now;
                    return true;
                }
                else
                {
                    delay = default;
                    return false;
                }
            }
        }
    }
}
