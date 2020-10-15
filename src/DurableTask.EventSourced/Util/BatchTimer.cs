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
        private readonly CancellationToken cancellationToken;
        private readonly Action<List<T>> handler;
        private readonly SortedList<(DateTime due, int id), T> schedule;
        private readonly SemaphoreSlim notify;
        private readonly Action<string> tracer;

        private MonitoredLock thisLock;
        private string name;

        private volatile int sequenceNumber;

        //TODO consider using a min heap for more efficient removal of entries

        public BatchTimer(CancellationToken token, Action<List<T>> handler, Action<string> tracer = null)
        {
            this.cancellationToken = token;
            this.handler = handler;
            this.tracer = tracer;
            this.schedule = new SortedList<(DateTime due, int id), T>();
            this.notify = new SemaphoreSlim(0, int.MaxValue);
            this.thisLock = new MonitoredLock();

            token.Register(() => this.notify.Release());
        }

        public void Start(string name)
        {
            var thread = new Thread(ExpirationCheckLoop);
            thread.Name = name;
            thisLock.Name = name;
            this.name = name;
            thread.Start();
        }

        public int GetFreshId()
        {
            using (thisLock.Lock())
            {
                return sequenceNumber++;
            }
        }

        public void Schedule(DateTime due, T what, int? id = null)
        {
            using (thisLock.Lock())
            {
                var key = (due, id ?? sequenceNumber++);

                this.schedule.Add(key, what);

                tracer?.Invoke($"{this.name} scheduled ({key.Item1:o},{key.Item2})");

                // notify the expiration check loop
                if (key == this.schedule.First().Key)
                {
                    this.notify.Release();
                }
            }
        }

        public bool TryCancel((DateTime due, int id) key)
        {
            using (thisLock.Lock())
            {
                if (this.schedule.Remove(key))
                {
                    tracer?.Invoke($"{this.name} canceled ({key.due:o},{key.id})");
                    return true;
                }
                else
                {
                    return false;
                }
            }
        }

        private void ExpirationCheckLoop()
        {
            List<T> batch = new List<T>();
            (DateTime due, int id) firstInBatch = default;
            (DateTime due, int id) nextAfterBatch = default;

            while (!cancellationToken.IsCancellationRequested)
            {
                // wait for the next expiration time or cleanup, but cut the wait short if notified
                if (this.RequiresDelay(out var delay, out var due))
                {
                    var startWait = DateTime.UtcNow;
                    this.notify.Wait(delay); // blocks thread until delay is over, or until notified                 
                    tracer?.Invoke($"{this.name} is awakening at {(DateTime.UtcNow - due).TotalSeconds}s");
                }

                using (thisLock.Lock())
                {
                    var next = this.schedule.FirstOrDefault();

                    while (this.schedule.Count > 0
                        && next.Key.due <= DateTime.UtcNow
                        && !this.cancellationToken.IsCancellationRequested)
                    {
                        this.schedule.RemoveAt(0);
                        batch.Add(next.Value);

                        if (batch.Count == 1)
                        {
                            firstInBatch = next.Key;
                        }

                        next = this.schedule.FirstOrDefault();
                    }

                    nextAfterBatch = next.Key;
                }

                if (batch.Count > 0)
                {
                    tracer?.Invoke($"starting {this.name} batch size={batch.Count} first=({firstInBatch.due:o},{firstInBatch.id}) next=({nextAfterBatch.due:o},{nextAfterBatch.id})");

                    // it is expected that the handler catches 
                    // all exceptions, since it has more meaningful ways to report errors
                    handler(batch);
                    batch.Clear();
                    
                    tracer?.Invoke($"completed {this.name} batch size={batch.Count} first=({firstInBatch.due:o},{firstInBatch.id}) next=({nextAfterBatch.due:o},{nextAfterBatch.id})");
                }
            }
        }

        private bool RequiresDelay(out TimeSpan delay, out DateTime due)
        {
            using (thisLock.Lock())
            {
                if (this.schedule.Count == 0)
                {
                    delay = TimeSpan.FromMilliseconds(-1); // represents infinite delay
                    due = DateTime.MaxValue;
                    return true;
                }

                var next = this.schedule.First();
                var now = DateTime.UtcNow;

                if (next.Key.due > now)
                {
                    due = next.Key.due;
                    delay = due - now;
                    return true;
                }
                else
                {
                    due = now;
                    delay = default;
                    return false;
                }
            }
        }
    }
}
