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

namespace DurableTask.EventHubs
{
    internal class BatchWorker<T>
    {
        private readonly object thisLock = new object();
        private readonly CancellationToken cancellationToken;
        private readonly Action<List<T>> handler;
        private List<T> schedule;
        private readonly SemaphoreSlim notify;

        public BatchWorker(CancellationToken token, Action<List<T>> handler)
        {
            this.cancellationToken = token;
            this.handler = handler;
            this.schedule = new List<T>();
            this.notify = new SemaphoreSlim(0, int.MaxValue);

            new Thread(WorkLoop).Start();
        }

        public void Add(T what)
        {
            lock (this.schedule)
            {
                this.schedule.Add(what);

                // notify the expiration check loop
                if (this.schedule.Count == 1)
                {
                    this.notify.Release();
                }
            }
        }

        public void AddRange(List<T> what)
        {
            lock (this.schedule)
            {
                this.schedule.AddRange(what);

                // notify the expiration check loop
                if (this.schedule.Count == what.Count)
                {
                    this.notify.Release();
                }
            }
        }

        private void WorkLoop()
        {
            List<T> batch = new List<T>();

            while (!cancellationToken.IsCancellationRequested)
            {
                // wait for the next expiration time, but cut the wait short if notified
                this.notify.Wait();

                lock (this.schedule)
                {
                    var temp = this.schedule;
                    this.schedule = batch;
                    batch = temp;
                }

                if (batch.Count > 0)
                {
                    try
                    {
                        handler(batch);
                    }
                    catch
                    {
                        //TODO
                    }

                    batch.Clear();
                }
            }
        }
    }
}
