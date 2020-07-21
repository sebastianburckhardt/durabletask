////  ----------------------------------------------------------------------------------
////  Copyright Microsoft Corporation
////  Licensed under the Apache License, Version 2.0 (the "License");
////  you may not use this file except in compliance with the License.
////  You may obtain a copy of the License at
////  http://www.apache.org/licenses/LICENSE-2.0
////  Unless required by applicable law or agreed to in writing, software
////  distributed under the License is distributed on an "AS IS" BASIS,
////  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
////  See the License for the specific language governing permissions and
////  limitations under the License.
////  ----------------------------------------------------------------------------------

//using DurableTask.Core.Common;
//using FASTER.core;
//using Microsoft.Azure.Storage;
//using Microsoft.Azure.Storage.Blob;
//using Microsoft.Azure.Storage.RetryPolicies;
//using Microsoft.Extensions.Logging;
//using Newtonsoft.Json;
//using System;
//using System.Collections.Generic;
//using System.IO;
//using System.Linq;
//using System.Threading;
//using System.Threading.Tasks;

using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventSourced.Faster
{
    /// <summary>
    /// Lease timing requires better reliability that what we get from asynchronous Task.Delay under starved thread pools, 
    /// so we implement a timer wheel.
    /// </summary>
    internal class LeaseTimer
    {
        private const int MaxDelay = 60;
        private const int TicksPerSecond = 4;

        private static readonly Lazy<LeaseTimer> instance = new Lazy<LeaseTimer>(() => new LeaseTimer());

        private readonly Timer timer;
        private readonly Entry[] schedule = new Entry[MaxDelay * TicksPerSecond];
       
        private volatile int position = 0;
        
        public static LeaseTimer Instance => instance.Value;

        private class Entry
        {
            public TaskCompletionSource<bool> Tcs;
            public Func<Task> Callback;
            public Entry Next;

            public async Task Run()
            {
                await Callback().ConfigureAwait(false);
                Tcs.SetResult(true);
            }

            public void RunAll()
            {
                var _ = this.Run();
                if (this.Next != null)
                {
                    this.Next.RunAll();
                }
            }
        }

        private LeaseTimer()
        {
            this.timer = new Timer(this.Run, null, 0, 1000 / TicksPerSecond);
        }

        public void Run(object _)
        {
            int position = this.position;
            this.position = (position + 1) % (MaxDelay * TicksPerSecond);

            Entry current;
            while (true)
            {
                current = this.schedule[position];
                if (current == null || Interlocked.CompareExchange<Entry>(ref this.schedule[position], null, current) == current)
                {
                    break;
                }
            }

            current?.RunAll();
        }

        public Task Schedule(int delay, Func<Task> callback)
        {
            if ((delay / 1000) >= MaxDelay || delay < 0)
            {
                throw new ArgumentException(nameof(delay));
            }

            var entry = new Entry()
            {
               Tcs = new TaskCompletionSource<bool>(),
               Callback = callback,
            };

            while (true)
            {
                int targetPosition = (this.position + (delay * TicksPerSecond) / 1000) % (MaxDelay * TicksPerSecond);

                if (targetPosition == this.position)
                {
                    return callback();
                }
                else
                {
                    Entry current = this.schedule[targetPosition];
                    entry.Next = current;
                    if (Interlocked.CompareExchange<Entry>(ref this.schedule[targetPosition], entry, current) == current)
                    {
                        return entry.Tcs.Task;
                    }
                }
            }
        }
    }
}
