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

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using DurableTask.Core.Common;

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
        public readonly object reentrancyLock = new object();

        private readonly Stopwatch stopwatch = new Stopwatch();
        private int performedSteps;

        private int position = 0;
        
        public static LeaseTimer Instance => instance.Value;

        public Action<int> DelayWarning { get; set; }

        private class Entry
        {
            public TaskCompletionSource<bool> Tcs;
            public CancellationTokenRegistration Registration;
            public Func<Task> Callback;
            public Entry Next;

            public async Task Run()
            {
                try
                {
                    await Callback().ConfigureAwait(false);
                    Tcs.TrySetResult(true);

                }
                catch (Exception exception) when (!Utils.IsFatal(exception))
                {
                    Tcs.TrySetException(exception);
                }
                Registration.Dispose();
            }

            public void Cancel()
            {
                Tcs.TrySetCanceled();
                Registration.Dispose();
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
            this.stopwatch.Start();
        }

        public void Run(object _)
        {
            if (Monitor.TryEnter(this.reentrancyLock))
            {
                try
                {
                    var stepsToDo = (this.stopwatch.ElapsedMilliseconds * TicksPerSecond / 1000) - this.performedSteps;

                    if (stepsToDo > 5 * TicksPerSecond)
                    {
                        this.DelayWarning?.Invoke((int)stepsToDo / TicksPerSecond);
                    }

                    for (int i = 0; i < stepsToDo; i++)
                    {
                        AdvancePosition();
                    }
                }
                finally
                {
                    Monitor.Exit(this.reentrancyLock);
                }
            }
        }

        private void AdvancePosition()
        {
            int position = this.position;
            this.position = (position + 1) % (MaxDelay * TicksPerSecond);
            this.performedSteps++;

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

        public Task Schedule(int delay, Func<Task> callback, CancellationToken token)
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

            entry.Registration = token.Register(entry.Cancel);

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
