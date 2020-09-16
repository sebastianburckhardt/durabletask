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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos.Table;

namespace DurableTask.EventSourced
{
    /// <summary>
    /// General pattern for an asynchronous worker that performs a work task, when notified,
    /// to service queued work on the thread pool. Each work cycle handles ALL the queued work. 
    /// If new work arrives during a work cycle, another cycle is scheduled. 
    /// The worker never executes more than one instance of the work cycle at a time, 
    /// and consumes no thread or task resources when idle.
    /// </summary>
    internal abstract class BatchWorker<T>
    {
        private readonly Stopwatch stopwatch;
        protected readonly CancellationToken cancellationToken;

        private volatile int state;
        private const int IDLE = 0;
        private const int RUNNING = 1;
        private const int SUSPENDED = 2;
        private const int SHUTTINGDOWN = 3;

        private ConcurrentQueue<object> work = new ConcurrentQueue<object>();

        private const int MAXBATCHSIZE = 10000;

        private object dummyEntry = new object();

        /// <summary>
        /// Default constructor.
        /// </summary>
        public BatchWorker(string name) : this(name, CancellationToken.None)
        {
        }

        /// <summary>
        /// Constructor including a cancellation token.
        /// </summary>
        public BatchWorker(string name, CancellationToken cancellationToken, bool suspended = false)
        {
            this.cancellationToken = cancellationToken;
            this.state = suspended ? SUSPENDED : IDLE;
            this.stopwatch = new Stopwatch();
        }

        /// <summary>Implement this member in derived classes to process a batch</summary>
        protected abstract Task Process(IList<T> batch);

        public void Submit(T entry)
        {
            work.Enqueue(entry);
            this.NotifyInternal();
        }

        public void SubmitBatch(IList<T> entries)
        {
            foreach (var e in entries)
            {
                work.Enqueue(e);
            }
            this.NotifyInternal();
        }

        public void SubmitBatch(IList<T> entries, SemaphoreSlim credits)
        {
            foreach (var e in entries)
            {
                work.Enqueue(e);
            }
            if (credits != null)
            {
                work.Enqueue(credits);
            }
            this.NotifyInternal();
        }

        public void Notify()
        {
            this.work.Enqueue(dummyEntry);
            this.NotifyInternal();
        }

        protected void Requeue(IList<T> entries)
        {
            this.requeued = entries;
        }

        public virtual Task WaitForCompletionAsync()
        {
            var tcs = new TaskCompletionSource<bool>();
            work.Enqueue(tcs);
            this.Notify();
            return tcs.Task;
        }

        private List<T> batch = new List<T>();
        private List<TaskCompletionSource<bool>> waiters = new List<TaskCompletionSource<bool>>();
        private IList<T> requeued = null;

        private int? GetNextBatch()
        {
            bool runAgain = false;

            this.batch.Clear();
            this.waiters.Clear();

            if (requeued != null)
            {
                runAgain = true;
                batch.AddRange(requeued);
                requeued = null;
            }

            while (batch.Count < MAXBATCHSIZE)
            {
                if (!this.work.TryDequeue(out object entry))
                {
                    break;
                }
                else if (entry is T t)
                {
                    runAgain = true;
                    batch.Add(t);
                }
                else if (entry == dummyEntry)
                {
                    runAgain = true;
                    continue;
                }
                else if (entry is SemaphoreSlim credits)
                {
                    runAgain = true;
                    credits.Release();
                }
                else
                {
                    runAgain = true;
                    waiters.Add((TaskCompletionSource<bool>)entry);
                }
            }

            return runAgain ? batch.Count : (int?)null;
        }


        private async Task Work()
        {
            EventTraceContext.Clear();
            int? previousBatch = null;

            while(!cancellationToken.IsCancellationRequested)
            {
                int? nextBatch = this.GetNextBatch();

                if (previousBatch.HasValue)
                {
                    this.WorkLoopCompleted(previousBatch.Value, this.stopwatch.Elapsed.TotalMilliseconds, nextBatch);
                }

                if (!nextBatch.HasValue)
                {
                    // no work found. Announce that we are planning to shut down.
                    // Using an interlocked exchange to enforce store-load fence.
                    Interlocked.Exchange(ref this.state, SHUTTINGDOWN);

                    // but recheck so we don't miss work that was just added
                    nextBatch = this.GetNextBatch();

                    if (!nextBatch.HasValue)
                    { 
                        // still no work. Try to transition to idle but revert if state has been changed.
                        var read = Interlocked.CompareExchange(ref this.state, IDLE, SHUTTINGDOWN);

                        if (read == SHUTTINGDOWN)
                        {
                            // shut down is complete
                            return;
                        }
                    }

                    // shutdown canceled, we are running
                    this.state = RUNNING;
                }

                this.stopwatch.Restart();

                try
                {
                    // recheck for cancellation right before doing work
                    cancellationToken.ThrowIfCancellationRequested();

                    // do the work, calling the virtual method
                    await this.Process(batch).ConfigureAwait(false);

                    // notify anyone who is waiting for completion
                    foreach (var w in waiters)
                    {
                        w.TrySetResult(true);
                    }
                }
                catch (Exception e)
                {
                    foreach (var w in waiters)
                    {
                        w.TrySetException(e);
                    }
                }
           
                stopwatch.Stop();

                previousBatch = this.batch.Count;
            }
        }

        /// <summary>
        /// Can be overridden by subclasses to trace progress of the batch work loop
        /// </summary>
        /// <param name="batchSize">The size of the batch that was processed</param>
        /// <param name="elapsedMilliseconds">The time in milliseconds it took to process</param>
        /// <param name="nextBatch">If there is more work, the current queue size of the next batch, or null otherwise</param>
        protected virtual void WorkLoopCompleted(int batchSize, double elapsedMilliseconds, int? nextBatch)
        {
        }

        public void Resume()
        {
            this.work.Enqueue(dummyEntry);
            this.NotifyInternal(true);
        }

        private void NotifyInternal(bool resume = false)
        {
            while (true)
            {
                int currentState = this.state;
                if (currentState == RUNNING)
                {
                    return;
                }
               else if (currentState == IDLE || (currentState == SUSPENDED && resume) || currentState == SHUTTINGDOWN)
                {
                    int read = Interlocked.CompareExchange(ref this.state, RUNNING, currentState);
                    if (read == currentState)
                    {
                        if (read != SHUTTINGDOWN)
                        {
                            Task.Run(this.Work);
                        }
                        break;
                    }
                    else
                    {
                        continue;
                    }
                }
            }
        }
    }
}