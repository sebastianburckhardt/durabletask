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
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

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
        private readonly object thisLock = new object();
        private readonly Stopwatch stopwatch;
        protected readonly CancellationToken cancellationToken;

        private List<T> batch = new List<T>();
        private List<T> queue = new List<T>();

        private TaskCompletionSource<bool> waiters;

        // Task for the current work cycle, or null if idle
        private volatile Task currentWorkCycle;

        // Flag is set to indicate that more work has arrived during execution of the task
        private bool moreWork;

        // while suspended, work is not getting processed
        private bool suspended;

        /// <summary>Implement this member in derived classes to process a batch</summary>
        protected abstract Task Process(IList<T> batch);

        public virtual void Submit(T entry)
        {
            lock (this.thisLock)
            {
                this.queue.Add(entry);
                this.NotifyInternal();
            }
        }

        public virtual void SubmitBatch(IEnumerable<T> entries)
        {
            lock (this.thisLock)
            {
                this.queue.AddRange(entries);
                this.NotifyInternal();
            }
        }

        protected void Requeue(IEnumerable<T> entries)
        {
            lock (this.thisLock)
            {
                this.queue.InsertRange(0, entries);
                this.NotifyInternal();
            }
        }

        public virtual Task WaitForCompletionAsync()
        {
            lock (this.thisLock)
            {
               if (this.waiters == null)
               {
                  this.waiters = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
               }

                this.NotifyInternal();

                return this.waiters.Task;
            }
        }

        private async Task Work()
        {
            EventTraceContext.Clear();
            TaskCompletionSource<bool> waiters;

            lock (this.thisLock)
            {
                // swap the queues
                var temp = queue;
                this.queue = this.batch;
                this.batch = temp;

                // take the waiters and reset 
                waiters = this.waiters;
                this.waiters = null;
            }

            try
            {
                // check for cancellation right before doing work
                cancellationToken.ThrowIfCancellationRequested();

                this.stopwatch.Restart();

                // do the work, calling the virtual method
                await this.Process(batch).ConfigureAwait(false);

                // notify anyone who is waiting for completion
                waiters?.SetResult(true);
            }
            catch (Exception e)
            {
                waiters?.SetException(e);
            }
            finally
            {
                stopwatch.Stop();
                int batchsize = this.batch.Count;

                this.batch.Clear();

                // we always check for more work since there may be someone waiting for completion
                int? nextBatch = this.CheckForMoreWork();

                this.WorkLoopCompleted(batchsize, this.stopwatch.Elapsed.TotalMilliseconds, nextBatch);
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

        /// <summary>
        /// Default constructor.
        /// </summary>
        public BatchWorker() : this(CancellationToken.None)
        {
        }

        /// <summary>
        /// Constructor including a cancellation token.
        /// </summary>
        public BatchWorker(CancellationToken cancellationToken, bool suspended = false)
        {
            this.cancellationToken = cancellationToken;
            this.suspended = suspended;
            this.stopwatch = new Stopwatch();
        }

        public void Suspend()
        {
            lock (this.thisLock)
            {
                this.suspended = true;
            }
        }

        public void Resume()
        {
            lock (this.thisLock)
            {
                this.suspended = false;
                this.NotifyInternal();
            }
        }

        /// <summary>
        /// Notify the worker that there is more work.
        /// </summary>
        public void Notify()
        {
            lock (this.thisLock)
            {
                this.NotifyInternal();
            }
        }

        private void NotifyInternal()
        {
            if (!this.suspended) // while suspended, the worker is remains unaware of new work
            {
                if (this.currentWorkCycle != null)
                {
                    // lets the current work cycle know that there is more work
                    this.moreWork = true;
                }
                else
                {
                    // Start the task that is doing the work, on the threadpool
                    // this is important for fairness, so that other workers or tasks get a chance to run
                    // before the next batch is processed
                    this.currentWorkCycle = Task.Run(this.Work);
                }
            }
        }

        /// <summary>
        /// Executes at the end of each work cycle.
        /// </summary>
        private int? CheckForMoreWork()
        {
            lock (this.thisLock)
            {
                if (this.moreWork)
                {
                    this.moreWork = false;
                    int currentSize = this.queue.Count;

                    // Start the task that is doing the work, on the threadpool
                    this.currentWorkCycle = Task.Run(this.Work);
                    return currentSize;
                }
                else
                {
                    this.currentWorkCycle = null;
                    return null;
                }
            }
        }
    }
}