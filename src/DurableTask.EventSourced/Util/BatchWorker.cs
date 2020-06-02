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
        protected readonly object thisLock = new object();
        protected readonly CancellationToken cancellationToken;

        private List<T> batch = new List<T>();
        private List<T> queue = new List<T>();

        private TaskCompletionSource<bool> waiters;

        // Task for the current work cycle, or null if idle
        private volatile Task currentWorkCycle;

        // Flag is set to indicate that more work has arrived during execution of the task
        private bool moreWork;

        private Action<Task> checkForMoreWorkAction;

        private bool suspended;

        /// <summary>Implement this member in derived classes to process a batch</summary>
        protected abstract Task Process(IList<T> batch);

        public virtual void Submit(T entry)
        {
            lock (this.thisLock)
            {
                this.queue.Add(entry);
                this.Notify();
            }
        }

        public void Submit(T entry1, T entry2)
        {
            lock (this.thisLock)
            {
                this.queue.Add(entry1);
                this.queue.Add(entry2);
                this.Notify();
            }
        }

        public virtual void SubmitIncomingBatch(IEnumerable<T> entries)
        {
            lock (this.thisLock)
            {
                this.queue.AddRange(entries);
                this.Notify();
            }
        }

        protected void Requeue(IEnumerable<T> entries)
        {
            lock (this.thisLock)
            {
                this.queue.InsertRange(0, entries);
                this.Notify();
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

               this.Notify();

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
                cancellationToken.ThrowIfCancellationRequested();

                await this.Process(batch).ConfigureAwait(false);

                waiters?.SetResult(true);
            }
            catch (Exception e)
            {
                waiters?.SetException(e);
            }
            finally
            {
                this.batch.Clear();
            }
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
            // store delegate so it does not get newly allocated on each call
            this.checkForMoreWorkAction = (t) => this.CheckForMoreWork();
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
                this.Notify();
            }
        }

        /// <summary>
        /// Notify the worker that there is more work.
        /// </summary>
        public void Notify()
        {
            lock (this.thisLock)
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
                        // start a work cycle
                        this.Start();
                    }
                }
            }
        }

        private void Start()
        {
            try
            {
                // Start the task that is doing the work, on the threadpool
                this.currentWorkCycle = Task.Run(this.Work);
            }
            finally
            {
                // chain a continuation that checks for more work
                this.currentWorkCycle.ContinueWith(this.checkForMoreWorkAction);
            }
        }

        /// <summary>
        /// Executes at the end of each work cycle.
        /// </summary>
        private void CheckForMoreWork()
        {
            lock (this.thisLock)
            {
                if (this.moreWork)
                {
                    this.moreWork = false;

                    // start the next work cycle
                    this.Start();
                }
                else
                {
                    this.currentWorkCycle = null;
                }
            }
        }
    }
}