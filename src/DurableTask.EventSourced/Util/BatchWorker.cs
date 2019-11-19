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
using System.Threading.Tasks;

namespace DurableTask.EventSourced
{
    /// <summary>
    /// General pattern for an asynchronous worker that performs a work task, when notified,
    /// to service queued work. Each work cycle handles ALL the queued work. 
    /// If new work arrives during a work cycle, another cycle is scheduled. 
    /// The worker never executes more than one instance of the work cycle at a time, 
    /// and consumes no thread or task resources when idle.
    /// </summary>
    internal abstract class BatchWorker<T>
    {
        protected readonly object lockable = new object();
        private List<T> batch = new List<T>();
        private List<T> queue = new List<T>();

        // Task for the current work cycle, or null if idle
        private volatile Task currentWorkCycle;

        // Flag is set to indicate that more work has arrived during execution of the task
        private volatile bool moreWork;

        private Action<Task> checkForMoreWorkAction;

        /// <summary>Implement this member in derived classes to process a batch</summary>
        protected abstract Task Process(List<T> batch);

        public void Submit(T entry)
        {
            lock(this.lockable)
            {
                queue.Add(entry);
                this.Notify();
            }
        }

        protected void Requeue(IEnumerable<T> entries)
        {
            lock (this.lockable)
            {
                this.queue.InsertRange(0, entries);
                Notify();
            }
        }

        private async Task Work()
        {
            lock (this.lockable)
            {
                if (queue.Count == 0)
                    return;

                var temp = queue;
                queue = batch;
                batch = temp;
            }

            await this.Process(batch);

            batch.Clear();
        }

        /// <summary>
        /// Default constructor.
        /// </summary>
        public BatchWorker()
        {
            // store delegate so it does not get newly allocated on each call
            this.checkForMoreWorkAction = (t) => this.CheckForMoreWork(); 
        }

        /// <summary>
        /// Notify the worker that there is more work.
        /// </summary>
        public void Notify()
        {
            lock (lockable)
            {
                if (currentWorkCycle != null)
                {
                    // lets the current work cycle know that there is more work
                    moreWork = true;
                }
                else
                {
                    // start a work cycle
                    Start();
                }
            }
        }

        private void Start()
        {
            try
            {
                // Start the task that is doing the work
                currentWorkCycle = Task.Run(Work);
            }
            finally
            {
                // chain a continuation that checks for more work
                currentWorkCycle.ContinueWith(this.checkForMoreWorkAction);
            }
        }

        /// <summary>
        /// Executes at the end of each work cycle.
        /// </summary>
        private void CheckForMoreWork()
        {
            lock (lockable)
            {
                if (moreWork)
                {
                    moreWork = false;

                    // start the next work cycle
                    Start();
                }
                else
                {
                    currentWorkCycle = null;
                }
            }
        }
    }
}