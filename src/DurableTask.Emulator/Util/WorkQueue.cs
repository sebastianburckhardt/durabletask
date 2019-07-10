using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventHubs
{
    internal class WorkQueue<T>
    {
        private readonly CancellationToken cancellationToken;
        private readonly object thisLock = new object();

        private readonly Queue<T> work = new Queue<T>();
        private readonly Queue<CancellablePromise<T>> waiters = new Queue<CancellablePromise<T>>();

        private readonly BatchTimer<CancellablePromise<T>> expirationTimer;

        public WorkQueue(CancellationToken token, Func<IEnumerable<CancellablePromise<T>>,Task> onExpiration)
        {
            this.cancellationToken = token;
            this.expirationTimer = new BatchTimer<CancellablePromise<T>>(token, onExpiration);
        }

        public void Add(T element)
        {
            lock (thisLock)
            {
                // if there are waiters, hand it to the oldest one in line
                while (waiters.Count > 0)
                {
                    var next = waiters.Dequeue();
                    if (next.TryFulfill(element))
                    {
                        return;
                    }
                }
                    
                work.Enqueue(element);
            }
        }

        public Task<T> GetNext(TimeSpan timeout, CancellationToken cancellationToken)
        {
            lock (thisLock)
            {
                while (work.Count > 0)
                {
                    var next = work.Dequeue();
                    return Task.FromResult(next);
                }

                var tcs = new CancellablePromise<T>(cancellationToken);

                this.expirationTimer.Schedule(DateTime.UtcNow + timeout, tcs);

                this.waiters.Enqueue(tcs);

                return tcs.Task;
            }
        }
    }
}
