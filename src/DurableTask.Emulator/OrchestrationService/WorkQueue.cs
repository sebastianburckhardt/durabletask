using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.Emulator
{
    internal class WorkQueue<T>
    {
        private readonly CancellationToken cancellationToken;
        private readonly object thisLock = new object();

        private readonly Queue<T> work = new Queue<T>();
        private readonly Queue<TaskCompletionSource<T>> waiters = new Queue<TaskCompletionSource<T>>();

        private readonly BatchTimer<TaskCompletionSource<T>> expirationTimer;

        public WorkQueue(CancellationToken token)
        {
            this.cancellationToken = token;
            this.expirationTimer = new BatchTimer<TaskCompletionSource<T>>(token, tcs => tcs.TrySetResult(default(T)));
            token.Register(OnCancel);
        }

        private void OnCancel()
        {
            foreach(var expiration in this.expirationTimer.Values)
            {
                expiration.TrySetException(new TaskCanceledException());
            }
        }

        public void Add(T element)
        {
            lock (thisLock)
            {
                // if there are waiters, hand it to the oldest one in line
                while (waiters.Count > 0)
                {
                    var next = waiters.Dequeue();
                    if (next.TrySetResult(element))
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

                var tcs = new TaskCompletionSource<T>();

                this.waiters.Enqueue(tcs);

                if (cancellationToken != this.cancellationToken)
                {
                    var ignoredTask = Link(tcs, cancellationToken);
                }

                this.expirationTimer.Schedule(DateTime.UtcNow + timeout, tcs);

                return tcs.Task;
            }          
        }

        private async Task Link(TaskCompletionSource<T> tcs, CancellationToken token)
        {
            using (cancellationToken.Register(() => Task.Run(() => tcs.SetException(new TaskCanceledException()))))
            {
                await tcs.Task; // awaiting here means we can unregister when the task completes
            }
        }
    }
}
