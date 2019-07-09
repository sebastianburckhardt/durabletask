using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.Emulator
{
    internal class BatchTimer<T> : IComparer<DateTime>
    {
        private readonly object thisLock = new object();
        private readonly CancellationToken cancellationToken;
        private readonly Task whenCancelled;
        private readonly Action<T> handler;
        private readonly SortedList<DateTime, T> schedule;

        private readonly SemaphoreSlim notify;

        public BatchTimer(CancellationToken token, Action<T> handler)
        {
            this.cancellationToken = token;
            this.whenCancelled = WhenCanceled();
            this.handler = handler;
            this.schedule = new SortedList<DateTime, T>(this);
            this.notify = new SemaphoreSlim(0, 1);

            new Thread(ExpirationCheckLoop).Start();
        }

        public int Compare(DateTime x, DateTime y)
        {
            int result = x.CompareTo(y);

            if (result == 0)
                return 1;   // Handle equality as being greater, so that the list can contain duplicate keys
            else
                return result;
        }

        private Task WhenCanceled()
        {
            var tcs = new TaskCompletionSource<bool>();
            this.cancellationToken.Register(s =>
            {
                ((TaskCompletionSource<bool>)s).SetResult(true);
                this.notify.Release();
            }, tcs);
            return tcs.Task;
        }

        public void Schedule(DateTime when, T what)
        {
            lock (this.schedule)
            {
                this.schedule.Add(when, what);

                // notify the expiration check loop
                if (when == this.schedule.First().Key)
                {
                    this.notify.Release();
                }
            }
        }

        private void ExpirationCheckLoop()
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                // wait for the next expiration time, but cut the wait short if notified
                if (this.RequiresDelay(out var delay))
                {
                    this.notify.Wait(delay); // blocks thread until delay is over, or until notified
                }

                // fire all expired entries
                while (this.TryGetNext(out var next))
                {
                    try
                    {
                        handler(next.Value);
                    }
                    catch
                    {
                        //TODO
                    }
                }
            }
        }

        private bool RequiresDelay(out TimeSpan delay)
        {
            lock (this.schedule)
            {
                if (this.schedule.Count == 0)
                {
                    delay = TimeSpan.FromMilliseconds(-1); // represents infinite delay
                    return true;
                }

                var next = this.schedule.First();
                var now = DateTime.UtcNow;

                if (next.Key > now)
                {
                    delay = next.Key - now;
                    return true;
                }
                else
                {
                    delay = default(TimeSpan);
                    return false;
                }
            }
        }

        private bool TryGetNext(out KeyValuePair<DateTime, T> next)
        {
            lock (this.schedule)
            {
                next = this.schedule.FirstOrDefault();

                if (this.schedule.Count > 0
                    && next.Key <= DateTime.UtcNow
                    && !this.cancellationToken.IsCancellationRequested)
                {
                    this.schedule.RemoveAt(0);
                    return true;
                }
                else
                {
                    return false;
                }
            }
        }
    }
}
