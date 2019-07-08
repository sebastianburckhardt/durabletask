using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.Emulator
{
    internal class BatchTimer<T>
    {
        private readonly object thisLock = new object();
        private readonly CancellationToken cancellationToken;
        private readonly Task whenCancelled;
        private readonly Action<T> handler;
        private readonly SortedList<DateTime, T> schedule = new SortedList<DateTime, T>();

        public BatchTimer(CancellationToken token, Action<T> handler)
        {
            this.cancellationToken = token;
            this.whenCancelled = WhenCanceled();
            this.handler = handler;
        }

        private Task WhenCanceled()
        {
            var tcs = new TaskCompletionSource<bool>();
            this.cancellationToken.Register(s => ((TaskCompletionSource<bool>)s).SetResult(true), tcs);
            return tcs.Task;
        }

        public void Schedule(DateTime when, T what)
        {
            lock (schedule)
            {
                schedule.Add(when, what);
                
                if (when == schedule.First().Key)
                {
                    CheckExpirations();
                }
            }
        }

        private void CheckExpirations()
        {
            var now = DateTime.UtcNow;

            // remove all expired timers from the schedule and submit them to the thread pool
            lock (schedule)
            {
                while (schedule.Count > 0 && !cancellationToken.IsCancellationRequested)
                {
                    var next = schedule.First();

                    if (next.Key > now)
                    {
                        Task.Run(() => CheckExpirationsAfter(next.Key - now), cancellationToken);
                        return;
                    }

                    schedule.RemoveAt(0);

                    Task.Run(() => handler(next.Value), cancellationToken);
                }
            }
        }

        private async Task CheckExpirationsAfter(TimeSpan timeSpan)
        {
            await Task.WhenAny(Task.Delay(timeSpan), this.whenCancelled);
            CheckExpirations();
        }

        public IList<T> Values
        {
            get
            {
                lock (schedule)
                {
                    return schedule.Values.ToList();
                }
            }
        }

        public void Clear()
        {
            lock (schedule)
            {
                schedule.Clear();
            }
        }
    }
}
