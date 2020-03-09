using Dynamitey.DynamicObjects;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace DurableTask.EventSourced
{
    // For indicating and initiating termination.
    // Is essentially a thin wrapper around CancellationTokenSource
    // that allows for more diagnostics.
    internal class Termination
    {
        private readonly CancellationTokenSource cts = new CancellationTokenSource();
        private readonly IDisposable registration;

        public CancellationToken Token => cts.Token;
        public Partition Partition { set; private get; }
        public bool IsTerminated => cts.Token.IsCancellationRequested;

        public Termination()
        {
            this.cts = new CancellationTokenSource();
        }

        public Termination(CancellationToken linkedToken)
        {
            if (linkedToken.CanBeCanceled)
            {
                if (linkedToken.IsCancellationRequested)
                {
                    cts.Cancel();
                }
                else
                {
                    this.registration = linkedToken.Register(this.Callback);
                }
            }
        }

        private void Callback()
        {
            this.Terminate("linked cancellation token");
            this.registration?.Dispose();
        }

        public void Terminate(string reason)
        {
            try
            {
                this.Partition?.TraceDetail($"terminating ({reason})");
                this.cts.Cancel();
                this.registration?.Dispose();
            }
            catch (AggregateException aggregate)
            {
                foreach (var e in aggregate.InnerExceptions)
                {
                    this.Partition?.HandleError("exception while terminating partition", e, true);
                }
            }
            catch (Exception e)
            {
                this.Partition?.HandleError("exception while terminating partition", e, true);
            }
        }
    }
}
