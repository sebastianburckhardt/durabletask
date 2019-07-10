using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventHubs
{
    internal class Promise<T> : TaskCompletionSource<T>
    { 
        protected virtual void Cleanup()
        {
        }

        public virtual void TryCancel()
        {
            if (this.TrySetCanceled())
            {
                this.Cleanup();
            }
        }

        public void TryTimeout()
        {
            if (this.TryFulfill(default(T)))
            {
                this.Cleanup();
            }
        }

        public bool TryFulfill(T result)
        {
            if (this.TrySetResult(result))
            {
                this.Cleanup();
                return true;
            }
            else
            {
                return false;
            }
        }
    }
}
