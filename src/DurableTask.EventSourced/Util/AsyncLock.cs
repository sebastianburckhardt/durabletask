using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventSourced
{ 
    /// <summary>
    /// Provides an async lock (does not block waiting threads)
    /// </summary>
    internal class AsyncLock : SemaphoreSlim, IDisposable
    {
        private readonly AcquisitionToken token; 

        public AsyncLock() : base(1, 1)
        {
            token = new AcquisitionToken()
            {
                AsyncLock = this
            };
        }

        public async ValueTask<AcquisitionToken> LockAsync()
        {
            await base.WaitAsync();
            return this.token;
        }

        internal struct AcquisitionToken : IDisposable
        {
            public AsyncLock AsyncLock;

            public void Dispose()
            {
                AsyncLock.Release();
            }
        }        
    }
}
