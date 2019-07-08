using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.Emulator
{
    internal class CancellablePromise<T> : Promise<T>
    {
        private readonly CancellationTokenRegistration CancellationRegistration;

        public CancellablePromise(CancellationToken token1)
        {
            this.CancellationRegistration = token1.Register(this.TryCancel);
        }

        protected override void Cleanup()
        {
            CancellationRegistration.Dispose();
        }
    }
}
