//  ----------------------------------------------------------------------------------
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
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventSourced
{
    internal class CancellableCompletionSource<T> 
    {
        private readonly CancellationTokenRegistration CancellationRegistration;
        private readonly TaskCompletionSource<T> inner = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task<T> Task => inner.Task;

        public CancellableCompletionSource(CancellationToken token1)
        {
            token1.ThrowIfCancellationRequested();
            this.CancellationRegistration = token1.Register(this.TrySetCanceled);
        }

        public virtual void TrySetCanceled()
        {
            if (this.inner.TrySetCanceled())
            {
                this.CancellationRegistration.Dispose();
            }
        }

        public virtual void TrySetTimeoutException()
        {
            if (this.inner.TrySetException(new TimeoutException()))
            {
                this.CancellationRegistration.Dispose();
            }
        }

        public virtual void TrySetException(Exception e)
        {
            if (this.inner.TrySetException(e))
            {
                this.CancellationRegistration.Dispose();
            }
        }

        public bool TrySetResult(T result)
        {
            if (this.inner.TrySetResult(result))
            {
                this.CancellationRegistration.Dispose();
                return true;
            }
            else
            {
                return false;
            }
        }
    }
}
