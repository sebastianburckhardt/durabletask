﻿//  ----------------------------------------------------------------------------------
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

namespace DurableTask.EventHubs
{
    internal class CancellableCompletionSource<T> : CompletionSourceWithCleanup<T>
    {
        private readonly CancellationTokenRegistration CancellationRegistration;

        public bool IsCompleted => this.Task.IsCompleted;

        public CancellableCompletionSource(CancellationToken token1)
        {
            this.CancellationRegistration = token1.Register(this.TryCancel);
        }

        protected override void Cleanup()
        {
            CancellationRegistration.Dispose();
        }
    }
}
