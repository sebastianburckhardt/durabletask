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
using FASTER.core;

namespace DurableTask.EventSourced.Faster
{
    internal class FasterLog
    {
        private readonly FASTER.core.FasterLog log;
        private readonly CancellationToken terminationToken;

        public FasterLog(BlobManager blobManager)
        {
            this.log = new FASTER.core.FasterLog(blobManager.EventLogSettings);
            this.terminationToken = blobManager.PartitionErrorHandler.Token;

            var _ = terminationToken.Register(
              () => {
                  try
                  {
                      this.log.Dispose();
                      blobManager.EventLogDevice.Close();
                  }
                  catch (Exception e)
                  {
                      blobManager.TraceHelper.FasterStorageError("Disposing FasterLog", e);
                  }
              },
              useSynchronizationContext: false);
        }

        public long BeginAddress => this.log.BeginAddress;
        public long TailAddress => this.log.TailAddress;
        public long CommittedUntilAddress => this.log.CommittedUntilAddress;

        public long Enqueue(byte[] entry)
        {
            try
            {
                return this.log.Enqueue(entry);
            }
            catch (Exception terminationException)
                when (this.terminationToken.IsCancellationRequested && !(terminationException is OutOfMemoryException))
            {
                throw new OperationCanceledException("partition was terminated", terminationException, this.terminationToken);
            }
        }

        public long Enqueue(ReadOnlySpan<byte> entry)
        {
            try
            {
                return this.log.Enqueue(entry);
            }
            catch (Exception terminationException)
                when (this.terminationToken.IsCancellationRequested && !(terminationException is OutOfMemoryException))
            {
                throw new OperationCanceledException("partition was terminated", terminationException, this.terminationToken);
            }
        }

        public async ValueTask CommitAsync()
        {
            try
            {
                await this.log.CommitAsync(this.terminationToken);
            }
            catch (Exception terminationException)
                when (this.terminationToken.IsCancellationRequested && !(terminationException is OutOfMemoryException))
            {
                throw new OperationCanceledException("partition was terminated", terminationException, this.terminationToken);
            }
        }

        public FasterLogScanIterator Scan(long beginAddress, long endAddress)
        {
            // used during recovery only

            // we are not wrapping termination exceptions here, since we would also have to wrap the iterator.
            // instead we wrap the whole replay loop in the caller.
            return this.log.Scan(beginAddress, endAddress); 
        }
    }
}