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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Net.NetworkInformation;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using DurableTask.Core.Common;
using DurableTask.EventSourced.Faster;
using FASTER.core;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.Storage.RetryPolicies;

namespace DurableTask.EventSourced.Faster
{
    /// <summary>
    /// A IDevice Implementation that is backed by<see href="https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blob-pageblob-overview">Azure Page Blob</see>.
    /// This device is slower than a local SSD or HDD, but provides scalability and shared access in the cloud.
    /// </summary>
    internal class AzureStorageDevice : StorageDeviceBase
    {
        private readonly ConcurrentDictionary<int, BlobEntry> blobs;
        private readonly CloudBlobDirectory blobDirectory;
        private readonly string blobName;
        private readonly bool underLease;

        internal IPartitionErrorHandler PartitionErrorHandler { get; private set; }
        internal BlobRequestOptions BlobRequestOptions { get; private set; }

        // Page Blobs permit blobs of max size 8 TB, but the emulator permits only 2 GB
        private const long MAX_BLOB_SIZE = (long)(2 * 10e8);
        // Azure Page Blobs have a fixed sector size of 512 bytes.
        private const uint PAGE_BLOB_SECTOR_SIZE = 512;

        /// <summary>
        /// Constructs a new AzureStorageDevice instance, backed by Azure Page Blobs
        /// </summary>
        /// <param name="blobName">A descriptive name that will be the prefix of all segments created</param>
        /// <param name="blobDirectory">the directory containing the blob</param>
        /// <param name="blobManager">the blob manager handling the leases</param>
        /// <param name="underLease">whether this device needs to be protected by the lease</param>
        public AzureStorageDevice(string blobName, CloudBlobDirectory blobDirectory, BlobManager blobManager, bool underLease)
            : base($"{blobDirectory}\\{blobName}", PAGE_BLOB_SECTOR_SIZE, Devices.CAPACITY_UNSPECIFIED)
        {
            this.blobs = new ConcurrentDictionary<int, BlobEntry>();
            this.blobDirectory = blobDirectory;
            this.blobName = blobName;
            this.PartitionErrorHandler = blobManager.PartitionErrorHandler;
            this.BlobManager = blobManager;
            this.underLease = underLease;
            this.BlobRequestOptions = underLease ? blobManager.BlobRequestOptionsUnderLease : blobManager.BlobRequestOptionsNotUnderLease; 
        }

        public async Task StartAsync()
        {
            // list all the blobs representing the segments

            int prevSegmentId = -1;
            var prefix = $"{blobDirectory.Prefix}{blobName}.";

            BlobContinuationToken continuationToken = null;
            do
            {
                if (this.underLease)
                {
                    await this.BlobManager.ConfirmLeaseIsGoodForAWhileAsync().ConfigureAwait(false);
                }
                var response = await this.blobDirectory.ListBlobsSegmentedAsync(useFlatBlobListing: false, blobListingDetails: BlobListingDetails.None, maxResults: 1000,
                    currentToken: continuationToken, options: this.BlobRequestOptions, operationContext: null).ConfigureAwait(false);

                foreach (IListBlobItem item in response.Results)
                {
                    if (item is CloudPageBlob pageBlob)
                    {
                        if (Int32.TryParse(pageBlob.Name.Replace(prefix, ""), out int segmentId))
                        {
                            if (segmentId != prevSegmentId + 1)
                            {
                                startSegment = segmentId;
                            }
                            else
                            {
                                endSegment = segmentId;
                            }
                            prevSegmentId = segmentId;
                        }
                    }
                }
                continuationToken = response.ContinuationToken;
            }
            while (continuationToken != null);

            for (int i = startSegment; i <= endSegment; i++)
            {
                bool ret = this.blobs.TryAdd(i, new BlobEntry(this.blobDirectory.GetPageBlobReference(GetSegmentBlobName(i)), this));

                if (!ret)
                {
                    throw new InvalidOperationException("Recovery of blobs is single-threaded and should not yield any failure due to concurrency");
                }
            }
        }

        /// <summary>
        /// Is called on exceptions, if non-null; can be set by application
        /// </summary>
        internal BlobManager BlobManager { get; set; }

        private string GetSegmentBlobName(int segmentId)
        {
            return $"{blobName}.{segmentId}";
        }

        /// <summary>
        /// <see cref="IDevice.Close">Inherited</see>
        /// </summary>
        public override void Close()
        {
        }

        /// <summary>
        /// <see cref="IDevice.RemoveSegmentAsync(int, AsyncCallback, IAsyncResult)"/>
        /// </summary>
        /// <param name="segment"></param>
        /// <param name="callback"></param>
        /// <param name="result"></param>
        public override void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result)
        {
            if (this.blobs.TryRemove(segment, out BlobEntry blob))
            {
                CloudPageBlob pageBlob = blob.PageBlob;

                if (this.underLease)
                {
                    this.BlobManager.ConfirmLeaseIsGoodForAWhile();
                }

                if (!this.PartitionErrorHandler.IsTerminated)
                {
                    pageBlob.DeleteAsync(cancellationToken: this.PartitionErrorHandler.Token)
                       .ContinueWith((Task t) =>
                       {
                           if (t.IsFaulted)
                           {
                               this.BlobManager?.HandleBlobError(nameof(RemoveSegmentAsync), "could not remove page blob for segment", pageBlob?.Name, t.Exception, false, true);
                           }
                           callback(result);
                       });
                }
            }
        }

        //---- The actual read and write accesses to the page blobs

        private unsafe Task WritePortionToBlobUnsafeAsync(CloudPageBlob blob, IntPtr sourceAddress, long destinationAddress, long offset, uint length)
        {
            return this.WritePortionToBlobAsync(new UnmanagedMemoryStream((byte*)sourceAddress + offset, length), blob, sourceAddress, destinationAddress, offset, length);
        }

        private async Task WritePortionToBlobAsync(UnmanagedMemoryStream stream, CloudPageBlob blob, IntPtr sourceAddress, long destinationAddress, long offset, uint length)
        {
            await BlobManager.AsynchronousStorageWriteMaxConcurrency.WaitAsync();

            try
            {
                if (this.underLease)
                {
                    await this.BlobManager.ConfirmLeaseIsGoodForAWhileAsync().ConfigureAwait(false);
                }

                await blob.WritePagesAsync(stream, destinationAddress + offset,
                    contentChecksum: null, accessCondition: null, options: this.BlobRequestOptions, operationContext: null, cancellationToken: this.PartitionErrorHandler.Token).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                this.BlobManager?.HandleBlobError(nameof(WritePortionToBlobAsync), "could not write to page blob", blob?.Name, exception, true, this.PartitionErrorHandler.IsTerminated);
                throw;
            }
            finally
            {
                stream.Dispose();
                BlobManager.AsynchronousStorageWriteMaxConcurrency.Release();
            }
        }

        private unsafe Task ReadFromBlobUnsafeAsync(CloudPageBlob blob, long sourceAddress, long destinationAddress, uint readLength)
        {
            return this.ReadFromBlobAsync(new UnmanagedMemoryStream((byte*)destinationAddress, readLength, readLength, FileAccess.Write), blob, sourceAddress, destinationAddress, readLength);
        }

        private async Task ReadFromBlobAsync(UnmanagedMemoryStream stream, CloudPageBlob blob, long sourceAddress, long destinationAddress, uint readLength)
        {
            await BlobManager.AsynchronousStorageReadMaxConcurrency.WaitAsync();

            try
            {
                this.BlobManager?.StorageTracer?.FasterStorageProgress($"AzureStorageDevice.ReadFromBlobAsync Called target={blob.Name} readLength={readLength} sourceAddress={sourceAddress}");
                
                if (this.underLease)
                {
                    this.BlobManager?.StorageTracer?.FasterStorageProgress($"confirm lease");
                    await this.BlobManager.ConfirmLeaseIsGoodForAWhileAsync().ConfigureAwait(false);
                    this.BlobManager?.StorageTracer?.FasterStorageProgress($"confirm lease done");
                }

                this.BlobManager?.StorageTracer?.FasterStorageProgress($"starting download target={blob.Name} readLength={readLength} sourceAddress={sourceAddress}");

                await blob.DownloadRangeToStreamAsync(stream, sourceAddress, readLength,
                         accessCondition: null, options: this.BlobRequestOptions, operationContext: null, cancellationToken: this.PartitionErrorHandler.Token);

                this.BlobManager?.StorageTracer?.FasterStorageProgress($"finished download target={blob.Name} readLength={readLength} sourceAddress={sourceAddress}");

                if (stream.Position != readLength)
                {
                    throw new InvalidDataException($"wrong amount of data received from page blob, expected={readLength}, actual={stream.Position}");
                }
            }
            catch (Exception exception)
            {
                this.BlobManager?.HandleBlobError(nameof(ReadFromBlobAsync), "could not read from page blob", blob?.Name, exception, true, this.PartitionErrorHandler.IsTerminated);
                throw;
            }
            finally
            {
                stream.Dispose();

                BlobManager.AsynchronousStorageReadMaxConcurrency.Release();
            }
        }

        //---- the overridden methods represent the interface for a generic storage device

        /// <summary>
        /// <see cref="IDevice.ReadAsync(int, ulong, IntPtr, uint, IOCompletionCallback, IAsyncResult)">Inherited</see>
        /// </summary>
        public override unsafe void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength, IOCompletionCallback callback, IAsyncResult asyncResult)
        {
            this.BlobManager?.StorageTracer?.FasterStorageProgress($"AzureStorageDevice.ReadAsync Called segmentId={segmentId} sourceAddress={sourceAddress} readLength={readLength}");

            // It is up to the allocator to make sure no reads are issued to segments before they are written
            if (!blobs.TryGetValue(segmentId, out BlobEntry blobEntry))
            {
                var nonLoadedBlob = this.blobDirectory.GetPageBlobReference(GetSegmentBlobName(segmentId));
                var exception = new InvalidOperationException("Attempt to read a non-loaded segment");
                this.BlobManager?.HandleBlobError(nameof(ReadAsync), exception.Message, nonLoadedBlob?.Name, exception, true, false);
                throw exception;
            }

            // Even though Azure Page Blob does not make use of Overlapped, we populate one to conform to the callback API
            Overlapped ov = new Overlapped(0, 0, IntPtr.Zero, asyncResult);
            NativeOverlapped* ovNative = ov.UnsafePack(callback, IntPtr.Zero);

            this.ReadFromBlobUnsafeAsync(blobEntry.PageBlob, (long)sourceAddress, (long)destinationAddress, readLength)
                  .ContinueWith((Task t) =>
                  {
                      if (t.IsFaulted)
                      {
                          this.BlobManager?.StorageTracer?.FasterStorageProgress("AzureStorageDevice.ReadAsync Returned (Failure)");
                          callback(uint.MaxValue, readLength, ovNative);
                      }
                      else
                      {
                          this.BlobManager?.StorageTracer?.FasterStorageProgress("AzureStorageDevice.ReadAsync Returned");
                          callback(0, readLength, ovNative);
                      }
                  });
        }

        /// <summary>
        /// <see cref="IDevice.WriteAsync(IntPtr, int, ulong, uint, IOCompletionCallback, IAsyncResult)">Inherited</see>
        /// </summary>
        public override void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite, IOCompletionCallback callback, IAsyncResult asyncResult)
        {
            this.BlobManager?.StorageTracer?.FasterStorageProgress($"AzureStorageDevice.WriteAsync Called segmentId={segmentId} destinationAddress={destinationAddress} numBytesToWrite={numBytesToWrite}");

            if (!blobs.TryGetValue(segmentId, out BlobEntry blobEntry))
            {
                BlobEntry entry = new BlobEntry(this);
                if (blobs.TryAdd(segmentId, entry))
                {
                    CloudPageBlob pageBlob = this.blobDirectory.GetPageBlobReference(GetSegmentBlobName(segmentId));

                    // If segment size is -1, which denotes absence, we request the largest possible blob. This is okay because
                    // page blobs are not backed by real pages on creation, and the given size is only a the physical limit of 
                    // how large it can grow to.
                    var size = segmentSize == -1 ? MAX_BLOB_SIZE : segmentSize;

                    // If no blob exists for the segment, we must first create the segment asynchronouly. (Create call takes ~70 ms by measurement)
                    // After creation is done, we can call write.
                    var ignoredTask = entry.CreateAsync(size, pageBlob);
                }
                // Otherwise, some other thread beat us to it. Okay to use their blobs.
                blobEntry = blobs[segmentId];
            }
            this.TryWriteAsync(blobEntry, sourceAddress, destinationAddress, numBytesToWrite, callback, asyncResult);
        }

        private void TryWriteAsync(BlobEntry blobEntry, IntPtr sourceAddress, ulong destinationAddress, uint numBytesToWrite, IOCompletionCallback callback, IAsyncResult asyncResult)
        {
            // If pageBlob is null, it is being created. Attempt to queue the write for the creator to complete after it is done
            if (blobEntry.PageBlob == null
                && blobEntry.TryQueueAction(p => this.WriteToBlobAsync(p, sourceAddress, destinationAddress, numBytesToWrite, callback, asyncResult)))
            {
                return;
            }
            // Otherwise, invoke directly.
            this.WriteToBlobAsync(blobEntry.PageBlob, sourceAddress, destinationAddress, numBytesToWrite, callback, asyncResult);
        }

        private unsafe void WriteToBlobAsync(CloudPageBlob blob, IntPtr sourceAddress, ulong destinationAddress, uint numBytesToWrite, IOCompletionCallback callback, IAsyncResult asyncResult)
        {
            // Even though Azure Page Blob does not make use of Overlapped, we populate one to conform to the callback API
            Overlapped ov = new Overlapped(0, 0, IntPtr.Zero, asyncResult);
            NativeOverlapped* ovNative = ov.UnsafePack(callback, IntPtr.Zero);

            this.WriteToBlobAsync(blob, sourceAddress, (long)destinationAddress, numBytesToWrite)
                .ContinueWith((Task t) =>
                    {
                        if (t.IsFaulted)
                        {
                            this.BlobManager?.StorageTracer?.FasterStorageProgress("AzureStorageDevice.WriteAsync Returned (Failure)");
                            callback(uint.MaxValue, numBytesToWrite, ovNative);
                        }
                        else
                        {
                            this.BlobManager?.StorageTracer?.FasterStorageProgress("AzureStorageDevice.WriteAsync Returned");
                            callback(0, numBytesToWrite, ovNative);
                        }
                    });
        }

        const int maxPortionSizeForPageBlobWrites = 256 * 1024; // better to not upload very large portions (e.g. so lease renewal is smoother)

        private async Task WriteToBlobAsync(CloudPageBlob blob, IntPtr sourceAddress, long destinationAddress, uint numBytesToWrite)
        {
            long offset = 0;
            while (numBytesToWrite > 0)
            {
                var length = Math.Min(numBytesToWrite, maxPortionSizeForPageBlobWrites);
                await this.WritePortionToBlobUnsafeAsync(blob, sourceAddress, destinationAddress, offset, length).ConfigureAwait(false);
                numBytesToWrite -= length;
                offset += length;
            }
        }
    }
}
