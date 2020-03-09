// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.NetworkInformation;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
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

        internal Termination Termination { get; private set; }
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
            this.Termination = blobManager.Termination;
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
            List<IListBlobItem> results = new List<IListBlobItem>();
            do
            {
                if (this.underLease)
                {
                    await this.BlobManager.ConfirmLeaseIsGoodForAWhileAsync();
                }
                var response = await this.blobDirectory.ListBlobsSegmentedAsync(useFlatBlobListing: false, blobListingDetails: BlobListingDetails.None, maxResults: 1000,
                    currentToken: continuationToken, options: this.BlobRequestOptions, operationContext: null);

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

                if (!Termination.IsTerminated)
                {
                    pageBlob.BeginDelete(ar =>
                    {
                        try
                        {
                            pageBlob.EndDelete(ar);
                        }
                        catch (Exception exception)
                        {
                            // Can I do anything else other than printing out an error message?
                            this.BlobManager?.Warning("could not remove page blob for segment", pageBlob, exception);
                        }
                        callback(ar);
                    }, result);
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
            try
            {
                if (this.underLease)
                {
                    await this.BlobManager.ConfirmLeaseIsGoodForAWhileAsync();
                }

                await blob.WritePagesAsync(stream, destinationAddress + offset,
                    contentChecksum: null, accessCondition: null, options: this.BlobRequestOptions, operationContext: null, cancellationToken: this.Termination.Token);
            }
            catch (Exception exception)
            {
                this.BlobManager?.FatalError("could not write to page blob", blob, exception);
                throw;
            }
            finally
            {
                stream.Dispose();
            }
        }

        private unsafe Task ReadFromBlobUnsafeAsync(CloudPageBlob blob, long sourceAddress, long destinationAddress, uint readLength)
        {
            return this.ReadFromBlobAsync(new UnmanagedMemoryStream((byte*)destinationAddress, readLength, readLength, FileAccess.Write), blob, sourceAddress, destinationAddress, readLength);
        }

        private async Task ReadFromBlobAsync(UnmanagedMemoryStream stream, CloudPageBlob blob, long sourceAddress, long destinationAddress, uint readLength)
        {
            try
            {
                if (this.underLease)
                {
                    await this.BlobManager.ConfirmLeaseIsGoodForAWhileAsync();
                }

                await blob.DownloadRangeToStreamAsync(stream, sourceAddress, readLength,
                    accessCondition: null, options: this.BlobRequestOptions, operationContext: null, cancellationToken: this.Termination.Token);

                if (stream.Position != readLength)
                {
                    throw new InvalidDataException($"wrong amount of data received from page blob, expected={readLength}, actual={stream.Position}");
                }
            }
            catch (Exception exception)
            {
                this.BlobManager?.FatalError("could not read from page blob", blob, exception);
                throw;
            }
            finally
            {
                stream.Dispose();
            }
        }

        //---- the overridden methods represent the interface for a generic storage device

        /// <summary>
        /// <see cref="IDevice.ReadAsync(int, ulong, IntPtr, uint, IOCompletionCallback, IAsyncResult)">Inherited</see>
        /// </summary>
        public override unsafe void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength, IOCompletionCallback callback, IAsyncResult asyncResult)
        {
            // It is up to the allocator to make sure no reads are issued to segments before they are written
            if (!blobs.TryGetValue(segmentId, out BlobEntry blobEntry))
            {
                var nonLoadedBlob = this.blobDirectory.GetPageBlobReference(GetSegmentBlobName(segmentId));
                var exception = new InvalidOperationException("Attempt to read a non-loaded segment");
                this.BlobManager?.FatalError(exception.Message, nonLoadedBlob, exception);
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
                          callback(uint.MaxValue, readLength, ovNative);
                      }
                      else
                      {
                          callback(0, readLength, ovNative);
                      }
                  });
        }

        /// <summary>
        /// <see cref="IDevice.WriteAsync(IntPtr, int, ulong, uint, IOCompletionCallback, IAsyncResult)">Inherited</see>
        /// </summary>
        public override void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite, IOCompletionCallback callback, IAsyncResult asyncResult)
        {
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
            TryWriteAsync(blobEntry, sourceAddress, destinationAddress, numBytesToWrite, callback, asyncResult);
        }

        private void TryWriteAsync(BlobEntry blobEntry, IntPtr sourceAddress, ulong destinationAddress, uint numBytesToWrite, IOCompletionCallback callback, IAsyncResult asyncResult)
        {
            // If pageBlob is null, it is being created. Attempt to queue the write for the creator to complete after it is done
            if (blobEntry.PageBlob == null
                && blobEntry.TryQueueAction(p => WriteToBlobAsync(p, sourceAddress, destinationAddress, numBytesToWrite, callback, asyncResult)))
            {
                return;
            }
            // Otherwise, invoke directly.
            WriteToBlobAsync(blobEntry.PageBlob, sourceAddress, destinationAddress, numBytesToWrite, callback, asyncResult);
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
                           callback(uint.MaxValue, numBytesToWrite, ovNative);
                       }
                       else
                       {
                           callback(0, numBytesToWrite, ovNative);
                       }
                   });
        }

        const int maxPortionSizeForPageBlobWrites = 4 * 1024 * 1024; // 4 MB is a limit on page blob write portions, apparently

        private async Task WriteToBlobAsync(CloudPageBlob blob, IntPtr sourceAddress, long destinationAddress, uint numBytesToWrite)
        {
            long offset = 0;
            while (numBytesToWrite > 0)
            {
                var length = Math.Min(numBytesToWrite, maxPortionSizeForPageBlobWrites);
                await this.WritePortionToBlobUnsafeAsync(blob, sourceAddress, destinationAddress, offset, length);
                numBytesToWrite -= length;
                offset += length;
            }
        }
    }
}
