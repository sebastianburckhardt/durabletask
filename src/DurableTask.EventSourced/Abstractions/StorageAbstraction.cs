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
using System.Runtime.Serialization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventSourced
{
    /// <summary>
    /// Abstractions for the partition storage, that allow different providers to be used.
    /// </summary>
    internal static class StorageAbstraction
    {
        /// <summary>
        /// Abstraction for accessing the storage functionality via a configured provider.
        /// </summary>
        internal interface IStorageProvider
        {
            /// <summary>
            /// Creates a <see cref="IPartitionState"/> object that can be used to store the partition state.
            /// </summary>
            /// <returns></returns>
            StorageAbstraction.IPartitionState CreatePartitionState();

            /// <summary>
            /// Deletes all partition states for this taskhub.
            /// </summary>
            /// <returns></returns>
            Task DeleteAllPartitionStatesAsync();
        }

        /// <summary>
        /// Storage abstraction for the event-sourced state of a partition.
        /// </summary>
        internal interface IPartitionState
        {
            /// <summary>
            /// Restore the state of a partition from storage, or create a new one if there is nothing stored.
            /// </summary>
            /// <param name="localPartition">The partition.</param>
            /// <param name="errorHandler">An error handler to initiate and/or indicate termination of this partition.</param>
            /// <param name="firstInputQueuePosition">For new partitions, the position of the first message to receive.</param>
            /// <returns>the input queue position from which to resume input processing</returns>
            /// <exception cref="OperationCanceledException">Indicates that termination was signaled before the operation completed.</exception>
            Task<ulong> CreateOrRestoreAsync(Partition localPartition, IPartitionErrorHandler errorHandler, ulong firstInputQueuePosition);

            /// <summary>
            /// Finish processing events and save the partition state to storage.
            /// </summary>
            /// <param name="takeFinalCheckpoint">Whether to take a final state checkpoint.</param>
            /// <returns>A task that completes when the state has been saved.</returns>
            /// <exception cref="OperationCanceledException">Indicates that termination was signaled before the operation completed.</exception>
            Task CleanShutdown(bool takeFinalCheckpoint);

            /// <summary>
            /// Queues a single event for processing on this partition state.
            /// </summary>
            /// <param name="evt">The event to process.</param>
            void SubmitEvent(PartitionEvent evt);

            /// <summary>
            /// Queues a collection of external events for processing on this partition state.
            /// </summary>
            /// <param name="evt">The collection of events to process.</param>
            void SubmitExternalEvents(IEnumerable<PartitionEvent> evt);

            /// <summary>
            /// Queues a internal read-only event for processing on this partition state.
            /// </summary>
            /// <param name="readEvent">The readonly event to process.</param>
            void SubmitInternalReadonlyEvent(IInternalReadonlyEvent readEvent);
        }

        /// <summary>
        /// An interface for events representing internal read-only operation on storage. Since this kind of
        /// event does not need to be serialized, it is kept simple and outside the Event class hierarchy.
        /// </summary>
        public interface IInternalReadonlyEvent
        {
            /// <summary>
            /// The target of the read operation.
            /// </summary>
            TrackedObjectKey ReadTarget { get; }

            /// <summary>
            /// The continuation for the read operation.
            /// </summary>
            /// <param name="target">The current value of the tracked object for this key, or null if not present</param>
            void OnReadComplete(TrackedObject target);

            /// <summary>
            /// An id for this read event, for diagnostic purposes
            /// </summary>
            string EventIdString { get; }
        }
    }
}