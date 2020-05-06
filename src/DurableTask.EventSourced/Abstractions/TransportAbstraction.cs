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
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;

namespace DurableTask.EventSourced
{
    /// <summary>
    /// Abstractions for defining a transport back-end functionality, including the 
    /// sending/receiving of messages, and the load balancing of partitions.
    /// The backend creates one client per connected host, and load-balances partitions over the 
    /// connected hosts.
    /// </summary>
    internal static class TransportAbstraction
    {
        /// <summary>
        /// Top-level functionality for starting and stopping the transport back-end on a machine.
        /// </summary>
        public interface ITaskHub
        {
            /// <summary>
            /// Tests whether this taskhub exists in storage.
            /// </summary>
            /// <returns>true if this taskhub has been created in storage.</returns>
            Task<bool> ExistsAsync();

            /// <summary>
            /// Creates this taskhub in storage.
            /// </summary>
            /// <returns>after the taskhub has been created in storage.</returns>
            Task CreateAsync();

            /// <summary>
            /// Deletes this taskhub and all of its associated data in storage.
            /// </summary>
            /// <returns>after the taskhub has been deleted from storage.</returns>
            Task DeleteAsync();

            /// <summary>
            /// Starts the transport backend.
            /// </summary>
            /// <returns>After the transport backend has started and created the client.</returns>
            Task StartAsync();

            /// <summary>
            /// Stops the transport backend.
            /// </summary>
            /// <returns>After the transport backend has stopped.</returns>
            Task StopAsync();
        }

        /// <summary>
        /// The host functionality visible to the transport back-end. 
        /// The transport back-end calls this interface to place clients and partitions on this host.
        /// </summary>
        public interface IHost
        {
            /// <summary>
            /// Assigned by the transport backend to inform the host about the number of partitions.
            /// </summary>
            uint NumberPartitions { set; }

            /// <summary>
            /// Returns the storage provider for storing the partition states.
            /// </summary>
            StorageAbstraction.IStorageProvider StorageProvider { get; }

            /// <summary>
            /// Creates a client on this host.
            /// </summary>
            /// <param name="clientId">A globally unique identifier for this client</param>
            /// <param name="batchSender">A sender that can be used by the client for sending messages</param>
            /// <returns>A sender for passing messages to the transport backend</returns>
            IClient AddClient(Guid clientId, ISender batchSender);

            /// <summary>
            /// Places a partition on this host.
            /// </summary>
            /// <param name="partitionId">The partition id.</param>
            /// <param name="batchSender">A sender for passing messages to the transport backend</param>
            /// <returns></returns>
            IPartition AddPartition(uint partitionId, ISender batchSender);

            /// <summary>
            /// Returns an error handler object for the given partition.
            /// </summary>
            /// <param name="partitionId">The partition id.</param>
            IPartitionErrorHandler CreateErrorHandler(uint partitionId);
        }

        /// <summary>
        /// The partition functionality, as seen by the transport back-end.
        /// </summary>
        public interface IPartition
        {
            /// <summary>
            /// The partition id of this partition.
            /// </summary>
            uint PartitionId { get; }

            /// <summary>
            /// Acquire partition ownership, recover partition state from storage, and resume processing.
            /// </summary>
            /// <param name="termination">A termination object for initiating and/or detecting termination of the partition.</param>
            /// <param name="firstInputQueuePosition">For new partitions, the position of the first message to receive.</param>
            /// <returns>The input queue position of the next message to receive.</returns>
            /// <remarks>
            /// The termination token source can be used for immediately terminating the partition.
            /// Also, it can be used to detect that the partition has terminated for any other reason, 
            /// be it cleanly (after StopAsync) or uncleanly (after losing a lease or hitting a fatal error).
            /// </remarks>
            Task<long> CreateOrRestoreAsync(IPartitionErrorHandler termination, long firstInputQueuePosition);

            /// <summary>
            /// Clean shutdown: stop processing, save partition state to storage, and release ownership.
            /// </summary>
            /// <returns>When all steps have completed and termination is performed.</returns>
            Task StopAsync();

            /// <summary>
            /// Queues a single event for processing on this partition.
            /// </summary>
            /// <param name="partitionEvent">The event to process.</param>
            void SubmitInternalEvent(PartitionUpdateEvent partitionEvent);

            /// <summary>
            /// Queues a batch of incoming external events for processing on this partition.
            /// </summary>
            /// <param name="partitionEvents">The events to process.</param>
            void SubmitExternalEvents(IEnumerable<PartitionEvent> partitionEvents);

            /// <summary>
            /// The error handler for this partition.
            /// </summary>
            IPartitionErrorHandler ErrorHandler { get; }

            /// <summary>
            /// The elapsed time in milliseconds since this partition was constructed. We use this
            /// mainly for measuring various timings inside a partition.
            /// </summary>
            double CurrentTimeMs { get; }
        }

        /// <summary>
        /// The client functionality, as seen by the transport back-end.
        /// </summary>
        public interface IClient
        {
            /// <summary>
            /// A unique identifier for this client.
            /// </summary>
            Guid ClientId { get; }

            /// <summary>
            /// Processes a single event on this client.
            /// </summary>
            /// <param name="clientEvent">The event to process.</param>
            void Process(ClientEvent clientEvent);

            /// <summary>
            /// Stop processing events and shut down.
            /// </summary>
            /// <returns>When the client is shut down.</returns>
            Task StopAsync();

            /// <summary>
            /// Indicates an observed error for diagnostic purposes.
            /// </summary>
            /// <param name="msg">A message describing the circumstances.</param>
            /// <param name="e">The exception that was observed.</param>
            void ReportTransportError(string msg, Exception e);
        }

        /// <summary>
        /// A sender abstraction, passed to clients and partitions, for sending messages via the transport.
        /// </summary>
        public interface ISender
        {
            /// <summary>
            /// Send an event. The destination is already determined by the event,
            /// which contains either a client id or a partition id.
            /// </summary>
            /// <param name="element"></param>
            void Submit(Event element);
        }

        /// <summary>
        /// A listener abstraction, used by clients and partitions, to receive acks after events have been
        /// durably processed.
        /// </summary>
        public interface IDurabilityListener
        {
            /// <summary>
            /// Indicates that this event has been durably persisted (incoming events) or sent (outgoing events).
            /// </summary>
            /// <param name="evt">The event that has been durably processed.</param>
            void ConfirmDurable(Event evt);
        }

        /// <summary>
        /// An <see cref="IDurabilityListener"/> that is also listening for exceptions. Used on the client
        /// to make transport errors visible to the calling code.
        /// </summary>
        public interface IDurabilityOrExceptionListener : IDurabilityListener
        {
            /// <summary>
            /// Indicates that there was an error while trying to send this event.
            /// </summary>
            /// <param name="evt"></param>
            /// <param name="e"></param>
            void ReportException(Event evt, Exception e);
        }
    }
}