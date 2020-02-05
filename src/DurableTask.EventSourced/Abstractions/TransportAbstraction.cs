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
    /// <summary>
    /// Abstractions for defining a transport back-end functionality, including the 
    /// sending/receiving of messages, and the load balancing of partitions.
    /// The backend creates one client per connected host, and load-balances partitions over the 
    /// connected hosts.
    /// </summary>
    internal static class TransportAbstraction
    {
        /// <summary>
        /// The transport back-end taskhub functionality.
        /// </summary>
        public interface ITaskHub
        {
            Task<bool> ExistsAsync();

            Task CreateAsync();

            Task DeleteAsync();

            Task StartAsync();

            Task StopAsync();
        }

        /// <summary>
        /// The host functionality, as seen by the transport back-end.
        /// </summary>
        public interface IHost
        {
            uint NumberPartitions { set; }

            StorageAbstraction.IPartitionState CreatePartitionState();

            IClient AddClient(Guid clientId, ISender batchSender);

            IPartition AddPartition(uint partitionId, StorageAbstraction.IPartitionState state, ISender batchSender);

            void ReportError(string msg, Exception e);
        }

        /// <summary>
        /// The partition functionality, as seen by the transport back-end.
        /// </summary>
        public interface IPartition
        {
            uint PartitionId { get; }

            Task<ulong> StartAsync(CancellationToken token);

            void Submit(PartitionEvent partitionEvent);

            void SubmitRange(IEnumerable<PartitionEvent> partitionEvent);

            void ReportError(string msg, Exception e);

            Task StopAsync();
        }

        /// <summary>
        /// The client functionality, as seen by the transport back-end.
        /// </summary>
        public interface IClient
        {
            Guid ClientId { get; }

            void Process(ClientEvent clientEvent);

            void ReportError(string msg, Exception e);

            Task StopAsync();
        }

        /// <summary>
        /// A sender abstraction, passed to clients and partitions, for sending messages
        /// </summary>
        public interface ISender
        {
            void Submit(Event element);
        }

        /// <summary>
        /// A listener abstraction, used by clients and partitions, to receive acks after events have been
        /// durably processed.
        /// </summary>
        public interface IAckListener
        {
            void Acknowledge(Event evt);
        }

        /// <summary>
        /// An <see cref="IAckListener"/> that is also listening for exceptions.
        /// </summary>
        public interface IAckOrExceptionListener : IAckListener
        {
            void ReportException(Event evt, Exception e);
        }
    }
}