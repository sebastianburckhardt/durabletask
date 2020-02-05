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
using System.Runtime.Serialization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventSourced
{
    /// <summary>
    /// Abstractions for the storage, that allow different providers to be used.
    /// </summary>
    internal static class StorageAbstraction
    {
        /// <summary>
        /// Storage abstraction for the event-sourced state of a partition
        /// </summary>
        internal interface IPartitionState
        {
            Task<ulong> CreateOrRestoreAsync(Partition localPartition, CancellationToken token);

            Task PersistAndShutdownAsync(); // clean shutdown

            void Submit(PartitionEvent evt);

            void SubmitRange(IEnumerable<PartitionEvent> evt);

            void ScheduleRead(IReadContinuation readContinuation);

            CancellationToken OwnershipCancellationToken { get; }
        }

        public interface IReadContinuation
        {
            TrackedObjectKey ReadTarget { get; }

            void OnReadComplete(TrackedObject target);
        }
    }
}