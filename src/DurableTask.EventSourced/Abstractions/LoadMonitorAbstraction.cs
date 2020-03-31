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
    /// An abstraction of the external load monitor service which is used to collect load information for all the partitions
    /// </summary>
    public static class LoadMonitorAbstraction
    {
        /// <summary>
        /// An interface for the load monitor service.
        /// </summary>
        public interface ILoadMonitorService
        {
            /// <summary>
            /// Publish the load of a partition to the service.
            /// </summary>
            /// <param name="loadInfo">A collection of load information for partitions</param>
            /// <param name="cancellationToken">A cancellation token</param>
            /// <returns>A task indicating completion</returns>
            Task PublishAsync(Dictionary<uint, PartitionLoadInfo> loadInfo, CancellationToken cancellationToken);

            /// <summary>
            /// Delete all load information for a taskhub.
            /// </summary>
            /// <param name="cancellationToken">A cancellation token</param>
            /// <returns>A task indicating completion</returns>
            Task DeleteIfExistsAsync(CancellationToken cancellationToken);

            /// <summary>
            /// Prepare the service for a taskhub.
            /// </summary>
            /// <param name="cancellationToken">A cancellation token</param>
            /// <returns>A task indicating completion</returns>
            Task CreateIfNotExistsAsync(CancellationToken cancellationToken);

            /// <summary>
            /// Query all load information for a taskhub.
            /// </summary>
            /// <param name="cancellationToken">A cancellation token</param>
            /// <returns>A task returning a dictionary with load information for the partitions</returns>
            Task<Dictionary<uint, PartitionLoadInfo>> QueryAsync(CancellationToken cancellationToken);
        }

        /// <summary>
        /// Information about a specific partition
        /// </summary>
        public class PartitionLoadInfo
        {
            /// <summary>
            /// The number of orchestration work items waiting to be processed.
            /// </summary>
            public int WorkItems { get; set; }

            /// <summary>
            /// The number of activities that are waiting to be processed.
            /// </summary>
            public int Activities { get; set; }

            /// <summary>
            /// The number of timers that are waiting to fire.
            /// </summary>
            public int Timers { get; set; }

            /// <summary>
            /// The number of work items that have messages waiting to be sent.
            /// </summary>
            public int Outbox { get; set; }

            /// <summary>
            /// The next timer to fire.
            /// </summary>
            public DateTime? NextTimer { get; set; }

            /// <summary>
            /// The input queue position of this partition, which is  the next expected EventHubs sequence number.
            /// </summary>
            public long InputQueuePosition { get; set; }

            /// <summary>
            /// The commit log position of this partition.
            /// </summary>
            public long CommitLogPosition { get; set; }
        }
    }
}
