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

namespace DurableTask.EventSourced
{
    using System;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// Settings for the <see cref="EventSourcedOrchestrationService"/> class.
    /// </summary>
    public class EventSourcedOrchestrationServiceSettings
    {
        /// <summary>
        /// Gets or sets the connection string for the event hubs namespace, if needed.
        /// </summary>
        public string EventHubsConnectionString { get; set; }

        /// <summary>
        /// Gets or sets the connection string for the Azure storage account.
        /// </summary>
        public string StorageConnectionString { get; set; }

        /// <summary>
        /// The name of the taskhub
        /// </summary>
        public string TaskHubName { get; set; }

        /// <summary>
        /// Gets or sets the identifier for the current worker.
        /// </summary>
        public string WorkerId { get; set; } = Environment.MachineName;

        /// <summary>
        /// The name to use for the Azure table with the load information
        /// </summary>
        public string LoadInformationAzureTableName { get; set; } = "DurableTaskPartitions";

        /// <summary>
        /// Gets or sets the maximum number of work items that can be processed concurrently on a single node.
        /// The default value is 100.
        /// </summary>
        public int MaxConcurrentTaskActivityWorkItems { get; set; } = 100;

        /// <summary>
        /// Gets or sets the maximum number of orchestrations that can be processed concurrently on a single node.
        /// The default value is 100.
        /// </summary>
        public int MaxConcurrentTaskOrchestrationWorkItems { get; set; } = 100;

        /// <summary>
        ///  Whether to keep the orchestration service running even if stop is called.
        ///  This is useful in a testing scenario, due to the inordinate time spent when shutting down EventProcessorHost.
        /// </summary>
        public bool KeepServiceRunning { get; set; } = false;

        /// <summary>
        /// Whether to checkpoint the current state of a partition when it is stopped. This improves recovery time but
        /// lengthens shutdown time.
        /// </summary>
        public bool TakeStateCheckpointWhenStoppingPartition { get; set; } = true;

        /// <summary>
        /// A limit on how many bytes to append to the log before initiating another state checkpoint.
        /// </summary>
        public long MaxNumberBytesBetweenCheckpoints { get; set; } = 200 * 1024 * 1024;

        /// <summary>
        /// A limit on how many events to append to the log before initiating another state checkpoint.
        /// </summary>
        //public long MaxNumberEventsBetweenCheckpoints { get; set; } = 10 * 1000;
        public long MaxNumberEventsBetweenCheckpoints { get; set; } = 1;

        /// <summary>
        /// A lower limit on the level of ETW events emitted by the transport layer.
        /// </summary>
        /// <remarks>This level applies only to ETW; the ILogger level can be controlled independently.</remarks>
        public LogLevel TransportEtwLevel { get; set; } = LogLevel.Information;

        /// <summary>
        /// A lower limit on the level of ETW events emitted by the storage layer.
        /// </summary>
        /// <remarks>This level applies only to ETW; the ILogger level can be controlled independently.</remarks>
        public LogLevel StorageEtwLevel { get; set; } = LogLevel.Information;

        /// <summary>
        /// A lower limit on the level of ETW events emitted by partitions and clients.
        /// </summary>
        /// <remarks>This level applies only to ETW; the ILogger level can be controlled independently.</remarks>
        public LogLevel EtwLevel { get; set; } = LogLevel.Debug;

        /// <inheritdoc/>
        public override bool Equals(object obj)
        {
            if (!(obj is EventSourcedOrchestrationServiceSettings other))
                return false;

            return
                (this.EventHubsConnectionString,
                this.StorageConnectionString,
                this.TaskHubName,
                this.WorkerId,
                this.LoadInformationAzureTableName,
                this.MaxConcurrentTaskActivityWorkItems,
                this.MaxConcurrentTaskOrchestrationWorkItems,
                this.KeepServiceRunning,
                this.TakeStateCheckpointWhenStoppingPartition,
                this.MaxNumberBytesBetweenCheckpoints,
                this.MaxNumberEventsBetweenCheckpoints,
                this.TransportEtwLevel,
                this.StorageEtwLevel,
                this.EtwLevel)
                ==
                (other.EventHubsConnectionString,
                other.StorageConnectionString,
                other.TaskHubName,
                other.WorkerId,
                other.LoadInformationAzureTableName,
                other.MaxConcurrentTaskActivityWorkItems,
                other.MaxConcurrentTaskOrchestrationWorkItems,
                other.KeepServiceRunning,
                other.TakeStateCheckpointWhenStoppingPartition,
                other.MaxNumberBytesBetweenCheckpoints,
                other.MaxNumberEventsBetweenCheckpoints,
                other.TransportEtwLevel,
                other.StorageEtwLevel,
                other.EtwLevel);
        }

        /// <inheritdoc/>
        public override int GetHashCode()
        {
            return (this.EventHubsConnectionString,
                this.StorageConnectionString,
                this.TaskHubName,
                this.WorkerId,
                this.LoadInformationAzureTableName,
                this.MaxConcurrentTaskActivityWorkItems,
                this.MaxConcurrentTaskOrchestrationWorkItems,
                this.KeepServiceRunning,
                this.TakeStateCheckpointWhenStoppingPartition,
                this.MaxNumberBytesBetweenCheckpoints,
                this.MaxNumberEventsBetweenCheckpoints,
                this.TransportEtwLevel,
                this.StorageEtwLevel,
                this.EtwLevel)
                .GetHashCode();
        }

        /// <summary>
        /// Validates the specified <see cref="EventSourcedOrchestrationServiceSettings"/> object.
        /// </summary>
        /// <param name="settings">The <see cref="EventSourcedOrchestrationServiceSettings"/> object to validate.</param>
        /// <returns>Returns <paramref name="settings"/> if successfully validated.</returns>
        public static EventSourcedOrchestrationServiceSettings Validate(EventSourcedOrchestrationServiceSettings settings)
        {
            if (settings == null)
            {
                throw new ArgumentNullException(nameof(settings));
            }

            if (string.IsNullOrEmpty(settings.EventHubsConnectionString))
            {
                throw new ArgumentNullException(nameof(settings.EventHubsConnectionString));
            }

            TransportConnectionString.Parse(settings.EventHubsConnectionString, out var storage, out var transport, out int? numPartitions);

            if (storage != TransportConnectionString.StorageChoices.Memory || transport != TransportConnectionString.TransportChoices.Memory)
            {
                if (string.IsNullOrEmpty(settings.StorageConnectionString))
                {
                    throw new ArgumentNullException(nameof(settings.StorageConnectionString));
                }
            }

            if (transport != TransportConnectionString.TransportChoices.EventHubs)
            {
                if (numPartitions < 1 || numPartitions > 32)
                {
                    throw new ArgumentOutOfRangeException(nameof(settings.EventHubsConnectionString));
                }
            }
            else
            {
                if (string.IsNullOrEmpty(TransportConnectionString.EventHubsNamespaceName(settings.EventHubsConnectionString)))
                {
                    throw new FormatException(nameof(settings.EventHubsConnectionString));
                }
            }

            if (settings.MaxConcurrentTaskOrchestrationWorkItems <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(settings.MaxConcurrentTaskOrchestrationWorkItems));
            }

            if (settings.MaxConcurrentTaskActivityWorkItems <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(settings.MaxConcurrentTaskActivityWorkItems));
            }

            return settings;
        }      
    }
}
