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
    using DurableTask.Core;
    using Microsoft.Azure.EventHubs;

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
        /// Determines the component to use for message delivery and partition load balancing.
        /// </summary>
        public TransportChoices TransportComponent
        {
            get
            {
                if (!string.IsNullOrEmpty(this.EventHubsConnectionString))
                {
                    if (this.UseMemoryTransport)
                    {
                        return TransportChoices.Memory;
                    }
                    else
                    {
                        return TransportChoices.EventHubs;
                    }
                }
                else
                {
                    return TransportChoices.AzureChannels;
                }
            }
        }

        /// <summary>
        /// Determines the component to use for storing the partition states.
        /// </summary>
        public StorageChoices StorageComponent
        {
            get
            {
                if (this.UseFasterWithMemoryTransport || !this.UseMemoryTransport)
                {
                    return StorageChoices.Faster;
                }
                else
                {
                    return StorageChoices.Memory;
                }
            }
        }

        /// <summary>
        /// Configuration options for the storage component
        /// </summary>
        public enum StorageChoices
        {
            /// <summary>
            /// Does not store any state to durable storage, just keeps it in memory. 
            /// Intended for testing scenarios.
            /// </summary>
            Memory = 0,

            /// <summary>
            /// Uses the Faster key-value store.
            /// </summary>
            Faster = 1,
        }

        /// <summary>
        /// Configuration options for the transport component
        /// </summary>
        public enum TransportChoices
        {
            /// <summary>
            /// Passes messages through memory and puts all partitions on a single host
            /// Intended for testing scenarios.
            /// </summary>
            Memory = 0,

            /// <summary>
            /// Passes messages through eventhubs; can distribute over multiple machines via
            /// the eventhubs EventProcessor.
            /// </summary>
            EventHubs = 1,

            /// <summary>
            /// Passes messages through azure tables; currently puts all partitions on a single host.
            /// </summary>
            AzureChannels = 2,
        }

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
        public ulong MaxNumberBytesBetweenCheckpoints { get; set; } = 200 * 1024 * 1024;

        /// <summary>
        /// A limit on how many events to append to the log before initiating another state checkpoint.
        /// </summary>
        public ulong MaxNumberEventsBetweenCheckpoints { get; set; } = 10 * 1000;


        /// <inheritdoc/>
        public override bool Equals(object obj)
        {
            if (!(obj is EventSourcedOrchestrationServiceSettings other))
                return false;

            return
                (this.EventHubsConnectionString,
                this.StorageConnectionString,
                this.TaskHubName,
                this.MaxConcurrentTaskActivityWorkItems,
                this.MaxConcurrentTaskOrchestrationWorkItems,
                this.KeepServiceRunning)
                ==
                (other.EventHubsConnectionString,
                other.StorageConnectionString,
                other.TaskHubName,
                other.MaxConcurrentTaskActivityWorkItems,
                other.MaxConcurrentTaskOrchestrationWorkItems,
                other.KeepServiceRunning);
        }

        /// <inheritdoc/>
        public override int GetHashCode()
        {
            return (this.EventHubsConnectionString,
                this.StorageConnectionString,
                this.TaskHubName,
                this.MaxConcurrentTaskActivityWorkItems,
                this.MaxConcurrentTaskOrchestrationWorkItems,
                this.KeepServiceRunning).GetHashCode();
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

            if (settings.UseMemoryTransport)
            {
                var numberPartitions = settings.MemoryPartitions;
                if (numberPartitions < 1 || numberPartitions > 32)
                {
                    throw new ArgumentOutOfRangeException(nameof(settings.EventHubsConnectionString));
                }
            }
            else
            {
                if (string.IsNullOrEmpty(settings.EventHubsNamespaceName))
                {
                    throw new FormatException(nameof(settings.EventHubsConnectionString));
                }

                if (string.IsNullOrEmpty(settings.StorageConnectionString))
                {
                    throw new ArgumentNullException(nameof(settings.StorageConnectionString));
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

        /// <summary>
        /// Uses in-memory simulation for transport and storage.
        /// </summary>
        public bool UseMemoryTransport => (this.EventHubsConnectionString.StartsWith("Memory"));

        /// <summary>
        /// Uses in-memory simulation with all messages being sent in emulator
        /// </summary>
        public bool UseFasterWithMemoryTransport => (this.EventHubsConnectionString.StartsWith("MemoryF"));

        /// <summary>
        /// Gets the number of partitions when using the memory emulation
        /// </summary>
        public uint MemoryPartitions => uint.Parse(this.EventHubsConnectionString.Substring((this.UseFasterWithMemoryTransport ? 7 : 6) + 1));

        /// <summary>
        /// Returns the name of the eventhubs namespace
        /// </summary>
        public string EventHubsNamespaceName
        {
            get
            {
                var builder = new EventHubsConnectionStringBuilder(this.EventHubsConnectionString);
                var host = builder.Endpoint.Host;
                return host.Substring(0, host.IndexOf('.'));
            }
        }
    }
}
