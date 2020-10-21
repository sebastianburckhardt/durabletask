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
using DurableTask.Core;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace DurableTask.EventSourced
{
    /// <summary>
    /// Settings for the <see cref="EventSourcedOrchestrationService"/> class.
    /// </summary>
    [JsonObject]
    public class EventSourcedOrchestrationServiceSettings
    {
        /// <summary>
        /// Gets or sets the connection string for the event hubs namespace, if needed.
        /// </summary>
        public string EventHubsConnectionString { get; set; }

        /// <summary>
        /// Gets or sets the EventProcessor management
        /// </summary>
        public string EventProcessorManagement { get; set; } = "EventHubs";

        /// <summary>
        /// Gets or sets the connection string for the Azure storage account, supporting all types of blobs, and table storage.
        /// </summary>
        public string StorageConnectionString { get; set; }

        /// <summary>
        /// Gets or sets the connection string for a premium Azure storage account supporting page blobs only.
        /// </summary>
        public string PremiumStorageConnectionString { get; set; }

        [JsonIgnore]
        internal bool UsePremiumStorage => !string.IsNullOrEmpty(PremiumStorageConnectionString);

        /// <summary>
        /// The name of the taskhub. Matches Microsoft.Azure.WebJobs.Extensions.DurableTask.
        /// </summary>
        public string HubName { get; set; }

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
        /// Matches Microsoft.Azure.WebJobs.Extensions.DurableTask.
        /// </summary>
        public int MaxConcurrentActivityFunctions { get; set; } = 100;

        /// <summary>
        /// Gets or sets the maximum number of orchestrations that can be processed concurrently on a single node.
        /// The default value is 100.
        /// Matches Microsoft.Azure.WebJobs.Extensions.DurableTask.
        /// </summary>
        public int MaxConcurrentOrchestratorFunctions { get; set; } = 100;

        /// <summary>
        /// Gets or sets the number of dispatchers used to dispatch orchestrations.
        /// </summary>
        public int OrchestrationDispatcherCount { get; set; } = 1;

        /// <summary>
        /// Gets or sets the number of dispatchers used to dispatch activities.
        /// </summary>
        public int ActivityDispatcherCount { get; set; } = 1;

        /// <summary>
        /// Gets or sets a flag indicating whether to enable caching of execution cursors to avoid replay.
        /// Matches Microsoft.Azure.WebJobs.Extensions.DurableTask.
        /// </summary>
        public bool ExtendedSessionsEnabled { get; set; } = true;

        /// <summary>
        /// Whether we should carry over unexecuted raised events to the next iteration of an orchestration on ContinueAsNew.
        /// </summary>
        public BehaviorOnContinueAsNew EventBehaviourForContinueAsNew { get; set; } = BehaviorOnContinueAsNew.Carryover;

        /// <summary>
        /// When true, will throw an exception when attempting to create an orchestration with an existing dedupe status.
        /// </summary>
        public bool ThrowExceptionOnInvalidDedupeStatus { get; set; } = false;

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
        /// A limit on how many bytes to append to the log before initiating a state checkpoint. The default is 20MB.
        /// </summary>
        public long MaxNumberBytesBetweenCheckpoints { get; set; } = 20 * 1024 * 1024;

        /// <summary>
        /// A limit on how many events to append to the log before initiating a state checkpoint. The default is 10000.
        /// </summary>
        public long MaxNumberEventsBetweenCheckpoints { get; set; } = 10 * 1000;

        /// <summary>
        /// A limit on how long to wait between state checkpoints, in milliseconds. The default is 60s.
        /// </summary>
        public long MaxTimeMsBetweenCheckpoints { get; set; } = 60 * 1000;

        /// <summary>
        /// Whether to use the Faster PSF support for handling queries.
        /// </summary>
        public bool UsePSFQueries { get; set; } = true;

        /// <summary>
        /// Whether to use the alternate object store implementation.
        /// </summary>
        public bool UseAlternateObjectStore { get; set; } = false;

        /// <summary>
        /// Forces steps to pe persisted before applying their effects, thus disabling all speculation.
        /// </summary>
        public bool PersistStepsFirst { get; set; } = false;

        /// <summary>
        /// Whether to use JSON serialization for eventhubs packets.
        /// </summary>
        [JsonConverter(typeof(StringEnumConverter))]
        public JsonPacketUse UseJsonPackets { get; set; } = JsonPacketUse.Never;

        /// <summary>
        /// Which packets to send in JSON format.
        /// </summary>
        public enum JsonPacketUse
        {
            /// <summary>
            /// Never send packets in JSON format
            /// </summary>
            Never,

            /// <summary>
            /// Send packets to clients in JSON format
            /// </summary>
            ForClients,

            /// <summary>
            /// Send all packets in JSON format
            /// </summary>
            ForAll,
        }

        /// <summary>
        /// A lower limit on the severity level of trace events emitted by the transport layer.
        /// </summary>
        /// <remarks>This level applies to both ETW events and ILogger events.</remarks>
        [JsonConverter(typeof(StringEnumConverter))]
        public LogLevel TransportLogLevelLimit { get; set; } = LogLevel.Information;

        /// <summary>
        /// A lower limit on the severity level of trace events emitted by the storage layer.
        /// </summary>
        /// <remarks>This level applies to both ETW events and ILogger events.</remarks>
        [JsonConverter(typeof(StringEnumConverter))]
        public LogLevel StorageLogLevelLimit { get; set; } = LogLevel.Information;

        /// <summary>
        /// A lower limit on the severity level of event processor trace events emitted.
        /// </summary>
        /// <remarks>This level applies to both ETW events and ILogger events.</remarks>
        [JsonConverter(typeof(StringEnumConverter))]
        public LogLevel EventLogLevelLimit { get; set; } = LogLevel.Warning;

        /// <summary>
        /// A lower limit on the severity level of all other trace events emitted.
        /// </summary>
        /// <remarks>This level applies to both ETW events and ILogger events.</remarks>
        [JsonConverter(typeof(StringEnumConverter))]
        public LogLevel LogLevelLimit { get; set; } = LogLevel.Information;

        /// <inheritdoc/>
        public override bool Equals(object obj)
        {
            if (!(obj is EventSourcedOrchestrationServiceSettings other))
                return false;

            return
                (this.EventHubsConnectionString,
                this.StorageConnectionString,
                this.EventProcessorManagement,
                this.HubName,
                this.WorkerId,
                this.LoadInformationAzureTableName,
                this.MaxConcurrentActivityFunctions,
                this.MaxConcurrentOrchestratorFunctions,
                this.ExtendedSessionsEnabled,
                this.KeepServiceRunning,
                this.TakeStateCheckpointWhenStoppingPartition,
                this.MaxNumberBytesBetweenCheckpoints,
                this.MaxNumberEventsBetweenCheckpoints,
                this.UsePSFQueries,
                this.UseAlternateObjectStore,
                this.UseJsonPackets,
                this.TransportLogLevelLimit,
                this.StorageLogLevelLimit,
                this.LogLevelLimit)
                ==
                (other.EventHubsConnectionString,
                other.StorageConnectionString,
                other.EventProcessorManagement,
                other.HubName,
                other.WorkerId,
                other.LoadInformationAzureTableName,
                other.MaxConcurrentActivityFunctions,
                other.MaxConcurrentOrchestratorFunctions,
                other.ExtendedSessionsEnabled,
                other.KeepServiceRunning,
                other.TakeStateCheckpointWhenStoppingPartition,
                other.MaxNumberBytesBetweenCheckpoints,
                other.MaxNumberEventsBetweenCheckpoints,
                other.UsePSFQueries,
                other.UseAlternateObjectStore,
                other.UseJsonPackets,
                other.TransportLogLevelLimit,
                other.StorageLogLevelLimit,
                other.LogLevelLimit);
        }

        /// <inheritdoc/>
        public override int GetHashCode()
        {
            return (this.EventHubsConnectionString,
                this.StorageConnectionString,
                this.EventProcessorManagement,
                this.HubName,
                this.WorkerId,
                this.LoadInformationAzureTableName,
                this.MaxConcurrentActivityFunctions,
                this.MaxConcurrentOrchestratorFunctions,
                this.ExtendedSessionsEnabled,
                this.KeepServiceRunning,
                this.TakeStateCheckpointWhenStoppingPartition,
                this.MaxNumberBytesBetweenCheckpoints,
                this.MaxNumberEventsBetweenCheckpoints,
                this.UsePSFQueries,
                this.UseAlternateObjectStore,
                this.UseJsonPackets,
                this.TransportLogLevelLimit,
                this.StorageLogLevelLimit,
                this.LogLevelLimit)
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

            if ((transport != TransportConnectionString.TransportChoices.EventHubs))
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

            if (settings.MaxConcurrentOrchestratorFunctions <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(settings.MaxConcurrentOrchestratorFunctions));
            }

            if (settings.MaxConcurrentActivityFunctions <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(settings.MaxConcurrentActivityFunctions));
            }

            return settings;
        }      
    }
}
