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

using DurableTask.Core;
using DurableTask.Core.Common;
using DurableTask.Core.History;
using DurableTask.EventSourced.Faster;
using Microsoft.Azure.Storage;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventSourced
{
    /// <summary>
    /// Local partition of the distributed orchestration service.
    /// </summary>
    public class EventSourcedOrchestrationService : 
        IOrchestrationService, 
        IOrchestrationServiceClient, 
        TransportAbstraction.IHost,
        StorageAbstraction.IStorageProvider,
        IDisposable
    {
        private readonly TransportAbstraction.ITaskHub taskHub;

        private const string LoggerName = "E1";

        private CancellationTokenSource serviceShutdownSource;

        //internal Dictionary<uint, Partition> Partitions { get; private set; }
        internal Client Client { get; private set; }

        internal LoadMonitorAbstraction.ILoadMonitorService LoadMonitorService { get; private set; }

        internal EventSourcedOrchestrationServiceSettings Settings { get; private set; }
        internal uint NumberPartitions { get; private set; }
        uint TransportAbstraction.IHost.NumberPartitions { set => this.NumberPartitions = value; }
        internal string StorageAccountName { get; private set; }

        internal WorkItemQueue<TaskActivityWorkItem> ActivityWorkItemQueue { get; private set; }
        internal WorkItemQueue<TaskOrchestrationWorkItem> OrchestrationWorkItemQueue { get; private set; }
        internal LoadPublisher LoadPublisher { get; private set; }

        internal Guid ServiceInstanceId { get; } = Guid.NewGuid();
        internal ILogger Logger { get; }
        internal ILoggerFactory LoggerFactory { get; }

        /// <inheritdoc/>
        public override string ToString()
        {
            return $"EventSourcedOrchestrationService on {this.Settings.TransportComponent}Transport and {this.Settings.StorageComponent}Storage";
        }

        /// <summary>
        /// Creates a new instance of the OrchestrationService with default settings
        /// </summary>
        public EventSourcedOrchestrationService(EventSourcedOrchestrationServiceSettings settings, ILoggerFactory loggerFactory)
        {
            this.Settings = settings;
            this.Logger = loggerFactory.CreateLogger(LoggerName);
            this.LoggerFactory = loggerFactory;
            this.StorageAccountName = CloudStorageAccount.Parse(this.Settings.StorageConnectionString).Credentials.AccountName;

            EtwSource.Log.OrchestrationServiceCreated(this.ServiceInstanceId, this.StorageAccountName, this.Settings.TaskHubName, this.Settings.WorkerId, TraceUtils.ExtensionVersion);
            this.Logger.LogInformation("EventSourcedOrchestrationService created, serviceInstanceId={serviceInstanceId}", this.ServiceInstanceId);

            switch (this.Settings.TransportComponent)
            {
                case EventSourcedOrchestrationServiceSettings.TransportChoices.Memory:
                    this.taskHub = new Emulated.MemoryTransport(this, settings, loggerFactory);
                    break;

                case EventSourcedOrchestrationServiceSettings.TransportChoices.EventHubs:
                    this.taskHub = new EventHubs.EventHubsTransport(this, settings, loggerFactory);
                    break;

                case EventSourcedOrchestrationServiceSettings.TransportChoices.AzureChannels:
                    this.taskHub = new AzureChannels.AzureChannelsTransport(this, settings, loggerFactory);
                    break;

                default:
                    throw new NotImplementedException("no such transport choice");
            }

            this.LoadMonitorService = new AzureLoadMonitorTable(settings.StorageConnectionString, settings.LoadInformationAzureTableName, settings.TaskHubName);
        }

        private async Task WorkitemExpirationCheck(CancellationToken token)
        {
            await Task.Delay(10, token);

            this.ActivityWorkItemQueue.CheckExpirations();
            this.OrchestrationWorkItemQueue.CheckExpirations();

            var ignoredTask = Task.Run(() => WorkitemExpirationCheck(token));
        }

        /******************************/
        // storage provider
        /******************************/

        StorageAbstraction.IPartitionState StorageAbstraction.IStorageProvider.CreatePartitionState()
        {
            switch (this.Settings.StorageComponent)
            {
                case EventSourcedOrchestrationServiceSettings.StorageChoices.Memory:
                    return new MemoryStorage();

                case EventSourcedOrchestrationServiceSettings.StorageChoices.Faster:
                    return new Faster.FasterStorage(this.Settings.StorageConnectionString, this.Settings.TaskHubName, this.LoggerFactory);

                default:
                    throw new NotImplementedException("no such storage choice");
            }
        }

        async Task StorageAbstraction.IStorageProvider.DeleteAllPartitionStatesAsync()
        {
            await this.LoadMonitorService.DeleteIfExistsAsync(CancellationToken.None);

            switch (this.Settings.StorageComponent)
            {
                case EventSourcedOrchestrationServiceSettings.StorageChoices.Memory:
                    await Task.Delay(10);
                    break;

                case EventSourcedOrchestrationServiceSettings.StorageChoices.Faster:
                    await Faster.FasterStorage.DeleteTaskhubStorageAsync(Settings.StorageConnectionString, this.Settings.TaskHubName);
                    break;

                default:
                    throw new NotImplementedException("no such storage choice");
            }
        }

        /******************************/
        // management methods
        /******************************/

        /// <inheritdoc />
        public async Task CreateAsync() => await ((IOrchestrationService)this).CreateAsync(true);

        /// <inheritdoc />
        public async Task CreateAsync(bool recreateInstanceStore)
        {
            if (await this.taskHub.ExistsAsync())
            {
                if (recreateInstanceStore)
                {
                    await this.taskHub.DeleteAsync();
                    await this.taskHub.CreateAsync();
                }
            }
            else
            {
                await this.taskHub.CreateAsync();
            }

            await this.LoadMonitorService.CreateIfNotExistsAsync(CancellationToken.None);
        }

        /// <inheritdoc />
        public async Task CreateIfNotExistsAsync() => await ((IOrchestrationService)this).CreateAsync(false);

        /// <inheritdoc />
        public async Task DeleteAsync()
        {
            await this.taskHub.DeleteAsync();

            await this.LoadMonitorService.DeleteIfExistsAsync(CancellationToken.None);
        }

        /// <inheritdoc />
        public async Task DeleteAsync(bool deleteInstanceStore) => await this.DeleteAsync();

        /// <inheritdoc />
        public async Task StartAsync()
        {
            if (this.serviceShutdownSource != null)
            {
                // we left the service running. No need to start it again.
                return;
            }

            this.Logger.LogInformation("EventSourcedOrchestrationService is starting, serviceInstanceId={serviceInstanceId}", this.ServiceInstanceId);

            this.serviceShutdownSource = new CancellationTokenSource();

            this.ActivityWorkItemQueue = new WorkItemQueue<TaskActivityWorkItem>(this.serviceShutdownSource.Token, SendNullResponses);
            this.OrchestrationWorkItemQueue = new WorkItemQueue<TaskOrchestrationWorkItem>(this.serviceShutdownSource.Token, SendNullResponses);

            this.LoadPublisher = new LoadPublisher(this.LoadMonitorService);

            await taskHub.StartAsync();

            var ignoredTask = Task.Run(() => WorkitemExpirationCheck(this.serviceShutdownSource.Token));

            System.Diagnostics.Debug.Assert(this.Client != null, "Backend should have added client");
        }

        private static void SendNullResponses<T>(IEnumerable<CancellableCompletionSource<T>> waiters) where T : class
        {
            foreach (var waiter in waiters)
            {
                waiter.TrySetResult(null);
            }
        }

        /// <inheritdoc />
        public async Task StopAsync(bool isForced)
        {
            this.Logger.LogInformation("EventSourcedOrchestrationService stopping, serviceInstanceId={serviceInstanceId}", this.ServiceInstanceId);

            if (!this.Settings.KeepServiceRunning && this.serviceShutdownSource != null)
            {
                this.serviceShutdownSource.Cancel();
                this.serviceShutdownSource.Dispose();
                this.serviceShutdownSource = null;

                await this.taskHub.StopAsync();

                this.Logger.LogInformation("EventSourcedOrchestrationService stopped, serviceInstanceId={serviceInstanceId}", this.ServiceInstanceId);
                EtwSource.Log.OrchestrationServiceStopped(this.ServiceInstanceId, this.StorageAccountName, this.Settings.TaskHubName, this.Settings.WorkerId, TraceUtils.ExtensionVersion);
            }
        }

        /// <inheritdoc />
        public Task StopAsync() => ((IOrchestrationService)this).StopAsync(false);

        /// <inheritdoc/>
        public void Dispose() => this.taskHub.StopAsync();

        /// <summary>
        /// Computes the partition for the given instance.
        /// </summary>
        /// <param name="instanceId">The instance id.</param>
        /// <returns>The partition id.</returns>
        public uint GetPartitionId(string instanceId) => Fnv1aHashHelper.ComputeHash(instanceId) % this.NumberPartitions;

        private uint GetNumberPartitions() => this.NumberPartitions;

        /******************************/
        // host methods
        /******************************/

        TransportAbstraction.IClient TransportAbstraction.IHost.AddClient(Guid clientId, TransportAbstraction.ISender batchSender)
        {
            System.Diagnostics.Debug.Assert(this.Client == null, "Backend should create only 1 client");

            this.Client = new Client(this, clientId, batchSender, this.serviceShutdownSource.Token);
            return this.Client;
        }

        TransportAbstraction.IPartition TransportAbstraction.IHost.AddPartition(uint partitionId, TransportAbstraction.ISender batchSender)
        {
            var partition = new Partition(this, partitionId, this.GetPartitionId, this.GetNumberPartitions, batchSender, this.Settings, this.StorageAccountName,
                this.ActivityWorkItemQueue, this.OrchestrationWorkItemQueue, this.LoadPublisher);

            return partition;
        }

        StorageAbstraction.IStorageProvider TransportAbstraction.IHost.StorageProvider => this;

        IPartitionErrorHandler TransportAbstraction.IHost.CreateErrorHandler(uint partitionId)
        {
            return new PartitionErrorHandler((int) partitionId, this.Logger, this.Settings.EtwLevel, this.StorageAccountName, this.Settings.TaskHubName);
        }

        /******************************/
        // client methods
        /******************************/

        /// <inheritdoc />
        public Task CreateTaskOrchestrationAsync(TaskMessage creationMessage)
            => Client.CreateTaskOrchestrationAsync(
                this.GetPartitionId(creationMessage.OrchestrationInstance.InstanceId),
                creationMessage,
                null);

        /// <inheritdoc />
        public Task CreateTaskOrchestrationAsync(TaskMessage creationMessage, OrchestrationStatus[] dedupeStatuses)
            => Client.CreateTaskOrchestrationAsync(
                this.GetPartitionId(creationMessage.OrchestrationInstance.InstanceId),
                creationMessage,
                dedupeStatuses);

        /// <inheritdoc />
        public Task SendTaskOrchestrationMessageAsync(TaskMessage message)
            => Client.SendTaskOrchestrationMessageBatchAsync(
                this.GetPartitionId(message.OrchestrationInstance.InstanceId),
                new[] { message });

        /// <inheritdoc />
        public Task SendTaskOrchestrationMessageBatchAsync(params TaskMessage[] messages)
            => messages.Length == 0
                ? Task.CompletedTask
                : Task.WhenAll(messages
                    .GroupBy(tm => this.GetPartitionId(tm.OrchestrationInstance.InstanceId))
                    .Select(group => Client.SendTaskOrchestrationMessageBatchAsync(group.Key, group)));

        /// <inheritdoc />
        public Task<OrchestrationState> WaitForOrchestrationAsync(
                string instanceId,
                string executionId,
                TimeSpan timeout,
                CancellationToken cancellationToken) 
            => Client.WaitForOrchestrationAsync(
                this.GetPartitionId(instanceId),
                instanceId,
                executionId,
                timeout,
                cancellationToken);

        /// <inheritdoc />
        public async Task<OrchestrationState> GetOrchestrationStateAsync(
            string instanceId, 
            string executionId)
        {
            var state = await Client.GetOrchestrationStateAsync(this.GetPartitionId(instanceId), instanceId);
            return state != null && (executionId == null || executionId == state.OrchestrationInstance.ExecutionId)
                ? state
                : null;
        }

        /// <inheritdoc />
        public async Task<IList<OrchestrationState>> GetOrchestrationStateAsync(
            string instanceId, 
            bool allExecutions)
        {
            // TODO: allExecutions is ignored both here and AzureStorageOrchestrationService?
            var state = await Client.GetOrchestrationStateAsync(this.GetPartitionId(instanceId), instanceId);
            return state != null 
                ? (new[] { state }) 
                : (new OrchestrationState[0]);
        }

        /// <summary>
        /// Gets the state of all orchestration instances.
        /// </summary>
        /// <returns>List of <see cref="OrchestrationState"/></returns>
        public Task<IList<OrchestrationState>> GetOrchestrationStateAsync(CancellationToken cancellationToken) 
            => Client.GetOrchestrationStateAsync(cancellationToken);

        /// <summary>
        /// Gets the state of selected orchestration instances.
        /// </summary>
        /// <returns>List of <see cref="OrchestrationState"/></returns>
        public Task<IList<OrchestrationState>> GetOrchestrationStateAsync(DateTime? CreatedTimeFrom = default,
                                                                          DateTime? CreatedTimeTo = default,
                                                                          IEnumerable<OrchestrationStatus> RuntimeStatus = default,
                                                                          string InstanceIdPrefix = default,
                                                                          CancellationToken CancellationToken = default)
            => Client.GetOrchestrationStateAsync(CreatedTimeFrom, CreatedTimeTo, RuntimeStatus, InstanceIdPrefix, CancellationToken);

        /// <inheritdoc />
        Task IOrchestrationServiceClient.ForceTerminateTaskOrchestrationAsync(
                string instanceId, 
                string message)
            => this.Client.ForceTerminateTaskOrchestrationAsync(this.GetPartitionId(instanceId), instanceId, message);

        /// <inheritdoc />
        public Task<string> GetOrchestrationHistoryAsync(
            string instanceId, 
            string executionId)
        {
            throw new NotSupportedException(); //TODO
        }

        /// <inheritdoc />
        public Task PurgeOrchestrationHistoryAsync(
            DateTime thresholdDateTimeUtc, 
            OrchestrationStateTimeRangeFilterType 
            timeRangeFilterType)
        {
            throw new NotSupportedException(); //TODO
        }

        /******************************/
        // Task orchestration methods
        /******************************/

        Task<TaskOrchestrationWorkItem> IOrchestrationService.LockNextTaskOrchestrationWorkItemAsync(
            TimeSpan receiveTimeout,
            CancellationToken cancellationToken)
        {
            return this.OrchestrationWorkItemQueue.GetNext(receiveTimeout, cancellationToken);
        }

        Task IOrchestrationService.CompleteTaskOrchestrationWorkItemAsync(
            TaskOrchestrationWorkItem workItem,
            OrchestrationRuntimeState newOrchestrationRuntimeState,
            IList<TaskMessage> outboundMessages,
            IList<TaskMessage> orchestratorMessages,
            IList<TaskMessage> timerMessages,
            TaskMessage continuedAsNewMessage,
            OrchestrationState state)
        {
            var orchestrationWorkItem = (OrchestrationWorkItem)workItem;
            var messageBatch = orchestrationWorkItem.MessageBatch;
            var partition = orchestrationWorkItem.Partition;

            List<TaskMessage> localMessages = null;
            List<TaskMessage> remoteMessages = null;

            // all continue as new requests are processed immediately ("fast" continue-as-new)
            // so by the time we get here, it is not a continue as new
            partition.Assert(continuedAsNewMessage == null);
 
            if (orchestratorMessages != null)
            {
                foreach (var taskMessage in orchestratorMessages)
                {
                    if (partition.PartitionId == partition.PartitionFunction(taskMessage.OrchestrationInstance.InstanceId))
                    {
                        if (Entities.IsDelayedEntityMessage(taskMessage, out _))
                        {
                            (timerMessages ?? (timerMessages = new List<TaskMessage>())).Add(taskMessage);
                        }
                        else
                        {
                            (localMessages ?? (localMessages = new List<TaskMessage>())).Add(taskMessage);
                        }
                    }
                    else
                    {
                        (remoteMessages ?? (remoteMessages = new List<TaskMessage>())).Add(taskMessage);
                    }
                }
            }

            try
            {
                partition.SubmitInternalEvent(new BatchProcessed()
                {
                    PartitionId = partition.PartitionId,
                    SessionId = messageBatch.SessionId,
                    InstanceId = workItem.InstanceId,
                    BatchStartPosition = messageBatch.BatchStartPosition,
                    BatchLength = messageBatch.BatchLength,
                    NewEvents = (List<HistoryEvent>)newOrchestrationRuntimeState.NewEvents,
                    WorkItem = orchestrationWorkItem,
                    State = state,
                    ActivityMessages = (List<TaskMessage>)outboundMessages,
                    LocalMessages = localMessages,
                    RemoteMessages = remoteMessages,
                    TimerMessages = (List<TaskMessage>)timerMessages,
                    Timestamp = DateTime.UtcNow,
                });
            }
            catch(OperationCanceledException e)
            {
                // we get here if the partition was terminated. The work is thrown away. It's unavoidable by design, but let's at least create a warning.
                partition.ErrorHandler.HandleError(nameof(IOrchestrationService.CompleteTaskOrchestrationWorkItemAsync), "Canceling completed orchestration work item because of partition termination", e, false, true);
            }

            return Task.CompletedTask;
        }

        Task IOrchestrationService.AbandonTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            // put it back into the work queue
            this.OrchestrationWorkItemQueue.Add(workItem);

            return Task.CompletedTask;
        }

        Task IOrchestrationService.ReleaseTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            return Task.CompletedTask;
        }

        Task IOrchestrationService.RenewTaskOrchestrationWorkItemLockAsync(TaskOrchestrationWorkItem workItem)
        {
            // no renewal required. Work items never time out.
            return Task.FromResult(workItem);
        }

        BehaviorOnContinueAsNew IOrchestrationService.EventBehaviourForContinueAsNew => BehaviorOnContinueAsNew.Carryover;

        bool IOrchestrationService.IsMaxMessageCountExceeded(int currentMessageCount, OrchestrationRuntimeState runtimeState)
        {
            return false;
        }

        int IOrchestrationService.GetDelayInSecondsAfterOnProcessException(Exception exception)
        {
            return 0;
        }

        int IOrchestrationService.GetDelayInSecondsAfterOnFetchException(Exception exception)
        {
            return 0;
        }

        int IOrchestrationService.MaxConcurrentTaskOrchestrationWorkItems => this.Settings.MaxConcurrentTaskOrchestrationWorkItems;

        int IOrchestrationService.TaskOrchestrationDispatcherCount => 1;


        /******************************/
        // Task activity methods
        /******************************/

        Task<TaskActivityWorkItem> IOrchestrationService.LockNextTaskActivityWorkItem(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            return this.ActivityWorkItemQueue.GetNext(receiveTimeout, cancellationToken);
        }

        Task IOrchestrationService.AbandonTaskActivityWorkItemAsync(TaskActivityWorkItem workItem)
        {
            // put it back into the work queue
            this.ActivityWorkItemQueue.Add(workItem);
            return Task.CompletedTask;
        }

        Task IOrchestrationService.CompleteTaskActivityWorkItemAsync(TaskActivityWorkItem workItem, TaskMessage responseMessage)
        {
            var activityWorkItem = (ActivityWorkItem)workItem;
            var partition = activityWorkItem.Partition;

            try
            {
                partition.SubmitInternalEvent(new ActivityCompleted()
                {
                    PartitionId = activityWorkItem.Partition.PartitionId,
                    ActivityId = activityWorkItem.ActivityId,
                    OriginPartitionId = activityWorkItem.OriginPartition,
                    ReportedLoad = this.ActivityWorkItemQueue.Load,
                    Timestamp = DateTime.UtcNow,
                    Response = responseMessage,
                });
            }
            catch (OperationCanceledException e)
            {
                // we get here if the partition was terminated. The work is thrown away. It's unavoidable by design, but let's at least create a warning.
                partition.ErrorHandler.HandleError(nameof(IOrchestrationService.CompleteTaskActivityWorkItemAsync), "Canceling completed activity work item because of partition termination", e, false, true);
            }

            return Task.CompletedTask;
        }
        
        Task<TaskActivityWorkItem> IOrchestrationService.RenewTaskActivityWorkItemLockAsync(TaskActivityWorkItem workItem)
        {
            // no renewal required. Work items never time out.
            return Task.FromResult(workItem);
        }

        int IOrchestrationService.MaxConcurrentTaskActivityWorkItems => this.Settings.MaxConcurrentTaskActivityWorkItems;

        int IOrchestrationService.TaskActivityDispatcherCount => 1;

    }
}