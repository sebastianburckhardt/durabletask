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

using DurableTask.Core;
using DurableTask.Core.History;
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
        BackendAbstraction.IHost,
        IDisposable
    {
        private readonly BackendAbstraction.ITaskHub taskHub;
        private readonly EventSourcedOrchestrationServiceSettings settings;
        private CancellationTokenSource serviceShutdownSource;
        private static readonly Task completedTask = Task.FromResult<object>(null);

        //internal Dictionary<uint, Partition> Partitions { get; private set; }
        internal Client Client { get; private set; }

        internal uint NumberPartitions { get; private set; }
        uint BackendAbstraction.IHost.NumberPartitions { set => this.NumberPartitions = value; }

        internal WorkQueue<TaskActivityWorkItem> ActivityWorkItemQueue { get; private set; }
        internal WorkQueue<TaskOrchestrationWorkItem> OrchestrationWorkItemQueue { get; private set; }

        /// <summary>
        /// Creates a new instance of the OrchestrationService with default settings
        /// </summary>
        public EventSourcedOrchestrationService(EventSourcedOrchestrationServiceSettings settings)
        {
            this.settings = settings;

            if (settings.UseEmulatedBackend)
            {
                this.taskHub = new Emulated.EmulatedBackend(this, settings);
                //this.taskHub = new AzureChannels.AzureChannelsBackend(this, settings);
            }
            else
            {
                this.taskHub = new EventHubs.EventHubsBackend(this, settings);
            }
        }

        internal Guid HostId { get; } = Guid.NewGuid();

        /// <inheritdoc/>
        public override string ToString()
        {
            return $"EventSourcedOrchestrationService on {(settings.UseEmulatedBackend ? "Emulator" : "EventHubs")}";
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
        }

        /// <inheritdoc />
        public async Task CreateIfNotExistsAsync() => await ((IOrchestrationService)this).CreateAsync(false);

        /// <inheritdoc />
        public async Task DeleteAsync() => await this.taskHub.DeleteAsync();

        /// <inheritdoc />
        public async Task DeleteAsync(bool deleteInstanceStore) => await this.taskHub.DeleteAsync();

        /// <inheritdoc />
        public async Task StartAsync()
        {
            if (this.serviceShutdownSource != null)
            {
                // we left the service running. No need to start it again.
                return;
            }

            EtwSource.Log.HostStarted(this.HostId, System.Environment.MachineName);

            this.serviceShutdownSource = new CancellationTokenSource();

            this.ActivityWorkItemQueue = new WorkQueue<TaskActivityWorkItem>(this.serviceShutdownSource.Token, SendNullResponses);
            this.OrchestrationWorkItemQueue = new WorkQueue<TaskOrchestrationWorkItem>(this.serviceShutdownSource.Token, SendNullResponses);

            await taskHub.StartAsync();

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
            if (!this.settings.KeepServiceRunning && this.serviceShutdownSource != null)
            {
                this.serviceShutdownSource.Cancel();
                this.serviceShutdownSource.Dispose();
                this.serviceShutdownSource = null;

                await this.taskHub.StopAsync();

                EtwSource.Log.HostStopped(this.HostId);
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

        /******************************/
        // host methods
        /******************************/

        BackendAbstraction.IClient BackendAbstraction.IHost.AddClient(Guid clientId, BackendAbstraction.ISender batchSender)
        {
            System.Diagnostics.Debug.Assert(this.Client == null, "Backend should create only 1 client");

            this.Client = new Client(this, clientId, batchSender, this.serviceShutdownSource.Token);

            EtwSource.Log.ClientStarted(clientId);

            return this.Client;
        }

        BackendAbstraction.IPartition BackendAbstraction.IHost.AddPartition(uint partitionId, StorageAbstraction.IPartitionState state, BackendAbstraction.ISender batchSender)
        {
            var partition = new Partition(this, partitionId, this.GetPartitionId, state, batchSender, this.settings, this.ActivityWorkItemQueue, this.OrchestrationWorkItemQueue, this.serviceShutdownSource.Token);

            EtwSource.Log.PartitionStarted(partitionId);

            return partition;
        }

        void BackendAbstraction.IHost.ReportError(string msg, Exception e)
            => System.Diagnostics.Trace.TraceError($"!!! {msg}: {e}");

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
                ? completedTask
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

            return state != null
                    && (executionId == null || executionId == state.OrchestrationInstance.ExecutionId)
                ? state
                : null;
        }

        /// <inheritdoc />
        public async Task<IList<OrchestrationState>> GetOrchestrationStateAsync(
            string instanceId, 
            bool allExecutions)
        {
            var partitionId = this.GetPartitionId(instanceId);
            var state = await Client.GetOrchestrationStateAsync(partitionId, instanceId);

            return state != null 
                ? (new[] { state }) 
                : (new OrchestrationState[0]);
        }

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
            IList<TaskMessage> workItemTimerMessages,
            TaskMessage continuedAsNewMessage,
            OrchestrationState state)
        {
            var orchestrationWorkItem = (OrchestrationWorkItem)workItem;
            var partition = orchestrationWorkItem.Partition;

            List<TaskMessage> localMessages = null;
            List<TaskMessage> remoteMessages = null;

            System.Diagnostics.Debug.Assert(continuedAsNewMessage == null); // current implementation is eager
 
            if (orchestratorMessages != null)
            {
                foreach (var taskMessage in orchestratorMessages)
                {
                    if (partition.PartitionId == partition.PartitionFunction(taskMessage.OrchestrationInstance.InstanceId))
                    {
                        (localMessages ?? (localMessages = new List<TaskMessage>())).Add(taskMessage);
                    }
                    else
                    {
                        (remoteMessages ?? (remoteMessages = new List<TaskMessage>())).Add(taskMessage);
                    }
                }
            }

            partition.Submit(new BatchProcessed()
            {
                PartitionId = orchestrationWorkItem.Partition.PartitionId,
                SessionId = orchestrationWorkItem.SessionId,
                InstanceId = workItem.InstanceId,
                BatchStartPosition = orchestrationWorkItem.BatchStartPosition,
                BatchLength = orchestrationWorkItem.BatchLength,
                NewEvents = (List<HistoryEvent>)newOrchestrationRuntimeState.NewEvents,
                InMemoryRuntimeState = newOrchestrationRuntimeState,
                State = state,
                ActivityMessages = (List<TaskMessage>)outboundMessages,
                LocalMessages = localMessages,
                RemoteMessages = remoteMessages,
                TimerMessages = (List<TaskMessage>)workItemTimerMessages,
                Timestamp = DateTime.UtcNow,
            });

            return completedTask;
        }

        Task IOrchestrationService.AbandonTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            // put it back into the work queue
            this.OrchestrationWorkItemQueue.Add(workItem);

            return completedTask;
        }

        Task IOrchestrationService.ReleaseTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            return completedTask;
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

        int IOrchestrationService.MaxConcurrentTaskOrchestrationWorkItems => settings.MaxConcurrentTaskOrchestrationWorkItems;

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
            return completedTask;
        }

        Task IOrchestrationService.CompleteTaskActivityWorkItemAsync(TaskActivityWorkItem workItem, TaskMessage responseMessage)
        {
            var activityWorkItem = (ActivityWorkItem)workItem;
            var partition = activityWorkItem.Partition;
            partition.Submit(new ActivityCompleted()
            {
                PartitionId = activityWorkItem.Partition.PartitionId,
                ActivityId = activityWorkItem.ActivityId,
                Response = responseMessage,
            });
            return completedTask;
        }
        
        Task<TaskActivityWorkItem> IOrchestrationService.RenewTaskActivityWorkItemLockAsync(TaskActivityWorkItem workItem)
        {
            // no renewal required. Work items never time out.
            return Task.FromResult(workItem);
        }

        int IOrchestrationService.MaxConcurrentTaskActivityWorkItems => settings.MaxConcurrentTaskActivityWorkItems;

        int IOrchestrationService.TaskActivityDispatcherCount => 1;
    }
}