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

namespace DurableTask.EventHubs
{
    using DurableTask.Core;
    using DurableTask.Core.Common;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.History;
    using Newtonsoft.Json;
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Local partition of the distributed orchestration service.
    /// </summary>
    public class LocalOrchestrationService : IOrchestrationService, IOrchestrationServiceClient, IDisposable
    {
        // back-end
        internal ITaskHub TaskHub { get; private set; }
        internal IPartitionedQueue Queue { get; private set; }
        internal IPartitionState State { get; private set; }

        // tracking for active work items, timers, and status requests

        internal WorkQueue<TaskActivityWorkItem> ActivityWorkItemQueue { get; private set; }
        internal WorkQueue<TaskOrchestrationWorkItem> OrchestrationWorkItemQueue { get; private set; }
        internal BatchTimer<TimerFired> PendingTimers { get; private set; }
        internal BatchWorker BatchSender { get; private set; }

        internal long ReceivePosition { get; private set; }
        internal long SendPosition { get; private set; }

        private PubSub<string, OrchestrationState> InstanceStatePubSub { get; set; }
        private BatchTimer<CancellablePromise<OrchestrationState>> ResponseTimeouts { get; set; }

        readonly int MaxConcurrentWorkItems = 20;

        private CancellationTokenSource shutdownTokenSource;

        private Task receiveLoopTask;

        /// <summary>
        ///     Creates a new instance of the OrchestrationService with default settings
        /// </summary>
        public LocalOrchestrationService()
        {
            this.TaskHub = new MemoryTaskHub();
        }

        private async Task ReceiveLoopAsync()
        {
            while (!this.shutdownTokenSource.IsCancellationRequested)
            {
                var batch = await Queue.ReceiveBatchAsync(this.ReceivePosition);

                for (int i = 0; i < batch.Count; i++)
                {
                    var next = batch[i];

                    next.QueuePosition = this.ReceivePosition + i;

                    await this.State.UpdateAsync(next);

                    if (next is BatchProcessed batchProcessed)
                    {
                        InstanceStatePubSub.Notify(batchProcessed.InstanceId, batchProcessed.State);
                    }
                }

                this.ReceivePosition += batch.Count;
            }
        }

        private async Task SendBatch()
        {
            var batch = await this.State.ReadAsync(this.State.Outbox.TryGetBatch, this.SendPosition);
            
            if (batch != null)
            {
                await this.Queue.SendBatchAsync(batch.Value.Messages);

                this.SendPosition = batch.Value.LastQueuePosition + 1;
            }
        }

        /******************************/
        // management methods
        /******************************/
        /// <inheritdoc />
        public Task CreateAsync()
        {
            return CreateAsync(true);
        }

        /// <inheritdoc />
        public async Task CreateAsync(bool recreateInstanceStore)
        {
            if (recreateInstanceStore)
            {
                if (await this.TaskHub.ExistsAsync())
                {
                    await this.TaskHub.DeleteAsync();
                }
                await this.TaskHub.CreateAsync();
            }
        }

        /// <inheritdoc />
        public async Task CreateIfNotExistsAsync()
        {
            if (!await this.TaskHub.ExistsAsync())
            {
                await this.TaskHub.CreateAsync();
            }
        }

        /// <inheritdoc />
        public Task DeleteAsync()
        {
            return DeleteAsync(true);
        }

        /// <inheritdoc />
        public async Task DeleteAsync(bool deleteInstanceStore)
        {
            await this.TaskHub.DeleteAsync();
        }

        /// <inheritdoc />
        public async Task StartAsync()
        {
            this.State = this.TaskHub.State;
            this.Queue = this.TaskHub.Queue;

            this.shutdownTokenSource = new CancellationTokenSource();
            var token = shutdownTokenSource.Token;

            // initialize collections for pending work
            this.ActivityWorkItemQueue = new WorkQueue<TaskActivityWorkItem>(token, SendNullResponses);
            this.OrchestrationWorkItemQueue = new WorkQueue<TaskOrchestrationWorkItem>(token, SendNullResponses);
            this.PendingTimers = new BatchTimer<TimerFired>(token, (timersFired) => this.Queue.SendBatchAsync(timersFired));
            this.InstanceStatePubSub = new PubSub<string, OrchestrationState>(token);
            this.ResponseTimeouts = new BatchTimer<CancellablePromise<OrchestrationState>>(token, SendNullResponses);
            this.BatchSender = new BatchWorker(SendBatch);

            // restore from last snapshot
            this.ReceivePosition = await State.Restore(this);
            this.SendPosition = await State.ReadAsync(State.Outbox.GetLastAckedQueuePosition);

            // run receive loop on thread pool
            this.receiveLoopTask = Task.Run(ReceiveLoopAsync);
        }

        private static Task SendNullResponses<T>(IEnumerable<CancellablePromise<T>> promises) where T : class
        {
            foreach (var promise in promises)
            {
                promise.TryFulfill(null);
            }
            return Task.FromResult<object>(null);
        }

        /// <inheritdoc />
        public Task StopAsync(bool isForced)
        {
            this.shutdownTokenSource.Cancel();
            return Task.FromResult<object>(null);
        }

        /// <inheritdoc />
        public Task StopAsync()
        {
            return StopAsync(false);
        }

        /// <inheritdoc />
        public void Dispose()
        {
            this.shutdownTokenSource.Cancel();
            this.shutdownTokenSource.Dispose();
        }

        /******************************/
        // client methods
        /******************************/

        /// <inheritdoc />
        public Task CreateTaskOrchestrationAsync(TaskMessage creationMessage)
        {
            return CreateTaskOrchestrationAsync(creationMessage, null);
        }

        /// <inheritdoc />
        public Task CreateTaskOrchestrationAsync(TaskMessage creationMessage, OrchestrationStatus[] dedupeStatuses)
        {
            return this.Queue.SendAsync(new OrchestrationCreationMessageReceived()
            {
                TaskMessage = creationMessage,
                DedupeStatuses = dedupeStatuses,
                Timestamp = DateTime.UtcNow,
            });
        }

        /// <inheritdoc />
        public Task SendTaskOrchestrationMessageAsync(TaskMessage message)
        {
            return SendTaskOrchestrationMessageBatchAsync(message);
        }

        /// <inheritdoc />
        public Task SendTaskOrchestrationMessageBatchAsync(params TaskMessage[] messages)
        {
            return this.Queue.SendBatchAsync(messages.Select(tm => new TaskMessageReceived() { TaskMessage = tm }));
        }

        /// <inheritdoc />
        public Task<OrchestrationState> WaitForOrchestrationAsync(
            string instanceId,
            string executionId,
            TimeSpan timeout,
            CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(instanceId))
            {
                throw new ArgumentException(nameof(instanceId));
            }
    
            var responsePromise = new StatusResponsePromise(cancellationToken);

            // response must time out if timeout is exceeded
            this.ResponseTimeouts.Schedule(DateTime.UtcNow + timeout, responsePromise);

            // listen to updates to the orchestration state
            responsePromise.Subscribe(this.InstanceStatePubSub, instanceId);

            // start an async read from state
            var ignoredTask = responsePromise.ReadFromStateAsync(this.State);
           
            return responsePromise.Task;
        }

        private class StatusResponsePromise : PubSub<string, OrchestrationState>.Listener
        {
            public StatusResponsePromise(CancellationToken token) : base(token) { }

            public override void Notify(OrchestrationState value)
            {
                if (value != null &&
                    value.OrchestrationStatus != OrchestrationStatus.Running &&
                    value.OrchestrationStatus != OrchestrationStatus.Pending &&
                    value.OrchestrationStatus != OrchestrationStatus.ContinuedAsNew)
                {
                    this.TryFulfill(value);
                }
            }

            public async Task ReadFromStateAsync(IPartitionState state)
            {
                var orchestrationState = await state.ReadAsync(state.GetInstance(this.Key).GetOrchestrationState);

                this.Notify(orchestrationState);
            }
        }

        /// <inheritdoc />
        public async Task<OrchestrationState> GetOrchestrationStateAsync(string instanceId, string executionId)
        {
            var state = await this.State.ReadAsync(State.GetInstance(instanceId).GetOrchestrationState);
            if (state != null && state.OrchestrationInstance.ExecutionId == executionId)
            {
                return state;
            }
            else
            {
                return null;
            }
        }

        /// <inheritdoc />
        public async Task<IList<OrchestrationState>> GetOrchestrationStateAsync(string instanceId, bool allExecutions)
        {
            var state = await this.State.ReadAsync(State.GetInstance(instanceId).GetOrchestrationState);
            if (state != null)
            {
                return new List<OrchestrationState>() { state };
            }
            else
            {
                return new List<OrchestrationState>();
            }
        }

        /// <inheritdoc />
        public Task<string> GetOrchestrationHistoryAsync(string instanceId, string executionId)
        {
            throw new NotSupportedException();
        }

        /// <inheritdoc />
        public Task PurgeOrchestrationHistoryAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType)
        {
            throw new NotSupportedException();
        }

        /******************************/
        // Task orchestration methods
        /******************************/

        /// <inheritdoc />
        public Task<TaskOrchestrationWorkItem> LockNextTaskOrchestrationWorkItemAsync(
            TimeSpan receiveTimeout,
            CancellationToken cancellationToken)
        {
            return this.OrchestrationWorkItemQueue.GetNext(receiveTimeout, cancellationToken);
        }

        /// <inheritdoc />
        public Task CompleteTaskOrchestrationWorkItemAsync(
            TaskOrchestrationWorkItem workItem,
            OrchestrationRuntimeState newOrchestrationRuntimeState,
            IList<TaskMessage> outboundMessages,
            IList<TaskMessage> orchestratorMessages,
            IList<TaskMessage> workItemTimerMessages,
            TaskMessage continuedAsNewMessage,
            OrchestrationState state)
        {
            var orchestrationWorkItem = (OrchestrationWorkItem)workItem;

            return this.Queue.SendAsync(new BatchProcessed()
            {
                SessionId = orchestrationWorkItem.SessionId,
                InstanceId = state.OrchestrationInstance.InstanceId,
                StartPosition = orchestrationWorkItem.StartPosition,
                Length = orchestrationWorkItem.NewMessages.Count,
                NewEvents = (List<HistoryEvent>)newOrchestrationRuntimeState.NewEvents,
                State = state,
                ActivityMessages = (List<TaskMessage>)outboundMessages,
                LocalOrchestratorMessages = orchestratorMessages.Where(tm => this.Queue.IsLocal(tm.OrchestrationInstance.InstanceId)).ToList(),
                RemoteOrchestratorMessages = orchestratorMessages.Where(tm => ! this.Queue.IsLocal(tm.OrchestrationInstance.InstanceId)).ToList(),
                TimerMessages = (List<TaskMessage>)workItemTimerMessages,
                Timestamp = DateTime.UtcNow,
            });
        }

        /// <inheritdoc />
        public Task AbandonTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            // put it back into the work queue
            this.OrchestrationWorkItemQueue.Add(workItem);
            return Task.FromResult<object>(null);
        }

        /// <inheritdoc />
        public Task ReleaseTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            return Task.FromResult<object>(null);
        }

        /// <inheritdoc />
        public async Task ForceTerminateTaskOrchestrationAsync(string instanceId, string message)
        {
            var taskMessage = new TaskMessage
            {
                OrchestrationInstance = new OrchestrationInstance { InstanceId = instanceId },
                Event = new ExecutionTerminatedEvent(-1, message)
            };

            await this.Queue.SendAsync(new TaskMessageReceived()
            {
                TaskMessage = taskMessage
            });
        }

        /// <inheritdoc />
        public Task RenewTaskOrchestrationWorkItemLockAsync(TaskOrchestrationWorkItem workItem)
        {
            // no renewal required. Work items never time out.
            return Task.FromResult(workItem);
        }

        /// <summary>
        ///  Should we carry over unexecuted raised events to the next iteration of an orchestration on ContinueAsNew
        /// </summary>
        public BehaviorOnContinueAsNew EventBehaviourForContinueAsNew => BehaviorOnContinueAsNew.Carryover;

        /// <inheritdoc />
        public bool IsMaxMessageCountExceeded(int currentMessageCount, OrchestrationRuntimeState runtimeState)
        {
            return false;
        }

        /// <inheritdoc />
        public int GetDelayInSecondsAfterOnProcessException(Exception exception)
        {
            return 0;
        }

        /// <inheritdoc />
        public int GetDelayInSecondsAfterOnFetchException(Exception exception)
        {
            return 0;
        }

        /// <inheritdoc />
        public int MaxConcurrentTaskOrchestrationWorkItems => this.MaxConcurrentWorkItems;

        /// <inheritdoc />
        public int TaskOrchestrationDispatcherCount => 1;

        /******************************/
        // Task activity methods
        /******************************/

        /// <inheritdoc />
        public Task<TaskActivityWorkItem> LockNextTaskActivityWorkItem(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            return this.ActivityWorkItemQueue.GetNext(receiveTimeout, cancellationToken);
        }

        /// <inheritdoc />
        public Task AbandonTaskActivityWorkItemAsync(TaskActivityWorkItem workItem)
        {
            // put it back into the work queue
            this.ActivityWorkItemQueue.Add(workItem);
            return Task.FromResult<object>(null);
        }

        /// <inheritdoc />
        public Task CompleteTaskActivityWorkItemAsync(TaskActivityWorkItem workItem, TaskMessage responseMessage)
        {
            var activityWorkItem = (ActivityWorkItem)workItem;

            return this.Queue.SendAsync(new ActivityCompleted()
            {
                ActivityId = activityWorkItem.ActivityId,
                Response = responseMessage,
            });
        }

        /// <inheritdoc />
        public Task<TaskActivityWorkItem> RenewTaskActivityWorkItemLockAsync(TaskActivityWorkItem workItem)
        {
            // no renewal required. Work items never time out.
            return Task.FromResult(workItem);
        }

        /// <inheritdoc />
        public int MaxConcurrentTaskActivityWorkItems => this.MaxConcurrentWorkItems;

        /// <inheritdoc />
        public int TaskActivityDispatcherCount => 1;
    }
}