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

namespace DurableTask.Emulator
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
    public class LocalPartition : IOrchestrationService, IOrchestrationServiceClient, IDisposable
    {
        private readonly IPartitionReceiver partitionReceiver;
        private readonly IPartitionSender partitionSender;

        private readonly CancellationTokenSource cancellationTokenSource;

        internal IState State { get; private set; }
        internal WorkQueue<TaskActivityWorkItem> ActivityWorkItemQueue { get; private set; }
        internal WorkQueue<TaskOrchestrationWorkItem> OrchestrationWorkItemQueue { get; private set; }
        internal BatchTimer<TimerFired> PendingTimers { get; private set; }

        readonly ConcurrentDictionary<string, TaskCompletionSource<OrchestrationState>> orchestrationWaiters;

        readonly int MaxConcurrentWorkItems = 20;

        /// <summary>
        ///     Creates a new instance of the OrchestrationService with default settings
        /// </summary>
        public LocalPartition()
        {
            this.cancellationTokenSource = new CancellationTokenSource();

            this.orchestrationWaiters = new ConcurrentDictionary<string, TaskCompletionSource<OrchestrationState>>();

            this.ActivityWorkItemQueue = new WorkQueue<TaskActivityWorkItem>(cancellationTokenSource.Token);
            this.OrchestrationWorkItemQueue = new WorkQueue<TaskOrchestrationWorkItem>(cancellationTokenSource.Token);
            this.PendingTimers = new BatchTimer<TimerFired>(this.cancellationTokenSource.Token, (timerFired) => this.partitionSender.Send(timerFired));

            this.State = new MemoryState();
            this.State.RestoreAsync(this);
        }

        internal bool Handles(TaskMessage message)
        {
            return true; // TODO implement multiple partitions
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
        public Task CreateAsync(bool recreateInstanceStore)
        {
            return Task.FromResult<object>(null);
        }

        /// <inheritdoc />
        public Task CreateIfNotExistsAsync()
        {
            return Task.FromResult<object>(null);
        }

        /// <inheritdoc />
        public Task DeleteAsync()
        {
            return DeleteAsync(true);
        }

        /// <inheritdoc />
        public Task DeleteAsync(bool deleteInstanceStore)
        {
            return Task.FromResult<object>(null);
        }

        /// <inheritdoc />
        public Task StartAsync()
        {
            return Task.FromResult<object>(null);
        }

        /// <inheritdoc />
        public Task StopAsync(bool isForced)
        {
            this.cancellationTokenSource.Cancel();
            return Task.FromResult<object>(null);
        }

        /// <inheritdoc />
        public Task StopAsync()
        {
            return StopAsync(false);
        }

        /// <summary>
        /// Determines whether is a transient or not.
        /// </summary>
        /// <param name="exception">The exception.</param>
        /// <returns>
        ///   <c>true</c> if is transient exception; otherwise, <c>false</c>.
        /// </returns>
        public bool IsTransientException(Exception exception)
        {
            return false;
        }

        /// <inheritdoc />
        public void Dispose()
        {
            this.cancellationTokenSource.Cancel();
            this.cancellationTokenSource.Dispose();
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
            partitionSender.Send(new OrchestrationCreationMessageReceived()
            {
                TaskMessage = creationMessage,
                DedupeStatuses = dedupeStatuses,
            });

            return Task.FromResult<object>(null);
        }

        /// <inheritdoc />
        public Task SendTaskOrchestrationMessageAsync(TaskMessage message)
        {
            return SendTaskOrchestrationMessageBatchAsync(message);
        }

        /// <inheritdoc />
        public Task SendTaskOrchestrationMessageBatchAsync(params TaskMessage[] messages)
        {
            //TODO
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public async Task<OrchestrationState> WaitForOrchestrationAsync(
            string instanceId,
            string executionId,
            TimeSpan timeout,
            CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(instanceId))
            {
                throw new ArgumentException(nameof(instanceId));
            }

            //TODO avoid polling

            TimeSpan statusPollingInterval = TimeSpan.FromSeconds(2);
            while (!cancellationToken.IsCancellationRequested && timeout > TimeSpan.Zero)
            {
                OrchestrationState state = await this.GetOrchestrationStateAsync(instanceId, executionId);
                if (state == null ||
                    state.OrchestrationStatus == OrchestrationStatus.Running ||
                    state.OrchestrationStatus == OrchestrationStatus.Pending ||
                    state.OrchestrationStatus == OrchestrationStatus.ContinuedAsNew)
                {
                    await Task.Delay(statusPollingInterval, cancellationToken);
                    timeout -= statusPollingInterval;
                }
                else
                {
                    return state;
                }
            }

            return null;
        }

        /// <inheritdoc />
        public Task<OrchestrationState> GetOrchestrationStateAsync(string instanceId, string executionId)
        {
            // TODO safeguard against data races
            var state = State.GetInstance(instanceId).OrchestrationState;
            if (state != null && state.OrchestrationInstance.ExecutionId == executionId)
            {
                return Task.FromResult(state);
            }
            else
            {
                return Task.FromResult<OrchestrationState>(null);
            }
        }

        /// <inheritdoc />
        public Task<IList<OrchestrationState>> GetOrchestrationStateAsync(string instanceId, bool allExecutions)
        { 
            // TODO safeguard against data races
            var state = State.GetInstance(instanceId).OrchestrationState;
            if (state != null)
            {
                return Task.FromResult<IList<OrchestrationState>>(new List<OrchestrationState>() { state });
            }
            else
            {
                return Task.FromResult<IList<OrchestrationState>>(new List<OrchestrationState>());
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

            return this.partitionSender.Send(new BatchProcessed()
            {
                SessionId = orchestrationWorkItem.SessionId,
                StartPosition = orchestrationWorkItem.StartPosition,
                Length = orchestrationWorkItem.NewMessages.Count,
                NewEvents = (List<HistoryEvent>)newOrchestrationRuntimeState.NewEvents,
                State = state,
                ActivityMessages = (List<TaskMessage>)outboundMessages,
                LocalOrchestratorMessages = (List<TaskMessage>)orchestratorMessages,
                RemoteOrchestratorMessages = null, // TODO
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
            // put it back into the work queue
            this.OrchestrationWorkItemQueue.Add(workItem);
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

            await this.partitionSender.Send(new TaskMessageReceived()
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

            return this.partitionSender.Send(new ActivityCompleted()
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