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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DurableTask.Core;
using DurableTask.Core.History;
using Microsoft.Extensions.Logging;

namespace DurableTask.EventSourced
{
    internal partial class Partition : TransportAbstraction.IPartition
    {
        private readonly EventSourcedOrchestrationService host;

        public uint PartitionId { get; private set; }
        public string TracePrefix { get; private set; }
        public Func<string, uint> PartitionFunction { get; private set; }
        public Func<uint> NumberPartitions { get; private set; }
        public IPartitionErrorHandler ErrorHandler { get; private set; }

        public EventSourcedOrchestrationServiceSettings Settings { get; private set; }
        public string StorageAccountName { get; private set; }

        public StorageAbstraction.IPartitionState State { get; private set; }
        public TransportAbstraction.ISender BatchSender { get; private set; }
        public WorkItemQueue<TaskActivityWorkItem> ActivityWorkItemQueue { get; private set; }
        public WorkItemQueue<TaskOrchestrationWorkItem> OrchestrationWorkItemQueue { get; private set; }

        public BatchTimer<PartitionEvent> PendingTimers { get; private set; }
        public PubSub<string, OrchestrationState> InstanceStatePubSub { get; private set; }
        public ConcurrentDictionary<long, ResponseWaiter> PendingResponses { get; private set; }

        public EventTraceHelper EventTraceHelper { get; }

        public bool RecoveryIsComplete { get; private set; }

        // A little helper property that allows us to conventiently check the condition for low-level event tracing
        public EventTraceHelper EventDetailTracer => this.EventTraceHelper.IsTracingDetails ? this.EventTraceHelper : null;

        public Partition(
            EventSourcedOrchestrationService host,
            uint partitionId,
            Func<string, uint> partitionFunction,
            Func<uint> numberPartitions,
            TransportAbstraction.ISender batchSender,
            EventSourcedOrchestrationServiceSettings settings,
            string storageAccountName,
            WorkItemQueue<TaskActivityWorkItem> activityWorkItemQueue,
            WorkItemQueue<TaskOrchestrationWorkItem> orchestrationWorkItemQueue)
        {
            this.host = host;
            this.PartitionId = partitionId;
            this.PartitionFunction = partitionFunction;
            this.NumberPartitions = numberPartitions;
            this.BatchSender = batchSender;
            this.Settings = settings;
            this.StorageAccountName = storageAccountName;
            this.ActivityWorkItemQueue = activityWorkItemQueue;
            this.OrchestrationWorkItemQueue = orchestrationWorkItemQueue;
            this.EventTraceHelper = new EventTraceHelper(host.Logger, this);

            host.Logger.LogInformation("Part{partition:D2} Started", this.PartitionId);
            EtwSource.Log.PartitionStarted(this.StorageAccountName, this.Settings.TaskHubName, (int)this.PartitionId, TraceUtils.ExtensionVersion);
        }

        public async Task<long> CreateOrRestoreAsync(IPartitionErrorHandler errorHandler, long firstInputQueuePosition)
        {
            EventTraceHelper.ClearTraceContext();
            this.EventDetailTracer?.TraceDetail("starting partition");

            this.ErrorHandler = errorHandler;

            // create or restore partition state from last snapshot
            try
            {
                // create the state
                this.State = ((StorageAbstraction.IStorageProvider)this.host).CreatePartitionState();

                // initialize collections for pending work
                this.PendingTimers = new BatchTimer<PartitionEvent>(this.ErrorHandler.Token, this.TimersFired);
                this.InstanceStatePubSub = new PubSub<string, OrchestrationState>();
                this.PendingResponses = new ConcurrentDictionary<long, ResponseWaiter>();

                // goes to storage to create or restore the partition state
                var inputQueuePosition = await State.CreateOrRestoreAsync(this, this.ErrorHandler, firstInputQueuePosition);

                this.RecoveryIsComplete = true;

                this.PendingTimers.Start($"Timer{this.PartitionId:D2}");

                return inputQueuePosition;
            }
            catch (Exception e)
            {
                this.ErrorHandler.HandleError(nameof(CreateOrRestoreAsync), "Could not start partition", e, true, false);
                throw;
            }
        }
       
        [Conditional("DEBUG")]
        public void Assert(bool condition)
        {
            if (!condition)
            {
                if (System.Diagnostics.Debugger.IsAttached)
                {
                    System.Diagnostics.Debugger.Break();
                }

                var stacktrace = System.Environment.StackTrace;

                this.ErrorHandler.HandleError(stacktrace, "Assertion failed", null, false, false);
            }
        }

        public async Task StopAsync()
        {
            try
            {
                if (!this.ErrorHandler.IsTerminated)
                {
                    // for a clean shutdown we try to save some of the latest progress to storage and then release the lease
                    await this.State.CleanShutdown(this.Settings.TakeStateCheckpointWhenStoppingPartition);
                }
            }
            catch(OperationCanceledException) when (this.ErrorHandler.IsTerminated)
            {
                // o.k. during termination
            }
            catch (Exception e)
            {
                this.ErrorHandler.HandleError(nameof(StopAsync), "Could not shut down partition state cleanly", e, true, false);
            }

            // at this point, the partition has been terminated (either cleanly or by exception)
            this.Assert(this.ErrorHandler.IsTerminated);

            host.Logger.LogInformation("Part{partition:D2} Stopped", this.PartitionId);
            EtwSource.Log.PartitionStopped(this.StorageAccountName, this.Settings.TaskHubName, (int) this.PartitionId, TraceUtils.ExtensionVersion);
        }

        private void TimersFired(List<PartitionEvent> timersFired)
        {
            try
            {
                foreach (var t in timersFired)
                {
                    this.Submit(t);
                }
            }
            catch (OperationCanceledException) when (this.ErrorHandler.IsTerminated)
            {
                // o.k. during termination
            }
            catch (Exception e)
            {
                this.ErrorHandler.HandleError("TimersFired", "Encountered exception while firing partition timers", e, true, false);
            }
        }

        public class ResponseWaiter : CancellableCompletionSource<ClientEvent>
        {
            protected readonly ClientRequestEvent Request;
            protected readonly Partition Partition;

            public ResponseWaiter(CancellationToken token, ClientRequestEvent request, Partition partition) : base(token)
            {
                this.Request = request;
                this.Partition = partition;
                this.Partition.PendingResponses.TryAdd(Request.RequestId, this);
            }
            protected override void Cleanup()
            {
                this.Partition.PendingResponses.TryRemove(Request.RequestId, out var _);
                base.Cleanup();
            }
        }

        public void TrySendResponse(ClientRequestEvent request, ClientEvent response)
        {
            if (this.PendingResponses.TryGetValue(request.RequestId, out var waiter))
            {
                waiter.TrySetResult(response);
            }
        }

        public void Send(Event evt)
        {
            // trace TaskMessages that are sent to other participants
            if (this.RecoveryIsComplete)
            {
                foreach (var taskMessage in evt.TracedTaskMessages)
                {
                    this.EventTraceHelper.TraceTaskMessageSent(taskMessage, evt.EventIdString);
                }
            }

            this.EventDetailTracer?.TraceSend(evt);
            this.BatchSender.Submit(evt);
        }

        public void Submit(PartitionEvent evt)
        {
            // trace TaskMessages that are "sent" in the sense that the partition sent it to itself
            if (this.RecoveryIsComplete)
            {
                foreach (var taskMessage in evt.TracedTaskMessages)
                {
                    this.EventTraceHelper.TraceTaskMessageSent(taskMessage, evt.EventIdString);
                }
            }

            this.State.SubmitEvent(evt);
        }

        public void SubmitInputEvents(IEnumerable<PartitionEvent> partitionEvents)
        {
            this.State.SubmitExternalEvents(partitionEvents);
        }

        public void EnqueueActivityWorkItem(ActivityWorkItem item)
        {
            this.EventDetailTracer?.TraceDetail($"Enqueueing ActivityWorkItem {item.WorkItemId}");
 
            this.ActivityWorkItemQueue.Add(item);
        }

 
        public void EnqueueOrchestrationWorkItem(OrchestrationWorkItem item)
        { 
            this.EventDetailTracer?.TraceDetail($"Enqueueing OrchestrationWorkItem batch={item.MessageBatch.CorrelationId}");

            this.OrchestrationWorkItemQueue.Add(item);
        }
    }
}
