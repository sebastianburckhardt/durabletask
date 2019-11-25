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
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DurableTask.Core;
using DurableTask.Core.History;

namespace DurableTask.EventSourced
{
    internal class Partition : Backend.IPartition
    {
        private readonly EventSourcedOrchestrationService host;

        public uint PartitionId { get; private set; }
        public Func<string, uint> PartitionFunction { get; private set; }

        public EventSourcedOrchestrationServiceSettings Settings { get; private set; }

        public Storage.IPartitionState State { get; private set; }
        public Backend.ISender BatchSender { get; private set; }
        public WorkQueue<TaskActivityWorkItem> ActivityWorkItemQueue { get; private set; }
        public WorkQueue<TaskOrchestrationWorkItem> OrchestrationWorkItemQueue { get; private set; }

        public CancellationToken PartitionShutdownToken => this.partitionShutdown.Token;

        public BatchTimer<PartitionEvent> PendingTimers { get; private set; }
        public PubSub<string, OrchestrationState> InstanceStatePubSub { get; private set; }
        public ConcurrentDictionary<long, ResponseWaiter> PendingResponses { get; private set; }

        private readonly CancellationTokenSource partitionShutdown;

        public AsyncLocal<string> TraceContext { get; private set; }

        public Partition(
            EventSourcedOrchestrationService host,
            uint partitionId,
            Func<string, uint> partitionFunction,
            Storage.IPartitionState state,
            Backend.ISender batchSender,
            EventSourcedOrchestrationServiceSettings settings,
            WorkQueue<TaskActivityWorkItem> activityWorkItemQueue,
            WorkQueue<TaskOrchestrationWorkItem> orchestrationWorkItemQueue,
            CancellationToken cancellationToken)
        {
            this.host = host;
            this.PartitionId = partitionId;
            this.PartitionFunction = partitionFunction;
            this.State = state;
            this.BatchSender = batchSender;
            this.Settings = settings;
            this.ActivityWorkItemQueue = activityWorkItemQueue;
            this.OrchestrationWorkItemQueue = orchestrationWorkItemQueue;

            this.partitionShutdown = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            this.TraceContext = new AsyncLocal<string>();
        }

        public async Task StartAsync()
        {
            // initialize collections for pending work
            this.PendingTimers = new BatchTimer<PartitionEvent>(this.PartitionShutdownToken, this.TimersFired);
            this.InstanceStatePubSub = new PubSub<string, OrchestrationState>();
            this.PendingResponses = new ConcurrentDictionary<long, ResponseWaiter>();

            // restore from last snapshot
            this.TraceContext.Value = "restore";
            await State.RestoreAsync(this);
            this.TraceContext.Value = null;

            this.PendingTimers.Start($"Timer{this.PartitionId:D2}");
        }

        public void ProcessAsync(PartitionEvent partitionEvent)
        {
            this.State.Submit(partitionEvent);
        }

        public async Task StopAsync()
        {
            this.partitionShutdown.Cancel();
            await this.State.WaitForTerminationAsync();

            EtwSource.Log.PartitionStopped(this.PartitionId);
        }

        private void TimersFired(List<PartitionEvent> timersFired)
        {
            this.TraceContext.Value = "TWorker";
            this.SubmitRange(timersFired);
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
            this.TraceSend(evt);
            this.BatchSender.Submit(evt);
        }

        public void Submit(PartitionEvent evt)
        {
            this.TraceSubmit(evt);
            this.State.Submit(evt);
        }

        public void SubmitRange(IEnumerable<PartitionEvent> partitionEvents)
        {
            foreach(var partitionEvent in partitionEvents)
            {
               this.TraceSubmit(partitionEvent);
            }
            this.State.SubmitRange(partitionEvents);
        }

        public void EnqueueActivityWorkItem(ActivityWorkItem item)
        {
            if (EtwSource.EmitDiagnosticsTrace)
            {
                this.DiagnosticsTrace($"create activity work item {item.WorkItemId}");
            }
            if (EtwSource.Log.IsVerboseEnabled)
            {
                EtwSource.Log.PartitionWorkItemEnqueued(this.PartitionId, this.TraceContext.Value ?? "", item.WorkItemId);
            }

            this.ActivityWorkItemQueue.Add(item);
        }

        public void EnqueueOrchestrationWorkItem(OrchestrationWorkItem item)
        {
            if (EtwSource.EmitDiagnosticsTrace)
            {
                this.DiagnosticsTrace($"create orchestration work item {item.WorkItemId}");
            }
            if (EtwSource.Log.IsVerboseEnabled)
            {
                EtwSource.Log.PartitionWorkItemEnqueued(this.PartitionId, this.TraceContext.Value ?? "", item.WorkItemId);
            }

            this.OrchestrationWorkItemQueue.Add(item);
        }

        public void ReportError(string where, Exception e)
        {
            if (EtwSource.EmitDiagnosticsTrace)
            {
                System.Diagnostics.Trace.TraceError($"Part{this.PartitionId:D2} !!! Exception in {where}: {e}");
            }
            if (EtwSource.EmitEtwTrace)
            {
                EtwSource.Log.PartitionErrorReported(this.PartitionId, where, e.GetType().Name, e.Message);
            }
        }

        public void TraceProcess(PartitionEvent evt)
        {
            this.TraceContext.Value = $"{evt.CommitPosition:D7}   ";

            if (EtwSource.EmitDiagnosticsTrace)
            {
                System.Diagnostics.Trace.TraceInformation($"Part{this.PartitionId:D2}.{evt.CommitPosition:D7} Committing {evt} {evt.WorkItem}");
            }
            if (EtwSource.Log.IsVerboseEnabled)
            {
                EtwSource.Log.PartitionEventReceived(this.PartitionId, this.TraceContext.Value ?? "", evt.WorkItem, evt.ToString());
            }
        }

        public void TraceSend(Event evt)
        {
            if (EtwSource.EmitDiagnosticsTrace)
            {
                this.DiagnosticsTrace($"Sending {evt} {evt.WorkItem}");
            }
            if (EtwSource.Log.IsVerboseEnabled)
            {
                EtwSource.Log.PartitionEventSent(this.PartitionId, this.TraceContext.Value ?? "", evt.WorkItem, evt.ToString());
            }
        }

        public void TraceSubmit(Event evt)
        {
            if (EtwSource.EmitDiagnosticsTrace)
            {
                this.DiagnosticsTrace($"Submitting {evt} {evt.WorkItem}");
            }
            if (EtwSource.Log.IsVerboseEnabled)
            {
                EtwSource.Log.PartitionEventSent(this.PartitionId, this.TraceContext.Value ?? "", evt.WorkItem, evt.ToString());
            }
        }

        public void DiagnosticsTrace(string msg)
        {
            var context = this.TraceContext.Value;
            if (string.IsNullOrEmpty(context))
            {
                System.Diagnostics.Trace.TraceInformation($"Part{this.PartitionId:D2}         {msg}");
            }
            else
            {
                System.Diagnostics.Trace.TraceInformation($"Part{this.PartitionId:D2}.{context} {msg}");
            }
        }
 
        /******************************/
        // Client requests
        /******************************/

        public async Task HandleAsync(WaitRequestReceived request)
        {
            try
            {
                var waiter = new OrchestrationWaiter(request, this);

                // start an async read from state
                var readTask = waiter.ReadFromStateAsync(this.State);

                var response = await waiter.Task;

                if (response != null)
                {
                    this.Send(response);
                }
            }
            catch (TaskCanceledException)
            {
            }
            catch (Exception e)
            {
                this.ReportError($"{nameof(HandleAsync)}({request.GetType().Name})", e);
            }
        }

        private class OrchestrationWaiter : ResponseWaiter, PubSub<string, OrchestrationState>.IListener
        {
            public OrchestrationWaiter(WaitRequestReceived request, Partition partition) 
                : base(partition.PartitionShutdownToken, request, partition)
            {
                Key = request.InstanceId;
                partition.InstanceStatePubSub.Subscribe(this);
            }

            public string Key { get; private set; }

            public void Notify(OrchestrationState value)
            {
                if (value != null &&
                    value.OrchestrationStatus != OrchestrationStatus.Running &&
                    value.OrchestrationStatus != OrchestrationStatus.Pending &&
                    value.OrchestrationStatus != OrchestrationStatus.ContinuedAsNew)
                {
                    this.TrySetResult(new WaitResponseReceived()
                    {
                        ClientId = Request.ClientId,
                        RequestId = Request.RequestId,
                        OrchestrationState = value
                    });
                }
            }

            public async Task ReadFromStateAsync(Storage.IPartitionState state)
            {
                var orchestrationState = await state.ReadAsync<InstanceState,OrchestrationState>(
                    TrackedObjectKey.Instance(this.Key),
                    InstanceState.GetOrchestrationState);

                this.Notify(orchestrationState);
            }

            protected override void Cleanup()
            {
                this.Partition.InstanceStatePubSub.Unsubscribe(this);
                base.Cleanup();
            }
        }

        public async Task HandleAsync(StateRequestReceived request)
        {
            try
            {
                var orchestrationState = await this.State.ReadAsync<InstanceState,OrchestrationState>(
                     TrackedObjectKey.Instance(request.InstanceId),
                     InstanceState.GetOrchestrationState);

                var response = new StateResponseReceived()
                {
                    ClientId = request.ClientId,
                    RequestId = request.RequestId,
                    OrchestrationState = orchestrationState,
                };

                this.Send(response);
            }
            catch (TaskCanceledException)
            {
            }
            catch (Exception e)
            {
                this.ReportError($"{nameof(HandleAsync)}({request.GetType().Name})", e);
            }
        }
    }
}
