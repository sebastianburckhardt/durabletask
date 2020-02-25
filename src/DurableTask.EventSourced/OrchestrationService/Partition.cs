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

        public EventSourcedOrchestrationServiceSettings Settings { get; private set; }

        public StorageAbstraction.IPartitionState State { get; private set; }
        public TransportAbstraction.ISender BatchSender { get; private set; }
        public WorkItemQueue<TaskActivityWorkItem> ActivityWorkItemQueue { get; private set; }
        public WorkItemQueue<TaskOrchestrationWorkItem> OrchestrationWorkItemQueue { get; private set; }

        public CancellationToken PartitionShutdownToken => this.partitionShutdown.Token;

        public BatchTimer<PartitionEvent> PendingTimers { get; private set; }
        public PubSub<string, OrchestrationState> InstanceStatePubSub { get; private set; }
        public ConcurrentDictionary<long, ResponseWaiter> PendingResponses { get; private set; }

        private readonly CancellationTokenSource partitionShutdown;

        public Partition(
            EventSourcedOrchestrationService host,
            uint partitionId,
            Func<string, uint> partitionFunction,
            Func<uint> numberPartitions,
            StorageAbstraction.IPartitionState state,
            TransportAbstraction.ISender batchSender,
            EventSourcedOrchestrationServiceSettings settings,
            WorkItemQueue<TaskActivityWorkItem> activityWorkItemQueue,
            WorkItemQueue<TaskOrchestrationWorkItem> orchestrationWorkItemQueue,
            CancellationToken serviceShutdownToken)
        {
            this.host = host;
            this.logger = host.Logger;
            this.PartitionId = partitionId;
            this.TracePrefix = GetTracePrefix();
            this.PartitionFunction = partitionFunction;
            this.NumberPartitions = numberPartitions;
            this.State = state;
            this.BatchSender = batchSender;
            this.Settings = settings;
            this.ActivityWorkItemQueue = activityWorkItemQueue;
            this.OrchestrationWorkItemQueue = orchestrationWorkItemQueue;

            this.partitionShutdown = CancellationTokenSource.CreateLinkedTokenSource(
                serviceShutdownToken,
                state.OwnershipCancellationToken);
        }

        public async Task<ulong> StartAsync(CancellationToken token)
        {
            // create or restore partition state from last snapshot
            try
            {
                // initialize collections for pending work
                this.PendingTimers = new BatchTimer<PartitionEvent>(this.PartitionShutdownToken, this.TimersFired);
                this.InstanceStatePubSub = new PubSub<string, OrchestrationState>();
                this.PendingResponses = new ConcurrentDictionary<long, ResponseWaiter>();

                var inputQueuePosition = await State.CreateOrRestoreAsync(this, token);
                
                this.PendingTimers.Start($"Timer{this.PartitionId:D2}");

                return inputQueuePosition;
            }
            catch (Exception e)
            {
                this.ReportError("could not start partition", e);
                throw;
            }
        }

        public void ProcessAsync(PartitionEvent partitionEvent)
        {
            this.State.Submit(partitionEvent);
        }

        public async Task StopAsync()
        {
            // create or restore partition state from last snapshot
            try
            {
                // stop all in-progress activities (timers, work items etc.)
                this.partitionShutdown.Cancel();

                // wait for current state (log and store) to be persisted
                await this.State.PersistAndShutdownAsync();

            }
            catch (Exception e)
            {
                this.ReportError("could not stop partition", e);
                throw;
            }
            finally
            {
                EtwSource.Log.PartitionStopped((int)this.PartitionId);
            }
        }

        private void TimersFired(List<PartitionEvent> timersFired)
        {
            try
            {
                this.SubmitRange(timersFired);
            }
            catch (Exception e)
            {
                this.ReportError("Submitting Partition Timers", e);
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
            this.TraceSend(evt);
            this.BatchSender.Submit(evt);
        }

        public void Submit(PartitionEvent evt)
        {
            this.State.Submit(evt);
        }

        public void SubmitRange(IEnumerable<PartitionEvent> partitionEvents)
        {
            this.State.SubmitRange(partitionEvents);
        }

        public void EnqueueActivityWorkItem(ActivityWorkItem item)
        {
            this.DetailTracer?.TraceDetail($"Creating ActivityWorkItem {item.WorkItemId}");
 
            this.ActivityWorkItemQueue.Add(item);
        }

        public void EnqueueOrchestrationWorkItem(OrchestrationWorkItem item)
        {
            this.DetailTracer?.TraceDetail($"Creating OrchestrationWorkItem {item.WorkItemId}");
 
            this.OrchestrationWorkItemQueue.Add(item);
        }
    }
}
