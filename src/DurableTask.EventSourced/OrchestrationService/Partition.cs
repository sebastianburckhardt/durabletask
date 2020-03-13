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

        public StorageAbstraction.IPartitionState State { get; private set; }
        public TransportAbstraction.ISender BatchSender { get; private set; }
        public WorkItemQueue<TaskActivityWorkItem> ActivityWorkItemQueue { get; private set; }
        public WorkItemQueue<TaskOrchestrationWorkItem> OrchestrationWorkItemQueue { get; private set; }

        public BatchTimer<PartitionEvent> PendingTimers { get; private set; }
        public PubSub<string, OrchestrationState> InstanceStatePubSub { get; private set; }
        public ConcurrentDictionary<long, ResponseWaiter> PendingResponses { get; private set; }

        public Partition(
            EventSourcedOrchestrationService host,
            uint partitionId,
            Func<string, uint> partitionFunction,
            Func<uint> numberPartitions,
            TransportAbstraction.ISender batchSender,
            EventSourcedOrchestrationServiceSettings settings,
            WorkItemQueue<TaskActivityWorkItem> activityWorkItemQueue,
            WorkItemQueue<TaskOrchestrationWorkItem> orchestrationWorkItemQueue)
        {
            this.host = host;
            this.logger = host.Logger;
            this.PartitionId = partitionId;
            this.PartitionFunction = partitionFunction;
            this.NumberPartitions = numberPartitions;
            this.BatchSender = batchSender;
            this.Settings = settings;
            this.ActivityWorkItemQueue = activityWorkItemQueue;
            this.OrchestrationWorkItemQueue = orchestrationWorkItemQueue;
        }

        public async Task<ulong> StartAsync(IPartitionErrorHandler errorHandler, ulong firstInputQueuePosition)
        {
            Partition.ClearTraceContext();
            this.TraceDetail("starting partition");

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
                
                this.PendingTimers.Start($"Timer{this.PartitionId:D2}");

                return inputQueuePosition;
            }
            catch (Exception e)
            {
                this.ErrorHandler.HandleError(nameof(StartAsync), "could not start partition", e, true, false);
                throw;
            }
        }

        public void ProcessAsync(PartitionEvent partitionEvent)
        {
            this.State.Submit(partitionEvent);
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

                this.ErrorHandler.HandleError(stacktrace, "assertion failed", null, false, false);
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
                this.ErrorHandler.HandleError(nameof(StopAsync), "could not shut down partition state cleanly", e, true, false);
            }

            // at this point, the partition has been terminated (either cleanly or by exception)
            this.Assert(this.ErrorHandler.IsTerminated);
            EtwSource.Log.PartitionStopped((int)this.PartitionId);
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
                this.ErrorHandler.HandleError("TimersFired", "Submitting Partition Timers", e, true, false);
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

        public void SubmitInputEvents(IEnumerable<PartitionEvent> partitionEvents)
        {
            this.State.SubmitInputEvents(partitionEvents);
        }

        public void EnqueueActivityWorkItem(ActivityWorkItem item)
        {
            this.DetailTracer?.TraceDetail($"Enqueueing ActivityWorkItem {item.WorkItemId}");
 
            this.ActivityWorkItemQueue.Add(item);
        }

 
        public void EnqueueOrchestrationWorkItem(OrchestrationWorkItem item)
        { 
            this.DetailTracer?.TraceDetail($"Enqueueing OrchestrationWorkItem batch={item.MessageBatch.WorkItemId}");

            this.OrchestrationWorkItemQueue.Add(item);
        }
    }
}
