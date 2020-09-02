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
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using DurableTask.Core;
using DurableTask.Core.History;
using Microsoft.Extensions.Logging;

namespace DurableTask.EventSourced
{
    internal class Client : TransportAbstraction.IClient
    {
        private readonly EventSourcedOrchestrationService host;
        private readonly CancellationToken shutdownToken;
        private readonly ClientTraceHelper traceHelper;
        private readonly string account;
        private readonly string taskHub;

        private static TimeSpan DefaultTimeout = TimeSpan.FromMinutes(5);

        public Guid ClientId { get; private set; }
        private TransportAbstraction.ISender BatchSender { get; set; }

        private long SequenceNumber; // for numbering requests that enter on this client

        private BatchTimer<PendingRequest> ResponseTimeouts;
        private ConcurrentDictionary<long, PendingRequest> ResponseWaiters;
        private Dictionary<string, MemoryStream> Fragments;
        
        // TODO: Have a field with unconfirmed events
            
        public static string GetShortId(Guid clientId) => clientId.ToString("N").Substring(0, 7);

        public Client(EventSourcedOrchestrationService host, Guid clientId, TransportAbstraction.ISender batchSender, CancellationToken shutdownToken)
        {
            this.host = host;
            this.ClientId = clientId;
            this.traceHelper = new ClientTraceHelper(host.Logger, host.Settings.LogLevelLimit, host.StorageAccountName, host.Settings.HubName, this.ClientId);
            this.account = host.StorageAccountName;
            this.taskHub = host.Settings.HubName;
            this.BatchSender = batchSender;
            this.shutdownToken = shutdownToken;
            this.ResponseTimeouts = new BatchTimer<PendingRequest>(this.shutdownToken, this.Timeout, this.traceHelper.TraceTimerProgress);
            this.ResponseWaiters = new ConcurrentDictionary<long, PendingRequest>();
            this.Fragments = new Dictionary<string, MemoryStream>();
            this.ResponseTimeouts.Start("ClientTimer");

            this.traceHelper.TraceProgress("Started");
        }

        public Task StopAsync()
        {
            this.traceHelper.TraceProgress("Stopped");
            return Task.CompletedTask;
        }

        public void ReportTransportError(string message, Exception e)
        {
            this.traceHelper.TraceError("ReportTransportError", message, e);
        }

        public void Process(ClientEvent clientEvent)
        {
            if (!(clientEvent is ClientEventFragment fragment))
            {
                ProcessInternal(clientEvent);
            }
            else
            {
                var originalEventString = fragment.OriginalEventId.ToString();

                if (!fragment.IsLast)
                {
                    if (!this.Fragments.TryGetValue(originalEventString, out var stream))
                    {
                        this.Fragments[originalEventString] = stream = new MemoryStream();
                    }
                    stream.Write(fragment.Bytes, 0, fragment.Bytes.Length);
                }
                else
                {
                    var reassembledEvent = FragmentationAndReassembly.Reassemble<ClientEvent>(this.Fragments[originalEventString], fragment);
                    this.Fragments.Remove(fragment.EventIdString);

                    ProcessInternal(reassembledEvent);
                }
            }
        }

        private void ProcessInternal(ClientEvent clientEvent)
        {
            this.traceHelper.TraceReceive(clientEvent);
            if (this.ResponseWaiters.TryRemove(clientEvent.RequestId, out var waiter))
            {
                waiter.Respond(clientEvent);
            }
        }

        public void Send(IClientRequestEvent evt)
        {
            var partitionEvent = (PartitionEvent)evt;
            this.traceHelper.TraceSend(partitionEvent);
            this.BatchSender.Submit(partitionEvent);
        }

        private void Timeout(List<PendingRequest> pendingRequests)
        {
            Parallel.ForEach(pendingRequests, pendingRequest => pendingRequest.TryTimeout());
        }

        // we align timeouts into buckets so we can process timeout storms more efficiently
        private const long ticksPerBucket = 2 * TimeSpan.TicksPerSecond;
        private DateTime GetTimeoutBucket(TimeSpan timeout) => new DateTime((((DateTime.UtcNow + timeout).Ticks / ticksPerBucket) * ticksPerBucket), DateTimeKind.Utc);
        
        private Task<ClientEvent> PerformRequestWithTimeoutAndCancellation(CancellationToken token, IClientRequestEvent request, bool doneWhenSent)
        {
            int timeoutId = this.ResponseTimeouts.GetFreshId();
            var pendingRequest = new PendingRequest(request.RequestId, this, request.TimeoutUtc, timeoutId);
            this.ResponseWaiters.TryAdd(request.RequestId, pendingRequest);
            this.ResponseTimeouts.Schedule(request.TimeoutUtc, pendingRequest, timeoutId);

            if (doneWhenSent)
            {
                DurabilityListeners.Register((Event) request, pendingRequest);
            }

            this.Send(request);

            return pendingRequest.Task;
        }

        internal class PendingRequest : TransportAbstraction.IDurabilityOrExceptionListener
        {
            private long requestId;
            private Client client;
            private (DateTime due, int id) timeoutKey;
            private TaskCompletionSource<ClientEvent> continuation;
            private static TimeoutException timeoutException = new TimeoutException("Client request timed out.");

            public Task<ClientEvent> Task => this.continuation.Task;
            public (DateTime, int) TimeoutKey => this.timeoutKey;

            public PendingRequest(long id, Client client, DateTime due, int timeoutId)
            {
                this.requestId = id;
                this.client = client;
                this.timeoutKey = (due, timeoutId);
                this.continuation = new TaskCompletionSource<ClientEvent>(TaskContinuationOptions.ExecuteSynchronously);
            }

            public void Respond(ClientEvent evt)
            {
                this.client.ResponseTimeouts.TryCancel(this.timeoutKey);
                this.continuation.TrySetResult(evt);
            }

            void TransportAbstraction.IDurabilityListener.ConfirmDurable(Event evt)
            {
                if (this.client.ResponseWaiters.TryRemove(this.requestId, out var _))
                {
                    this.client.ResponseTimeouts.TryCancel(this.timeoutKey);
                    this.continuation.TrySetResult(null); // task finishes when the send has been confirmed, no result is returned
                }
            }

            void TransportAbstraction.IDurabilityOrExceptionListener.ReportException(Event evt, Exception e)
            {
                if (this.client.ResponseWaiters.TryRemove(this.requestId, out var _))
                {
                    this.client.ResponseTimeouts.TryCancel(this.timeoutKey);
                    this.continuation.TrySetException(e); // task finishes with exception
                }
            }

            public void TryTimeout()
            {
                this.client.traceHelper.TraceTimerProgress($"firing ({timeoutKey.due:o},{timeoutKey.id})");
                if (this.client.ResponseWaiters.TryRemove(this.requestId, out var _))
                {
                    this.continuation.TrySetException(timeoutException); 
                }
            }
        }

        /******************************/
        // orchestration client methods
        /******************************/

        public Task CreateTaskOrchestrationAsync(uint partitionId, TaskMessage creationMessage, OrchestrationStatus[] dedupeStatuses)
        {
            var request = new CreationRequestReceived()
            {
                PartitionId = partitionId,
                ClientId = this.ClientId,
                RequestId = Interlocked.Increment(ref this.SequenceNumber),
                TaskMessage = creationMessage,
                DedupeStatuses = dedupeStatuses,
                Timestamp = DateTime.UtcNow,
                TimeoutUtc = this.GetTimeoutBucket(DefaultTimeout),
            };

            return PerformRequestWithTimeoutAndCancellation(CancellationToken.None, request, false);
        }

        public Task SendTaskOrchestrationMessageBatchAsync(uint partitionId, IEnumerable<TaskMessage> messages)
        {
            var request = new ClientTaskMessagesReceived()
            {
                PartitionId = partitionId,
                ClientId = this.ClientId,
                RequestId = Interlocked.Increment(ref this.SequenceNumber),
                TaskMessages = messages.ToArray(),
                TimeoutUtc = this.GetTimeoutBucket(DefaultTimeout),
            };

            return PerformRequestWithTimeoutAndCancellation(CancellationToken.None, request, true);
        }

        public async Task<OrchestrationState> WaitForOrchestrationAsync(
            uint partitionId,
            string instanceId,
            string executionId,
            TimeSpan timeout,
            CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(instanceId))
            {
                throw new ArgumentException(nameof(instanceId));
            }

            var request = new WaitRequestReceived()
            {
                PartitionId = partitionId,
                ClientId = this.ClientId,
                RequestId = Interlocked.Increment(ref this.SequenceNumber),
                InstanceId = instanceId,
                ExecutionId = executionId,
                TimeoutUtc = this.GetTimeoutBucket(timeout),         
            };

            var response = await PerformRequestWithTimeoutAndCancellation(cancellationToken, request, false).ConfigureAwait(false);
            return ((WaitResponseReceived)response)?.OrchestrationState;
        }

        public async Task<OrchestrationState> GetOrchestrationStateAsync(uint partitionId, string instanceId)
        {
            if (string.IsNullOrWhiteSpace(instanceId))
            {
                throw new ArgumentException(nameof(instanceId));
            }

            var request = new StateRequestReceived()
            {
                PartitionId = partitionId,
                ClientId = this.ClientId,
                RequestId = Interlocked.Increment(ref this.SequenceNumber),
                InstanceId = instanceId,
                TimeoutUtc = this.GetTimeoutBucket(DefaultTimeout),
            };

            var response = await PerformRequestWithTimeoutAndCancellation(CancellationToken.None, request, false).ConfigureAwait(false);
            return ((StateResponseReceived)response)?.OrchestrationState;
        }

        public Task<IList<OrchestrationState>> GetOrchestrationStateAsync(CancellationToken cancellationToken) 
            => RunPartitionQueries(partitionId => new InstanceQueryReceived()
                {
                    PartitionId = partitionId,
                    ClientId = this.ClientId,
                    RequestId = Interlocked.Increment(ref this.SequenceNumber),
                    TimeoutUtc = this.GetTimeoutBucket(DefaultTimeout),
                }, cancellationToken);

        public Task<IList<OrchestrationState>> GetOrchestrationStateAsync(DateTime? createdTimeFrom, DateTime? createdTimeTo,
                    IEnumerable<OrchestrationStatus> runtimeStatus, string instanceIdPrefix, CancellationToken cancellationToken = default)
            => RunPartitionQueries(partitionId => new InstanceQueryReceived()
               {
                   PartitionId = partitionId,
                   ClientId = this.ClientId,
                   RequestId = Interlocked.Increment(ref this.SequenceNumber),
                   TimeoutUtc = this.GetTimeoutBucket(DefaultTimeout),
                   CreatedTimeFrom = createdTimeFrom,
                   CreatedTimeTo = createdTimeTo,
                   RuntimeStatus = runtimeStatus?.ToArray(),
                   InstanceIdPrefix = instanceIdPrefix
               }, cancellationToken);

        private async Task<IList<OrchestrationState>> RunPartitionQueries(Func<uint, InstanceQueryReceived> requestCreator, CancellationToken cancellationToken)
        {
            IEnumerable<Task<ClientEvent>> launchQueries()
            {
                for (uint partitionId = 0; partitionId < this.host.NumberPartitions; ++partitionId)
                {
                    yield return PerformRequestWithTimeoutAndCancellation(cancellationToken, requestCreator(partitionId), false);
                }
            }

            return (await Task.WhenAll(launchQueries()).ConfigureAwait(false)).Cast<QueryResponseReceived>().SelectMany(response => response.OrchestrationStates).ToList();
        }

        public Task ForceTerminateTaskOrchestrationAsync(uint partitionId, string instanceId, string message)
        {
            var taskMessages = new[] { new TaskMessage
            {
                OrchestrationInstance = new OrchestrationInstance { InstanceId = instanceId },
                Event = new ExecutionTerminatedEvent(-1, message)
            } };

            var request = new ClientTaskMessagesReceived()
            {
                PartitionId = partitionId,
                ClientId = this.ClientId,
                RequestId = Interlocked.Increment(ref this.SequenceNumber),
                TaskMessages = taskMessages,
                TimeoutUtc = this.GetTimeoutBucket(DefaultTimeout),
            };

            return PerformRequestWithTimeoutAndCancellation(CancellationToken.None, request, true);
        }

        public Task<string> GetOrchestrationHistoryAsync(uint partitionId, string instanceId, string executionId)
        {
            throw new NotSupportedException(); //TODO
        }

        public Task PurgeOrchestrationHistoryAsync(uint partitionId, DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType)
        {
            throw new NotSupportedException(); //TODO
        }
    }
}
