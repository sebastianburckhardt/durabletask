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
    internal class Client : TransportAbstraction.IClient
    {
        private readonly EventSourcedOrchestrationService host;
        private readonly CancellationToken shutdownToken; 
        private static TimeSpan DefaultTimeout = TimeSpan.FromMinutes(5);

        public Guid ClientId { get; private set; }
        private TransportAbstraction.ISender BatchSender { get; set; }


        private long SequenceNumber; // for numbering requests that enter on this client

        private BatchTimer<ResponseWaiter> ResponseTimeouts;
        private ConcurrentDictionary<long, ResponseWaiter> ResponseWaiters;
        private Dictionary<Guid, List<ClientEventFragment>> Fragments;

        public string AbbreviatedClientId; // used for tracing
        
        public Client(EventSourcedOrchestrationService host, Guid clientId, TransportAbstraction.ISender batchSender, CancellationToken shutdownToken)
        {
            this.host = host;
            this.ClientId = clientId;
            this.AbbreviatedClientId = clientId.ToString("N").Substring(0,7);
            this.BatchSender = batchSender;
            this.shutdownToken = shutdownToken;
            this.ResponseTimeouts = new BatchTimer<ResponseWaiter>(this.shutdownToken, Timeout);
            this.ResponseWaiters = new ConcurrentDictionary<long, ResponseWaiter>();
            this.Fragments = new Dictionary<Guid, List<ClientEventFragment>>();
            this.ResponseTimeouts.Start("ClientTimer");
        }

        public Task StopAsync()
        {
            EtwSource.Log.ClientStopped(this.ClientId);
            return Task.CompletedTask;
        }

        public void Process(ClientEvent clientEvent)
        {
            if (!(clientEvent is ClientEventFragment fragment))
            {
                ProcessInternal(clientEvent);
            }
            else
            {
                if (!fragment.IsLast)
                {
                    if (!this.Fragments.TryGetValue(fragment.CohortId, out var list))
                    {
                        this.Fragments[fragment.CohortId] = list = new List<ClientEventFragment>();
                    }
                    list.Add(fragment);
                }
                else
                {
                    var reassembledEvent = (ClientEvent)FragmentationAndReassembly.Reassemble(this.Fragments[fragment.CohortId], fragment);
                    this.Fragments.Remove(fragment.CohortId);

                    ProcessInternal(reassembledEvent);
                }
            }
        }

        private void ProcessInternal(ClientEvent clientEvent)
        {
            TraceReceive(clientEvent);
            if (this.ResponseWaiters.TryGetValue(clientEvent.RequestId, out var waiter))
            {
                waiter.TrySetResult(clientEvent);
            }
        }

        public void Send(Event evt)
        {
            TraceSend(evt);
            this.BatchSender.Submit(evt);
        }

        public void ReportError(string where, Exception e)
        {
            if (EtwSource.EmitDiagnosticsTrace)
            {
                Trace.TraceError($"Client.{this.AbbreviatedClientId} !!! Exception in {where}: {e}");
            }
            if (EtwSource.EmitEtwTrace)
            {
                EtwSource.Log.ClientErrorReported(this.ClientId, where, e.GetType().Name, e.Message);
            }
        }

        private void TraceSend(Event m)
        {
            if (EtwSource.EmitDiagnosticsTrace)
            {
                Trace.TraceInformation($"Client.{this.AbbreviatedClientId} Sending {m}");
            }
            if (EtwSource.Log.IsVerboseEnabled)
            {
                EtwSource.Log.ClientEventSent(this.ClientId, m.ToString());
            }
        }

        private void TraceReceive(Event m)
        {
            if (EtwSource.EmitDiagnosticsTrace)
            {
                Trace.TraceInformation($"Client.{this.AbbreviatedClientId} Processing {m}");
            }
            if (EtwSource.Log.IsVerboseEnabled)
            {
                EtwSource.Log.ClientEventReceived(this.ClientId, m.ToString());
            }
        }

        private static void Timeout<T>(IEnumerable<CancellableCompletionSource<T>> promises) where T : class
        {
            foreach (var promise in promises)
            {
                promise.TrySetTimeoutException();
            }
        }

        private Task<ClientEvent> PerformRequestWithTimeoutAndCancellation(CancellationToken token, ClientRequestEvent request, bool doneWhenSent)
        {
            var waiter = new ResponseWaiter(this.shutdownToken, request.RequestId, this);
            this.ResponseWaiters.TryAdd(request.RequestId, waiter);
            this.ResponseTimeouts.Schedule(DateTime.UtcNow + request.Timeout, waiter);

            if (doneWhenSent)
            {
                AckListeners.Register(request, waiter);
            }

            this.Send(request);

            return waiter.Task;
        }

        internal class ResponseWaiter : CancellableCompletionSource<ClientEvent>, TransportAbstraction.IAckOrExceptionListener
        {
            private long id;
            private Client client;

            public ResponseWaiter(CancellationToken token, long id, Client client) : base(token)
            {
                this.id = id;
                this.client = client;
            }

            public void Acknowledge(Event evt)
            {
                this.TrySetResult(null); // task finishes when the send has been confirmed, no result is returned
            }

            public void ReportException(Event evt, Exception e)
            {
                this.TrySetException(e); // task finishes with exception
            }

            protected override void Cleanup()
            {
                client.ResponseWaiters.TryRemove(this.id, out var _);
                base.Cleanup();
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
                Timeout = DefaultTimeout,
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
                Timeout = DefaultTimeout,
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
                Timeout = timeout,         
            };

            var response = await PerformRequestWithTimeoutAndCancellation(cancellationToken, request, false);            
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
                Timeout = DefaultTimeout,
            };

            var response = await PerformRequestWithTimeoutAndCancellation(CancellationToken.None, request, false);
            return ((StateResponseReceived)response)?.OrchestrationState;
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
                Timeout = DefaultTimeout,
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
