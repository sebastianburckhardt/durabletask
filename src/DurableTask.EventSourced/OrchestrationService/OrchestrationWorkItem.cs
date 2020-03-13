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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DurableTask.Core;
using DurableTask.Core.History;

namespace DurableTask.EventSourced
{
    internal class OrchestrationWorkItem : TaskOrchestrationWorkItem
    {

        public OrchestrationMessageBatch MessageBatch { get; set; }

        public Partition Partition => MessageBatch.Partition;

        public string WorkItemId => $"S{MessageBatch.SessionId:D6}:{MessageBatch.BatchStartPosition}[{MessageBatch.BatchLength}]";

        public OrchestrationWorkItem(OrchestrationMessageBatch messageBatch, List<HistoryEvent> previousHistory = null)
        {
            this.MessageBatch = messageBatch;
            this.InstanceId = messageBatch.InstanceId;
            this.NewMessages = messageBatch.MessagesToProcess;
            this.OrchestrationRuntimeState = new OrchestrationRuntimeState(previousHistory);
            this.LockedUntilUtc = DateTime.MaxValue; // this backend does not require workitem lock renewals
            this.Session = null; // we don't need the extended session API in this provider because we are caching the work items
        }

        public void SetNextMessageBatch(OrchestrationMessageBatch messageBatch)
        {
            this.MessageBatch = messageBatch;
            this.NewMessages = messageBatch.MessagesToProcess;
            this.OrchestrationRuntimeState.NewEvents.Clear();
        }
    }
}
