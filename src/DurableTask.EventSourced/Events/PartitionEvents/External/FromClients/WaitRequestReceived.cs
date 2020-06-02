//  You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  ----------------------------------------------------------------------------------

using DurableTask.Core;
using System;
using System.Diagnostics;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace DurableTask.EventSourced
{
    [DataContract]
    internal class WaitRequestReceived : ClientReadRequestEvent
    {
        [DataMember]
        public string InstanceId { get; set; }

        [DataMember]
        public string ExecutionId { get; set; }

        [IgnoreDataMember]
        OrchestrationWaiter waiter;

        protected override void ExtraTraceInformation(StringBuilder s)
        {
            s.Append(' ');
            s.Append(this.InstanceId);
        }

        public override void OnReadIssued(Partition partition)
        { 
            // to avoid race conditions, we must subscribe to instance status changes BEFORE starting the read
            this.waiter = new OrchestrationWaiter(this, partition);

            // start a waiter that eventually responds to the client
            _ = WaitForOrchestrationCompletionTask(partition);
        }

        [IgnoreDataMember]
        public override TrackedObjectKey ReadTarget => TrackedObjectKey.Instance(this.InstanceId);

        public override void OnReadComplete(TrackedObject target, Partition partition)
        {
            this.waiter.Notify(((InstanceState)target)?.OrchestrationState);
        }

        public async Task WaitForOrchestrationCompletionTask(Partition partition)
        {
            try
            {
                var response = await this.waiter.Task.ConfigureAwait(false);

                if (response != null)
                {
                    partition.Send(response);
                }
            }
            catch (OperationCanceledException)
            {
                // o.k. during shutdown or termination
            }
            catch (Exception e)
            {
                partition.ErrorHandler.HandleError($"{nameof(WaitRequestReceived)}.{nameof(WaitForOrchestrationCompletionTask)}", "Encountered exception while waiting for orchestration", e, false, false);
            }
        }

        private class OrchestrationWaiter :
            CancellableCompletionSource<ClientEvent>,
            PubSub<string, OrchestrationState>.IListener
        {
            private readonly WaitRequestReceived Request;
            private readonly Partition Partition;

            public OrchestrationWaiter(WaitRequestReceived request, Partition partition)
                : base(partition.ErrorHandler.Token)
            {
                this.Request = request;
                this.Partition = partition;
                this.Key = request.InstanceId;
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

            protected override void Cleanup()
            {
                this.Partition.InstanceStatePubSub.Unsubscribe(this);
                base.Cleanup();
            }
        }
    }
}
