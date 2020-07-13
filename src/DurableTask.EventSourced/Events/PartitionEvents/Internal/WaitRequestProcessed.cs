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
    internal class WaitRequestProcessed : PartitionUpdateEvent
    {
        [DataMember]
        public Guid ClientId { get; set; }

        [DataMember]
        public long RequestId { get; set; }

        [DataMember]
        public DateTime TimeoutUtc { get; set; }

        [DataMember]
        public string InstanceId { get; set; }

        [DataMember]
        public string ExecutionId { get; set; }

        [IgnoreDataMember]
        public override EventId EventId => EventId.MakeSubEventId(EventId.MakeClientRequestEventId(this.ClientId, this.RequestId), 1);

        protected override void ExtraTraceInformation(StringBuilder s)
        {
            s.Append(' ');
            s.Append(this.InstanceId);
        }

        public static bool SatisfiesWaitCondition(OrchestrationState value)
            => (value != null &&
                value.OrchestrationStatus != OrchestrationStatus.Running &&
                value.OrchestrationStatus != OrchestrationStatus.Pending &&
                value.OrchestrationStatus != OrchestrationStatus.ContinuedAsNew);

        public WaitResponseReceived CreateResponse(OrchestrationState value)
            => new WaitResponseReceived()
            {
                ClientId = this.ClientId,
                RequestId = this.RequestId,
                OrchestrationState = value
            };

        public override void DetermineEffects(EffectTracker effects)
        {
            effects.Add(TrackedObjectKey.Instance(this.InstanceId));
        }
    }
}
