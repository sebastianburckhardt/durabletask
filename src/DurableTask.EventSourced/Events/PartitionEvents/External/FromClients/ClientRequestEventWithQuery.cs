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
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using DurableTask.Core;

namespace DurableTask.EventSourced
{
    [DataContract]
    internal abstract class ClientRequestEventWithQuery : ClientRequestEvent, IClientRequestEvent, InstanceQueries.IQuerySpec
    {
        [DataMember]
        public ProcessingPhase Phase { get; set; }

        /// <summary>
        /// The subset of runtime statuses to return, or null if all
        /// </summary>
        [DataMember]
        public OrchestrationStatus[] RuntimeStatus { get; set; }

        /// <summary>
        /// The lowest creation time to return, or null if no lower bound
        /// </summary>
        [DataMember]
        public DateTime? CreatedTimeFrom { get; set; }

        /// <summary>
        /// The latest creation time to return, or null if no upper bound
        /// </summary>
        [DataMember]
        public DateTime? CreatedTimeTo { get; set; }

        /// <summary>
        /// A prefix of the instance ids to return, or null if no specific prefix
        /// </summary>
        [DataMember]
        public string InstanceIdPrefix { get; set; }

        [IgnoreDataMember]
        public override EventId EventId => EventId.MakeClientRequestEventId(ClientId, RequestId);

        public virtual Task OnQueryCompleteAsync(IAsyncEnumerable<OrchestrationState> result, Partition partition)
        {
            return Task.CompletedTask;
        }

        public sealed override void DetermineEffects(EffectTracker effects)
        {
            effects.Add(TrackedObjectKey.Queries);
        }

        public enum ProcessingPhase
        { 
             Query,
             Confirm,
             ConfirmAndProcess,
        }
    }
}