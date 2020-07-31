﻿//  You may obtain a copy of the License at
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
    internal interface IClientRequestEvent
    {
        Guid ClientId { get; set; }

        long RequestId { get; set; }

        DateTime TimeoutUtc { get; set; }

        EventId EventId { get; }
    }
}