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

using System.Runtime.Serialization;

namespace DurableTask.EventSourced
{
    // There is no persistent instance state as PSFs are now used, so this is just for the initial InstanceQueryReceived ReadTarget.

    [DataContract]
    internal class IndexState : TrackedObject
    {
        [IgnoreDataMember]
        public override TrackedObjectKey Key => TrackedObjectKey.Index;
    }
}