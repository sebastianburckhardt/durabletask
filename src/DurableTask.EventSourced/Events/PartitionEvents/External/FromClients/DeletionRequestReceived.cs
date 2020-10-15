﻿//  ----------------------------------------------------------------------------------
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
using System.Diagnostics;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using DurableTask.Core;
using DurableTask.Core.Exceptions;
using DurableTask.Core.History;

namespace DurableTask.EventSourced
{
    [DataContract]
    internal class DeletionRequestReceived : ClientRequestEventWithPrefetch
    {
        [DataMember]
        public string InstanceId { get; set; }

        [DataMember]
        public DateTime? CreatedTime { get; set; }  // works like an e-tag, if specified

        [IgnoreDataMember]
        public override TrackedObjectKey Target => TrackedObjectKey.Instance(this.InstanceId);

        [IgnoreDataMember]
        public override TrackedObjectKey? Prefetch => TrackedObjectKey.History(this.InstanceId);

        protected override void ExtraTraceInformation(StringBuilder s)
        {
            s.Append(' ');
            s.Append(this.InstanceId);
        }
    }
}
