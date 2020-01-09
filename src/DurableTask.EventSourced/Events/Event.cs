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
using System.Runtime.Serialization;
using System.Text;
using DurableTask.Core;

namespace DurableTask.EventSourced
{
    [DataContract]
    [KnownTypeAttribute("KnownTypes")]
    internal abstract class Event
    {
        /// <summary>
        /// The position at which this event committed, filled in by the storage back-end.
        /// </summary>
        [IgnoreDataMember]
        public long CommitPosition { get; set; } = -1;

        /// <summary>
        /// Some events should not be duplicated, so we do not retry them when enqueue is ambigous
        /// </summary>
        [IgnoreDataMember]
        public abstract bool AtLeastOnceDelivery { get; }

        [IgnoreDataMember]
        public Backend.IAckListener AckListener { get; set; }

        public override string ToString()
        {
            var s = new StringBuilder();
            s.Append(this.GetType().Name);
            this.TraceInformation(s);
            return s.ToString();
        }

        protected virtual void TraceInformation(StringBuilder s)
        {
        }

        public static IEnumerable<Type> KnownTypes()
        {
            yield return typeof(ClientEventFragment);
            yield return typeof(CreationResponseReceived);
            yield return typeof(StateResponseReceived);
            yield return typeof(WaitResponseReceived);
            yield return typeof(ClientTaskMessagesReceived);
            yield return typeof(CreationRequestReceived);
            yield return typeof(StateRequestReceived);
            yield return typeof(WaitRequestReceived);
            yield return typeof(ActivityCompleted);
            yield return typeof(BatchProcessed);
            yield return typeof(SentOrPersisted);
            yield return typeof(TimerFired);
            yield return typeof(HostStarted);
            yield return typeof(TaskhubCreated);
            yield return typeof(TaskMessageReceived);
            yield return typeof(PartitionEventFragment);
        }

        #region ETW trace properties

        [IgnoreDataMember]
        public virtual string WorkItem => string.Empty;

        #endregion
    }
}