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
using System.Runtime.Serialization;
using System.Text;

namespace DurableTask.EventSourced
{
    [DataContract]
    internal abstract class PartitionReadEvent : PartitionEvent
    {
        /// <summary>
        /// The target of the read operation.
        /// </summary>
        [IgnoreDataMember]
        public abstract TrackedObjectKey ReadTarget { get; }

        /// <summary>
        /// A secondary target, to be prefetched also, before executing the read
        /// </summary>
        [IgnoreDataMember]
        public virtual TrackedObjectKey? Prefetch => null;

        /// <summary>
        /// Optionally, some extra action to perform before issuing the read
        /// </summary>
        public virtual void OnReadIssued(Partition partition) { }

        /// <summary>
        /// The continuation for the read operation.
        /// </summary>
        /// <param name="target">The current value of the tracked object for this key, or null if not present</param>
        /// <param name="partition">The partition</param>
        public abstract void OnReadComplete(TrackedObject target, Partition partition);


        #region prefetch state machine

        [IgnoreDataMember]
        private bool prefetchLoaded;

        [IgnoreDataMember]
        private bool targetLoaded;

        [IgnoreDataMember]
        private TrackedObject target;

        public void Deliver(TrackedObjectKey key, TrackedObject trackedObject, out bool isReady)
        {
            if (!this.Prefetch.HasValue)
            {
                prefetchLoaded = true;
                targetLoaded = true;
                target = trackedObject;
            }
            else if (key.Equals(Prefetch.Value))
            {
                prefetchLoaded = true;
            }
            else
            {
                targetLoaded = true;
                target = trackedObject;
            }

            isReady = (prefetchLoaded && targetLoaded);
        }

        public void Fire(Partition partition)
        {
            this.OnReadComplete(target, partition);
        }
      
        #endregion
    }

}
