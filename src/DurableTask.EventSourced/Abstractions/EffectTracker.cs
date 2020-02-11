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

using System.Collections.Generic;

namespace DurableTask.EventSourced
{
    /// <summary>
    /// Is used while applying an effect to a partition state, to carry
    /// information about the context, and to enumerate the objects on which the effect
    /// is being processed.
    /// </summary>
    internal class EffectTracker : List<TrackedObjectKey>
    {
        public EffectTracker(Partition Partition)
        {
            this.Partition = Partition;
        }

        /// <summary>
        /// The current partition.
        /// </summary>
        public Partition Partition { get; }

        /// <summary>
        /// The effect currently being applied.
        /// </summary>
        public dynamic Effect { get; set; }

        /// <summary>
        /// True if we are replaying this effect during recovery.
        /// Typically, external side effects (such as launching tasks, sending responses, etc.)
        /// are suppressed during replay.
        /// </summary>
        public bool IsReplaying { get; set; }

        /// <summary>
        /// Applies the event to the given tracked object, using dynamic dispatch to 
        /// select the correct Process method overload for the event.
        /// </summary>
        /// <param name="trackedObject"></param>
        public void ProcessEffectOn(dynamic trackedObject)
        {
            trackedObject.Process(Effect, this);
        }
    }
}
