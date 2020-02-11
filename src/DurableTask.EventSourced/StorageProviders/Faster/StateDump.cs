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
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventSourced.Faster
{
    // Useful for debugging.
    internal class StateDump : StorageAbstraction.IReadContinuation, IComparer<TrackedObjectKey>
    {
        private readonly SortedDictionary<TrackedObjectKey, TrackedObject> results;

        public StateDump()
        {
            this.results = new SortedDictionary<TrackedObjectKey, TrackedObject>(this);
        }

        public TrackedObjectKey ReadTarget => throw new NotImplementedException(); // not used

        public int Compare(TrackedObjectKey x, TrackedObjectKey y)
        {
            return TrackedObjectKey.Compare(ref x, ref y);
        }

        public void OnReadComplete(TrackedObject target)
        {
            results.Add(target.Key, target);
        }

        public string PrintSimpleSummary()
        {
            var stringBuilder = new StringBuilder();
            foreach (var trackedObject in this.results.Values)
            {
                stringBuilder.Append(trackedObject.ToString());
                stringBuilder.AppendLine();
            }
            return stringBuilder.ToString();
        }
    }
}