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

namespace DurableTask.EventHubs
{
    internal class CircularLinkedList<T> where T: class
    {
        private readonly object thisLock = new object();

        private Node tail;

        public abstract class Node
        {
            [IgnoreDataMember]
            public Node next;

            [IgnoreDataMember]
            public Node prev;

            [IgnoreDataMember]
            public CircularLinkedList<T> List;

            public void RemoveFromList()
            {
                if (List == null)
                {
                    throw new InvalidOperationException("cannot be removed, not a list member");
                }

                lock (List.thisLock)
                {
                    if (List.tail == this && this.next == this)
                    {
                        // special case: we are removing the last node in the list
                        List.tail = null;
                    }
                    else
                    {
                        // unlink from ring
                        var n = next;
                        var p = prev;
                        n.prev = p;
                        p.prev = n;
                    }

                    next = null;
                    prev = null;
                    List = null;
                }
            }
        }

        public void Add(Node node)
        {
            if (node.List != null)
            {
                throw new InvalidOperationException("cannot be added to more than one list");
            }

            lock (thisLock)
            {
                if (tail == null)
                {
                    node.next = node;
                    node.prev = node;
                }
                else
                {
                    node.next = tail.next;
                    tail.next.prev = node;
                    node.prev = tail;
                    tail.next = node;
                }

                tail = node;
            }
        } 
    }
}
