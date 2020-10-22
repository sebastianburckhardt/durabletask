﻿//  Copyright Microsoft Corporation
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
using System.IO;

namespace DurableTask.EventSourced.EventHubs
{
    /// <summary>
    /// Functionality for parsing the partition scripts used by <see cref="ScriptedEventProcessorHost"/>.
    /// </summary>
    internal static class PartitionScript
    {
        private static char[] Separators = new char[] { ' ' };

        public static IEnumerable<ProcessorHostEvent> ParseEvents(DateTime scenarioStartTimeUtc, string workerId, int numPartitions, Stream script)
        {
            int currentTimeSeconds = 0;
            DateTime currentTimeUtc = scenarioStartTimeUtc;

            using (var reader = new StreamReader(script))
            {
                while (true)
                {
                    string line = reader.ReadLine();
                    if (line == null)
                    {
                        break;
                    }

                    var words = line.Split(Separators);

                    if (words[0] == "wait")
                    {
                        var seconds = int.Parse(words[1]);
                        currentTimeSeconds += seconds;
                        currentTimeUtc += TimeSpan.FromSeconds(seconds);
                    }
                    else if (words[1] == workerId || words[1] == "*")
                    {
                        int from;
                        int to;

                        if (words[2] == "*")
                        {
                            (from, to) = (0, numPartitions - 1);
                        }
                        else
                        {
                            from = int.Parse(words[2]);
                            to = (words.Length == 3) ? from : int.Parse(words[3]);
                        }

                        for (int i = from; i <= to; i++)
                        {
                            yield return new ProcessorHostEvent()
                            {
                                TimeSeconds = currentTimeSeconds,
                                TimeUtc = currentTimeUtc,
                                Action = words[0],
                                PartitionId = i,
                            };
                        }
                    }
                }
            }
        }


        /// <summary>
        /// This represents events that the CustomProcessorHost can handle, e.g. starting, stopping, restarting a partition.
        /// </summary>
        public class ProcessorHostEvent
        {
            public DateTime TimeUtc { get; set; }
            public int TimeSeconds { get; set; }
            public string Action { get; set; }
            public int PartitionId { get; set; }
        }
    }
}