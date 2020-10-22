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
using System.Text;

namespace DurableTask.EventSourced.Scaling
{
    /// <summary>
    /// Reported load information about a specific partition.
    /// </summary>
    public class PartitionLoadInfo
    {
        /// <summary>
        /// The number of orchestration work items waiting to be processed.
        /// </summary>
        public int WorkItems { get; set; }

        /// <summary>
        /// The number of activities that are waiting to be processed.
        /// </summary>
        public int Activities { get; set; }

        /// <summary>
        /// The number of timers that are waiting to fire.
        /// </summary>
        public int Timers { get; set; }

        /// <summary>
        /// The number of client requests waiting to be processed.
        /// </summary>
        public int Requests { get; set; }

        /// <summary>
        /// The number of work items that have messages waiting to be sent.
        /// </summary>
        public int Outbox { get; set; }

        /// <summary>
        /// The next time on which to wake up.
        /// </summary>
        public DateTime? Wakeup { get; set; }

        /// <summary>
        /// The input queue position of this partition, which is  the next expected EventHubs sequence number.
        /// </summary>
        public long InputQueuePosition { get; set; }

        /// <summary>
        /// The commit log position of this partition.
        /// </summary>
        public long CommitLogPosition { get; set; }

        /// <summary>
        /// The latency of the activity queue.
        /// </summary>
        public long ActivityLatencyMs { get; set; }

        /// <summary>
        /// The latency of the work item queue.
        /// </summary>
        public long WorkItemLatencyMs { get; set; }

        /// <summary>
        /// The worker id of the host that is currently running this partition.
        /// </summary>
        public string WorkerId { get; set; }

        /// <summary>
        /// A string encoding of the latency trend.
        /// </summary>
        public string LatencyTrend { get; set; }

        /// <summary>
        /// Percentage of message batches that miss in the cache.
        /// </summary>
        public double MissRate { get; set; }

        /// <summary>
        /// The character representing idle load.
        /// </summary>
        public const char Idle = 'I';

        /// <summary>
        /// The character representing low latency.
        /// </summary>
        public const char LowLatency = 'L';

        /// <summary>
        /// The character representing medium latency.
        /// </summary>
        public const char MediumLatency = 'M';

        /// <summary>
        /// The character representing high latency.
        /// </summary>
        public const char HighLatency = 'H';

        /// <summary>
        /// All of the latency category characters in order
        /// </summary>
        public static char[] LatencyCategories = new char[] { Idle, LowLatency, MediumLatency, HighLatency };

        /// <summary>
        /// The maximum length of the latency trend
        /// </summary>
        public static int LatencyTrendLength = 5;

    }
}
