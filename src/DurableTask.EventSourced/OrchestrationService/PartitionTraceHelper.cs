using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;

namespace DurableTask.EventSourced
{
    internal class PartitionTraceHelper
    {
        private readonly ILogger logger;
        private readonly string account;
        private readonly string taskHub;
        private readonly int partitionId;
        private readonly LogLevel etwLogLevel;

        public PartitionTraceHelper(ILogger logger, LogLevel etwLogLevel, string storageAccountName, string taskHubName, uint partitionId)
        {
            this.logger = logger;
            this.account = storageAccountName;
            this.taskHub = taskHubName;
            this.partitionId = (int) partitionId;
            this.etwLogLevel = etwLogLevel;
        }

        public void TraceProgress(string details)
        {
            if (this.etwLogLevel <= LogLevel.Information)
            {
                this.logger.LogInformation("Part{partition:D2} {details}", this.partitionId, details);
                EtwSource.Log.PartitionProgress(this.account, this.taskHub, this.partitionId, details, TraceUtils.ExtensionVersion);
            }
        }

        public void TracePartitionLoad(LoadMonitorAbstraction.PartitionLoadInfo info)
        {
            logger.LogInformation("Part{partition:D2} Publishing LoadInfo WorkItems={workItems} Activities={activities} Timers={timers} Outbox={outbox} NextTimer={nextTimer} InputQueuePosition={inputQueuePosition} CommitLogPosition={commitLogPosition}",
                partitionId, info.WorkItems, info.Activities, info.Timers, info.Outbox, info.NextTimer, info.InputQueuePosition, info.CommitLogPosition);
            if (this.etwLogLevel <= LogLevel.Information)
            {
                EtwSource.Log.PartitionLoadPublished(this.account, this.taskHub, partitionId, info.WorkItems, info.Activities, info.Timers, info.Outbox, info.NextTimer?.ToString("o") ?? "", info.InputQueuePosition, info.CommitLogPosition, TraceUtils.ExtensionVersion);
            }
        }
    }
}