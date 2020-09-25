using System;
using System.Collections.Generic;
using System.Text;
using DurableTask.Core;
using DurableTask.EventSourced.Scaling;
using Microsoft.Extensions.Logging;

namespace DurableTask.EventSourced
{
    internal class PartitionTraceHelper
    {
        private readonly ILogger logger;
        private readonly string account;
        private readonly string taskHub;
        private readonly int partitionId;
        private readonly LogLevel logLevelLimit;

        public PartitionTraceHelper(ILogger logger, LogLevel logLevelLimit, string storageAccountName, string taskHubName, uint partitionId)
        {
            this.logger = logger;
            this.account = storageAccountName;
            this.taskHub = taskHubName;
            this.partitionId = (int)partitionId;
            this.logLevelLimit = logLevelLimit;
        }

        public void TraceProgress(string details)
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                this.logger.LogInformation("Part{partition:D2} {details}", this.partitionId, details);
                if (EtwSource.Log.IsEnabled())
                {
                    EtwSource.Log.PartitionProgress(this.account, this.taskHub, this.partitionId, details, TraceUtils.ExtensionVersion);
                }
            }
        }

        public void TracePartitionLoad(PartitionLoadInfo info)
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                logger.LogInformation("Part{partition:D2} Publishing LoadInfo WorkItems={workItems} Activities={activities} Timers={timers} Outbox={outbox} Wakeup={wakeup} ActivityLatencyMs={activityLatencyMs} WorkItemLatencyMs={workItemLatencyMs} WorkerId={workerId} LatencyTrend={latencyTrend} MissRate={missRate} InputQueuePosition={inputQueuePosition} CommitLogPosition={commitLogPosition}",
                    partitionId, info.WorkItems, info.Activities, info.Timers, info.Outbox, info.Wakeup, info.ActivityLatencyMs, info.WorkItemLatencyMs, info.WorkerId, info.LatencyTrend, info.MissRate, info.InputQueuePosition, info.CommitLogPosition);

                if (EtwSource.Log.IsEnabled())
                {
                    EtwSource.Log.PartitionLoadPublished(this.account, this.taskHub, partitionId, info.WorkItems, info.Activities, info.Timers, info.Outbox, info.Wakeup?.ToString("o") ?? "", info.ActivityLatencyMs, info.WorkItemLatencyMs, info.WorkerId, info.LatencyTrend, info.MissRate, info.InputQueuePosition, info.CommitLogPosition, TraceUtils.ExtensionVersion);
                }
            }
        }

        public void TraceWorkItemProgress(string workItemId, string instanceId, string format, params object[] args)
        {
            if (this.logLevelLimit <= LogLevel.Debug)
            {
                if (logger.IsEnabled(LogLevel.Debug))
                {
                    object[] objarray = new object[3 + args.Length];
                    objarray[0] = partitionId;
                    objarray[1] = workItemId;
                    objarray[2] = instanceId;
                    Array.Copy(args, 0, objarray, 3, args.Length);
                    logger.LogDebug("Part{partition:D2} OrchestrationWorkItem workItemId={workItemId} instanceId={instanceId} " + format, objarray);
                }
            }
        }
    }
}