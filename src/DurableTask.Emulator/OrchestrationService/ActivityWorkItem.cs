using System;
using System.Collections.Generic;
using System.Text;
using DurableTask.Core;

namespace DurableTask.EventHubs
{
    internal class ActivityWorkItem : TaskActivityWorkItem
    {
        public long ActivityId;


        public ActivityWorkItem(long activityId, TaskMessage message)
        {
            this.ActivityId = activityId;
            this.Id = activityId.ToString();
            this.LockedUntilUtc = DateTime.MaxValue;
            this.TaskMessage = message;
        }
    }
}
