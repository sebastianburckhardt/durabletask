using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DurableTask.Core;

namespace DurableTask.EventHubs
{
    internal class OrchestrationWorkItem : TaskOrchestrationWorkItem
    {
        public long SessionId;

        public long StartPosition;

        public LocalOrchestrationService LocalPartition;
   
        public static void EnqueueWorkItem(LocalOrchestrationService localPartition, string instanceId, SessionsState.Session session)
        {
            var workItem = new OrchestrationWorkItem()
            {
                SessionId = session.SessionId,
                StartPosition = session.BatchStartPosition,
                LocalPartition = localPartition,
                InstanceId = instanceId,
                LockedUntilUtc = DateTime.MaxValue,
                Session = null,
                NewMessages = session.Batch.ToList(), // make a copy
            };

            Task.Run(workItem.LoadAsync);
        }

        public async Task LoadAsync()
        {
            var r = (Func<OrchestrationRuntimeState>)LocalPartition.State.GetInstance(this.InstanceId).GetRuntimeState;

            // load the runtime state
            this.OrchestrationRuntimeState = await LocalPartition.State.ReadAsync(
                LocalPartition.State.GetInstance(this.InstanceId).GetRuntimeState);

            // the work item is ready to process
            LocalPartition.OrchestrationWorkItemQueue.Add(this);
        }
    }
}
