using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace DurableTask.EventHubs
{
    internal interface IPartitionState
    {
        // ------ intialization ------

        Task<long> Restore(LocalOrchestrationService localPartition);

        // ------ reads and updates ------

        Task UpdateAsync(PartitionEvent evt);

        Task<TResult> ReadAsync<TResult>(Func<TResult> read);

        Task<TResult> ReadAsync<TArgument1, TResult>(Func<TArgument1, TResult> read, TArgument1 argument);

        // ------ tracked objects ------

        ClocksState Clocks { get; }

        OutboxState Outbox { get; }

        TimersState Timers { get; }

        ActivitiesState Activities { get; }

        SessionsState Sessions { get; }

        InstanceState GetInstance(string instanceId);
    }
}
