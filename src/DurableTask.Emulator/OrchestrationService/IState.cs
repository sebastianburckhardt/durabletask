using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace DurableTask.Emulator
{
    internal interface IState
    {
        // ------ intialization ------

        Task RestoreAsync(LocalPartition localPartition);

        // ------ reads and updates ------

        Task UpdateAsync(ProcessorEvent evt);

        Task<TResult> ReadAsync<TResult>(Func<TResult> read);

        // ------ tracked objects ------

        ClocksState Clocks { get; }

        OutboxState Outbox { get; }

        TimersState Timers { get; }

        ActivitiesState Activities { get; }

        SessionsState Sessions { get; }

        InstanceState GetInstance(string instanceId);
    }
}
