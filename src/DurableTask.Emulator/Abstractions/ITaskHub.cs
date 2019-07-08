﻿using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace DurableTask.Emulator
{
    interface ITaskHub
    {
        Task<bool> ExistsAsync();

        Task CreateAsync();

        Task DeleteAsync();

        IPartitionState State { get; }

        IPartitionedQueue Queue { get; }
    }
}
