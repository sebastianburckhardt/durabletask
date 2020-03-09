using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventSourced
{
    class TaskHelpers
    {
        public static async Task WaitForCancellationAsync(CancellationToken token)
        {
            try
            {
                await Task.Delay(-1, token);
            }
            catch (OperationCanceledException)
            {
            }
        }
    }
}
