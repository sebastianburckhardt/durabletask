using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventSourced
{
    class TaskHelpers
    {
        // simple helpers, but should only be used if there are few calls of this until token is cancelled
        // otherwise we may leak memory since the tasks don't get GC'd

        public static async Task WaitForCancellationAsync(CancellationToken token)
        {
            try
            {
                await Task.Delay(-1, token); // TODO check for memory leak
            }
            catch (OperationCanceledException)
            {
            
            }
        }

        public static async Task<T> WaitForTaskOrCancellation<T>(Task<T> task, CancellationToken token)
        {
            var first = await Task.WhenAny(task, Task.Delay(-1, token));
            if (first == task)
            {
                return await ((Task<T>)first);
            }
            else
            {
                throw new OperationCanceledException("the token was canceled before task completed", token);
            }
        }
    }
}
