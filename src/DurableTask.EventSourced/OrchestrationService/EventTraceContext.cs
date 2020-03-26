using System;
using System.Collections.Generic;
using System.Text;

namespace DurableTask.EventSourced
{
    internal static class EventTraceContext
    {
        [ThreadStatic]
        private static (long commitLogPosition, string eventId) context;

        private static readonly TraceContextClear traceContextClear = new TraceContextClear();

        public static IDisposable MakeContext(long commitLogPosition, string eventId)
        {
            EventTraceContext.context = (commitLogPosition, eventId);
            return traceContextClear;
        }

        public static (long commitLogPosition, string eventId) Current => EventTraceContext.context;

        private class TraceContextClear : IDisposable
        {
            public void Dispose()
            {
                EventTraceContext.context = (0L, null);
            }
        }

        public static void Clear()
        {
            EventTraceContext.context = (0L, null);
        }
    }
}
