using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit.Abstractions;

namespace DurableTask.EventSourced.Tests
{
    public class XunitLoggerProvider : ILoggerProvider
    {
        public ITestOutputHelper Output { get; set; }

        public XunitLoggerProvider(ITestOutputHelper testOutputHelper = null)
        {
            Output = testOutputHelper;
        }

        public ILogger CreateLogger(string categoryName)
            => new XunitLogger(this, categoryName);

        public void Dispose()
        { }

        public class XunitLogger : ILogger
        {
            private readonly XunitLoggerProvider provider;
            private readonly string categoryName;

            public XunitLogger(XunitLoggerProvider provider, string categoryName)
            {
                this.provider = provider;
                this.categoryName = categoryName;
            }

            public IDisposable BeginScope<TState>(TState state)
                => NoopDisposable.Instance;

            public bool IsEnabled(LogLevel logLevel)
                => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
            {
                this.provider.Output?.WriteLine($"{formatter(state, exception)}");
                if (exception != null)
                    this.provider.Output?.WriteLine(exception.ToString());
            }

            private class NoopDisposable : IDisposable
            {
                public static NoopDisposable Instance = new NoopDisposable();
                public void Dispose()
                { }
            }
        }
    }
}
