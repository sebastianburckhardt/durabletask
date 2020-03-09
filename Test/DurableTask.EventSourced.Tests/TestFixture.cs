using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit.Abstractions;

namespace DurableTask.EventSourced.Tests
{
    public class TestFixture : IDisposable
    {
        public TestFixture()
        {
            LoggerFactory = new LoggerFactory();
            LoggerProvider = new XunitLoggerProvider();
            LoggerFactory.AddProvider(LoggerProvider);
            this.Host = TestHelpers.GetTestOrchestrationHost(LoggerFactory);
            this.Host.StartAsync().Wait();
        }

        public void Dispose()
        {
            LoggerProvider.Output = null;
            this.Host.StopAsync().Wait();
            this.Host.Dispose();
        }

        internal TestOrchestrationHost Host { get; private set; }

        internal XunitLoggerProvider LoggerProvider { get; private set; }

        internal ILoggerFactory LoggerFactory { get; private set; }

    }
}
