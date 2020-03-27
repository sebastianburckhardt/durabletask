//  Copyright Microsoft Corporation
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  ----------------------------------------------------------------------------------
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.Text;
using System.Threading.Tasks;

namespace DurableTask.EventSourced.EventHubs
{
    internal class EventHubsTraceHelper : ILogger
    {
        private readonly ILogger logger;
        private readonly string account;
        private readonly string taskHub;
        private readonly string eventHubsNamespace;
        private readonly LogLevel etwLogLevel;

        public EventHubsTraceHelper(ILoggerFactory loggerFactory, LogLevel etwLogLevel, string storageAccountName, string taskHubName, string eventHubsNamespace)
        {
            this.logger = loggerFactory.CreateLogger("EventHubsTransport");
            this.account = storageAccountName;
            this.taskHub = taskHubName;
            this.eventHubsNamespace = eventHubsNamespace;
            this.etwLogLevel = etwLogLevel;
        }

        public bool IsEnabled(LogLevel logLevel) => 
            logger.IsEnabled(logLevel) || (logLevel >= this.etwLogLevel && IsEnabledForEtw(logLevel));
     
        private static bool IsEnabledForEtw(LogLevel logLevel)
        {
            switch (logLevel)
            {
                case LogLevel.Trace: 
                case LogLevel.Debug: 
                    return EtwSource.Log.IsEnabled(EventLevel.Verbose, EventKeywords.None);
                case LogLevel.Information:
                    return EtwSource.Log.IsEnabled(EventLevel.Informational, EventKeywords.None);
                case LogLevel.Warning:
                    return EtwSource.Log.IsEnabled(EventLevel.Warning, EventKeywords.None);
                case LogLevel.Error:
                case LogLevel.Critical:
                    return EtwSource.Log.IsEnabled(EventLevel.Error, EventKeywords.None);
                default:
                    return false;
            }
        }

        public IDisposable BeginScope<TState>(TState state) => NoopDisposable.Instance;

        private class NoopDisposable : IDisposable
        {
            public static NoopDisposable Instance = new NoopDisposable();
            public void Dispose()
            { }
        }

        public void Log<TState>(LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
            // always pass through to the ILogger
            this.logger.Log(logLevel, eventId, state, exception, formatter);

            // additionally, if configured, pass on to ETW
            if (this.etwLogLevel <= logLevel)
            {
                string details = formatter(state, exception);

                switch (logLevel)
                {
                    case LogLevel.Trace:
                        EtwSource.Log.EventHubsTrace(this.account, taskHub, this.eventHubsNamespace, details, TraceUtils.ExtensionVersion);
                        break;

                    case LogLevel.Debug:
                        EtwSource.Log.EventHubsDebug(this.account, taskHub, this.eventHubsNamespace, details, TraceUtils.ExtensionVersion);
                        break;

                    case LogLevel.Information:
                        EtwSource.Log.EventHubsInformation(this.account, taskHub, this.eventHubsNamespace, details, TraceUtils.ExtensionVersion);
                        break;

                    case LogLevel.Warning:
                        EtwSource.Log.EventHubsWarning(this.account, taskHub, this.eventHubsNamespace, details, TraceUtils.ExtensionVersion);
                        break;

                    case LogLevel.Error:
                    case LogLevel.Critical:
                        EtwSource.Log.EventHubsError(this.account, taskHub, this.eventHubsNamespace, details, TraceUtils.ExtensionVersion);
                        break;

                    default:
                        break;
                }
            }
        }
    }
}