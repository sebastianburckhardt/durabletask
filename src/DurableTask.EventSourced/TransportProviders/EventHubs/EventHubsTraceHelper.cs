﻿//  Copyright Microsoft Corporation
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

namespace DurableTask.EventSourced.EventHubs
{
    /// <summary>
    /// Trace helpers for the event hubs transport.
    /// </summary>
    internal class EventHubsTraceHelper : ILogger
    {
        private readonly ILogger logger;
        private readonly string account;
        private readonly string taskHub;
        private readonly string eventHubsNamespace;
        private readonly LogLevel logLevelLimit;

        public EventHubsTraceHelper(ILoggerFactory loggerFactory, LogLevel logLevelLimit, string storageAccountName, string taskHubName, string eventHubsNamespace)
        {
            this.logger = loggerFactory.CreateLogger($"{EventSourcedOrchestrationService.LoggerCategoryName}.EventHubsTransport");
            this.account = storageAccountName;
            this.taskHub = taskHubName;
            this.eventHubsNamespace = eventHubsNamespace;
            this.logLevelLimit = logLevelLimit;
        }

        public bool IsEnabled(LogLevel logLevel) => logLevel >= this.logLevelLimit;
     
        public IDisposable BeginScope<TState>(TState state) => NoopDisposable.Instance;

        private class NoopDisposable : IDisposable
        {
            public static NoopDisposable Instance = new NoopDisposable();
            public void Dispose()
            { }
        }

        public void Log<TState>(LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
            // quit if not enabled
            if (this.logLevelLimit <= logLevel)
            {
                // pass through to the ILogger
                this.logger.Log(logLevel, eventId, state, exception, formatter);

                // additionally, if etw is enabled, pass on to ETW   
                if (EtwSource.Log.IsEnabled())
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
}