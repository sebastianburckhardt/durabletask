using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;

namespace DurableTask.EventSourced
{
    internal class ClientTraceHelper
    {
        private readonly ILogger logger;
        private readonly string account;
        private readonly string taskHub;
        private readonly Guid clientId;
        private readonly LogLevel etwLogLevel;
        private readonly string tracePrefix;

        public ClientTraceHelper(ILogger logger, LogLevel etwLogLevel, string storageAccountName, string taskHubName, Guid clientId)
        {
            this.logger = logger;
            this.account = storageAccountName;
            this.taskHub = taskHubName;
            this.clientId = clientId;
            this.etwLogLevel = etwLogLevel;
            this.tracePrefix = $"Client.{Client.GetShortId(clientId)}";
        }

        public void TraceProgress(string details)
        {
            if (this.logger.IsEnabled(LogLevel.Information))
            {
                this.logger.LogError("{client} {details}", this.tracePrefix, details);
            }
            if (this.etwLogLevel <= LogLevel.Information)
            {
                EtwSource.Log.ClientProgress(this.account, this.taskHub, this.clientId, details, TraceUtils.ExtensionVersion);
            }
        }

        public void TraceError(string context, string message, Exception exception)
        {
            if (this.logger.IsEnabled(LogLevel.Error))
            {
                this.logger.LogError("{client} !!! {message}: {exception}", this.tracePrefix, message, exception);
            }
            if (this.etwLogLevel <= LogLevel.Error)
            {
                EtwSource.Log.ClientError(this.account, this.taskHub, this.clientId, context, message, exception.ToString(), TraceUtils.ExtensionVersion);
            }
        }

        public void TraceSend(Event @event)
        {
            if (this.logger.IsEnabled(LogLevel.Debug))
            {
                this.logger.LogTrace("{client} Sending event {eventId}: {event}", this.tracePrefix, @event.EventIdString, @event);
            }
            if (this.etwLogLevel <= LogLevel.Debug)
            {
                EtwSource.Log.ClientEventSent(this.account, this.taskHub, this.clientId, @event.EventIdString, @event.ToString(), TraceUtils.ExtensionVersion);
            }
        }

        public void TraceReceive(Event @event)
        {
            if (this.logger.IsEnabled(LogLevel.Debug))
            {
                this.logger.LogTrace("{client} Processing event {eventId}: {event}", this.tracePrefix, @event.EventIdString, @event);
            }
            if (this.etwLogLevel <= LogLevel.Debug)
            {
                EtwSource.Log.ClientEventReceived(this.account, this.taskHub, this.clientId, @event.EventIdString, @event.ToString(), TraceUtils.ExtensionVersion);
            }
        }

    }
}