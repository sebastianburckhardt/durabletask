//  ----------------------------------------------------------------------------------
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

using System;
using DurableTask.EventSourced.Faster;
using Microsoft.Extensions.Logging;

namespace DurableTask.EventSourced.Tests
{
    internal static class TestHelpers
    {
        public static EventSourcedOrchestrationServiceSettings GetEventSourcedOrchestrationServiceSettings()
        {
            return new EventSourcedOrchestrationServiceSettings
            {
                EventHubsConnectionString = GetEventHubsConnectionString(),
                StorageConnectionString = GetStorageConnectionString(),
                TaskHubName = GetTestTaskHubName(),
                TakeStateCheckpointWhenStoppingPartition = true,  // set to false for testing recovery from log
                //MaxNumberBytesBetweenCheckpoints = 10000000, // set this low for testing frequent checkpointing
                //MaxNumberEventsBetweenCheckpoints = 10, // set this low for testing frequent checkpointing
            };
        }

        public static EventSourcedOrchestrationService GetTestOrchestrationService(ILoggerFactory loggerFactory)
        {
            return new EventSourcedOrchestrationService(GetEventSourcedOrchestrationServiceSettings(), loggerFactory);
        }

        public static TestOrchestrationHost GetTestOrchestrationHost(
            ILoggerFactory loggerFactory)
        {
            var settings = GetEventSourcedOrchestrationServiceSettings();
            return new TestOrchestrationHost(settings, loggerFactory);
        }

        public static string GetTestTaskHubName()
        {
            return "test-taskhub";
            //Configuration appConfig = ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None);
            //return appConfig.AppSettings.Settings["TaskHubName"].Value;
        }

        public const string DurableTaskTestPrefix = "DurableTaskTest";

        public static bool DeleteStorageBeforeRunningTests => false; // set to false for testing log-based recovery

        public static string GetAzureStorageConnectionString() => GetTestSetting("StorageConnectionString", true);

        public static string GetStorageConnectionString()
        {
            // NOTE: If using the local file system, modify GetEventHubsConnectionString use one of the memory options.
            // return FasterStorage.UseLocalFileStorage;

            return GetAzureStorageConnectionString();
        }

        public static string GetEventHubsConnectionString()
        {
            // NOTE: If using any of the memory options, modify GetStorageConnectionString use the local file system.
            // return "Memory:1";
            // return "Memory:4";
            // return "Memory:32";
            // return "MemoryF:1";
            // return "MemoryF:4";
            // return "MemoryF:32";

            return GetTestSetting("EventHubsConnectionString", false);
        }

        static string GetTestSetting(string name, bool require)
        {
            var setting =  Environment.GetEnvironmentVariable(DurableTaskTestPrefix + name);

            if (require && string.IsNullOrEmpty(setting))
            {
                throw new ArgumentNullException($"The environment variable {DurableTaskTestPrefix + name} must be defined for the tests to run");
            }

            return setting;
        }
    }
}
