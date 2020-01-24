﻿//  ----------------------------------------------------------------------------------
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

namespace DurableTask.EventSourced.Tests
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Configuration;

    internal static class TestHelpers
    {
        public static EventSourcedOrchestrationService GetTestOrchestrationService()
        {
            var settings = new EventSourcedOrchestrationServiceSettings
            {
                EventHubsConnectionString = GetEventHubsConnectionString(),
                StorageConnectionString = GetStorageConnectionString(),
            };
            return new EventSourcedOrchestrationService(settings);
        }


        public static TestOrchestrationHost GetTestOrchestrationHost(
            bool enableExtendedSessions,
            int extendedSessionTimeoutInSeconds = 30)
        {
            var settings = new EventSourcedOrchestrationServiceSettings
            {
                EventHubsConnectionString = GetEventHubsConnectionString(),
                StorageConnectionString = GetStorageConnectionString(),
            };
            return new TestOrchestrationHost(settings);
        }

        public const string DurabeTaskTestPrefix = "DurableTaskTest";

        public static string GetStorageConnectionString()
        {
            return GetTestSetting("StorageConnectionString", true);
        }

        public static string GetEventHubsConnectionString()
        {
            // return "Memory:1";
            // return "Memory:4";
            // return "Memory:32";
            return "MemoryF:1";
            // return "MemoryF:8";
            //return "MemoryF:32";
            //return GetTestSetting("EventHubsConnectionString", false);
        }

        static string GetTestSetting(string name, bool require)
        {
            var setting =  Environment.GetEnvironmentVariable(DurabeTaskTestPrefix + name);

            if (require && string.IsNullOrEmpty(setting))
            {
                throw new ArgumentNullException($"The environment variable {DurabeTaskTestPrefix + name} must be defined for the tests to run");
            }

            return setting;
        }

     
    }
}
