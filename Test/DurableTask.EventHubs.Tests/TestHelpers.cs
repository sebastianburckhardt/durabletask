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

namespace DurableTask.EventHubs.Tests
{
    using System.Threading.Tasks;
    using Microsoft.Extensions.Configuration;

    public static class TestHelpers
    {
        static EventHubsTestConfig config;

        public static EventHubsOrchestrationService GetTestOrchestrationService(string taskHub = null)
        {
            EventHubsTestConfig testConfig = GetRedisTestConfig();
            var settings = new EventHubsOrchestrationServiceSettings
            {
                //RedisConnectionString = testConfig.RedisConnectionString,
                TaskHubName = taskHub ?? "TaskHub"
            };

            return new EventHubsOrchestrationService(settings);
        }

    

        private static EventHubsTestConfig GetRedisTestConfig()
        {
            if (config == null)
            {
                config = new EventHubsTestConfig();
                IConfigurationRoot root = new ConfigurationBuilder()
                    .SetBasePath(System.IO.Directory.GetCurrentDirectory())
                    .AddJsonFile("appsettings.json", optional: true)
                    .Build();
                root.Bind(config);
            }
            return config;
        }

        public class EventHubsTestConfig
        {
            public string RedisConnectionString { get; set; }
        }
    }
}
