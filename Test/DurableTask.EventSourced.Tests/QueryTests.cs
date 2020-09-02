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
using System.Diagnostics;
using System.Threading.Tasks;
using DurableTask.Core;
using Xunit;
using Xunit.Abstractions;
using System.Linq;
using System.Collections.Generic;

using TestTraceListener = DurableTask.EventSourced.Tests.PortedAzureScenarioTests.TestTraceListener;
using Orchestrations = DurableTask.EventSourced.Tests.PortedAzureScenarioTests.Orchestrations;
using Microsoft.Extensions.Logging;

namespace DurableTask.EventSourced.Tests
{
    // These tests are copied from AzureStorageScenarioTests
    [Collection("EventSourcedTests")]
    public partial class QueryTests : IClassFixture<TestFixture>, IDisposable
    {
        private readonly TestFixture fixture;
        private readonly TestOrchestrationHost host;
        private readonly TestTraceListener traceListener;

        public QueryTests(TestFixture fixture, ITestOutputHelper outputHelper)
        {
            this.fixture = fixture;
            this.host = fixture.Host;
            this.fixture.LoggerProvider.Output = outputHelper;
            this.traceListener = new TestTraceListener(outputHelper);
            Trace.Listeners.Add(this.traceListener);
        }

        public void Dispose() => Trace.Listeners.Remove(this.traceListener);

        /// <summary>
        /// Ported from AzureStorageScenarioTests
        /// </summary>
        [Fact]
        public async Task QueryOrchestrationInstancesByDateRange()
        {
            const int numInstances = 3;
            string getPrefix(int ii) => $"@inst{ii}@__";

            // Start multiple orchestrations. Use 1-based to avoid confusion where we use explicit values in asserts.
            for (var ii = 1; ii <= numInstances; ++ii)
            {
                var client = await host.StartOrchestrationAsync(typeof(Orchestrations.SayHelloInline), $"world {ii}", $"{getPrefix(ii)}__{Guid.NewGuid()}");
                await Task.Delay(100);  // To ensure time separation for the date time range queries
                await client.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
            }

            var results = await host.GetAllOrchestrationInstancesAsync();
            Assert.Equal(numInstances, results.Count);
            for (var ii = 1; ii <= numInstances; ++ii)
            {
                Assert.NotNull(results.SingleOrDefault(r => r.Output == $"\"Hello, world {ii}!\""));
            }

            // Select the middle instance for the time range query.
            var middleInstance = results.SingleOrDefault(r => r.Output == $"\"Hello, world 2!\"");
            void assertIsMiddleInstance(IList<OrchestrationState> testStates)
            {
                Assert.Equal(1, testStates.Count);
                Assert.Equal(testStates[0].OrchestrationInstance.InstanceId, middleInstance.OrchestrationInstance.InstanceId);
            }

            assertIsMiddleInstance(await host.GetOrchestrationStateAsync(CreatedTimeFrom: middleInstance.CreatedTime,
                                                                            CreatedTimeTo: middleInstance.CreatedTime.AddMilliseconds(50)));
            assertIsMiddleInstance(await host.GetOrchestrationStateAsync(InstanceIdPrefix: getPrefix(2)));
        }

        /// <summary>
        /// Validate query functions.
        /// </summary>
        [Fact]
        public async Task QueryOrchestrationInstanceByRuntimeStatus()
        {
            // Reuse counter as it provides a wait for the actor to complete itself.
            var client = await host.StartOrchestrationAsync(typeof(Orchestrations.Counter), 0);

            // Need to wait for the instance to start before sending events to it.
            // TODO: This requirement may not be ideal and should be revisited.
            await client.WaitForStartupAsync(TimeSpan.FromSeconds(10));

            // We should have one orchestration state
            var instanceStates = await host.GetAllOrchestrationInstancesAsync();
            Assert.Equal(1, instanceStates.Count);

            var inProgressStatus = new[] { OrchestrationStatus.Running, OrchestrationStatus.ContinuedAsNew };
            var completedStatus = new[] { OrchestrationStatus.Completed };

            async Task assertCounts(int running, int completed)
            {
                var runningStates = await host.GetOrchestrationStateAsync(RuntimeStatus: inProgressStatus);
                Assert.Equal(running, runningStates.Count);
                var completedStates = await host.GetOrchestrationStateAsync(RuntimeStatus: completedStatus);
                Assert.Equal(completed, completedStates.Count);
            }

            // Make sure the client and instance are still running and didn't complete early (or fail).
            var status = await client.GetStatusAsync();
            Assert.NotNull(status);
            Assert.Contains(status.OrchestrationStatus, inProgressStatus);
            await assertCounts(1, 0);

            // The end message will cause the actor to complete itself.
            await client.RaiseEventAsync(Orchestrations.Counter.OpEventName, Orchestrations.Counter.OpEnd);
            status = await client.WaitForCompletionAsync(TimeSpan.FromSeconds(10));

            // The client and instance should be Completed
            Assert.NotNull(status);
            Assert.Contains(status.OrchestrationStatus, completedStatus);
            await assertCounts(0, 1);
        }

        [Fact]
        public async void NoInstancesCreated()
        {
            var instanceStates = await host.GetAllOrchestrationInstancesAsync();
            Assert.Equal(0, instanceStates.Count);
        }
    }

    [Collection("EventSourcedTests")]
    public partial class NonFixtureQueryTests : IDisposable
    {
        private readonly TestTraceListener traceListener;
        private readonly ILoggerFactory loggerFactory;
        private readonly XunitLoggerProvider provider;

        public NonFixtureQueryTests(ITestOutputHelper outputHelper)
        {
            this.traceListener = new TestTraceListener(outputHelper);
            loggerFactory = new LoggerFactory();
            this.provider = new XunitLoggerProvider(outputHelper);
            loggerFactory.AddProvider(this.provider);
            lock (this.provider)
            {
                Trace.Listeners.Add(this.traceListener);
            }
        }

        public void Dispose()
        {
            lock (this.provider)
            {
                Trace.Listeners.Remove(this.traceListener);
            }
        }

        /// <summary>
        /// This exercises what LinqPAD queries do.
        /// </summary>
        [Fact]
        public async void SingleServiceQuery()
        {
            var settings = new EventSourcedOrchestrationServiceSettings()
            {
                EventHubsConnectionString = TestHelpers.GetEventHubsConnectionString(),
                StorageConnectionString = TestHelpers.GetStorageConnectionString(),
                HubName = TestHelpers.GetTestTaskHubName()
            };

            var service = new EventSourcedOrchestrationService(settings, loggerFactory);
            await service.CreateAsync(true);
            await service.StartAsync();
            var states = await service.GetOrchestrationStateAsync();
            await service.StopAsync();
            Assert.Equal(0, states.Count);
        }
    }
}
