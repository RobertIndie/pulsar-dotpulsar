/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace DotPulsar.Tests.Internal
{
    using DotPulsar.Internal;
    using DotPulsar.Internal.Abstractions;
    using DotPulsar.Internal.Events;
    using NSubstitute;
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Xunit;

    public class ConsumerProcessTests
    {
        [Fact]
        public async Task TestConsumerProcessStateManage_WhenSubConsumersStateChange_ThenParentConsumerStateChangeCorrectly()
        {
            var connectionPool = Substitute.For<IConnectionPool>();
            var establishNewChannel = Substitute.For<IEstablishNewChannel>();

            var processManager = new ProcessManager(connectionPool);

            // sub consumers guid
            var consumerGuids = new Dictionary<int, Guid>(3);

            // parent consumer related stuff
            var parentConsumerGuid = Guid.NewGuid();
            var partitionedStateManager = new StateManager<ConsumerState>(ConsumerState.Disconnected, ConsumerState.Closed, ConsumerState.ReachedEndOfTopic, ConsumerState.Faulted);
            var consumerProcess = new ConsumerProcess(parentConsumerGuid, partitionedStateManager, establishNewChannel, false);
            processManager.Add(consumerProcess);
            var consumer = new Consumer<string>(parentConsumerGuid, new Uri("pulsar://localhost:6650"), "", "", processManager, Substitute.For<IConsumerChannel<string>>(),
                Substitute.For<IExecute>(), partitionedStateManager, Substitute.For<IConsumerChannelFactory<string>>());

            for (var i = 0; i < 3; i++)
            {
                var stateManager = new StateManager<ConsumerState>(ConsumerState.Disconnected, ConsumerState.Closed, ConsumerState.ReachedEndOfTopic, ConsumerState.Faulted);
                var correlationId = Guid.NewGuid();

                var process = new ConsumerProcess(correlationId, stateManager, establishNewChannel, false, parentConsumerGuid);
                consumerGuids[i] = correlationId;
                var subConsumer = new SubConsumer<string>(correlationId, new Uri("pulsar://localhost:6650"), "", "", processManager, Substitute.For<IConsumerChannel<string>>(),
                    Substitute.For<IExecute>(), stateManager, Substitute.For<IConsumerChannelFactory<string>>());

                processManager.Add(process);
                processManager.Register(new AddSubConsumer(parentConsumerGuid, i, process, subConsumer));
            }

            // Because the state synchronization between parent and sub consumers is asynchronous,
            // We need to perform the await operation to wait for synchronization.
            var cts = new CancellationTokenSource();
            cts.CancelAfter(TimeSpan.FromSeconds(10));

            // Test connect
            Assert.Equal(ConsumerState.Disconnected, partitionedStateManager.CurrentState);

            processManager.Register(new ChannelActivated(consumerGuids[0]));
            await consumer.OnStateChangeTo(ConsumerState.PartiallyActive, cts.Token);
            Assert.Equal(ConsumerState.PartiallyActive, partitionedStateManager.CurrentState);

            processManager.Register(new ChannelConnected(consumerGuids[1]));
            await Task.Delay(100, cts.Token); // Delay for a short time to ensure that the state has not changed
            Assert.Equal(ConsumerState.PartiallyActive, partitionedStateManager.CurrentState);

            processManager.Register(new ChannelConnected(consumerGuids[2]));
            await consumer.OnStateChangeTo(ConsumerState.Active, cts.Token);
            Assert.Equal(ConsumerState.Active, partitionedStateManager.CurrentState);

            // Test disconnect
            processManager.Register(new ChannelDisconnected(consumerGuids[1]));
            await consumer.OnStateChangeTo(ConsumerState.PartiallyActive, cts.Token);
            Assert.Equal(ConsumerState.PartiallyActive, partitionedStateManager.CurrentState);

            // Test reconnect
            processManager.Register(new ChannelActivated(consumerGuids[1]));
            await consumer.OnStateChangeTo(ConsumerState.Active, cts.Token);
            Assert.Equal(ConsumerState.Active, partitionedStateManager.CurrentState);

            // Test fault
            processManager.Register(new ExecutorFaulted(consumerGuids[1]));
            await consumer.OnStateChangeTo(ConsumerState.Faulted, cts.Token);
            Assert.Equal(ConsumerState.Faulted, partitionedStateManager.CurrentState);
        }
    }
}
