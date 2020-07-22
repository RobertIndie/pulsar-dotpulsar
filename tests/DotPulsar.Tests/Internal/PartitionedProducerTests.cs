﻿/*
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
    using DotPulsar.Abstractions;
    using DotPulsar.Internal;
    using Moq;
    using System;
    using System.Collections.Concurrent;
    using System.Threading;
    using Xunit;
    public class PartitionedProducerTests
    {
        [Fact]
        public async void StateChangedFrom_SetupPartitionedProducer_ShouldConnectAllPartitionTopic()
        {
            var producers = new ConcurrentDictionary<int, IProducer>();
            var producerMock = new Mock<IProducer>();
            IProducer producer = producerMock.Object;
            _ = producerMock.Setup(m => m.StateChangedFrom(ProducerState.Disconnected, It.IsAny<CancellationToken>()))
                .ReturnsAsync(new ProducerStateChanged(producer, ProducerState.Connected));
            for (int i = 0; i < 3; i++)
            {
                producers[i] = producerMock.Object;
            }

            var stateManager = new StateManager<ProducerState>(ProducerState.Disconnected, ProducerState.Closed, ProducerState.Faulted);
            var partitionedProducer = new PartitionedProducer(Guid.NewGuid(), "", null, null, stateManager, new PartitionedTopicMetadata(3), producers, new RoundRobinPartitionRouter());
            var stateChanged = await partitionedProducer.StateChangedFrom(ProducerState.Disconnected).ConfigureAwait(false);

            Assert.True(stateChanged.ProducerState == ProducerState.Connected);
        }
    }
}
