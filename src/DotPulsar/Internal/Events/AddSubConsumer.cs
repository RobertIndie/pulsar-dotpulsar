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

namespace DotPulsar.Internal.Events
{
    using Abstractions;
    using DotPulsar.Abstractions;
    using System;

    /// <summary>
    /// Representation of the partitions count of the partitioned topic updating.
    /// </summary>
    public sealed class AddSubConsumer : IEvent
    {
        public Guid CorrelationId { get; }

        public int PartitionId { get; }

        public ConsumerProcess ConsumerProcess { get; }

        public IConsumer Consumer { get; }

        public AddSubConsumer(Guid correlationId, int partitionId, ConsumerProcess consumerProcess, IConsumer consumer)
        {
            CorrelationId = correlationId;
            PartitionId = partitionId;
            ConsumerProcess = consumerProcess;
            Consumer = consumer;
        }
    }
}
