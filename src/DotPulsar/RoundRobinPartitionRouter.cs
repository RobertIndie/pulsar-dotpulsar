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

namespace DotPulsar
{
    using DotPulsar.Abstractions;
    using DotPulsar.Internal;
    using System.Threading;

    public class RoundRobinPartitionRouter : IMessageRouter
    {
        private long _partitionIndex = -1;
        public int ChoosePartition(MessageMetadata? message, PartitionedTopicMetadata partitionedTopic)
        {
            if (message != null && !string.IsNullOrEmpty(message.Key))
            {
                return MathUtils.SignSafeMod(Murmur3_32Hash.Instance.MakeHash(message.Key!), partitionedTopic.Partitions);
            }
            return MathUtils.SignSafeMod(Interlocked.Increment(ref _partitionIndex), partitionedTopic.Partitions);
        }
    }
}
