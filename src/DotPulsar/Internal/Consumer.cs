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

namespace DotPulsar.Internal
{
    using Abstractions;
    using DotPulsar.Abstractions;
    using Events;
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    public sealed class Consumer<TMessage> : IEstablishNewChannel, IConsumer<TMessage>
    {
        private readonly Guid _correlationId;
        private readonly IRegisterEvent _eventRegister;
        private readonly IExecute _executor;
        private readonly IStateChanged<ConsumerState> _state;
        private readonly ConsumerOptions<TMessage> _options;
        private readonly PulsarClient _pulsarClient;
        private readonly ConcurrentDictionary<int, IConsumer<TMessage>> _consumers;
        private readonly CancellationTokenSource _cts = new();
        private int _consumersCount;
        private int _isDisposed;

        public Uri ServiceUrl { get; }
        public string SubscriptionName { get; }
        public string Topic { get; }

        public Consumer(
            Guid correlationId,
            Uri serviceUrl,
            string subscriptionName,
            string topic,
            IRegisterEvent eventRegister,
            IExecute executor,
            IStateChanged<ConsumerState> state,
            ConsumerOptions<TMessage> options,
            PulsarClient pulsarClient)
        {
            _correlationId = correlationId;
            ServiceUrl = serviceUrl;
            SubscriptionName = subscriptionName;
            Topic = topic;
            _eventRegister = eventRegister;
            _executor = executor;
            _state = state;
            _options = options;
            _pulsarClient = pulsarClient;
            _isDisposed = 0;

            _consumers = new ConcurrentDictionary<int, IConsumer<TMessage>>(1, 31);
        }

        private void CreateSubConsumers(int startIndex, int count)
        {
            if (count == 0)
            {
                var consumer = _pulsarClient.NewSubConsumer(Topic, _options, _executor, _correlationId);
                _consumers[0] = consumer;
                return;
            }

            for (var i = startIndex; i < count; ++i)
            {
                var consumer = _pulsarClient.NewSubConsumer(Topic, _options, _executor, _correlationId, (uint) i);
                _consumers[i] = consumer;
            }
        }

        private async void UpdatePartitions(CancellationToken cancellationToken)
        {
            var partitionsCount = (int) await _pulsarClient.GetNumberOfPartitions(Topic, cancellationToken).ConfigureAwait(false);
            _eventRegister.Register(new UpdatePartitions(_correlationId, (uint) partitionsCount));
            CreateSubConsumers(_consumers.Count, partitionsCount);
            _consumersCount = partitionsCount;
        }

        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
                return;

            _cts.Cancel();
            _cts.Dispose();

            foreach (var consumer in _consumers.Values)
            {
                await consumer.DisposeAsync().ConfigureAwait(false);
            }
        }

        public async Task EstablishNewChannel(CancellationToken cancellationToken)
        {
            await _executor.Execute(() => UpdatePartitions(cancellationToken), cancellationToken).ConfigureAwait(false);
        }

        public ValueTask<MessageId> GetLastMessageId(CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        private static bool IsIllegalMultiTopicsMessageId(MessageId messageId)
        {
            //only support earliest/latest
            return !MessageId.Earliest.Equals(messageId) && !MessageId.Latest.Equals(messageId);
        }

        public async ValueTask Seek(MessageId messageId, CancellationToken cancellationToken = default)
            => await _executor.Execute(() =>
            {
                var tasks = new List<Task>();

                if (messageId == null || IsIllegalMultiTopicsMessageId(messageId))
                {
                    throw new ArgumentException("Illegal messageId, messageId can only be earliest/latest.");
                }

                _consumers.Values.ToList().ForEach(consumer =>
                {
                    var task = consumer.Seek(messageId, cancellationToken).AsTask();
                    task.ConfigureAwait(false);
                    tasks.Add(task);
                });
                Task.WaitAll(tasks.ToArray());
            }, cancellationToken).ConfigureAwait(false);

        public async ValueTask Seek(ulong publishTime, CancellationToken cancellationToken = default)
            => await _executor.Execute(() =>
            {
                var tasks = new List<Task>();

                _consumers.Values.ToList().ForEach(consumer =>
                {
                    var task = consumer.Seek(publishTime, cancellationToken).AsTask();
                    task.ConfigureAwait(false);
                    tasks.Add(task);
                });
                Task.WaitAll(tasks.ToArray());
            }, cancellationToken).ConfigureAwait(false);

        public bool IsFinalState()
            => _state.IsFinalState();

        public bool IsFinalState(ConsumerState state)
            => _state.IsFinalState(state);

        public async ValueTask<ConsumerState> OnStateChangeTo(ConsumerState state, CancellationToken cancellationToken)
            => await _state.StateChangedTo(state, cancellationToken).ConfigureAwait(false);

        public async ValueTask<ConsumerState> OnStateChangeFrom(ConsumerState state, CancellationToken cancellationToken)
            => await _state.StateChangedFrom(state, cancellationToken).ConfigureAwait(false);

        public async ValueTask Acknowledge(MessageId messageId, CancellationToken cancellationToken = default)
            => await _executor.Execute((() => _consumers[messageId.Partition].Acknowledge(messageId, cancellationToken)), cancellationToken).ConfigureAwait(false);

        public async ValueTask AcknowledgeCumulative(MessageId messageId, CancellationToken cancellationToken = default)
            => await _executor.Execute((() => _consumers[messageId.Partition].AcknowledgeCumulative(messageId, cancellationToken)), cancellationToken).ConfigureAwait(false);

        public async ValueTask Unsubscribe(CancellationToken cancellationToken = default)
            => await _executor.Execute(() =>
            {
                var tasks = new List<Task>();

                _consumers.Values.ToList().ForEach(consumer =>
                {
                    var task = consumer.Unsubscribe(cancellationToken).AsTask();
                    task.ConfigureAwait(false);
                    tasks.Add(task);
                });
                Task.WaitAll(tasks.ToArray());
            }, cancellationToken).ConfigureAwait(false);

        public async ValueTask RedeliverUnacknowledgedMessages(IEnumerable<MessageId> messageIds, CancellationToken cancellationToken = default)
            => await _executor.Execute(() =>
            {
                var tasks = new List<Task>();

                messageIds.ToList().GroupBy()ForEach(messageId =>
                {
                    var task = _consumers[messageId.Partition].RedeliverUnacknowledgedMessages(cancellationToken).AsTask();
                    task.ConfigureAwait(false);
                    tasks.Add(task);
                });
                Task.WaitAll(tasks.ToArray());
            }, cancellationToken).ConfigureAwait(false);

        public ValueTask RedeliverUnacknowledgedMessages(CancellationToken cancellationToken = default)
            => await _executor.Execute(() =>
            {
                var tasks = new List<Task>();

                _consumers.Values.ToList().ForEach(consumer =>
                {
                    var task = consumer.RedeliverUnacknowledgedMessages(cancellationToken).AsTask();
                    task.ConfigureAwait(false);
                    tasks.Add(task);
                });
                Task.WaitAll(tasks.ToArray());
            }, cancellationToken).ConfigureAwait(false);

        public ValueTask<IMessage<TMessage>> Receive(CancellationToken cancellationToken = default)
            => throw new NotImplementedException();
    }
}
