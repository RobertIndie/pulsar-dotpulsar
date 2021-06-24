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
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    public sealed class Consumer<TMessage> : IEstablishNewChannel, IConsumer<TMessage>
    {
        private readonly Guid _correlationId;
        private readonly IRegisterEvent _eventRegister;
        private readonly IExecute _executor;
        private readonly IStateChanged<ConsumerState> _state;
        private readonly IConsumerChannelFactory<TMessage> _factory;
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
            IConsumerChannel<TMessage> initialChannel,
            IExecute executor,
            IStateChanged<ConsumerState> state,
            IConsumerChannelFactory<TMessage> factory)
        {
            _correlationId = correlationId;
            ServiceUrl = serviceUrl;
            SubscriptionName = subscriptionName;
            Topic = topic;
            _eventRegister = eventRegister;
            _executor = executor;
            _state = state;
            _factory = factory;
            _isDisposed = 0;

            _eventRegister.Register(new ConsumerCreated(_correlationId));
        }

        public ValueTask DisposeAsync()
            => throw new NotImplementedException();

        public Task EstablishNewChannel(CancellationToken cancellationToken)
            => throw new NotImplementedException();

        public ValueTask<MessageId> GetLastMessageId(CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public ValueTask Seek(MessageId messageId, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public ValueTask Seek(ulong publishTime, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public bool IsFinalState()
            => throw new NotImplementedException();

        public bool IsFinalState(ConsumerState state)
            => throw new NotImplementedException();

        public async ValueTask<ConsumerState> OnStateChangeTo(ConsumerState state, CancellationToken cancellationToken)
            => await _state.StateChangedTo(state, cancellationToken).ConfigureAwait(false);

        public async ValueTask<ConsumerState> OnStateChangeFrom(ConsumerState state, CancellationToken cancellationToken)
            => await _state.StateChangedFrom(state, cancellationToken).ConfigureAwait(false);

        public ValueTask Acknowledge(MessageId messageId, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public ValueTask AcknowledgeCumulative(MessageId messageId, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public ValueTask Unsubscribe(CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public ValueTask RedeliverUnacknowledgedMessages(IEnumerable<MessageId> messageIds, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public ValueTask RedeliverUnacknowledgedMessages(CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public ValueTask<IMessage<TMessage>> Receive(CancellationToken cancellationToken = default)
            => throw new NotImplementedException();
    }
}
