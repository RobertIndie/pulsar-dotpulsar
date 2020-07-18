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

namespace DotPulsar.Internal
{
    using DotPulsar.Abstractions;
    using DotPulsar.Internal.Abstractions;
    using DotPulsar.Internal.Events;
    using System;
    using System.Buffers;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    public sealed class PartitionedProducer : IProducer
    {
        private readonly ConcurrentDictionary<int, IProducer> _producers;
        private readonly Guid _correlationId;
        private readonly IRegisterEvent _eventRegister;
        private readonly IExecute _executor;
        private readonly StateManager<ProducerState> _state;
        private readonly PartitionedTopicMetadata _partitionedTopicMetadata;
        private int _isDisposed;
        private ConcurrentBag<Task> _monitorStateTask;
        private CancellationTokenSource _cancellationTokenSource;
        private int _connectedProducerCount = 0;

        public string Topic { get; }

        public PartitionedProducer(
            Guid correlationId,
            string topic,
            IRegisterEvent registerEvent,
            IExecute executor,
            StateManager<ProducerState> state,
            PartitionedTopicMetadata partitionedTopicMetadata,
            ConcurrentDictionary<int, IProducer> producers)
        {
            _correlationId = correlationId;
            Topic = topic;
            _eventRegister = registerEvent;
            _executor = executor;
            _state = state;
            _partitionedTopicMetadata = partitionedTopicMetadata;
            _producers = producers;
            _isDisposed = 0;

            _cancellationTokenSource = new CancellationTokenSource();
            _monitorStateTask = new ConcurrentBag<Task>();

            //_eventRegister.Register(new PartitionedProducerCreated(_correlationId, this));
            foreach (var producer in _producers.Values)
            {
                _monitorStateTask.Add(MonitorState(producer, _cancellationTokenSource.Token));
            }
        }

        private async Task MonitorState(IProducer producer, CancellationToken cancellationToken = default)
        {
            await Task.Yield();

            var state = ProducerState.Disconnected;

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    var stateChanged = await producer.StateChangedFrom(state).ConfigureAwait(false);
                    state = stateChanged.ProducerState;

                    switch (state)
                    {
                        case ProducerState.Disconnected:
                            Interlocked.Decrement(ref _connectedProducerCount);
                            _state.SetState(ProducerState.Disconnected);
                            break;
                        case ProducerState.Faulted:
                            Interlocked.Decrement(ref _connectedProducerCount);
                            _state.SetState(ProducerState.Faulted);
                            break;
                        case ProducerState.Closed:
                            Interlocked.Decrement(ref _connectedProducerCount);
                            _state.SetState(ProducerState.Closed);
                            break;
                        case ProducerState.Connected:
                            Interlocked.Increment(ref _connectedProducerCount);
                            break;
                    }

                    if (_connectedProducerCount == _partitionedTopicMetadata.Partitions)
                        _state.SetState(ProducerState.Connected);

                    if (IsFinalState(state))
                        _cancellationTokenSource.Cancel(); // cancel other monitor tasks

                    if (producer.IsFinalState(state))
                        return;
                }
            }
            catch (OperationCanceledException)
            { }
        }

        private IProducer GetProducer()
        {
            if (_producers.Count > 0) return _producers[0];
            return null;
        }

        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
                return;

            _cancellationTokenSource.Cancel();
            _cancellationTokenSource.Dispose();
            //_eventRegister.Register(new PartitionedProducerDisposed(_correlationId, this));
        }

        public bool IsFinalState()
            => _state.IsFinalState();

        public bool IsFinalState(ProducerState state)
            => _state.IsFinalState(state);

        public ValueTask<MessageId> Send(byte[] data, CancellationToken cancellationToken)
            => GetProducer().Send(new ReadOnlySequence<byte>(data), cancellationToken);

        public ValueTask<MessageId> Send(ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
            => GetProducer().Send(new ReadOnlySequence<byte>(data), cancellationToken);

        public ValueTask<MessageId> Send(ReadOnlySequence<byte> data, CancellationToken cancellationToken)
            => GetProducer().Send(data, cancellationToken);

        public ValueTask<MessageId> Send(MessageMetadata metadata, byte[] data, CancellationToken cancellationToken)
            => GetProducer().Send(metadata, new ReadOnlySequence<byte>(data), cancellationToken);

        public ValueTask<MessageId> Send(MessageMetadata metadata, ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
            => GetProducer().Send(metadata, new ReadOnlySequence<byte>(data), cancellationToken);

        public ValueTask<MessageId> Send(MessageMetadata metadata, ReadOnlySequence<byte> data, CancellationToken cancellationToken)
            => GetProducer().Send(metadata, data, cancellationToken);

        public async ValueTask<ProducerStateChanged> StateChangedFrom(ProducerState state, CancellationToken cancellationToken = default)
        {
            var newState = await _state.StateChangedFrom(state, cancellationToken).ConfigureAwait(false);
            return new ProducerStateChanged(this, newState);
        }

        public async ValueTask<ProducerStateChanged> StateChangedTo(ProducerState state, CancellationToken cancellationToken = default)
        {
            var newState = await _state.StateChangedTo(state, cancellationToken).ConfigureAwait(false);
            return new ProducerStateChanged(this, newState);
        }
    }
}
