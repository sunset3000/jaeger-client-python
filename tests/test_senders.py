# Copyright (c) 2018 Uber Technologies, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import print_function

import sys
import time
import socket
import collections

import mock
import pytest
from tornado import ioloop
from tornado.testing import AsyncTestCase, gen_test

from jaeger_client import senders, thrift
from jaeger_client import Span, SpanContext
from jaeger_client.local_agent_net import LocalAgentSender
from jaeger_client.thrift_gen.agent import Agent
from jaeger_client.thrift_gen.jaeger import ttypes
from thrift.protocol import TCompactProtocol


def test_base_sender_create_io_loop_if_not_provided():

    sender = senders.Sender()

    assert sender._io_loop is not None
    assert isinstance(sender._io_loop, ioloop.IOLoop)


def test_base_sender_send_not_implemented():
    sender = senders.Sender()
    with pytest.raises(NotImplementedError):
        sender.send(1).result()


def test_base_sender_set_process_instantiate_jaeger_process():
    sender = senders.Sender()
    sender.set_process('service', {}, max_length=0)
    assert isinstance(sender._process, ttypes.Process)
    assert sender._process.serviceName == 'service'


def test_base_sender_spanless_flush_is_noop():
    sender = senders.Sender()
    flushed = sender.flush().result()
    assert flushed == 0


def test_base_sender_processless_flush_is_noop():
    sender = senders.Sender()
    sender.spans.append('foo')
    flushed = sender.flush().result()
    assert flushed == 0


class CustomException(Exception):
    pass


class SenderFlushTest(AsyncTestCase):

    def span(self):
        FakeTracer = collections.namedtuple('FakeTracer', ['ip_address', 'service_name', 'max_tag_value_length'])
        tracer = FakeTracer(ip_address='127.0.0.1', service_name='reporter_test', max_tag_value_length=sys.maxsize)
        ctx = SpanContext(trace_id=1, span_id=1, parent_id=None, flags=1)
        span = Span(context=ctx, tracer=tracer, operation_name='foo')
        span.start_time = time.time()
        span.end_time = span.start_time + 0.001  # 1ms
        return span

    @gen_test
    def test_base_sender_flush_raises_exceptions(self):
        sender = senders.Sender()
        sender.set_process('service', {}, max_length=0)

        sender.spans = [self.span()]

        sender.send = mock.MagicMock(side_effect=CustomException('Failed to send batch.'))
        assert sender.span_count == 1

        try:
            yield sender.flush()
        except Exception as exc:
            assert isinstance(exc, CustomException)
            assert str(exc) == 'Failed to send batch.'
        else:
            assert False, "Didn't Raise"
        assert sender.span_count == 0

    @gen_test
    def test_udp_sender_flush_reraises_exceptions(self):
        exceptions = ((CustomException, 'Failed to send batch.',
                       'Failed to submit traces to jaeger-agent: Failed to send batch.'),
                      (socket.error, 'Connection Failed',
                       'Failed to submit traces to jaeger-agent socket: Connection Failed'))
        for exception, value, expected_value in exceptions:
            sender = senders.UDPSender(host='mock', port=4242)
            sender.set_process('service', {}, max_length=0)

            sender.spans = [self.span()]

            sender._agent.emitBatch = mock.MagicMock(side_effect=exception(value))
            assert sender.span_count == 1

            try:
                yield sender.flush()
            except Exception as exc:
                assert isinstance(exc, exception)
                assert str(exc) == expected_value
            else:
                assert False, "Didn't Raise"
            assert sender.span_count == 0

    @gen_test
    def test_udp_sender_large_span_dropped_on_flush(self):
        sender = senders.UDPSender(host='mock', port=4242)
        sender.set_process('service', {'tagOne': 'someTagValue'}, max_length=0)

        span_to_send = self.span()
        span_to_filter = self.span()
        span_to_filter.set_tag('someTag', '.' * 65000)
        sender.spans = [span_to_send, span_to_filter]

        batch_store = []
        def mock_emit_batch(batch):
            batch_store.append(batch)

        with mock.patch.object(sender._agent, 'emitBatch',
                               gen.coroutine(mock_emit_batch)):
            try:
                yield sender.flush()
            except Exception as exc:
                assert isinstance(exc, senders.UDPSenderException)
                assert 'Cannot send span of size' in str(exc)
            else:
                assert False, "Didn't Raise"

        assert batch_store[0].spans == [thrift.make_jaeger_span(span_to_send)]

    @gen_test
    def test_udp_sender_batches_spans_over_multiple_packets_on_flush(self):
        sender = senders.UDPSender(host='mock', port=4242)
        sender.set_process('service', {'tagOne': 'someTagValue'}, max_length=0)

        spans = [self.span() for _ in range(60)]
        for span in spans:
            span.set_tag('tag', '.' * 10000)  # Send 10 batches of 6
        sender.spans = list(spans)

        batch_store = []
        def mock_emit_batch(batch):
            batch_store.append(batch)

        with mock.patch.object(sender._agent, 'emitBatch',
                               gen.coroutine(mock_emit_batch)):
            yield sender.flush()

        assert len(batch_store) == 10
        c = 0
        for i in range(10):
            assert batch_store[i].spans == [thrift.make_jaeger_span(span) for span in spans[c:c+6]]
            c += 6


def test_udp_sender_instantiate_thrift_agent():

    sender = senders.UDPSender(host='mock', port=4242)

    assert sender._agent is not None
    assert isinstance(sender._agent, Agent.Client)


def test_udp_sender_intantiate_local_agent_channel():

    sender = senders.UDPSender(host='mock', port=4242)

    assert sender._channel is not None
    assert sender._channel.io_loop == sender._io_loop
    assert isinstance(sender._channel, LocalAgentSender)


def test_udp_sender_calls_agent_emitBatch_on_send():

    test_data = {'foo': 'bar'}
    sender = senders.UDPSender(host='mock', port=4242)
    sender._agent = mock.Mock()

    sender.send(test_data)

    sender._agent.emitBatch.assert_called_once_with(test_data)


def test_udp_sender_implements_thrift_protocol_factory():

    sender = senders.UDPSender(host='mock', port=4242)

    assert callable(sender.getProtocol)
    protocol = sender.getProtocol(mock.MagicMock())
    assert isinstance(protocol, TCompactProtocol.TCompactProtocol)
