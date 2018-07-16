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

import time
import collections

import mock
import pytest
from tornado import ioloop
from tornado import gen
from tornado.testing import AsyncTestCase, gen_test

from jaeger_client import senders
from jaeger_client import Span, SpanContext
from jaeger_client.local_agent_net import LocalAgentSender
from jaeger_client.thrift_gen.agent import Agent
from jaeger_client.thrift_gen.jaeger import ttypes
from thrift.protocol import TCompactProtocol


def test_base_sender_create_io_loop_if_not_provided():

    sender = senders.Sender()

    assert sender.io_loop is not None
    assert isinstance(sender.io_loop, ioloop.IOLoop)


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
    assert flushed is None

    
def test_base_sender_processless_flush_is_noop():
    sender = senders.Sender()
    sender.spans.append('foo')
    flushed = sender.flush().result()
    assert flushed is None


class CustomException(Exception):
    pass


class SenderFlushTest(AsyncTestCase):
    @gen_test
    def test_base_sender_flush_yields_exceptions(self):
        FakeTracer = collections.namedtuple('FakeTracer', ['ip_address', 'service_name'])
        tracer = FakeTracer(ip_address='127.0.0.1', service_name='reporter_test')
        sender = senders.Sender()
        sender.set_process('service', {}, max_length=0)

        ctx = SpanContext(trace_id=1, span_id=1, parent_id=None, flags=1)
        span = Span(context=ctx, tracer=tracer, operation_name='foo')
        span.start_time = time.time()
        span.end_time = span.start_time + 0.001  # 1ms
        sender.spans = [span]

        sender.send = mock.MagicMock(side_effect=CustomException)
        num_spans, exc, error_msg = yield sender.flush()
        assert num_spans == 1
        assert isinstance(exc, CustomException)
        assert error_msg == 'Failed to send batch: %s'

    @gen_test
    def test_flush_batcher_accounting_information_yielded_from_flush(self):
        @gen.coroutine
        def stubbed_send(self, batch):
            pass

        @gen.coroutine
        def stubbed__flush(self, spans, process):
            raise gen.Return((100, CustomException(), 'SomeMessage'))

        sender = senders.Sender()
        sender.set_process('service', {}, max_length=0)
        sender.spans = ['foo', 'bar']
        sender._flush = stubbed__flush.__get__(sender)
        sender.send = stubbed_send.__get__(sender)

        num_spans, exc, error_msg = yield sender.flush()
        assert num_spans == 100
        assert isinstance(exc, CustomException)
        assert error_msg == 'SomeMessage'


def test_udp_sender_instantiate_thrift_agent():

    sender = senders.UDPSender(host='mock', port=4242)

    assert sender.agent is not None
    assert isinstance(sender.agent, Agent.Client)


def test_udp_sender_intantiate_local_agent_channel():

    sender = senders.UDPSender(host='mock', port=4242)

    assert sender.channel is not None
    assert sender.channel.io_loop == sender.io_loop
    assert isinstance(sender.channel, LocalAgentSender)


def test_udp_sender_calls_agent_emitBatch_on_send():

    test_data = {'foo': 'bar'}
    sender = senders.UDPSender(host='mock', port=4242)
    sender.agent = mock.Mock()

    sender.send(test_data)

    sender.agent.emitBatch.assert_called_once_with(test_data)


def test_udp_sender_implements_thrift_protocol_factory():

    sender = senders.UDPSender(host='mock', port=4242)

    assert callable(sender.getProtocol)
    protocol = sender.getProtocol(mock.MagicMock())
    assert isinstance(protocol, TCompactProtocol.TCompactProtocol)
