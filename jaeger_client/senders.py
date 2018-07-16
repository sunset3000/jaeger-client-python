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
from __future__ import absolute_import

import socket
import logging
import tornado.gen
from threadloop import ThreadLoop

from . import thrift
from .local_agent_net import LocalAgentSender
from thrift.protocol import TCompactProtocol

from jaeger_client.thrift_gen.agent import Agent


DEFAULT_SAMPLING_PORT = 5778

logger = logging.getLogger('jaeger_tracing')


class Sender(object):
    def __init__(self, io_loop=None):
        from threading import Lock
        self.io_loop = io_loop or self._create_new_thread_loop()
        self._process_lock = Lock()
        self._process = None
        self.spans = []

    def append(self, span):
        """Queue a span for subsequent submission calls to flush()"""
        self.spans.append(span)

    @tornado.gen.coroutine
    def flush(self):
        """
        Examine span and process state before yielding to _flush() for batching and transport.
        Returns a tuple for accounting the number of spans attempted to send along with any
        raised Exception and error message info, if any, yielded from _flush().
        """
        if not self.spans:
            return
        with self._process_lock:
            process = self._process
            if not process:
                return

        num_spans, exc, error_msg = yield self._flush(self.spans, self._process)

        self.spans = []

        raise tornado.gen.Return((num_spans, exc, error_msg))

    @tornado.gen.coroutine
    def _flush(self, spans, process):
        """
        Batch spans and invokes send() while calculating spans sent and providing all exception
        handling.  Override with specific batching logic and error messaging, if desired.
        """
        num_spans = len(spans)
        exc = None
        error_msg = ''

        batch = thrift.make_jaeger_batch(spans=spans, process=process)

        try:
            yield self.send(batch)
        except Exception as e:
            exc = e
            error_msg = 'Failed to send batch: %s'

        raise tornado.gen.Return((num_spans, exc, error_msg))

    @tornado.gen.coroutine
    def send(self, batch):
        """
        Send batch of spans out via thrift transport. Any exceptions thrown
        will be caught in the flush exception handler of _flush().
        """
        raise NotImplementedError('This method should be implemented by subclasses')

    def set_process(self, service_name, tags, max_length):
        with self._process_lock:
            self._process = thrift.make_process(
                service_name=service_name, tags=tags, max_length=max_length,
            )

    def _create_new_thread_loop(self):
        """
        Create a daemonized thread that will run Tornado IOLoop.
        :return: the IOLoop backed by the new thread.
        """
        self._thread_loop = ThreadLoop()
        if not self._thread_loop.is_ready():
            self._thread_loop.start()
        return self._thread_loop._io_loop


class UDPSender(Sender):
    def __init__(self, host, port, io_loop=None):
        super(UDPSender, self).__init__(io_loop=io_loop)
        self.host = host
        self.port = port
        self.channel = self._create_local_agent_channel(self.io_loop)
        self.agent = Agent.Client(self.channel, self)

    @tornado.gen.coroutine
    def send(self, batch):
        return self.agent.emitBatch(batch)

    def _create_local_agent_channel(self, io_loop):
        """
        Create an out-of-process channel communicating to local jaeger-agent.
        Spans are submitted as SOCK_DGRAM Thrift, sampling strategy is polled
        via JSON HTTP.

        :param self: instance of Config
        """
        logger.info('Initializing Jaeger Tracer with UDP reporter')
        return LocalAgentSender(
            host=self.host,
            sampling_port=DEFAULT_SAMPLING_PORT,
            reporting_port=self.port,
            io_loop=io_loop
        )

    @tornado.gen.coroutine
    def _flush(self, spans, process):
        num_spans = len(spans)
        exc = None
        error_msg = ''

        batch = thrift.make_jaeger_batch(spans=spans, process=process)

        try:
            yield self.send(batch)
        except socket.error as e:
            exc = e
            error_msg = 'Failed to submit traces to jaeger-agent socket: %s'
        except Exception as e:
            exc = e
            error_msg = 'Failed to submit traces to jaeger-agent: %s'

        raise tornado.gen.Return((num_spans, exc, error_msg))

    # method for protocol factory
    def getProtocol(self, transport):
        """
        Implements Thrift ProtocolFactory interface
        :param: transport:
        :return: Thrift compact protocol
        """
        return TCompactProtocol.TCompactProtocol(transport)
