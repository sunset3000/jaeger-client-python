# Modified by SignalFx
# Copyright (c) 2016-2018 Uber Technologies, Inc.
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
import os
import unittest
import opentracing.tracer
from jaeger_client import Config, ConstSampler, ProbabilisticSampler, RateLimitingSampler
from jaeger_client.config import DEFAULT_THROTTLER_PORT
from jaeger_client.metrics import MetricsFactory
from jaeger_client.reporter import NullReporter
from jaeger_client.senders import HTTPSender
from jaeger_client import constants


class ConfigTests(unittest.TestCase):

    def setUp(self):
        if Config.initialized():
            Config._initialized = False

    def test_enabled(self):
        c = Config({'enabled': True}, service_name='x')
        assert c.enabled
        c = Config({'enabled': False}, service_name='x')
        assert not c.enabled

    def test_reporter_batch_size(self):
        c = Config({'reporter_batch_size': 12345}, service_name='x')
        assert c.reporter_batch_size == 12345
        c = Config({}, service_name='x')
        assert c.reporter_batch_size == 10

    def test_tags(self):
        os.environ['JAEGER_TAGS'] = 'a=b,c=d'
        c = Config({'tags': {'e': 'f'}}, service_name='x')
        assert c.tags == {'a': 'b', 'c': 'd', 'e': 'f'}
        c.create_tracer(NullReporter(), ConstSampler(True))

    def test_no_sampler(self):
        c = Config({}, service_name='x')
        assert c.sampler is None

    def test_const_sampler(self):
        c = Config({'sampler': {'type': 'const', 'param': True}},
                   service_name='x')
        assert type(c.sampler) is ConstSampler
        assert c.sampler.decision
        c = Config({'sampler': {'type': 'const', 'param': False}},
                   service_name='x')
        assert type(c.sampler) is ConstSampler
        assert not c.sampler.decision

    def test_probabilistic_sampler(self):
        with self.assertRaises(Exception):
            cfg = {'sampler': {'type': 'probabilistic', 'param': 'xx'}}
            Config(cfg, service_name='x').sampler
        c = Config({'sampler': {'type': 'probabilistic', 'param': 0.5}},
                   service_name='x')
        assert type(c.sampler) is ProbabilisticSampler
        assert c.sampler.rate == 0.5

    def test_rate_limiting_sampler(self):
        with self.assertRaises(Exception):
            cfg = {'sampler': {'type': 'rate_limiting', 'param': 'xx'}}
            Config(cfg, service_name='x').sampler
        c = Config({'sampler': {'type': 'rate_limiting', 'param': 1234}},
                   service_name='x')
        assert type(c.sampler) is RateLimitingSampler
        assert c.sampler.traces_per_second == 1234

    def test_bad_sampler(self):
        c = Config({'sampler': {'type': 'bad-sampler'}}, service_name='x')
        with self.assertRaises(ValueError):
            c.sampler.is_sampled(0)

    def test_agent_reporting_host(self):
        c = Config({}, service_name='x')
        assert c.local_agent_reporting_host == 'localhost'

        c = Config({'local_agent': {'reporting_host': 'jaeger.local'}}, service_name='x')
        assert c.local_agent_reporting_host == 'jaeger.local'

        os.environ['JAEGER_AGENT_HOST'] = 'jaeger-env.local'
        c = Config({}, service_name='x')
        assert c.local_agent_reporting_host == 'jaeger-env.local'

    def test_max_tag_value_length(self):
        c = Config({}, service_name='x')
        assert c.max_tag_value_length == constants.MAX_TAG_VALUE_LENGTH

        c = Config({'max_tag_value_length': 333}, service_name='x')
        assert c.max_tag_value_length == 333

        t = c.create_tracer(NullReporter(), ConstSampler(True))
        assert t.max_tag_value_length == 333

    def test_propagation(self):
        c = Config({}, service_name='x')
        assert c.propagation == {}

        c = Config({'propagation': 'b3'}, service_name='x')
        assert len(c.propagation) == 1

    def test_throttler(self):
        c = Config({
            'throttler': {}
        }, service_name='x')
        assert not c.throttler_group()
        assert c.throttler_port == DEFAULT_THROTTLER_PORT
        assert c.throttler_refresh_interval == constants.DEFAULT_THROTTLER_REFRESH_INTERVAL

        c = Config({
            'throttler': {
                'port': '1234',
                'refresh_interval': '10'
            }
        }, service_name='x')
        assert c.throttler_group()
        assert c.throttler_port == 1234
        assert c.throttler_refresh_interval == 10

        c = Config({}, service_name='x')
        assert c.throttler_group() is None
        assert c.throttler_port is None
        assert c.throttler_refresh_interval is None

    def test_for_unexpected_config_entries(self):
        with self.assertRaises(Exception):
            Config({'unexpected': 'value'}, validate=True)

    def test_reporter_queue_size_valid(self):
        config = Config({'reporter_queue_size': 100}, service_name='x', validate=True)
        assert config.reporter_queue_size == 100

    def test_missing_service_name(self):
        with self.assertRaises(ValueError):
            Config({})

    def test_disable_metrics(self):
        config = Config({'metrics': False}, service_name='x')
        assert isinstance(config._metrics_factory, MetricsFactory)

    def test_initialize_tracer(self):
        c = Config({}, service_name='x')
        tracer = c.initialize_tracer()

        assert opentracing.tracer == tracer
        tracer.close()

    def test_default_local_agent_reporting_port(self):
        c = Config({}, service_name='x')
        assert c.local_agent_reporting_port == 6831

    def test_generate_128bit_trace_id(self):
        c = Config({}, service_name='x')
        assert c.generate_128bit_trace_id is False

        c = Config({'generate_128bit_trace_id': True}, service_name='x')
        assert c.generate_128bit_trace_id is True

        os.environ['JAEGER_TRACEID_128BIT'] = 'true'
        c = Config({'generate_128bit_trace_id': False}, service_name='x')
        assert c.generate_128bit_trace_id is False

        c = Config({}, service_name='x')
        assert c.generate_128bit_trace_id is True

        os.environ.pop('JAEGER_TRACEID_128BIT')
        assert os.getenv('JAEGER_TRACEID_128BIT', None) is None

    def test_global_tracer_initializaion(self):
        c = Config({}, service_name='x')
        tracer = c.initialize_tracer()
        assert tracer
        attempt = c.initialize_tracer()
        assert attempt is None
        tracer.close()

    def test_jaeger_endpoint(self):
        c = Config({'jaeger_endpoint': 'some_endpoint'}, service_name='x')
        assert c.jaeger_endpoint == 'some_endpoint'
        os.environ['JAEGER_ENDPOINT'] = 'SomeEndpoint'
        c = Config({}, service_name='x')
        assert c.jaeger_endpoint == 'SomeEndpoint'
        del os.environ['JAEGER_ENDPOINT']

    def test_jaeger_auth_token(self):
        c = Config({'jaeger_auth_token': 'some_token'},
                   service_name='x', validate=True)
        assert c.jaeger_auth_token == 'some_token'
        os.environ['JAEGER_AUTH_TOKEN'] = 'SomeToken'
        c = Config({}, service_name='x')
        assert c.jaeger_auth_token == 'SomeToken'
        del os.environ['JAEGER_AUTH_TOKEN']

    def test_jaeger_user_and_password(self):
        c = Config({'jaeger_user': 'some_user',
                    'jaeger_password': 'some_password'},
                   service_name='x', validate=True)
        assert c.jaeger_user == 'some_user'
        assert c.jaeger_password == 'some_password'
        os.environ['JAEGER_USER'] = 'SomeUser'
        os.environ['JAEGER_PASSWORD'] = 'SomePassword'
        c = Config({}, service_name='x')
        assert c.jaeger_user == 'SomeUser'
        assert c.jaeger_password == 'SomePassword'
        del os.environ['JAEGER_USER']
        del os.environ['JAEGER_PASSWORD']

    def test_jaeger_auth_token_and_basic_mutually_exclusive(self):
        with self.assertRaises(ValueError) as e:
            Config({'jaeger_auth_token': 'some_token',
                    'jaeger_user': 'some_user',
                    'jaeger_password': 'some_password'},
                   service_name='x', validate=True)
        assert e.exception.args[0] == ('Cannot accept both jaeger_auth_token and '
                                       'jaeger_user/jaeger_password for authentication')

    def test_jaeger_user_and_password_required(self):
        for cfg in ({'jaeger_user': 'some_user'},
                    {'jaeger_password': 'some_password'}):
            with self.assertRaises(ValueError) as e:
                Config(cfg, service_name='x', validate=True)

            assert e.exception.args[0] == ('Must provide both jaeger_user and '
                                           'jaeger_password for authentication.')

    def test_specifying_jaeger_endpoint_creates_http_sender(self):
        c = Config({'jaeger_endpoint': 'some_endpoint'}, service_name='x')
        tracer = c.initialize_tracer()
        assert isinstance(tracer.reporter._sender, HTTPSender)
        tracer.close()
