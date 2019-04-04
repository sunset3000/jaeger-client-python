# Modified by SignalFx
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
import json
import mock

import pytest

from jaeger_client.throttler import RemoteThrottler
from .conftest import wait_for


@pytest.fixture
def throttler():
    channel = mock.MagicMock()
    throttler = RemoteThrottler(channel, 'test-service',
                                refresh_interval=.1)
    yield throttler
    throttler.close()


def test_throttler_simple(throttler):
    allowed = throttler.is_allowed('test-operation')
    assert not allowed
    allowed = throttler.is_allowed('test-operation')
    assert not allowed


def test_throttler_credits(throttler):
    throttler.credits['test-operation'] = 3.0
    allowed = throttler.is_allowed('test-operation')
    assert allowed
    assert throttler.credits['test-operation'] == 2.0


def test_throttler_init_polling(throttler):
    # noinspection PyProtectedMember
    throttler._init_polling()
    throttler.close()
    # noinspection PyProtectedMember
    throttler._init_polling()


def test_throttler_delayed_polling(throttler):
    throttler.credits = {'test-operation': 0}
    # noinspection PyProtectedMember
    throttler._delayed_polling()
    assert wait_for(lambda: throttler.channel.request_throttling_credits.call_count == 1)
    assert throttler.periodic
    throttler.close()
    throttler.periodic = None
    throttler._delayed_polling()
    assert throttler.periodic is None


def test_throttler_request_callback(throttler):
    throttler.error_reporter = mock.MagicMock()
    # noinspection PyProtectedMember
    throttler._request_callback(None, Exception())
    assert throttler.error_reporter.error.call_count == 1
    response = mock.MagicMock()
    content = """
        {
            \"balances\": [
                {
                    \"operation\": \"test-operation\",
                    \"balance\": 2.0
                }
            ]
        }
    """
    response.content = content
    response.json = lambda: json.loads(response.content)
    throttler._request_callback(response, None)
    assert throttler.credits['test-operation'] == 2.0
    assert throttler.error_reporter.error.call_count == 1

    response.content = '{ "bad": "json" }'
    throttler._request_callback(response, None)
    assert throttler.error_reporter.error.call_count == 2
