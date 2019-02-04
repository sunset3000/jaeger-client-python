# Copyright (c) 2019 Uber Technologies, Inc.
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

import time

import pytest

from jaeger_client.ioloop_util import PeriodicCallback
from .conftest import wait_for


class TestPeriodicCallback(object):

    def test_callback_and_callback_time_required(self):
        with pytest.raises(TypeError):
            PeriodicCallback()

        with pytest.raises(TypeError):
            PeriodicCallback(callback=lambda: True)

        with pytest.raises(TypeError):
            PeriodicCallback(callback_time=100)

    def test_callback_time_gt_zero(self):
        with pytest.raises(ValueError) as exc:
            PeriodicCallback(lambda: True, -1)
        assert str(exc.value) == 'callback_time must be positive'

    def test_start_runs_callback_and_stop_halts_scheduling(self):
        count = [0]

        def callback():
            count[0] += 1

        pc = PeriodicCallback(callback, 500)
        assert not pc._running
        assert pc._scheduler_thread is None
        assert pc._callback_thread is None

        pc.start()
        assert wait_for(lambda: count[0])
        pc.stop()

        assert count[0] == 1
        assert pc._running is False
        assert pc._scheduler_thread is None
        assert pc._callback_thread is None

    def test_runtime_exceeding_period_prevents_subsequent_runs(self):
        count = [0]

        def callback():
            time.sleep(.05)
            count[0] += 1
            time.sleep(.05)

        pc = PeriodicCallback(callback, 1)
        pc.start()
        assert wait_for(lambda: count[0])
        pc.stop()

        assert count[0] == 1
        assert pc._running is False
        assert pc._scheduler_thread is None
        assert pc._callback_thread is None

    def test_start_is_idempotent(self):
        count = [0]

        def callback():
            time.sleep(.05)
            count[0] += 1
            time.sleep(.05)

        pc = PeriodicCallback(callback, 1)
        for _ in range(100):
            pc.start()

        assert wait_for(lambda: count[0])
        pc.stop()

        assert count[0] == 1
        assert pc._running is False
        assert pc._scheduler_thread is None
        assert pc._callback_thread is None

    def test_out_of_order_invocations_harmless(self):
        count = [0]

        def callback():
            count[0] += 1
            time.sleep(.05)

        pc = PeriodicCallback(callback, 0)

        pc.stop()
        pc.start()
        assert wait_for(lambda: count[0])
        pc.stop()

        assert count[0] == 1
        assert pc._running is False
        assert pc._scheduler_thread is None
        assert pc._callback_thread is None
