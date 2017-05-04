
# python-mqlight - high-level API by which you can interact with MQ Light
#
# Copyright 2015-2017 IBM Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# pylint: disable=bare-except,broad-except,invalid-name,no-self-use
# pylint: disable=too-many-public-methods,unused-argument
import unittest
import threading
import pytest
import mqlight
from mqlight.stubmqlproton import _MQLightSocket

class TestCallBackFunction(unittest.TestCase):
    """
    A set of unitest to ensure that callback function correctly detected
    and handler callout error with those methods.
    On detection it is expected to ask a stop of the client with the
    error message received.
    """

    TEST_TIMEOUT = 10.0
    UNITTEST_ERROR = 'Unitest generated'


    def test_send_on_sent(self):
        test_is_done = threading.Event()
        def bad_callback(client, err, topic, data, options):
            raise TypeError(self.UNITTEST_ERROR)
        def state_change(client, state, err):
            if state == 'stopped':
                assert str(err[1]) == self.UNITTEST_ERROR
                test_is_done.set()
        def started(client):
            client.send('topic', 'message', {}, bad_callback)
        client = mqlight.Client('amqp://host', on_started=started, on_state_changed=state_change)
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()

    def test_subscribe_on_subscribed(self):
        test_is_done = threading.Event()
        def bad_callback(client, err, topic, share):
            raise TypeError(self.UNITTEST_ERROR)
        def state_change(client, state, err):
            if state == 'stopped':
                assert str(err[1]) == self.UNITTEST_ERROR
                test_is_done.set()
        def started(client):
            client.subscribe('topic', on_subscribed=bad_callback)
        client = mqlight.Client('amqp://host', on_started=started, on_state_changed=state_change)
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()

    def test_subscribe_on_message(self):
        test_is_done = threading.Event()
        def bad_callback(client, type, message):
            raise TypeError(self.UNITTEST_ERROR)
        def state_change(client, state, err):
            if state == 'stopped':
                assert str(err[1]) == self.UNITTEST_ERROR
                test_is_done.set()
        def trigger_message(client, err, topic, share):
            _MQLightSocket.trigger_on_read()
        def started(client):
            client.subscribe('topic', on_subscribed=trigger_message, on_message=bad_callback)
        client = mqlight.Client('amqp://host-message', on_started=started, on_state_changed=state_change)
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()

    def test_unsubscribe_on_unsubscribed(self):
        test_is_done = threading.Event()
        def bad_callback(client, err, topic, share):
            raise TypeError(self.UNITTEST_ERROR)
        def subscribed(client, err, topic, share):
            client.unsubscribe(topic, on_unsubscribed=bad_callback)
        def state_change(client, state, err):
            if state == 'stopped':
                assert str(err[1]) == self.UNITTEST_ERROR
                test_is_done.set()
        def started(client):
            client.subscribe('topic', on_subscribed=subscribed)
        client = mqlight.Client('amqp://host', on_started=started, on_state_changed=state_change)
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()
