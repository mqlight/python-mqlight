
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
import pytest
import threading
import mqlight
from mqlight.stubmqlproton import _MQLightMessenger


def side_effect(service, ssl_trust_certificate, ssl_verify_name):
    """mock side effect function"""
    if 'bad' in service.netloc:
        raise TypeError('bad service ' + service.netloc)


class TestStart(unittest.TestCase):
    """
    Unit tests for client.start()
    """
    TEST_TIMEOUT = 10.0

    def test_successful_start_stop(self):
        """
        Test a successful start / stop, ensuring that both the 'started'
        event and the callback passed into client.start(...) are driven.
        """
        test_is_done = threading.Event()

        def started(client):
            """started listener"""
            assert client.get_state() == mqlight.STARTED

            def stopped(client, error):
                """stopped listener"""
                assert client.get_state() == mqlight.STOPPED
                test_is_done.set()
            client.stop(stopped)
        client = mqlight.Client('amqp://host:1234',
                                'test_successful_start_stop',
                                on_started=started)
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()

    def test_start_argument_is_function(self):
        """
        Test that when an argument is specified to the client.start(...)
        function it must be a callback (e.g. of type function)
        """
        def started(client):
            pytest.raises(TypeError, client.start, 1234)
            client.stop()
            test_is_done.set()
        test_is_done = threading.Event()
        client = mqlight.Client('amqp://host:1234',
                                'test_start_argument_is_function',on_started=started)
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()

    def test_start_method_returns_client(self):
        """
        Test that the start(...) method returns the instance of the client
        that it is invoked on.
        """
        def started(client):
            result = client.start()
            assert client == result
            client.stop()
            test_is_done.set()
        test_is_done = threading.Event()
        client = mqlight.Client('amqp://host:1234',
                                'test_start_method_returns_client', on_started=started)
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()

    def test_start_too_many_arguments(self):
        """
        Test that if too many arguments are supplied to start - then an
        exception is raised.
        """
        def Dummy():
            pass
        def stopped(client, error):
            test_is_done.set()
        def started(client):
            callback = Dummy()
            pytest.raises(TypeError, client.start, callback, 'gooseberry')
            client.stop(on_stopped=stopped)
        test_is_done = threading.Event()
        client = mqlight.Client('amqp://host',
                                'test_start_too_many_arguments',on_started=started)
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()

    def test_start_retry(self):
        """
        Tests that calling start on an endpoint that is currently down retries
        until successful.
        """
        test_is_done = threading.Event()
        required_connect_status = 1
        _MQLightMessenger.set_connect_status(required_connect_status)

        def state_changed(client, state, err):
            if state == mqlight.RETRYING:
                """error callback"""
                _MQLightMessenger.set_connect_status(
                    _MQLightMessenger.get_connect_status() - 1)

        def started(client):
            """started listener"""
            assert _MQLightMessenger.get_connect_status() == 0
            client.stop()
            test_is_done.set()
        client = mqlight.Client('amqp://host:1234', 'test_start_retry',
                                on_started=started,
                                on_state_changed=state_changed)
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()
