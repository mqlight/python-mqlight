"""
<copyright
notice="lm-source-program"
pids="5725-P60"
years="2013,2014"
crc="3568777996" >
Licensed Materials - Property of IBM

5725-P60

(C) Copyright IBM Corp. 2013, 2015

US Government Users Restricted Rights - Use, duplication or
disclosure restricted by GSA ADP Schedule Contract with
IBM Corp.
</copyright>
"""
# pylint: disable=bare-except,broad-except,invalid-name,no-self-use
# pylint: disable=too-many-public-methods,unused-argument
import pytest
import threading
from mock import Mock, patch
import mqlight
from mqlight.stubproton import _MQLightMessenger


def side_effect(service, ssl_trust_certificate, ssl_verify_name):
    """mock side effect function"""
    if 'bad' in service.netloc:
        raise TypeError('bad service ' + service.netloc)


@patch('mqlight.mqlightproton._MQLightMessenger.connect',
       Mock(side_effect=side_effect))
@patch('mqlight.mqlightproton._MQLightMessenger.get_remote_idle_timeout',
       Mock(return_value=0))
class TestStart(object):

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

        def started(err):
            """started listener"""
            assert err is None
            assert client.get_state() == mqlight.STARTED

            def stopped(err):
                """stopped listener"""
                assert err is None
                assert client.get_state() == mqlight.STOPPED
                test_is_done.set()
            client.stop(stopped)
        client = mqlight.Client('amqp://host:1234',
                                'test_successful_start_stop',
                                on_started=started)
        assert client.get_state() in (mqlight.STARTED, mqlight.STARTING)

    def test_start_argument_is_function(self):
        """
        Test that when an argument is specified to the client.start(...)
        function it must be a callback (e.g. of type function)
        """
        client = mqlight.Client('amqp://host:1234',
                                'test_start_argument_is_function')
        pytest.raises(TypeError, client.start, 1234)

    def test_start_method_returns_client(self):
        """
        Test that the start(...) method returns the instance of the client
        that it is invoked on.
        """
        client = mqlight.Client('amqp://host:1234',
                                'test_start_method_returns_client')
        result = client.start()
        assert client == result
        client.stop()

    def test_start_too_many_arguments(self):
        """
        Test that if too many arguments are supplied to start - then an
        exception is raised.
        """
        client = mqlight.Client('amqp://host',
                                'test_start_too_many_arguments')
        callback = Mock()
        pytest.raises(TypeError, client.start, callback, 'gooseberry')

    def test_start_retry(self):
        """
        Tests that calling start on an endpoint that is currently down retries
        until successful.
        """
        test_is_done = threading.Event()
        required_connect_status = 2
        _MQLightMessenger.set_connect_status(required_connect_status)

        def state_changed(state, err):
            if state == mqlight.ERROR:
                """error callback"""
                _MQLightMessenger.set_connect_status(
                    _MQLightMessenger.get_connect_status() - 1)

        def started(err):
            """started listener"""
            assert err is None
            assert _MQLightMessenger.get_connect_status() == 0
            client.stop()
            test_is_done.set()
        client = mqlight.Client('amqp://host:1234', 'test_start_retry',
                                on_started=started,
                                on_state_changed=state_changed)
        done = test_is_done.wait(self.TEST_TIMEOUT)
        assert done
