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


@patch('mqlight.mqlightproton._MQLightMessenger.connect',
       Mock(return_value=None))
class TestStop(object):

    """
    Unit tests for client.stop()
    """
    TEST_TIMEOUT = 10.0

    def test_stop_callback_event(self):
        """
        Test a successful stop, ensuring that both the 'stopped'
        event and the callback passed into client.stopped(...) are driven.
        """
        test_is_done = threading.Event()
        client = mqlight.Client('amqp://host:1234',
                                'test_stop_callback_event')

        def stopped(err):
            """stopped listener"""
            test_is_done.set()
        client.stop(stopped)
        done = test_is_done.wait(self.TEST_TIMEOUT)
        assert done
        assert client.get_state() == mqlight.STOPPED

    def test_stop_argument_is_function(self):
        """
        Test that when an argument is specified to the client.stop(...)
        function it must be a callback (e.g. of type function).
        """
        client = mqlight.Client('amqp://host:1234',
                                'test_stop_argument_is_function')
        pytest.raises(TypeError, client.stop, 1234)

    def test_stop_method_returns_client(self):
        """
        Test that the stop(...) method returns the instance of the client
        that it is invoked on.
        """
        client = mqlight.Client('amqp://host:1234',
                                'test_stop_method_returns_client')
        result = client.stop()
        assert client == result

    def test_stop_when_already_stopped(self):
        """
        Tests that calling stop on an already stopped client has no
        effect other than to callback any supplied callback function to
        indicate success.
        """
        test_is_done = threading.Event()

        def second_callback(err):
            """second stopped callback"""
            assert err is None
            assert client.get_state() == mqlight.STOPPED
            test_is_done.set()

        def first_callback(err):
            """first stopped callback"""
            assert err is None
            assert client.get_state() == mqlight.STOPPED
            client.stop(second_callback)

        def started(err):
            """started listener"""
            assert err is None
            assert client.get_state() == mqlight.STARTED
            client.stop(first_callback)
        client = mqlight.Client('amqp://host:1234',
                                'test_stop_when_already_stopped',
                                on_started=started)
        done = test_is_done.wait(self.TEST_TIMEOUT)
        assert done

    def test_stop_too_many_arguments(self):
        """
        Test that if too many arguments are supplied to stop, an error is
        raised
        """
        client = mqlight.Client('amqp://host', 'test_stop_too_many_arguments')
        with pytest.raises(TypeError):
            # pylint: disable=too-many-function-args
            client.stop(Mock(), 'extra')
