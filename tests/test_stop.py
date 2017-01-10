"""
<copyright
notice="lm-source-program"
pids="5724-H72"
years="2013,2016"
crc="3451277450" >
Licensed Materials - Property of IBM

5725-P60

(C) Copyright IBM Corp. 2013, 2016

US Government Users Restricted Rights - Use, duplication or
disclosure restricted by GSA ADP Schedule Contract with
IBM Corp.
</copyright>
"""
# pylint: disable=bare-except,broad-except,invalid-name,no-self-use
# pylint: disable=too-many-public-methods,unused-argument
import unittest
import pytest
import threading
import mqlight


class TestStop(unittest.TestCase):

    """
    Unit tests for client.stop()
    """
    TEST_TIMEOUT = 10.0

    def test_stop_callback_event(self):
        """
        Test a successful stop, ensuring that both the 'stopped'
        event and the callback passed into client.stopped(...) are driven.
        """
        def started(client):
            def stopped(client, error):
                """stopped listener"""
                test_is_done.set()
            client.stop(stopped)
        test_is_done = threading.Event()
        client = mqlight.Client(
            'amqp://host:1234',
            'test_stop_callback_event',
            on_started=started)

        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()
        assert client.get_state() == mqlight.STOPPED

    def test_stop_argument_is_function(self):
        """
        Test that when an argument is specified to the client.stop(...)
        function it must be a callback (e.g. of type function).
        """
        test_is_done = threading.Event()

        def started(client):
            pytest.raises(TypeError, client.stop, 1234)
            client.stop()
            test_is_done.set()
        client = mqlight.Client('amqp://host:1234',
                                'test_stop_argument_is_function', on_started=started)
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()

    def test_stop_method_returns_client(self):
        """
        Test that the stop(...) method returns the instance of the client
        that it is invoked on.
        """
        test_is_done = threading.Event()
        def started(client):
                    result = client.stop()
                    assert client == result
                    test_is_done.set()
        client = mqlight.Client('amqp://host:1234',
                                'test_stop_method_returns_client', on_started=started)
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()

    def test_stop_when_already_stopped(self):
        """
        Tests that calling stop on an already stopped client has no
        effect other than to callback any supplied callback function to
        indicate success.
        """
        test_is_done = threading.Event()

        def second_callback(client, error):
            """second stopped callback"""
            assert client.get_state() == mqlight.STOPPED
            test_is_done.set()

        def first_callback(client, error):
            """first stopped callback"""
            assert client.get_state() == mqlight.STOPPED
            client.stop(second_callback)

        def started(client):
            """started listener"""
            assert client.get_state() == mqlight.STARTED
            client.stop(first_callback)
        client = mqlight.Client('amqp://host:1234',
                                'test_stop_when_already_stopped',
                                on_started=started)
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()

    def test_stop_too_many_arguments(self):
        """
        Test that if too many arguments are supplied to stop, an error is
        raised
        """
        test_is_done = threading.Event()
        def started(client):
            with pytest.raises(TypeError):
                def dummy():
                    pass
                # pylint: disable=too-many-function-args
                client.stop(dummy(), 'extra')
            client.stop()   # Remove created client
            test_is_done.set()

        client = mqlight.Client('amqp://host', 'test_stop_too_many_arguments', on_started=started)
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()
