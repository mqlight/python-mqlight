"""
<copyright
notice="lm-source-program"
pids="5724-H72"
years="2013,2016"
crc="1615301132" >
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
