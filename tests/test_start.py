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
import unittest
from mock import Mock, patch
import mqlight
import mqlight.mqlightexceptions as mqlexc

CONNECT_STATUS = 0


def side_effect(service, ssl_trust_certificate, ssl_verify_name):
    if 'bad' in service.netloc:
        raise TypeError('bad service ' + service.netloc)
    if CONNECT_STATUS != 0:
        raise mqlight.MQLightError('connect error ' + str(CONNECT_STATUS))
    pass


@patch('mqlight.mqlightproton._MQLightMessenger.connect',
       Mock(side_effect=side_effect))
@patch('mqlight.mqlightproton._MQLightMessenger.get_remote_idle_timeout',
       Mock(return_value=0))
class TestStart(unittest.TestCase):

    def setUp(self):
        global CONNECT_STATUS
        CONNECT_STATUS = 0

    def test_successful_start_stop(self):
        """
        Test a successful start / stop, ensuring that both the 'started'
        event and the callback passed into client.start(...) are driven.
        """
        client = mqlight.Client('amqp://host:1234')
        def started(err):
            self.assertEqual(err, None)
            self.assertEqual(client.get_state(), mqlight.STARTED)
            def stopped(err):
                self.assertEqual(err, None)
                self.assertEqual(client.get_state(), mqlight.STOPPED)
            client.stop(stopped)
        client.add_listener(mqlight.STARTED, started)
        self.assertIn(client.get_state(), (mqlight.STARTED, mqlight.STARTING))


    def test_start_argument_is_function(self):
        """
        Test that when an argument is specified to the client.start(...)
        function it must be a callback (e.g. of type function)
        """
        client = mqlight.Client('amqp://host:1234')
        self.assertRaises(TypeError, client.start, 1234)

    def test_start_method_returns_client(self):
        """
        Test that the start(...) method returns the instance of the client
        that it is invoked on.
        """
        client = mqlight.Client('amqp://host:1234')
        result = client.start()
        self.assertEqual(client, result)
        client.stop()

    def test_start_when_already_started(self):
        """
        Tests that calling start on an already start client has no effect
        other than to callback any supplied callback function to indicate
        success.
        """
        def started(err, cli):
            self.assertEqual(err, None)
            self.assertEqual(cli.get_state(), mqlight.STARTED)

            def inner_started(err):
                self.assertEqual(err, None)
                self.assertFalse(
                    True,
                    'should not receive started event if already started')
            cli.add_listener(mqlight.STARTED, inner_started)
            cli.start()

            def stopped(err):
                self.assertEqual(err, None)
            cli.stop(stopped)
        mqlight.Client('amqp://host:1234', callback=started)

    def test_start_too_many_arguments(self):
        """
        Test that if too many arguments are supplied to start - then an
        exception is raised.
        """
        client = mqlight.Client('amqp://host')
        callback = Mock()
        self.assertRaises(TypeError, client.start, callback, 'gooseberry')

    def test_start_retry(self):
        """
        Tests that calling start on an endpoint that is currently down retries
        until successful.
        """
        required_connect_status = 2
        global CONNECT_STATUS
        CONNECT_STATUS = required_connect_status
        client = mqlight.Client('amqp://host:1234')

        def error_callback(err):
            global CONNECT_STATUS
            CONNECT_STATUS -= 1
        client.add_listener(mqlight.ERROR, error_callback)

        def start_callback(err):
            self.assertEqual(err, None)
            global CONNECT_STATUS
            self.assertEqual(CONNECT_STATUS, 0)
            client.stop()
        client.start(start_callback)

if __name__ == 'main':
    unittest.main()
