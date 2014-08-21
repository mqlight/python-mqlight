import unittest
from mock import Mock, patch
import mqlight

CONNECT_STATUS = 0

def side_effect(service, ssl_trust_certificate, ssl_verify_name):
    if 'bad' in service.netloc:
        raise TypeError('bad service ' + service.netloc)
    else:
        if CONNECT_STATUS != 0:
            raise mqlight.MQLightError('connect error ' + str(CONNECT_STATUS))
        else:
            return None

@patch('mqlight._MQLightMessenger.connect', Mock(side_effect=side_effect))
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
        self.assertRaises(ValueError, client.start, 1234)

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
        client = mqlight.Client('amqp://host:1234', callback=started)

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
        client = mqlight.Client(service='amqp://host')
        def error_callback(err):
            required_connect_status -= 1
            global CONNECT_STATUS
            CONNECT_STATUS = required_connect_status
        client.add_listener(mqlight.ERROR, error_callback)

        def start_callback(err):
            self.assertEqual(required_connect_status, 0)
            client.stop()
        client.start(start_callback)

if __name__ == 'main':
    unittest.main()
