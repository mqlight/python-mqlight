import unittest
from mock import Mock, patch
import mqlight

@patch('mqlight._MQLightMessenger.connect', Mock(return_value=None))
@patch('mqlight._MQLightMessenger.get_last_error_text', Mock(return_value=None))
class TestStop(unittest.TestCase):

    def test_stop_callback_event(self):
        """
        Test a successful stop, ensuring that both the 'sopped'
        event and the callback passed into client.stopped(...) are driven.
        """
        client = mqlight.Client('amqp://host:1234')
        callback = Mock()
        client.add_listener(mqlight.STOPPED, callback)
        client.stop()
        self.assertEqual(client.get_state(), mqlight.STOPPED)
        callback.assert_called_with(True)

    def test_stop_argument_is_function(self):
        """
        Test that when an argument is specified to the client.stop(...)
        function it must be a callback (e.g. of type function).
        """
        client = mqlight.Client('amqp://host:1234')
        self.assertRaises(ValueError, client.stop, 1234)

    def test_stop_method_returns_client(self):
        """
        Test that the stop(...) method returns the instance of the client
        that it is invoked on.
        """
        client = mqlight.Client('amqp://host:1234')
        result = client.stop()
        self.assertEqual(client, result)

    def test_stop_when_already_stopped(self):
        """
        Tests that calling stop on an already stopped client has no
        effect other than to callback any supplied callback function to indicate
        success.
        """
        client = mqlight.Client('amqp://host:1234')
        callback = Mock()
        client.stop(callback)
        callback.assert_called_with(None)
        client.stop(callback)
        callback.assert_called_with(None)

    def test_stop_too_many_arguments(self):
        """
        Test that if too many arguments are supplied to stop, an error is
        raised
        """
        client = mqlight.Client('amqp://host')
        with self.assertRaises(TypeError):
            callback = Mock()
            client.stop(callback, 'extra')

if __name__ == 'main':
    unittest.main()


