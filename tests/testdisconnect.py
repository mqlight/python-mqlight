import unittest
from mock import Mock
from mqlight import *

class TestDisconnect(unittest.TestCase):

    def test_disconnect_callback_event(self):
        """
        Test a successful disconnect, ensuring that both the 'disconnected'
        event and the callback passed into client.disconnect(...) are driven.
        """
        client = create_client('amqp://host:1234')
        self.assertEqual(client.get_state(), DISCONNECTED)
        callback = Mock()
        client.on(DISCONNECTED, callback)
        client.connect()
        client.disconnect()
        self.assertEqual(client.get_state(), DISCONNECTED)
        callback.assert_called_with((True,))

    def test_disconnect_argument_is_function(self):
        """
        Test that when an argument is specified to the client.disconnect(...)
        function it must be a callback (e.g. of type function).
        """
        client = create_client('amqp://host:1234')
        self.assertRaises(ValueError, client.disconnect, 1234)

    def test_disconnect_method_returns_client(self):
        """
        Test that the disconnect(...) method returns the instance of the client that
        it is invoked on.
        """
        client = create_client('amqp://host:1234')
        result = client.disconnect()
        self.assertEqual(client, result)

    def test_disconnect_when_already_disconnected(self):
        """
        Tests that calling disconnect on an already disconnected client has no
        effect other than to callback any supplied callback function to indicate
        success.
        """
        client = create_client('amqp://host:1234')
        callback = Mock()
        client.disconnect(callback)
        callback.assert_called_with()
        client.disconnect(callback)
        callback.assert_called_with()   

if __name__ == 'main':
    unittest.main()

        
