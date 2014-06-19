import unittest
from mock import Mock
from mqlight import *

class TestConnect(unittest.TestCase):


    def test_successful_connect_disconnect(self):
        """
        Test a successful connect / disconnect, ensuring that both the 'connected'
        event and the callback passed into client.connect(...) are driven. 
        """
        client = create_client('amqp://host:1234')
        self.assertEqual(client.get_state(), DISCONNECTED)
        callback = Mock()
        client.on(CONNECTED, callback)
        client.connect()
        self.assertEqual(client.get_state(), CONNECTED)
        callback.assert_called_with((True,))
        client.disconnect()
        self.assertEqual(client.get_state(), DISCONNECTED)

    def test_connect_argument_is_function(self):
        """
        Test that when an argument is specified to the client.connect(...) function
        it must be a callback (e.g. of type function)
        """
        client = create_client('amqp://host:1234')
        self.assertRaises(ValueError, client.connect, 1234)

    def test_connect_method_returns_client(self):
        """
        Test that the connect(...) method returns the instance of the client that
        it is invoked on. 
        """
        client = create_client('amqp://host:1234')
        result = client.connect()
        self.assertEqual(client, result)
        client.disconnect()

    def test_connect_when_already_connected(self):
        """
        Test that calling connect on an already connected client has no effect
        other than to callback any supplied callback function to indicate
        success
        """
        client = create_client('amqp://host:1234')
        callback = Mock()
        client.on(CONNECTED, callback)
        client.connect()
        callback.assert_called_with((True,))
        self.assertEqual(client.get_state(), CONNECTED)
        client.connect()
        callback.assert_called_with((True,))
        self.assertEqual(client.get_state(), CONNECTED)
        client.disconnect()      

if __name__ == 'main':
    unittest.main()

        
