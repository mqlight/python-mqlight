import unittest
from mock import Mock
from mqlight import *

class TestSubscribe(unittest.TestCase):
        
    def test_subscribe_too_few_arguments(self):
        """
        Test a calling client.subscribe(...) with too few arguments 
        (no arguments) causes an Error to be thrown.
        """
        client = create_client('amqp://localhost')  
        def connected(value):
            with self.assertRaise(ValueError) as ve:
                client.subscribe()
            client.disconnect()
        client.on(CONNECTED, connected)
        client.connect()
      
if __name__ == 'main':
    unittest.main()

        
