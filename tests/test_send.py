import unittest
import threading
"""
<copyright
notice="lm-source-program"
pids="5725-P60"
years="2013,2014"
crc="3568777996" >
Licensed Materials - Property of IBM

5725-P60

(C) Copyright IBM Corp. 2013, 2014

US Government Users Restricted Rights - Use, duplication or
disclosure restricted by GSA ADP Schedule Contract with
IBM Corp.
</copyright>
"""
from mock import Mock
import mqlight
import mqlight.mqlightexceptions as mqlexc

class TestSend(unittest.TestCase):
    """
    Unit tests for client.send()
    """

    def test_send_too_few_arguments(self):
        """
        Test a calling client.send(...) with too few arguments
        (no arguments) causes an Error to be thrown.
        """
        client = mqlight.Client('amqp://host')
        def started(value):
            with self.assertRaises(TypeError) as te:
                client.send()
            with self.assertRaises(TypeError) as te:
                client.send('topic')
            client.stop()
        client.add_listener(mqlight.STARTED, started)

    def test_send_too_many_arguments(self):
        """
        Test that calling client.send(...) with too many arguments raises
        an Error
        """
        client = mqlight.Client('amqp://host')
        def started(err):
            callback = Mock()
            with self.assertRaises(TypeError) as e:
                client.send('topic', 'message', {}, callback, 'extra')
            client.stop()
        client.add_listener(mqlight.STARTED, started)

    def test_send_topics(self):
        """
        Test a variety of valid and invalid topic names. Invalid topic names
        should result in the client.send(...) method throwing a TypeError.
        """
        func = Mock()
        data = [
            {'valid': False, 'topic': ''},
            {'valid': False, 'topic': None},
            {'valid': True, 'topic': 1234},
            {'valid': True, 'topic': func},
            {'valid': True, 'topic': 'kittens'},
            {'valid': True, 'topic': '/kittens'}
        ]
        client = mqlight.Client('amqp://host')
        def started(err):
            for test in data:
                if test['valid']:
                    try:
                        client.send(test['topic'], 'message')
                    except Exception as exc:
                        print exc
                        self.assertTrue(False)
                else:
                    with self.assertRaises(mqlexc.InvalidArgumentError) as ve:
                        client.send(test['topic'], 'message')
            client.stop()
        client.add_listener(mqlight.STARTED, started)

    def test_send_callback(self):
        """
        Tests that, if a callback function is supplied to client.test(...) then
        the function is invoked when the send operation completes, and this
        references the client.
        """
        data = [
            {'topic': 'topic1', 'data': 'data1', 'options': {}},
            {'topic': 'topic2', 'data': 'data2', 'options': None}
        ]
        def timeout():
            self.assertTrue(False)
        timer = threading.Timer(5, timeout)
        client = mqlight.Client('amqp://host')
        self.count = 0
        def callback(err, topic, d, options):
            self.assertEqual(err, None)
            self.assertEqual(topic, data[self.count]['topic'])
            self.assertEqual(d, data[self.count]['data'])
            self.assertEqual(options, data[self.count]['options'])
            self.count += 1
            print 'COUNT ' + str(self.count)
            if self.count == len(data):
                timer.cancel()
                client.stop()

        def started(err):
            for test in data:
                try:
                    client.send(test['topic'], test['data'], test['options'], callback)
                except Exception as exc:
                    self.assertTrue(False)
        client.add_listener(mqlight.STARTED, started)

    def test_send_fail_if_stopped(self):
        """
        Tests that client.send(...) throws and error if it is called while the
        client is in stopped state.
        """
        client = mqlight.Client('amqp://host')
        def started(err):
            def stopped(err):
                with self.assertRaises(mqlexc.StoppedError):
                    client.send('topic', 'message')
            client.stop(stopped)
        client.add_listener(mqlight.STARTED, started)

    def test_send_options(self):
        """
        Test a variety of valid and invalid options values. Invalid options
        should result in the client.send(...) method throwing a TypeError.
        Note that this test just checks that the options parameter is only
        accepted when it is of the correct type. The actual validation of
        individual options will be in separate tests
        """
        func = Mock()
        data = [
            {'valid': False, 'options': ''},
            {'valid': True, 'options': None},
            {'valid': False, 'options': func},
            {'valid': False, 'options': '1'},
            {'valid': False, 'options': 2},
            {'valid': False, 'options': True},
            {'valid': True, 'options': {}},
            {'valid': True, 'options': {'a': 1}}
        ]
        client = mqlight.Client('amqp://host')
        def started(err):
            for test in data:
                if test['valid']:
                    try:
                        client.send('test', 'message', test['options'], func)
                    except Exception as exc:
                        self.assertTrue(False)
                else:

                    with self.assertRaises(TypeError) as ve:
                        client.send('test', 'message', test['options'], func)
            client.stop()
        client.add_listener(mqlight.STARTED, started)

    def test_send_qos(self):
        """
        Test a variety of valid and invalid QoS values.  Invalid QoS values
        should result in the client.send(...) method throwing a ValueError.
        """
        func = Mock()
        data = [
            {'valid': False, 'qos': ''},
            {'valid': False, 'qos': None},
            {'valid': False, 'qos': func},
            {'valid': False, 'qos': '1'},
            {'valid': False, 'qos': 2},
            {'valid': True, 'qos': 0},
            {'valid': True, 'qos': 1},
            {'valid': True, 'qos': 9-8},
            {'valid': True, 'qos': mqlight.QOS_AT_MOST_ONCE},
            {'valid': True, 'qos': mqlight.QOS_AT_LEAST_ONCE}
        ]
        client = mqlight.Client('amqp://host')
        def started(err):
            for test in data:
                opts = { 'qos': test['qos'] }
                if test['valid']:
                    try:
                        client.send('test', 'message', opts, func)
                    except Exception as exc:
                        print exc
                        self.assertTrue(False)
                else:
                    with self.assertRaises(mqlexc.InvalidArgumentError):
                        client.send('test', 'message', opts, func)
            client.stop()
        client.add_listener(mqlight.STARTED, started)

    def test_send_qos_function(self):
        """
        Test that a function is required when QoS is 1.
        """
        func = Mock()
        data = [
            {'valid': False, 'qos': 1, 'callback': None},
            {'valid': True, 'qos': 1, 'callback': func},
            {'valid': True, 'qos': 0, 'callback': None},
            {'valid': True, 'qos': 0, 'callback': func}
        ]
        client = mqlight.Client('amqp://host')
        def started(err):
            for test in data:
                opts = { 'qos': test['qos'] }
                if test['valid']:
                    try:
                        client.send('test', 'message', opts, test['callback'])
                    except Exception:
                        self.assertTrue(False)
                else:
                    with self.assertRaises(mqlexc.InvalidArgumentError):
                        client.send('test', 'message', opts, test['callback'])
            client.stop()
        client.add_listener(mqlight.STARTED, started)

if __name__ == 'main':
    unittest.main()


