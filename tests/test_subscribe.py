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
import unittest
import time
from mock import Mock
import mqlight
import mqlightexceptions as mqlexc

class TestSubscribe(unittest.TestCase):

    def test_subscribe_too_few_arguments(self):
        """
        Test a calling client.subscribe(...) with too few arguments
        (no arguments) causes an Error to be thrown.
        """
        client = mqlight.Client('amqp://host')
        def started(err):
            with self.assertRaises(TypeError):
                client.subscribe()
            client.stop()
        client.add_listener(mqlight.STARTED, started)

    def test_subscribe_too_many_arguments(self):
        """
        Test that calling client.subscribe(...) with too many arguments raises
        an Error
        """
        client = mqlight.Client('amqp://host')
        def started(err):
            callback = Mock()
            with self.assertRaises(TypeError):
                client.subscribe('/foo', 'share1', {}, callback, 'extra')
            client.stop()
        client.add_listener(mqlight.STARTED, started)

    def test_subscribe_callback_must_be_a_function(self):
        """
        Test that the callback argument to client.subscribe(...) must be a
        function
        """
        client = mqlight.Client('amqp://host')
        def started(err):
            with self.assertRaises(TypeError):
                client.subscribe('/foo1', 'share', {}, 7)
            callback = Mock()
            client.subscribe('/foo2', callback)
            client.subscribe('/foo3', 'share', callback)
            client.subscribe('/foo4', 'share', {}, callback)
            time.sleep(2)
            self.assertEqual(3, callback.call_count)
            client.stop()
        client.add_listener(mqlight.STARTED, started)

    def test_subscribe_ok_callback(self):
        """
        Test that the callback (invoked when the subscribe operation completes
        successfully) specifies the right number of arguments
        """
        client = mqlight.Client('amqp://host')
        def started(err):
            callback = Mock()
            client.subscribe('/foo', callback)
            time.sleep(2)
            callback.assert_called_with(None, '/foo', None)
            client.stop()
        client.add_listener(mqlight.STARTED, started)

    def test_subscribe_fail_callback(self):
        """
        Test that the callback (invoked when the subscribe operation completes
        unsuccessfully) specifies the right number of arguments
        """
        client = mqlight.Client('amqp://host')
        def started(err):
            callback = Mock()
            client.subscribe('/bad', 'share', callback)
            time.sleep(2)
            err = callback.call_args[0][0]
            self.assertTrue(isinstance(err, mqlexc.MQLightError))
            callback.assert_called_with(err, '/bad', 'share')
            client.stop()
        client.add_listener(mqlight.STARTED, started)

    def test_subscribe_when_stopped(self):
        """
        Test that trying to establish a subscription, while the client is in
        stopped state, throws an Error
        """
        client = mqlight.Client('amqp://host')
        def stopped(err):
            with self.assertRaises(mqlexc.MQLightError):
                client.subscribe('/foo')
        client.add_listener(mqlight.STOPPED, stopped)
        client.stop()

    def test_subscribe_returns_client(self):
        """
        Test that calling the subscribe(...) method returns, as a value, the
        client object that the method was invoked on
        """
        client = mqlight.Client('amqp://host')
        def started(err):
            self.assertEqual(client.subscribe('/foo'), client)
            client.stop()
        client.add_listener(mqlight.STARTED, started)

    def test_subscribe_topics(self):
        """
        Test a variety of valid and invalid patterns.  Invalid patterns
        should result in the client.subscribe(...) method throwing a ValueError.
        """
        func = Mock()
        data = [
            {'valid': False, 'pattern': ''},
            {'valid': False, 'pattern': None},
            {'valid': True, 'pattern': 1234},
            {'valid': True, 'pattern': func},
            {'valid': True, 'pattern': 'kittens'},
            {'valid': True, 'pattern': '/kittens'},
            {'valid': True, 'pattern': '+'},
            {'valid': True, 'pattern': '#'},
            {'valid': True, 'pattern': '/#'},
            {'valid': True, 'pattern': '/+'}
        ]
        client = mqlight.Client('amqp://host')
        def started(err):
            for test in data:
                if test['valid']:
                    try:
                        client.subscribe(test['pattern'])
                    except Exception:
                        self.assertTrue(False)
                else:
                    with self.assertRaises(mqlexc.InvalidArgumentError):
                        client.subscribe(test['pattern'])
            client.stop()
        client.add_listener(mqlight.STARTED, started)

    def test_subscribe_share_names(self):
        """
        Tests a variety of valid and invalid share names to check that they are
        accepted or rejected (by throwing an Error) as appropriate.
        """
        data = [
            {'valid': True, 'share': 'abc'},
            {'valid': True, 'share': 7},
            {'valid': False, 'share': ':'},
            {'valid': False, 'share': 'a:'},
            {'valid': False, 'share': ':a'}
        ]
        client = mqlight.Client('amqp://host')
        def started(err):
            for test in data:
                if test['valid']:
                    try:
                        client.subscribe('/foo', test['share'])
                    except Exception:
                        self.assertTrue(False)
                else:
                    with self.assertRaises(mqlexc.InvalidArgumentError):
                        client.subscribe('/foo', test['share'])
            client.stop()
        client.add_listener(mqlight.STARTED, started)

    def test_subscribe_options(self):
        """
        Test a variety of valid and invalid options values. Invalid options
        should result in the client.subscribe(...) method throwing a ValueError.
        """
        func = Mock()
        data = [
            {'valid': True, 'options': ''},
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
            for i in range(len(data)):
                test = data[i]
                if test['valid']:
                    try:
                        client.subscribe('/foo' + str(i), 'share', test['options'], func)
                    except Exception:
                        self.assertTrue(False)
                else:
                    with self.assertRaises(TypeError):
                        client.subscribe('/foo' + str(i), 'share', test['options'], func)
            client.stop()
        client.add_listener(mqlight.STARTED, started)

    def test_subscribe_qos(self):
        """
        Test a variety of valid and invalid QoS options.  Invalid QoS values
        should result in the client.subscribe(...) method throwing a ValueError
        """
        func = Mock()
        data = [
            {'valid': False, 'qos': ''},
            {'valid': False, 'qos': None},
            {'valid': False, 'qos': func},
            {'valid': False, 'qos': '1'},
            {'valid': False, 'qos': 2},
            {'valid': True, 'qos': 0},
            {'valid': True, 'qos': 9-8},
            {'valid': True, 'qos': mqlight.QOS_AT_MOST_ONCE},
            {'valid': True, 'qos': mqlight.QOS_AT_LEAST_ONCE}
        ]
        client = mqlight.Client('amqp://host')
        def started(err):
            for i in range(len(data)):
                test = data[i]
                opts = { 'qos': test['qos'] }
                if test['valid']:
                    try:
                        client.subscribe('/foo' + str(i), opts)
                    except Exception as exc:
                        print 'exc is ', str(exc)
                        self.assertTrue(False)
                else:
                    with self.assertRaises(mqlexc.InvalidArgumentError):
                        client.subscribe('/foo' + str(i), opts)
            client.stop()
        client.add_listener(mqlight.STARTED, started)

    def test_subscribe_auto_confirm(self):
        """
        Test a variety of valid and invalid auto_confirm options. Invalid
        auto_confirm values should result in the client.subscribe(...) method
        throwing a ValueError
        """
        func = Mock()
        tmp = True
        data = [
            {'valid': False, 'opts': {'auto_confirm': ''}},
            {'valid': False, 'opts': {'auto_confirm': None}},
            {'valid': False, 'opts': {'auto_confirm': func}},
            {'valid': False, 'opts': {'auto_confirm': 'True'}},
            {'valid': False, 'opts': {'auto_confirm': 'False'}},
            {'valid': True, 'opts': {'auto_confirm': True}},
            {'valid': True, 'opts': {'auto_confirm': False}},
            {'valid': True, 'opts': {'qos': 0, 'auto_confirm': True}},
            {'valid': True, 'opts': {'qos': 0, 'auto_confirm': False}},
            {'valid': True, 'opts': {'qos': 1, 'auto_confirm': True}},
            {'valid': True, 'opts': {'qos': 1, 'auto_confirm': False}},
            {'valid': True, 'opts': {'auto_confirm': 1 == 1}},
            {'valid': True, 'opts': {'auto_confirm': 'abc' == 'abc'}},
            {'valid': True, 'opts': {'auto_confirm': tmp}}
        ]
        client = mqlight.Client('amqp://host')
        def started(err):
            for i in range(len(data)):
                test = data[i]
                if test['valid']:
                    try:
                        client.subscribe('/foo' + str(i), test['opts'])
                    except Exception as exc:
                        print 'exc is ', str(exc)
                        self.assertTrue(False)
                else:
                    with self.assertRaises(mqlexc.InvalidArgumentError):
                        client.subscribe('/foo' + str(i), test['opts'])
            client.stop()
        client.add_listener(mqlight.STARTED, started)

    def test_subscribe_ttl_validity(self):
        """
        Test a variety of valid and invalid ttl options.  Invalid ttl values
        should result in the client.subscribe(...) method throwing a ValueError
        """
        func = Mock()
        data = [
            {'valid': False, 'ttl': None},
            {'valid': False, 'ttl': func},
            {'valid': False, 'ttl': -9007199254740992},
            {'valid': False, 'ttl': float('nan')},
            {'valid': False, 'ttl': float('-nan')},
            {'valid': False, 'ttl': float('inf')},
            {'valid': False, 'ttl': float('-inf')},
            {'valid': False, 'ttl': ''},
            {'valid': True, 'ttl': 0},
            {'valid': True, 'ttl': 1},
            {'valid': True, 'ttl': 9-8},
            {'valid': True, 'ttl': 9007199254740992}
        ]
        client = mqlight.Client('amqp://host')
        def started(err):
            for i in range(len(data)):
                test = data[i]
                opts = { 'ttl': test['ttl'] }
                if test['valid']:
                    try:
                        client.subscribe('/foo' + str(i), opts)
                    except Exception as exc:
                        print 'exc is ', str(exc)
                        self.assertTrue(False)
                else:
                    with self.assertRaises(TypeError):
                        client.subscribe('/foo' + str(i), opts)
            client.stop()
        client.add_listener(mqlight.STARTED, started)

if __name__ == 'main':
    unittest.main()


