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
import threading
import time
from mock import Mock, patch
import mqlight
import mqlight.mqlightexceptions as mqlexc


@patch('mqlight.mqlightproton._MQLightMessenger.connect',
       Mock())
@patch('mqlight.mqlightproton._MQLightMessenger.get_remote_idle_timeout',
       Mock(return_value=0))
class TestUnsubscribe(unittest.TestCase):
    """
    Unit tests for client.unsubscribe()
    """

    def test_unsubscribe_too_few_arguments(self):
        """
        Test a calling client.unsubscribe(...) with too few arguments
        (no arguments) causes an Error to be thrown.
        """
        client = mqlight.Client(
            'amqp://host',
            'test_unsubscribe_too_few_arguments')
        with self.assertRaises(TypeError):
            client.unsubscribe()
        client.stop()

    def test_unsubscribe_too_many_arguments(self):
        """
        Test that calling client.unsubscribe(...) with too many arguments
        causes an Error to be thrown.
        """
        func = Mock()
        client = mqlight.Client(
            'amqp://host',
            'test_unsubscribe_too_many_arguments')
        def started(err):
            client.subscribe('/foo', 'share1')
            with self.assertRaises(TypeError):
                client.unsubscribe('/foo', 'share1', {}, func, 'stowaway')
            client.stop()
        client.add_listener(mqlight.STARTED, started)

    def test_unsubscribe_callback_must_be_function(self):
        """
        Test that the callback argument to client.unsubscribe(...) must be a
        function
        """
        client = mqlight.Client(
            'amqp://host',
            'test_unsubscribe_callback_must_be_function'
        )

        def started(err):
            func = Mock()
            with self.assertRaises(TypeError):
                client.subscribe('/foo1', 'share')
                client.unsubscribe('/foo1', 'share', {}, 7)

            client.subscribe('/foo2')
            client.unsubscribe('/foo2', func)

            client.subscribe('/foo3', 'share')
            client.unsubscribe('/foo3', 'share', func)

            client.subscribe('/foo4', 'share')
            client.unsubscribe('/foo4', 'share', {}, func)

            client.stop()
        client.add_listener(mqlight.STARTED, started)

    def test_unsubscribe_parameters(self):
        """
        Test that unsubscribe correctly interprets its parameters.  This can be
        tricky for two and three parameter invocations where there is the
        potential for ambiguity between what is a share name and what is the
        options object.
        """
        service = 'amqp://host:5672'
        pattern = '/pattern'
        self.current_callback_invocations = 0
        expected_callback_invocations = 0
        def cb(err, topic, share):
            self.current_callback_invocations += 1

        share = 'share'
        obj = {}

        # Data to drive the test with. 'args' is the argument list to pass into
        # the unsubscribe function.  The 'share', 'object' and 'callback'
        # properties indicate the expected interpretation of 'args'.
        data = [
            { 'args': [ pattern, None, None, None ] },
            { 'args': [ pattern, cb, None, None ], 'callback': cb },
            { 'args': [ pattern, share, None, None ], 'share': share },
            { 'args': [ pattern, obj, None, None ], 'object': obj},
            {
                'args': [ pattern, share, cb, None ],
                'share': share,
                'callback': cb },
            {
                'args': [ pattern, obj, cb, None ],
                'object': obj,
                'callback': cb },
            {
                'args': [ pattern, share, obj, None ],
                'share': share,
                'object': obj },
            { 'args': [ pattern, 7, None, None ], 'share': 7 },
            { 'args': [ pattern, 'boo', None, None ], 'share': 'boo' },
            { 'args': [ pattern, {}, None, None ], 'object': {} },
            { 'args': [ pattern, 7, cb, None ], 'share': 7, 'callback': cb },
            { 'args': [ pattern, {}, cb, None ], 'object': {}, 'callback': cb },
            #{ 'args': [ pattern, [], [], None ], 'share': [], 'object': [] },
            {
                'args': [ pattern, share, obj, cb ],
                'share': share,
                'object': obj,
                'callback': cb
            }
        ]

        # Count up the expected number of callback invocations, so the test can
        # wait for these to complete.
        for test in data:
            if 'callback' in test:
                expected_callback_invocations += 1

        # Replace the messenger unsubscribe method with our own implementation
        # that simply records the address that mqlight.js tries to unsubscribe
        # from.
        last_unsubscribed_address = None

        client = mqlight.Client(service, 'test_unsubscribe_parameters')
        def started(err):
            for test in data:
                args = test['args']
                last_unsubscribed_address = None
                client.subscribe(args[0], args[1], args[2], args[3])
                time.sleep(2)
                client.unsubscribe(args[0], args[1], args[2], args[3])
                last_unsubscribed_address = client._messenger.last_address

                if 'share' in test:
                    sha = 'share:' + str(test['share']) + ':'
                else:
                    sha = 'private:'
                expected_address = service + '/' + sha + pattern

                self.assertEqual(last_unsubscribed_address, expected_address)

            client.stop()
        client.add_listener(mqlight.STARTED, started)

        # Callbacks passed into unsubscribe(...) are scheduled to be run once
        # outside of the main loop
        def test_is_done():
            total = 2 * expected_callback_invocations
            if self.current_callback_invocations == total:
                self.assertTrue(True)
            else:
                timer = threading.Timer(1, test_is_done)
                timer.daemon = True
                timer.start()
        test_is_done()

    def test_unsubscribe_ok_callback(self):
        """
        Test that the callback (invoked when the unsubscribe operation completes
        successfully) specifies the right number of arguments.
        """
        client = mqlight.Client(
            'amqp://host',
            'test_unsubscribe_ok_callback')
        def started(err):
            client.subscribe('/foo')
            def unsub(err, topic, share):
                self.assertEqual(err, None)
                self.assertEqual(topic, '/foo')
                self.assertEqual(share, None)
            client.unsubscribe('/foo', unsub)
            client.subscribe('/foo2', 'share')
            def unsub2(err, topic, share):
                self.assertEqual(err, None)
                self.assertEqual(topic, '/foo2')
                self.assertEqual(share, 'share')
            client.unsubscribe('/foo2', 'share', unsub2)
            client.stop()
        client.add_listener(mqlight.STARTED, started)


    def test_unsubscribe_when_stopped(self):
        """
        Test that trying to remove a subscription, while the client is in
        stopped state, throws an Error.
        """
        client = mqlight.Client('amqp://host', 'test_unsubscribe_when_stopped')
        def stopped(err):
            with self.assertRaises(mqlexc.StoppedError):
                client.unsubscribe('/foo')
        client.add_listener(mqlight.STOPPED, stopped)
        client.stop()

    def test_unsubscribe_when_not_subscribed(self):
        """
        Test that trying to remove a subscription that does not exist throws an
        Error.
        """
        client = mqlight.Client(
            'amqp://host',
            'test_unsubscribe_when_not_subscribed')
        def started(err):
            client.subscribe('/bar')
            with self.assertRaises(mqlexc.UnsubscribedError):
                client.unsubscribe('/foo')
            client.stop()
        client.add_listener(mqlight.STARTED, started)


    def test_unsubscribe_returns_client(self):
        """
        Test that calling the unsubscribe(...) method returns, as a value, the
        client object that the method was invoked on (for method chaining
        purposes).
        """
        client = mqlight.Client(
            'amqp://host',
            'test_unsubscribe_returns_client')
        def started(err):
            client.subscribe('/foo')
            self.assertEqual(client.unsubscribe('/foo'), client)
            client.stop()
        client.add_listener(mqlight.STARTED, started)


    def test_unsubscribe_topics(self):
        """
        Test a variety of valid and invalid patterns.  Invalid patterns
        should result in the client.unsubscribe(...) method throwing a
        TypeError.
        """
        func = Mock()
        data = [
            { 'valid': False, 'pattern': '' },
            { 'valid': False, 'pattern': None },
            { 'valid': True, 'pattern': 1234},
            { 'valid': True, 'pattern': func },
            { 'valid': True, 'pattern': 'kittens' },
            { 'valid': True, 'pattern': '/kittens' },
            { 'valid': True, 'pattern': '+' },
            { 'valid': True, 'pattern': '#' },
            { 'valid': True, 'pattern': '/#' },
            { 'valid': True, 'pattern': '/+' }
        ]

        client = mqlight.Client('amqp://host' , 'test_unsubscribe_topics')
        def started(err):
            for test in data:
                if test['valid']:
                    try:
                        client.subscribe(test['pattern'])
                        client.unsubscribe(test['pattern'])
                    except Exception:
                        self.assertTrue(False)
                else:
                    with self.assertRaises(mqlexc.MQLightError):
                        client.unsubscribe(test['pattern'])
            client.stop()
        client.add_listener(mqlight.STARTED, started)

    def test_unsubscribe_share_names(self):
        """
        Tests a variety of valid and invalid share names to check that they are
        accepted or rejected (by throwing an Error) as appropriate.
        """
        data = [
            { 'valid': True, 'share': 'abc' },
            { 'valid': True, 'share': 7 },
            { 'valid': False, 'share': ':' },
            { 'valid': False, 'share': 'a:' },
            { 'valid': False, 'share': ':a' }
        ]

        client = mqlight.Client('amqp://host', 'test_unsubscribe_share_names')
        def started(err):
            for test in data:
                if test['valid']:
                    try:
                        client.subscribe('/foo', test['share'])
                        client.unsubscribe('/foo', test['share'])
                    except Exception:
                        self.assertTrue(False)
                else:
                    with self.assertRaises(mqlexc.InvalidArgumentError):
                        client.unsubscribe('/foo', test['share'])

            client.stop()
        client.add_listener(mqlight.STARTED, started)

    def test_unsubscribe_options(self):
        """
        Test a variety of valid and invalid options values. Invalid options
        should result in the client.unsubscribe(...) method throwing a
        TypeError.

        Note that this test just checks that the options parameter is only
        accepted when it is of the correct type. The actual validation of
        individual options will be in separate tests.
        """
        func = Mock()
        data = [
            { 'valid': False, 'options': '' },
            { 'valid': True, 'options': None },
            { 'valid': False, 'options': func },
            { 'valid': False, 'options': '1' },
            { 'valid': False, 'options': 2 },
            { 'valid': False, 'options': True },
            { 'valid': True, 'options': {} },
            { 'valid': True, 'options': { 'a': 1 } }
        ]

        client = mqlight.Client('amqp://host', 'test_unsubscribe_options')
        def started(err):
            for test in data:
                if test['valid']:
                    try:
                        client.subscribe('testpattern', 'share')
                        client.unsubscribe(
                            'testpattern',
                            'share',
                            test['options'],
                            func)
                    except Exception:
                        self.assertTrue(False)
                else:
                    with self.assertRaises(mqlexc.MQLightError):
                        client.unsubscribe(
                            'testpattern',
                            'share',
                            test['options'],
                            func)
            client.stop()
        client.add_listener(mqlight.STARTED, started)

    def test_unsubscribe_ttl_validity(self):
        """
        Test a variety of valid and invalid ttl options.  Invalid ttl values
        should result in the client.unsubscribe(...) method throwing a
        TypeError.
        """
        func = Mock()
        data = [
            { 'valid': False, 'ttl': None },
            { 'valid': False, 'ttl': func },
            { 'valid': False, 'ttl': -9007199254740992 },
            { 'valid': False, 'ttl': float('-nan') },
            { 'valid': False, 'ttl': float('nan') },
            { 'valid': False, 'ttl': float('-inf') },
            { 'valid': False, 'ttl': float('inf') },
            { 'valid': False, 'ttl': -1 },
            { 'valid': False, 'ttl': 1 },
            { 'valid': False, 'ttl': 9007199254740992 },
            { 'valid': True, 'ttl': 0 },
            { 'valid': False, 'ttl': '' }
        ]

        client = mqlight.Client('amqp://host', 'test_unsubscribe_ttl_validity')
        def started(err):
            for test in data:
                opts = { 'ttl': test['ttl'] }
                if test['valid']:
                    try:
                        client.subscribe('testpattern')
                        client.unsubscribe('testpattern', opts)
                    except Exception:
                        self.assertTrue(False)
                else:
                    print 'testing ' + str(test['ttl'])
                    with self.assertRaises(ValueError):
                        client.unsubscribe('testpattern', opts)

            client.stop()
        client.add_listener(mqlight.STARTED, started)
