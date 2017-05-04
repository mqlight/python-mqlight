
# python-mqlight - high-level API by which you can interact with MQ Light
#
# Copyright 2015-2017 IBM Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# pylint: disable=bare-except,broad-except,invalid-name,no-self-use
# pylint: disable=too-many-public-methods,unused-argument
import unittest
import pytest
import threading
import traceback
import time
import mqlight
from mqlight.exceptions import StoppedError, RangeError, InvalidArgumentError,\
    UnsubscribedError, MQLightError


class TestUnsubscribe(unittest.TestCase):
    """
    Unit tests for client.unsubscribe()
    """
    TEST_TIMEOUT = 10.0

    def func004(self, client, a ,b, c):
        pass

    def test_unsubscribe_too_few_arguments(self):
        """
        Test a calling client.unsubscribe(...) with too few arguments
        (no arguments) causes an Error to be thrown.
        """
        # pylint: disable=no-value-for-parameter
        test_is_done = threading.Event()
        def started(client):
            with pytest.raises(TypeError):
                client.unsubscribe()
            def stopped(client, error):
                 test_is_done.set()
            client.stop(on_stopped=stopped)

        client = mqlight.Client(
            'amqp://host',
            'test_unsubscribe_too_few_arguments',on_started = started)
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()

    def test_unsubscribe_too_many_arguments(self):
        """
        Test that calling client.unsubscribe(...) with too many arguments
        causes an Error to be thrown.
        """
        # pylint: disable=too-many-function-args
        test_is_done = threading.Event()

        def started(client):
            """started listener"""
            client.subscribe('/foo', 'share1')
            with pytest.raises(TypeError):
                client.unsubscribe('/foo', 'share1', {}, func, 'stowaway')
            client.stop()
            test_is_done.set()
        client = mqlight.Client(
            'amqp://host',
            'test_unsubscribe_too_many_arguments',
            on_started=started)
        func = self.func004
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()

    class Counter(object):
        def __init__(self, count):
            self.count = count

        def decrementAndGet(self):
            self.count -= 1
            return self.count

    def test_unsubscribe_callback_must_be_function(self):
        """
        Test that the callback argument to client.unsubscribe(...) must be a
        function
        """
        test_is_done = threading.Event()

        def on_stopped(self, error):
            """client stop callback"""
            test_is_done.set()

        def started(client):
            """started listener"""
            COUNTER = self.Counter(3)

            def func(c, e, t, s):
                if COUNTER.decrementAndGet() <= 0:
                    client.stop(on_stopped=on_stopped)

            def subscribed1(client, err, pattern, share):
                with pytest.raises(TypeError):
                    client.unsubscribe('/foo1', 'share', {}, on_unsubscribed=7)
            client.subscribe('/foo1', 'share', on_subscribed=subscribed1)

            def subscribed2(client, err, pattern, share):
                assert err is None
                client.unsubscribe('/foo2', on_unsubscribed=func)
            client.subscribe('/foo2', on_subscribed=subscribed2)

            def subscribed3(client, err, pattern, share):
                assert err is None
                client.unsubscribe('/foo3', 'share', on_unsubscribed=func)
            client.subscribe('/foo3', 'share', on_subscribed=subscribed3)

            def subscribed4(client, err, pattern, share):
                assert err is None
                client.unsubscribe('/foo4', 'share', {}, on_unsubscribed=func)
            client.subscribe('/foo4', 'share', on_subscribed=subscribed4)

        client = mqlight.Client(
            'amqp://host',
            'test_unsubscribe_callback_must_be_function',
            on_started=started
        )

        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()

    def test_unsubscribe_ok_callback(self):
        """
        Test that the callback (invoked when the unsubscribe operation
        completes successfully) specifies the right number of arguments.
        """
        test_is_done = threading.Event()

        def on_stopped(client, error):
            """client stop callback"""
            test_is_done.set()

        def started(client):
            """started listener"""
            def unsub(client, err, topic, share):
                """unsubscribe callback"""
                assert topic == '/foo'
                assert share is None

            def sub(client, err, topic, share):
                """subscribe callback"""
                client.unsubscribe('/foo', on_unsubscribed=unsub)
            client.subscribe('/foo', on_subscribed=sub)

            def unsub2(client, err, topic, share):
                """unsubscribe callback"""
                assert err is None
                assert topic == '/foo2'
                assert share == 'share'
                client.stop(on_stopped=on_stopped)

            def sub2(client, err, topic, share):
                """subscribe callback"""
                client.unsubscribe('/foo2', 'share', on_unsubscribed=unsub2)
            client.subscribe('/foo2', 'share', on_subscribed=sub2)

        client = mqlight.Client(
            'amqp://host',
            'test_unsubscribe_ok_callback',
            on_started=started)
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()

    def test_unsubscribe_when_stopped(self):
        """
        Test that trying to remove a subscription, while the client is in
        stopped state, throws an Error.
        """
        test_is_done = threading.Event()

        def started(client):
            def stopped(client, error):
                """stopped listener"""
                with pytest.raises(StoppedError):
                    client.unsubscribe('/foo')
                test_is_done.set()
            client.stop(stopped)
        client = mqlight.Client('amqp://host', 'test_unsubscribe_when_stopped', on_started=started)
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()

    def test_unsubscribe_when_not_subscribed(self):
        """
        Test that trying to remove a subscription that does not exist throws an
        Error.
        """
        test_is_done = threading.Event()

        def started(client):
            """started listener"""
            subscribe_event = threading.Event()
            client.subscribe(
                '/bar',
                on_subscribed=lambda _w, _x, _y, _z: subscribe_event.set())
            subscribe_event.wait(2.0)
            assert subscribe_event.is_set()
            with pytest.raises(UnsubscribedError):
                client.unsubscribe('/foo')
            client.stop()
            test_is_done.set()
        client = mqlight.Client(
            'amqp://host',
            'test_unsubscribe_when_not_subscribed',
            on_started=started)
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()

    def test_unsubscribe_returns_client(self):
        """
        Test that calling the unsubscribe(...) method returns, as a value, the
        client object that the method was invoked on (for method chaining
        purposes).
        """
        test_is_done = threading.Event()
        def started(client):
            """started listener"""
            subscribe_event = threading.Event()
            client.subscribe(
                '/foo',
                on_subscribed=lambda _w, _x, _y, _z: subscribe_event.set())
            subscribe_event.wait(2.0)
            assert  subscribe_event.is_set()
            assert client.unsubscribe('/foo') == client
            client.stop()
            test_is_done.set()
        client = mqlight.Client(
            'amqp://host',
            'test_unsubscribe_returns_client',
            on_started=started)
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()

    def test_unsubscribe_topics(self):
        """
        Test a variety of valid and invalid patterns.  Invalid patterns
        should result in the client.unsubscribe(...) method throwing a
        TypeError.
        """
        test_is_done = threading.Event()
        data = [
            {'name': 'EmptyStr',     'valid': False, 'pattern': ''},
            {'name': 'None',         'valid': False, 'pattern': None},
            {'name': 'Number',       'valid': False, 'pattern': 1234},
            {'name': 'Func',         'valid': True, 'pattern': lambda *args: 'topic'},
            {'name': 'Pat:kittens',  'valid': True, 'pattern': 'kittens'},
            {'name': 'Pat:/kittens', 'valid': True, 'pattern': '/kittens'},
            {'name': 'Pat:+',        'valid': True, 'pattern': '+'},
            {'name': 'Pat:#',        'valid': True, 'pattern': '#'},
            {'name': 'Pat:/#',       'valid': True, 'pattern': '/#'},
            {'name': 'Pat:/+',       'valid': True, 'pattern': '/+'}
        ]

        def started(client):
            """started listener"""
            try:
                for test in data:
                    if test['valid']:
                        test_pattern = test['pattern']
                        client.subscribe(
                            test['pattern'],
                            on_subscribed=lambda pattern=test_pattern:
                            client.unsubscribe(pattern))
                    else:
                        with pytest.raises(TypeError):
                            client.unsubscribe(test['pattern'])
            except InvalidArgumentError:
                pass
            except Exception as exc:
                pytest.fail('Test: {0} reports unexpected Exception {1}'.format(test['name'], exc))
            finally:
                client.stop()
                test_is_done.set()
        client = mqlight.Client(
            'amqp://host',
            'test_unsubscribe_topics',
            on_started=started)

        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()

    def test_unsubscribe_share_names(self):
        """
        Tests a variety of valid and invalid share names to check that they are
        accepted or rejected (by throwing an Error) as appropriate.
        """
        test_is_done = threading.Event()
        data = [
            {'valid': True, 'share': 'abc'},
            {'valid': False, 'share': 7},
            {'valid': False, 'share': ':'},
            {'valid': False, 'share': 'a:'},
            {'valid': False, 'share': ':a'}
        ]

        def started(client):
            """started listener"""
            try:
                for test in data:
                    if test['valid']:
                        test_share = test['share']
                        def on_unsubscribed(err, pattern, share):
                            sub_test_is_done.set()
                        sub_test_is_done = threading.Event()
                        client.subscribe(
                            '/foo',
                            test_share,
                            on_subscribed=lambda err=None, pattern=None, share=test_share:
                            client.unsubscribe('/foo', share, on_unsubscribed=on_unsubscribed))
                        sub_test_is_done.wait(self.TEST_TIMEOUT)
                    else:
                        with pytest.raises(InvalidArgumentError):
                            client.unsubscribe('/foo', test['share'])
            except UnsubscribedError:
                pass
            except Exception as exc:
                pytest.fail('Unexpected Exception ' + str(exc))
            finally:
                client.stop()
                test_is_done.set()
        client = mqlight.Client(
            'amqp://host',
            'test_unsubscribe_share_names',
            on_started=started)

        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()

    def test_unsubscribe_options(self):
        """
        Test a variety of valid and invalid options values. Invalid options
        should result in the client.unsubscribe(...) method throwing a
        TypeError.

        Note that this test just checks that the options parameter is only
        accepted when it is of the correct type. The actual validation of
        individual options will be in separate tests.
        """
        test_is_done = threading.Event()
        func = self.func004
        data = [
            {'valid': False, 'pattern' : 'TestEmpty',    'options': ''},
            {'valid': True,  'pattern' : 'TestNone',     'options': None},
            {'valid': False, 'pattern' : 'TestFunc',     'options': func},
            {'valid': False, 'pattern' : 'TestString',   'options': '1'},
            {'valid': False, 'pattern' : 'TestNumber',   'options': 2},
            {'valid': False, 'pattern' : 'TestBoolean',  'options': True},
            {'valid': True,  'pattern' : 'TestEmptyList','options': {}},
            {'valid': True,  'pattern' : 'TestList',     'options': {'a': 1}},
        ]

        def started(client):
            """started listener"""
            try:
                for test in data:
                    if test['valid']:
                        test_options = test['options']

                        def on_subscribed(err,topic_pattern, share):
                            client.unsubscribe(topic_pattern,
                                               'share',
                                               options=test['options'],
                                               on_unsubscribed=func)
                            sub_test_is_done.set()

                        sub_test_is_done = threading.Event()
                        client.subscribe(
                            test['pattern'],
                            'share',
                            on_subscribed=on_subscribed)
                        sub_test_is_done.wait(self.TEST_TIMEOUT)
                    else:
                        with pytest.raises(TypeError):
                            client.unsubscribe(test['pattern'],
                                               'share',
                                               options=test['options'],
                                               on_unsubscribed=func)
            except Exception as exc:
                pytest.fail('Unexpected Exception ' + str(exc))
            finally:
                client.stop()
                test_is_done.set()
        client = mqlight.Client(
            'amqp://host',
            'test_unsubscribe_options',
            on_started=started)
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()

    def test_unsubscribe_ttl_validity(self):
        """
        Test a variety of valid and invalid ttl options.  Invalid ttl values
        should result in the client.unsubscribe(...) method throwing a
        TypeError.
        """
        test_is_done = threading.Event()
        func = self.func004
        data = [
            {'valid': True,  'pattern': 'TTL0',       'ttl': 0},
            {'valid': False, 'pattern': 'TTLNone',    'ttl': None},
            {'valid': False, 'pattern': 'TTLfunc',    'ttl': func},
            {'valid': False, 'pattern': 'TTLNegLarge','ttl': -9007199254740992},
            {'valid': False, 'pattern': 'TTLNegNan',  'ttl': float('-nan')},
            {'valid': False, 'pattern': 'TTLPosNan',  'ttl': float('nan')},
            {'valid': False, 'pattern': 'TTLNegInf',  'ttl': float('-inf')},
            {'valid': False, 'pattern': 'TTLPosInf',  'ttl': float('inf')},
            {'valid': False, 'pattern': 'TTLNeg1',    'ttl': -1},
            {'valid': False, 'pattern': 'TTLPos1',    'ttl': 1},
            {'valid': False, 'pattern': 'TTLPosLarge','ttl': 9007199254740992},
            {'valid': False, 'pattern': 'TTLEmpty',   'ttl': ''}
        ]

        def started(client):
            """started listener"""
            try:
                for test in data:
                    test_opts = {'ttl': test['ttl']}
                    if test['valid']:
                        def on_subscribed(client, err, topic_pattern, share):
                            client.unsubscribe(topic_pattern,
                                               'share',
                                               test_opts,
                                               on_unsubscribed=func)
                            sub_test_is_done.set()

                        sub_test_is_done = threading.Event()
                        client.subscribe(
                            test['pattern'],
                            'share',
                            on_subscribed=on_subscribed
                        )
                        sub_test_is_done.wait(self.TEST_TIMEOUT)
                    else:
                        with pytest.raises(RangeError):
                            client.unsubscribe(
                                test['pattern'], options=test_opts)
            except Exception as exc:
                traceback.print_exc(exc)
                pytest.fail('Unexpected Exception ' + str(exc))
            finally:
                client.stop()
                test_is_done.set()
        client = mqlight.Client(
            'amqp://host',
            'test_unsubscribe_ttl_validity',
            on_started=started)

        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()
