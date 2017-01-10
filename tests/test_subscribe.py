"""
<copyright
notice="lm-source-program"
pids="5724-H72"
years="2013,2016"
crc="1615301132" >
Licensed Materials - Property of IBM

5725-P60

(C) Copyright IBM Corp. 2013, 2016

US Government Users Restricted Rights - Use, duplication or
disclosure restricted by GSA ADP Schedule Contract with
IBM Corp.
</copyright>
"""
# pylint: disable=bare-except,broad-except,invalid-name,no-self-use
# pylint: disable=too-many-public-methods,unused-argument
import unittest
import threading
import pytest
import traceback
import mqlight
from mqlight.exceptions import MQLightError, InvalidArgumentError, RangeError


class TestSubscribe(unittest.TestCase):
    """
    Unit tests for client.subscribe()
    """
    TEST_TIMEOUT = 10.0

    def func003(self, client, a ,b):
        pass

    def func004(self, client, a ,b, c):
        pass

    def test_subscribe_too_few_arguments(self):
        """
        Test a calling client.subscribe(...) with too few arguments
        (no arguments) causes an Error to be thrown.
        """
        # pylint: disable=no-value-for-parameter
        test_is_done = threading.Event()

        def started(client):
            """started listener"""
            with pytest.raises(TypeError):
                client.subscribe()
            client.stop()
            test_is_done.set()
        client = mqlight.Client('amqp://host',
                                'test_subscribe_too_few_arguments',
                                on_started=started)
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()

    def test_subscribe_too_many_arguments(self):
        """
        Test that calling client.subscribe(...) with too many arguments raises
        an Error
        """
        test_is_done = threading.Event()

        def started(client):
            """started listener"""
            callback = self.func003
            with pytest.raises(TypeError):
                # pylint: disable=too-many-function-args
                client.subscribe('/foo', 'share1', {}, callback, 'extra')
            client.stop()
            test_is_done.set()
        client = mqlight.Client('amqp://host',
                                'test_subscribe_too_many_arguments',
                                on_started=started)
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()

    def test_subscribe_callback_must_be_a_function(self):
        """
        Test that the callback argument to client.subscribe(...) must be a
        function
        """
        test_is_done = threading.Event()

        def started(client):
            """started listener"""
            with pytest.raises(TypeError):
                client.subscribe('/foo1', 'share', {}, 7)
            callback = self.func004
            client.subscribe('/foo2', 'share', {}, callback)
            client.stop()
            test_is_done.set()
        client = mqlight.Client('amqp://host',
                                'test_subscribe_callback_must_be_a_function',
                                on_started=started)
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()

    def test_subscribe_ok_callback(self):
        """
        Test that the callback (invoked when the subscribe operation completes
        successfully) specifies the right number of arguments
        """
        test_is_done = threading.Event()

        def started(client):
            """started listener"""

            def subscribed(client, err, topic_pattern, share):
                """subscribe callback"""
                assert topic_pattern == '/foo'
                assert share is None
                client.stop()
                test_is_done.set()

            client.subscribe('/foo', on_subscribed=subscribed)
        client = mqlight.Client('amqp://host',
                                'test_subscribe_ok_callback',
                                on_started=started)
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()

    def test_subscribe_fail_callback(self):
        """
        Test that the callback (invoked when the subscribe operation completes
        unsuccessfully) specifies the right number of arguments
        """
        test_is_done = threading.Event()

        def started(client):
            """started listener"""
            def subscribed(client, err, topic_pattern, share):
                """subscribe callback"""
                assert isinstance(err, MQLightError)
                assert topic_pattern == '/bad'
                assert share == 'share'
                client.stop()
                test_is_done.set()
            client.subscribe('/bad', 'share', on_subscribed=subscribed)
        client = mqlight.Client('amqp://host',
                                'test_subscribe_fail_callback',
                                on_started=started)
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()

    def test_subscribe_when_stopped(self):
        """
        Test that trying to establish a subscription, while the client is in
        stopped state, throws an Error
        """
        test_is_done = threading.Event()
        def stopped(client, error):
            """stopped listener"""
            with pytest.raises(MQLightError):
                client.subscribe('/foo')
            test_is_done.set()
        def started(client):
            client.stop(on_stopped=stopped)
        client = mqlight.Client('amqp://host',
                                'test_subscribe_when_stopped', on_started=started)
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()


    def test_subscribe_returns_client(self):
        """
        Test that calling the subscribe(...) method returns, as a value, the
        client object that the method was invoked on
        """
        test_is_done = threading.Event()

        def started(client):
            """started listener"""
            assert client.subscribe('/foo') == client
            client.stop()
            test_is_done.set()
        client = mqlight.Client('amqp://host',
                                'test_subscribe_returns_client',
                                on_started=started)
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()

    def test_subscribe_topics(self):
        """
        Test a variety of valid and invalid patterns.  Invalid patterns
        should result in the client.subscribe(...) method throwing a
        TypeError
        """
        test_is_done = threading.Event()
        func = self.func003
        data = [
            {'valid': False, 'pattern': ''},
            {'valid': False, 'pattern': None},
            {'valid': False, 'pattern': 1234},
            {'valid': False, 'pattern': func},
            {'valid': True, 'pattern': 'kittens'},
            {'valid': True, 'pattern': '/kittens'},
            {'valid': True, 'pattern': '+'},
            {'valid': True, 'pattern': '#'},
            {'valid': True, 'pattern': '/#'},
            {'valid': True, 'pattern': '/+'}
        ]

        def started(client):
            """started listener"""
            try:
                for test in data:
                    if test['valid']:
                        client.subscribe(test['pattern'])
                    else:
                        with pytest.raises(InvalidArgumentError):
                            client.subscribe(test['pattern'])
            except Exception as exc:
                pytest.fail('Unexpected Exception ' + str(exc))
            finally:
                client.stop()
                test_is_done.set()
        client = mqlight.Client('amqp://host',
                                'test_subscribe_topics',
                                on_started=started)
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()

    def test_subscribe_share_names(self):
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
                        client.subscribe('/foo', test['share'])
                    else:
                        with pytest.raises(InvalidArgumentError):
                            client.subscribe('/foo', test['share'])
            except Exception as exc:
                pytest.fail('Unexpected Exception ' + str(exc))
            finally:
                client.stop()
                test_is_done.set()
        client = mqlight.Client('amqp://host',
                                'test_subscribe_share_names',
                                on_started=started)
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()

    def test_subscribe_options(self):
        """
        Test a variety of valid and invalid options values. Invalid options
        should result in the client.subscribe(...) method throwing a
        ValueError.
        """
        test_is_done = threading.Event()
        func = self.func003
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

        def started(client):
            """started listener"""
            try:
                for i in range(len(data)):
                    test = data[i]
                    if test['valid']:
                        client.subscribe('/foo' + str(i), 'share',
                                         test['options'], func)
                    else:
                        with pytest.raises(TypeError):
                            client.subscribe('/foo' + str(i), 'share',
                                             test['options'], func)
            except Exception as exc:
                pytest.fail('Unexpected Exception ' + str(exc))
            finally:
                client.stop()
                test_is_done.set()
        client = mqlight.Client('amqp://host',
                                'test_subscribe_options',
                                on_started=started)
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()

    def test_subscribe_qos(self):
        """
        Test a variety of valid and invalid QoS options.  Invalid QoS values
        should result in the client.subscribe(...) method throwing a ValueError
        """
        test_is_done = threading.Event()
        func = self.func003
        data = [
            {'valid': False, 'qos': ''},
            {'valid': False, 'qos': None},
            {'valid': False, 'qos': func},
            {'valid': False, 'qos': '1'},
            {'valid': False, 'qos': 2},
            {'valid': True, 'qos': 0},
            {'valid': True, 'qos': 9 - 8},
            {'valid': True, 'qos': mqlight.QOS_AT_MOST_ONCE},
            {'valid': True, 'qos': mqlight.QOS_AT_LEAST_ONCE}
        ]

        def started(client):
            """started listener"""
            try:
                for i in range(len(data)):
                    test = data[i]
                    opts = {'qos': test['qos']}
                    if test['valid']:
                        client.subscribe('/foo' + str(i), options=opts)
                    else:
                        with pytest.raises(RangeError):
                            client.subscribe('/foo' + str(i), options=opts)
            except Exception as exc:
                pytest.fail('Unexpected Exception ' + str(exc))
            finally:
                client.stop()
                test_is_done.set()
        client = mqlight.Client('amqp://host',
                                'test_subscribe_qos',
                                on_started=started)
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()

    def test_subscribe_auto_confirm(self):
        """
        Test a variety of valid and invalid auto_confirm options. Invalid
        auto_confirm values should result in the client.subscribe(...) method
        throwing a ValueError
        """
        test_is_done = threading.Event()
        func = self.func003
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

        def started(client):
            """started listener"""
            try:
                for i in range(len(data)):
                    test = data[i]
                    if test['valid']:
                        client.subscribe('/foo' + str(i), options=test['opts'])
                    else:
                        with pytest.raises(TypeError):
                            client.subscribe('/foo' + str(i),
                                             options=test['opts'])
            except Exception as exc:
                traceback.print_exc()
                pytest.fail('Unexpected Exception ' + str(exc))
            finally:
                client.stop()
                test_is_done.set()
        client = mqlight.Client('amqp://host',
                                'test_subscribe_auto_confirm',
                                on_started=started)
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()

    def test_subscribe_ttl_validity(self):
        """
        Test a variety of valid and invalid ttl options.  Invalid ttl values
        should result in the client.subscribe(...) method throwing a ValueError
        """
        test_is_done = threading.Event()
        func = self.func003
        data = [
            {'name': 'TTLBoolean',  'result': TypeError, 'ttl': None},
            {'name': 'TTLFunction', 'result': TypeError, 'ttl': func},
            {'name': 'TTLNegNum',   'result': RangeError, 'ttl': -9007199254740992},
            {'name': 'TTLPosNan',   'result': TypeError, 'ttl': float('nan')},
            {'name': 'TTLNegNan',   'result': TypeError, 'ttl': float('-nan')},
            {'name': 'TTLPosInf',   'result': TypeError, 'ttl': float('inf')},
            {'name': 'TTLNegInf',   'result': TypeError, 'ttl': float('-inf')},
            {'name': 'TTLEmptyStr', 'result': TypeError, 'ttl': ''},
            {'name': 'TTLZero',     'result': None,  'ttl': 0},
            {'name': 'TTLOne',      'result': None,  'ttl': 1},
            {'name': 'TTLSum',      'result': None,  'ttl': 9 - 8},
            {'name': 'TTLLargeNum', 'result': None,  'ttl': 9007199254740992}
        ]

        def started(client):
            """started listener"""
            name = ''
            try:
                for i in range(len(data)):
                    test = data[i]
                    name = test['name']
                    opts = {'ttl': test['ttl']}
                    if test['result'] is None:
                        client.subscribe('/foo' + str(i), options=opts)
                    else:
                        with pytest.raises(test['result']):
                            client.subscribe('/foo' + str(i), options=opts)
            except Exception as exc:
                pytest.fail('Test {0} : {1} Unexpected Exception {2}'.format(name, str(type(exc)), str(exc)))
            finally:
                client.stop()
                test_is_done.set()
        client = mqlight.Client('amqp://host',
                                'test_subscribe_ttl_validity',
                                on_started=started)
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()
