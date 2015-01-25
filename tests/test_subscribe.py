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
# pylint: disable=bare-except,broad-except,invalid-name,no-self-use
# pylint: disable=too-many-public-methods,unused-argument
import threading
import pytest
from mock import Mock
import mqlight
import mqlight.mqlightexceptions as mqlexc


class TestSubscribe(object):

    """
    Unit tests for client.subscribe()
    """
    TEST_TIMEOUT = 10.0

    def test_subscribe_too_few_arguments(self):
        """
        Test a calling client.subscribe(...) with too few arguments
        (no arguments) causes an Error to be thrown.
        """
        # pylint: disable=no-value-for-parameter
        test_is_done = threading.Event()

        def started(err, service):
            """started listener"""
            with pytest.raises(TypeError):
                client.subscribe()
            client.stop()
            test_is_done.set()
        client = mqlight.Client('amqp://host',
                                'test_subscribe_too_few_arguments',
                                callback=started)
        done = test_is_done.wait(self.TEST_TIMEOUT)
        assert done

    def test_subscribe_too_many_arguments(self):
        """
        Test that calling client.subscribe(...) with too many arguments raises
        an Error
        """
        test_is_done = threading.Event()

        def started(err, service):
            """started listener"""
            callback = Mock()
            with pytest.raises(TypeError):
                # pylint: disable=too-many-function-args
                client.subscribe('/foo', 'share1', {}, callback, 'extra')
            client.stop()
            test_is_done.set()
        client = mqlight.Client('amqp://host',
                                'test_subscribe_too_many_arguments',
                                callback=started)
        done = test_is_done.wait(self.TEST_TIMEOUT)
        assert done

    def test_subscribe_callback_must_be_a_function(self):
        """
        Test that the callback argument to client.subscribe(...) must be a
        function
        """
        test_is_done = threading.Event()

        def started(err, service):
            """started listener"""
            with pytest.raises(TypeError):
                client.subscribe('/foo1', 'share', {}, 7)
            callback = Mock()
            client.subscribe('/foo2', 'share', {}, callback)
            client.stop()
            test_is_done.set()
        client = mqlight.Client('amqp://host',
                                'test_subscribe_callback_must_be_a_function',
                                callback=started)
        done = test_is_done.wait(self.TEST_TIMEOUT)
        assert done

    def test_subscribe_ok_callback(self):
        """
        Test that the callback (invoked when the subscribe operation completes
        successfully) specifies the right number of arguments
        """
        test_is_done = threading.Event()

        def started(err, service):
            """started listener"""

            def subscribed(err, topic_pattern, share):
                """subscribe callback"""
                assert err is None
                assert topic_pattern == '/foo'
                assert share is None
                client.stop()
                test_is_done.set()

            client.subscribe('/foo', callback=subscribed)
        client = mqlight.Client('amqp://host',
                                'test_subscribe_ok_callback',
                                callback=started)
        done = test_is_done.wait(self.TEST_TIMEOUT)
        assert done

    def test_subscribe_fail_callback(self):
        """
        Test that the callback (invoked when the subscribe operation completes
        unsuccessfully) specifies the right number of arguments
        """
        test_is_done = threading.Event()

        def started(err, service):
            """started listener"""
            def subscribed(err, topic_pattern, share):
                """subscribe callback"""
                assert isinstance(err, mqlexc.MQLightError)
                assert topic_pattern == '/bad'
                assert share == 'share'
                client.stop()
                test_is_done.set()
            client.subscribe('/bad', 'share', callback=subscribed)
        client = mqlight.Client('amqp://host',
                                'test_subscribe_fail_callback',
                                callback=started)
        done = test_is_done.wait(self.TEST_TIMEOUT)
        assert done

    def test_subscribe_when_stopped(self):
        """
        Test that trying to establish a subscription, while the client is in
        stopped state, throws an Error
        """
        client = mqlight.Client('amqp://host',
                                'test_subscribe_when_stopped')

        def stopped(err):
            """stopped listener"""
            with pytest.raises(mqlexc.MQLightError):
                client.subscribe('/foo')
        client.stop(stopped)

    def test_subscribe_returns_client(self):
        """
        Test that calling the subscribe(...) method returns, as a value, the
        client object that the method was invoked on
        """
        test_is_done = threading.Event()

        def started(err, service):
            """started listener"""
            assert client.subscribe('/foo') == client
            client.stop()
            test_is_done.set()
        client = mqlight.Client('amqp://host',
                                'test_subscribe_returns_client',
                                callback=started)
        done = test_is_done.wait(self.TEST_TIMEOUT)
        assert done

    def test_subscribe_topics(self):
        """
        Test a variety of valid and invalid patterns.  Invalid patterns
        should result in the client.subscribe(...) method throwing a
        ValueError.
        """
        test_is_done = threading.Event()
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

        def started(err, service):
            """started listener"""
            try:
                for test in data:
                    if test['valid']:
                        client.subscribe(test['pattern'])
                    else:
                        with pytest.raises(mqlexc.InvalidArgumentError):
                            client.subscribe(test['pattern'])
            except Exception as exc:
                pytest.fail('Unexpected Exception ' + str(exc))
            finally:
                client.stop()
                test_is_done.set()
        client = mqlight.Client('amqp://host',
                                'test_subscribe_topics',
                                callback=started)
        done = test_is_done.wait(self.TEST_TIMEOUT)
        assert done

    def test_subscribe_share_names(self):
        """
        Tests a variety of valid and invalid share names to check that they are
        accepted or rejected (by throwing an Error) as appropriate.
        """
        test_is_done = threading.Event()
        data = [
            {'valid': True, 'share': 'abc'},
            {'valid': True, 'share': 7},
            {'valid': False, 'share': ':'},
            {'valid': False, 'share': 'a:'},
            {'valid': False, 'share': ':a'}
        ]

        def started(err, service):
            """started listener"""
            try:
                for test in data:
                    if test['valid']:
                        client.subscribe('/foo', test['share'])
                    else:
                        with pytest.raises(mqlexc.InvalidArgumentError):
                            client.subscribe('/foo', test['share'])
            except Exception as exc:
                pytest.fail('Unexpected Exception ' + str(exc))
            finally:
                client.stop()
                test_is_done.set()
        client = mqlight.Client('amqp://host',
                                'test_subscribe_share_names',
                                callback=started)
        done = test_is_done.wait(self.TEST_TIMEOUT)
        assert done

    def test_subscribe_options(self):
        """
        Test a variety of valid and invalid options values. Invalid options
        should result in the client.subscribe(...) method throwing a
        ValueError.
        """
        test_is_done = threading.Event()
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

        def started(err, service):
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
                                callback=started)
        done = test_is_done.wait(self.TEST_TIMEOUT)
        assert done

    def test_subscribe_qos(self):
        """
        Test a variety of valid and invalid QoS options.  Invalid QoS values
        should result in the client.subscribe(...) method throwing a ValueError
        """
        test_is_done = threading.Event()
        func = Mock()
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

        def started(err, service):
            """started listener"""
            try:
                for i in range(len(data)):
                    test = data[i]
                    opts = {'qos': test['qos']}
                    if test['valid']:
                        client.subscribe('/foo' + str(i), opts)
                    else:
                        with pytest.raises(mqlexc.InvalidArgumentError):
                            client.subscribe('/foo' + str(i), opts)
            except Exception as exc:
                pytest.fail('Unexpected Exception ' + str(exc))
            finally:
                client.stop()
                test_is_done.set()
        client = mqlight.Client('amqp://host',
                                'test_subscribe_qos',
                                callback=started)
        done = test_is_done.wait(self.TEST_TIMEOUT)
        assert done

    def test_subscribe_auto_confirm(self):
        """
        Test a variety of valid and invalid auto_confirm options. Invalid
        auto_confirm values should result in the client.subscribe(...) method
        throwing a ValueError
        """
        test_is_done = threading.Event()
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

        def started(err, service):
            """started listener"""
            try:
                for i in range(len(data)):
                    test = data[i]
                    if test['valid']:
                        client.subscribe('/foo' + str(i), test['opts'])
                    else:
                        with pytest.raises(mqlexc.InvalidArgumentError):
                            client.subscribe('/foo' + str(i), test['opts'])
            except Exception as exc:
                pytest.fail('Unexpected Exception ' + str(exc))
            finally:
                client.stop()
                test_is_done.set()
        client = mqlight.Client('amqp://host',
                                'test_subscribe_auto_confirm',
                                callback=started)
        done = test_is_done.wait(self.TEST_TIMEOUT)
        assert done

    def test_subscribe_ttl_validity(self):
        """
        Test a variety of valid and invalid ttl options.  Invalid ttl values
        should result in the client.subscribe(...) method throwing a ValueError
        """
        test_is_done = threading.Event()
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
            {'valid': True, 'ttl': 9 - 8},
            {'valid': True, 'ttl': 9007199254740992}
        ]

        def started(err, service):
            """started listener"""
            try:
                for i in range(len(data)):
                    test = data[i]
                    opts = {'ttl': test['ttl']}
                    if test['valid']:
                        client.subscribe('/foo' + str(i), opts)
                    else:
                        with pytest.raises(TypeError):
                            client.subscribe('/foo' + str(i), opts)
            except Exception as exc:
                pytest.fail('Unexpected Exception ' + str(exc))
            finally:
                client.stop()
                test_is_done.set()
        client = mqlight.Client('amqp://host',
                                'test_subscribe_ttl_validity',
                                callback=started)
        done = test_is_done.wait(self.TEST_TIMEOUT)
        assert done
