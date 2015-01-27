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
import pytest
import threading
from mock import Mock, patch
import mqlight
import mqlight.mqlightexceptions as mqlexc


@patch('mqlight.mqlightproton._MQLightMessenger.connect',
       Mock())
@patch('mqlight.mqlightproton._MQLightMessenger.get_remote_idle_timeout',
       Mock(return_value=0))
class TestUnsubscribe(object):

    """
    Unit tests for client.unsubscribe()
    """
    TEST_TIMEOUT = 10.0

    def test_unsubscribe_too_few_arguments(self):
        """
        Test a calling client.unsubscribe(...) with too few arguments
        (no arguments) causes an Error to be thrown.
        """
        # pylint: disable=no-value-for-parameter
        client = mqlight.Client(
            'amqp://host',
            'test_unsubscribe_too_few_arguments')
        with pytest.raises(TypeError):
            client.unsubscribe()
        client.stop()

    def test_unsubscribe_too_many_arguments(self):
        """
        Test that calling client.unsubscribe(...) with too many arguments
        causes an Error to be thrown.
        """
        # pylint: disable=too-many-function-args
        test_is_done = threading.Event()

        def started(err):
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
        func = Mock()
        done = test_is_done.wait(self.TEST_TIMEOUT)
        assert done

    def test_unsubscribe_callback_must_be_function(self):
        """
        Test that the callback argument to client.unsubscribe(...) must be a
        function
        """
        test_is_done = threading.Event()

        def started(err):
            """started listener"""
            func = Mock()
            with pytest.raises(TypeError):
                client.subscribe('/foo1', 'share')
                client.unsubscribe('/foo1', 'share', {}, 7)

            client.subscribe('/foo2')
            client.unsubscribe('/foo2', on_unsubscribed=func)

            client.subscribe('/foo3', 'share')
            client.unsubscribe('/foo3', 'share', on_unsubscribed=func)

            client.subscribe('/foo4', 'share')
            client.unsubscribe('/foo4', 'share', {}, on_unsubscribed=func)

            client.stop()
            test_is_done.set()

        client = mqlight.Client(
            'amqp://host',
            'test_unsubscribe_callback_must_be_function',
            on_started=started
        )

        done = test_is_done.wait(self.TEST_TIMEOUT)
        assert done

    def test_unsubscribe_ok_callback(self):
        """
        Test that the callback (invoked when the unsubscribe operation
        completes successfully) specifies the right number of arguments.
        """
        test_is_done = threading.Event()

        def started(err):
            """started listener"""
            def unsub(err, topic, share):
                """unsubscribe callback"""
                assert err is None
                assert topic == '/foo'
                assert share is None

            def sub(err, topic, share):
                """subscribe callback"""
                client.unsubscribe('/foo', on_unsubscribed=unsub)
            client.subscribe('/foo', on_subscribed=sub)

            def unsub2(err, topic, share):
                """unsubscribe callback"""
                assert err is None
                assert topic == '/foo2'
                assert share == 'share'
                client.stop()
                test_is_done.set()

            def sub2(err, topic, share):
                """subscribe callback"""
                client.unsubscribe('/foo2', 'share', on_unsubscribed=unsub2)
            client.subscribe('/foo2', 'share', on_subscribed=sub2)

        client = mqlight.Client(
            'amqp://host',
            'test_unsubscribe_ok_callback',
            on_started=started)
        done = test_is_done.wait(self.TEST_TIMEOUT)
        assert done

    def test_unsubscribe_when_stopped(self):
        """
        Test that trying to remove a subscription, while the client is in
        stopped state, throws an Error.
        """
        test_is_done = threading.Event()
        client = mqlight.Client('amqp://host', 'test_unsubscribe_when_stopped')

        def stopped(err):
            """stopped listener"""
            with pytest.raises(mqlexc.StoppedError):
                client.unsubscribe('/foo')
            test_is_done.set()
        client.stop(stopped)
        done = test_is_done.wait(self.TEST_TIMEOUT)
        assert done

    def test_unsubscribe_when_not_subscribed(self):
        """
        Test that trying to remove a subscription that does not exist throws an
        Error.
        """
        test_is_done = threading.Event()

        def started(err):
            """started listener"""
            subscribe_event = threading.Event()
            client.subscribe(
                '/bar',
                on_subscribed=lambda _x, _y, _z: subscribe_event.set())
            assert subscribe_event.wait(2.0)
            with pytest.raises(mqlexc.UnsubscribedError):
                client.unsubscribe('/foo')
            client.stop()
            test_is_done.set()
        client = mqlight.Client(
            'amqp://host',
            'test_unsubscribe_when_not_subscribed',
            on_started=started)
        done = test_is_done.wait(self.TEST_TIMEOUT)
        assert done

    def test_unsubscribe_returns_client(self):
        """
        Test that calling the unsubscribe(...) method returns, as a value, the
        client object that the method was invoked on (for method chaining
        purposes).
        """
        test_is_done = threading.Event()

        def started(err):
            """started listener"""
            client.subscribe('/foo')
            assert client.unsubscribe('/foo') == client
            client.stop()
            test_is_done.set()
        client = mqlight.Client(
            'amqp://host',
            'test_unsubscribe_returns_client',
            on_started=started)
        done = test_is_done.wait(self.TEST_TIMEOUT)
        assert done

    def test_unsubscribe_topics(self):
        """
        Test a variety of valid and invalid patterns.  Invalid patterns
        should result in the client.unsubscribe(...) method throwing a
        TypeError.
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

        def started(err):
            """started listener"""
            try:
                for test in data:
                    if test['valid']:
                        client.subscribe(test['pattern'])
                        client.unsubscribe(test['pattern'])
                    else:
                        with pytest.raises(mqlexc.MQLightError):
                            client.unsubscribe(test['pattern'])
            except:
                pytest.fail('Unexpected Exception')
            finally:
                client.stop()
                test_is_done.set()
        client = mqlight.Client(
            'amqp://host',
            'test_unsubscribe_topics',
            on_started=started)

        done = test_is_done.wait(self.TEST_TIMEOUT)
        assert done

    def test_unsubscribe_share_names(self):
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

        def started(err):
            """started listener"""
            try:
                for test in data:
                    if test['valid']:
                        client.subscribe('/foo', test['share'])
                        client.unsubscribe('/foo', test['share'])
                    else:
                        with pytest.raises(mqlexc.InvalidArgumentError):
                            client.unsubscribe('/foo', test['share'])
            except:
                pytest.fail('Unexpected Exception')
            finally:
                client.stop()
                test_is_done.set()
        client = mqlight.Client(
            'amqp://host',
            'test_unsubscribe_share_names',
            on_started=started)

        done = test_is_done.wait(self.TEST_TIMEOUT)
        assert done

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

        def started(err):
            """started listener"""
            try:
                for test in data:
                    if test['valid']:
                        client.subscribe('testpattern', 'share')
                        client.unsubscribe('testpattern',
                                           'share',
                                           test['options'],
                                           func)
                    else:
                        with pytest.raises(TypeError):
                            client.unsubscribe('testpattern',
                                               'share',
                                               test['options'],
                                               func)
            except:
                pytest.fail('Unexpected Exception')
            finally:
                client.stop()
                test_is_done.set()
        client = mqlight.Client(
            'amqp://host',
            'test_unsubscribe_options',
            on_started=started)

        done = test_is_done.wait(self.TEST_TIMEOUT)
        assert done

    def test_unsubscribe_ttl_validity(self):
        """
        Test a variety of valid and invalid ttl options.  Invalid ttl values
        should result in the client.unsubscribe(...) method throwing a
        TypeError.
        """
        test_is_done = threading.Event()
        func = Mock()
        data = [
            {'valid': False, 'ttl': None},
            {'valid': False, 'ttl': func},
            {'valid': False, 'ttl': -9007199254740992},
            {'valid': False, 'ttl': float('-nan')},
            {'valid': False, 'ttl': float('nan')},
            {'valid': False, 'ttl': float('-inf')},
            {'valid': False, 'ttl': float('inf')},
            {'valid': False, 'ttl': -1},
            {'valid': False, 'ttl': 1},
            {'valid': False, 'ttl': 9007199254740992},
            {'valid': True, 'ttl': 0},
            {'valid': False, 'ttl': ''}
        ]

        def started(err):
            """started listener"""
            try:
                for test in data:
                    opts = {'ttl': test['ttl']}
                    if test['valid']:
                        client.subscribe('testpattern')
                        client.unsubscribe('testpattern', opts)
                    else:
                        with pytest.raises(mqlexc.RangeError):
                            client.unsubscribe('testpattern', opts)
            except:
                pytest.fail('Unexpected Exception')
            finally:
                client.stop()
                test_is_done.set()
        client = mqlight.Client(
            'amqp://host',
            'test_unsubscribe_ttl_validity',
            on_started=started)

        done = test_is_done.wait(self.TEST_TIMEOUT)
        assert done
