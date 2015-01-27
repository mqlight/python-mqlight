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


class TestSend(object):

    """
    Unit tests for client.send()
    """
    TEST_TIMEOUT = 10.0

    def test_send_too_few_arguments(self):
        """
        Test a calling client.send(...) with too few arguments
        (no arguments) causes an Error to be thrown.
        """
        # pylint: disable=no-value-for-parameter
        test_is_done = threading.Event()

        def started(err):
            """started listener"""
            with pytest.raises(TypeError):
                client.send()
            with pytest.raises(TypeError):
                client.send('topic')
            client.stop()
            test_is_done.set()
        client = mqlight.Client('amqp://host', on_started=started)
        done = test_is_done.wait(self.TEST_TIMEOUT)
        assert done

    def test_send_too_many_arguments(self):
        """
        Test that calling client.send(...) with too many arguments raises
        an Error
        """
        # pylint: disable=too-many-function-args
        test_is_done = threading.Event()

        def started(err):
            """started listener"""
            callback = Mock()
            with pytest.raises(TypeError):
                client.send('topic', 'message', {}, callback, 'extra')
            client.stop()
            test_is_done.set()
        client = mqlight.Client('amqp://host', on_started=started)
        done = test_is_done.wait(self.TEST_TIMEOUT)
        assert done

    def test_send_topics(self):
        """
        Test a variety of valid and invalid topic names. Invalid topic names
        should result in the client.send(...) method throwing a TypeError.
        """
        test_is_done = threading.Event()
        func = Mock()
        data = [
            {'valid': False, 'topic': ''},
            {'valid': False, 'topic': None},
            {'valid': True, 'topic': 1234},
            {'valid': True, 'topic': func},
            {'valid': True, 'topic': 'kittens'},
            {'valid': True, 'topic': '/kittens'}
        ]

        def started(err):
            """started listener"""
            try:
                for test in data:
                    if test['valid']:
                        client.send(test['topic'], 'message')
                    else:
                        with pytest.raises(mqlexc.InvalidArgumentError):
                            client.send(test['topic'], 'message')
            except Exception as exc:
                pytest.fail('Unexpected Exception ' + str(exc))
            finally:
                client.stop()
                test_is_done.set()
        client = mqlight.Client('amqp://host', on_started=started)
        done = test_is_done.wait(self.TEST_TIMEOUT)
        assert done

    def test_send_callback(self):
        """
        Tests that, if a callback function is supplied to client.test(...) then
        the function is invoked when the send operation completes, and this
        references the client.
        """
        test_is_done = threading.Event()
        data = [
            {'topic': 'topic1', 'data': 'data1', 'options': {}},
            {'topic': 'topic2', 'data': 'data2', 'options': None}
        ]

        def started(err):
            """started listener"""
            def send_callback(err, topic, d, options):
                """send callback"""
                opts = data.pop()
                assert err is None
                assert topic == opts['topic']
                assert d == opts['data']
                assert options == opts['options']
                if len(data) == 0:
                    client.stop()
                    test_is_done.set()

            try:
                for test in reversed(data):
                    client.send(test['topic'],
                                test['data'],
                                test['options'],
                                send_callback)
            except Exception as exc:
                pytest.fail('Unexpected Exception ' + str(exc))
        client = mqlight.Client('amqp://host',
                                client_id='test_send_callback',
                                on_started=started)
        done = test_is_done.wait(self.TEST_TIMEOUT)
        assert done

    def test_send_fail_if_stopped(self):
        """
        Tests that client.send(...) throws and error if it is called while the
        client is in stopped state.
        """
        test_is_done = threading.Event()

        def started(err):
            """started listener"""
            def stopped(err):
                """stopped listener"""
                with pytest.raises(mqlexc.StoppedError):
                    client.send('topic', 'message')
                test_is_done.set()
            client.stop(stopped)
        client = mqlight.Client('amqp://host', on_started=started)
        done = test_is_done.wait(self.TEST_TIMEOUT)
        assert done

    def test_send_options(self):
        """
        Test a variety of valid and invalid options values. Invalid options
        should result in the client.send(...) method throwing a TypeError.
        Note that this test just checks that the options parameter is only
        accepted when it is of the correct type. The actual validation of
        individual options will be in separate tests
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
                        client.send('test', 'message', test['options'], func)
                    else:
                        with pytest.raises(TypeError):
                            client.send('test', 'message', test['options'],
                                        func)
            except Exception as exc:
                pytest.fail('Unexpected Exception ' + str(exc))
            client.stop()
            test_is_done.set()
        client = mqlight.Client('amqp://host', on_started=started)
        done = test_is_done.wait(self.TEST_TIMEOUT)
        assert done

    def test_send_qos(self):
        """
        Test a variety of valid and invalid QoS values.  Invalid QoS values
        should result in the client.send(...) method throwing a ValueError.
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
            {'valid': True, 'qos': 1},
            {'valid': True, 'qos': 9 - 8},
            {'valid': True, 'qos': mqlight.QOS_AT_MOST_ONCE},
            {'valid': True, 'qos': mqlight.QOS_AT_LEAST_ONCE}
        ]

        def started(err):
            """started listener"""
            try:
                for test in data:
                    opts = {'qos': test['qos']}
                    if test['valid']:
                        client.send('test', 'message', opts, func)
                    else:
                        with pytest.raises(Exception):
                            client.send('test', 'message', opts, func)
            except Exception as exc:
                pytest.fail('Unexpected Exception ' + str(exc) + ' for qos ' +
                            str(test['qos']))
            client.stop()
            test_is_done.set()
        client = mqlight.Client('amqp://host', on_started=started)
        done = test_is_done.wait(self.TEST_TIMEOUT)
        assert done

    def test_send_qos_function(self):
        """
        Test that a function is required when QoS is 1.
        """
        test_is_done = threading.Event()
        func = Mock()
        data = [
            {'valid': False, 'qos': 1, 'callback': None},
            {'valid': True, 'qos': 1, 'callback': func},
            {'valid': True, 'qos': 0, 'callback': None},
            {'valid': True, 'qos': 0, 'callback': func}
        ]

        def started(err):
            """started listener"""
            try:
                for test in data:
                    opts = {'qos': test['qos']}
                    if test['valid']:
                        client.send('test', 'message', opts, test['callback'])
                    else:
                        with pytest.raises(mqlexc.InvalidArgumentError):
                            client.send('test', 'message', opts,
                                        test['callback'])
            except Exception as exc:
                pytest.fail('Unexpected Exception ' + str(exc))
            client.stop()
            test_is_done.set()
        client = mqlight.Client('amqp://host', on_started=started)
        done = test_is_done.wait(self.TEST_TIMEOUT)
        assert done
