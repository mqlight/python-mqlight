"""
<copyright
notice="lm-source-program"
pids="5724-H72"
years="2013,2016"
crc="725605481" >
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
from mqlight.exceptions import StoppedError, InvalidArgumentError


class TestSend(unittest.TestCase):

    """
    Unit tests for client.send()
    """
    TEST_TIMEOUT = 10.0

    def func001(self, a):
        pass

    def func005(self, a, b, c, d, e):
        pass

    def test_send_too_few_arguments(self):
        """
        Test a calling client.send(...) with too few arguments
        (no arguments) causes an Error to be thrown.
        """
        # pylint: disable=no-value-for-parameter
        test_is_done = threading.Event()

        def started(client):
            """started listener"""
            with pytest.raises(TypeError):
                client.send()
            with pytest.raises(TypeError):
                client.send('topic')
            client.stop()
            test_is_done.set()
        client = mqlight.Client('amqp://host', on_started=started)
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()

    def test_send_too_many_arguments(self):
        """
        Test that calling client.send(...) with too many arguments raises
        an Error
        """
        # pylint: disable=too-many-function-args
        test_is_done = threading.Event()

        def started(client):
            """started listener"""
            callback = self.func005
            with pytest.raises(TypeError):
                client.send('topic', 'message', {}, callback, 'extra')
            client.stop()
            test_is_done.set()
        client = mqlight.Client('amqp://host', on_started=started)
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()

    def test_send_topics(self):
        """
        Test a variety of valid and invalid topic names. Invalid topic names
        should result in the client.send(...) method throwing a TypeError.
        """
        test_is_done = threading.Event()
        func = self.func001
        data = [
            {'name': 'TopicEmptyStr','error': InvalidArgumentError, 'topic': ''},
            {'name': 'TopicNone',    'error': InvalidArgumentError, 'topic': None},
            {'name': 'Topic1234',    'error': InvalidArgumentError, 'topic': 1234},
            {'name': 'TopicFunc',    'error': InvalidArgumentError, 'topic': func},
            {'name': 'TopicKittens', 'error': None,  'topic': 'kittens'},
            {'name': 'Topic/Kittens','error': None,  'topic': '/kittens'}
        ]

        def started(client):
            """started listener"""
            try:
                for test in data:
                    if test['error'] is None:
                        client.send(test['topic'], 'message')
                    else:
                        with pytest.raises(test['error']):
                            client.send(test['topic'], 'message')
            except Exception as exc:
                pytest.fail('Test {0} : Unexpected Exception {1}{2}'.format(test['name'], str(type(exc)),str(exc)))
            finally:
                client.stop()
                test_is_done.set()
        client = mqlight.Client('amqp://host', on_started=started)
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()

    def test_send_callback(self):
        """
        Tests that, if a callback function is supplied to client.test(...) then
        the function is invoked when the send operation completes, and this
        references the client.
        """
        test_is_done = threading.Event()
        data = [
            {'topic': 'topic1', 'data': 'last', 'options': {'qos':0, 'ttl': 1}},
            {'topic': 'topic2', 'data': 'data2', 'options': {'qos':1, 'ttl': 10000}}
        ]

        def started(client):
            """started listener"""
            def send_callback(client, err, topic, d, options):
                """send callback"""
                opts = data.pop()
                assert err is None
                assert topic == opts['topic']
                assert d == opts['data']
                assert options == opts['options']
                if d == 'last':
                    client.stop()
                    test_is_done.set()

            try:
                for test in reversed(data):
                    client.send(test['topic'],
                                test['data'],
                                test['options'],
                                send_callback)
            except Exception as exc:
                traceback.print_exc()
                pytest.fail('Unexpected Exception ' + str(exc))
        client = mqlight.Client('amqp://host',
                                client_id='test_send_callback',
                                on_started=started)
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()

    def test_send_fail_if_stopped(self):
        """
        Tests that client.send(...) throws and error if it is called while the
        client is in stopped state.
        """
        test_is_done = threading.Event()

        def started(client):
            """started listener"""
            def stopped(client, err):
                """stopped listener"""
                with pytest.raises(StoppedError):
                    client.send('topic', 'message')
                test_is_done.set()
            client.stop(stopped)
        client = mqlight.Client('amqp://host', on_started=started)
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()

    def test_send_options(self):
        """
        Test a variety of valid and invalid options values. Invalid options
        should result in the client.send(...) method throwing a TypeError.
        Note that this test just checks that the options parameter is only
        accepted when it is of the correct type. The actual validation of
        individual options will be in separate tests
        """
        test_is_done = threading.Event()
        func = self.func005
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
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()

    def test_send_qos(self):
        """
        Test a variety of valid and invalid QoS values.  Invalid QoS values
        should result in the client.send(...) method throwing a ValueError.
        """
        test_is_done = threading.Event()
        func = self.func005
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

        def started(client):
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
                pytest.fail('Unexpected Exception \'' + str(exc) + '\' for qos ' +
                            str(test['qos']))
            client.stop()
            test_is_done.set()
        client = mqlight.Client('amqp://host', on_started=started)
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()

    def test_send_qos_function(self):
        """
        Test that a function is required when QoS is 1.
        """
        test_is_done = threading.Event()
        func = self.func005
        data = [
            {'name': 'Qos1CallbackNone', 'valid': False, 'qos': 1, 'callback': None},
            {'name': 'Qos1CallbackFunc', 'valid': True,  'qos': 1, 'callback': func},
            {'name': 'Qos0CallbackNone', 'valid': True,  'qos': 0, 'callback': None},
            {'name': 'Qos0CallbackFunc', 'valid': True,  'qos': 0, 'callback': func}
        ]

        def started(client):
            """started listener"""
            try:
                for test in data:
                    opts = {'qos': test['qos']}
                    if test['valid']:
                        client.send('test', 'message', opts, test['callback'])
                    else:
                        with pytest.raises(InvalidArgumentError):
                            client.send('test', 'message', opts,
                                        test['callback'])
            except Exception as exc:
                pytest.fail('Test {0} : Unexpected Exception {1}'.format(test['name'], str(exc)))
            client.stop()
            test_is_done.set()
        client = mqlight.Client('amqp://host', on_started=started)
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()
