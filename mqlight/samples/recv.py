
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

from __future__ import print_function
import sys
import argparse
import mqlight
from time import sleep
from uuid import uuid4
from threading import RLock, Event

AMQP_SERVICE = 'amqp://localhost'
AMQPS_SERVICE = 'amqps://localhost'

PYTHON2 = sys.version_info == (2)
PYTHON3 = sys.version_info == (3)


def arguments():
    """
    The collection of valid arguments for this sample.
    """
    parser = argparse.ArgumentParser(
        description='Connect to an MQ Light server and subscribe to the '
                    'specified topic.')
    parser.add_argument(
        '-s',
        '--service',
        dest='service',
        type=str,
        default=None,
        help='service to connect to, for example: amqp://user:password@host:'
             '5672 or amqps://host:5671 to use SSL/TLS (default: %(default)s)')
    parser.add_argument(
        '-t',
        '--topic-pattern',
        dest='topic_pattern',
        type=str,
        default='public',
        help='subscribe to receive messages matching TOPIC_PATTERN '
             '(default: %(default)s)')
    parser.add_argument(
        '-i',
        '--id',
        dest='client_id',
        type=str,
        default=None,
        help='the ID to use when connecting to MQ Light '
             '(default: send_[0-9a-f]{7})')
    parser.add_argument(
        '--destination-ttl',
        dest='destination_ttl',
        type=int,
        default=None,
        help='set destination time-to-live to DESTINATION_TTL seconds '
             '(default: %(default)s)')
    parser.add_argument(
        '-n',
        '--share-name',
        dest='share_name',
        type=str,
        default=None,
        help='optionally, subscribe to a shared destination using SHARE_NAME'
             'as the share name.')
    parser.add_argument(
        '-f',
        '--file',
        dest='file',
        type=str,
        default=None,
        help='write the payload of the next message received to FILE '
             '(overwriting previous file contents then end. (default is to '
             'print messages to stdout)')
    parser.add_argument(
        '-d',
        '--delay',
        dest='delay',
        type=int,
        default=0,
        help='delays the confirmation for DELAY seconds each time a message '
             'is received. (default: %(default)s)')
    parser.add_argument(
        '--verbose',
        dest='verbose',
        action='store_true',
        help='print additional information about each message.')

    ssl_group = parser.add_argument_group('ssl arguments')
    ssl_group.add_argument(
        '-c',
        '--trust-certificate',
        metavar='FILE',
        dest='ssl_trust_certificate',
        type=str,
        default=None,
        help='use the certificate contained in FILE (in PEM or DER format) to '
             'validate the identify of the server. The connection must be '
             'secured with SSL/TLS (e.g. the service URL must start with '
             '"amqps://")')
    ssl_group.add_argument(
        '--client-certificate ',
        metavar='FILE',
        dest='ssl_client_certificate',
        type=str,
        help='use the certificate contained in FILE (in PEM format) to '
             'supply the identity of the client. The connection must'
             'be secured with SSL/TLS')
    ssl_group.add_argument(
        '--client-key',
        metavar='FILE',
        dest='ssl_client_key',
        type=str,
        help='use the private key contained in FILE (in PEM format) '
             'for encrypting the specified client certificate')
    ssl_group.add_argument(
        '--client-key-passphrase',
        metavar='PASSPHRASE',
        dest='ssl_client_key_passphrase',
        type=str,
        help='use PASSPHRASE to access the client private key')
    ssl_group.add_argument(
        '--no-verify-name',
        dest='ssl_verify_name',
        default=None,
        help='specify to not additionally check the server\'s common name in '
             'the specified trust certificate matches the actual server\'s '
             'DNS name')
    return parser.parse_args()


class AtomicVariable(object):
    """
    Helper class to ensure increment/decrement functions correctly when
    called by different threads.
    """
    def __init__(self, initial_value=0):
        self.lock = RLock()
        self.counter = initial_value

    def add(self):
        """
        Increases the counter by one
        :return: the resultant value
        """
        with self.lock:
            self.counter += 1
        return self.counter

    def remove(self):
        """
        Reduces the counter by one
        :return: the resultant value
        """
        with self.lock:
            self.counter -= 1
        return self.counter


class Receive(object):
    """
    A class to perform a Mqlight client creation and then wait to receive
    messages from the server.
    """
    def __init__(self, args):
        self._using_ssl = False
        self._security_options = {}
        ssl_arg_names = [
            'ssl_trust_certificate',
            'ssl_client_certificate',
            'ssl_client_key',
            'ssl_client_key_passphrase',
            'ssl_verify_name']
        for ssl_arg_name in ssl_arg_names:
            value = args.__dict__[ssl_arg_name]
            if value is not None:
                self._security_options[ssl_arg_name] = value
                self._using_ssl = True
        if 'ssl_verify_name' not in self._security_options:
            self._security_options[ssl_arg_name] = True
        # Select the relevant service
        if args.service is None:
            self._service = AMQPS_SERVICE if self._using_ssl else AMQP_SERVICE
        else:
            if self._using_ssl:
                if not args.service.startswith('amqps'):
                    self._close(None, 'The service URL must start with '
                                '"amqps://" when using any of the ssl '
                                'options.')
            self._service = args.service

        self._topic_pattern = args.topic_pattern
        if args.client_id is not None:
            self._client_id = args.client_id
        else:
            self._client_id = 'recv_' + str(uuid4()).replace('-', '_')[0:7]
        self._delay = args.delay
        self._share = args.share_name
        self._verbose = args.verbose
        self._destination_ttl = args.destination_ttl
        self._file = args.file
        self._client = None
        self._exit = Event()
        self._num_of_messages = AtomicVariable(0)

    def _close(self, client, err=None):
        """
        Handles closing this sample
        :param client the associate client to close
        :param err an error condtion to report.
        """
        if err:
            error = err[1] if isinstance(err, tuple) else err
            print('*** error ***\n{0}'.format(error), file=sys.stderr)
        if client:
            client.stop()
        self._exit.set()

    def _output(self, msg):
        print(msg)
        sys.stdout.flush()

    def _started(self, client):
        """
        Started callback. Indicate a successful connection to client and
        open for business.
        :param client the associate client that has been successfully connected
        """
        print('Connected to {0} using client-id {1}'.format(
            client.get_service(), client.get_id()))
        options = {
            'qos': mqlight.QOS_AT_LEAST_ONCE,
            'auto_confirm': False
        }
        if self._destination_ttl is not None:
            options['ttl'] = self._destination_ttl*1000
        if self._delay is not None and self._delay > 0:
            options['credit'] = 1
        client.subscribe(
            topic_pattern=self._topic_pattern,
            share=self._share,
            options=options,
            on_subscribed=self._subscribed,
            on_message=self._message)

    def _subscribed(self, client, err, pattern, share):
        """
        Subscribe callback. Indicates the subscription has been completed. If
        not error is report then it is open to receive messages.
        :param client the associate client for this subscription
        :param err issues detected while making the subscription, None means
        successful
        :param pattern the topic or pattern for the subscription
        :param share is the subscription is shared.
        """
        if err is not None:
            self._close(client, 'problem with subscribe request {0}'.
                        format(err))
        if pattern:
            if share:
                self._output('Subscribed to share: {0}, pattern: {1}'.
                             format(share, pattern))
            else:
                self._output('Subscribed to pattern: {0}'.format(pattern))

    def _write_to_file(self, filename, data):
        """
        Write the contains of the data to the named file
        :param filename the filepath to write the data to
        :data data the data to be written
        """
        self._output('Writing message data to {0}'.format(self._file))
        try:
            with open(self._file, 'wb') as f:
                if PYTHON2:
                    if isinstance(data, str) or isinstance(data, unicode):
                        f.write(data)
                    else:
                        f.write(''.join(chr(byte) for byte in data))
                else:
                    if isinstance(data, str):
                        f.write(data)
                    else:
                        f.write(bytes(data))
        except IOError as e:
            self._close(None, 'Failed to write message to {0} '
                        'because {1}'.format(self._file, e))

    def _message(self, message_type, data, delivery):
        """
        Message callback. This indicate there is a message has been
        receive and ready for processing.
        :param client the reference to the connected client
        :param message_type, the type of message received
        (message or malformed)
        :param data, the actual message recevied
        :param delivery, the associated delivery information for the message
        """
        if message_type == mqlight.MALFORMED:
            malformed_message = ('*** received malformed message ***\n'
                                 'Data: {0}\nDelivery: {1}'.
                                 format(data, delivery))
            print(malformed_message, file=sys.stderr)
        else:
            if self._verbose:
                self._output('# received message {0}'.
                             format(self._num_of_messages.add()))
            if self._file:
                self._write_to_file(self._file, data)
                delivery['message']['confirm_delivery']()
            else:
                print('{0}{1}\n'.format(
                    data[:50],
                    (' ...' if len(data) > 50 else '')), end="")
                sys.stdout.flush()
            if self._verbose:
                self._output(delivery)
            if self._delay > 0:
                sleep(self._delay)
            if self._client.get_state() == 'started':
                delivery['message']['confirm_delivery']()

    def _state_changed(self, client, new_state, error):
        """
        state_change callback. Indicates that there has been a change of
        connection state to the server.
        :param client the client reference whoses state has changed
        :param new_state the new state for the client
        :param any assoicated error with the state change. None means no error
        """
        if error:
            if new_state in ('stopping', 'stopped', 'retrying'):
                self._close(client, error)

    def run(self):
        """
        Start the sample by performing a client connection. The associated
        callback will handle the subsequent stages of receiving messages
        """
        if not self._exit.isSet():
            try:
                self._client = mqlight.Client(
                    service=self._service,
                    client_id=self._client_id,
                    security_options=self._security_options,
                    on_started=self._started,
                    on_state_changed=self._state_changed)
            except Exception as exc:
                self._close(self._client, exc)

            # waiting for finish
            try:
                while not self._exit.wait(1.0):
                    pass
            except KeyboardInterrupt:
                self._output('Closing ...')
                self._close(self._client)


if __name__ == "__main__":
    """
    Start of the sample
    """
    receive = Receive(arguments())
    try:
        receive.run()
    except Exception as e:
        print('*** ERROR: Failed to start a client because {0}'.format(e))
