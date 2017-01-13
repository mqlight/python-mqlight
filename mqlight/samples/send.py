"""
<copyright
notice="lm-source-program"
pids="5725-P60"
years="2013,2016"
crc="3568777996" >
Licensed Materials - Property of IBM

5725-P60

(C) Copyright IBM Corp. 2013, 2016

US Government Users Restricted Rights - Use, duplication or
disclosure restricted by GSA ADP Schedule Contract with
IBM Corp.
</copyright>
"""
from __future__ import print_function
import sys
import argparse
import mqlight
from time import sleep
from uuid import uuid4
from threading import Event, RLock

AMQP_SERVICE = 'amqp://localhost'
AMQPS_SERVICE = 'amqps://localhost'


def arguments():
    """
    The collection of valid argument for this sample.
    """
    parser = argparse.ArgumentParser(
        description='Send a message to a MQ Light server.')
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
        '--topic',
        dest='topic',
        type=str,
        default='public',
        help='send messages to topic TOPIC (default: %(default)s)')
    parser.add_argument(
        '-i',
        '--id',
        dest='client_id',
        type=str,
        default=None,
        help='the ID to use when connecting to MQ Light '
             '(default: send_[0-9a-f]{7})')
    parser.add_argument(
        '--message-ttl',
        dest='message_ttl',
        type=int,
        default=None,
        help='set message time-to-live to MESSAGE_TTL seconds '
             '(default: %(default)s)')
    parser.add_argument(
        '-d',
        '--delay',
        dest='delay',
        type=int,
        default=0,
        help='add NUM seconds delay between each request '
             '(default: %(default)s)')
    parser.add_argument(
        '-r',
        '--repeat',
        dest='repeat',
        type=int,
        default=1,
        help='send messages REPEAT times, if REPEAT <= 0 then repeat forever '
             '(default: %(default)s)')
    parser.add_argument(
        '--sequence',
        dest='sequence',
        action='store_true',
        help='prefix a sequence number to the message payload, ignored for '
             'binary messages')
    parser.add_argument(
        '-f',
        '--file',
        dest='file',
        type=str,
        help='send FILE as binary data. Cannot be specified at the same time '
             'as MESSAGE')
    parser.add_argument(
        '--verbose',
        dest='verbose',
        action='store_true',
        help='print additional information about each message.')
    parser.add_argument(
        'messages',
        metavar='MESSAGE',
        type=str,
        nargs='*',
        default=None,
        help='message to be sent (default: Hello world!)')
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


class Send(object):
    """
    A class to perform a Mqlight client creation and then send one or
    more messages to the server.
    """
    def __init__(self, args):
        # Process SSL arguments
        self._using_ssl = False
        self._security_options = {}
        ssl_arg_names = [
            'ssl_trust_certificate',
            'ssl_client_certificate',
            'ssl_client_key',
            'ssl_client_key_passphrase',
            'ssl_verify_name']
        for ssl_arg_name in ssl_arg_names:
            if ssl_arg_name in args.__dict__ and \
                    args.__dict__[ssl_arg_name] is not None:
                self._security_options[ssl_arg_name] = \
                        args.__dict__[ssl_arg_name]
                self._using_ssl = True
        # Add default values
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

        self._topic = args.topic
        if args.client_id is not None:
            self._client_id = args.client_id
        else:
            self._client_id = 'send_' + str(uuid4()).replace('-', '_')[0:7]
        self._repeat = args.repeat
        self._delay = args.delay
        self._message_ttl = args.message_ttl
        self._verbose = args.verbose
        self._send_complete = Event()

        def _read_message_from_file(file):
            """
            Loads the given file name as binary data into a message arrays
            :param file: the file path of the filke to be loaded.
            :return: tuple (an array of one item of the loaded file, a reported
            error condition)
            """
            messages = []
            message = bytearray()
            try:
                with open(file, 'rb') as f:
                    byte = f.read(1)
                    while byte:
                        message.append(ord(byte))
                        byte = f.read(1)
            except IOError as e:
                return None, e
            if len(message) > 0:
                messages.append(message)
            return messages, None
        # Message source
        if args.file is not None:
            if len(args.messages) > 0:
                self._close(None, 'Only one file or one message can '
                            'be specified')
            self._messages, error = _read_message_from_file(args.file)
            if error is not None:
                self._close(None, 'Failed to load file {0} because {1}'.
                            format(args.file, error))
            self._binary_message = True
        elif len(args.messages) > 0:
            self._messages = args.messages
            self._binary_message = False
        else:
            self._messages = ['Hello World']
            self._binary_message = False
        if self._repeat is not None and self._repeat > 1:
            self._messages = self._messages * self._repeat
        # Sequence
        if args.sequence:
            if self._binary_message:
                self._close(None, 'The \'--sequence option\' is not '
                            'supported with a binary message')
            self._sequence = 0
        else:
            self._sequence = None
        self._pending = AtomicVariable(len(self._messages))

    def output(self, msg):
        print(msg)
        sys.stdout.flush()

    def _close(self, client, err=None):
        """
        Sample program will now terminate either because it has completed
        or that an error occurred.
        :param client the client being closed.
        :param err optional error condition to report.
        """
        if err:
            print('*** error ***', file=sys.stderr)
            print('{0}'.format(err), file=sys.stderr)
        if client:
            client.stop()

    def _sent(self, client, err, topic, data, options):
        """
        Message sent callback. Indicates that a message has confirmed as being
        sent.
        :param client that sent the message
        :param err any error detect during sending the message. None means
        successfully sent
        :param topic the topic the message was sent to.
        :param data the message that was sent
        :param options associated options given at send request time.
        """
        if err:
            self._close(client, 'Problem with send request: {0}'.
                        format(err))
        else:
            if data:
                if self._binary_message:
                    print('Binary message sent')
                else:
                    print('{0}{1}\n'.format(
                        data[:50],
                        (' ...' if len(data) > 50 else '')), end="")
                sys.stdout.flush()
        # if no more pending messages then stop the client
        if self._pending.remove() == 0:
            client.stop()

    def _drained(self, client):
        """
        Drain callback. Indicates the backlog of message have been drained
        :param client the client that has been drained
        """
        self._send_complete.set()

    def _started(self, client):
        """
        Started callback - client has been started and ready to send
        message to the server
        :param client the client that has successfully started
        """
        self.output('Connected to {0} using client-id {1}'.format(
            client.get_service(), client.get_id()))
        self.output('Sending to: {0}'.format(self._topic))
        wait = False
        while self._messages:
            if wait:
                if self._delay > 0:
                    sleep(self._delay)
            else:
                wait = True

            self._send_complete.clear()
            body = self._messages.pop(0)
            options = {'qos': mqlight.QOS_AT_LEAST_ONCE}
            if self._message_ttl is not None:
                options['ttl'] = message_ttl * 1000
            if self._sequence is not None:
                self._sequence += 1
                body = '{0}: {1}'.format(self._sequence, body)
            if not client.send(
                    topic=self._topic,
                    data=body,
                    options=options,
                    on_sent=self._sent):
                # There's a backlog of messages to send, so wait until the
                # backlog is cleared before sending any more
                if self._verbose:
                    self.output('Waiting for backlog to clear')
                self._send_complete.wait()
            if self._verbose:
                self.output('Request message to be sent')

    def _state_changed(self, client, new_state, error):
        """
        state_change callback. Indicates that there has been a change of
        connection state to the server.
        :param client the client that has changed state
        :param new_state, the new state for the client
        :param error an associated error for the state change. None indicate
        no error to report.
        """
        if error and new_state in ('stopping', 'stopped', 'retrying'):
            self._close(client, 'Connection to server failed because '
                        '{0}'.format(error))

    def run(self):
        """
        Start the sample by performing a client connection. The associated
        callback will handle the subsequent stages of sending a message
        """
        self._client = mqlight.Client(
            service=self._service,
            client_id=self._client_id,
            security_options=self._security_options,
            on_started=self._started,
            on_state_changed=self._state_changed,
            on_drain=self._drained)

if __name__ == "__main__":
    """
    Start of the sample
    """
    send = Send(arguments())
    try:
        send.run()
    except Exception as err:
        print('*** error ***', file=sys.stderr)
        print('{0}'.format(err), file=sys.stderr)
