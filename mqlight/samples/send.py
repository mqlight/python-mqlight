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
from __future__ import print_function
import sys
import argparse
import mqlight
import time
import uuid
import threading

SEQUENCE = 0
SERVICE = 'amqp://localhost'
TIMEOUT = 3.0
LOCK = threading.RLock()

parser = argparse.ArgumentParser(
    description='Send a message to an MQ Light server.')
parser.add_argument(
    '-s',
    '--service',
    dest='service',
    type=str,
    default=SERVICE,
    help='service to connect to, for example: amqp://user:password@host:5672 '
         'or amqps://host:5671 to use SSL/TLS (default: %(default)s)'
)
parser.add_argument(
    '-c',
    '--trust-certificate',
    dest='trust_certificate',
    type=str,
    default=None,
    help='use the certificate contained in FILE (in PEM or DER format) to '
         'validate the identify of the server. The connection must be secured '
         'with SSL/TLS (e.g. the service URL must start with "amqps://")')
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
    help='add NUM seconds delay between each request (default: %(default)s)')
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
    help='send FILE as binary data. Cannot be specified at the same time as '
         'MESSAGE')
parser.add_argument(
    'messages',
    metavar='MESSAGE',
    type=str,
    nargs='*',
    default=['Hello World!'],
    help='message to be sent (default: %(default)s)')
args = parser.parse_args()

service = args.service
topic = args.topic
if args.client_id is not None:
    client_id = args.client_id
else:
    client_id = 'send_' + str(uuid.uuid4()).replace('-', '_')[0:7]
repeat = args.repeat
delay = args.delay
messages = args.messages
message_ttl = args.message_ttl
sequence = args.sequence
send_complete = threading.Event()

if args.repeat is not None and repeat > 1:
    messages = messages * repeat

security_options = {}
if args.trust_certificate is not None:
    security_options['ssl_trust_certificate'] = args.trust_certificate
    if args.service != SERVICE:
        if not service.startswith('amqps'):
            error('The service URL must start with "amqps://" when using a '
                  'trust certificate.')
    else:
        service = 'amqps://localhost'

if args.file is not None:
    messages = []
    message = []
    with open(args.file, 'rb') as f:
        byte = f.read(1)
        while byte:
            message.append(byte)
            byte = f.read(1)
    if len(message) == 0:
        error('An error happened while reading {0}'.format(args.file))
    else:
        messages.append(message)


def send_next_message():
    """
    Sends the next message
    """
    if delay > 0:
        time.sleep(delay)
    send_message()


def started(err):
    """
    Started callback
    """
    print('Connected to {0} using client-id {1}'.format(
        client.get_service(), client.get_id()))
    print('Sending to: {0}'.format(topic))
    send_message()


def state_changed(state, msg=None):
    if state == mqlight.ERROR:
        error(msg)
    elif state == mqlight.DRAIN:
        send_complete.set()
        send_next_message()


def send_message():
    """
    Sends a message
    """
    if len(messages) > 0:
        with LOCK:
            send_complete.clear()
            body = messages.pop(0)
            options = {'qos': mqlight.QOS_AT_LEAST_ONCE}
            if message_ttl is not None:
                options['ttl'] = message_ttl * 1000
            if sequence and args.file is None:
                global SEQUENCE
                SEQUENCE += 1
                body = '{0}: {1}'.format(SEQUENCE, body)
            if client.send(
                    topic=topic,
                    data=body,
                    options=options,
                    on_sent=sent):
                # Send the next message now
                send_next_message()
            else:
                # There's a backlog of messages to send, so wait until the backlog
                # is cleared before sending any more
                send_complete.wait(TIMEOUT)

    else:
        # No more messages to send, so disconnect
        client.stop()


def sent(err, topic, data, options):
    """
    Message sent callback
    """
    if err:
        error('Problem with send request: {0}'.format(err))
    else:
        if data:
            print('{0}{1}'.format(
                data[:50],
                (' ...' if len(data) > 50 else '')))


def error(err):
    """
    Error callback
    """
    print('*** error ***', file=sys.stderr)
    if err:
        print(err, file=sys.stderr)
    if client:
        client.stop()
    print('Exiting.')
    exit(1)

client = None
try:
    client = mqlight.Client(
        service=service,
        client_id=client_id,
        security_options=security_options,
        on_started=started,
        on_state_changed=state_changed)
except Exception as exc:
    error(exc)
