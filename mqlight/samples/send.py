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
import argparse
import mqlight
import time
import uuid

COUNT = 0
SEQUENCE = 0
SERVICE = 'amqp://localhost'

parser = argparse.ArgumentParser(
    description='Send a message to an MQ Light server.')
parser.add_argument(
    '-s',
    '--service',
    dest='service',
    type=str,
    default=SERVICE,
    help='service to connect to, for example: amqp://user:password@host:5672 ' +
        'or amqps://host:5671 to use SSL/TLS (default: %(default)s)')
parser.add_argument(
    '-c',
    '--trust-certificate',
    dest='trust_certificate',
    type=str,
    default=None,
    help='use the certificate contained in FILE (in PEM or DER format) to ' +
        'validate the identify of the server. The connection must be secured ' +
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
    help='the ID to use when connecting to MQ Light ' +
        '(default: send_[0-9a-f]{7})')
parser.add_argument(
    '--message-ttl',
    dest='message_ttl',
    type=int,
    default=None,
    help='set message time-to-live to MESSAGE_TTL seconds ' +
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
    help='send messages REPEAT times, if REPEAT <= 0 then repeat forever ' +
        '(default: %(default)s)')
parser.add_argument(
    '--sequence',
    dest='sequence',
    type=bool,
    default=False,
    help='prefix a sequence number to the message payload, ignored for ' +
        'binary messages (default: %(default)s)')
parser.add_argument(
    '-f',
    '--file',
    dest='file',
    type=str,
    help='send FILE as binary data. Cannot be specified at the same time as ' +
        'MESSAGE')
parser.add_argument(
    'messages',
    metavar='MESSAGE',
    type=str,
    nargs='*',
    default=['Hello world !'],
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

security_options = {}
if args.trust_certificate is not None:
    security_options['ssl_trust_certificate'] = args.trust_certificate
    if args.service != SERVICE:
        if not service.startswith('amqps'):
            print '*** error ***'
            print 'The service URL must start with "amqps://" when using a ' + \
                'trust certificate.'
            print 'Exiting.'
            exit(1)
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
        print '*** error ***'
        print 'An error happened while reading ' + args.file
        print 'Exiting.'
        exit(1)
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
    print 'Connected to ' + client.get_service() + ' using client-id ' + \
        client.get_id()
    print 'Sending to: ' + topic
    send_message()

def send_message():
    """
    Sends a message
    """
    global COUNT
    msg_num = COUNT
    COUNT += 1

    # Check if messages should be repeated again
    if len(messages) == COUNT:
        global repeat
        if repeat != 1:
            COUNT = 0
        if repeat > 1:
            repeat -= 1

    # Keep going until all messages have been sent
    if len(messages) > msg_num:
        body = messages[msg_num]
        options = { 'qos': mqlight.QOS_AT_LEAST_ONCE }
        if message_ttl is not None:
            options['ttl'] = message_ttl
        if sequence and args.file is None:
            global SEQUENCE
            SEQUENCE += 1
            body = str(SEQUENCE) + ': ' + body
        if client.send(topic=topic, data=body, options=options, callback=sent):
            send_next_message()
        else:
            client.add_once_listener(mqlight.DRAIN, send_next_message)
    else:
        # No more messages to send, so disconnect
        client.stop()

def sent(err, topic, data, options):
    """
    Message sent callback
    """
    if err:
        print 'Problem with send request: ' + str(err)
        client.stop()
        exit(1)
    else:
        if data:
            print '# sent message:'
            print 'data: ', data
            print 'topic: ', topic
            print 'options: ', options

def error(err):
    """
    Error callback
    """
    print '*** error ***'
    if err:
        print err
    client.stop()
    print 'Exiting.'
    exit(1)

# Create client to connect to server with
client = mqlight.Client(service, client_id, security_options)
client.add_listener(mqlight.STARTED, started)
client.add_listener(mqlight.ERROR, error)
