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
SERVICE = 'amqp://localhost'

parser = argparse.ArgumentParser(
    description='Connect to an MQ Light server and subscribe to the ' + \
    'specified topic.')
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
    '--topic-pattern',
    dest='topic_pattern',
    type=str,
    default='public',
    help='subscribe to receive messages matching TOPIC_PATTERN '+
        '(default: %(default)s)')
parser.add_argument(
    '-i',
    '--id',
    dest='client_id',
    type=str,
    default=None,
    help='the ID to use when connecting to MQ Light ' +
        '(default: send_[0-9a-f]{7})')
parser.add_argument(
    '--destination-ttl',
    dest='destination_ttl',
    type=int,
    default=None,
    help='set destination time-to-live to DESTINATION_TTL seconds ' +
        '(default: %(default)s)')
parser.add_argument(
    '-n',
    '--share-name',
    dest='share_name',
    type=str,
    default=None,
    help='optionally, subscribe to a shared destination using SHARE_NAME as ' +
        'the share name.')
parser.add_argument(
    '-f',
    '--file',
    dest='file',
    type=str,
    default=None,
    help='write the payload of the next message received to FILE ' +
        '(overwriting previous file contents then end. (default is to print ' +
        'messages to stdout)')
parser.add_argument(
    '-d',
    '--delay',
    dest='delay',
    type=int,
    default=0,
    help='delay for DELAY seconds each time a message is received. (default: ' +
        '%(default)s)')
parser.add_argument(
    '--verbose',
    dest='verbose',
    action='store_true',
    help='print additional information about each message.')
args = parser.parse_args()

service = args.service
topic_pattern = args.topic_pattern
if args.client_id is not None:
    client_id = args.client_id
else:
    client_id = 'recv_' + str(uuid.uuid4()).replace('-', '_')[0:7]
delay = args.delay
share = args.share_name
verbose = args.verbose

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

def subscribe(err):
    print 'Connected to ' + client.get_service() + ' using client-id ' + \
        client.get_id()
    options = {
        'qos': mqlight.QOS_AT_LEAST_ONCE,
        'autoConfirm': False
    }
    if args.destination_ttl is not None:
        options['ttl'] = args.destination_ttl
    if args.delay is not None and args.delay > 0:
        options['credit'] = 1
    client.add_listener(mqlight.MESSAGE, message)
    client.subscribe(topic_pattern, share, options, subscribed)

def subscribed(err, pattern, share):
    if err is not None:
        print '*** error ***'
        print 'problem with subscribe request ', err
        exit(1)
    if pattern:
        if share:
            print 'Subscribed to share: ' + share + ' pattern: ' + pattern
        else:
            print 'Subscribed to pattern: ' + pattern

def message(data, delivery):
    global COUNT
    COUNT += 1
    if verbose:
        print '# received message ', COUNT
    if args.file:
        print 'Writing message data to ' + args.file
        with open(args.file, 'wb') as f:
            f.write(''.join(data))
        delivery['message']['confirm_delivery'](None)
        client.stop()
    else:
        print data
        if verbose:
            print delivery
        if delay > 0:
            time.sleep(delay)
        delivery['message']['confirm_delivery'](None)

def error(err):
    print '*** error ***'
    if err:
        print err
    client.stop()
    print 'Exiting.'
    exit(1)

def malformed(data, delivery):
    print '*** received malformed message ***'
    print 'data: ', data
    print 'delivery: ', delivery

client = mqlight.Client(service, client_id, security_options)
client.add_listener(mqlight.STARTED, subscribe)
client.add_listener(mqlight.ERROR, error)
client.add_listener(mqlight.MALFORMED, malformed)
