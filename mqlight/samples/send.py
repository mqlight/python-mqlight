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
import time
import mqlight

id = 'send.py'
hostname = 'localhost'
port = 5672

parser = argparse.ArgumentParser(
    description='Send a message to an MQLight broker.')
parser.add_argument(
    'messages',
    metavar='N',
    type=str,
    nargs='*',
    default=["Hello world !"],
    help='message to be sent')
parser.add_argument(
    '-t',
    '--topic',
    dest='topic',
    type=str,
    default='public',
    help='topic of the message (default: public)')
parser.add_argument(
    '-a',
    '--address',
    dest='address',
    type=str,
    default='amqp://' + hostname + ':' + str(port),
    help='address of the MQLight broker (default: amqp://' + hostname +  \
    ':' + str(port) + ')')
parser.add_argument(
    '-d',
    '--delay',
    dest='delay',
    type=int,
    default=0,
    help='delay in seconds between each request (default: 0)')
args = parser.parse_args()

messages = args.messages
delay = args.delay
topic = args.topic
address = args.address
count = 0
options = {
    'qos': mqlight.QOS_AT_LEAST_ONCE
}

def send_messages(value):
    print 'Connected to ' + address + ' using client-id ' + client.get_id()
    print 'Publishing to: ' + topic
    for msg in messages:
        client.send(topic=topic, data=msg, options=options, callback=sent)

def sent(err, topic, data, options):
    global count
    count += 1
    if err:
        'Problem with send request: ', err.message
        quit()
    else:
        if data:
            print '# sent message:'
            print data
            print topic
            print options
    if count == len(messages):
        client.stop()

client = mqlight.Client(address, id)
client.add_listener(mqlight.STARTED, send_messages)
