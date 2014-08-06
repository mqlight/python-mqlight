import argparse
import time
from mqlight import *

id = 'send.py'
hostname = 'localhost'
port = 5672

parser = argparse.ArgumentParser(description='Send a message to an MQLight broker.')
parser.add_argument('messages', metavar='N', type=str, nargs='*', default=["Hello world !"], help='message to be sent')
parser.add_argument('-t', '--topic', dest='topic', type=str, default='public', help='topic of the message (default: public)')
parser.add_argument('-a', '--address', dest='address', type=str, default='amqp://' + hostname + ':' + str(port), help='address of the MQLight broker (default: amqp://' + hostname + ':' + str(port) + ')')
parser.add_argument('-d', '--delay', dest='delay', type=int, default=0, help='delay in seconds between each request (default: 0)')
args = parser.parse_args()

messages = args.messages
delay = args.delay
topic = args.topic
address = args.address
count = 0

def send_messages(value):
    print 'Connected to ' + address + ' using client-id ' + client.get_id()
    print 'Publishing to: ' + topic
    for msg in messages:
        client.send(topic=topic, data=msg, callback=sent)

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

client = Client(address, id)
client.add_listener(STARTED, send_messages)
