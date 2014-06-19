import argparse
import time
from mqlight import *

id = 'recv.py'
hostname = 'localhost'
port = 5672

parser = argparse.ArgumentParser(description='Connect to an MQLight broker and subscribe to the specified topic.')
parser.add_argument('topic', type=str, nargs='?', default='public', help='topic')
parser.add_argument('-a', '--address', dest='address', type=str, default='amqp://' + hostname + ':' + str(port), help='address of the MQLight broker (default: amqp://' + hostname + ':' + str(port) + ')')
parser.add_argument('-m', '--max', dest='maxmsg', type=int, default=10, help='maximum number of messages to receive (default: 10)')
parser.add_argument('-t', '--timeout', dest='timeout', type=int, default=60, help='maximum number of seconds to wait for messages (default: 60)')
args = parser.parse_args()

topic = args.topic
maxmsg = args.maxmsg
address = args.address
timeout = args.timeout
count = 0

if maxmsg < 1:
    print 'The maximum number of message must be a positive number'
    quit()

if timeout < 0:
    print 'The timeout must be a positive number'
    quit()

def subscribe(value):
    print 'Connected to ' + address + ' using client-id ' + client.get_id()
    print 'Subscribing to: ' + topic
    client.on(MESSAGE, message)
    client.subscribe(topic, subscribed)
    t = threading.Timer(timeout, timedout)
    t.start()

def timedout():
    print 'Timeout reached, disconnecting'
    client.disconnect()

def subscribed(err, pattern):
    if err is not None:
        print 'error subscribing'
    else:
        print 'subscribed to ' + pattern

def message(args):
    global count
    count = count + 1
    print '# received message ' + str(count)
    print str(args[0])
    print str(args[1])
    if count == maxmsg:
        print 'Received the maximum number of messages, disconnecting ...'
        client.disconnect()

def callback_connect(err):
    if err:
        print 'error connecting ' + str(err)
    return

client = create_client('amqp://localhost:5672', id)
client.on(CONNECTED, subscribe)
client.connect(callback_connect)
