
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
from mqlight import Client, QOS_AT_MOST_ONCE
from time import sleep
from random import randint
from threading import Thread, RLock
from argparse import ArgumentParser


# The number of clients that will connect to any given shared destination
CLIENTS_PER_SHARED_DESTINATION = 2
VERBOSE = False
STATS = None


def arguments():
    """
    Validates and processes the command arguments into variables
    """
    parser = ArgumentParser(
        description='Send and receives a number of messages to a MQ Light'
                    'server.')
    parser.add_argument(
        '-s',
        '--service',
        dest='service',
        type=str,
        default=None,
        help='service to connect to, for example: amqp://user:password@'
        'host:5672 or amqps://host:5671 to use SSL/TLS (default: amqp://'
        'localhost)'
    )
    parser.add_argument(
        '-v',
        '--verbose',
        dest='verbose',
        default=False,
        action='store_true',
        help='Increase the verbose output of the sample')
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
        action='store_false',
        help='specify to not additionally check the server\'s common name in '
             'the specified trust certificate matches the actual server\'s '
             'DNS name')
    return parser.parse_args()


class MessageTopic(object):
    """
    A help singleton class to create random message and assigned topic
    and share values.
    """
    """ The topics to subscribe for shared destinations """
    _shared_topics = ['shared1', 'shared/shared2']

    """ The topics to subscribe to for private destinations """
    _private_topics = [
      'private1',
      'private/private2',
      'private/private3',
      'private4'
    ]
    """ The complete list of all topics both private and shared """
    _all_topics = _shared_topics + _private_topics

    """
    Text used to compose message bodies.
    A random number of words are picked.
    """
    _lorem_ipsum = \
        'Lorem ipsum dolor sit amet, consectetur adipisicing elit,'\
        ' sed do eiusmod tempor incididunt ut labore et dolore '\
        'magna aliqua. Ut enim ad minim veniam, quis nostrud '\
        'exercitation ullamco laboris nisi ut aliquip ex ea '\
        'commodo consequat. Duis aute irure dolor in reprehenderit '\
        'in voluptate velit esse cillum dolore eu fugiat nulla '\
        'pariatur. Excepteur sint occaecat cupidatat non proident, '\
        'sunt in culpa qui officia deserunt mollit anim id est '\
        'laborum.'

    _lorem_ipsum_words = [0]

    def __new__(cls):
        """
        One off execution to generate the list of word separates.
        """
        next_space = -1
        while True:
            next_space = MessageTopic._lorem_ipsum.find(' ', next_space + 1)
            if next_space < 0:
                MessageTopic._lorem_ipsum_words.append(
                    len(MessageTopic._lorem_ipsum))
                break
            else:
                MessageTopic._lorem_ipsum_words.append(next_space)
                next_space += 1
        return super(MessageTopic, cls).__new__(cls)

    def _get_private_topics(self):
        """
        :returns: The list of private topics.
        """
        return self._private_topics
    private_topics = property(_get_private_topics)

    def _get_shared_topics(self):
        """
        :returns: The list of shared topics.
        """
        return self._shared_topics
    shared_topics = property(_get_shared_topics)

    def _get_topic(self):
        """
        :returns: A random topic from either private or shared list
        """
        topic = self._all_topics[randint(0, len(self._all_topics)-1)]
        return topic
    topic = property(_get_topic)

    def _get_message(self):
        """
        :returns: A message containing a random number of words
        """
        start_idx = randint(0, len(self._lorem_ipsum_words) - 2)
        end_idx = randint(start_idx + 1, len(self._lorem_ipsum_words) - 1)
        message = self._lorem_ipsum[
                    self._lorem_ipsum_words[start_idx]:
                    self._lorem_ipsum_words[end_idx]]
        return message
    message = property(_get_message)


class Stats(object):
    """
    Count the number of message sent and received and reports
    for every 10 messages.
    """
    def __init__(self):
        self._lock = RLock()
        self._sent = 0
        self._received = 0

    def output(self, msg):
        print(msg)
        sys.stdout.flush()

    def message_sent(self):
        with self._lock:
            self._sent += 1
            if self._sent % 10 == 0:
                self.output('Sent {0} messages'.format(self._sent))

    def message_received(self):
        with self._lock:
            self._received += 1
            if self._received % 10 == 0:
                self.output('Received {0} messages'.format(self._received))


class Sender(Thread):
    """
    A class extending the thread object to handle sending message t
    to the server.
    Once started the Mqlight client is created and the random message
    are sent to random  choice of topics repeatedly.
    """
    def __init__(self, service, ssl_options):
        Thread.__init__(self, name='Sender')
        self._service = service
        self._client_id = 'SEND_{0}'.format(randint(100000, 999999))
        self._ssl_options = ssl_options
        self._running = True
        self._mt = MessageTopic()

    def _start_confirmed(self, client):
        """
        Internal method call when there has been a successful connection to
        the server. This method will repeatedly send random messages to
        the server while in an active state.
        """
        if VERBOSE:
            print('Sender connected to {0} using id {1}'.
                  format(self._service, self._client_id))
        time_to_next = 0
        while self._running:
            if time_to_next <= 0:
                client.send(self._mt.topic, self._mt.message, {})
                STATS.message_sent()
                time_to_next = randint(5, 20)
            else:
                time_to_next -= 1
                sleep(1.0)
        client.stop()
        if VERBOSE:
            print('Sender {0} stopped'.format(self._client_id))

    def _state_changed(self, client, new_state, error):
        """
        Internal method call when the client has changed state. If any of the
        state change are caused by an error condition this method reports and
        then request this client to be stopped. Atypical example would
        be the server has been lost.
        """
        if error:
            err = error[1] if isinstance(error, tuple) else error
            print('*** ERROR: Sender {0} changing state to {1} because {2}'.
                  format(self._client_id, new_state, err))
            self._running = False
            self.stop()

    def run(self):
        """
        This method is called when the thread has been started. It then
        request a start of client and connection to the server.
        """
        self._client = Client(
            service=self._service,
            client_id=self._client_id,
            security_options=self._ssl_options,
            on_started=self._start_confirmed,
            on_state_changed=self._state_changed)

    def stop(self):
        """
        Request to stop this client
        """
        if VERBOSE:
            print('Sender {0} stopping'.format(self._client_id))

        def _stopped(client, err):
            self._running = False
        self._client.stop(_stopped)

    def _is_running(self):
        """
        Request to stop this client
        """
        return self._running
    running = property(_is_running)

    def _get_id(self):
        """
        :returns: the assoicated clients id
        """
        return self._client_id
    id = property(_get_id)


class Receiver(Thread):
    """
    A class extending the thread object to handle receiving messages
    from the server.
    Once started the Mqlight client is created and then subscribes to
    the given topic and optional share. It then waits to receive messages
    """
    def __init__(self, service, topic_pattern, share, ssl_options):
        Thread.__init__(self, name='Receiver')
        self._service = service
        self._topic_pattern = topic_pattern
        self._share = share
        self._ssl_options = ssl_options
        self._client_id = 'RECV_{0}'.format(randint(100000, 999999))
        self._running = True

    def _subscribed(self, client, err, pattern, share):
        """
        Internal method called when the subscription action has completed.
        """
        if VERBOSE:
            print('Receiving messages from topic pattern: \'{0}\''.
                  format(pattern))
        if VERBOSE:
            share_info = 'with share \'{0}\''.format(share) if share else ''
            print('Receiving messages from topic pattern: \'{0}\' {1}'.
                  format(pattern, share_info))

    def _message(self, message_type, data, delivery):
        """
        Internal method called when a message has been received and is
        ready to be collected.
        """
        topic = delivery['destination']['topic_pattern']
        if VERBOSE:
            if 'share' in delivery['destination']:
                print('Received message from Topic:{0} Share:{1}'.
                      format(topic, delivery['destination']['share']))
            else:
                print('Received message from Topic:{0}'.format(topic))
        STATS.message_received()

    def _start_confirmed(self, client):
        """
        Internal method called when the client has successfully connected
        to the server.
        """
        if VERBOSE:
            print('Receiver connected to {0} using id {1}'.
                  format(self._service, self._client_id))
        options = {
            'qos': QOS_AT_MOST_ONCE,
            'auto_confirm': True
        }
        client.subscribe(
            topic_pattern=self._topic_pattern,
            share=self._share,
            options=options,
            on_subscribed=self._subscribed,
            on_message=self._message)

    def _state_changed(self, client, new_state, error):
        """
        Internal method call when the client has changed state. If any of the
        state change are caused by an error condition this method reports and
        then request this client to be stopped. Atypical example would
        be the server has been lost.
        """
        if error:
            err = error[1] if isinstance(error, tuple) else error
            print('*** ERROR: Receiver {0} changing state to {1} because {2}'.
                  format(self._client_id, new_state, err))
            self.stop()

    def run(self):
        """
        This method is called when the thread has been started. It then
        request a start of client and connection to the server.
        """
        self._client = Client(
            service=self._service,
            client_id=self._client_id,
            security_options=self._ssl_options,
            on_started=self._start_confirmed,
            on_state_changed=self._state_changed)

    def stop(self):
        """
        Request to stop this client
        """
        if VERBOSE:
            print('Receiver {0} stopping'.format(self._client_id))

        def _stopped(client, err):
            self._running = False
        self._client.stop(_stopped)

    def _is_running(self):
        """
        :returns: True is the client is in an active state
        """
        return self._running
    running = property(_is_running)

    def _get_id(self):
        """
        :returns: the assoicated clients id
        """
        return self._client_id
    id = property(_get_id)


class ManageThreads(object):
    """
    A class to assist managing all the sender and receiver threads that are
    generated. The class has a list of active threads and a number of
    method to manage the overall condition.
    """
    def __init__(self):
        self._threads = []

    def add(self, thread):
        """
        Add a thread object (Receiver/Sender) to the know list.
        """
        self._threads.append(thread)
        thread.start()

    def stop_all(self):
        """
        Call the stop() method on all of the know threads.
        """
        for thread in self._threads:
            thread.stop()

    def join_all(self):
        """
        Perform a join on all of the known threads.
        :return: when all threads are no longer active.
        """
        for thread in self._threads:
            thread.join()

    """
    Monitor the current know thread list and removes any thread which
    is no longer active.
    :return: the size of the active thread list.
    """
    def monitor(self):
        for thread in self._threads:
            if not thread.running:
                self._threads.remove(thread)
        return len(self._threads)


def main():
    """
    The outer level method to start this sample.
    After initialisation and validating the arguments a number of Sender
    thread are create based on the private topic and shared topic list.
    Similarly a number of receive threads are also created. The method
    then waits for either (1) Keyboard interrupt or (2) all of the creates
    client have stopped.
    """
    args = arguments()
    global VERBOSE, STATS
    VERBOSE = args.verbose
    STATS = Stats()
    threads = ManageThreads()

    # Generate a list of possible SSL arguments
    ssl_options = {}
    ssl_arg_names = [
        'ssl_trust_certificate',
        'ssl_client_certificate',
        'ssl_client_key',
        'ssl_client_key_passphrase',
        'ssl_verify_name']
    using_ssl = False
    for ssl_arg_name in ssl_arg_names:
        if ssl_arg_name in args.__dict__ and \
                args.__dict__[ssl_arg_name] is not None:
            ssl_options[ssl_arg_name] = args.__dict__[ssl_arg_name]
            using_ssl = True

    service = args.service
    if service is None:
        service = 'amqp://localhost' if using_ssl else 'amqps://localhost'

    mt = MessageTopic()
    try:
        for topic in mt.shared_topics:
            for topic_idx in range(0, CLIENTS_PER_SHARED_DESTINATION):
                share_name = 'share' + str(topic_idx + 1)
                threads.add(Receiver(service, topic, share_name, ssl_options))
                threads.add(Sender(service, ssl_options))

        for topic in mt.private_topics:
            threads.add(Receiver(service, topic, None, ssl_options))
            threads.add(Sender(service, ssl_options))
        while threads.monitor() > 0:
            sleep(1.0)
        print('Closing ...')
    except Exception as e:
        print('*** ERROR: Failed to start a client because {0}'.format(e))
    except KeyboardInterrupt:
        print('Closing ...')
        threads.stop_all()
        if VERBOSE:
            print('Waiting for clients stop')
        threads.join_all()

if __name__ == "__main__":
    main()
