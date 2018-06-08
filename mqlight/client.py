
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

"""
mqlight
~~~~~~~
MQ Light is designed to allow applications to exchange discrete pieces of
information in the form of messages. This might sound a lot like TCP/IP
networking, and MQ Light does use TCP/IP under the covers, but MQ Light takes
away much of the complexity and provides a higher level set of abstractions to
build your applications with.
"""
from __future__ import division, absolute_import
import sys
import uuid
import threading
import os.path
import codecs
import time
import inspect
import re
from json import loads, dumps
from random import random
from pkg_resources import get_distribution, DistributionNotFound
from .utils import ManagedList, \
        SubscriptionRecord, SubscriptionRecordBuild, \
        UnsubscriptionRecord, UnsubscriptionRecordBuild, \
        SendRecord, SendRecordBuild, decode_link_address, \
        validate_callback_function, Security, Service, \
        ServiceGenerator, hide_password, is_text
import ssl
try:
    import httplib
    from urlparse import urlparse
    from urllib import quote, quote_plus
except ImportError:
    import http.client as httplib
    from urllib.parse import urlparse
    from urllib.parse import quote, quote_plus
from .exceptions import MQLightError, InvalidArgumentError, \
    NetworkError, NotPermittedError, ReplacedError, \
    StoppedError, SubscribedError, UnsubscribedError, SecurityError, \
    InternalError
from .logging import get_logger, NO_CLIENT_ID
from .definitions import QOS_AT_MOST_ONCE, QOS_AT_LEAST_ONCE
PYTHON2 = sys.version_info < (3, 0)
PYTHON3 = sys.version_info >= (3, 0)
if PYTHON3:
    from queue import Queue, Empty
    from importlib import reload
else:
    from Queue import Queue, Empty
    from exceptions import SystemExit

CMD = ' '.join(sys.argv)
if 'setup.py test' in CMD \
        or 'py.test' in CMD \
        or 'unittest' in CMD \
        or 'runfiles.py' in CMD:
    from .stubmqlproton import _MQLightMessage, \
        _MQLightMessenger, _MQLightSocket
else:
    from .mqlproton import _MQLightMessenger, _MQLightMessage, _MQLightSocket
    # The connection retry interval in seconds

if PYTHON2:
    reload(sys)
    sys.setdefaultencoding('utf8')

try:
    __version__ = get_distribution('mqlight').version
except DistributionNotFound:
    __version__ = 1.0

# Set up logging (to stderr by default). The level of output is
# configured by the value of the MQLIGHT_NODE_LOG environment
# variable. The default is 'ffdc'.
LOG = get_logger(__name__)
LOG.show_trace_header()

# Regex for the client id
INVALID_CLIENT_ID_REGEX = r'[^A-Za-z0-9%/\._]'

STARTED = 'started'
STARTING = 'starting'
STOPPED = 'stopped'
STOPPING = 'stopping'
RETRYING = 'retrying'
ERROR = 'error'
MESSAGE = 'message'
MALFORMED = 'malformed'
DRAIN = 'drain'

STATES = (
    STARTED,
    STARTING,
    STOPPED,
    STOPPING,
    RETRYING
)

CONNECT_RETRY_INTERVAL = 1


class ActiveClients(object):

    """
    Set of active clients
    """

    def __init__(self):
        self.clients = {}

    def add(self, client):
        """
        Add client to set
        """
        self.clients[client.get_id()] = client

    def remove(self, client_id):
        """
        Remove client from set
        """
        self.clients.pop(client_id, None)

    def get(self, client_id):
        """
        Get client from set
        """
        if self.has(client_id):
            client = self.clients[client_id]
        else:
            client = None
        return client

    def has(self, client_id):
        """
        Return True if the specified client is in the set
        """
        found = client_id in self.clients
        return found


def _should_reconnect(error):
    """
    Generic helper method to determine if we should automatically reconnect
    for the given type of error.
    """
    LOG.entry('_should_reconnect', NO_CLIENT_ID)
    LOG.parms(NO_CLIENT_ID, 'error:', type(error))
    result = not isinstance(error, (
        TypeError,
        InvalidArgumentError,
        NotPermittedError,
        ReplacedError,
        StoppedError,
        SubscribedError,
        UnsubscribedError,
        SecurityError))
    LOG.exit('_should_reconnect', NO_CLIENT_ID, result)
    return result


def _get_http_service_function(http, http_url):
    """
    Function to take a single HTTP URL and using the JSON retrieved from it to
    return an array of service URLs.
    """
    LOG.entry('_get_http_service_function', NO_CLIENT_ID)
    LOG.parms(NO_CLIENT_ID, 'http:', http)
    LOG.parms(NO_CLIENT_ID, 'http_url:', http_url)

    def _http_service_function(callback):
        LOG.entry('_http_service_function', NO_CLIENT_ID)
        LOG.parms(NO_CLIENT_ID, 'callback:', callback)

        func = httplib.HTTPConnection
        if http_url.scheme == 'https':
            func = httplib.HTTPSConnection
        LOG.data(NO_CLIENT_ID, 'using :', func.__name__)

        host = http_url.netloc
        if http_url.port:
            host += ':{0}'.format(http_url.port)
        LOG.data(NO_CLIENT_ID, 'host:', host)

        path = http[http.index(http_url.netloc) + len(http_url.netloc):]
        LOG.data(NO_CLIENT_ID, 'path:', path)
        try:
            conn = func(host)
            conn.request('GET', path)
            res = conn.getresponse()
            if res.status == httplib.OK:
                try:
                    json_obj = loads(res.read())
                    if 'service' in json_obj:
                        service = json_obj['service']
                    else:
                        service = None
                    callback(None, service)
                except Exception as exc:
                    err = TypeError(
                        '{0} request to {1} returned '
                        'unparseable JSON: {2}'.format(
                            http_url.scheme, http, exc))
                    LOG.error('_http_service_function', NO_CLIENT_ID, err)
                    callback(err, None)
            else:
                err = NetworkError(
                    '{0} request to {1} failed with a status code '
                    'of {2}'.format(http_url.scheme, http, res.status))
                LOG.error('_http_service_function', NO_CLIENT_ID, err)
                callback(err, None)
        except httplib.HTTPException as exc:
            err = NetworkError(
                '{0} request to {1} failed: {2}'.format(
                    http_url.scheme, http, exc))
            LOG.error('_http_service_function', NO_CLIENT_ID, err)
            callback(err, None)
        LOG.exit('_http_service_function', NO_CLIENT_ID, None)

    LOG.exit(
        '_get_http_service_function',
        NO_CLIENT_ID,
        _http_service_function)
    return _http_service_function


def _get_file_service_function(file_url):
    """
    Function to take a single FILE URL and using the JSON retrieved from it to
    return an array of service URLs.
    """
    LOG.entry('_get_file_service_function', NO_CLIENT_ID)
    LOG.parms(NO_CLIENT_ID, 'file_url:', file_url)
    if not isinstance(file_url, str):
        err = TypeError('file_url must be a string')
        LOG.error('_get_file_service_function', NO_CLIENT_ID, err)
        raise err

    file_path = file_url
    # Special case for windows drive letters in file URIS, trim the leading /
    if os.name == 'nt' and re.match(r'^\/[a-zA-Z]:\/', file_path):
        file_path = file_path[1:]

    def _file_service_function(callback):
        LOG.entry('_file_service_function', NO_CLIENT_ID)
        LOG.parms(NO_CLIENT_ID, 'callback:', callback)
        opened = False
        with codecs.open(file_path, encoding='utf-8', mode='r') as file_obj:
            try:
                opened = True
                json_obj = loads(file_obj.read())
                if 'service' in json_obj:
                    service = json_obj['service']
                else:
                    service = None
                LOG.data(NO_CLIENT_ID, 'service:', service)
                callback(None, service)
            except Exception as exc:
                err = MQLightError(
                    'The content read from {0} contained '
                    'unparseable JSON: {1}'.format(file_path, exc))
                LOG.error('_file_service_function', NO_CLIENT_ID, err)
                callback(err, None)
        file_obj.close()
        if not opened:
            err = MQLightError(
                'attempt to read {0} failed'.format(file_path))
            LOG.error('_file_service_function', NO_CLIENT_ID, err)
            callback(err, None)
        LOG.exit('_file_service_function', NO_CLIENT_ID, None)
    LOG.exit(
        '_get_file_service_function',
        NO_CLIENT_ID,
        _file_service_function)
    return _file_service_function


def _generate_service_list(service, security_options):
    """
    Function to take a single service URL, or list of service URLs, validate
    them, returning a list of service URLs
    """
    LOG.entry('_generate_service_list', NO_CLIENT_ID)
    LOG.parms(NO_CLIENT_ID, 'security_options:', security_options)

    # Ensure the service is a list
    input_service_list = []
    if not service:
        error = TypeError('service is undefined')
        LOG.error('_generate_service_list', NO_CLIENT_ID, error)
        raise error
    elif hasattr(service, '__call__'):
        error = TypeError('service cannot be a function')
        LOG.error('_generate_service_list', NO_CLIENT_ID, error)
        raise error
    elif isinstance(service, list):
        if not service:
            error = TypeError('service array is empty')
            LOG.error('_generate_service_list', NO_CLIENT_ID, error)
            raise error
        input_service_list = service
    elif isinstance(service, str):
        input_service_list = [service]
    else:
        error = TypeError('service must be a str or list type')
        LOG.error('_generate_service_list', NO_CLIENT_ID, error)
        raise error

    # Validate the list of URLs for the service, inserting default values as
    # necessary Expected format for each URL is: amqp://host:port or
    # amqps://host:port (port is optional, defaulting to 5672 or 5671 as
    # appropriate)
    service_list = []
    auth_user = None
    auth_password = None
    for i, service in enumerate(input_service_list):
        service_url = urlparse(service)
        protocol = service_url.scheme

        # Check for auth details
        if service_url.username:
            if service_url.password:
                auth_user = service_url.username
                auth_password = service_url.password
            else:
                error = InvalidArgumentError(
                    'URLs supplied via the service property must specify both '
                    'a user name and a password value, or omit both values')
                LOG.error('_generate_service_list', NO_CLIENT_ID, error)
                raise error

            user = security_options.user
            if user and user != auth_user:
                error = InvalidArgumentError(
                    'User name supplied as user property '
                    'security_options.user does not match '
                    'username supplied via a URL passed via the '
                    'service property {0}'.format(auth_user))
                LOG.error('_generate_service_list', NO_CLIENT_ID, error)
                raise error
            password = security_options.password
            if password and password != auth_password:
                error = InvalidArgumentError(
                    'Password name supplied as password property '
                    'security_options.password  does not match '
                    'password supplied via a URL passed via the '
                    'service property {0}'.format(auth_password))
                LOG.error('_generate_service_list', NO_CLIENT_ID, error)
                raise error
            if i == 0:
                security_options.url_user = auth_user
                security_options.url_password = auth_password

        # Check whatever URL user names / passwords are present this
        # time through the loop - match the ones set on security_options
        #  by the first pass through the loop.
        if i > 0:
            if security_options.url_user != auth_user:
                error = InvalidArgumentError(
                    'URLs supplied via the service property contain '
                    'inconsistent username values')
                LOG.error('_generateServiceList', NO_CLIENT_ID, error)
                raise error
            elif security_options.url_password != auth_password:
                error = InvalidArgumentError(
                    'URLs supplied via the service property contain '
                    'inconsistent password values')
                LOG.error('_generateServiceList', NO_CLIENT_ID, error)
                raise error

        # Check we are trying to use the amqp protocol
        if protocol not in ('amqp', 'amqps'):
            error = InvalidArgumentError(
                'Unsupported URL {0} specified for service. '
                'Only the amqp or amqps protocol are supported.'.format(
                    service))
            LOG.error('_generate_service_list', NO_CLIENT_ID, error)
            raise error

        # Check we have a hostname
        host = service_url.hostname
        if not host:
            error = InvalidArgumentError(
                'Unsupported URL {0} specified for service. Must supply '
                'a hostname.'.format(service))
            LOG.error('_generate_service_list', NO_CLIENT_ID, error)
            raise error

        # Set default port if not supplied
        port = None
        if hasattr(service_url, 'port'):
            port = service_url.port
        if port is None:
            port = 5672 if protocol == 'amqp' else 5671

        # Check for no path
        path = service_url.path
        if path and path != '/':
            error = InvalidArgumentError(
                'Unsupported URL {0} paths ({1}) cannot be part of a '
                'service URL.'.format(service, path))
            LOG.error('_generate_service_list', NO_CLIENT_ID, error)
            raise error

        # Check that we can reconstruct the netloc
        url = host
        if hasattr(service_url, 'port') and service_url.port is not None:
            url += ':{0}'.format(service_url.port)
        credentials = None
        if service_url.username:
            credentials = service_url.username.lower()
        if service_url.password:
            credentials += ':' + service_url.password.lower()
        if service_url.username or service_url.password:
            url = '{0}@{1}'.format(credentials, url)
        if service_url.netloc.lower() != url:
            error = InvalidArgumentError(
                'Unsupported URL {0} is not valid'.format(
                    service))
            LOG.error('_generate_service_list', NO_CLIENT_ID, error)
            raise error

        service_list.append('{0}://{1}:{2}'.format(protocol, host, port))
    LOG.exit('_generate_service_list', NO_CLIENT_ID, service_list)
    return service_list


class Client(object):

    """
    The Client class represents an MQLight client instance. Once created, the \
    class will initiate connection to the server.
    """

    def __init__(
            self,
            service,
            client_id=None,
            security_options=None,
            on_started=None,
            on_state_changed=None,
            on_drain=None):
        """Constructs and starts a new Client.

        :type service: str, list of str or function
        :param service: takes one of three types to define the address of the \
        service to connect to. As a ``str`` it is a single \
        URL connection. As a ``[str, str, ...]`` it is a list of URL \
        connections which are each tried in turn until either a connection is \
        successfully established, or all of the URLs have been tried. As a \
        function, which is invoked each time the Client wants to establish a \
        connection. The function prototype must be ``func(callback)`` and on \
        completion must perform a call to the ``callback`` function as \
        ``callback(error, services)``. Where ``error`` indicates a failure in \
        generating the service or None to indicate success. The ``services`` \
        can either be a ``str`` or a ``[str , str, ...]`` containing list URLs\
        to be attempted.
        :type client_id: str or None
        :param client_id: An identifier that is associated with \
        this client. If none is supplied then a random name will be generated.\
        The identifier must be unique and should two clients have the same \
        identifier then the server will elect which client will be \
        disconnected.
        :param dict security_options: A optional set of security options, see \
        below for details
        :type  on_started: function or None
        :param on_started: A function to be called when the Client \
        as successfully connected and reached the ``started`` state. This \
        function prototype must be ``func(client)`` where ``client`` is \
        this instance.
        :type  on_state_changed: function or None
        :param on_state_changed: A function to be called when the \
        client connection changes state. This function prototype must be \
        ``func(client, state, err)`` where ``client`` is this instance, \
        ``state`` one of started, starting, stopped, stopping, retrying. \
        ``err`` optional contains an error report that caused the state \
        changed.
        :type  on_drain: function or None
        :param function on_drain: A function to be called when a backlog of \
        messages to be sent have been cleared. The function prototype must \
        be ``func(client)``, where ``client`` is this instance.
        :return: This Client's instance.
        :raises TypeError: if an argument was the incorrect type
        :raises InvalidArgumentError: if an arguments was invalid.

        **Security options**

        * user (str) - the user reference when SASL is enabled
        * password (str) - the password for the user when SASL is enabled.
        * ssl_trust_certificate (str) - file path to the CA trust certificate
        * ssl_client_certificate (str) - file path to the client certificate
        * ssl_client_key (str)- file path to the client key
        * ssl_client_key_passphrase (str)- the passphrase for client key
        * ssl_verify_name (True, False) - True(default) will reject the \
        connection of the supplied server certificate does not match the \
        expected server host

        """
        LOG.entry('Client.__init__', NO_CLIENT_ID)
        LOG.parms(NO_CLIENT_ID, 'service:', service)
        LOG.parms(NO_CLIENT_ID, 'client_id:', client_id)
        LOG.parms(NO_CLIENT_ID, 'security_options:',
                  hide_password(security_options))
        LOG.parms(NO_CLIENT_ID, 'on_started:', on_started)
        LOG.parms(NO_CLIENT_ID, 'on_state_changed:', on_state_changed)
        LOG.parms(NO_CLIENT_ID, 'on_drain:', on_drain)

        # If client id has not been specified then generate an id
        if client_id is None:
            client_id = 'AUTO_' + str(uuid.uuid4()).replace('-', '_')[0:7]
        LOG.data('client_id', client_id)
        client_id = str(client_id)

        # If the client id is incorrectly formatted then throw an error
        self._client_id_max_len = 256
        if len(client_id) > self._client_id_max_len:
            error = InvalidArgumentError(
                'Client identifier {0} is longer than the maximum ID length '
                'of {1}'.format(client_id, self._client_id_max_len))
            LOG.error('Client.__init__', NO_CLIENT_ID, error)
            raise error

        # If client id is not a string then throw an error
        if not isinstance(client_id, str):
            error = TypeError('Client identifier must be a str')
            LOG.error('Client.__init__', NO_CLIENT_ID, error)
            raise error

        # currently client ids are restricted, reject any invalid ones
        matches = re.search(INVALID_CLIENT_ID_REGEX, client_id)
        if matches is not None:
            error = InvalidArgumentError(
                'Client Identifier {0} contains invalid char: {1}'.format(
                    client_id, matches.group(0)))
            LOG.error('Client.__init__', NO_CLIENT_ID, error)
            raise error

        # Validate and group security options.
        self._security_options = Security(security_options)
        # Validate and handle service
        self._sg = ServiceGenerator(service, self._security_options)

        validate_callback_function('on_started', on_started, 1, 0)
        validate_callback_function('on_state_changed', on_state_changed, 3, 0)
        validate_callback_function('on_drain', on_drain, 1, 0)

        self._id = client_id
        self._messenger = _MQLightMessenger(self._id)
        self._sock = None
        self._queued_chunks = []
        self._connect_thread = None

        # Set the initial state to starting
        self._state = STOPPED
        self._service = None
        # The first start, set to False after start and back to True on stop
        self._first_start = True

        # List of subscriptions and send details
        def SubscriptionComparison(item, topic_pattern, share):
            if item.topic_pattern != topic_pattern:
                return False
            if item.share != share:
                return False
            return True
        # List of message subscriptions
        self._subscriptions = ManagedList(self._id,
                                          SubscriptionRecord,
                                          SubscriptionComparison)
        self._queued_subscriptions = ManagedList(self._id,
                                                 SubscriptionRecord,
                                                 SubscriptionComparison)
        self._queued_unsubscribes = ManagedList(self._id,
                                                UnsubscriptionRecord,
                                                SubscriptionComparison)
        # List of outstanding send operations waiting to be accepted, settled,
        #
        self._outstanding_sends = ManagedList(self._id,
                                              SendRecord,
                                              None)
        self._next_message = True

        # List of queued sends for resending on a reconnect
        self._queued_sends = []
        # List of callbacks to notify when a send operation completes
        self._queued_send_callbacks = []

        # An identifier for the connection
        self._connection_id = 0

        # Connection retry timer
        self._retry_timer = None

        # Thread to process actions without blocking main thread
        self.action_handler = ActionHandler(self)
        self.action_handler.start()

        # Heartbeat
        self._heartbeat_timeout = None

        # callbacks
        self._on_started = on_started
        self._on_drain = on_drain
        self._on_stopped = None
        self._on_state_changed = on_state_changed

        # No drain event initially required
        self._on_drain_required = False

        # Number of attempts the client has tried to reconnect
        self._retry_count = 0

        self._connect_thread = self.create_thread(
            target=self._perform_connect,
            args=(on_started, True),
            name=':connect',
            user_callout=False)
        LOG.exit('Client.__init__', self._id, None)

    def _queue_on_read(self, chunk):
        self.action_handler.put((self._on_read, chunk))

    def _on_read(self, chunk):
        LOG.entry_often('Client._on_read', self._id)
        # Queue up the chunk
        self._queued_chunks.append(chunk)
        self._push_chunks()
        LOG.exit_often('Client._on_read', self._id, None)

    def _queue_on_close(self):
        self.action_handler.put((self._on_close,))

    def _on_close(self):
        LOG.entry('Client._on_close', self._id)
        self._push_chunks()
        error = None
        try:
            self._messenger.closed()
        except Exception as exc:
            LOG.error('Client._on_closed', self._id, exc)
            error = exc

        # Force any final data from the messenger, which may give it the
        # chance to close any connections.
        if self._sock is not None:
            self._messenger.pop(self._sock, True)

        if self.state == STARTED:
            self._set_state(RETRYING, error)
            if not self._subscriptions.empty:
                self._reconnect()

        LOG.exit('Client._on_close', self._id, None)

    def _push_chunks(self):
        LOG.entry_often('Client._push_chunks', self._id)
        pushed = 0

        # Build up a merged buffer of all the chunks
        if self._queued_chunks:
            chunk = b''.join(self._queued_chunks)
            self._queued_chunks = []

            # Keep pushing data into proton until the whole chunk is
            # pushed.
            while chunk:
                # Attempt to push the whole chunk into proton.
                written = self._messenger.push(chunk)

                if written < len(chunk):
                    if written <= 0:
                        # If we failed to push anything in, then quit
                        # pushing now and schedule work to happen again
                        # shortly.
                        self._queued_chunks.append(chunk)
                        self._messenger.pop(self._sock, True)
                        timer = threading.Timer(0.5, self._push_chunks)
                        timer.start()
                        LOG.exit_often('Client._push_chunks', self._id, pushed)
                        return pushed
                    else:
                        # A push into proton may mean data also needs to be
                        # written.
                        pushed += written
                        self._messenger.pop(self._sock, False)

                    # Remove the pushed part of the chunk.
                    chunk = chunk[written:]
                else:
                    # This chunk has been fully dealt with.
                    pushed += written
                    break

            # A push into proton may mean data also needs to be written.
            # Force a messenger tick.
            self._messenger.pop(self._sock, True)

            # If data has been read, messages may have arrived, so perform
            # a check.
#            if not self._subscriptions.empty:
            if self.state == STARTED:
                time.sleep(0.005)
                self._check_for_messages()
        LOG.exit_often('Client._push_chunks', self._id, None)

    def _perform_connect(self, on_started, new_client):
        """
        Performs the connection
        """
        LOG.entry('Client._perform_connect', self._id)
        LOG.parms(NO_CLIENT_ID, 'on_started:', on_started)
        LOG.parms(NO_CLIENT_ID, 'new_client:', new_client)

        if not new_client:
            current_state = self.state
            LOG.data(self._id, 'current_state:', current_state)
            # if we are not disconnected or disconnecting return with the
            # client object
            if current_state not in (STOPPING, STOPPED, RETRYING):
                if on_started:
                    LOG.entry(
                        'Client._perform_connect.on_started',
                        self._id)
                    on_started(self)
                    LOG.exit(
                        'Client._perform_connect.on_started',
                        self._id,
                        None)
                LOG.exit('Client._perform_connect', self._id, self)
                return self

        if self.state == STOPPED:
            self._set_state(STARTING)
        self.action_handler.start()

        # Obtain the list of services for connect and connect to one of the
        # services, retrying until a connection can be established
        def _service_list_callback(error, services):
            LOG.entry('Client._service_list_callback', self._id)
            LOG.parms(NO_CLIENT_ID, 'error:', error)
            LOG.parms(NO_CLIENT_ID, 'services:', services)
            if error is None:
                self._connect_to_service(on_started, services)
            else:
                if self._on_state_changed:
                    self._on_state_changed(self, STOPPED, error)
            LOG.exit('Client._service_list_callback', self._id, self)
        self._sg.get_service_list(_service_list_callback)

        LOG.exit('Client._perform_connect', self._id, None)

    def start(self, on_started=None):
        """Will initiate a request to reconnect to the MQ Light service \
        following a stop request.

        :type  on_started: function or None
        :param on_started: A function to be called when the Client \
        as successfully connected and reached the ``started`` state. This \
        function prototype must be ``func(client)`` where ``client`` is \
        this instance.
        :returns: The Client instance.
        :raises TypeError: if on_started argument is not a function.
        """
        LOG.entry('Client.start', self._id)

        if self.get_state in (STARTING, STARTED, RETRYING):
            LOG.exit('Client.start', self._id, self)
            return self

        if self.get_state == STOPPING:
            err = StoppedError('Client is in the process of stopping')
            LOG.error('Client.start', self._id, err)
            raise err

        validate_callback_function('on_started', on_started, 1, 0)
        LOG.parms(NO_CLIENT_ID, 'on_started:', on_started)

        self._perform_connect(on_started, False)

        LOG.exit('Client.start', self._id, self)
        return self

    def _process_queued_actions(self, *unused):
        """
        Called on reconnect or first connect to process any actions that may
        have been queued. Note argument is not used but present to be
        compatible with the on-start callback
        """
        # this set to the appropriate client via apply call in
        # connect_to_service
        if self is None:
            LOG.entry('_process_queued_actions', 'client was not set')
            LOG.exit(
                '_process_queued_actions',
                'client not set returning',
                None)
            return

        LOG.entry('_process_queued_actions', self._id)
        LOG.data(self._id, 'state:', self.state)

        self._reinstate_subscriptions()

        LOG.data(
            self._id,
            'client._queued_subscriptions:',
            self._queued_subscriptions)
        while not self._queued_subscriptions.empty \
                and self.state == STARTED:
            sub = self._queued_subscriptions.pop()
            if sub.ignore:
                # no-op so just trigger the callback without actually
                # subscribing
                if sub.on_subscribed:
                    sub.on_subscribed(
                        None,
                        sub.topic_pattern,
                        sub.original_share_value)
            else:
                self._subscribe_with_record(sub)
        LOG.data(
            self._id,
            'client._queued_unsubscribes:',
            self._queued_unsubscribes)

        while not self._queued_unsubscribes.empty \
                and self.state == STARTED:
            sub = self._queued_unsubscribes.pop()
            if sub.ignore:
                # no-op so just trigger the callback without actually
                # unsubscribing
                if sub.on_unsubscribed:
                    sub.on_unsubscribed(
                        None,
                        sub.topic_pattern,
                        sub.share)
            else:
                try:
                    self._unsubscribe_with_record(sub)
                except Exception in err:
                    if sub.on_unsubscribed:
                        self.create_thread(
                            target=sub.on_unsubscribed,
                            args=(err,
                                  unsubscribe.topic_pattern,
                                  unsubscribe.share),
                            name=':on_unsubscribed')
        LOG.data(
            self._id,
            'client._queued_sends:',
            self._queued_sends)
        while self._queued_sends and self.state == STARTED:
            remaining = len(self._queued_sends)
            send_info = self._queued_sends.pop(0)
            self._send_with_sendinfo(send_info)
            if len(self._queued_sends) >= remaining:
                # Calling client.send can cause messages to be added back
                # into _queued_sends, if the network connection is broken.
                # Check that the size of the array is decreasing to avoid
                # looping forever...
                break

        LOG.exit('_process_queued_actions', self._id, None)

    # Reinstate any previous registered subscriptions.
    def _reinstate_subscriptions(self):
        LOG.entry('Client.reinstate_subscriptions', self._id)
        for sub in self._subscriptions.list:
            try:
                address = sub.generate_address_with_share(
                                                self.get_service())
                LOG.data(self._id, "Reinstating sub ", address)
                self._messenger.subscribe(
                    address,
                    sub.qos,
                    sub.ttl,
                    sub.credit,
                    self._sock)

                if sub.credit > 0:
                    self._messenger.flow(
                        sub.generate_address_with_share(
                            self.get_service()),
                        sub.credit,
                        self._sock)
            except Exception as exc:
                LOG.data(self._id,
                         'Reconnect of subscription {0} '
                         'failed because {1}'.
                         format(sub.generate_address_with_share(
                            self.get_service()), exc))
                if sub.on_subscribed:
                    self.create_thread(
                        target=sub.on_subscribed,
                        args=(exc, sub.topic_pattern, sub.share),
                        name=':on_subscribed')
        LOG.exit('Client.reinstate_subscriptions', self._id, None)

    def _check_for_messages(self):
        """
        Function to force the client to check for messages. The on_message
        callback set in subscribe() is called when a message is received.
        """
        LOG.entry_often('Client._check_for_messages', self._id)
        if self.get_state() != STARTED or self._subscriptions.empty:
            LOG.exit_often('Client._check_for_messages', self._id, None)
            return
        try:
            messages = self._messenger.receive(self._sock)
            if messages:
                LOG.debug(
                    self._id,
                    'received {0} messages'.format(len(messages)))
                for i, message in enumerate(messages):
                    LOG.debug(
                        self._id,
                        'processing message {0}'.format(i))
                    self._process_message(message)
                    if i < (len(messages) - 1):
                        # Unless this is the last pass around the loop, call
                        # pop() so that Messenger has a chance to respond to
                        # any heartbeat requests that may have arrived from the
                        # server.
                        self._messenger.pop(self._sock, True)
        except ReplacedError as error:
            self._perform_disconnect(None, error)
        except Exception:
            LOG.ffdc(
                    'Client._check_for_messages',
                    'ffdc002',
                    self._id,
                    err=sys.exc_info())

        LOG.exit_often('Client._check_for_messages', self._id, None)

    def _process_message(self, msg):
        """
        Process received message
        """
        LOG.entry('Client._process_message', self._id)
        LOG.parms(self._id, 'msg:', msg)
        msg.connection_id = self._connection_id

        data = msg.body
        topic = msg.address
        if topic.startswith('amqp://'):
            topic = topic[topic.index('/', 7) + 1:]
        auto_confirm = True
        qos = QOS_AT_MOST_ONCE

        topic, share = decode_link_address(msg.link_address)
        subscription = self._subscriptions.find(topic, share)
        if subscription:
            qos = subscription.qos
            if qos == QOS_AT_LEAST_ONCE:
                auto_confirm = subscription.auto_confirm
            subscription.unconfirmed += 1
        else:
            # ideally we shouldn't get here, but it can happen in
            # a timing window if we had received a message from a
            # subscription we've subsequently unsubscribed from
            LOG.debug(
                self._id,
                'No subscription matched message: {0} going to address: '
                '{1}'.format(data, msg.address))
            msg = None
            LOG.exit('Client._process_message', self._id, None)
            return

        confirmation = {
            'delivery_confirmed': False,
        }

        def _still_settling(sub, msg):
            LOG.entry_often('Client._process_message._still_settling',
                            self._id)
            settled = self._messenger.settled(msg)
            if settled:
                sub.unconfirmed -= 1
                sub.confirmed += 1
                LOG.data(
                    self._id,
                    '[credit, unconfirmed, confirmed]:',
                    '[{0}, {1}, {2}]'.format(
                        sub.credit,
                        sub.unconfirmed,
                        sub.confirmed))
                # Ask to flow more messages if >= 80% of available
                # credit (e.g. not including unconfirmed messages)
                # has been used or we have just confirmed
                # everything
                available = sub.credit - sub.unconfirmed
                to_confirm = sub.unconfirmed == 0 and sub.confirmed > 0
                if (sub.confirmed and available / sub.confirmed <=
                        1.25) or to_confirm:
                    self._messenger.flow(
                        self._service.address + '/' + msg.link_address,
                        sub.confirmed,
                        self._sock)
                    sub.confirmed = 0
            else:
                timer = threading.Timer(0.1, _still_settling, [sub, msg])
                timer.start()
            LOG.exit_often(
                'Client._process_message._still_settling',
                self._id,
                not settled)

        def _confirm():
            LOG.entry(
                'Client._process_message._confirm',
                self._id)
            LOG.data(self._id, 'delivery:', delivery)
            LOG.data(self._id, 'msg:', msg)
            LOG.data(
                self._id,
                'delivery_confirmed:',
                confirmation['delivery_confirmed'])
            if self.is_stopped():
                err = NetworkError('not started')
                LOG.error(
                    'Client._process_message._confirm',
                    self._id,
                    err)
                raise err
            if not confirmation['delivery_confirmed'] and msg:
                # Also throw NetworkError if the client has
                # disconnected at some point since this particular
                # message was received
                if msg.connection_id != self._connection_id:
                    err = NetworkError(
                        'Client has reconnected since this '
                        'message was received')
                    LOG.error(
                        'Client._process_message._confirm',
                        self._id,
                        err)
                    raise err
                confirmation['delivery_confirmed'] = True
                self._messenger.settle(msg, self._sock)
                _still_settling(subscription, msg)
            LOG.exit(
                'Client._process_message._confirm',
                self._id,
                None)

        delivery = {
            'message': {
                'topic': topic,
            }
        }

        if qos >= QOS_AT_LEAST_ONCE and not auto_confirm:
            delivery['message']['confirm_delivery'] = _confirm
        link_address = msg.link_address
        if link_address:
            delivery['destination'] = {}
            link = link_address
            if link.startswith('share:'):
                # Remove 'share:' prefix from link name
                link = link[6:len(link_address)]
                # Extract share name and add to delivery information
                delivery['destination']['share'] = link[0:link.index(':')]
            # Extract topic_pattern and add to delivery information
            delivery['destination']['topic_pattern'] = link[
                link.index(':') + 1:len(link)]

        if msg.ttl > 0:
            delivery['message']['ttl'] = msg.ttl

        annots = msg.annotations
        malformed = {
            'MQMD': {},
            'condition': None
        }
        mal_cond = 'x-opt-message-malformed-condition'
        mal_desc = 'x-opt-message-malformed-description'
        mal_ccsi = 'x-opt-message-malformed-MQMD.CodedCharSetId'
        mal_form = 'x-opt-message-malformed-MQMD.Format'

        def as_string(value):
            if PYTHON3:
                return str(value, 'utf8')
            else:
                return value

        for annot in annots:
            if 'key' in annot:
                annot_key = as_string(annot['key'])
                if annot_key == mal_cond:
                    malformed['condition'] = as_string(annot['value'])
                elif annot_key == mal_desc:
                    malformed['description'] = as_string(annot['value'])
                elif annot_key == mal_ccsi:
                    malformed['MQMD']['CodedCharSetId'] = int(annot['value'])
                elif annot_key == mal_form:
                    malformed['MQMD']['Format'] = as_string(annot['value'])

        state = MESSAGE
        if malformed['condition']:
            state = MALFORMED
            delivery['malformed'] = malformed

        LOG.state(
            'Client._process_message',
            self._id,
            state,
            data,
            delivery)
        try:
            # TODO should this not be wrapped within it own thread?
            if subscription.on_message:
                subscription.on_message(state, data, delivery)
        except:
            LOG.error(
                'Client._process_message',
                self._id,
                sys.exc_info()[0],
                sys.exc_info()[2])
            if self._on_state_changed:
                self.create_thread(
                    target=self._on_state_changed,
                    args=(self, STOPPED, sys.exc_info()),
                    name=':on_state_changed')
        if self.is_stopped():
            LOG.debug(
                self._id,
                'client is stopped so not accepting or settling message')
            msg = None
        else:
            if qos == QOS_AT_MOST_ONCE:
                self._messenger.accept(msg)
            if qos == QOS_AT_MOST_ONCE or auto_confirm:
                self._messenger.settle(msg, self._sock)

                subscription.unconfirmed -= 1
                subscription.confirmed += 1
                LOG.data(
                    self._id,
                    '[credit, unconfirmed, confirmed]:',
                    '[{0}, {1}, {2}]'.format(
                        subscription.credit,
                        subscription.unconfirmed,
                        subscription.confirmed))

                # Ask to flow more messages if >= 80% of available
                # credit (e.g. not including unconfirmed messages)
                # has been used. Or we have just confirmed
                # everything.
                available = subscription.credit - subscription.unconfirmed
                if (subscription.confirmed and
                        (available / subscription.confirmed <= 1.25) or
                        (subscription.unconfirmed == 0) and
                        subscription.confirmed > 0):
                    self._messenger.flow(
                        self._service.address + '/' + msg.link_address,
                        subscription.confirmed,
                        self._sock)
                    subscription.confirmed = 0
                    msg = None
        LOG.exit_often('Client._process_message', self._id, None)

    def stop(self, on_stopped=None):
        """Initiates a stop request and disconnects the client from the server \
        implicitly closing any subscriptions that the client has open. \
        Once the stop has completed the optional callback is performed.

        :type on_stopped: function or None
        :param on_stopped: function to call when the connection is \
        closed. This function prototype must be ``func(client, err)`` \
         where ``client`` is the instance that has stopped and \
        ``err`` will contain any error report that occurred during the \
        stop request
        :return: The Client instance.
        :raises TypeError: if the type of any of the arguments is incorrect.
        :raises InvalidArgumentError: if any of the arguments are invalid.
        :raises TypeError: if the on_stopped argument was not a function
        """
        LOG.entry('Client.stop', self._id)

        validate_callback_function('on_stopped', on_stopped, 2, 0)
        LOG.parms(self._id, 'on_stopped:', on_stopped)

        # Cancel retry timer
        if self._retry_timer:
            self._retry_timer.cancel()

        # just return if already stopped or in the process of
        # stopping
        if self.is_stopped():
            if on_stopped:
                LOG.entry('Client.stop.on_stopped', self._id)
                on_stopped(self, None)
                LOG.exit('Client.stop.on_stopped', self._id, None)
            LOG.exit('Client.stop', self._id, self)
            return self
        self._perform_disconnect(on_stopped)
        LOG.exit('Client.stop', self._id, self)
        return self

    def _perform_disconnect(self, on_stopped, error=None):
        """
        Performs the disconnection
        """
        LOG.entry('Client._perform_disconnect', self._id)
        LOG.parms(self._id, 'on_stopped:', on_stopped)
        LOG.parms(self._id, 'error:', error)
        self._set_state(STOPPING, error)

        if self._connect_thread and \
                self._connect_thread != threading.current_thread():
            self._connect_thread.join(1)
        if self._retry_timer:
            self._retry_timer.join(1)
        # Only disconnect when all outstanding send operations are complete
        if self._outstanding_sends.empty:
            def stop_processing(client, on_stopped):
                LOG.entry(
                    'Client._perform_disconnect.stop_processing',
                    self._id)
                if client._heartbeat_timeout:
                    client._heartbeat_timeout.cancel()
                    client._heartbeat_timeout = None

                if self._sock:
                    self._sock.close()
                    self._sock = None

                # Clear all queued sends as we are disconnecting
                while self._queued_sends:
                    msg = self._queued_sends.pop(0)

                    def next_tick():
                        """
                        next tick
                        """
                        LOG.entry(
                            'Client._perform_disconnect.next_tick',
                            self._id)
                        msg.on_sent(
                            StoppedError(
                                'send aborted due to disconnect'),
                            None,
                            None,
                            None)
                        LOG.exit(
                            'Client._perform_disconnect.next_tick',
                            self._id,
                            None)
                    timer = threading.Timer(1, next_tick)
                    timer.start()

                # Discard current subscriptions
                self._subscriptions.clear()
                # Indicate that we've disconnected
                client._set_state(STOPPED)

                # Wakeup the action_handler_thread
                self.action_handler.wakeup()

                LOG.state(
                    'Client._perform_disconnect.stop_processing',
                    self._id,
                    STOPPED)
                if not self._first_start:
                    self._first_start = True
                    if self._on_state_changed:
                        self._on_state_changed(self, STOPPED, None)

                if on_stopped:
                    LOG.entry(
                        'Client._perform_disconnect.on_stopped',
                        self._id)
                    on_stopped(self, None)
                    LOG.exit(
                        'Client._perform_disconnect.on_stopped',
                        self._id,
                        None)
                LOG.exit(
                    'Client._perform_disconnect.stop_processing',
                    self._id,
                    None)
            self._stop_messenger(stop_processing, on_stopped)
            LOG.exit('Client._perform_disconnect', self._id, None)
            return

        # Try disconnect again
        timer = threading.Timer(1, self._perform_disconnect, [on_stopped])
        timer.start()
        LOG.exit('Client._perform_disconnect', self._id, None)

    def _stop_messenger(self, stop_processing_callback, callback=None):
        """
        Function to trigger the client to disconnect
        """
        LOG.entry('Client._stop_messenger', self._id)
        LOG.parms(
            NO_CLIENT_ID,
            'stop_processing_callback:',
            stop_processing_callback)
        LOG.parms(NO_CLIENT_ID, 'callback:', callback)
        stopped = True
        # If messenger available then request it to stop
        # (otherwise it must have already been stopped)
        if self._messenger:
            stopped = self._messenger.stop(self._sock, 100)
            if stopped:
                if self._heartbeat_timeout:
                    self._heartbeat_timeout.cancel()
                    self._heartbeat_timeout = None
                stop_processing_callback(self, callback)

        LOG.exit('Client._stop_messenger', self._id, None)

    def _connect_to_service(self, callback, services):
        """
        Function to connect to the service, tries each available service in
        turn. If none can connect it emits an error, waits and attempts to
        connect again. Callback happens once a successful connect/reconnect
        occurs
        """
        LOG.entry('Client._connect_to_service', self._id)
        LOG.parms(NO_CLIENT_ID, 'callback:', callback)
        LOG.parms(NO_CLIENT_ID, 'services:', services)
        if self.is_stopped():
            if callback and self._on_state_changed is not None:
                LOG.entry('Client._connect_to_service.callback', self._id)
                self._on_state_changed(
                    self, STOPPED,
                    StoppedError('connect aborted due to stop request'))
                LOG.exit('Client._connect_to_service.callback', self._id, None)
            LOG.exit('Client._connect_to_service', self._id, None)
            return
        error = None
        connected = False

        # Try each service in turn until we can successfully connect, or
        # exhaust the list
        for i, service in enumerate(services):
            if connected:
                break
            try:
                LOG.data(self._id, 'attempting to connect to:', str(service))
                try:
                    self._sock = _MQLightSocket(
                        service,
                        self._security_options,
                        self._queue_on_read,
                        self._queue_on_close)
                    self._messenger.connect(service)
                    # Wait for client to start
                    while not self._messenger.started():
                        # Pass any data to proton.
                        self._messenger.pop(self._sock, False)
                        if self.state not in (RETRYING, STARTING):
                            # Don't keep waiting if we're no longer in a
                            # starting state
                            LOG.data(self._id, 'client no longer starting')
                            break
                        time.sleep(0.5)
                    else:
                        connected = True
                except (ssl.SSLError) as exc:
                    error = SecurityError(exc)
                    LOG.data(
                        self._id,
                        'failed to connect to: {0} due to error: {1}'.format(
                            service, error))
                except (NetworkError, SecurityError) as exc:
                    error = exc
                    LOG.data(
                        self._id,
                        'failed to connect to: {0} due to error: {1}'.format(
                            service, error))
                except Exception as exc:
                    LOG.ffdc(
                        'Client._connect_to_service',
                        'ffdc012',
                        self._id,
                        err=sys.exc_info())
                    error = exc
                if connected:
                    LOG.data(
                        self._id,
                        'successfully connected to:',
                        service)
                    self._service = service

                    # Indicate that we're connected
                    self._set_state(STARTED)
                    event_to_emit = None
                    if self._first_start:
                        event_to_emit = STARTED
                        self._first_start = False
                        self._retry_count = 0
                        LOG.data(self._id, 'first start since being stopped')
                        self._process_queued_actions()
                    else:
                        self._retry_count = 0
                        event_to_emit = STARTED
                    self._connection_id += 1

                    # Fire callbacks
                    LOG.state(
                        'Client._connect_to_service',
                        self._id,
                        event_to_emit)

                    if self._on_state_changed:
                        self.create_thread(
                                       target=self._on_state_changed,
                                       args=(self, event_to_emit, None),
                                       name=':on_state_changed')
                    if callback:
                        self.create_thread(
                                       target=callback,
                                       args=(self,),
                                       name=':callback')

                    # Setup heartbeat timer to ensure that while connected we
                    # send heartbeat frames to keep the connection alive, when
                    # required.
                    timeout = self._messenger.get_remote_idle_timeout(
                                                    service.address_only)
                    interval = timeout / 2 if timeout > 0 else timeout
                    LOG.data(self._id, 'heartbeat_interval:', interval)
                    if interval > 0:
                        def perform_heartbeat(interval):
                            LOG.entry(
                                'Client._connect_to_service.perform_heartbeat',
                                self._id)
                            if self._messenger:
                                self._messenger.heartbeat(self._sock)
                                self._heartbeat_timeout = threading.Timer(
                                    interval,
                                    perform_heartbeat,
                                    [interval])
                                self._heartbeat_timeout.start()
                            LOG.exit(
                                'Client._connect_to_service.perform_heartbeat',
                                self._id,
                                None)
                        self._heartbeat_timeout = threading.Timer(
                            interval,
                            perform_heartbeat,
                            [interval])
                        self._heartbeat_timeout.start()
            except Exception as exc:
                # Should never get here, as it means that messenger.connect has
                # been called in an invalid way, so FFDC
                error = exc
                LOG.ffdc(
                    'Client._connect_to_service',
                    'ffdc003',
                    self._id,
                    err=sys.exc_info())
                raise MQLightError(exc)

        if not connected and not self.is_stopped():
            def retry():
                LOG.entry('Client._connect_to_service.retry', self._id)
                if not self.is_stopped():
                    self._perform_connect(callback, False)
                LOG.exit(
                    'Client._connect_to_service.retry',
                    self._id,
                    None)
            if _should_reconnect(error):
                # We've tried all services without success. Pause for a while
                # before trying again
                self._set_state(RETRYING, error)

                self._retry_count += 1
                retry_cap = 60
                # limit to the power of 8 as anything above this will put the
                # interval higher than the cap straight away.
                exponent = self._retry_count if self._retry_count <= 8 else 8
                upper_bound = pow(2, exponent)
                lower_bound = 0.75 * upper_bound
                jitter = random() * (0.25 * upper_bound)
                interval = min(retry_cap, (lower_bound + jitter))
                # times by CONNECT_RETRY_INTERVAL for unittest purposes
                # TODO Review retry internal seem tooo long
                interval = round(interval) * CONNECT_RETRY_INTERVAL
                LOG.data(
                    self._id,
                    'trying to connect again after {0} seconds'.format(
                        interval))
                self._retry_timer = threading.Timer(interval, retry)
                self._retry_timer.start()
            else:
                self._perform_disconnect(None, error)

        LOG.exit('Client._connect_to_service', self._id, None)

    def _reconnect(self):
        """
        Reconnects the client to the MQ Light service, The 'reconnected' event
        will be emitted once the client has reconnected.

        Returns:
            The instance of the client if reconnect succeeded otherwise None
        """
        LOG.entry('Client._reconnect', self._id)
        if self.is_stopped():
            LOG.exit('Client._reconnect', self._id, None)
            return None

        # Stop the messenger to free the object then attempt a reconnect
        def stop_processing(client, callback=None):
            LOG.entry('Client.reconnect.stop_processing', client.get_id())

            if client._heartbeat_timeout:
                client._heartbeat_timeout.cancel()

            if self.state not in (STOPPING, STOPPED):
                # Initiate the reconnection
                self._set_state(RETRYING)
                client._perform_connect(self._process_queued_actions, False)

            LOG.exit('Client.reconnect.stop_processing', client.get_id(), None)

        self._stop_messenger(stop_processing)
        LOG.exit('Client._reconnect', self._id, self)
        return self

    def get_id(self):
        """
        :returns: The client id
        """
        LOG.data(self._id, self._id)
        return self._id

    def get_service(self):
        """
        :returns: The service if connected otherwise ``None``
        """
        if self.state == STARTED:
            address = self._service.address_only
            LOG.data(self._id, 'service:', address)
            return address
        else:
            LOG.data(self._id, 'Not connected')
            LOG.data(self._id, 'service: None')
            return None

    def get_state(self):
        """
        :returns: The state of the client

        **States**

        * started - client is connected to the server and ready to process \
        messages.
        * starting - client is attempting to connect to the server following \
        a stop event.
        * stopped - client is stopped and not connected to the server. This \
        can occur following a user request or a non-recovery connection error
        * stopping - occurs before ``stopped`` state and is closing any \
        current connections.
        * retrying - attempting to connect to the server following a \
        recoverable error. Previous states would be ``starting`` or ``started``
        """
        LOG.data(self._id, 'state:', self._state)
        return self._state

    def _set_state(self, new_state, error=None):
        """
        Sets the state of the client
        """
        LOG.entry('Client._set_state', self._id)
        LOG.parms(self._id, 'new state:', new_state)
        LOG.parms(self._id, 'error:', str(type(error)), error)
        LOG.parms(self._id, 'error type:', str(type(error)))

        if new_state not in STATES:
            raise InvalidArgumentError('invalid state')

        if self._state != new_state:
            self._state = new_state
            if self._on_state_changed:
                LOG.state('Client._set_state', self._id, self._state)
                self.create_thread(
                               target=self._on_state_changed,
                               args=(self, self._state, error),
                               name=':on_state_changed')

        LOG.exit('Client._set_state', self._id, self._state)
        return self._state

    state = property(get_state)

    def is_stopped(self):
        """
        :returns: ``True`` if the Client is in the stopped or stopping state,
            otherwise ``False``
        """
        LOG.data(self._id, 'state:', self.state)
        return self.state in (STOPPED, STOPPING)

    def send(self, topic, data, options=None, on_sent=None):
        """Sends a message to the MQLight service.

        :param str topic: The topic of the message to be sent to.
        :type data: str or bytearray.
        :param data: Body of the message.  A ``str`` will be send as Text and \
        ``bytearray`` will be sent as Binary.
        :param dict options: A set of attributes for the message to be sent, \
        see details below.
        :param function on_sent: A function to called when the message has \
        been sent. \
        This function prototype must be \
        ``func(client, err, topic, data,  options)`` where  \
        ``client`` is the instance that has completed the send, \
        ``err`` contains an error message or ``None`` if was successfully \
        sent \
        ``topic`` is the topic that the message was sent to, \
        ``data`` is the body of the message sent, \
        ``options`` are the message attributes (see below).
        :returns: ``True`` if this message was either sent or is the next \
        to be sent or ``False`` if the message was queued in user memory, \
        because either there was a backlog of messages, or the client was not \
        in a started state.
        :raises TypeError: if an argument had an incorrect type.
        :raises RangeError: if an argument was out of range
        :raises InvalidArgumentError: if an argument was invalid
        :raises StoppedError: if the client is stopped.

        **Message attributes**

        * ``qos`` specifies the \
        quality of service. This can be 1 for at-least-once, where the \
        client waits to receive confirmation of the server received the \
        message before issuing a ``on_sent`` callback, or 0 for at-most-once, \
        where there is no confirmation and no callback.
        * ``ttl`` specifies the time-to-live of the message in milli-seconds. \
        This is how long the message will persist if sent to a topic that \
        has a subscription that hasn't expired.
        """
        LOG.entry('Client.send', self._id)
        self._next_message = False

        sb = SendRecordBuild(self._id)
        sb.set_topic(topic)
        sb.set_options(options)
        sb.set_on_sent(on_sent)
        sb.set_data(data)
        send_info = sb.build_send_record()
        rc = self._send_with_sendinfo(send_info)
        LOG.exit('Client.send', self._id, rc)
        return rc

    def _send_with_sendinfo(self, send_info):
        LOG.entry('Client._send_with_sendinfo', self._id)
        LOG.parms(self._id, 'send_info:', send_info)
        # Ensure we have attempted a connect
        if self.is_stopped():
            raise StoppedError('not started')

        # Ensure we are not retrying otherwise queue message and return
        if self.state in (RETRYING, STARTING):
            self._queued_sends.append(send_info)
            self._on_drain_required = True
            self._reconnect()
            LOG.exit('Client._send_with_sendinfo', self._id, False)
            return False
        # TODO - really confused here over the parameters
        self.action_handler.put(self._send(send_info))
        # FIXME: the drain behaviour seems badly implemented to me
        self._next_message = len(self._outstanding_sends.list) <= 1
        LOG.exit('Client._send_with_sendinfo', self._id, self._next_message)
        return self._next_message

    def _send(self, send_info):
        """
        The internals of the send method that takes place without
        blocking the main thread.
        """
        # Send the data as a message to the specified topic
        msg = None
        try:
            msg = _MQLightMessage()
            msg.address = self.get_service() + '/' + send_info.topic
            if send_info.ttl:
                msg.ttl = send_info.ttl
            if is_text(send_info.data):
                msg.content_type = 'text/plain'
                msg.body = send_info.data
            elif isinstance(send_info.data, bytearray):
                msg.content_type = 'application/octet-stream'
                msg.body = send_info.data
            else:
                msg.content_type = 'application/json'
                msg.body = dumps(send_info.data)
            send_info.message = msg

            # Record that a send operation is in progress
            self._outstanding_sends.append(send_info)
            self._messenger.put(send_info.message, send_info.qos)
            self._messenger.send(self._sock)

            if len(self._outstanding_sends.list) == 1:
                def send_outbound_msg():
                    LOG.entry('Client.send.send_outbound_msg', self._id)
                    LOG.data(
                        self._id,
                        'Number of pending messages: ',
                        self._outstanding_sends)
                    try:
                        if not self._messenger.stopped:
                            # Write any data buffered within messenger
                            tries = 50
                            p_o = self._messenger.pending_outbound(
                                self.get_service())
                            while tries > 0 and p_o:
                                tries -= 1
                                self._messenger.send(self._sock)
                                p_o = self._messenger.pending_outbound(
                                    self.get_service())

                            if tries == 0:
                                LOG.debug(self._id, 'Output still pending')

                            # See if any of the outstanding send operations
                            # have now been completed
                            LOG.data(
                                self._id,
                                'Number of pending messages: ',
                                self._outstanding_sends)
                            while not self._outstanding_sends.empty:
                                in_flight = self._outstanding_sends.read()
                                try:
                                    status = str(
                                                 self._messenger.status(
                                                     in_flight.message))
                                # we get a TypeError because a tracker
                                # hasn't been set on the message yet,
                                # so skip and try again
                                except TypeError:
                                    time.sleep(0.1)
                                    continue
                                LOG.data(self._id,
                                         'proton send status:',
                                         status)
                                complete = False
                                err = None
                                if in_flight.qos == QOS_AT_MOST_ONCE:
                                    complete = (status == 'UNKNOWN')
                                else:
                                    if status in ('ACCEPTED', 'SETTLED'):
                                        self._messenger.settle(
                                            in_flight.message,
                                            self._sock)
                                        complete = True
                                    elif status == 'REJECTED':
                                        complete = True
                                        err_msg = self._messenger.status_error(
                                            in_flight.message)
                                        if err_msg is None or err_msg == '':
                                            err_msg = 'send failed - ' \
                                                'message was rejected'
                                        err = MQLightError(err_msg)
                                    elif status == 'RELEASED':
                                        complete = True
                                        err = MQLightError(
                                            'send failed - message was '
                                            'released')
                                    elif status == 'MODIFIED':
                                        complete = True
                                        err = MQLightError(
                                            'send failed - message was '
                                            'modified')
                                    elif status == 'ABORTED':
                                        complete = True
                                        err = MQLightError(
                                            'send failed - message was '
                                            'aborted')
                                    elif status == 'PENDING':
                                        self._messenger.send(self._sock)
                                LOG.data(self._id, 'complete:', complete)
                                if complete:
                                    # Remove send operation from list of
                                    # outstanding send ops
                                    self._outstanding_sends.pop()

                                    # Generate drain event
                                    if self._on_drain_required and len(
                                            self._outstanding_sends.list) <= 1:
                                        LOG.data(
                                            'Client.send.send_outbound_msg',
                                            self._id,
                                            "Has been drained")

                                        self._on_drain_required = False
                                        if self._on_drain:
                                            self.create_thread(
                                                target=self._on_drain,
                                                args=(self,),
                                                name=':on_drain')

                                    # invoke on_sent, if specified
                                    if in_flight.on_sent:
                                        LOG.entry(
                                            'Client.send.send_outbound_msg.'
                                            'cb1',
                                            self._id)
                                        self.create_thread(
                                            target=in_flight.on_sent,
                                            args=(self,
                                                  err,
                                                  in_flight.topic,
                                                  in_flight.message.body,
                                                  in_flight.options),
                                            name=':on_sent')
                                        LOG.exit(
                                            'Client.send.send_outbound_msg.'
                                            'cb1',
                                            self._id,
                                            None)
                                else:
                                    # Can't make any more progress for now -
                                    # schedule remaining work for processing in
                                    # the future
                                    self.action_handler.put((
                                        send_outbound_msg,))
                                    LOG.exit_often(
                                        'Client.send.send_outbound_msg',
                                        self._id,
                                        None)
                                    return
                        else:
                            # Messenger has been stopped
                            self._queued_sends = self._outstanding_sends
                            self._outstanding_sends = []

                    except Exception as exc:
                        error = exc
                        callback_error = None
                        LOG.error(
                            'Client.send.send_outbound_msg',
                            self._id,
                            error)

                        # Error so empty the outstanding_sends array
                        while not self._outstanding_sends.empty:
                            in_flight = self._outstanding_sends.pop()
                            if in_flight.qos == QOS_AT_LEAST_ONCE:
                                # Retry AT_LEAST_ONCE messages
                                self._queued_sends.append(in_flight)
                            else:
                                # we don't know if an at-most-once message made
                                # it across. Call the callback with an err of
                                # null to indicate success otherwise the
                                # application could decide to resend
                                # (duplicate) the message
                                if in_flight.on_sent:
                                    LOG.entry(
                                        'Client.send.send_outbound_msg.cb2',
                                        self._id)
                                    try:
                                        in_flight.on_sent(
                                            None,
                                            in_flight.topic,
                                            in_flight.msg.body,
                                            in_flight.options)
                                    except Exception as exc:
                                        LOG.error(
                                            'Client.send.send_outbound_msg.'
                                            'cb2',
                                            self._id,
                                            exc)
                                        if callback_error is None:
                                            callback_error = exc
                                    LOG.exit(
                                        'Client.send.send_outbound_msg.cb2',
                                        self._id,
                                        None)

                        if error:
                            LOG.error(
                                'Client.send.send_outbound_msg',
                                self._id,
                                error)

                        if _should_reconnect(error):
                            self._reconnect()

                        if callback_error is not None:
                            LOG.error(
                                'Client.send.send_outbound_msg',
                                self._id,
                                callback_error)
                            raise callback_error

                    LOG.exit_often(
                        'Client.send.send_outbound_msg',
                        self._id,
                        None)
                send_outbound_msg()

            # If we have a backlog of messages, then record the need to emit a
            # drain event later to indicate the backlog has been cleared
            LOG.data(
                self._id,
                'outstandingSends:',
                len(self._outstanding_sends.list))
            if len(self._outstanding_sends.list) <= 1:
                self._next_message = True
            else:
                self._on_drain_required = True

        except MQLightError as exc:
            send_info.error = exc
            LOG.error('Client.send', self._id, exc)

            # Error condition so won't retry send need to remove it from list
            # of unsent
            if not self._outstanding_sends.empty:
                self._outstanding_sends.pop()

            if send_info.qos == QOS_AT_LEAST_ONCE:
                self._queued_sends.append(send_info)

            self._queued_send_callbacks.append(send_info)
            # Reconnect can result in many callbacks being fired in a single
            # tick, group these together into a single chunk to avoid them
            # being spread out over a, potentially, long period of time.
            if len(self._queued_send_callbacks) <= 1:
                def immediate():
                    do_reconnect = False
                    while self._queued_send_callbacks:
                        invocation = self._queued_send_callbacks.pop()
                        if invocation.on_sent:
                            if invocation.qos == QOS_AT_MOST_ONCE:
                                LOG.entry('Client.send.on_sent', NO_CLIENT_ID)
                                invocation.on_sent(
                                    invocation.error,
                                    invocation.topic,
                                    invocation.data,
                                    invocation.options)
                                LOG.exit(
                                    'Client.send.on_sent',
                                    NO_CLIENT_ID,
                                    None)
                        LOG.error(
                            'Client.send',
                            self._id,
                            invocation.error)
                        do_reconnect |= _should_reconnect(invocation.error)
                    if do_reconnect:
                        self._reconnect()
                self.create_thread(target=immediate, name=':immediate',
                                   user_callout=False)
        except Exception as exc:
            # Unexcepted exception so report it
            LOG.ffdc(
                'Client._send ',
                'ffdc004',
                self._id,
                err=sys.exc_info(),
                data=send_info)

    def subscribe(
            self,
            topic_pattern,
            share=None,
            options=None,
            on_subscribed=None,
            on_message=None):
        """Initiates a subscription with the server and issue message callbacks
        each time a message arrives for matches topic pattern.

        :param str topic_pattern: The topic to subscribe to.
        :type share: str or None
        :param share: The share name of the subscription.
        :type options: dict or None
        :param options: The subscription attributes , see note below
        :type on_subscribed: function on None
        :param on_subscribed: A function to call when the subscription has \
        completed. This function prototype must be \
        `func(client, err, pattern, share)`` where ``client`` is the \
        instance that has completed the subscription, ``err`` is ``None`` if \
        the client subscribed successfully otherwise contains an error \
        message, ``pattern`` is the subscription pattern and \
        ``share`` is the share name.
        :type on_message: function on None
        :param on_message: function to call when a message is received. \
        This function prototype must be \
        ``func(message_type, message, delivery)`` where \
        ``message_type`` is 'message' if a wellformed message has been \
        received or 'malformed' if a malformed message has been received \
        ``message`` is the message contents and \
        ``delivery`` is the associate information for the message.
        :return: The client instance.
        :raises TypeError: if an argument has an incorrect type
        :raises RangeError: if an argument is not within certain values.
        :raise StoppedError: if the client is stopped
        :raises InvalidArgumentError: if an argument is invalid.

        **Subscription Attributes**

        * qos - specifies the quality of service. This can be 0 for \
        ``at-most-once`` and no confirmation is required and 1 for \
        ``at-least-once`` where a confirmation is required. See \
        ``auto_confirm`` attribute
        * ttl - specifies the time-to-live of the subscription in \
        milli-seconds. This is how long the subscription will persist \
        before being destroyed.
        * credit - specifies the link credit: the number of messages that can \
        be sent without confirmation before the server stops delivering \
        messages from the subscription. The default value is 1024 and a \
        value 0 will block messages being received.
        * auto_confirm - a value of ``True`` means the client will \
        automatically confirm  messages as they are received and a value \
        of ``False`` will require the caller to manaully confirm each \
        message. This is performed by the function call within the delivery \
        object of the message
        """
        LOG.entry('Client.subscribe', self._id)

        srb = SubscriptionRecordBuild(self._id)
        srb.set_topic_pattern(topic_pattern)
        srb.set_share(share)
        srb.set_options(options)
        srb.set_on_subscribed(on_subscribed)
        srb.set_on_message(on_message)
        subscription = srb.build_subscription_record()
        rc = self._subscribe_with_record(subscription)
        LOG.exit('Client.subscribe', self._id, rc)
        return rc

    def _subscribe_with_record(self, subscription):
        LOG.entry('Client._subscribe_with_record', self._id)
        LOG.parms(self._id, 'subscription:', subscription)
        # Ensure we have attempted a connect
        if self.is_stopped():
            raise StoppedError('not started')

        # If client is in the retrying state, then queue this subscribe request
        if self.state in (RETRYING, STARTING):
            LOG.data(
                self._id,
                'Client waiting for connections so queue subscription')
            # first check if its already there and if so remove old and add new
            self._queued_subscriptions.remove(subscription)
            self._queued_subscriptions.append(subscription)
            LOG.exit('Client._subscribe_with_record', self._id, self)
            return self
        self._subscribe(subscription)
        LOG.exit('Client._subscribe_with_record', self._id, self)
        return self

    def _subscribe(self, subscription):
        LOG.entry('Client._subscribe', self._id)
        err = None
        # if we already believe this subscription exists, we should reject the
        # request to subscribe by throwing a SubscribedError
        if self._subscriptions.find(subscription.topic_pattern,
                                    subscription.share):
            err = SubscribedError(
                'client is already subscribed to this address')
            LOG.error('Client._subscribe', self._id, err)
            raise err

        def finished_subscribing(err, callback):
            LOG.entry('Client._subscribe.finished_subscribing', self._id)
            LOG.parms(self._id, 'err:', err)
            LOG.parms(self._id, 'callback:', callback)

            if err:
                self._subscriptions.remove(subscription)
                # Is the error indicate a close/disconnect then update state
                if isinstance(err, SecurityError):
                    self._set_state(STOPPED, err)
                if isinstance(err, NetworkError):
                    self._set_state(RETRYING, err)

            if callback:
                self.create_thread(
                    target=callback,
                    args=(self,
                          err,
                          subscription.topic_pattern,
                          subscription.share),
                    name=':subscribing')

            LOG.exit('Client._subscribe.finished_subscribing', self._id, None)

        if err is None:
            try:
                self._subscriptions.append(subscription)
                self._messenger.subscribe(
                    subscription.generate_address_with_share(
                        self.get_service()),
                    subscription.qos,
                    subscription.ttl,
                    subscription.credit,
                    self._sock)

                def still_subscribing(on_subscribed):
                    LOG.entry('Client._subscribe.still_subscribing', self._id)
                    err = None
                    try:
                        address = subscription.generate_address_with_share(
                            self.get_service())
                        while not self._messenger.subscribed(address):
                            if self.state == STOPPED:
                                err = StoppedError('not started')
                                break
                            time.sleep(0.5)
                        else:
                            if subscription.credit > 0:
                                self._messenger.flow(
                                    address,
                                    subscription.credit,
                                    self._sock)
                            finished_subscribing(
                                        err,
                                        subscription.on_subscribed)
                    except Exception as exc:
                        LOG.error(
                            'Client._subscribe.still_subscribing',
                            self._id,
                            exc)
                        finished_subscribing(exc, subscription.on_subscribed)
                    LOG.exit(
                        'Client._subscribe.still_subscribing',
                        self._id,
                        None)

                self.create_thread(
                    target=still_subscribing,
                    args=(subscription.on_subscribed,),
                    daemon=True,
                    name=':on_subscribed',
                    user_callout=False)
            except MQLightError as exc:
                LOG.error('Client._subscribe', self._id, exc)
                finished_subscribing(exc, subscription.on_subscribed)
            except Exception as exc:
                LOG.ffdc(
                    'Client._send ',
                    'ffdc005',
                    self._id,
                    err=sys.exc_info(),
                    data=subscription)
        else:
            finished_subscribing(err, subscription.on_subscribed)
        LOG.exit('Client._subscribe', self._id, self)
        return self

    def unsubscribe(
            self,
            topic_pattern,
            share=None,
            options=None,
            on_unsubscribed=None):
        """Initiates the disconnection of an existing subscription and thereby \
        stop the flow of messages.

        :param str topic_pattern: the topic_pattern that was supplied in the
            previous call to subscribe.
        :type share: str or None
        :param share: the share that was supplied in the previous
            call to subscribe.
        :type options: dict or None
        :param options: Subscription attributes, see note below
        :type on_unsubscribed: function or None
        :param on_unsubscribed: Indicates the unsubscribe request has \
        compeleted. This function prototype must be \
        ``func(client, err, pattern, share)`` where \
        ``client`` is the instance that has completed the unsubscription, \
        ``err`` is ``None`` if the client unsubscribed successfully \
        otherwise contains an error message, \
        ``pattern`` is the unsubscription pattern \
        and ``share`` is the share name.
        :returns: The instance of the client.
        :raises TypeError: if an argument has an incorrect type.
        :raises RangeError: if an argument is not within certain values.
        :raises StoppedError: if the client is stopped.
        :raises InvalidArgumentError: if an argument has an invalid value.

        **Subscription attributes**

        * ttl - a value of 0 will result in the subscription being deleted \
        within the server. A positive value will indicate the time in \
        milliseconds that existing and new message persist before they are \
        removed.
        """
        LOG.entry('Client.unsubscribe', self._id)
        usrb = UnsubscriptionRecordBuild(self._id, self.get_service())
        usrb.set_topic_pattern(topic_pattern)
        usrb.set_share(share)
        usrb.set_options(options)
        usrb.set_on_unsubscribed(on_unsubscribed)
        unsubscribe = usrb.build_unsubscription_record()
        rc = self._unsubscribe_with_record(unsubscribe)
        LOG.exit('Client.unsubscribe', self._id, rc)
        return rc

    def _unsubscribe_with_record(self, unsubscribe):
        LOG.entry('Client._unsubscribe_with_record', self._id)
        LOG.parms(self._id, 'unsubscribe:', unsubscribe)

        # Ensure we have attempted a connect
        if self.is_stopped():
            err = StoppedError('not started')
            LOG.error('Client._unsubscribe_with_record', self._id, err)
            raise err

        address_with_share = unsubscribe.generate_address_with_share(
            self.get_service())

        # Check that there is actually a subscription for the pattern and share
        if self._subscriptions.find(unsubscribe.topic_pattern,
                                    unsubscribe.share):
            pass
        else:
            if self._queued_subscriptions.find(unsubscribe.topic_pattern,
                                               unsubscribe.share):
                pass
            else:
                err = UnsubscribedError(
                    'client is not subscribed to this address: {0}'.format(
                        address_with_share))
                LOG.error('Client._unsubscribe_with_record', self._id, err)
                raise err

        def queue_unsubscribe():
            """Add the unsubscribe request to the internal queue"""
            # check if there's a queued subscribe for the same topic, if so
            # mark that as a no-op operation, so the callback is called but a
            # no-op takes place on reconnection
            noop = False
            sub = self._queued_subscriptions.find(unsubscribe.topic_pattern,
                                                  unsubscribe.share)
            if sub:
                sub.ignore = True
                noop = True

            # queue unsubscribe request as appropriate
            if noop:
                LOG.data(
                    self._id,
                    'client already had a queued subscribe '
                    'request for this address, so marked that as a noop and '
                    'will queue this unsubscribe request as a noop too')
            else:
                LOG.data(self._id, 'client waiting for connection so '
                         'queueing the unsubscribe request')

            self._queued_unsubscribes.append(unsubscribe)

        # if client is in the retrying state, then queue this unsubscribe
        # request
        if self.state in (RETRYING, STARTING):
            LOG.data(
                self._id,
                'client still in the process of connecting '
                'so queueing the unsubscribe request')
            queue_unsubscribe()
            LOG.exit('Client._unsubscribe_with_record', self._id, self)
            return self

        def finished_unsubscribing(err, callback):
            LOG.entry(
                'Client._unsubscribe_with_record.finished_unsubscribing',
                self._id)
            LOG.parms(self._id, 'err:', err)
            LOG.parms(self._id, 'callback:', callback)

            if err:
                LOG.error('Client._unsubscribe_with_record', self._id, err)
                if _should_reconnect(err):
                    queue_unsubscribe()
                    self._reconnect()
            else:
                # if no errors, remove this from the stored list of
                # subscriptions
                sub = self._subscriptions.find(unsubscribe.topic_pattern,
                                               unsubscribe.share)
                if sub:
                    self._subscriptions.remove(sub)

            if callback:
                self.create_thread(
                    target=callback,
                    args=(self,
                          err,
                          unsubscribe.topic_pattern,
                          unsubscribe.share),
                    name=':on_unsubscribed')
            LOG.exit(
                'Client._unsubscribe_with_record.finished_unsubscribing',
                self._id,
                None)

        # unsubscribe using the specified topic pattern and share options
        try:
            self._messenger.unsubscribe(address_with_share,
                                        unsubscribe.ttl,
                                        self._sock)

            def still_unsubscribing(on_unsubscribed):
                LOG.entry('Client._unsubscribe_with_record.still_'
                          'unsubscribing', self._id)
                err = None
                try:
                    while not self._messenger.unsubscribed(
                            address_with_share):
                        if self.state == STOPPED:
                            err = StoppedError('not started')
                            break
                        time.sleep(0.5)
                    else:
                        finished_unsubscribing(err,
                                               unsubscribe.on_unsubscribed)
                except MQLightError as exc:
                    LOG.error(
                        'Client._unsubscribe_with_record.still_unsubscribing',
                        self._id,
                        exc)
                    finished_unsubscribing(exc, unsubscribe.on_unsubscribed)
                except Exception as exc:
                    LOG.ffdc(
                        'Client._send ',
                        'ffdc006',
                        self._id,
                        err=sys.exc_info(),
                        data=unsubscribe)
                LOG.exit(
                    'Client._unsubscribe_with_record.still_unsubscribing',
                    self._id,
                    None)

            self.create_thread(
                target=still_unsubscribing,
                args=(unsubscribe.on_unsubscribed,),
                daemon=True,
                name=':on_unsubscribed',
                user_callout=False)

        except MQLightError as exc:
            LOG.error('Client._unsubscribe_with_record', self._id, exc)
            finished_unsubscribing(exc, unsubscribe.on_unsubscribed)
        except Exception as exc:
            LOG.ffdc(
                'Client._send ',
                'ffdc007',
                self._id,
                err=sys.exc_info(),
                data=unsubscribe)

        LOG.exit('Client._unsubscribe_with_record', self._id, self)
        return self

    def create_thread(self, target, name=None, args=None,
                      daemon=False, user_callout=True):
        LOG.entry('create_thread', NO_CLIENT_ID)
        LOG.parms(NO_CLIENT_ID, 'name:', name)
        LOG.parms(NO_CLIENT_ID, 'target:', target)
        LOG.parms(NO_CLIENT_ID, 'args:', args)
        LOG.parms(NO_CLIENT_ID, 'daemon:', daemon)
        LOG.parms(NO_CLIENT_ID, 'user_callout:', user_callout)
        if target is None:
            LOG.ffdc(
                    'Client.create_thread',
                    'ffdc011',
                    NO_CLIENT_ID,
                    InternalError('Target does not have a valid value'))

        def thread_wrapper(*args, **kwargs):
            try:
                target(*args, **kwargs)
            except:
                if user_callout:
                    if isinstance(sys.exc_info()[1], SystemExit):
                        LOG.data('User requested exit({0})'.
                                 format(sys.exc_info()[1]))
                        exit(sys.exc_info()[1])
                    LOG.error('create_thread',
                              NO_CLIENT_ID,
                              'Detected user generated exception of \'{0}\''.
                              format(sys.exc_info()[1]))
                    self._set_state(STOPPED, sys.exc_info())
                elif LOG:
                    LOG.ffdc(
                        'Client.thread_wrapper',
                        'ffdc001',
                        NO_CLIENT_ID,
                        err=sys.exc_info())
        parms = dict(target=thread_wrapper)
        if args is not None:
            parms.update({'args': args})
        t = threading.Thread(**parms)
        if name:
            t.setName(t.getName() + name)
        t.setDaemon(daemon)
        t.start()
        LOG.exit('create_thread', NO_CLIENT_ID, t)
        return t


class ActionHandler():
    """
    Class to handle the processing of queued actions
    """
    def __init__(self, client):
        self._action_handler_lock = threading.Lock()
        self.client = client
        self._id = client._id
        self._action_queue = Queue()

    def start(self):
        LOG.entry('ActionHandler.start', self._id)
        if self._action_handler_lock.locked():
            LOG.data(self._id, 'Action handler already running - skipping')
            return
        self._action_handler_thread = self.client.create_thread(
            target=self._action_handler,
            daemon=True,
            name=':Action-Handler',
            user_callout=False)
        LOG.exit('ActionHandler.start', self._id, None)

    def put(self, args):
        self._action_queue.put(args)

    def wakeup(self):
        self._action_queue.put((lambda *args: None,))

    def _action_handler(self):
        LOG.entry('ActionHandler._action_handler', self._id)
        with self._action_handler_lock:
            while self.client.state not in STOPPED:
                try:
                    args = self._action_queue.get(True, 5)
                    if args:
                        callback = args[0]
                        if len(args) > 1:
                            callback(*args[1:])
                        else:
                            callback()
                except Empty:
                    pass
        LOG.exit('ActionHandler._action_handler', self._id, None)
