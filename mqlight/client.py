# <copyright
# notice="lm-source-program"
# pids="5725-P60"
# years="2013,2014"
# crc="3568777996" >
# Licensed Materials - Property of IBM
#
# 5725-P60
#
# (C) Copyright IBM Corp. 2013, 2014
#
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
# </copyright>
"""
mqlight
~~~~~~~
MQ Light is designed to allow applications to exchange discrete pieces of
information in the form of messages. This might sound a lot like TCP/IP
networking, and MQ Light does use TCP/IP under the covers, but MQ Light
takes away much of the complexity and provides a higher level set of
abstractions to build your applications with.
"""
import uuid
import threading
import os.path
import re
import sys
import codecs
import httplib
import socket
import traceback
from . import mqlightexceptions as mqlexc
from .mqlightlog import get_logger, NO_CLIENT_ID
from json import loads
from random import random
from urlparse import urlparse
from urllib import quote
from pkg_resources import get_distribution, DistributionNotFound

CMD = ' '.join(sys.argv)
if 'setup.py test' in CMD or 'unittest' in CMD:
    from . import stubproton as mqlightproton
    # The connection retry interval in seconds
    CONNECT_RETRY_INTERVAL = 1
else:
    from . import mqlightproton
    # The connection retry interval in seconds
    CONNECT_RETRY_INTERVAL = 10

try:
    __version__ = get_distribution('mqlight').version
except DistributionNotFound:
    __version__ = 1.0

# Set up logging (to stderr by default). The level of output is
# configured by the value of the MQLIGHT_NODE_LOG environment
# variable. The default is 'ffdc'.
LOG = get_logger(__name__)

# Regex for the client id
INVALID_CLIENT_ID_REGEX = r'[^A-Za-z0-9%/\._]'

STARTED = 'started'
STARTING = 'starting'
STOPPED = 'stopped'
STOPPING = 'stopping'
RESTARTED = 'restarted'
RETRYING = 'retrying'
ERROR = 'error'
MESSAGE = 'message'
MALFORMED = 'malformed'
DRAIN = 'drain'

QOS_AT_MOST_ONCE = 0
QOS_AT_LEAST_ONCE = 1

STATES = (
    STARTED,
    STARTING,
    STOPPED,
    STOPPING,
    RETRYING
)

QOS = (
    QOS_AT_MOST_ONCE,
    QOS_AT_LEAST_ONCE
)


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

ACTIVE_CLIENTS = ActiveClients()


class SecurityOptions(object):

    """
    Wrapper object for the security options arguments
    """

    def __init__(self, options):
        if 'property_user' in options:
            self.property_user = options['property_user']
        else:
            self.property_user = None
        if 'property_password' in options:
            self.property_password = options['property_password']
        else:
            self.property_password = None
        self.url_user = None
        self.url_password = None
        if 'ssl_trust_certificate' in options:
            self.ssl_trust_certificate = options['ssl_trust_certificate']
        else:
            self.ssl_trust_certificate = None
        if 'ssl_verify_name' in options:
            self.ssl_verify_name = options['ssl_verify_name']
        else:
            self.ssl_verify_name = True

    def __str__(self):
        return 'SecurityOptions=(' \
            'property_user: {0}, '  \
            'property_password: {1}, ' \
            'url_user: {2},' \
            'url_pass: {3},' \
            'ssl_trust_certificate: {4}, ' \
            'ssl_verify_name: {5}' \
            ')'.format(
                self.property_user,
                ('********' if self.property_password else 'None'),
                self.url_user,
                ('********' if self.url_password else 'None'),
                self.ssl_trust_certificate,
                self.ssl_verify_name)

    def __repr__(self):
        return str(self)


def _should_reconnect(error):
    """
    Generic helper method to determine if we should automatically reconnect
    for the given type of error.
    """
    LOG.entry('_should_reconnect', NO_CLIENT_ID)
    result = type(error) not in (
        TypeError,
        mqlexc.InvalidArgumentError,
        mqlexc.ReplacedError,
        mqlexc.StoppedError,
        mqlexc.SubscribedError,
        mqlexc.UnsubscribedError)
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
                    callback(err)
            else:
                err = mqlexc.NetworkError(
                    '{0} request to {1} failed with a status code '
                    'of {2}'.format(http_url.scheme, http, res.status))
                LOG.error('_http_service_function', NO_CLIENT_ID, err)
                callback(err, None)
        except (httplib.HTTPException, socket.error) as exc:
            err = mqlexc.NetworkError(
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
                err = mqlexc.MQLightError(
                    'The content read from {0} contained '
                    'unparseable JSON: {1}'.format(file_path, exc))
                LOG.error('_file_service_function', NO_CLIENT_ID, err)
                callback(err, None)
        file_obj.close()
        if not opened:
            err = mqlexc.MQLightError(
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
    elif isinstance(service, unicode):
        input_service_list = [str(service)]
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
    for i in range(len(input_service_list)):
        service_url = urlparse(input_service_list[i])
        protocol = service_url.scheme

        # Check for auth details
        if service_url.username:
            if service_url.password:
                auth_user = service_url.username
                auth_password = service_url.password
            else:
                error = mqlexc.InvalidArgumentError(
                    'URLs supplied via the service property must '
                    'specify both a user name and a password value, '
                    'or omit both values')
                LOG.error('_generate_service_list', NO_CLIENT_ID, error)
                raise error

            user = security_options.property_user
            if user and user != auth_user:
                error = mqlexc.InvalidArgumentError(
                    'User name supplied as user property '
                    'security_options.property_user does not match '
                    'username supplied via a URL passed via the '
                    'service property {0}'.format(auth_user))
                LOG.error('_generate_service_list', NO_CLIENT_ID, error)
                raise error
            password = security_options.property_password
            if password and password != auth_password:
                error = mqlexc.InvalidArgumentError(
                    'Password name supplied as password property '
                    'security_options.property_password  does not match '
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
                error = mqlexc.InvalidArgumentError(
                    'URLs supplied via the service property contain '
                    'inconsistent username values')
                LOG.error('_generateServiceList', NO_CLIENT_ID, error)
                raise error
            elif security_options.url_password != auth_password:
                error = mqlexc.InvalidArgumentError(
                    'URLs supplied via the service property contain '
                    'inconsistent password values')
                LOG.error('_generateServiceList', NO_CLIENT_ID, error)
                raise error

        # Check we are trying to use the amqp protocol
        if protocol not in ('amqp', 'amqps'):
            error = mqlexc.InvalidArgumentError(
                'Unsupported URL {0} specified for service. '
                'Only the amqp or amqps protocol are supported.'.format(
                    input_service_list[i]))
            LOG.error('_generate_service_list', NO_CLIENT_ID, error)
            raise error

        # Temporary disable amqps
        if protocol == 'amqps':
            error = NotImplementedError(
                'SSL is currently not implemented')
            LOG.error('_genere_service_list', NO_CLIENT_ID, error)
            raise error

        # Check we have a hostname
        host = service_url.hostname
        if host is None or host == '':
            error = mqlexc.InvalidArgumentError(
                'Unsupported URL {0} specified for service. Must supply '
                'a hostname.'.format(input_service_list[i]))
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
            error = mqlexc.InvalidArgumentError(
                'Unsupported URL {0} paths ({1}) cannot be part of a '
                'service URL.'.format(input_service_list[i], path))
            LOG.error('_generate_service_list', NO_CLIENT_ID, error)
            raise error

        # Check that we can reconstruct the netloc
        url = host
        if hasattr(service_url, 'port') and service_url.port is not None:
            url += ':' + str(service_url.port)
        credentials = None
        if service_url.username:
            credentials = service_url.username.lower()
        if service_url.password:
            credentials += ':' + service_url.password.lower()
        if service_url.username or service_url.password:
            url = credentials + '@' + url
        if service_url.netloc.lower() != url:
            error = mqlexc.InvalidArgumentError(
                'Unsupported URL {0} is not valid'.format(
                    input_service_list[i]))
            LOG.error('_generate_service_list', NO_CLIENT_ID, error)
            raise error

        service_list.append('{0}://{1}:{2}'.format(protocol, host, port))
    LOG.exit('_generate_service_list', NO_CLIENT_ID, service_list)
    return service_list


class Client(object):

    """
    The Client class represents an MQLight client instance.
    """

    def __init__(
            self,
            service,
            client_id=None,
            security_options=None,
            on_started=None,
            on_state_changed=None):
        """Constructs and starts a new Client.

        :param service: when an instance of string, this is a URL to
            connect to. When an instance of list, this is a list of URLs
            to connect to - each will be tried in turn until either a
            connection is successfully established to one of the URLs, or
            all of the URLs have been tried. When an instance of function
            is specified for this argument, then the function is invoked each
            time the Client wants to establish a connection.
        :param client_id: (optional) an identifier that is used to identify
            this client. Two different instances of Client can have the same
            id, however only one instance can be connected to the MQ Light
            service at a given moment in time. If two instances of Client
            have the same id and both try to connect then the first
            instance to establish its connection is disconnected in favour
            of the second instance. If this property is not specified then
            the Client will generate a probabilistically unique ID.
        :param security_options: (optional) Any required security options for
            user name/password authentication and SSL.
        :param on_started: (optional) A function to be called when the Client
            reaches the started state. This function prototype must be
            ``func(err)`` where ``err`` is ``None`` if the client started
            correctly, otherwise it is the error message.
        :param on_state_changed: (optional) A function to be called when the
            client changes state. This function prototype must be
            ``func(state, msg)`` where ``state`` is started, starting, stopped,
            stopping, restarted, retrying, error or drain and ``msg`` is
            ``None`` except if state is error, in this case it is the error
            message.
        :return: The Client instance.
        :raises TypeError: if the type of any of the arguments is incorrect.
        :raises mqlexc.InvalidArgumentError: if any of the arguments are
            invalid.
        """
        LOG.entry('Client.constructor', NO_CLIENT_ID)
        LOG.parms(NO_CLIENT_ID, 'service:', service)
        LOG.parms(NO_CLIENT_ID, 'client_id:', client_id)
        LOG.parms(NO_CLIENT_ID, 'security_options:', security_options)
        LOG.parms(NO_CLIENT_ID, 'on_started:', on_started)
        LOG.parms(NO_CLIENT_ID, 'on_state_changed:', on_state_changed)

        # Ensure the service is a list or function
        service_function = None
        if hasattr(service, '__call__'):
            service_function = service
        elif isinstance(service, str):
            service_url = urlparse(service)
            if service_url.scheme in ('http', 'https'):
                service_function = _get_http_service_function(
                    service, service_url)
            elif service_url.scheme == 'file':
                if (service_url.hostname and
                        service_url.hostname != 'localhost'):
                    error = mqlexc.InvalidArgumentError(
                        'service contains unsupported file URI of {0}'
                        ', only file:///path or file://localhost/path are '
                        ' supported.'.format(service))
                    LOG.error('Client.constructor', NO_CLIENT_ID, error)
                    raise error
                service_function = _get_file_service_function(service_url.path)

        # If client id has not been specified then generate an id
        if client_id is None:
            client_id = 'AUTO_' + str(uuid.uuid4()).replace('-', '_')[0:7]
        LOG.data('client_id', client_id)
        client_id = str(client_id)

        # If the client id is incorrectly formatted then throw an error
        if len(client_id) > 48:
            error = mqlexc.InvalidArgumentError(
                'Client identifier {0} is longer than the maximum ID length '
                'of 48'.format(client_id))
            LOG.error('Client.constructor', NO_CLIENT_ID, error)
            raise error

        # If client id is not a string then throw an error
        if not isinstance(client_id, str):
            error = TypeError('Client identifier must be a str')
            LOG.error('Client.constructor', NO_CLIENT_ID, error)
            raise error

        # currently client ids are restricted, reject any invalid ones
        matches = re.search(INVALID_CLIENT_ID_REGEX, client_id)
        if matches is not None:
            error = mqlexc.InvalidArgumentError(
                'Client Identifier {0} contains invalid char: {1}'.format(
                    client_id, matches.group(0)))
            LOG.error('Client.constructor', NO_CLIENT_ID, error)
            raise error

        # User/password must either both be present, or both be absent.
        if security_options:
            if isinstance(security_options, dict):
                s_o = SecurityOptions(security_options)
                # User/password must either both be present, or both be absent.
                if (s_o.property_user and s_o.property_password is None) or (
                        s_o.property_user is None and s_o.property_password):
                    error = mqlexc.InvalidArgumentError(
                        'both user and password properties must be '
                        'specified together')
                    LOG.error('Client.constructor', NO_CLIENT_ID, error)
                    raise error
            else:
                error = TypeError('security_options must be a dict')
                LOG.error('Client.constructor', NO_CLIENT_ID, error)
                raise error

            # Validate the ssl security options
            if s_o.ssl_verify_name:
                if s_o.ssl_verify_name not in [True, False]:
                    error = mqlexc.InvalidArgumentError(
                        'ssl_verify_name value {0} is invalid. '
                        'Must evaluate to True of False'.format(
                            s_o.ssl_verify_name))
                    LOG.error('Client.constructor', NO_CLIENT_ID, error)
                    raise error
            if s_o.ssl_trust_certificate:
                if not isinstance(s_o.ssl_trust_certificate, str):
                    error = TypeError(
                        'ssl_trust_certificate value {0} is invalid. '
                        'Must be a string'.format(s_o.ssl_trust_certificate))
                    LOG.error('Client.constructor', NO_CLIENT_ID, error)
                    raise error
                if not os.path.isfile(s_o.ssl_trust_certificate):
                    error = TypeError(
                        'The file specified for ssl_trust_certificate is not '
                        'a regular file')
                    LOG.error('Client.constructor', NO_CLIENT_ID, error)
                    raise error
        else:
            s_o = SecurityOptions({})

        if on_started and not hasattr(on_started, '__call__'):
            error = TypeError('on_started must be a function')
            LOG.error('Client.constructor', NO_CLIENT_ID, error)
            raise error

        if on_state_changed and not hasattr(on_state_changed, '__call__'):
            error = TypeError('on_state_changed must be a function')
            LOG.error('Client.constructor', NO_CLIENT_ID, error)
            raise error

        # Save the required data as client fields
        self._service_function = service_function
        self._service_list = None
        self._service_param = service
        self._id = client_id
        self._security_options = s_o

        self._messenger = mqlightproton._MQLightMessenger(self._id)

        # Set the initial state to starting
        self._state = STARTING
        self._service = None
        # The first start, set to False after start and back to True on stop
        self._first_start = True

        # List of message subscriptions
        self._subscriptions = []
        self._queued_subscriptions = []
        self._queued_unsubscribes = []

        # List of outstanding send operations waiting to be accepted, settled,
        # etc by the listener
        self._outstanding_sends = []

        # List of queued sends for resending on a reconnect
        self._queued_sends = []
        # List of callbacks to notify when a send operation completes
        self._queued_send_callbacks = []

        # An identifier for the connection
        self._connection_id = 0

        # Connection retry timer
        self._retry_timer = None

        # Heartbeat
        self._heartbeat_timeout = None

        # callbacks
        self._on_started = on_started
        self._on_stopped = None
        self._on_state_changed = on_state_changed

        # No drain event initially required
        self._on_drain_required = False

        # Number of attempts the client has tried to reconnect
        self._retry_count = 0

        if service_function is None:
            self._service_list = _generate_service_list(
                service,
                self._security_options)

        # Check that the id for this instance is not already in use. If it is
        # then we need to stop the active instance before starting
        if ACTIVE_CLIENTS.has(self._id):
            LOG.data(
                self._id,
                'stopping previously active client with same client id')
            previous_active_client = ACTIVE_CLIENTS.get(self._id)
            ACTIVE_CLIENTS.add(self)

            def stop_callback(err):
                LOG.data(
                    self._id,
                    'stopped previously active client with same client id')
                err = mqlexc.LocalReplacedError()
                LOG.error('Client.constructor', self._id, err)
                if previous_active_client._on_state_changed:
                    previous_active_client._on_state_changed(ERROR, err)

                def connect_callback(err):
                    if on_started:
                        on_started(err)
                self._perform_connect(connect_callback, service, True)
            previous_active_client.stop(stop_callback)
        else:
            ACTIVE_CLIENTS.add(self)

            def connect_callback(err):
                if on_started:
                    on_started(err)
            self._perform_connect(connect_callback, service, True)
        LOG.exit('Client.constructor', self._id, None)

    def _perform_connect(self, on_started, service, new_client):
        """
        Performs the connection
        """
        LOG.entry('Client._perform_connect', self._id)
        LOG.parms(NO_CLIENT_ID, 'on_started:', on_started)
        LOG.parms(NO_CLIENT_ID, 'service:', service)
        LOG.parms(NO_CLIENT_ID, 'new_client:', new_client)

        # If there is no active client (i.e. we've been stopped) then add
        # ourselves back to the active list. Otherwise if there is another
        # active client (that's replaced us) then exit function now
        active_client = ACTIVE_CLIENTS.get(self._id)
        if active_client is None:
            LOG.data(
                self._id,
                'Adding client to active list, as there is no currently '
                'active client')
            ACTIVE_CLIENTS.add(self)
        elif self != active_client:
            LOG.data(
                self._id,
                'Not connecting because client has been replaced')
            if on_started:
                err = mqlexc.LocalReplacedError()
                LOG.entry('Client._perform_connect.on_started', self._id)
                on_started(err)
                LOG.exit('Client.perform_connect.on_started', self._id, None)
            LOG.exit('Client._perform_connect', self._id, None)
            return

        if not new_client:
            current_state = self.state
            LOG.data(self._id, 'current_state:', current_state)
            # if we are not disconnected or disconnecting return with the
            # client object
            if current_state not in (STOPPED, RETRYING):
                if current_state == STOPPING:
                    def _still_disconnecting(client, on_started):
                        """
                        Waits while the client is disconnecting
                        """
                        LOG.entry(
                            'Client._still_disconnecting',
                            client.get_id())
                        LOG.parms(NO_CLIENT_ID, 'on_started:', on_started)
                        if client.state == STOPPING:
                            _still_disconnecting(client, on_started)
                        else:
                            client._perform_connect(
                                client,
                                service,
                                on_started)
                        LOG.exit(
                            'Client._still_disconnecting',
                            client.get_id(),
                            None)
                    _still_disconnecting(self, on_started)
                else:
                    if on_started:
                        LOG.entry(
                            'Client._perform_connect.on_started',
                            self._id)
                        on_started(None)
                        LOG.exit(
                            'Client._perform_connect.on_started',
                            self._id,
                            None)
                LOG.exit('Client._perform_connect', self._id, self)
                return self

            if self.state == STOPPED:
                self._set_state(STARTING)

            # If the messenger is not already stopped then something has gone
            # wrong
            if self._messenger and not self._messenger.stopped:
                err = mqlexc.MQLightError('messenger is not stopped')
                LOG.ffdc('Client._perform_connect', 'ffdc001', self._id, err)
                LOG.error('Client._perform_connect', self._id, err)
                raise err
        else:
            self._set_state(STARTING)

        # Obtain the list of services for connect and connect to one of the
        # services, retrying until a connection can be established
        if hasattr(self._service_function, '__call__'):
            def _callback(err, service):
                LOG.entry(
                    'Client._perform_connect._callback',
                    self._id)
                if err:
                    ACTIVE_CLIENTS.remove(self._id)
                    on_started(None)
                else:
                    try:
                        self._service_list = _generate_service_list(
                            service,
                            self._security_options)
                        self._connect_to_service(on_started)
                    except Exception as exc:
                        ACTIVE_CLIENTS.remove(self._id)
                        on_started(exc)
                LOG.exit(
                    'Client._perform_connect._callback',
                    self._id,
                    None)
            self._service_function(_callback)
        else:
            try:
                self._service_list = _generate_service_list(
                    service,
                    self._security_options)
                self._connect_to_service(on_started)
            except Exception as exc:
                ACTIVE_CLIENTS.remove(self._id)
                LOG.error('Client._perform_connect', self._id, exc)
                if on_started:
                    on_started(exc)
        LOG.exit('Client._perform_connect', self._id, None)

    def start(self, on_started=None):
        """Connects to the MQ Light service. This method is asynchronous and
        calls the optional on_started function when the client has successfully
        connected to the MQ Light service, or the ``client.stop()`` method has
        been invoked before a successful connection could be established, or
        the client could not connect to the MQ Light service.
        If this method is invoked while the client is in 'starting',
        'started' or 'retrying' states then the method will complete without
        performing any work or changing the state of the client. If this method
        is invoked while the client is in 'stopping' state then its
        effect will be deferred until the client has transitioned into
        'stopped' state.

        :param on_started: (optional) function to call when the client reaches
            the started state. This function prototype must be ``func(err)``
            where ``err`` is ``None`` if the client started successfully,
            otherwise it is the error message.
        :returns: The Client instance.
        :raises TypeError: if on_started is not a function.
        """
        LOG.entry('Client.start', self._id)

        if on_started and not hasattr(on_started, '__call__'):
            error = TypeError('on_started must be a function')
            LOG.error('Client.start', self._id, error)
            raise error
        LOG.parms(NO_CLIENT_ID, 'on_started:', on_started)

        # Check that the id for this instance is not already in use. If it is
        # then we need to stop the active instance before starting
        previous_client = ACTIVE_CLIENTS.get(self._id)
        LOG.data(self._id, 'previous_client:', previous_client)
        LOG.data(self._id, 'self:', self)
        if previous_client is not None and previous_client != self:
            LOG.debug(
                self._id,
                'stopping previously active client with same client id')
            ACTIVE_CLIENTS.add(self)

            def stop_callback(err):
                LOG.debug(
                    self._id,
                    'stopped previously active client with same client id')
                err = mqlexc.LocalReplacedError()
                if self._on_state_changed:
                    self._on_state_changed(ERROR, err)
                LOG.error(
                    'Client.start.stop_callback',
                    previous_client.get_id(),
                    err)
                if previous_client._on_state_changed:
                    previous_client._on_state_changed(ERROR, err)
                self._perform_connect(on_started, self._service_param, False)
            previous_client.stop(stop_callback)
        else:
            ACTIVE_CLIENTS.add(self)
            self._perform_connect(on_started, self._service_param, False)

        LOG.exit('Client.start', self._id, self)
        return self

    def _process_queued_actions(self, err=None):
        """
        Called on reconnect or first connect to process any actions that may
        have been queued.
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
        LOG.parms(self._id, 'err:', err)
        LOG.data(self._id, 'state:', self.state)
        if err is None:
            LOG.data(
                self._id,
                'client._queued_subscriptions:',
                self._queued_subscriptions)
            while (len(self._queued_subscriptions) > 0 and
                   self.state == STARTED):
                sub = self._queued_subscriptions.pop(0)
                if sub['noop']:
                    # no-op so just trigger the callback without actually
                    # subscribing
                    if sub['callback']:
                        sub['callback'](
                            err,
                            sub['topic_pattern'],
                            sub['original_share_value'])
                else:
                    self.subscribe(
                        sub.topic_pattern,
                        sub.share,
                        sub.options,
                        sub.callback)
            LOG.data(
                self._id,
                'client._queued_unsubscribes:',
                self._queued_unsubscribes)
            while len(self._queued_unsubscribes) > 0 and self.state == STARTED:
                sub = self._queued_unsubscribes.pop(0)
                if sub['noop']:
                    # no-op so just trigger the callback without actually
                    # unsubscribing
                    if sub['callback']:
                        sub['callback'](
                            None,
                            sub['topic_pattern'],
                            sub['share'])
                else:
                    self.unsubscribe(
                        sub.topic_pattern,
                        sub.share,
                        sub.options,
                        sub.callback)
            LOG.data(
                self._id,
                'client._queued_sends:',
                self._queued_sends)
            while len(self._queued_sends) > 0 and self.state == STARTED:
                remaining = len(self._queued_sends)
                msg = self._queued_sends.pop(0)
                self.send(msg.topic, msg.data, msg.options, msg.callback)
                if len(self._queued_sends) >= remaining:
                    # Calling client.send can cause messages to be added back
                    # into _queued_sends, if the network connection is broken.
                    # Check that the size of the array is decreasing to avoid
                    # looping forever...
                    break

        LOG.exit('_process_queued_actions', self._id, None)

    def _check_for_messages(self):
        """
        Function to force the client to check for messages. The on_message
        callback set in subscribe() is called when a message is received.
        """
        LOG.entry_often('Client._check_for_messages', self._id)
        if self.get_state() != STARTED or len(
                self._subscriptions) == 0:
            LOG.exit_often('Client._check_for_messages', self._id, None)
            return
        requeue = True
        try:
            messages = self._messenger.receive(50)
            if messages and len(messages) > 0:
                LOG.debug(
                    self._id,
                    'received {0} messages'.format(len(messages)))
                for message in range(len(messages)):
                    LOG.debug(
                        self._id,
                        'processing message {0}'.format(message))
                    self._process_message(messages[message])
                    if message < (len(messages) - 1):
                        # Unless this is the last pass around the loop, call
                        # work() so that Messenger has a chance to respond to
                        # any heartbeat requests that may have arrived from the
                        # server.
                        self._messenger.work(0)
        except Exception as exc:
            LOG.error('Client._check_for_messages', self._id, exc)

            if not _should_reconnect(exc):
                requeue = False

            def next_tick():
                LOG.error('Client._check_for_messages', self._id, exc)
                if self._on_state_changed:
                    self._on_state_changed(ERROR, exc)
                if _should_reconnect(exc):
                    self._reconnect()
            timer = threading.Timer(0.2, next_tick)
            timer.start()

        if self.get_state() == STARTED and requeue:
            timer = threading.Timer(0.2, self._check_for_messages)
            timer.start()

        LOG.exit_often('Client._check_for_messages', self._id, None)

    def _process_message(self, msg):
        """
        Process received message
        """
        LOG.entry_often('Client._process_message', self._id)
        LOG.parms(self._id, 'msg:', msg)
        msg.connection_id = self._connection_id

        data = msg.body
        topic = urlparse(msg.address).path[1:]
        auto_confirm = True
        qos = QOS_AT_MOST_ONCE

        def filter_func(item):
            # 1 added to length to account for the / we add
            address_no_service = item['address'][len(self._service) + 1:]
            # Possible to have 2 matches work out whether this is
            # for a share or private topic
            link_address = None
            if item['share'] is None and msg.link_address.startswith(
                    'private:'):
                # Slice off 'private:' prefix
                link_address = msg.link_address[8:]
            elif item['share'] and msg.link_address.startswith('share:'):
                # Starting after the share: look for the next : denoting the
                # end of the share name and get everything past that
                link_address = msg.link_address[
                    msg.link_address.index(':', 7) + 1:]
            if address_no_service == link_address:
                return True
            else:
                return False

        matched_subs = [
            sub for sub in self._subscriptions if filter_func(sub)]
        # Should only ever be one entry in matched_subs
        if len(matched_subs) > 1:
            err = mqlexc.MQLightError(
                'received message matched more than one subscription')
            LOG.ffdc(
                'Client._process_message',
                'ffdc002',
                self._id,
                err)
        subscription = matched_subs[0]
        if subscription:
            qos = subscription['qos']
            if qos == QOS_AT_LEAST_ONCE:
                auto_confirm = subscription['auto_confirm']
            subscription['unconfirmed'] += 1
        else:
            # ideally we shouldn't get here, but it can happen in
            # a timing window if we had received a message from a
            # subscription we've subsequently unsubscribed from
            LOG.debug(
                self._id,
                'No subscription matched message: {0} going to address: '
                '{1}'.format(data, msg.address))
            msg = None
            LOG.exit_often('Client._process_message', self._id, None)
            return

        confirmation = {
            'delivery_confirmed': False,
        }

        def _still_settling(subscription, msg):
            LOG.entry_often('Client._process_message._still_settling',
                            self._id)

            settled = self._messenger.settled(msg)
            if settled:
                subscription['unconfirmed'] -= 1
                subscription['confirmed'] += 1
                LOG.data(
                    self._id,
                    '[credit, unconfirmed, confirmed]:',
                    '[{0}, {1}, {2}]'.format(
                        subscription['credit'],
                        subscription['unconfirmed'],
                        subscription['confirmed']))
                # Ask to flow more messages if >= 80% of available
                # credit (e.g. not including unconfirmed messages)
                # has been used or we have just confirmed
                # everything
                available = subscription['credit'] - \
                    subscription['unconfirmed']
                if (available / subscription[
                    'confirmed']) <= 1.25 or (subscription[
                        'unconfirmed'] == 0 and subscription[
                        'confirmed'] > 0):
                    self._messenger.flow(
                        self._service + '/' + msg.link_address,
                        subscription['confirmed'])
                    subscription['confirmed'] = 0
            else:
                timer = threading.Timer(
                    0.1,
                    _still_settling, [subscription, msg])
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
                err = mqlexc.NetworkError('not started')
                LOG.error(
                    'Client._process_message._confirm',
                    self._id,
                    err)
                raise err
            if not confirmation['delivery_confirmed'] and msg:
                # Also throw mqlexc.NetworkError if the client has
                # disconnected at some point since this particular
                # message was received
                if msg.connection_id != self._connection_id:
                    err = mqlexc.NetworkError(
                        'Client has reconnected since this '
                        'message was received')
                    LOG.error(
                        'Client._process_message._confirm',
                        self._id,
                        err)
                    raise err
                confirmation['delivery_confirmed'] = True
                self._messenger.settle(msg)
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
            if 'share:' in link and link.index('share:') == 0:
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
        mal_ccsi = 'x-opt-message-malformed-MQMD-CodedCharSetId'
        mal_form = 'x-opt-message-malformed-MQMD.Format'
        if annots is not None:
            for i in range(len(annots)):
                if annots[i] and annots[i].key:
                    if annots[i].key == mal_cond:
                        malformed['condition'] = annots[i].value
                    elif annots[i].key == mal_desc:
                        malformed['description'] = annots[i].value
                    elif annots[i].key == mal_ccsi:
                        malformed['MQMD']['CodedCharSetId'] = int(
                            annots[i].value)
                    elif annots[i].key == mal_form:
                        malformed['MQMD']['Format'] = annots[i].value

        if malformed['condition']:
            delivery['malformed'] = malformed
            subscription['on_message'](MALFORMED, msg.body, delivery)
        else:
            LOG.data(
                self._id,
                MESSAGE,
                data,
                delivery)
            subscription['on_message'](MESSAGE, data, delivery)

        if self.is_stopped():
            LOG.debug(
                self._id,
                'client is stopped so not accepting or settling message')
            msg = None
        else:
            if qos == QOS_AT_MOST_ONCE:
                self._messenger.accept(msg)
            if qos == QOS_AT_MOST_ONCE or auto_confirm:
                self._messenger.settle(msg)
                subscription['unconfirmed'] -= 1
                subscription['confirmed'] += 1
                LOG.data(
                    self._id,
                    '[credit, unconfirmed, confirmed]:',
                    '[{0}, {1}, {2}]'.format(
                        subscription['credit'],
                        subscription['unconfirmed'],
                        subscription['confirmed']))
                # Ask to flow more messages if >= 80% of available
                # credit (e.g. not including unconfirmed messages)
                # has been used. Or we have just confirmed
                # everything.
                available = subscription['credit'] - \
                    subscription['unconfirmed']
                if available / subscription[
                    'confirmed'] <= 1.25 or (subscription[
                        'unconfirmed'] == 0 and subscription[
                        'confirmed'] > 0):
                    self._messenger.flow(
                        self._service + '/' + msg.link_address,
                        subscription['confirmed'])
                    subscription['confirmed'] = 0
                    msg = None
        LOG.exit_often('Client._process_message', self._id, None)

    def stop(self, on_stopped=None):
        """Disconnects the client from the MQ Light service, implicitly closing
        any subscriptions that the client has open. This method works
        asynchronously, and will invoke the optional on_stopped function once
        the client has disconnected. Calling client.stop() when the client is
        in 'stopping' or 'stopped' state has no effect. Calling client.stop()
        from any other state results in the client disconnecting and the
        'stopped' event being generated.

        :param on_stopped: (optional) function to call when the connection is
            closed. This function prototype must be ``func(err)`` where
            ``err`` is always ``None``.
        :raises TypeError: if on_stopped is not a function
        """
        LOG.entry('Client.stop', self._id)

        if on_stopped and not hasattr(on_stopped, '__call__'):
            raise TypeError('on_stopped must be a function')
        LOG.parms(NO_CLIENT_ID, 'on_stopped:', on_stopped)

        # Cancel retry timer
        if self._retry_timer:
            self._retry_timer.cancel()

        # just return if already stopped or in the process of
        # stopping
        if self.is_stopped():
            if on_stopped:
                LOG.entry('Client.stop.on_stopped', self._id)
                on_stopped(None)
                LOG.exit('Client.stop.on_stopped', self._id, None)
            LOG.exit('Client.stop', self._id, self)
            return self

        self._perform_disconnect(on_stopped)
        LOG.exit('Client.stop', self._id, self)
        return self

    def _perform_disconnect(self, on_stopped):
        """
        Performs the disconnection
        """
        LOG.entry('Client._perform_disconnect', self._id)
        LOG.parms(NO_CLIENT_ID, 'on_stopped:', on_stopped)
        self._set_state(STOPPING)

        # Only disconnect when all outstanding send operations are complete
        if not self._outstanding_sends:
            def stop_processing(client, on_stopped):
                LOG.entry(
                    'Client._perform_disconnect.stop_processing',
                    self._id)
                if client._heartbeat_timeout:
                    client._heartbeat_timeout.cancel()

                # Clear all queued sends as we are disconnecting
                while len(self._queued_sends) > 0:
                    msg = self._queued_sends.pop(0)

                    def next_tick():
                        """
                        next tick
                        """
                        LOG.entry(
                            'Client._perform_disconnect.next_tick',
                            self._id)
                        msg['on_sent'](
                            mqlexc.StoppedError(
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

                # Clear the active subscriptions list as we were asked to
                # disconnect
                LOG.data(self._id, 'self._subscriptions:', self._subscriptions)
                self._subscriptions = []

                # Indicate that we've disconnected
                client._set_state(STOPPED)

                # Remove ourself from the active client list
                active_client = ACTIVE_CLIENTS.get(self._id)
                if self == active_client:
                    ACTIVE_CLIENTS.remove(self._id)
                LOG.state(
                    'Client._perform_disconnect.stop_processing',
                    self._id,
                    STOPPED)
                if not self._first_start:
                    self._first_start = True
                    if self._on_state_changed:
                        self._on_state_changed(STOPPED, None)

                if on_stopped:
                    LOG.entry(
                        'Client._perform_disconnect.on_stopped',
                        self._id)
                    on_stopped(None)
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
        timer.daemon = True
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
            stopped = self._messenger.stop()

        # If stopped then perform the required stop processing
        if stopped:
            if self._heartbeat_timeout:
                self._heartbeat_timeout.cancel()
            stop_processing_callback(self, callback)
        else:
            # Otherwise check for the messenger being stopped again
            timer = threading.Timer(
                1,
                self._stop_messenger, [stop_processing_callback, callback])
            timer.daemon = True
            timer.start()
        LOG.exit('Client._stop_messenger', self._id, None)

    def _connect_to_service(self, callback):
        """
        Function to connect to the service, tries each available service in
        turn. If none can connect it emits an error, waits and attempts to
        connect again. Callback happens once a successful connect/reconnect
        occurs
        """
        LOG.entry('Client._connect_to_service', self._id)
        LOG.parms(NO_CLIENT_ID, 'callback:', callback)
        if self.is_stopped():
            if callback:
                LOG.entry('Client._connect_to_service.callback', self._id)
                callback(
                    mqlexc.StoppedError('connect aborted due to disconnect'))
                LOG.exit('Client._connect_to_service.callback', self._id, None)
            LOG.exit('Client._connect_to_service', self._id, None)
            return
        error = None
        connected = False

        # Try each service in turn until we can successfully connect, or
        # exhaust the list
        for i in range(len(self._service_list)):
            service = self._service_list[i]
            try:
                # check if we will be providing authentication information
                auth = None
                if self._security_options.url_user is not None:
                    auth = quote(str(self._security_options.url_user))
                    auth += ':'
                    auth += quote(str(self._security_options.url_password))
                    auth += '@'
                elif self._security_options.property_user is not None:
                    auth = quote(str(self._security_options.property_user))
                    auth += ':'
                    auth += quote(
                        str(self._security_options.property_password))
                    auth += '@'
                log_url = None
                # reparse the service url to prepend authentication information
                # back on as required
                if auth:
                    service_url = urlparse(service)
                    service = service_url.scheme + \
                        '://' + auth + service_url.hostname
                    if service_url.port:
                        service += ':' + str(service_url.port)
                    log_url = service_url.scheme + '://' + \
                        re.sub(r':[^:]+@', ':********@', auth) + \
                        service_url.hostname + ':' + str(service_url.port)
                else:
                    log_url = service
                LOG.data(self._id, 'attempting to connect to: ', log_url)

                connect_url = urlparse(service)
                # Remove any path elements from the URL
                if connect_url.path:
                    href_length = len(service) - len(connect_url.path)
                    connect_service = service[0:href_length]
                else:
                    connect_service = service

                s_o = self._security_options
                if s_o.ssl_trust_certificate is not None:
                    ssl_trust_certificate = s_o.ssl_trust_certificate
                else:
                    ssl_trust_certificate = None
                if s_o.ssl_verify_name is not None:
                    ssl_verify_name = s_o.ssl_verify_name
                else:
                    ssl_verify_name = None

                try:
                    self._messenger.connect(
                        urlparse(connect_service),
                        ssl_trust_certificate,
                        ssl_verify_name)
                    LOG.data(self._id, 'successfully connected to:', log_url)
                    self._service = self._service_list[i]
                    connected = True
                    break
                except Exception as exc:
                    error = exc
                    LOG.data(
                        self._id,
                        'failed to connect to: {0} due to error: {1}'.format(
                            log_url, error))
            except Exception as exc:
                # Should never get here, as it means that messenger.connect has
                # been called in an invalid way, so FFDC
                error = exc
                LOG.ffdc(
                    'Client._connect_to_service',
                    'ffdc003',
                    self._id,
                    traceback.format_exc())
                raise mqlexc.MQLightError(exc)

        # If we've successfully connected then we're done, otherwise we'll
        # retry
        if connected:
            # Indicate that we're connected
            self._set_state(STARTED)
            event_to_emit = None
            if self._first_start:
                event_to_emit = STARTED
                self._first_start = False
                self._retry_count = 0
                # could be queued actions so need to process those here.
                # On reconnect this would be done via the callback we set,
                # first connect its the users callback so won't process
                # anything
                LOG.data(self._id, 'first start since being stopped')
                self._process_queued_actions()
            else:
                self._retry_count = 0
                event_to_emit = RESTARTED
            self._connection_id += 1

            def next_tick():
                LOG.state(
                    'Client._connect_to_service.next_tick',
                    self._id,
                    event_to_emit)
                if self._on_state_changed:
                    self._on_state_changed(event_to_emit, None)
                if callback:
                    LOG.entry('Client._connect_to_service.callback2', self._id)
                    callback(None)
                    LOG.exit(
                        'Client._connect_to_service.callback2',
                        self._id,
                        None)
            timer = threading.Timer(0.2, next_tick)
            timer.start()

            # Setup heartbeat timer to ensure that while connected we send
            # heartbeat frames to keep the connection alive, when required
            remote_idle_timeout = self._messenger.get_remote_idle_timeout(
                self._service)
            if remote_idle_timeout > 0:
                heartbeat_interval = remote_idle_timeout / 2
            else:
                heartbeat_interval = remote_idle_timeout
            LOG.data(self._id, 'heartbeat_interval: ', heartbeat_interval)
            if heartbeat_interval > 0:
                def perform_heartbeat(heartbeat_interval):
                    """
                    Performs an action on the client to keep the connection
                    alive
                    """
                    LOG.entry(
                        'Client._connect_to_service.perform_heartbeat',
                        self._id)
                    if self._messenger:
                        self._messenger.work(0)
                        self._heartbeat_timeout = threading.Timer(
                            heartbeat_interval,
                            perform_heartbeat,
                            [heartbeat_interval])
                        self._heartbeat_timeout.daemon = True
                        self._heartbeat_timeout.start()
                    LOG.exit(
                        'Client._connect_to_service.perform_heartbeat',
                        self._id,
                        None)
                self._heartbeat_timeout = threading.Timer(
                    heartbeat_interval,
                    perform_heartbeat,
                    [heartbeat_interval])
                self._heartbeat_timeout.daemon = True
                self._heartbeat_timeout.start()

        else:
            # We've tried all services without success. Pause for a while
            # before trying again
            self._set_state(RETRYING)

            def retry():
                LOG.entry_often('Client._connect_to_service.retry', self._id)
                if not self.is_stopped():
                    self._perform_connect(callback, self._service_list, False)
                LOG.exit_often(
                    'Client._connect_to_service.retry',
                    self._id,
                    None)

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
            interval = round(interval) * CONNECT_RETRY_INTERVAL
            LOG.data(
                self._id,
                'trying to connect again after {0} seconds'.format(interval))
            self._retry_timer = threading.Timer(interval, retry)
            self._retry_timer.start()

            if error:
                def next_tick():
                    LOG.error(
                        'Client._connect_to_service',
                        self._id,
                        error)
                    if self._on_state_changed:
                        self._on_state_changed(ERROR, error)
                timer = threading.Timer(1, next_tick)
                timer.start()
        LOG.exit('Client._connect_to_service', self._id, None)

    def _reconnect(self):
        """
        Reconnects the client to the MQ Light service, implicitly closing any
        subscriptions that the client has open. The 'reconnected' event will be
        emitted once the client has reconnected.

        Returns:
            The instance of the client if reconnect succeeded otherwise None
        """
        LOG.entry('Client._reconnect', self._id)
        if self.state != STARTED:
            if self.is_stopped():
                return
            elif self.state == RETRYING:
                return self
        self._set_state(RETRYING)

        # Stop the messenger to free the object then attempt a reconnect
        def stop_processing(client, callback=None):
            LOG.entry('Client.reconnect.stop_processing', client.get_id())

            if client._heartbeat_timeout:
                client._heartbeat_timeout.cancel()

            # clear the subscriptions list, if the cause of the reconnect
            # happens during check for messages we need a 0 length so it will
            # check once reconnected.
            client._queued_subscriptions = client._queued_subscriptions
            # also clear any left over outstanding sends
            client._outstanding_sends = []
            client._perform_connect(
                self._process_queued_actions,
                self._service,
                False)

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
            LOG.data(self._id, 'service:', self._service)
            return self._service
        else:
            LOG.data(self._id, 'Not connected')
            LOG.data(self._id, 'service: None')
            return None

    def get_state(self):
        """
        :returns: The state of the client
        """
        LOG.data(self._id, 'state:', self._state)
        return self._state

    def _set_state(self, state):
        """
        Sets the state of the client
        """
        LOG.data(self._id, 'state:', state)
        if state in STATES:
            self._state = state
        else:
            raise mqlexc.InvalidArgumentError('invalid state')

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

        :param topic: Topic of the message.
        :param data: Body of the message.
        :param options: (optional) Message attributes.
        :param on_sent: (optional) A function to call when the message is sent
            This function prototype must be ``func(err, topic, data, options)``
            where ``err`` is ``None`` if the message was sent correctly,
            otherwise it is the error message, ``topic`` is the topic of the
            message, ``data`` is the body of the message, ``options`` are the
            message attributes.
        :returns: ``True`` if this message was either sent or is the next to be
            sent or ``False`` if the message was queued in user memory, because
            either there was a backlog of messages, or the client was not in a
            started state.
        :raises TypeError: if the type of any of the arguments is incorrect.
        :raises mqlexc.RangeError: if the value of any argument is not within
            certain values.
        :raises mqlexc.StoppedError: if the client is stopped.
        :raises mqlexc.InvalidArgumentError: if any of the arguments are
            invalid.
        """
        LOG.entry('Client.send', self._id)
        next_message = False
        # Validate the passed parameters
        if topic is None:
            raise TypeError('Cannot send to None topic')
        else:
            topic = str(topic)
            if not topic:
                raise TypeError('Cannot send to None topic')
        LOG.parms(self._id, 'topic:', topic)

        if data is None:
            raise TypeError('Cannot send no data')
        elif hasattr(data, '__call__'):
            raise TypeError('Cannot send a function')
        LOG.parms(self._id, 'type(data)', str(type(data)))
        LOG.parms(self._id, 'data:', data)

        # Validate the options parameter, when specified
        if options is not None:
            if isinstance(options, dict):
                LOG.parms(self._id, 'options:', options)
            else:
                raise TypeError('options must be a dict type')

        qos = QOS_AT_MOST_ONCE
        ttl = None
        if options:
            if 'qos' in options:
                if options['qos'] in QOS:
                    qos = options['qos']
                else:
                    raise mqlexc.RangeError(
                        'options[\'qos\'] value {0} is invalid must evaluate '
                        'to 0 or 1'.format(options['qos']))
            if 'ttl' in options:
                try:
                    ttl = int(options['ttl'])
                    if ttl <= 0:
                        raise TypeError()
                    if ttl > 4294967295:
                        # Cap at max AMQP value for TTL (2^32-1)
                        ttl = 4294967295
                except Exception as err:
                    raise mqlexc.RangeError(
                        'options[\'ttl\'] value {0} is invalid must be an '
                        'unsigned integer number'.format(options['ttl']))

        if on_sent:
            if not hasattr(on_sent, '__call__'):
                raise TypeError('on_sent must be a function')
        elif qos == QOS_AT_LEAST_ONCE:
            raise mqlexc.InvalidArgumentError(
                'on_sent must be specified when options[\'qos\'] value of 1 '
                '(at least once) is specified')
        LOG.parms(self._id, 'on_sent:', on_sent)

        # Ensure we have attempted a connect
        if self.is_stopped():
            raise mqlexc.StoppedError('not started')

        # Ensure we are not retrying otherwise queue message and return
        if self.state in (RETRYING, STARTING):
            self._queued_sends.append({
                'topic': topic,
                'data': data,
                'options': options,
                'on_sent': on_sent
            })
            self._on_drain_required = True
            LOG.exit('Client.send', self._id, False)
            return False

        # Send the data as a message to the specified topic
        msg = None
        in_outstanding_sends = False
        try:
            msg = mqlightproton._MQLightMessage()
            address = self.get_service()
            if topic:
                # need to encode the topic component but / has meaning that
                # shouldn't be encoded
                topic_levels = topic.split('/')
                encoded_topic_levels = [quote(x) for x in topic_levels]
                encoded_topic = '/'.join(encoded_topic_levels)
                address += '/' + encoded_topic
                msg.address = address
            if ttl:
                msg.ttl = ttl

            if isinstance(data, str):
                msg.body = unicode(data)
                msg.content_type = 'text/plain'
            else:
                msg.body = data
                msg.content_type = 'application/octet-stream'

            # Record that a send operation is in progress
            self._outstanding_sends.append({
                'msg': msg,
                'qos': qos,
                'on_sent': on_sent,
                'topic': topic,
                'options': options
            })
            in_outstanding_sends = True

            self._messenger.put(msg, qos)
            self._messenger.send()

            if len(self._outstanding_sends) == 1:
                def send_outbound_msg():
                    LOG.entry_often('Client.send.send_outbound_msg', self._id)
                    LOG.data(
                        self._id,
                        '_outstanding_sends:',
                        self._outstanding_sends)
                    try:
                        if not self._messenger.stopped:
                            # Write any data buffered within messenger
                            tries = 50
                            p_o = self._messenger.pending_outbound(
                                self.get_service())
                            while tries > 0 and p_o:
                                tries -= 1
                                self._messenger.send()
                                p_o = self._messenger.pending_outbound(
                                    self.get_service())

                            if tries == 0:
                                LOG.debug(self._id, 'output still pending')

                            # See if any of the outstanding send operations
                            # have now been completed
                            LOG.data(
                                self._id,
                                'length:',
                                len(self._outstanding_sends))
                            while len(self._outstanding_sends) > 0:
                                in_flight = self._outstanding_sends[0:1][0]
                                status = str(self._messenger.status(
                                    in_flight['msg']))
                                LOG.data(self._id, 'status:', status)
                                complete = False
                                err = None
                                if in_flight['qos'] == QOS_AT_MOST_ONCE:
                                    complete = (status == 'UNKNOWN' or
                                                status == 'SETTLED')
                                else:
                                    if status in ('ACCEPTED', 'SETTLED'):
                                        self._messenger.settle(
                                            in_flight['msg'])
                                        complete = True
                                    elif status == 'REJECTED':
                                        complete = True
                                        err_msg = self._messenger.status_error(
                                            in_flight['msg'])
                                        if err_msg is None or err_msg == '':
                                            err_msg = 'send failed - ' \
                                                'message was rejected'
                                        err = mqlexc.MQLightError(err_msg)
                                    elif status == 'RELEASED':
                                        complete = True
                                        err = mqlexc.MQLightError(
                                            'send failed - message was '
                                            'released')
                                    elif status == 'MODIFIED':
                                        complete = True
                                        err = mqlexc.MQLightError(
                                            'send failed - message was '
                                            'modified')
                                    elif status == 'ABORTED':
                                        complete = True
                                        err = mqlexc.MQLightError(
                                            'send failed - message was '
                                            'aborted')
                                    elif status == 'PENDING':
                                        self._messenger.send()
                                LOG.data(self._id, 'complete:', complete)
                                if complete:
                                    # Remove send operation from list of
                                    # outstanding send ops
                                    self._outstanding_sends.pop(0)

                                    # Generate drain event
                                    if self._on_drain_required and len(
                                            self._outstanding_sends) <= 1:
                                        LOG.state(
                                            'Client.send.send_outbound_msg',
                                            self._id,
                                            DRAIN)
                                        self._on_drain_required = False
                                        callback_thread = threading.Thread(
                                            target=self._on_state_changed,
                                            args=(DRAIN, None))
                                        callback_thread.start()

                                    # invoke on_sent, if specified
                                    if in_flight['on_sent']:
                                        LOG.entry(
                                            'Client.send.send_outbound_msg.'
                                            'cb1',
                                            self._id)
                                        in_flight['on_sent'](
                                            err,
                                            in_flight['topic'],
                                            in_flight['msg'].body,
                                            in_flight['options'])
                                        LOG.exit(
                                            'Client.send.send_outbound_msg.'
                                            'cb1',
                                            self._id,
                                            None)
                                else:
                                    # Can't make any more progress for now -
                                    # schedule remaining work for processing in
                                    # the future
                                    timer = threading.Timer(
                                        0.1,
                                        send_outbound_msg)
                                    timer.start()
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
                        while len(self._outstanding_sends) > 0:
                            in_flight = self._outstanding_sends.pop(0)
                            if in_flight['qos'] == QOS_AT_LEAST_ONCE:
                                # Retry AT_LEAST_ONCE messages
                                self._queued_sends.append({
                                    'topic': in_flight['topic'],
                                    'data': in_flight['msg'].body,
                                    'options': in_flight['options'],
                                    'on_sent': in_flight['on_sent']
                                })
                            else:
                                # we don't know if an at-most-once message made
                                # it across. Call the callback with an err of
                                # null to indicate success otherwise the
                                # application could decide to resend
                                # (duplicate) the message
                                if in_flight['on_sent']:
                                    LOG.entry(
                                        'Client.send.send_outbound_msg.cb2',
                                        self._id)
                                    try:
                                        in_flight['on_sent'](
                                            None,
                                            in_flight['topic'],
                                            in_flight['msg'].body,
                                            options)
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
                            if self._on_state_changed:
                                self._on_state_changed(ERROR, error)

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
                len(self._outstanding_sends))
            if len(self._outstanding_sends) <= 1:
                next_message = True
            else:
                self._on_drain_required = True

        except Exception as exc:
            err = mqlexc.MQLightError(exc)
            LOG.error('Client.send', self._id, err)

            # Error condition so won't retry send need to remove it from list
            # of unsent
            if in_outstanding_sends:
                self._outstanding_sends.pop(0)

            if qos == QOS_AT_LEAST_ONCE:
                self._queued_sends.append({
                    'topic': topic,
                    'data': data,
                    'options': options,
                    'on_sent': on_sent
                })

            # Reconnect can result in many callbacks being fired in a single
            # tick, group these together into a single chunk to avoid them
            # being spread out over a, potentially, long period of time.
            if not self._queued_send_callbacks:
                def immediate():
                    do_reconnect = False
                    while len(self._queued_send_callbacks) > 0:
                        invocation = self._queued_send_callbacks.pop(0)
                        if invocation['on_sent']:
                            if invocation['qos'] == QOS_AT_MOST_ONCE:
                                LOG.entry('Client.send.on_sent', NO_CLIENT_ID)
                                invocation['on_sent'](
                                    invocation['error'],
                                    invocation['topic'],
                                    invocation['body'],
                                    invocation['options'])
                                LOG.exit(
                                    'Client.send.on_sent',
                                    NO_CLIENT_ID,
                                    None)
                        LOG.error(
                            'Client.send',
                            self._id,
                            invocation['error'])
                        if self._on_state_changed:
                            self._on_state_changed(ERROR, invocation['error'])
                        do_reconnect |= _should_reconnect(invocation['error'])
                    if do_reconnect:
                        self._reconnect()
                timer = threading.Thread(target=immediate)
                timer.daemon = True
                timer.start()

            self._queued_send_callbacks.append({
                'body': msg.body,
                'on_sent': on_sent,
                'error': err,
                'options': options,
                'qos': qos,
                'topic': topic
            })

        LOG.exit('Client.send', self._id, next_message)
        return next_message

    def subscribe(
            self,
            topic_pattern,
            share=None,
            options=None,
            on_subscribed=None,
            on_message=None):
        """Constructs a subscription object and starts the emission of message
        events each time a message arrives, at the MQ Light service, that
        matches topic pattern.

        :param topic_pattern: The topic to subscribe to.
        :param share: The share name of the subscription.
        :param options: Subscription attributes.
        :param on_subscribed: A function to call when the subscription is done.
            This function prototype must be ``func(err, pattern, share)`` where
            ``err`` is ``None`` if the client subscribed successfully otherwise
            the error message, ``pattern`` is the subscription pattern and
            ``share`` is the share name.
        :param on_message: function to call when a message is received.
            his function prototype must be ``func(message_type, message)``
            where ``message_type`` is 'message' if a message has been received
            otherwise 'malformed' if a malformed message has been received and
            ``message`` is the message.
        :return: The client instance.
        :raises TypeError: if the type of any of the arguments is incorrect.
        :raises mqlexc.RangeError: if the value of any argument is not within
            certain values.
        :raise mqlexc.StoppedError: if the client is stopped
        :raises mqlexc.InvalidArgumentError: if any of the arguments are
            invalid.
        """
        LOG.entry('Client.subscribe', self._id)
        if topic_pattern is None or topic_pattern == '':
            raise TypeError(
                'Cannot subscribe to an empty pattern')

        topic_pattern = str(topic_pattern)
        LOG.parms(self._id, 'topic_pattern:', topic_pattern)
        original_share_value = share
        if share:
            share = str(share)
            if ':' in share:
                raise mqlexc.InvalidArgumentError(
                    'share argument value {0} is invalid because it contains '
                    'a colon character'.format(share))
            share = 'share:{0}:'.format(share)
        else:
            share = 'private:'
        LOG.parms(self._id, 'share:', share)

        # Validate the options parameter, when specified
        if options:
            if isinstance(options, dict):
                LOG.parms(self._id, 'options:', options)
            else:
                raise TypeError('options must be a dict')

        qos = QOS_AT_MOST_ONCE
        auto_confirm = True
        ttl = 0
        credit = 1024
        if options:
            if 'qos' in options:
                if options['qos'] in QOS:
                    qos = options['qos']
                else:
                    raise mqlexc.RangeError(
                        'options[\'qos\'] value {0} is invalid must evaluate '
                        'to 0 or 1'.format(options['qos']))
            if 'auto_confirm' in options:
                if options['auto_confirm'] in (True, False):
                    auto_confirm = options['auto_confirm']
                else:
                    raise TypeError(
                        'options[\'auto_confirm\'] value {0} is invalid must '
                        'evaluate to True or False'.format(
                            options['auto_confirm']))
            if 'ttl' in options:
                try:
                    ttl = int(options['ttl'])
                    if ttl < 0:
                        raise TypeError()
                except Exception as err:
                    raise mqlexc.RangeError(
                        'options[\'ttl\'] value {0} is invalid must be an '
                        'unsigned integer number'.format(options['ttl']))
            if 'credit' in options:
                try:
                    credit = int(options['credit'])
                    if credit < 0:
                        raise TypeError()
                except Exception as err:
                    raise mqlexc.RangeError(
                        'options[\'credit\'] value {0} is invalid must be an '
                        'unsigned integer number'.format(options['credit']))

        if on_subscribed and not hasattr(on_subscribed, '__call__'):
            raise TypeError('on_subscribed must be a function')
        LOG.parms(self._id, 'on_subscribed:', on_subscribed)

        if on_message and not hasattr(on_message, '__call__'):
            raise TypeError('on_message must be a function')
        LOG.parms(self._id, 'on_message:', on_message)

        # Ensure we have attempted a connect
        if self.is_stopped():
            raise mqlexc.StoppedError('not started')

        # Subscribe using the specified pattern and share options
        address = self.get_service() + '/' + share + topic_pattern
        subscription_address = self.get_service() + '/' + topic_pattern

        # If client is in the retrying state, then queue this subscribe request
        if self.state in (RETRYING, STARTING):
            LOG.data(
                self._id,
                'Client waiting for connections so queued subscription')
            # first check if its already there and if so remove old and add new
            for sub in self._queued_subscriptions:
                if sub['address'] == subscription_address and sub[
                        'share'] == original_share_value:
                    self._queued_subscriptions.remove(sub)

            self._queued_subscriptions.append({
                'noop': False,  # FIXME: implement noop behaviour for subscribe
                'address': subscription_address,
                'qos': qos,
                'auto_confirm': auto_confirm,
                'topic_pattern': topic_pattern,
                'share': original_share_value,
                'options': options,
                'on_subscribed': on_subscribed,
                'on_message': on_message
            })
            LOG.exit('Client.subscribe', self._id, self)
            return self

        err = None
        # if we already believe this subscription exists, we should reject the
        # request to subscribe by throwing a mqlexc.SubscribedError
        for sub in self._subscriptions:
            if sub['address'] == subscription_address and sub[
                    'share'] == original_share_value:
                err = mqlexc.SubscribedError(
                    'client is already subscribed to this address')
                LOG.error('Client.subscribe', self._id, err)
                raise err
        if err is None:
            try:
                self._messenger.subscribe(address, qos, ttl, credit)
            except Exception as exc:
                LOG.error('Client.subscribe', self._id, exc)
                err = mqlexc.MQLightError(exc)

        if on_subscribed:
            def on_subscribed_callback():
                on_subscribed(err, topic_pattern, original_share_value)
            callback_thread = threading.Thread(target=on_subscribed_callback)
            callback_thread.daemon = True
            callback_thread.start()

        if err:
            LOG.error('Client.subscribe', self._id, err)
            if self._on_state_changed:
                self._on_state_changed(ERROR, err)

            if _should_reconnect(err):
                LOG.data(self._id, 'queued subscription and calling reconnect')
                self._queued_subscriptions.append({
                    'noop': False,  # FIXME: implement noop behaviour
                    'address': subscription_address,
                    'qos': qos,
                    'auto_confirm': auto_confirm,
                    'topic_pattern': topic_pattern,
                    'share': original_share_value,
                    'options': options,
                    'on_subscribed': on_subscribed,
                    'on_message': on_message
                })
                self._reconnect()
        else:
            # if no errors, add this to the stored list of subscriptions
            is_first_sub = (len(self._subscriptions) == 0)
            LOG.data(self._id, 'is_first_sub:', is_first_sub)

            self._subscriptions.append({
                'address': subscription_address,
                'qos': qos,
                'auto_confirm': auto_confirm,
                'topic_pattern': topic_pattern,
                'share': original_share_value,
                'options': options,
                'on_subscribed': on_subscribed,
                'on_message': on_message,
                'credit': credit,
                'unconfirmed': 0,
                'confirmed': 0
            })

            # If this is the first subscription to be added, schedule a request
            # to start the polling loop to check for messages arriving
            if is_first_sub:
                timer2 = threading.Timer(0, self._check_for_messages)
                timer2.start()

        LOG.exit('Client.subscribe', self._id, self)
        return self

    def unsubscribe(
            self,
            topic_pattern,
            share=None,
            options=None,
            on_unsubscribed=None):
        """Stops the flow of messages from a destination to this client. The
        client's on_message callback will no longer be driven when messages
        arrive, that match the pattern associate with the destination. The
        pattern and optional share arguments must match those specified when
        the destination was created by calling the original
        client.subscribe(...) method.

        :param topic_pattern: the topic_pattern that was supplied in the
            previous call to subscribe.
        :param share: (optional) the share that was supplied in the previous
            call to subscribe.
        :param options: (optional) Unsubscription attributes.
        :param on_unsubscribed: (optional) Invoked if the unsubscribe request
            has been processed successfully.
            This function prototype must be ``func(err, pattern, share)``
            where ``err`` is ``None`` if the client unsubscribed successfully
            otherwise the error message, ``pattern`` is the unsubscription
            pattern and ``share`` is the share name.
        :returns: The instance of the client.
        :raises TypeError: if the type of any of the arguments is incorrect.
        :raises mqlexc.RangeError: if the value of any argument is not within
            certain values.
        :raises mqlexc.StoppedError: if the client is stopped.
        :raises mqlexc.InvalidArgumentError: if any of the arguments are
            invalid.
        """
        LOG.entry('Client.unsubscribe', self._id)
        LOG.parms(self._id, 'topic_pattern:', topic_pattern)

        if topic_pattern is None:
            err = TypeError('You must specify a topic_pattern argument')
            LOG.error('Client.unsubscribe', self._id, err)
            raise err

        topic_pattern = str(topic_pattern)

        original_share_value = share
        if share:
            share = str(share)
            if ':' in share:
                error = mqlexc.InvalidArgumentError(
                    'share argument value {0} is invalid because it contains '
                    'a colon (:) character'.format(share))
                LOG.error('Client.unsubscribe', self._id, error)
                raise error
            share = 'share:' + share + ':'
        else:
            share = 'private:'

        LOG.parms(self._id, 'share:', share)

        # Validate the options parameter, when specified
        if options:
            if isinstance(options, dict):
                LOG.parms(self._id, 'options:', options)
            else:
                error = TypeError(
                    'options must be a dict type not a {0}'.format(
                        type(options)))
                LOG.error('Client.unsubscribe', self._id, error)
                raise error

        ttl = None
        if options:
            if 'ttl' in options:
                try:
                    ttl = int(options['ttl'])
                    if ttl != 0:
                        raise ValueError()
                except Exception as err:
                    raise mqlexc.RangeError(
                        'options[\'ttl\'] value {0} is invalid, only 0 is a '
                        'supported value for  an unsubscribe request'.format(
                            options['ttl']))

        if on_unsubscribed and not hasattr(on_unsubscribed, '__call__'):
            err = TypeError('on_unsubscribed must be a function')
            LOG.error('Client.unsubscribe', self._id, err)
            raise err

        # Ensure we have attempted a connect
        if self.is_stopped():
            err = mqlexc.StoppedError('not started')
            LOG.error('Client.unsubscribe', self._id, err)
            raise err

        address = self._service + '/' + share + topic_pattern
        subscription_address = self._service + '/' + topic_pattern

        # Check that there is actually a subscription for the pattern and share
        subscribed = False
        for sub in self._subscriptions:
            if sub['address'] == subscription_address and sub[
                    'share'] == original_share_value:
                subscribed = True
                break
        if not subscribed:
            for sub in self._queued_subscriptions:
                if (sub['address'] == subscription_address and
                        sub['share'] == original_share_value and
                        not sub['noop']):
                    subscribed = True
                    break

        if not subscribed:
            err = mqlexc.UnsubscribedError(
                'client is not subscribed to this address:' + address)
            LOG.error('Client.unsubscribe', self._id, err)
            raise err

        def queue_unsubscribe():
            """Add the unsubscribe request to the internal queue"""
            # check if there's a queued subscribe for the same topic, if so
            # mark that as a no-op operation, so the callback is called but a
            # no-op takes place on reconnection
            noop = False
            for sub in self._queued_subscriptions:
                if sub['address'] == subscription_address and sub[
                        'share'] == original_share_value:
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

            self._queued_unsubscribes.append({
                'noop': noop,
                'address': subscription_address,
                'topic_pattern': topic_pattern,
                'share': original_share_value,
                'options': options,
                'on_unsubscribed': on_unsubscribed
            })

        # if client is in the retrying state, then queue this unsubscribe
        # request
        if self.state in (RETRYING, STARTING):
            LOG.data(
                self._id,
                'client still in the process of connecting '
                'so queueing the unsubscribe request')
            queue_unsubscribe()
            LOG.exit('Client.unsubscribe', self._id, self)
            return self

        # unsubscribe using the specified topic pattern and share options
        error = None
        try:
            self._messenger.unsubscribe(address, ttl)

            if on_unsubscribed:
                def on_unsubscribed_callback():
                    LOG.entry(
                        'Client.unsubscribe.on_unsubscribed_callback',
                        self._id)
                    on_unsubscribed(None, topic_pattern, original_share_value)
                    LOG.exit(
                        'Client.unsubscribe.on_unsubscribed_callback',
                        self._id,
                        None)
                callback_thread = threading.Thread(
                    target=on_unsubscribed_callback)
                callback_thread.daemon = True
                callback_thread.start()

            # if no errors, remove this from the stored list of subscriptions
            for sub in self._subscriptions:
                if sub['address'] == subscription_address and sub[
                        'share'] == original_share_value:
                    self._subscriptions.remove(sub)
                    break

        except Exception as exc:
            LOG.error('Client.unsubscribe', self._id, exc)
            if self._on_state_changed:
                self._on_state_changed(ERROR, exc)
            if _should_reconnect(exc):
                queue_unsubscribe()
                self._reconnect()
        LOG.exit('Client.unsubscribe', self._id, self)
        return self
