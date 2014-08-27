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
import uuid
import threading
import os.path
import re
import sys
import codecs
import httplib
import socket
from mqlightlog import get_logger, NO_CLIENT_ID
from json import loads
from random import random
from urlparse import urlparse, unquote
from urllib import quote
import mqlightexceptions as mqlex

CMD = sys.argv[0].split(' ')
if 'unittest' in CMD:
    import stubproton as mqlightproton
else:
    import mqlightproton

# Set up logging (to stderr by default). The level of output is
# configured by the value of the MQLIGHT_NODE_LOG environment
# variable. The default is 'ffdc'.
LOG = get_logger(__name__)

# Allowed chars for the client id
VALID_CLIENT_ID_CHARS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvw' + \
    'xyz0123456789%/._'

# The connection retry interval in seconds
CONNECT_RETRY_INTERVAL = 10

STARTED = 'STARTED'
STARTING = 'STARTING'
STOPPED = 'STOPPED'
STOPPING = 'STOPPING'
RESTARTED = 'RESTARTED'
RETRYING = 'RETRYING'
ERROR = 'ERROR'
MESSAGE = 'MESSAGE'
MALFORMED = 'MALFORMED'
DRAIN = 'DRAIN'

QOS_AT_MOST_ONCE = 0
QOS_AT_LEAST_ONCE = 1

STATES = {
    'STARTED': STARTED,
    'STARTING': STARTING,
    'STOPPED': STOPPED,
    'STOPPING': STOPPING,
    'RETRYING': RETRYING
}

QOS = {
    'QOS_AT_MOST_ONCE': QOS_AT_MOST_ONCE,
    'QOS_AT_LEAST_ONCE': QOS_AT_LEAST_ONCE
}

EVENTS = {
    'STARTED': STARTED,
    'STOPPED': STOPPED,
    'RESTARTED': RESTARTED,
    'ERROR': ERROR,
    'MESSAGE': MESSAGE,
    'MALFORMED': MALFORMED,
    'DRAIN': DRAIN
}


def create_client(options, callback=None):
    LOG.entry('create_client', NO_CLIENT_ID)
    err = None
    if not isinstance(options, dict):
        err = TypeError('options argument must be a dict')
        LOG.error('create_client', NO_CLIENT_ID, err)
        raise err
    if callback and not hasattr(callback, '__call__'):
        err = TypeError('callback argument must be a function')
        LOG.error('create_client', NO_CLIENT_ID, err)
        raise err
    if 'service' not in options:
        err = mqlex.InvalidArgumentError('service is required')
        LOG.error('create_client', NO_CLIENT_ID, err)
        raise err
    if 'id' in options:
        client_id = options['id']
    else:
        client_id = None
    if 'security_options' in options:
        security_options = options['security_options']
    else:
        security_options = None

    client = Client(
        options['service'],
        client_id,
        security_options,
        callback)
    return client


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
            self.ssl_verify_name = None

    def __str__(self):
        return '[' + \
            '\'property_user\': ' + str(self.property_user) + \
            '\'property_pass\': ' + \
            ('********' if str(self.property_password) else 'None') + \
            '\'url_user\': ' + str(self.url_user) + \
            '\'url_pass\': ' + \
            ('********' if str(self.url_password) else 'None') + \
            '\'ssl_trust_certificate\': ' + str(self.ssl_trust_certificate) + \
            '\'ssl_verify_name\': ' + str(self.ssl_verify_name) + \
            ']'

    def __repr__(self):
        return str(self)


def _should_reconnect(error):
    """
    Generic helper method to determine if we should automatically reconnect
    for the given type of error.
    """
    err_type = type(error)
    return err_type not in (
        TypeError,
        mqlex.InvalidArgumentError,
        mqlex.ReplacedError,
        mqlex.StoppedError,
        mqlex.SubscribedError,
        mqlex.UnsubscribedError)


def _get_http_service_function(service_url):
    """
    Function to take a single HTTP URL and using the JSON retrieved from it to
    return an array of service URLs.
    """
    LOG.entry('_get_http_service_function', NO_CLIENT_ID)
    LOG.parms(NO_CLIENT_ID, 'service_url:', service_url)
    if not isinstance(service_url, str):
        err = TypeError('service_url must be a string')
        LOG.error('_get_http_service_function', NO_CLIENT_ID, err)
        raise err

    def _http_service_function(callback):
        LOG.entry('_http_service_function', NO_CLIENT_ID)
        LOG.parms(NO_CLIENT_ID, 'callback:', callback)
        try:
            conn = httplib.HTTPConnection(service_url.netloc)
            conn.request('GET', service_url.path)
            res = conn.getresponse()
            if res.status == httplib.OK:
                try:
                    json_obj = loads(res)
                    if 'service' in json_obj:
                        service = json_obj['service']
                    else:
                        service = None
                    callback(None, service)
                except Exception as exc:
                    err = TypeError(
                        'http request to ' + service_url + ' returned ' +
                        'unparseable JSON: ' + str(exc))
                    LOG.error('_http_service_function', NO_CLIENT_ID, err)
                    callback(err)
            else:
                err = mqlex.NetworkError(
                    'http request to ' + service_url + ' failed with a ' +
                    'status code of ' + str(res.status))
                LOG.error('_http_service_function', NO_CLIENT_ID, err)
                callback(err, None)
        except (httplib.HTTPException, socket.error) as exc:
            err = mqlex.NetworkError(
                'http request to ' + service_url + ' failed:' + str(exc))
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
        print codecs.open
        with codecs.open(file_path, encoding='utf-8', mode='r') as file_obj:
            print file_obj
            print file_obj.read
            try:
                opened = True
                r = file_obj.read()
                print 'json'
                print r
                json_obj = loads(r)
                if 'service' in json_obj:
                    service = json_obj['service']
                else:
                    service = None
                LOG.data(NO_CLIENT_ID, 'service:', service)
                callback(None, service)
            except Exception as exc:
                err = mqlex.MQLightError(
                    'The content read from ' + file_path + ' contained ' +
                    'unparseable JSON: ' + str(exc))
                LOG.error('_file_service_function', NO_CLIENT_ID, err)
                callback(err, None)
        file_obj.close()
        if not opened:
            err = mqlex.MQLightError(
                'attempt to read ' +
                file_path +
                ' failed')
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
    LOG.parms(NO_CLIENT_ID, 'service:', service)

    # Ensure the service is a list
    input_service_list = []
    if not service:
        error = TypeError('service is None')
        LOG.error('_generate_service_list', NO_CLIENT_ID, error)
        raise error
    elif hasattr(service, '__call__'):
        error = TypeError('service cannot be a function')
        LOG.error('_generate_service_list', NO_CLIENT_ID, error)
        raise error
    elif isinstance(service, list):
        if len(service) == 0:
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
    for i in range(len(input_service_list)):
        service_url = urlparse(input_service_list[i])
        protocol = service_url.scheme

        # Check for auth details
        if service_url.username:
            if service_url.password:
                auth_user = service_url.username
                auth_password = service_url.password
            else:
                error = mqlex.InvalidArgumentError(
                    'URLs supplied via the service property must ' +
                    'specify both a user name and a password value, ' +
                    'or omit both values')
                LOG.error('_generate_service_list', NO_CLIENT_ID, error)
                raise error

            if security_options.property_user != auth_user:
                error = mqlex.InvalidArgumentError(
                    'User name supplied as user property ' +
                    'security_options.user  does not match ' +
                    'username supplied via a URL passed via the ' +
                    'service property ' + auth_user)
                LOG.error('_generate_service_list', NO_CLIENT_ID, error)
                raise error
            if security_options.property_password != auth_password:
                error = mqlex.InvalidArgumentError(
                    'Password name supplied as password property ' +
                    'security_options.password  does not match ' +
                    'password supplied via a URL passed via the ' +
                    'service property ' + auth_password)
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
                error = mqlex.InvalidArgumentError(
                    'URLs supplied via the service property contain ' +
                    'inconsistent user names')
                LOG.error('_generateServiceList', NO_CLIENT_ID, error)
                raise error
            elif security_options.url_password != auth_password:
                error = mqlex.InvalidArgumentError(
                    'URLs supplied via the service property contain ' +
                    'inconsistent password values')
                LOG.error('_generateServiceList', NO_CLIENT_ID, error)
                raise error

        # Check we are trying to use the amqp protocol
        if protocol not in ('amqp', 'amqps'):
            error = mqlex.InvalidArgumentError(
                'Unsupported URL ' + input_service_list[i] +
                ' specified for service. ' +
                'Only the amqp or amqps protocol are supported.' +
                protocol)
            LOG.error('_generate_service_list', NO_CLIENT_ID, error)
            raise error

        # Check we have a hostname
        host = service_url.hostname
        if host is None:
            error = mqlex.InvalidArgumentError(
                'Unsupported URL ' + input_service_list[i] +
                ' specified for service. Must supply a hostname.')
            LOG.error('_generate_service_list', NO_CLIENT_ID, error)
            raise error

        # Set default port if not supplied
        port = service_url.port
        if port is None:
            port = 5672 if protocol == 'amqp' else 5671

        # Check for no path
        path = service_url.path
        if path and path != '/':
            error = mqlex.InvalidArgumentError(
                'Unsupported URL ' + input_service_list[i] +
                ' paths (' + path + ' ) cannot be part of a service ' +
                'URL.')
            LOG.error('_generate_service_list', NO_CLIENT_ID, error)
            raise error

        service_list.append(protocol + '://' + host + ':' + str(port))
    LOG.exit('_generate_service_list', NO_CLIENT_ID, service_list)
    return service_list


def _stop_messenger(client, stop_processing_callback, callback=None):
    LOG.entry('_stop_messenger', client.get_id())
    stopped = True
    # If messenger available then request it to stop
    # (otherwise it must have already been stopped)
    if client._messenger:
        stopped = client._messenger.stop()

    # If stopped then perform the required stop processing
    if stopped:
        if client._heartbeat_timeout:
            client._heartbeat_timeout.cancel()
        stop_processing_callback(client, callback)
    else:
        # Otherwise check for the messenger being stopped again
        timer = threading.Timer(
            1, _stop_messenger, [
                client, stop_processing_callback, callback])
        timer.start()
    LOG.exit('_stop_messenger', client.get_id(), None)


class Client(object):

    """
    The Client class represents an MQLight client instance
    """

    def __init__(
            self,
            service,
            client_id=None,
            security_options=None,
            callback=None):
        """
        Constructs a new Client object in the started state

        Args:
            service: Required: when an instance of String this is a URL to
                connect to. When an instance of Array this is an array of URLs
                to connect to - each will be tried in turn until either a
                connection is successfully established to one of the URLs, or
                all of the URLs have been tried. When an instance of function
                is specified for this argument, then function is invoked each
                time the client wants to establish a connection (e.g. for any
                of the state transitions, on the state diagram shown earlier on
                this page, which lead to the 'connected' state) and is supplied
                a single parameter containing a callback in the form
                function(err, service). The function must supply the service
                URL as either an instance of string or array to the callback
                function and this will be treated in the same manner described
                previously.
            id: Optional; an identifier that is used to identify this client.
                Two different instances of Client can have the same id, however
                only one instance can be connected to the MQ Light service at a
                given moment in time. If two instances of Client have the same
                id and both try to connect then the first instance to establish
                its connection is diconnected in favour of the second instance.
                If this property is not specified then the client will generate
                a probabalistically unique ID.
            security_options: Optional; Any required security options for
                user name/password authentication and SSL.
            password: Optional; the password to use for authentication.
        Returns:
            An instance of Client
        Raises:
            TypeError, mqlex.InvalidArgumentError: if any of the passed
                arguments is invalid
        """
        LOG.entry('Client.constructor', NO_CLIENT_ID)
        LOG.parms(NO_CLIENT_ID, 'service:', service)
        LOG.parms(NO_CLIENT_ID, 'client_id:', client_id)
        LOG.parms(NO_CLIENT_ID, 'security_options:', security_options)
        LOG.parms(NO_CLIENT_ID, 'callback:', callback)

        # Ensure the service is a list or function
        service_function = None
        if hasattr(service, '__call__'):
            service_function = service
        elif isinstance(service, str):
            service_url = urlparse(service)
            if service_url.scheme in ('http', 'https'):
                service_function = _get_http_service_function(service_url)
            elif service_url.scheme == 'file':
                if service_url.hostname and service_url.hostname != 'localhost':
                    error = mqlex.InvalidArgumentError(
                        'service contains unsupported file URI of ' + service +
                        ', only file:///path or file://localhost/path are ' +
                        ' supported.')
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
            error = mqlex.InvalidArgumentError(
                'Client identifier ' + client_id +
                ' is longer than the maximum ID length of 48')
            LOG.error('Client.constructor', NO_CLIENT_ID, error)
            raise error

        # If client id is not a string then throw an error
        if not isinstance(client_id, str):
            error = TypeError('Client identifier must be a str')
            LOG.error('Client.constructor', NO_CLIENT_ID, error)
            raise error

        # currently client ids are restricted, reject any invalid ones
        for i in range(len(client_id)):
            if client_id[i] not in VALID_CLIENT_ID_CHARS:
                error = mqlex.InvalidArgumentError(
                    'Client Identifier ' + client_id +
                    ' contains invalid char: ' + client_id[i])
                LOG.error('Client.constructor', NO_CLIENT_ID, error)
                raise error

        # User/password must either both be present, or both be absent.
        if security_options:
            if isinstance(security_options, dict):
                security_options = SecurityOptions(security_options)
                # User/password must either both be present, or both be absent.
                if (security_options.property_user and security_options.property_password is None) or (
                        security_options.property_user is None and security_options.property_password):
                    error = mqlex.InvalidArgumentError(
                        'both user and password properties must be ' +
                        'specified together')
                    LOG.error('Client.constructor', NO_CLIENT_ID, error)
                    raise error
            else:
                error = TypeError('security_options must be a dict')
                LOG.error('Client.constructor', NO_CLIENT_ID, error)
                raise error

            # Validate the ssl security options
            print security_options.ssl_verify_name
            if security_options.ssl_verify_name:
                if security_options.ssl_verify_name not in [True, False]:
                    error = mqlex.InvalidArgumentError(
                        'ssl_verify_name value ' +
                        security_options.ssl_verify_name + ' is invalid. ' +
                        'Must evaluate to True of False')
                    LOG.error('Client.constructor', NO_CLIENT_ID, error)
                    raise error
            if security_options.ssl_trust_certificate:
                if not isinstance(security_options.ssl_trust_certificate, str):
                    error = TypeError(
                        'ssl_trust_certificate value ' +
                        str(security_options.ssl_trust_certificate) +
                        ' is invalid. Must be a string')
                    LOG.error('Client.constructor', NO_CLIENT_ID, error)
                    raise error
                if not os.path.isfile(security_options.ssl_trust_certificate):
                    error = TypeError(
                        'The file specified for ssl_trust_certificate is not ' +
                        'a regular file')
                    LOG.error('Client.constructor', NO_CLIENT_ID, error)
                    raise error
        else:
            security_options = SecurityOptions({})

        if callback and not hasattr(callback, '__call__'):
            error = TypeError('callback must be a function')
            LOG.error('Client.constructor', NO_CLIENT_ID, error)
            raise error

        # Save the required data as client fields
        self._service_function = service_function
        self._service_list = None
        self._id = client_id
        self._security_options = security_options

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

        # AN identifier for the connection
        self._connection_id = 0

        # Heartbeat
        self._heartbeat_timeout = None

        self._callbacks = {}

        # No drain event initially required
        self._drain_event_required = False

        # Number of attempts the client has tried to reconnect
        self._retry_count = 0

        if service_function is None:
            self._service_list = _generate_service_list(
                service,
                self._security_options)

        def connect_callback(err):
            if callback:
                callback(err, self)
        self._perform_connect(connect_callback, service, True)

        LOG.exit('Client.constructor', self._id, None)

    def _perform_connect(self, callback, service, new_client):
        """
        Performs the connection
        """
        LOG.entry('Client._perform_connect', self._id)
        if not new_client:
            current_state = self.get_state()
            # if we are not disconnected or disconnecting return with the client
            # object
            if current_state is not STOPPED and current_state != RETRYING:
                if (current_state == STOPPING):
                    def _still_disconnecting(client, callback):
                        """
                        Waits while the client is disconnecting
                        """
                        LOG.entry('Client._still_disconnecting', client._id)
                        if client.get_state() == STOPPING:
                            _still_disconnecting(client, callback)
                        else:
                            client._perform_connect(client, service, callback)
                        LOG.exit(
                            'Client._still_disconnecting',
                            client._id,
                            None)
                    _still_disconnecting(self, callback)
                else:
                    if callback:
                        LOG.entry(
                            'Client._perform_connect.callback',
                            self._id)
                        callback(None)
                        LOG.exit(
                            'Client._perform_connect.callback',
                            self._id,
                            None)
                LOG.exit('Client._perform_connect', self._id, self)
                return self

            if self.get_state() == STOPPED:
                self._set_state(STARTING)

            # If the messenger is not already stopped then something has gone
            # wrong
            if self._messenger and not self._messenger.stopped:
                error = mqlex.MQLightError('messenger is not stopped')
                LOG.error('Client._perform_connect', error)
                raise error
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
                    callback(None)
                else:
                    self._service_list = _generate_service_list(
                        service,
                        self._security_options)
                    self._connect_to_service(callback)
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
                self._connect_to_service(callback)
            except Exception as exc:
                LOG.error('Client._perform_connect', exc)
                if callback:
                    callback(exc)
        LOG.exit('Client._perform_connect', self._id, None)
        return

    #def __del__(self):
    #    LOG.entry('Client.destructor', NO_CLIENT_ID)
    #    if (self and self._state == STARTED):
    #        self._id = 'STOPPING'
    #        self._messenger.send()
    #        self.stop()
    #    LOG.exit('Client.destructor', NO_CLIENT_ID, None)

    def __enter__(self):
        LOG.entry('Client.__enter__', self._id)
        self.start()
        while self.get_state() != STARTED:
            # Wait for the connection to be established
            pass
        LOG.exit('Client.__enter__', self._id, self)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        LOG.entry('Client.__exit__', self._id)
        self.stop()
        LOG.exit('Client.__exit__', self._id, None)

    def __iadd__(self, args):
        LOG.entry('Client.__iadd__', self._id)
        if isinstance(args, tuple) and len(args) == 2:
            LOG.parms(self._id, 'event:', args[0])
            LOG.parms(self._id, 'callback:', args[1])
            self.add_listener(args[0], args[1])
        else:
            raise TypeError('args must be a tuple (event, callback)')
        LOG.exit('Client.__iadd__', self._id, self)
        return self

    def __isub__(self, args):
        LOG.entry('Client.__isub__', self._id)
        if isinstance(args, tuple) and len(args) == 2:
            LOG.parms(self._id, 'event:', args[0])
            LOG.parms(self._id, 'callback:', args[1])
            self.del_listener(args[0], args[1])
        else:
            raise TypeError('args must be a tuple (event, callback)')
        LOG.exit('Client.__isub__', self._id, self)
        return self

    def callback(self, event):
        def func_wrapper(func):
            self.add_listener(event, func)
        return func_wrapper

    def start(self, callback=None):
        """
        Connects to the MQ Light service.

        This method is asynchronous and calls the optional callback function
        when:
        a) the client has successfully connected to the MQ Light service, or
        b) the client.disconnect() method has been invoked before a successful
        connection could be established, or
        c) the client could not connect to the MQ Light service. The callback
        function should accept a single argument which will be set to None
        if the client connects successfully or an Error object if the client
        cannot connect to the MQ Light service or is disconnected before a
        connection can be established.

        Calling this method will result in either the 'connected' event being
        emitted or an 'error' event being emitted (if a connection cannot be
        established). These events are guaranteed to be dispatched on a
        subsequent pass through the event loop - so, to avoid missing an event,
        the corresponding listeners must be registered either prior to calling
        client.connect() or on the same tick as calling client.connect().

        If this method is invoked while the client is in 'connecting',
        'connected' or 'retrying' states then the method will complete without
        performing any work or changing the state of the client. If this method
        is invoked while the client is in 'disconnecting' state then it's
        effect will be deferred until the client has transitioned into
        'disconnected' state.

        Args:
            callback: function to call when the connection is established
        Returns:
            The Client instance
        Raises:
            TypeError: if callback is not a function
        """
        LOG.entry('Client.start', self._id)

        if callback and not hasattr(callback, '__call__'):
            error = TypeError('callback must be a function')
            LOG.error('Client.start', self._id, error)
            raise error
        self._perform_connect(callback, self._service, False)
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

        LOG.entry('_process_queued_actions', self.get_id())
        LOG.parms(self._id, 'err:', err)
        LOG.parms(self._id, 'state:', self.get_state())
        if err is None:
            LOG.data(
                self._id,
                'client._queued_subscriptions:',
                self._queued_subscriptions)
            while len(self._queued_subscriptions) > 0 and self.get_state() == STARTED:
                sub = self._queued_subscriptions.pop()
                if sub['noop']:
                    # no-op so just trigger the callback wihtout actually
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
            while len(self._queued_unsubscribes) > 0 and self.get_state() == STARTED:
                sub = self._queued_unsubscribes.pop()
                if sub['noop']:
                    # no-op so just trigger the callback wihtout actually
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
            while len(self._queued_sends) > 0 and self.get_state() == STARTED:
                msg = self._queued_sends.pop()
                self.send(msg.topic, msg.data, msg.options, msg.callback)

        LOG.exit('_process_queued_actions', self._id, None)

    def _check_for_messages(self):
        """
        Function to force the client to check for messages, outputting the
        contents of any that have arrived to the client event emitter.
        """
        LOG.entry_often('Client._check_for_messages', self._id)
        if self.get_state() != STARTED or len(
                self._subscriptions) == 0 or MESSAGE not in self._callbacks:
            LOG.exit_often('Client._check_for_messages', self._id, None)
            return
        try:
            messages = self._messenger.receive(50)
            if messages and len(messages) > 0:
                LOG.debug(self._id, 'received ' +
                          str(len(messages)) +
                          ' messages')
                for message in range(len(messages)):
                    LOG.debug(self._id, 'processing message ' + str(message))
                    msg = messages[message]
                    msg.connection_id = self._connection_id
                    data = msg.body

                    topic = urlparse(msg.address).path[1:]
                    auto_confirm = True
                    qos = QOS_AT_MOST_ONCE

                    def filter_function(item):
                        # 1 added to length to account for the / we add
                        address_no_service = item['address'][
                            len(self._service) + 1:]
                        # Possible to have 2 matches work out whether this is
                        # for a share or private topic
                        if item['share'] is None and 'private:' in msg.link_address:
                            link_no_priv_share = msg.link_address[8:]
                            if address_no_service == link_no_priv_share:
                                return True
                        elif item['share'] and 'share:' in msg.link_address:
                            # Starting after the share: look for the next :
                            # denoting the end of the share name and get
                            # everything past that
                            link_no_share = msg.link_address[
                                msg.link_address.index(
                                    ':',
                                    7) +
                                1:]
                            if address_no_service == link_no_share:
                                return True
                            else:
                                return False

                    matched_subs = [
                        sub for sub in self._subscriptions if filter_function(sub)]
                    # Should only ever be one entry in matched_subs
                    if len(matched_subs) == 1:
                        qos = matched_subs[0]['qos']
                        if qos == QOS_AT_LEAST_ONCE:
                            auto_confirm = matched_subs[0]['auto_confirm']
                        matched_subs[0]['unconfirmed'] += 1
                    else:
                        # ideally we shouldn't get here, but it can happen in
                        # a timing window if we had received a message from a
                        # subscription we've subsequently unsubscribed from
                        LOG.debug(
                            self._id,
                            'No subscription matched message: ' +
                            data + ' going to address: ' +
                            msg.address)
                        msg = None
                        continue

                    def _auto_confirm(delivery):
                        LOG.entry(
                            'Client._check_for_messages._auto_confirm',
                            self._id)
                        LOG.data(self._id, 'data:', data)
                        LOG.exit(
                            'Client._check_for_messages._auto_confirm',
                            self._id,
                            None)

                    def _no_auto_confirm(delivery, msg=None):
                        LOG.entry(
                            'Client._check_for_messages._no_auto_confirm',
                            self._id)
                        LOG.data(self._id, 'delivery:', delivery)
                        if self.is_stopped():
                            err = mqlex.NetworkError('not started')
                            LOG.error(
                                'Client._check_for_messages._no_auto_confirm',
                                self._id,
                                err)
                            raise err
                        if msg:
                            # Also throw mqlex.NetworkError if the client has
                            # disconnected at some point since this particular
                            # message was received
                            if msg.connection_id != self._connection_id:
                                err = mqlex.NetworkError(
                                    'Client has reconnected since this ' +
                                    'message was received')
                                LOG.error('_no_auto_confirm', self._id, err)
                                raise err
                            subscription = matched_subs[0]
                            self._messenger.settle(msg)
                            subscription['unconfirmed'] -= 1
                            subscription['confirmed'] += 1
                            LOG.data(
                                self._id,
                                '[credit, unconfirmed, confirmed]:',
                                '[' +
                                subscription['credit'] +
                                ', ' +
                                subscription['unconfirmed'] +
                                ', ' +
                                subscription['confirmed'])
                            # Ask to flow more messages if >= 80% of available
                            # credit (e.g. not including unconfirmed messages)
                            # has been used or we have just confirmed
                            # everything
                            available = subscription[
                                'credit'] - subscription['unconfirmed']
                            if (
                                    available /
                                    subscription['confirmed']) <= 1.25 or (
                                    subscription['unconfirmed'] == 0 and subscription['confirmed'] > 0):
                                self._messenger.flow(
                                    self._service + '/' + msg.link_address,
                                    subscription['confirmed'])
                                subscription['confirmed'] = 0
                            #del msg
                        LOG.exit(
                            'Client._check_for_messages._no_auto_confirm',
                            self._id,
                            None)
                    if auto_confirm:
                        confirm_delivery = _auto_confirm
                    else:
                        confirm_delivery = _no_auto_confirm
                    delivery = {
                        'message': {
                            'properties': {
                                'content_type': msg.content_type
                            },
                            'topic': topic,
                            'confirm_delivery': confirm_delivery
                        }
                    }
                    link_address = msg.link_address
                    if link_address:
                        delivery['destination'] = {}
                        split = link_address.split(':', 3)
                        if 'share:' in link_address:
                            share = link_address.index('share:')
                        else:
                            share = 1
                        if share == 0:
                            delivery['destination']['share'] = split[1]
                            delivery['destination']['topic_pattern'] = split[2]
                        else:
                            delivery['destination']['topic_pattern'] = split[1]

                    if msg.ttl > 0:
                        delivery['message']['ttl'] = msg.ttl

                    annots = msg.annotations
                    malformed = {
                        'MQMD': {},
                        'condition': None
                    }
                    if annots is not None:
                        for i in range(len(annots)):
                            if annots[i] and annots[i].key:
                                if annots[i].key == 'x-opt-message-malformed-condition':
                                    malformed['condition'] = annots[i].value
                                elif annots[i].key == 'x-opt-message-malformed-description':
                                    malformed['description'] = annots[i].value
                                elif annots[i].key == 'x-opt-message-malformed-MQMD-CodedCharSetId':
                                    malformed['MQMD']['CodedCharSetId'] = int(
                                        annots[i].value)
                                elif annots[i].key == 'x-opt-message-malformed-MQMD.Format':
                                    malformed['MQMD'][
                                        'Format'] = annots[i].value

                    if malformed['condition']:
                        if MALFORMED in self._callbacks:
                            delivery['malformed'] = malformed
                            self._emit(MALFORMED, msg.body, delivery)
                        else:
                            msg = None
                            raise mqlex.MQLightError(
                                'no listener for malformed event')
                    else:
                        LOG.emit(
                            'Client._check_for_messages',
                            self._id,
                            MESSAGE,
                            data,
                            delivery)
                        self._emit(MESSAGE, data, delivery)

                    if self.is_stopped():
                        LOG.debug(
                            self._id,
                            'client is stopped so not accepting or settling ' +
                            'message')
                        msg = None
                        # def msg
                    else:
                        if qos == QOS_AT_MOST_ONCE:
                            self._messenger.accept(msg)
                        if qos == QOS_AT_MOST_ONCE or auto_confirm:
                            self._messenger.settle(msg)
                            matched_subs[0]['unconfirmed'] -= 1
                            matched_subs[0]['confirmed'] += 1
                            LOG.data(
                                self._id,
                                '[credit, unconfirmed, confirmed]:',
                                '[' +
                                str(matched_subs[0]['credit']) +
                                ', ' +
                                str(matched_subs[0]['unconfirmed']) +
                                ', ' +
                                str(matched_subs[0]['confirmed']) +
                                ']')
                            # Ask to flow more messages if >= 80% of available
                            # credit (e.g. not including unconfirmed messages)
                            # has been used. Or we have just confirmed
                            # everything.
                            available = matched_subs[0][
                                'credit'] - matched_subs[0]['unconfirmed']
                            if available / matched_subs[0]['confirmed'] <= 1.25 or (
                                    matched_subs[0]['unconfirmed'] == 0 and matched_subs[0]['confirmed'] > 0):
                                self._messenger.flow(
                                    self._service +
                                    '/' +
                                    msg.link_address,
                                    matched_subs[0]['confirmed'])
                                matched_subs[0]['confirmed'] = 0
                                msg = None
                                #del msg

        except Exception as exc:
            LOG.error('Client._check_for_messages', self._id, exc)

            def next_tick():
                LOG.emit('Client._check_for_messages', self._id, ERROR, exc)
                self._emit(ERROR, exc)
                if _should_reconnect(exc):
                    self._reconnect()
            timer = threading.Timer(1, next_tick)
            timer.start()

        if self.get_state() == STARTED:
            timer = threading.Timer(0.2, self._check_for_messages)
            timer.start()

        LOG.exit_often('Client._check_for_messages', self._id, None)

    def stop(self, callback=None):
        """
        Disconnects the client from the MQ Light service, implicitly closing any
        subscriptions that the client has open. The 'disconnected' event will be
        emitted once the client has disconnected.

        This method works asynchronously, and will invoke the optional callback
        once the client has disconnected. The callback function should accept a
        single Error argument, although there is currently no situation where
        this will be set to any other value than undefined.

        Calling client.disconnect() when the client is in 'disconnecting' or
        'disconnected' state has no effect. Calling client.disconnect() from any
        other state results in the client disconnecting and the 'disconnected'
        event being generated.

        Args:
            callback: function to call when the connection is closed
        """
        LOG.entry('Client.stop', self._id)
        if (callback and not hasattr(callback, '__call__')):
            raise TypeError('callback must be a function')

        # just return if already stopped or in the process of
        # stopping
        if self.is_stopped():
            if callback:
                LOG.entry('Client.stop.callback', self._id)
                callback(None)
                LOG.exit('Client.stop.callback', self._id, None)
            LOG.exit('Client.stop', self._id, self)
            return self

        self._perform_disconnect(callback)
        LOG.exit('Client.stop', self._id, None)
        return self

    def _perform_disconnect(self, callback):
        """
        Performs the disconnection
        """
        LOG.entry('Client._perform_disconnect', self._id)
        self._set_state(STOPPING)

        # Only disconnect when all outstanding send operations are complete
        if len(self._outstanding_sends) == 0:
            def stop_processing(client, callback):
                LOG.entry(
                    'Client._perform_disconnect.stop_processing',
                    self._id)
                if client._heartbeat_timeout:
                    client._heartbeat_timeout.cancel()

                # Clear all queued sends as we are disconnecting
                while len(self._queued_sends) > 0:
                    msg = self._queued_sends.pop()

                    def next_tick():
                        """
                        next tick
                        """
                        LOG.entry(
                            'Client._perform_disconnect.next_tick',
                            self._id)
                        msg['callback'](
                            mqlex.StoppedError(
                                'send aborted due to disconnect'))
                        LOG.exit(
                            'Client._perform_disconnect.next_tick',
                            self._id,
                            None)
                    timer = threading.Timer(1, next_tick)
                    timer.start()

                # Clear the active subscriptions list as we were asked to
                # disconnect
                LOG.data(self._id, 'self._subscriptions:', self._subscriptions)
                self._subscriptions = self._subscriptions[::-1]

                # Indicate that we've disconnected
                client._set_state(STOPPED)
                LOG.emit(
                    'Client._perform_disconnect.stop_processing',
                    self._id,
                    STOPPED)
                self._first_start = True
                self._emit(STOPPED, True)

                if callback:
                    LOG.entry('Client._perform_disconnect.callback', self._id)
                    callback(None)
                    LOG.exit(
                        'Client._perform_disconnect.callback',
                        self._id,
                        None)
                LOG.exit(
                    'Client._perform_disconnect.stop_processing',
                    self._id,
                    None)
            _stop_messenger(self, stop_processing, callback)

        LOG.exit('Client._perform_disconnect', self._id, None)
        return

    def _connect_to_service(self, callback):
        """
        Function to connect to the service, tries each available service in turn
        If none can connect it emits an error, waits and attempts to connect
        again. Callback happens once a successful connect/reconnect occurs
        """
        LOG.entry('Client._connect_to_service', self._id)
        if self.is_stopped():
            if callback:
                LOG.entry('Client._connect_to_service.callback', self._id)
                callback(
                    mqlex.StoppedError('connect aborted due to disconnect'))
                LOG.exit('Client._connect_to_service.callback', self._id, None)
            LOG.exit('Client._connect_to_service', self._id, None)
            return
        error = None
        connected = False

        # Try each service in turn until we can successfully connect, or exhaust
        # the list
        for service in self._service_list:
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
                    auth += quote(str(self._security_options.property_password))
                    auth += '@'
                log_url = None
                # reparse the service url to prepend authentication information
                # back on as required
                if auth:
                    service_url = urlparse(service)
                    service = service_url.scheme + \
                        '://' + auth + service_url.hostname
                    log_url = service_url.scheme + '://' + \
                        re.sub(r'/:[^\/:]+@/g', '********@', auth) + \
                        service_url.hostname + ':' + str(service_url.port)
                else:
                    log_url = service
                LOG.data(self._id, 'attempting to connect to: ' + service)
                LOG.data(self._id, 'log_url: ' + log_url)

                connect_url = urlparse(service)
                # Remove any path elements from the URL (for ipv6 which appends
                # /)
                if connect_url.path:
                    href_length = len(service) - len(connect_url.path)
                    connect_service = service[0:href_length]
                else:
                    connect_service = service

                if self._security_options.ssl_trust_certificate is not None:
                    ssl_trust_certificate = self._security_options.ssl_trust_certificate
                else:
                    ssl_trust_certificate = None
                if self._security_options.ssl_verify_name is not None:
                    ssl_verify_name = self._security_options.ssl_verify_name
                else:
                    ssl_verify_name = None

                try:
                    self._messenger.connect(
                        urlparse(connect_service),
                        ssl_trust_certificate,
                        ssl_verify_name)
                    LOG.data(self._id, 'successfully connected to:', log_url)
                    self._service = service
                    connected = True
                    break
                except Exception as exc:
                    error = exc
                    LOG.data(
                        self._id,
                        'failed to connect to: ' + str(log_url) +
                        ' due to error: ' + str(error))
            except Exception as exc:
                # Should never get here, as it means that messenger.connect has
                # been called in an invalid way, so FFDC
                error = exc
                LOG.ffdc(
                    'Client._connect_to_service',
                    'ffdc001',
                    self._id,
                    exc)
                raise mqlex.MQLightError(exc)

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
                LOG.emit(
                    'Client._connect_to_service.next_tick',
                    self._id,
                    event_to_emit)
                self._emit(event_to_emit, None)
            timer = threading.Timer(1, next_tick)
            timer.start()

            if callback:
                LOG.entry('Client._connect_to_service.callback2', self._id)
                callback(None)
                LOG.exit(
                    'Client._connect_to_service.callback2',
                    self._id,
                    None)

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
                    LOG.entry(
                        'Client._connect_to_service.perform_heartbeat',
                        self._id)
                    if self._messenger:
                        self._messenger.work(0)
                        self._heartbeat_timeout = threading.Timer(
                            heartbeat_interval,
                            perform_heartbeat,
                            [heartbeat_interval])
                        self._heartbeat_timeout.start()
                    LOG.exit(
                        'Client._connect_to_service.perform_heartbeat',
                        self._id,
                        None)
                self._heartbeat_timeout = threading.Timer(
                    heartbeat_interval,
                    perform_heartbeat,
                    [heartbeat_interval])
                self._heartbeat_timeout.start()

        else:
            # We've tried all services without success. Pause for a while before
            # trying again
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
            LOG.data(self._id, 'trying to connect again ' +
                     'after ' + str(interval) + ' seconds')
            timer = threading.Timer(interval, retry)
            timer.start()

            if error:
                def next_tick():
                    LOG.emit(
                        'Client._connect_to_service',
                        self._id,
                        ERROR,
                        error)
                    self._emit(ERROR, error)
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
        if self.get_state() != STARTED:
            if self.is_stopped():
                return
            elif self.get_state() == RETRYING:
                return self
        self._set_state(RETRYING)

        # Stop the messenger to free the object then attempt a reconnect
        def stop_processing(client, callback=None):
            LOG.entry('Client.reconnect.stop_processing', client._id)

            if client._heartbeat_timeout:
                client._heartbeat_timeout.cancel()

            # clear the subscriptions list, if the cause of the reconnect
            # happens during check for messages we need a 0 length so it will
            # check once reconnected.
            client._queued_subscriptions = client._queued_subscriptions[::-1]
            # also clear any left over outstanding sends
            client._outstanding_sends = client._outstanding_sends[::-1]
            client._perform_connect(
                self._process_queued_actions,
                self._service,
                False)

            LOG.exit('Client.reconnect.stop_processing', client._id, None)

        _stop_messenger(self, stop_processing)
        LOG.exit('Client._reconnect', self._id, self)
        return self

    def get_id(self):
        """
        Returns:
            The client id
        """
        LOG.data('id', self._id)
        return self._id

    def get_service(self):
        """
        Returns:
            The service if connected otherwise None
        """
        if self.get_state() == STARTED:
            LOG.data(self._id, 'service:', self._service)
            return self._service
        else:
            LOG.data(self._id, 'Not connected')
            LOG.data(self._id, 'service: None')
            return None

    def get_state(self):
        """
        Returns:
            The state of the client
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
            raise mqlex.InvalidArgumentError('invalid state')

    def is_stopped(self):
        """
        Returns:
            True if in disconnected or disconnecting state, otherwise False
        """
        LOG.data(self._id, 'state:', self._state)
        return self._state in (STOPPED, STOPPING)

    def add_listener(self, event, callback):
        """
        Registers a callback to be called when the event is emitted

        Args:
            event: event the callback is registered on
            callback: function to call when the event is triggered
        Raises:
            TypeError: if callback is not a function
            mqlex.InvalidArgumentError: if event is invalid
        """
        LOG.entry('Client.add_listener', self._id)
        LOG.parms(self._id, 'event:', event)
        LOG.parms(self._id, 'callback:', callback.__name__)

        if event in EVENTS:
            if hasattr(callback, '__call__'):
                if event not in self._callbacks:
                    self._callbacks[event] = []
                self._callbacks[event].append(callback)
            else:
                raise TypeError('callback must be a function')
        else:
            raise mqlex.InvalidArgumentError('invalid event ' + str(event))
        LOG.exit('Client.add_listener', self._id, None)

    def del_listener(self, event, callback):
        """
        Removes a callback for the specified event

        Args:
            event: event the callback is registered on
            callback: callback function to remove
        Raises:
            mqlex.InvalidArgumentError: if event is invalid
        """
        LOG.entry('Client.del_listener', self._id)
        if event in EVENTS:
            if event in self._callbacks and callback in self._callbacks[event]:
                self._callbacks[event].remove(callback)
        else:
            raise mqlex.InvalidArgumentError('invalid event ' + str(event))
        LOG.exit('Client.del_listener', self._id, None)

    def _emit(self, event, *args, **kwargs):
        """
        Calls all the callbacks registered with the events that is emitted

        Raises:
            mqlex.InvalidArgumentError: if event is invalid
        """
        LOG.entry('Client._emit', self._id)
        LOG.parms(self._id, 'event:', event)
        LOG.parms(self._id, 'args:', args)
        LOG.parms(self._id, 'kwargs:', kwargs)
        callbacks = []
        if event in EVENTS:
            if event in self._callbacks:
                for callback in self._callbacks[event]:
                    callbacks.append(callback)
                for callback in callbacks:
                    callback(*args, **kwargs)
        else:
            raise mqlex.InvalidArgumentError('invalid event ' + str(event))
        LOG.exit('Client._emit', self._id, None)

    def send(self, topic, data, options=None, callback=None):
        """
        Sends a message to the MQLight service.

        Args:
            topic: topic of the message
            data: content of the message
            options: message attributes
            callback: function to call whent the message is sent
        Returns:
            True if this message was either sent or is the next to be sent or
            False if the message was queued in user memory, because either there
            was a backlog of messages, or the client was not in a connected
            state
        Raises:
            TypeError: if any of the arguments is invalid
            mqlex.StoppedError: if the client is disconnected
        """
        LOG.entry('Client.send', self._id)
        next_message = False
        # Validate the passed parameters
        if topic is None or len(topic) == 0:
            raise mqlex.InvalidArgumentError('Cannot send to None topic')
        else:
            topic = str(topic)
        LOG.parms(self._id, 'topic:', topic)

        if data is None:
            raise mqlex.InvalidArgumentError('Cannot send no data')
        elif hasattr(data, '__call__'):
            raise TypeError('Cannot send a function')
        LOG.parms(self._id, 'data:', data)

        # Validate the remaining optional parameters, assigning local variables
        # to the appropriate parameter
        if options is not None and callback is None:
            if hasattr(options, '__call__'):
                callback = options
                options = None

        if options is not None:
            if isinstance(options, dict):
                LOG.parms(self._id, 'options:', options)
            else:
                raise TypeError('options must be an dict type')

        qos = QOS_AT_MOST_ONCE
        ttl = None
        if options:
            if 'qos' in options:
                if options['qos'] in QOS:
                    qos = options['qos']
                else:
                    raise mqlex.InvalidArgumentError(
                        'options.qos value ' + str(options['qos']) +
                        ' is invalid must evaluate to 0 or 1')
            if 'ttl' in options:
                try:
                    ttl = int(options['ttl'])
                    if ttl < 0:
                        raise TypeError()
                    if ttl > 4294967295:
                        # Cap at max AMQP value for TTL (2^32-1)
                        ttl = 4294967295
                except Exception as err:
                    raise TypeError(
                        'options.ttl value ' + str(options['ttl']) +
                        ' is invalid must be an unsigned integer number')

        if callback:
            if not hasattr(callback, '__call__'):
                raise TypeError('callback must be a function type')
        elif qos == QOS_AT_LEAST_ONCE:
            raise mqlex.InvalidArgumentError(
                'callback must be specified when options.qos value of 1 ' +
                '(at least once) is specified')

        # Ensure we have attempted a connect
        if self.is_stopped():
            raise mqlex.StoppedError('not started')

        # Ensure we are not retrying otherwise queue message and return
        if self.get_state() == RETRYING or self.get_state() == STARTING:
            self._queued_sends.append({
                'topic': topic,
                'data': data,
                'options': options,
                'callback': callback
            })
            self._drain_event_required = True
            LOG.exit('Client.send', self._id, False)
            return False

        # Send the data as a message to the specified topic
        msg = None
        local_message_id = None
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

            self._messenger.put(msg, qos)
            self._messenger.send()

            # Record that a send operation is in progress
            local_message_id = str(uuid.uuid4()).replace('-', '_')
            self._outstanding_sends.append(local_message_id)

            def until_send_complete(msg, local_message_id, send_callback):
                """
                Setup a timer to trigger the callback once the msg has been
                sent, or immediately if no message to be sent
                """
                LOG.entry('Client.send.until_send_complete', self._id)
                LOG.parms(self._id, 'msg:', msg)
                LOG.parms(self._id, 'local_message_id:', local_message_id)
                LOG.parms(self._id, 'send_callback:', send_callback)
                try:
                    complete = False
                    err = None
                    if (not self._messenger.stopped):
                        status = self._messenger.status(msg)
                        LOG.data(self._id, 'status:', status)
                        if str(status) in ('ACCEPTED', 'SETTLED'):
                            self._messenger.settle(msg)
                            complete = True
                        elif str(status) == 'REJECTED':
                            complete = True
                            err_msg = self._messenger.status_error(msg)
                            if err_msg is None:
                                err_msg = 'send failed - message was rejected'
                            err = mqlex.MQLightError(err_msg)
                        elif str(status) == 'RELEASED':
                            complete = True
                            err = mqlex.MQLightError(
                                'send failed - message was released')
                        elif str(status) == 'MODIFIED':
                            complete = True
                            err = mqlex.MQLightError(
                                'send failed - message was modified')
                        elif str(status) == 'ABORTED':
                            complete = True
                            err = mqlex.MQLightError(
                                'send failed - message was aborted')

                        # if complete then do final processing of this message.
                        if complete:
                            if local_message_id in self._outstanding_sends:
                                self._outstanding_sends.remove(
                                    local_message_id)

                            # If previously send() returned false and now the
                            # backlog of messages is cleared, emit a drain
                            # event
                            LOG.data(
                                self._id,
                                'outstandingSends:',
                                len(self._outstanding_sends))
                            if self._drain_event_required and len(
                                    self._outstanding_sends) <= 1:
                                LOG.emit(
                                    'Client.send.until_send_complete',
                                    self._id,
                                    DRAIN)
                                self._drain_event_required = False
                                self._emit(DRAIN)

                            # invoke the callback, if specified
                            if send_callback:
                                decoded = unquote(msg.address)
                                topic = urlparse(decoded).path[1:]
                                LOG.entry(
                                    'Client.send.until_send_complete.callback1',
                                    self._id)
                                send_callback(
                                    err,
                                    topic,
                                    msg.body,
                                    options)
                                LOG.exit(
                                    'Client.send.until_send_complete.callback1',
                                    self._id,
                                    None)
                            #del msg
                            LOG.exit(
                                'Client.send.until_send_complete',
                                self._id,
                                None)
                            return

                        # message not sent yet, so check again in a second or
                        # so
                        self._messenger.send()
                        timer = threading.Timer(
                            1,
                            until_send_complete,
                            [msg, local_message_id, send_callback])
                        timer.start()
                    else:
                        # TODO Not sure we can actually get here (so FFDC?)
                        if local_message_id:
                            if local_message_id in self._outstanding_sends:
                                self._outstanding_sends.remove(
                                    local_message_id)
                        if send_callback:
                            err = mqlex.StoppedError(
                                'send may not have completed due to ' +
                                'client stop')
                            LOG.entry(
                                'Client.send.until_send_complete.callback2',
                                self._id)
                            send_callback(
                                err,
                                topic,
                                msg.body,
                                options)
                            LOG.exit(
                                'Client.send.until_send_complete.callback2',
                                self._id,
                                None)
                        #del msg
                        LOG.exit(
                            'Client.send.until_send_complete',
                            self._id,
                            None)
                        return
                except Exception as exc:
                    error = exc
                    LOG.error(
                        'Client.send.until_send_complete',
                        self._id,
                        error)
                    # Error condition so won't retry send remove from list of
                    # unsent
                    if local_message_id in self._outstanding_sends:
                        self._outstanding_sends.remove(local_message_id)
                    # An error here could still mean the message made it over
                    # so we only care about at least once messages
                    if qos == QOS_AT_LEAST_ONCE:
                        self._queued_sends.append({
                            'topic': topic,
                            'data': data,
                            'options': options,
                            'callback': callback
                        })

                    def next_tick():
                        """
                        next_tick
                        """
                        if send_callback:
                            if qos == QOS_AT_MOST_ONCE:
                                # we don't know if an at most once message made
                                # it across call the callback with an err of
                                # None to indicate success to avoid the user
                                # resending on error.
                                LOG.entry(
                                    'Client.send.until_send_complete.next_tick',
                                    self._id)
                                send_callback(
                                    err,
                                    topic,
                                    msg.body,
                                    options)
                                LOG.exit(
                                    'Client.send.until_send_complete.next_tick',
                                    self._id,
                                    None)
                        if error:
                            LOG.emit(
                                'Client.send.until_send_complete.next_tick',
                                self._id,
                                ERROR,
                                error)
                            self._emit(ERROR, error)
                            if _should_reconnect(error):
                                self._reconnect()

                    timer = threading.Timer(1, next_tick)
                    timer.start()
                LOG.exit_often(
                    'Client.send.until_send_complete',
                    self._id,
                    None)
            # start the timer to trigger it to keep sending until msg has sent
            until_send_complete(msg, local_message_id, callback)

            # If we have a backlog of messages, then record the need to emit a
            # drain event later to indicate the backlog has been cleared
            LOG.data(
                self._id, 'outstandingSends:', len(
                    self._outstanding_sends))
            if len(self._outstanding_sends) <= 1:
                next_message = True
            else:
                self._drain_event_required = True

        except Exception as exc:
            err = mqlex.MQLightError(exc)
            LOG.error('Client.send', self._id, err)
            if local_message_id:
                if local_message_id in self._outstanding_sends:
                    self._outstanding_sends.remove(local_message_id)

            if qos == QOS_AT_LEAST_ONCE:
                self._queued_sends.append({
                    'topic': topic,
                    'data': data,
                    'options': options,
                    'callback': callback
                })

            def next_tick():
                """
                next_tick
                """
                if callback:
                    if self.get_state() in (STOPPED, STOPPING):
                        LOG.entry('Client.send.next_tick', self._id)
                        callback(err, msg)
                        LOG.exit('Client.send.next_tick', self._id, None)

                LOG.emit('Client.send.next_tick', self._id, ERROR, err)
                self._emit(ERROR, err)
                if _should_reconnect(err):
                    self._reconnect()
            timer = threading.Timer(1, next_tick)
            timer.start()

        LOG.exit('Client.send', self._id, next_message)
        return next_message

    def subscribe(
            self,
            topic_pattern,
            share=None,
            options=None,
            callback=None):
        """
        Constructs a subscription object and starts the emission of message
        events each time a message arrives, at the MQ Light service, that
        matches topic pattern.

        Args:
            topic_pattern: topic to subscribe to
            share: share name of the subscription
            options:
            callback: function to call when the subscription is done
        Raises:
            TypeError, mqlex.InvalidArgumentError: if any argument is invalid
            mqlex.StoppedError: if the client is disconnected
        """
        LOG.entry('Client.subscribe', self._id)
        if topic_pattern is None or topic_pattern == '':
            raise mqlex.InvalidArgumentError(
                'Cannot subscribe to an empty pattern')

        topic_pattern = str(topic_pattern)
        LOG.parms(self._id, 'topic_pattern:', topic_pattern)

        if options is None and callback is None:
            if hasattr(share, '__call__'):
                callback = share
                share = None
            elif isinstance(share, dict):
                options = share
                share = None
        elif callback is None:
            if hasattr(options, '__call__'):
                callback = options
                if not isinstance(share, str):
                    options = share
                    share = None
                else:
                    options = None

        LOG.parms(self._id, 'callback:', callback)
        original_share_value = share
        if share:
            share = str(share)
            if ':' in share:
                raise mqlex.InvalidArgumentError(
                    'share argument value ' + share +
                    ' is invalid because it contains a colon character')
            share = 'share:' + share + ':'
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
                if options['qos'] in (QOS_AT_MOST_ONCE, QOS_AT_LEAST_ONCE):
                    qos = options['qos']
                else:
                    raise mqlex.InvalidArgumentError(
                        'options[\'qos\'] value ' + str(options['qos']) +
                        ' is invalid must evaluate to 0 or 1')
            if 'auto_confirm' in options:
                if options['auto_confirm'] in (True, False):
                    auto_confirm = options['auto_confirm']
                else:
                    raise mqlex.InvalidArgumentError(
                        'options[\'auto_confirm\'] value ' +
                        str(options['auto_confirm']) +
                        ' is invalid must evaluate to True or False')
            if 'ttl' in options:
                try:
                    ttl = int(options['ttl'])
                    if ttl < 0:
                        raise TypeError()
                except Exception as err:
                    raise TypeError(
                        'options[\'ttl value\'] ' + str(options['ttl']) +
                        ' is invalid must be an unsigned integer number')
            if 'credit' in options:
                try:
                    credit = int(options['credit'])
                    if credit < 0:
                        raise TypeError()
                except Exception as err:
                    raise TypeError(
                        'options[\'credit\'] value ' + str(options['credit']) +
                        ' is invalid must be an unsigned integer number')

        LOG.parms(self._id, 'share:', share)

        if callback and not hasattr(callback, '__call__'):
            raise TypeError('callback must be a function')

        # Ensure we have attempted a connect
        if self.is_stopped():
            raise mqlex.StoppedError('not started')

        # Subscribe using the specified pattern and share options
        address = self.get_service() + '/' + share + topic_pattern
        subscription_address = self.get_service() + '/' + topic_pattern

        # If client is in the retrying state, then queue this subscribe request
        if self.get_state() == RETRYING or self.get_state() == STARTING:
            LOG.data(
                self._id,
                'Client waiting for connections so queued subscription')
            # first check if its already there and if so remove old and add new
            for sub in self._queued_subscriptions:
                if sub['address'] == subscription_address and sub[
                        'share'] == original_share_value:
                    self._queued_subscriptions.remove(sub)

            self._queued_subscriptions.append({
                'address': subscription_address,
                'qos': qos,
                'auto_confirm': auto_confirm,
                'topic_pattern': topic_pattern,
                'share': original_share_value,
                'options': options,
                'callback': callback
            })
            LOG.exit('Client.subscribe', self._id, self)
            return self

        err = None
        # if we already believe this subscription exists, we should reject the
        # request to subscribe by throwing a mqlex.SubscribedError
        for sub in self._subscriptions:
            if sub['address'] == subscription_address and sub[
                    'share'] == original_share_value:
                err = mqlex.SubscribedError(
                    'client is already subscribed to this address')
                LOG.error('Client.subscribe', self._id, err)
                raise err
        if err is None:
            try:
                self._messenger.subscribe(address, qos, ttl, credit)
            except Exception as exc:
                LOG.error('Client.subscribe', self._id, exc)
                err = mqlex.MQLightError(exc)

        if callback:
            def next_tick():
                callback(err, topic_pattern, original_share_value)
            timer1 = threading.Timer(1, next_tick)
            timer1.start()

        if err:
            LOG.emit('Client.subscribe', self._id, ERROR, err)
            self._emit(ERROR, err)

            if _should_reconnect(err):
                LOG.data(self._id, 'queued subscription and calling reconnect')
                self._queued_subscriptions.append({
                    'address': subscription_address,
                    'qos': qos,
                    'auto_confirm': auto_confirm,
                    'topic_pattern': topic_pattern,
                    'share': original_share_value,
                    'options': options,
                    'callback': callback
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
                'callback': callback,
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
            callback=None):
        """
        Stops the flow of messages from a destination to this client. The
        client's message callback will no longer be driven when messages arrive,
        that match the pattern associate with the destination. The
        pattern (and optional) <code>share</code> arguments must match
        those specified when the destination was created by calling the original
        client.subscribe(...) method.

        The optional options argument can be used to specify how the
        call to client.unsubscribe(...) behaves. If the
        options argument has any of the following properties they will
        be interpreted as follows:
            ttl - Optional, coerced to a number, if specified and must be equal
                to 0. If specified the client will reset the destination's time
                to live to 0 as part of the unsubscribe operation. If the
                destination is private to the client, then setting the TTL to
                zero will ensure that the destination is deleted. If the
                destination is shared when setting the TTL to zero, the
                destination will be deleted when no more clients are associated
                with the destination.
        Args:
            topic_pattern that was supplied in the previous call to subscribe.
            share (Optional) that was supplied in the previous call to
                 subscribe.
            options (Optional) The options argument accepts an object with
                 properties set to customise the unsubscribe behaviour.
            callback - (Optional) Invoked if the unsubscribe request has
                 been processed successfully.
        Returns:
            The instance of the client this was called on which will emit
            'message' events on arrival.

        Raises:
            mqlex.InvalidArgumentError: If the topic pattern parameter is
                undefined.
        """
        LOG.entry('Client.unsubscribe', self._id)
        LOG.parms(self._id, 'topic_pattern:', topic_pattern)

        if topic_pattern is None:
            error = mqlex.InvalidArgumentError(
                'You must specify a topic_pattern argument')
            LOG.error('Client.unsubscribe', self._id, error)
            raise error

        topic_pattern = str(topic_pattern)

        # Two or three arguments are the interesting cases - the rules we use to
        # disambiguate are:
        #   1) If the last argument is a function - it's the callback
        #   2) If we are unsure if something is the share or the options then
        #      a) It's the share if it's a String
        #      b) It's the options if it's an Object
        #      c) If it's neither of the above, then it's the share
        #         (and convert it to a String).
        if options is None and callback is None:
            if hasattr(share, '__call__'):
                callback = share
                share = None
            elif not isinstance(share, str) and isinstance(share, dict):
                options = share
                share = None
        elif callback is None:
            if hasattr(options, '__call__'):
                callback = options
                if not isinstance(share, str) and isinstance(share, dict):
                    options = share
                    share = None
                else:
                    options = None

        original_share_value = share
        if share:
            share = str(share)
            if ':' in share:
                error = mqlex.InvalidArgumentError(
                    'share argument value ' +
                    share +
                    ' is invalid because it contains a colon (:) character')
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
                    'options must be an object type not a ' +
                    type(options) +
                    ')')
                LOG.error('Client.unsubscribe', self._id, error)
                raise error

        ttl = None
        if options:
            if 'ttl' in options:
                try:
                    ttl = int(options['ttl'])
                    if ttl < 0:
                        raise TypeError()
                except Exception as err:
                    raise TypeError(
                        'options[\'ttl value\'] ' + str(options['ttl']) +
                        ' is invalid must be an unsigned integer number')

        if callback and not hasattr(callback, '__call__'):
            err = TypeError('callback must be a function type')
            LOG.error('Client.unsubscribe', self._id, err)
            raise err

        # Ensure we have attempted a connect
        if self.is_stopped():
            err = mqlex.StoppedError('not started')
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
                if sub['address'] == subscription_address and sub[
                        'share'] == original_share_value and not sub['noop']:
                    subscribed = True
                    break

        if not subscribed:
            err = mqlex.UnsubscribedError(
                'client is not subscribed to this address')
            LOG.error('Client.unsubscribe', self._id, err)
            raise err

        def queue_unsubscribe():
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
                    'client already had a queued subscribe ' +
                    'request for this address, so marked that as a noop and ' +
                    'will queue this unsubscribe request as a noop too')
            else:
                LOG.data(self._id, 'client waiting for connection so ' +
                         'queueing the unsubscribe request')

            self._queued_unsubscribes.append({
                'noop': noop,
                'address': subscription_address,
                'topic_pattern': topic_pattern,
                'share': original_share_value,
                'options': options,
                'callback': callback
            })

        # if client is in the retrying state, then queue this unsubscribe
        # request
        if self.get_state() == RETRYING or self.get_state() == STARTING:
            LOG.data(self._id, 'client still in the process of connecting ' +
                     'so queueing the unsubscribe request')
            queue_unsubscribe()
            LOG.exit('Client.unsubscribe', self._id, self)
            return self

        # unsubscribe using the specified topic pattern and share options
        error = None
        try:
            self._messenger.unsubscribe(address, ttl)

            if callback:
                def next_tick():
                    LOG.entry('Client.unsubscribe.callback', self._id)
                    callback(None)
                    LOG.exit('Client.unsubscribe.callback', self._id, None)
                timer = threading.Timer(1, next_tick)
                timer.start()

            # if no errors, remove this from the stored list of subscriptions
            for sub in self._subscriptions:
                if sub['address'] == subscription_address and sub[
                        'share'] == original_share_value:
                    self._subscriptions.remove(sub)
                    break

        except Exception as exc:
            LOG.error('Client.unsubscribe', self._id, exc)
            LOG.emit('Client.unsubscribe', self._id, ERROR, exc)
            self._emit(ERROR, exc)
            if _should_reconnect(exc):
                queue_unsubscribe()
                self._reconnect()
        LOG.exit('Client.unsubscribe', self._id, self)
        return self
