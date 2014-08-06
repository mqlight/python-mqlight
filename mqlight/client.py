import sys
import uuid
import time
import threading
import os.path
import re
from mqlightlog import *
from proton import *
from cproton import *
from urlparse import urlparse, urlunparse, unquote
from urllib import quote

log = get_logger(__name__)
VALID_CLIENT_ID_CHARS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789%/._'
# The connection retry interval in seconds
CONNECT_RETRY_INTERVAL = 10


class MQLightException(Exception):
    pass


class Constant(object):

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return self.name

STARTED = Constant('started')
STARTING = Constant('starting')
STOPPED = Constant('stopped')
STOPPING = Constant('stopping')
RESTARTED = Constant('restarted')
RETRYING = Constant('retrying')
ERROR = Constant('error')
MESSAGE = Constant('message')
MALFORMED = Constant('malformed')
DRAIN = Constant('drain')

QOS_AT_MOST_ONCE = 0
QOS_AT_LEAST_ONCE = 1
QOS_EXACTLY_ONCE = 2
PN_SND_UNSETTLED = 0
PN_SND_SETTLED = 1
PN_SND_MIXED = 2
PN_RCV_FIRST = 0
PN_RCV_SECOND = 1

STATES = {
    STARTED: STARTED,
    STARTING: STARTING,
    STOPPED: STOPPED,
    STOPPING: STOPPING,
    RETRYING: RETRYING
}

QOS = {
    QOS_AT_MOST_ONCE: QOS_AT_MOST_ONCE,
    QOS_AT_LEAST_ONCE: QOS_AT_LEAST_ONCE
}


EVENTS = {
    STARTED: STARTED,
    STOPPED: STOPPED,
    RESTARTED: RESTARTED,
    ERROR: ERROR,
    MESSAGE: MESSAGE,
    MALFORMED: MALFORMED,
    DRAIN: DRAIN
}

PENDING = Constant("PENDING")
ACCEPTED = Constant("ACCEPTED")
REJECTED = Constant("REJECTED")
RELEASED = Constant("RELEASED")
ABORTED = Constant("ABORTED")
SETTLED = Constant("SETTLED")

STATUSES = {
    PN_STATUS_ABORTED: ABORTED,
    PN_STATUS_ACCEPTED: ACCEPTED,
    PN_STATUS_REJECTED: REJECTED,
    PN_STATUS_RELEASED: RELEASED,
    PN_STATUS_PENDING: PENDING,
    PN_STATUS_SETTLED: SETTLED,
    PN_STATUS_UNKNOWN: None
}


def get_http_service_function(service_url):
    """
    Function to take a single HTTP URL and using the JSON retrieved from it to
    return an array of service URLs.
    """
    log.entry('get_http_service_function', NO_CLIENT_ID)
    log.parms(NO_CLIENT_ID, 'service_url:', service_url)
    if not isinstance(service_url, str):
        error = ValueError('service_url must be a string')
        log.error('Client.constructor', NO_CLIENT_ID, error)
        raise error

    def http_service_function(callback):
        log.entry('get_http_service_function.callback', NO_CLIENT_ID)
        log.exit('get_http_service_function.callback', NO_CLIENT_ID, None)

    log.exit('get_http_service_function', NO_CLIENT_ID, http_service_function)
    return http_service_function


def get_file_service_function(file_url):
    return


def _process_queued_actions(client, err=None):
    # this set to the appropriate client via apply call in connectToService
    if client is None:
        log.entry('_process_queued_actions', 'client was not set')
        log.exit('_process_queued_actions', 'client not set returning', None)
        return

    log.entry('_process_queued_actions', client.get_id())
    log.parms(client.get_id(), 'err:', err)
    log.parms(client.get_id(), 'state:', client.get_state())
    if err is None:
        log.data(
            client.get_id(),
            'client._queued_subscriptions:',
            client._queued_subscriptions)
        while len(client._queued_subscriptions) > 0 and client.get_state() == STARTED:
            sub = client._queued_subscriptions.pop()
            if sub['noop']:
                # no-op so just trigger the callback wihtout actually
                # subscribing
                if sub['callback']:
                    sub['callback'](
                        err,
                        sub['topic_pattern'],
                        sub['original_share_value'])
            else:
                client.subscribe(
                    sub.topic_pattern,
                    sub.share,
                    sub.options,
                    sub.callback)
        log.data(
            client.get_id(),
            'client._queued_unsubscribes:',
            client._queued_unsubscribes)
        while len(client._queued_unsubscribes) > 0 and client.get_state() == STARTED:
            sub = client._queued_subscribes.pop()
            if sub['noop']:
                # no-op so just trigger the callback wihtout actually
                # unsubscribing
                if sub['callback']:
                    sub['callback'](None, sub['topic_pattern'], sub['share'])
            else:
                client.unsubscribe(
                    sub.topic_pattern,
                    sub.share,
                    sub.options,
                    sub.callback)
        log.data(
            client.get_id(),
            'client._queued_sends:',
            client._queued_sends)
        while len(client._queued_sends) > 0 and client.get_state() == STARTED:
            msg = client._queued_sends.pop()
            client.send(msg.topic, msg.data, msg.options, msg.callback)

    log.exit('_process_queued_actions', client.get_id(), None)


def _generate_service_list(service, security_options):
    """
    Function to take a single service URL, or list of service URLs, validate
    them, returning a list of service URLs
    """
    log.entry('_generate_service_list', NO_CLIENT_ID)
    log.parms(NO_CLIENT_ID, 'service:', service)

    # Ensure the service is a list
    input_service_list = []
    if not service:
        error = ValueError('service is None')
        log.error('_generate_service_list', NO_CLIENT_ID, error)
        raise error
    elif hasattr(service, '__call__'):
        error = ValueError('service cannot be a function')
        log.error('_generate_service_list', NO_CLIENT_ID, error)
        raise error
    elif isinstance(service, list):
        if len(service) == 0:
            error = ValueError('service array is empty')
            log.error('_generate_service_list', NO_CLIENT_ID, error)
            raise error
        input_service_list = service
    elif isinstance(service, str):
        input_service_list = [service]
    else:
        error = ValueError('service must be a str or list type')
        log.error('_generate_service_list', NO_CLIENT_ID, error)
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
        msg = None

        # Check for auth details
        if service_url.username:
            if service_url.password:
                auth_user = service_url.username
                auth_password = service_url.password
            else:
                error = ValueError(
                    'URLs supplied via the service property must ' +
                    'specify both a user name and a password value, ' +
                    'or omit both values')
                log.error('_generate_service_list', NO_CLIENT_ID, error)
                raise error

            if security_options['user'] != auth_user:
                error = ValueError(
                    'User name supplied as user property ' +
                    'security_options[\'user\']  does not match ' +
                    'username supplied via a URL passed via the ' +
                    'service property ' + auth_user)
                log.error('_generate_service_list', NO_CLIENT_ID, error)
                raise error
            if security_options['password'] != auth_password:
                error = ValueError(
                    'Password name supplied as password property ' +
                    'security_options[\'password\']  does not match ' +
                    'password supplied via a URL passed via the ' +
                    'service property ' + auth_password)
                log.error('_generate_service_list', NO_CLIENT_ID, error)
                raise error
            if i == 0:
                security_options['url_user'] = auth_user
                security_options['url_password'] = auth_password

        # Check whatever URL user names / passwords are present this
        # time through the loop - match the ones set on security_options
        #  by the first pass through the loop.
        if i > 0:
            if security_options['url_user'] != auth_user:
                error = ValueError(
                    'URLs supplied via the service property contain ' +
                    'inconsistent user names')
                log.error('_generateServiceList', NO_CLIENT_ID, error)
                raise error
            elif security_options['url_password'] != auth_password:
                error = ValueError(
                    'URLs supplied via the service property contain ' +
                    'inconsistent password values')
                log.error('_generateServiceList', NO_CLIENT_ID, error)
                raise error

        # Check we are trying to use the amqp protocol
        if protocol not in ('amqp', 'amqps'):
            error = ValueError(
                'Unsupported URL ' + input_service_list[i] +
                ' specified for service. ' +
                'Only the amqp or amqps protocol are supported.' +
                protocol)
            log.error('_generate_service_list', NO_CLIENT_ID, error)
            raise error

        # Check we have a hostname
        host = service_url.hostname
        if host is None:
            error = ValueError(
                'Unsupported URL ' + input_service_list[i] +
                ' specified for service. Must supply a hostname.')
            log.error('_generate_service_list', NO_CLIENT_ID, error)
            raise error

        # Set default port if not supplied
        port = service_url.port
        if port is None:
            port = 5672 if protocol == 'amqp' else 5671

        # Check for no path
        path = service_url.path
        if path and path != '/':
            error = ValueError(
                'Unsupported URL ' + input_service_list[i] +
                ' paths (' + path + ' ) cannot be part of a service ' +
                'URL.')
            log.error('_generate_service_list', NO_CLIENT_ID, error)
            raise error

        service_list.append(protocol + '://' + host + ':' + str(port))
    log.exit('_generate_service_list', NO_CLIENT_ID, service_list)
    return service_list


def _stop_messenger(client, stop_processing_callback, callback=None):
    log.entry('_stop_messenger', client.get_id())
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
    log.exit('_stop_messenger', client.get_id(), None)


class Client(object):

    """
    The Client class represents an MQLight client instance
    """

    def __init__(
            self,
            service=None,
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
            ValueError: if any of the passed arguments is invalid
        """
        log.entry('Client.constructor', NO_CLIENT_ID)
        log.parms(NO_CLIENT_ID, 'service:', service)
        log.parms(NO_CLIENT_ID, 'client_id:', client_id)
        log.parms(NO_CLIENT_ID, 'security_options:', security_options)

        # Ensure the service is a list or function
        service_list = None
        service_function = None
        if hasattr(service, '__call__'):
            service_function = service
        elif isinstance(service, str):
            service_url = urlparse(service)
            if service_url.scheme in ('http', 'https'):
                service_function = get_http_service_function
            elif service_url.scheme == 'file':
                if len(service_url.hostname) > 0 and service_url.hostname != 'localhost':
                    error = ValueError(
                        'service contains unsupported file URI of ' + service +
                        ', only file:///path or file://localhost/path are ' +
                        ' supported.')
                    log.error('Client.constructor', NO_CLIENT_ID, error)
                    raise error
                service_function = get_file_service_function(service_url.path)

        # If client id has not been specified then generate an id
        if client_id is None:
            client_id = 'AUTO_' + str(uuid.uuid4()).replace('-', '_')[0:7]
        log.data('client_id', client_id)
        client_id = str(client_id)

        # If the client id is incorrectly formatted then throw an error
        if len(client_id) > 48:
            error = ValueError(
                'Client identifier ' + client_id +
                ' is longer than the maximum ID length of 48')
            log.error('Client.constructor', NO_CLIENT_ID, error)
            raise error

        # If client id is not a string then throw an error
        if not isinstance(client_id, str):
            error = ValueError('Client identifier must be a str')
            log.error('Client.constructor', NO_CLIENT_ID, error)
            raise error

        # currently client ids are restricted, reject any invalid ones
        for i in range(len(client_id)):
            if client_id[i] not in VALID_CLIENT_ID_CHARS:
                error = ValueError(
                    'Client Identifier ' + client_id +
                    ' contains invalid char: ' + client_id[i])
                log.error('Client.constructor', NO_CLIENT_ID, error)
                raise error

        # User/password must either both be present, or both be absent.
        user = None
        password = None
        if security_options:
            if isinstance(security_options, dict):
                # User/password must either both be present, or both be absent.
                if ('property_user' in security_options and 'property_password' not in security_options) or (
                        'property_user' not in security_options and 'property_password' in security_options):
                    error = ValueError(
                        'both user and password properties must be ' +
                        'specified together')
                    log.error('Client.constructor', NO_CLIENT_ID, error)
                    raise error
            else:
                error = ValueError('security_options must be a dict')
                log.error('Client.constructor', NO_CLIENT_ID, error)
                raise error
        else:
            security_options = {}

        # Validate the ssl security options
        if 'ssl_verify_name' in security_options:
            if security_options['ssl_verify_name'] not in [True, False]:
                error = ValueError(
                    'ssl_verify_name value ' +
                    security_options['ssl_verify_name'] + ' is invalid. ' +
                    'Must evaluate to True of False')
                log.error('Client.constructor', NO_CLIENT_ID, error)
                raise error
        if 'ssl_trust_certificate' in security_options:
            if not isinstance(security_options['ssl_trust_certificate'], str):
                error = ValueError(
                    'ssl_trust_certificate value ' +
                    security_options['ssl_trust_certificate'] +
                    ' is invalid. Must be a string')
                log.error('Client.constructor', NO_CLIENT_ID, error)
                raise error
            if not os.path.isfile(security_options['ssl_trust_certificate']):
                error = ValueError(
                    'The file specified for ssl_trust_certificate is not a ' +
                    'regular file')
                log.error('Client.constructor', NO_CLIENT_ID, error)
                raise error

        # Save the required data as client fields
        self._service_function = service_function
        self._service_list = service_list
        self._id = client_id
        self._security_options = security_options

        self._messenger = _MQLightMessenger(self._id)

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

        # Heartbeat
        self._heartbeat_timeout = None

        self._callbacks = {}

        # No drain event initially required
        self._drain_event_required = False

        if service_function is None:
            service_list = _generate_service_list(
                service,
                self._security_options)

        def connect_callback(err):
            if callback:
                callback(err, self)
        self._perform_connect(connect_callback, service, True)

        log.exit('Client.constructor', self._id, None)

    def _perform_connect(self, callback, service, new_client):
        """
        Performs the connection
        """
        log.entry('Client._perform_connect', self._id)
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
                        log.entry('Client._still_disconnecting', client._id)
                        if client.get_state() == STOPPING:
                            _still_disconnecting(client, callback)
                        else:
                            client._perform_connect(client, service, callback)
                        log.exit(
                            'Client._still_disconnecting',
                            client._id,
                            None)
                    _still_disconnecting(self, callback)
                else:
                    if callback:
                        log.entry(
                            'Client._perform_connect.callback',
                            self._id)
                        callback(None)
                        log.exit(
                            'Client._perform_connect.callback',
                            self._id,
                            None)
                log.exit('Client._perform_connect', self._id, self)
                return self

            if self.get_state() == STOPPED:
                self._set_state(STARTING)

            # If the messenger is not already stopped then something has gone
            # wrong
            if self._messenger and not self._messenger.stopped:
                error = MQLightException('messenger is not stopped')
                log.error('Client._perform_connect', error)
                raise error
        else:
            self._set_state(STARTING)

        # Obtain the list of services for connect and connect to one of the
        # services, retrying until a connection can be established
        if hasattr(self._service_function, '__call__'):
            def _callback(err, service):
                log.entry(
                    'Client._perform_connect._callback',
                    self._id)
                if err:
                    callback()
                else:
                    self._service_list = _generate_service_list(
                        service,
                        self._security_options)
                    self._connect_to_service(callback)
                log.exit(
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
                log.error('Client._perform_connect', exc)
                callback(exc)
        log.exit('Client._perform_connect', self._id, None)
        return

    def __del__(self):
        log.entry('Client.destructor', self._id)
        if (self and self.get_state() == STARTED):
            self._messenger.send()
            self.stop()
        log.exit('Client.destructor', NO_CLIENT_ID, None)

    def __enter__(self):
        log.entry('Client.__enter__', self._id)
        self.start()
        while self.get_state() != STARTED:
            # Wait for the connection to be established
            pass
        log.exit('Client.__enter__', self._id, self)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        log.entry('Client.__exit__', self._id)
        self.stop()
        log.exit('Client.__exit__', self._id, None)

    def __iadd__(self, args):
        log.entry('Client.__iadd__', self._id)
        if isinstance(args, tuple) and len(args) == 2:
            log.parms(self._id, 'event:', args[0])
            log.parms(self._id, 'callback:', args[1])
            self.add_listener(args[0], args[1])
        else:
            raise ValueError('args must be a tuple (event, callback)')
        log.exit('Client.__iadd__', self._id, self)
        return self

    def __isub__(self, args):
        log.entry('Client.__isub__', self._id)
        if isinstance(args, tuple) and len(args) == 2:
            log.parms(self._id, 'event:', args[0])
            log.parms(self._id, 'callback:', args[1])
            self.del_listener(args[0], args[1])
        else:
            raise ValueError('args must be a tuple (event, callback)')
        log.exit('Client.__isub__', self._id, self)
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
            ValueError: if callback is not a function
        """
        log.entry('Client.start', self._id)

        if callback and not hasattr(callback, '__call__'):
            error = ValueError('callback must be a function')
            log.error('Client.start', self._id, error)
            raise error
        self._perform_connect(callback, self._service, False)
        log.exit('Client.start', self._id, self)
        return self

    def _check_for_messages(self):
        """
        Function to force the client to check for messages, outputting the
        contents of any that have arrived to the client event emitter.
        """
        log.entry_often('Client._check_for_messages', self._id)
        if self.get_state() != STARTED or len(
                self._subscriptions) == 0 or MESSAGE not in self._callbacks:
            log.exit_often('Client._check_for_messages', self._id, None)
            return
        try:
            messages = self._messenger.recv(50)
            if messages and len(messages) > 0:
                log.debug(self._id, 'received ' +
                          str(len(messages)) +
                          ' messages')
                for message in range(len(messages)):
                    log.debug(self._id, 'processing message ' + str(message))
                    msg = messages[message]
                    data = msg.message.body

                    topic = urlparse(msg.message.address).path[1:]
                    auto_confirm = True
                    qos = QOS_AT_MOST_ONCE

                    def filter_function(item):
                        # 1 added to length to account for the / we add
                        address_no_service = item['address'][
                            len(self._service) + 1:]
                        # Possible to have 2 matches work out whether this is
                        # for a share or private topic
                        if item[
                                'share'] is None and 'private:' in msg.link_address:
                            link_no_priv_share = msg.link_address[8:]
                            if address_no_service == link_no_priv_share:
                                return True
                        elif item['share'] is not None and 'share:' in msg.link_address:
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
                        # shouldn't get here
                        error = MQLightException(
                            'No listener for this message ' +
                            data +
                            ' going to address: ' +
                            msg.message.address)
                        raise error

                    def _auto_confirm(delivery):
                        log.entry(
                            'Client._check_for_messages._auto_confirm',
                            self._id)
                        log.data(self._id, 'data:', data)
                        log.exit(
                            'Client._check_for_messages._auto_confirm',
                            self._id,
                            None)

                    def _no_auto_confirm(delivery, msg=None):
                        log.entry(
                            'Client._check_for_messages._no_auto_confirm',
                            self._id)
                        log.data(self._id, 'data:', data)
                        subscription = matched_subs[0]
                        if msg:
                            self._messenger.settle(msg)
                            subscription['unconfirmed'] -= 1
                            subscription['confirmed'] += 1
                            log.data(
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
                            if (available / subscription['confirmed']) <= 1.25 or (
                                    subscription['unconfirmed'] == 0 and subscription['confirmed'] > 0):
                                self._messenger.flow(
                                    self._service +
                                    '/' +
                                    msg.link_address,
                                    subscription['confirmed'])
                                subscription['confirmed'] = 0
                            #del msg
                        log.exit(
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
                                'content_type': msg.message.content_type
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

                    annots = msg.message.annotations
                    malformed = {
                        'MQMD': {},
                        'condition': None
                    }
                    if annots is not None:
                        for i in range(len(annots)):
                            if annots[i] and annots[i].key:
                                if annots[
                                        i].key == 'x-opt-message-malformed-condition':
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
                            self._emit(MALFORMED, msg.message.body, delivery)
                        else:
                            #del msg
                            raise MQLightException(
                                'no listener for malformed event')
                    else:
                        log.emit(
                            'Client._check_for_messages',
                            self._id,
                            MESSAGE,
                            data,
                            delivery)
                        self._emit(MESSAGE, data, delivery)

                    if qos == QOS_AT_MOST_ONCE:
                        self._messenger.accept(msg)
                    elif qos == QOS_AT_MOST_ONCE or auto_confirm:
                        self._messenger.settle(msg)
                        matched_subs[0]['unconfirmed'] -= 1
                        matched_subs[0]['confirmed'] += 1
                        log.data(
                            self._id,
                            '[credit, unconfirmed, confirmed]:',
                            '[' +
                            matched_subs[0]['credit'] +
                            ', ' +
                            matched_subs[0]['unconfirmed'] +
                            ', ' +
                            matched_subs[0]['confirmed'] +
                            ']')
                        # Ask to flow more messages if >= 80% of available
                        # credit (e.g. not including unconfirmed messages)
                        # has been used or we have just confirmed everything
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
                        #del msg
            else:
                log.debug(self._id, 'no messages')
        except Exception as exc:
            log.error('Client._check_for_messages', self._id, exc)
            err = MQLightException(exc)
            self.stop()
            if err:
                log.emit('Client._check_for_messages', self._id, ERROR, err)
                self._emit(ERROR, err)

        if self.get_state() == STARTED:
            timer = threading.Timer(1, self._check_for_messages)
            timer.start()
        log.exit_often('Client._check_for_messages', self._id, None)

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
        log.entry('Client.stop', self._id)
        if (callback and not hasattr(callback, '__call__')):
            raise ValueError('callback must be a function')

        # just return if already disconnected or in the process of
        # disconnecting
        if self.is_stopped():
            if callback:
                log.entry('Client.stop.callback', self._id)
                callback()
                log.exit('Client.stop.callback', self._id, None)
            log.exit('Client.stop', self._id, self)
            return self

        self._perform_disconnect(callback)
        log.exit('Client.stop', self._id, None)
        return self

    def _perform_disconnect(self, callback):
        """
        Performs the disconnection
        """
        log.entry('Client._perform_disconnect', self._id)
        self._set_state(STOPPING)

        # Only disconnect when all outstanding send operations are complete
        if len(self._outstanding_sends) == 0:
            def stop_processing(client, callback):
                log.entry(
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
                        log.entry(
                            'Client._perform_disconnect.next_tick',
                            self._id)
                        msg['callback'](
                            MQLightException('send aborted due to disconnect'))
                        log.exit(
                            'Client._perform_disconnect.next_tick',
                            self._id,
                            None)
                    timer = threading.Timer(1, next_tick)
                    timer.start()

                # Clear the active subscriptions list as we were asked to
                # disconnect
                log.data(self._id, 'self._subscriptions:', self._subscriptions)
                self._subscriptions = self._subscriptions[::-1]

                # Indicate that we've disconnected
                client._set_state(STOPPED)
                log.emit(
                    'Client._perform_disconnect.stop_processing',
                    self._id,
                    STOPPED)
                self._first_start = True
                self._emit(STOPPED, True)

                if callback:
                    log.entry('Client._perform_disconnect.callback', self._id)
                    callback()
                    log.exit(
                        'Client._perform_disconnect.callback',
                        self._id,
                        None)
                log.exit(
                    'Client._perform_disconnect.stop_processing',
                    self._id,
                    None)
            _stop_messenger(self, stop_processing, callback)

        log.exit('Client._perform_disconnect', self._id, None)
        return

    def _connect_to_service(self, callback):
        """
        Function to connect to the service, tries each available service in turn
        If none can connect it emits an error, waits and attempts to connect
        again. Callback happens once a successful connect/reconnect occurs
        """
        log.entry('Client._connect_to_service', self._id)
        if self.get_state() == STOPPED or self.get_state() == STOPPING:
            if callback:
                log.entry('Client._connect_to_service.callback', self._id)
                callback(MQLightException('connect aborted due to disconnect'))
                log.exit('Client._connect_to_service.callback', self._id, None)
            log.exit('Client._connect_to_service', self._id, None)
            return
        error = None
        connected = False

        # Try each service in turn until we can successfully connect, or exhaust
        # the list
        for service in self._service_list:
            try:
                # check if we will be providing authentication information
                auth = None
                if 'url_user' in self._security_options:
                    auth = quote(str(self._security_options['url_user']))
                    auth += ':'
                    auth += quote(str(self._security_options['url_password']))
                    auth += '@'
                elif 'user' in self._security_options:
                    auth = quote(str(self._security_options['user']))
                    auth += ':'
                    auth += quote(str(self._security_options['password']))
                    auth += '@'
                log_url = None
                # reparse the service url to prepend authentication information
                # back on as required
                if auth:
                    service_url = urlparse(service)
                    service = service_url.scheme + \
                        '://' + auth + service_url.hostname
                    log_url = service_url.scheme + '://' + \
                        re.sub('/:[^\/:]+@/g', '********@', auth) + \
                        service_url.hostname + ':' + service_url.port
                else:
                    log_url = service
                log.data(self._id, 'attempting to connect to: ' + service)

                connect_url = urlparse(service)
                # Remove any path elements from the URL (for ipv6 which appends
                # /)
                if connect_url.path:
                    href_length = len(service) - len(connect_url.path)
                    connect_service = service[0, href_length]
                else:
                    connect_service = service

                ssl_trust_certificate = self._security_options[
                    'ssl_trust_certificate'] if 'ssl_trust_certificate' in self._security_options else None
                ssl_verify_name = self._security_options[
                    'ssl_verify_name'] if 'verify_name' in self._security_options else None

                rc = self._messenger.connect(
                    urlparse(connect_service),
                    ssl_trust_certificate,
                    ssl_verify_name)
                if rc:
                    error = MQLightException(
                        self._messenger.get_last_error_text())
                    log.data(
                        self._id,
                        'failed to connect to:',
                        log_url,
                        ' due to error:',
                        error)
                    raise error
                else:
                    log.data(self._id, 'successfully connected to:', log_url)
                    self._service = service
                    connected = True
                    break
            except Exception as exc:
                # Should never get here, as it means that messenger.connect has
                # been called in an invalid way, so FFDC
                log.ffdc(
                    'Client._connect_to_service',
                    'ffdc001',
                    self._id,
                    exc)
                raise MQLightException(exc)

        # If we've successfully connected then we're done, otherwise we'll
        # retry
        if connected:
            # Indicate that we're connected
            self._set_state(STARTED)
            event_to_emit = None
            if self._first_start:
                event_to_emit = STARTED
                self._first_start = False
                # could be queued actions so need to process those here.
                # On reconnect this would be done via the callback we set,
                # first connect its the users callback so won't process
                # anything
                log.data(self._id, 'first start since being stopped')
                _process_queued_actions(self)
            else:
                event_to_emit = RESTARTED

            def next_tick():
                log.emit(
                    'Client._connect_to_service.next_tick',
                    self._id,
                    event_to_emit)
                self._emit(event_to_emit, None)
            timer = threading.Timer(1, next_tick)
            timer.start()

            if callback:
                log.entry('Client._connect_to_service.callback2', self._id)
                callback(None)
                log.exit(
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
            log.data(self._id, 'heartbeat_interval: ', heartbeat_interval)
            if heartbeat_interval > 0:
                def perform_heartbeat(heartbeat_interval):
                    log.entry(
                        'Client._connect_to_service.perform_heartbeat',
                        self._id)
                    if self._messenger:
                        self._messenger.work(0)
                        self._heartbeat_timeout = threading.Timer(
                            heartbeat_interval,
                            perform_heartbeat,
                            [heartbeat_interval])
                        self._heartbeat_timeout.start()
                    log.exit(
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
                log.entry_often('Client._connect_to_service.retry', self._id)
                if not self.is_stopped():
                    self._perform_connect(callback, self._service, False)
                log.exit_often(
                    'Client._connect_to_service.retry',
                    self._id,
                    None)
            # TODO 10 seconds is an arbitrary value, need to review if this is
            # appropriate. Timeout should be adjusted based on reconnect algo.
            timer = threading.Timer(CONNECT_RETRY_INTERVAL, retry)
            timer.start()

            # XXX: should we even emit an error in this case? we're going to
            # retry
            log.emit('Client._connect_to_service', self._id, ERROR, error)
            self._emit(ERROR, error)
        log.exit('Client._connect_to_service', self._id, None)

    def _reconnect(self):
        """
        Reconnects the client to the MQ Light service, implicitly closing any
        subscriptions that the client has open. The 'reconnected' event will be
        emitted once the client has reconnected.

        Returns:
            The instance of the client if reconnect succeeded otherwise None
        """
        log.entry('Client._reconnect', self._id)
        if self.get_state() != STARTED:
            if self.is_stopped():
                return None
            elif self.get_state() == RETRYING:
                return self
        self._set_state(RETRYING)

        # Stop the messenger to free the object then attempt a reconnect
        def stop_processing(client, callback=None):
            log.entry('Client.reconnect.stop_processing', client._id)

            if client._heartbeat_timeout:
                client._heartbeat_timeout.cancel()

            # clear the subscriptions list, if the cause of the reconnect
            # happens during check for messages we need a 0 length so it will
            # check once reconnected.
            client._queued_subscriptions = client._queued_subscriptions[::-1]
            # also clear any left over outstanding sends
            client._outstanding_sends = client._outstanding_sends[::-1]
            client._perform_connect(
                _process_queued_actions,
                self._service,
                False)

            log.exit('Client.reconnect.stop_processing', client._id, None)

        _stop_messenger(self, stop_processing)
        log.exit('Client._reconnect', self._id, self)
        return self

    def get_id(self):
        """
        Returns:
            The client id
        """
        log.data('id', self._id)
        return self._id

    def get_service(self):
        """
        Returns:
            The service if connected otherwise None
        """
        if self.get_state() == STARTED:
            log.data(self._id, 'service:', self._service)
            return self._service
        else:
            log.data(self._id, 'service: None')
            return None

    def get_state(self):
        """
        Returns:
            The state of the client
        """
        log.data(self._id, 'state:', self._state)
        return self._state

    def _set_state(self, state):
        """
        Sets the state of the client
        """
        log.data(self._id, 'state:', state)
        if state in STATES:
            self._state = state
        else:
            raise ValueError('invalid state')

    def is_stopped(self):
        """
        Returns:
            True if in disconnected or disconnecting state, otherwise False
        """
        log.data(self._id, 'state:', self._state)
        return self._state in (STOPPED, STOPPING)

    def add_listener(self, event, callback):
        """
        Registers a callback to be called when the event is emitted

        Args:
            event: event the callback is registered on
            callback: function to call when the event is triggered
        Raises:
            ValueError: if the event is invalid or callback is not a function
        """
        log.entry('Client.add_listener', self._id)
        if event in EVENTS:
            if hasattr(callback, '__call__'):
                if event not in self._callbacks:
                    self._callbacks[event] = []
                self._callbacks[event].append(callback)
            else:
                raise ValueError('callback must be a function')
        else:
            raise ValueError('invalid event ' + str(event))
        log.exit('Client.add_listener', self._id, None)

    def del_listener(self, event, callback):
        """
        Removes a callback for the specified event

        Args:
            event: event the callback is registered on
            callback: callback function to remove
        Raises:
            MQLightException: if event is invalid
        """
        log.entry('Client.del_listener', self._id)
        if event in EVENTS:
            if event in self._callbacks and callback in self._callbacks[event]:
                self._callbacks[event].remove(callback)
        else:
            raise MQLightException('invalid event ' + str(event))
        log.exit('Client.del_listener', self._id, None)

    def _emit(self, event, *args, **kwargs):
        """
        Calls all the callbacks registered with the events that is emitted
        """
        log.entry('Client._emit', self._id)
        log.parms(self._id, 'event:', event)
        log.parms(self._id, 'args:', args)
        log.parms(self._id, 'kwargs:', kwargs)
        if event in EVENTS:
            if event in self._callbacks:
                for callback in self._callbacks[event]:
                    callback(*args, **kwargs)
        else:
            raise MQLightException('invalid event ' + str(event))
        log.exit('Client._emit', self._id, None)

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
            ValueError: if any of the arguments is invalid
            MQLightException: if the client is disconnected
        """
        log.entry('Client.send', self._id)
        next_message = False
        # Validate the passed parameters
        if topic is None or len(topic) == 0:
            raise ValueError('Cannot send to None topic')
        else:
            topic = str(topic)
        log.parms(self._id, 'topic:', topic)

        if data is None:
            raise ValueError('Cannot send no data')
        elif hasattr(data, '__call__'):
            raise ValueError('Cannot send a function')
        log.parms(self._id, 'data:', data)

        # Validate the remaining optional parameters, assigning local variables
        # to the appropriate parameter
        if options is not None and callback is None:
            if hasattr(options, '__call__'):
                callback = options
                options = None

        if options is not None:
            if isinstance(options, dict):
                log.parms(self._id, 'options:', options)
            else:
                raise ValueError('options must be an dict type')

        qos = QOS_AT_MOST_ONCE
        ttl = None
        if options:
            if 'qos' in options:
                if options['qos'] in QOS:
                    qos = options['qos']
                else:
                    raise ValueError(
                        'options.qos value ' + str(options['qos']) +
                        ' is invalid must evaluate to 0 or 1')
            if 'ttl' in options:
                try:
                    ttl = int(options['ttl'])
                    if ttl < 0:
                        raise ValueError()
                    if ttl > 4294967295:
                        # Cap at max AMQP value for TTL (2^32-1)
                        ttl = 4294967295
                except Exception as err:
                    raise ValueError(
                        'options.ttl value ' + str(options['ttl']) +
                        ' is invalid must be an unsigned integer number')

        if callback:
            if not hasattr(callback, '__call__'):
                raise ValueError('callback must be a function type')
        elif qos == QOS_AT_LEAST_ONCE:
            raise ValueError(
                'callback must be specified when options.qos value of 1 ' +
                '(at least once) is specified')

        # Ensure we have attempted a connect
        if self.is_stopped():
            raise MQLightException('not started')

        # Ensure we are not retrying otherwise queue message and return
        if self.get_state() == RETRYING or self.get_state() == STARTING:
            self._queued_sends.append({
                'topic': topic,
                'data': data,
                'options': options,
                'callback': callback
            })
            self._drain_event_required = True
            log.exit('Client.send', self._id, False)
            return False

        # Send the data as a message to the specified topic
        msg = None
        local_message_id = None
        try:
            msg = _MQLightMessage()
            address = self.get_service()
            if topic:
                # need to encode the topic component but / has meaning that
                # shouldn't be encoded
                topic_levels = topic.split('/')
                encoded_topic_levels = [quote(x) for x in topic_levels]
                encoded_topic = '/'.join(encoded_topic_levels)
                address += '/' + encoded_topic
                msg.message.address = address
            if ttl:
                msg.message.ttl = ttl

            if isinstance(data, str):
                msg.message.body = unicode(data)
                msg.message.content_type = 'text/plain'
            else:
                msg.message.body = data
                msg.message.content_type = 'application/octet-stream'

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
                log.entry('Client.send.until_send_complete', self._id)
                log.parms(self._id, 'msg:', msg)
                log.parms(self._id, 'local_message_id:', local_message_id)
                log.parms(self._id, 'send_callback:', send_callback)
                try:
                    complete = False
                    err = None
                    if (not self._messenger.stopped):
                        status = self._messenger.status(msg)
                        log.data(self._id, 'status:', status)
                        if str(status) in ('ACCEPTED', 'SETTLED'):
                            self._messenger.settle(msg)
                            complete = True
                        elif str(status) == 'REJECTED':
                            complete = True
                            err = MQLightException(
                                'send failed - message was rejected')
                        elif str(status) == 'RELEASED':
                            complete = True
                            err = MQLightException(
                                'send failed - message was released')
                        elif str(status) == 'MODIFIED':
                            complete = True
                            err = MQLightException(
                                'send failed - message was modified')
                        elif str(status) == 'ABORTED':
                            complete = True
                            err = MQLightException(
                                'send failed - message was aborted')

                        # if complete then do final processing of this message.
                        if complete:
                            if local_message_id in self._outstanding_sends:
                                self._outstanding_sends.remove(
                                    local_message_id)

                            # If previously send() returned false and now the
                            # backlog of messages is cleared, emit a drain
                            # event
                            log.data(
                                self._id,
                                'outstandingSends:',
                                len(self._outstanding_sends))
                            if self._drain_event_required and len(
                                    self._outstanding_sends) <= 1:
                                log.emit(
                                    'Client.send.until_send_complete',
                                    self._id,
                                    DRAIN)
                                self._drain_event_required = False
                                self._emit(DRAIN)

                            # invoke the callback, if specified
                            if send_callback:
                                decoded = unquote(msg.message.address)
                                topic = urlparse(decoded).path[1:]
                                log.entry(
                                    'Client.send.until_send_complete.callback1',
                                    self._id)
                                send_callback(
                                    err,
                                    topic,
                                    msg.message.body,
                                    options)
                                log.exit(
                                    'Client.send.until_send_complete.callback1',
                                    self._id,
                                    None)
                            #del msg
                            log.exit(
                                'Client.send.until_send_complete',
                                self._id,
                                None)
                            return

                        # message not send yet, so check again ina  second or
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
                            err = MQLightException(
                                'send may have not complete due to disconnect')
                            log.entry(
                                'Client.send.until_send_complete.callback2',
                                self._id)
                            send_callback(
                                err,
                                topic,
                                msg.message.body,
                                options)
                            log.exit(
                                'Client.send.until_send_complete.callback2',
                                self._id,
                                None)
                        #del msg
                        log.exit(
                            'Client.send.until_send_complete',
                            self._id,
                            None)
                        return

                except Exception as exc:
                    log.error('Client.send.until_send_complete', self._id, exc)
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
                            log.entry(
                                'Client.send.until_send_complete.next_tick',
                                self._id)
                            send_callback(err, None, None, None)
                            log.exit(
                                'Client.send.until_send_complete.next_tick',
                                self._id,
                                None)
                        if exc:
                            log.emit(
                                'Client.send.until_send_complete.next_tick',
                                self._id,
                                ERROR,
                                err)
                            self._emit(ERROR, err)
                        self._reconnect()

                    timer = threading.Timer(1, next_tick)
                    timer.start()
                log.exit_often(
                    'Client.send.until_send_complete',
                    self._id,
                    None)
            # start the timer to trigger it to keep sending until msg has sent
            until_send_complete(msg, local_message_id, callback)

            # If we have a backlog of messages, then record the need to emit a
            # drain event later to indicate the backlog has been cleared
            log.data(
                self._id, 'outstandingSends:', len(
                    self._outstanding_sends))
            if len(self._outstanding_sends) <= 1:
                next_message = True
            else:
                self._drain_event_required = True
        except Exception as exc:
            err = MQLightException(exc)
            log.error('Client.send', self._id, err)
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
                        log.entry('Client.send.next_tick', self._id)
                        callback(err, msg)
                        log.exit('Client.send.next_tick', self._id, None)

                log.emit('Client.send.next_tick', self._id, ERROR, err)
                self._emit(ERROR, err)
                self._reconnect()
            timer = threading.Timer(1, next_tick)
            timer.start()
        log.exit('Client.send', self._id, next_message)
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
            ValueError: if any argument is invalid
            MQLightException: if the client is disconnected
        """
        log.entry('Client.subscribe', self._id)
        if topic_pattern is None or topic_pattern == '':
            raise ValueError('Cannot subscribe to an empty pattern')

        topic_pattern = str(topic_pattern)
        log.parms(self._id, 'topic_pattern:', topic_pattern)

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

        log.parms(self._id, 'callback:', callback)
        original_share_value = share
        if share:
            share = str(share)
            if ':' in share:
                raise ValueError(
                    'share argument value ' + share +
                    ' is invalid because it contains a colon character')
            share = 'share:' + share + ':'
        else:
            share = 'private:'
        log.parms(self._id, 'share:', share)

        # Validate the options parameter, when specified
        if options:
            if isinstance(options, dict):
                log.parms(self._id, 'options:', options)
            else:
                raise ValueError('options must be a dict')
        qos = QOS_AT_MOST_ONCE
        auto_confirm = True
        ttl = 0
        credit = 1024
        if options:
            if 'qos' in options:
                if options['qos'] in (QOS_AT_MOST_ONCE, QOS_AT_LEAST_ONCE):
                    qos = options['qos']
                else:
                    raise ValueError(
                        'options[\'qos\'] value ' + str(options['qos']) +
                        ' is invalid must evaluate to 0 or 1')
            if 'auto_confirm' in options:
                if options['auto_confirm'] in (True, False):
                    auto_confirm = options['auto_confirm']
                else:
                    raise ValueError(
                        'options[\'auto_confirm\'] value ' +
                        str(options['auto_confirm']) +
                        ' is invalid must evaluate to True or False')
            if 'ttl' in options:
                try:
                    ttl = int(options['ttl'])
                    if ttl < 0:
                        raise ValueError()
                except Exception as err:
                    raise ValueError(
                        'options[\'ttl value\'] ' + str(options['ttl']) +
                        ' is invalid must be an unsigned integer number')
            if 'credit' in options:
                try:
                    credit = int(options['credit'])
                    if credit < 0:
                        raise ValueError()
                except Exception as err:
                    raise ValueError(
                        'options[\'credit\'] value ' + str(options['credit']) +
                        ' is invalid must be an unsigned integer number')

        log.parms(self._id, 'share:', share)

        if callback and not hasattr(callback, '__call__'):
            raise ValueError('callback must be a function')

        # Ensure we have attempted a connect
        if self.is_stopped():
            raise MQLightException('not connected')

        # Subscribe using the specified pattern and share options
        address = self.get_service() + '/' + share + topic_pattern
        subscription_address = self.get_service() + '/' + topic_pattern

        # If client is in the retrying state, then queue this subscribe request
        if self.get_state() == RETRYING or self.get_state() == STARTING:
            log.data(
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
            log.exit('Client.subscribe', self._id, self)
            return self

        err = None
        try:
            self._messenger.subscribe(address, qos, ttl, credit)
        except Exception as exc:
            log.error('Client.subscribe', self._id, exc)
            err = MQLightException(exc)

        if callback:
            callback(err, topic_pattern, original_share_value)

        if err:
            log.emit('Client.subscribe', self._id, ERROR, err)
            self._emit(ERROR, err)

            log.data(self._id, 'queued subscription and calling reconnect')
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
            # if no errors, add this to the stored list of subscriptions,
            # replacing any existing entry
            is_first_sub = (len(self._subscriptions) == 0)
            log.data(self._id, 'is_first_sub:', is_first_sub)

            for sub in self._subscriptions:
                if sub['address'] == subscription_address and sub[
                        'share'] == original_share_value:
                    self._subscriptions.remove(sub)
                    break

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
                timer = threading.Timer(0, self._check_for_messages)
                timer.start()

        log.exit('Client.subscribe', self._id, self)
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
            ValueError If the topic pattern parameter is undefined.
        """
        log.entry('Client.unsubscribe', self._id)
        log.parms(self._id, 'topic_pattern:', topic_pattern)

        if topic_pattern is None:
            error = ValueError('You must specify a topic_pattern argument')
            log.error('Client.unsubscribe', self._id, error)
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
                error = ValueError(
                    'share argument value ' +
                    share +
                    ' is invalid because it contains a colon (:) character')
                log.error('Client.unsubscribe', self._id, error)
                raise error
            share = 'share:' + share + ':'
        else:
            share = 'private:'

        log.parms(self._id, 'share:', share)

        # Validate the options parameter, when specified
        if options:
            if isinstance(options, dict):
                log.parms(self._id, 'options:', options)
            else:
                error = ValueError(
                    'options must be an object type not a ' +
                    type(options) +
                    ')')
                log.error('Client.unsubscribe', self._id, error)
                raise error

        ttl = None
        if options:
            if 'ttl' in options:
                try:
                    ttl = int(options['ttl'])
                    if ttl < 0:
                        raise ValueError()
                except Exception as err:
                    raise ValueError(
                        'options[\'ttl value\'] ' + str(options['ttl']) +
                        ' is invalid must be an unsigned integer number')

        if callback and not hasattr(callback, '__call__'):
            error = ValueError('callback must be a function type')
            log.error('Client.unsubscribe', self._id, error)
            raise error

        # Ensure we have attempted a connect
        if self.is_stopped():
            error = ValueError('not started')
            log.error('Client.unsubscribe', self._id, error)
            raise error

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
            # TODO define a proper type for this Error (e.g. StateError)
            error = MQLightException('not subscribed to ' + address)
            log.error('Client.unsubscribe', self._id, error)
            raise error

        def queue_unsubscribe():
            # check if there's a queued subscribe for the same topic, if so mark that
            # as a no-op operation, so the callback is called but a no-op takes place
            # on reconnection
            noop = False
            for sub in self._queued_subscriptions:
                if sub['address'] == subscription_address and sub[
                        'share'] == original_share_value:
                    noop = True

            # queue unsubscribe request as appropriate
            if noop:
                log.data(
                    self._id,
                    'client already had a queued subscribe ' +
                    'request for this address, so marked that as a noop and ' +
                    'will queue this unsubscribe request as a noop too')
            else:
                log.data(self._id, 'client waiting for connection so ' +
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
            log.data(self._id, 'client still in the process of connecting ' +
                     'so queueing the unsubscribe request')
            queue_unsubscribe()
            log.exit('Client.unsubscribe', self._id, self)
            return self

        # unsubscribe using the specified topic pattern and share options
        error = None
        try:
            self._messenger.unsubscribe(address, ttl)

            if callback:
                def next_tick():
                    log.entry('Client.unsubscribe.callback', self._id)
                    callback(None)
                    log.exit('Client.unsubscribe.callback', self._id, None)
                timer = threading.Timer(1, next_tick)
                timer.start()

            # if no errors, remove this from the stored list of subscriptions
            for sub in self._subscriptions:
                if sub['address'] == subscription_address and sub[
                        'share'] == original_share_value:
                    self._subscriptions.remove(sub)
                    break

        except Exception as exc:
            log.error('Client.unsubscribe', self._id, exc)
            log.emit('Client.unsubscribe', self._id, ERROR, exc)
            self._emit('error', exc)
            queue_unsubscribe()
            self._reconnect()
        log.exit('Client.unsubscribe', self._id, self)
        return self


class _MQLightMessage(object):

    """
    Wrapper for the Proton Message class
    """

    def __init__(self, message=None):
        """
        MQLight Message constructor
        """
        log.entry('_MQLightMessage.constructor', NO_CLIENT_ID)
        if message:
            self._msg = message
        else:
            self._msg = Message()
        self._tracker = None
        self._link_address = None
        log.exit('_MQLightMessage.constructor', NO_CLIENT_ID, None)

    def _set_tracker(self, tracker):
        """
        Sets the tracker
        """
        self._tracker = tracker

    def _get_tracker(self):
        """
        Returns the tracker
        """
        return self._tracker

    tracker = property(_get_tracker, _set_tracker)

    def _set_link_address(self, link_address):
        """
        Sets the link address
        """
        self._link_address = link_address

    def _get_link_address(self):
        """
        Returns the link address
        """
        return self._link_address

    link_address = property(_get_link_address, _set_link_address)

    def _set_time_to_live(self, ttl):
        """
        Sets the ttl
        """
        pn_message_set_ttl(self._msg._msg, ttl)

    def _get_time_to_live(self):
        """
        Returns the ttl
        """
        return pn_message_get_ttl(self._msg._msg)

    ttl = property(_get_time_to_live, _set_time_to_live)

    def _get_message(self):
        """
        Returns the Proton Message object
        """
        return self._msg

    message = property(_get_message)


class _MQLightMessenger(object):

    """
    Wrapper for the Proton Messenger class
    """

    def __init__(self, name, username=None, password=None):
        """
        MQLightMessenger constructor
        """
        log.entry('_MQLightMessenger.constructor', NO_CLIENT_ID)
        log.parms(NO_CLIENT_ID, 'name:', name)
        log.parms(NO_CLIENT_ID, 'username:', username)
        pwd = '*******' if password is not None else None
        log.parms(NO_CLIENT_ID, 'password:', pwd)
        self._mng = Messenger(name)
        self._username = username
        self._password = password
        self._last_connection_error = None
        log.exit('_MQLightMessenger.constructor', NO_CLIENT_ID, None)

    def connect(self, service, ssl_trust_certificate, ssl_verify_name):
        """
        Connects to the specified service
        """
        log.entry('_MQLightMessenger.connect', NO_CLIENT_ID)
        log.parms(NO_CLIENT_ID, 'service:', service)
        log.parms(
            NO_CLIENT_ID,
            'ssl_trust_certificate:',
            ssl_trust_certificate)
        log.parms(NO_CLIENT_ID, 'ssl_verify_name:', ssl_verify_name)

        if ssl_trust_certificate:
            if not os.path.isfile(ssl_trust_certificate):
                self._last_connection_error = 'The file specified for ssl_trust_certificate ' + \
                    ssl_trust_certificate + ' does not exist or is not accessible'
                return -1
        ssl_mode = PN_SSL_VERIFY_NULL
        if ssl_verify_name is not None:
            if ssl_verify_name:
                ssl_mode = PN_SSL_VERIFY_PEER_NAME
            else:
                ssl_mode = PN_SSL_VERIFY_PEER

        self._mng.blocking = False
        self._mng.incoming_window = 2147483647
        self._mng.outgoing_window = 2147483647

        # Set the SSL trust certificate when required
        error = pn_messenger_set_trusted_certificates(
            self.messenger,
            ssl_trust_certificate)
        if error:
            err = MQLightException('Failed to set trusted certificates')
            raise err
        if ssl_mode != PN_SSL_VERIFY_NULL:
            error = pn_messenger_set_ssl_peer_authentication_mode(
                self.messenger,
                ssl_mode)
            if error:
                err = MQLightException(
                    'Failed to set SSL peer authentication mode')
                raise err

        # Set the route and enable PN_FLAGS_CHECK_ROUTES so that messenger
        # confirms that it can connect at startup.
        address = urlunparse(service)
        url_protocol = service.scheme
        port_and_host = service.netloc
        pattern = url_protocol + '://' + port_and_host
        validation_address = address + '/$1'
        error = pn_messenger_route(self.messenger, pattern, validation_address)
        if error:
            err = MQLightException('Failed to set messenger route')
            raise err
        # Indicate that the route should be validated
        pn_messenger_set_flags(self.messenger, PN_FLAGS_CHECK_ROUTES)
        # Start the messenger. This will fail if the route is invalid
        error = pn_messenger_start(self.messenger)
        if error:
            self._last_connection_error = pn_error_text(
                pn_messenger_error(
                    self.messenger))
        else:
            self._last_connection_error = None
        log.exit('_MQLightMessenger.connect', NO_CLIENT_ID, None)

    def stop(self):
        """
        Calls stop() on the proton Messenger
        """
        log.entry('_MQLightMessenger.stop', NO_CLIENT_ID)
        self._mng.stop()
        stopped = pn_messenger_stopped(self.messenger)
        log.exit('_MQLightMessenger.stop', NO_CLIENT_ID, stopped)
        return stopped

    def _get_outgoing_tracker(self):
        """
        Returns the tracker for the message most recently given to put
        """
        return pn_messenger_outgoing_tracker(self.messenger)

    outgoing_tracker = property(_get_outgoing_tracker)

    def _get_messenger(self):
        """
        Returns the Proton Messenger object
        """
        return self._mng._mng

    messenger = property(_get_messenger)

    def _is_stopped(self):
        """
        Returns true if the messenger if currently stopped
        """
        return self._mng.stopped

    stopped = property(_is_stopped)

    def set_snd_settle_mode(self, mode):
        """
        Sets the settle mode for sending messages
        """
        log.entry('_MQLightMessenger.set_snd_settle_mode', NO_CLIENT_ID)
        log.parms(NO_CLIENT_ID, 'mode:', mode)
        pn_messenger_set_snd_settle_mode(self.messenger, mode)
        log.exit('_MQLightMessenger.set_snd_settle_mode', NO_CLIENT_ID, None)

    def set_rcv_settle_mode(self, mode):
        """
        Sets the settle mode for receiving messages
        """
        log.entry('_MQLightMessenger.set_rcv_settle_mode', NO_CLIENT_ID)
        log.parms(NO_CLIENT_ID, 'mode:', mode)
        pn_messenger_set_rcv_settle_mode(self.messenger, mode)
        log.exit('_MQLightMessenger.set_rcv_settle_mode', NO_CLIENT_ID, None)

    def get_last_error_text(self):
        """
        Returns the last error the Messenger threw
        """
        log.entry('_MQLightMessenger.get_last_error_text', NO_CLIENT_ID)
        error_text = None
        if self._mng:
            error_text = pn_error_text(pn_messenger_error(self.messenger))
        else:
            error_text = self._last_connection_error
        log.exit(
            '_MQLightMessenger.get_last_error_text',
            NO_CLIENT_ID,
            error_text)
        return error_text

    def get_remote_idle_timeout(self, address):
        """
        Returns the idle timeout of the Messenger
        """
        log.entry('_MQLightMessenger.get_remote_idle_timeout', NO_CLIENT_ID)
        log.parms(NO_CLIENT_ID, 'address:', address)
        if not self._mng:
            raise MQLightException('not connected')
        remote_idle_timeout = pn_messenger_get_remote_idle_timeout(
            self.messenger,
            address)
        log.exit(
            '_MQLightMessenger.get_remote_idle_timeout',
            NO_CLIENT_ID,
            remote_idle_timeout)
        return remote_idle_timeout / 1000

    def work(self, timeout):
        """
        Sends or receives any outstanding messages
        """
        log.entry('_MQLightMessenger.work', NO_CLIENT_ID)
        log.parms(NO_CLIENT_ID, 'timeout:', timeout)
        if not self._mng:
            raise MQLightException('not connected')
        status = pn_messenger_work(self.messenger, timeout)
        log.exit(
            '_MQLightMessenger.work',
            NO_CLIENT_ID,
            status)

    def flow(self, address, credit):
        # throw exception if not connected
        if not self.messenger:
            raise MQLightException('Not connected')

        # Find link based on address, and flow link credit.
        link = pn_messenger_get_link(self.messenger, address, False)
        if link:
            pn_link_flow(link, credit)
        else:
            log.parms(NO_CLIENT_ID, 'link:', None)

    def put(self, msg, qos):
        """
        Puts a message on the outgoing queue
        """
        log.entry('_MQLightMessenger.put', NO_CLIENT_ID)
        log.parms(NO_CLIENT_ID, 'msg:', msg)
        log.parms(NO_CLIENT_ID, 'qos:', qos)
        # Set the required QoS, by setting the sender settler mode to settled
        # (QoS = AMO) or unsettled (QoS = ALO). Note that the receiver settler
        # mode is always set to first, as the MQ Light listener will
        # negotiate down any receiver settler mode to first.
        if qos == QOS_AT_MOST_ONCE:
            self.set_snd_settle_mode(PN_SND_SETTLED)
            self.set_rcv_settle_mode(PN_RCV_FIRST)
        elif qos == QOS_AT_LEAST_ONCE:
            self.set_snd_settle_mode(PN_SND_UNSETTLED)
            self.set_snd_settle_mode(PN_RCV_FIRST)
        else:
            raise ValueError('invalid qos argument')

        log.data(NO_CLIENT_ID, 'msg:', msg.message)
        self._mng.put(msg.message)
        tracker = self.outgoing_tracker
        log.data(NO_CLIENT_ID, 'tracker:', tracker)
        msg.tracker = tracker
        log.exit('_MQLightMessenger.put', NO_CLIENT_ID, None)

    def send(self):
        """
        Sends the messages on the outgoing queue
        """
        log.entry('_MQLightMessenger.send', NO_CLIENT_ID)
        self._mng.send()
        error = pn_messenger_errno(self.messenger)
        if error:
            raise MQLightException(
                pn_error_text(
                    pn_messenger_error(
                        self.messenger)))
        self._mng.work(50)
        error = pn_messenger_errno(self.messenger)
        if error:
            raise MQLightException(
                pn_error_text(
                    pn_messenger_error(
                        self.messenger)))
        log.exit('_MQLightMessenger.send', NO_CLIENT_ID, None)

    def recv(self, timeout):
        """
        Retrieves messages from the incoming queue
        """
        log.entry('_MQLightMessenger.recv', NO_CLIENT_ID)
        if timeout is None:
            raise ValueError('missing required timeout argument')
        log.parms(NO_CLIENT_ID, 'timeout:', timeout)
        pn_messenger_name(self.messenger)
        self._mng.work(timeout)
        messages = []
        count = pn_messenger_incoming(self.messenger)
        log.data(NO_CLIENT_ID, 'messages count:', str(count))
        message = Message()
        while pn_messenger_incoming(self.messenger) > 0:
            self._mng.get(message)
            msg = _MQLightMessage(message)
            tracker = pn_messenger_incoming_tracker(self.messenger)
            msg.tracker = tracker
            link_address = pn_messenger_tracker_link(self.messenger, tracker)
            if link_address:
                msg.link_address = pn_terminus_get_address(
                    pn_link_remote_target(link_address))
            messages.append(msg)
            pn_messenger_accept(self.messenger, tracker, 0)
        log.exit('_MQLightMessenger.recv', NO_CLIENT_ID, messages)
        return messages

    def settle(self, message):
        """
        Settles a message
        """
        log.entry('_MQLightMessenger.settle', NO_CLIENT_ID)
        log.parms(NO_CLIENT_ID, 'message:', message)
        tracker = message.tracker
        self._mng.settle(tracker)
        log.exit('_MQLightMessenger.settle', NO_CLIENT_ID, None)

    def accept(self, message):
        """
        Accepts a message
        """
        log.entry('_MQLightMessenger.accept', NO_CLIENT_ID)
        log.parms(NO_CLIENT_ID, 'message:', message)
        tracker = message.tracker
        pn_messenger_accept(self.messenger, tracker, 0)
        log.exit('_MQLightMessenger.accept', NO_CLIENT_ID, None)

    def status(self, message):
        """
        Get the status of a message
        """
        log.entry('_MQLightMessenger.status', NO_CLIENT_ID)
        log.parms(NO_CLIENT_ID, 'message:', message)
        tracker = message.tracker
        disp = pn_messenger_status(self.messenger, tracker)
        status = STATUSES.get(disp, disp)
        log.exit('_MQLightMessenger.status', NO_CLIENT_ID, status)
        return status

    def subscribe(self, address, qos, ttl, credit):
        """
        Subscribes to a topic
        """
        log.entry('_MQLightMessenger.subscribe', NO_CLIENT_ID)
        log.parms(NO_CLIENT_ID, 'address:', address)
        log.parms(NO_CLIENT_ID, 'qos:', qos)
        log.parms(NO_CLIENT_ID, 'ttl:', ttl)
        log.parms(NO_CLIENT_ID, 'credit:', credit)
        # Set the required QoS, by setting the sender settler mode to settled
        # (QoS = AMO) or unsettled (QoS = ALO).
        # Note that our API client implementation will always specify a value
        # of first - meaning "The Receiver will spontaneously settle all
        # incoming transfers" - this equates to a maximum QoS of "at least once
        # delivery".
        if qos == 0:
            self.set_snd_settle_mode(PN_SND_SETTLED)
            self.set_rcv_settle_mode(PN_RCV_FIRST)
        elif qos == 1:
            self.set_snd_settle_mode(PN_SND_UNSETTLED)
            self.set_rcv_settle_mode(PN_RCV_FIRST)
        else:
            raise ValueError('invalid qos')
        pn_messenger_subscribe_ttl(self.messenger, address, ttl)
        pn_messenger_recv(self.messenger, -2)
        error = pn_messenger_errno(self.messenger)
        if error:
            # throw Error if error from messenger
            raise MQLightException(
                pn_error_text(
                    pn_messenger_error(
                        self.messenger)))
        link = pn_messenger_get_link(self.messenger, address, False)
        if not link:
            # throw Error if unable to find a matching Link
            raise MQLightException("unable to locate link for " + address)
        # XXX: this is less than ideal, but as a temporary fix we will block
        #      until we've received the @attach response back from the server
        #      and the link is marked as active. Ideally we should be passing
        #      callbacks around between JS and C++, so will fix better later
        while not (pn_link_state(link) & PN_REMOTE_ACTIVE):
            pn_messenger_work(self.messenger, 50)
            error = pn_messenger_errno(self.messenger)
            if error:
                text = pn_error_text(pn_messenger_error(self.messenger))
                # throw Error if error from messenger
                raise MQLightException(text)

        if credit > 0:
            pn_link_flow(link, credit)

        log.exit('_MQLightMessenger.subscribe', NO_CLIENT_ID, True)
        return True

    def unsubscribe(self, address, ttl):
        # find link based on address
        link = pn_messenger_get_link(self.messenger, address, False)

        if link is None:
            # throw Error if unable to find a matching Link
            raise MQLightException('unable to locate link for ' + address)

        if ttl == 0:
            pn_terminus_set_expiry_policy(pn_link_target(link), PN_LINK_CLOSE)
            pn_terminus_set_expiry_policy(pn_link_source(link), PN_LINK_CLOSE)
            log.parms(NO_CLIENT_ID, 'ttl:', ttl)
            pn_terminus_set_timeout(pn_link_target(link), ttl)
            pn_terminus_set_timeout(pn_link_source(link), ttl)

        pn_link_close(link)
        pn_messenger_work(self.messenger, 50)
        error = pn_messenger_errno(self.messenger)

        if error:
            text = pn_error_text(pn_messenger_error(self.messenger))
            # throw Error if error from messenger
            raise MQLightException(text)

        log.exit('_MQLightMessenger.Unsubscribe', NO_CLIENT_ID, True)
        return True
