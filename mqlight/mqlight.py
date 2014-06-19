import sys
import uuid
import time
import threading
from mqlightlog import *
from proton import *
from cproton import *
from urlparse import urlparse, unquote
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

CONNECTED = Constant('connected')
CONNECTING = Constant('connecting')
DISCONNECTED = Constant('disconnected')
DISCONNECTING = Constant('disconnecting')
RECONNECTED = Constant('reconnected')
RETRYING = Constant('retrying')
ERROR = Constant('error')
MESSAGE = Constant('message')
MALFORMED = Constant('malformed')

QOS_AT_MOST_ONCE = 0
QOS_AT_LEAST_ONCE = 1
QOS_EXACTLY_ONCE = 2
PN_SND_UNSETTLED = 0
PN_SND_SETTLED = 1
PN_SND_MIXED = 2
PN_RCV_FIRST = 0
PN_RCV_SECOND = 1

STATES = {
    CONNECTED: CONNECTED,
    CONNECTING: CONNECTING,
    DISCONNECTED: DISCONNECTED,
    DISCONNECTING: DISCONNECTING,
    RETRYING: RETRYING
}

QOS = {
    QOS_AT_MOST_ONCE: QOS_AT_MOST_ONCE,
    QOS_AT_LEAST_ONCE: QOS_AT_LEAST_ONCE
}


EVENTS = {
    CONNECTED: CONNECTED,
    DISCONNECTED: DISCONNECTED,
    RECONNECTED: RECONNECTED,
    ERROR: ERROR,
    MESSAGE: MESSAGE,
    MALFORMED: MALFORMED
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


def create_client(service, client_id=None, user=None, password=None):
    """
    Constructs a new Client object in the disconnected state

        @type service: string
        @param service: Required; when an instance of String this is a URL to
        connect to. When an instance of Array this is an array of URLs to 
        connect to - each will be tried in turn until either a connection is
        successfully established to one of the URLs, or all of the URLs
        have been tried. When an instance of Function is specified for
        this argument, then function is invoked each time the client
        wants to establish a connection (e.g. for any of the state
        transitions, on the state diagram shown earlier on this page,
        which lead to the 'connected' state). The function must return
        either an instance of String or Array, which are treated in the
        manner described previously.

        @type id: string
        @param id: Optional; an identifier that is used to identify this client.
        Two different instances of Client can have the same id, however only
        one instance can be connected to the MQ Light service at a given
        moment in time.  If two instances of Client have the same id and
        both try to connect then the first instance to establish its
        connection is disconnected in favour of the second instance. If
        this property is not specified then the client will generate a
        probabilistically unique ID.

        @type user: string
        @param user: Optional; the user name to use for authentication to the
        MQ Light service.

        @type password: string
        @param password: Optional; the password to use for authentication.
    """
    log.entry('create_client', NO_CLIENT_ID)
    client = Client(service, client_id, user, password)
    log.exit('create_client', client_id, client)
    return client


def generate_service_list(service):
    """
    Function to take a single service URL, or array of service URLs, validate
    them, returning an array of service URLs
    """
    log.entry('generate_service_list', NO_CLIENT_ID)
    log.parms(NO_CLIENT_ID, 'service:', service)

    # Ensure the service is an Array
    input_service_list = []
    if not service:
        raise ValueError('service is undefined')
    elif hasattr(service, '__call__'):
        raise ValueError('service cannot be a function')
    elif isinstance(service, list):
        if len(service) == 0:
            raise ValueError('service array is empty')
        input_service_list = service
    elif isinstance(service, str):
        input_service_list = [service]
    else:
        raise ValueError('service must be a string or array type')

    # Validate the list of URLs for the service, inserting default values as
    # necessary Expected format for each URL is: amqp://host:port or
    # amqps://host:port (port is optional, defaulting to 5672)
    service_list = []
    for i in range(len(input_service_list)):
        service_url = urlparse(input_service_list[i])
        protocol = service_url.scheme
        host = service_url.hostname
        port = service_url.port
        path = service_url.path
        msg = None

        # Check we are trying to use the amqp protocol
        if (protocol is None or protocol != 'amqp' and protocol != 'amqps'):
            msg = 'Unsupported URL ' + \
                input_service_list[i] + ' specified for service. ' + \
                'Only the amqp or amqps protocol are supported.' + protocol
            raise ValueError(msg)

        # Check we have a hostname
        if host is None:
            msg = 'Unsupported URL ' + \
                input_service_list[i] + ' specified for service. ' + \
                'Must supply a hostname.'
            raise ValueError(msg)

        # Set default port if not supplied
        if port is None:
            port = 5672 if protocol == 'amqp' else 5671

        # Check for no path
        if path:
            msg = 'Unsupported URL ' + \
                input_service_list[i] + ' paths (' + path + ' ) cannot be ' + \
                'part of a service URL.'
            raise ValueError(msg)

        service_list.append(protocol + '://' + host + ':' + str(port))
    log.exit('generate_service_list', NO_CLIENT_ID, service_list)
    return service_list


def get_http_service_function(service_url):
    """
    Function to take a single HTTP URL and using the JSON retrieved from it to
    return an array of service URLs.
    """
    log.entry('get_http_service_function', NO_CLIENT_ID)
    log.parms(NO_CLIENT_ID, 'service_url:', service_url)
    if not isinstance(service_url, str):
        raise ValueError('service_url must be a string')

    def http_service_function(callback):
        log.entry('get_http_service_function.callback', NO_CLIENT_ID)
        #TODO
        log.exit('get_http_service_function.callback', NO_CLIENT_ID, None)

    log.exit('get_http_service_function', NO_CLIENT_ID, http_service_function)
    return http_service_function


class Client(object):

    """
    The L{Client} class represents an MQLight client instance
    """

    def __init__(self, service=None, client_id=None, user=None, password=None):
        log.entry('Client.constructor', NO_CLIENT_ID)
        log.parms(NO_CLIENT_ID, 'service:', service)
        log.parms(NO_CLIENT_ID, 'client_id:', client_id)
        log.parms(NO_CLIENT_ID, 'user:', user)
        log.parms(NO_CLIENT_ID, 'password:', password)

        # Ensure the service is an Array or Function
        service_list = None
        service_function = None
        if hasattr(service, '__call__'):
            service_function = service
        elif isinstance(service, str):
            service_url = urlparse(service)
            if service_url.scheme == 'http' or service_url.scheme == 'https':
                service_function = get_http_service_function

        if service_function is None:
            service_list = generate_service_list(service)

        # If client id has not been specified then generate an id
        if (client_id is None):
            client_id = 'AUTO_' + str(uuid.uuid4()).replace('-', '_')[0:7]
        log.data('client_id', client_id)
        client_id = str(client_id)

        # If the client id is incorrectly formatted then throw an error
        if len(client_id) > 48:
            msg = 'Client identifier ' + client_id + \
                ' is longer than the maximum ID length of 48'
            raise ValueError(msg)

        # If client id is not a string then throw an error
        if not isinstance(client_id, str):
            raise ValueError('Client identifier must be a string type')

        # currently client ids are restricted, reject any invalid ones
        for i in range(len(client_id)):
            if client_id[i] not in VALID_CLIENT_ID_CHARS:
                err = 'Client Identifier ' + client_id + \
                    ' contains invalid char: ' + client_id[i]
                raise ValueError(err)

        user = str(user)
        # Validate user and password parameters, when specified
        if (user and not isinstance(user, str)):
            raise ValueError('user must be a string type')

        password = str(password)
        if (password and not isinstance(password, str)):
            raise ValueError('password must be a string type')

        if ((user and password is None) or (user is None and password)):
            raise ValueError(
                'both user and password properties must be specified together')

        # Save the required data as client fields
        self._service_function = service_function
        self._service_list = service_list
        self._id = client_id

        # Initialize the messenger with auth details
        if user:
            # URI encode username and password before passing them to proton
            usr = quote(user)
            pwd = quote(password)
            self._messenger = _MQLightMessenger(self._id, usr, pwd)
        else:
            self._messenger = _MQLightMessenger(self._id)

        # Set the initial state to disconnected
        self._state = DISCONNECTED
        self._service = None
        # The first connect, set to False after connect and back to True on
        # disconnect
        self._first_connect = True

        # List of message subscriptions
        self._subscriptions = []

        # List of outstanding send operations waiting to be accepted, settled,
        # etc by the listener
        self._outstanding_sends = []

        # Heartbeat
        self._heartbeat_timeout = None

        self._callbacks = {}
        log.exit('Client.constructor', self._id, None)

    def __del__(self):
        log.entry('Client.destructor', self._id)
        if (self and self.get_state() == CONNECTED):
            self._messenger.send()
            self.disconnect()
        log.exit('Client.destructor', NO_CLIENT_ID, None)

    def connect(self, callback=None):
        """
        Attempts to connect the client to the MQ Light service - as per the
        options specified when the client object was created by the
        mqlight.createClient() method. Connects to the MQ Light service.

        This method is asynchronous and calls the optional callback function
        when:
        a) the client has successfully connected to the MQ Light service, or
        b) the client.disconnect() method has been invoked before a successful
        connection could be established, or
        c) the client could not connect to the MQ Light service. The callback
        function should accept a single argument which will be set to undefined
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
        """
        log.entry('Client.connect', self._id)

        if callback and not hasattr(callback, '__call__'):
            raise ValueError('callback must be a function')
        self._perform_connect(callback)
        log.exit('Client.connect', self._id, self)
        return self

    def _still_disconnecting(self, callback):
        """
        Waits while the client is disconnecting
        """
        log.entry('Client._still_disconnecting', self._id)
        if self.get_state() == DISCONNECTING:
            self._still_disconnecting(callback)
        else:
            self._perform_connect(callback)
        log.exit('Client._still_disconnecting', self._id, None)

    def _perform_connect(self, callback):
        """
        Performs the connection
        """
        log.entry('Client._perform_connect', self._id)
        current_state = self.get_state()
        # if we are not disconnected or disconnecting return with the client
        # object
        if (current_state is not DISCONNECTED):
            if (current_state == DISCONNECTING):
                self._still_disconnecting(callback)
            else:
                if callback:
                    log.entry('Client._perform_connect.callback', self._id)
                    callback(None)
                    log.exit(
                        'Client._perform_connect.callback',
                        self._id,
                        None)
            log.exit('Client._perform_connect', self._id, self)
            return self

        self._set_state(CONNECTING)

        # Obtain the list of services for connect and connect to one of the
        # services, retrying until a connection can be established
        if hasattr(self._service_function, '__call__'):
            def _callback(err, service):
                log.entry('Client._perform_connect._callback', self._id)
                if err:
                    callback()
                else:
                    self._service_list = generate_service_list(service)
                    self._connect_to_service(callback)
                log.exit('Client._perform_connect._callback', self._id, None)
            self._service_function(_callback)
        else:
            self._connect_to_service(callback)

        log.exit('Client._perform_connect', self._id, None)

    def _check_for_messages(self):
        """
        Function to force the client to check for messages, outputting the
        contents of any that have arrived to the client event emitter.
        """
        log.entry_often('Client._check_for_messages', self._id)
        if self.get_state() != CONNECTED or len(self._subscriptions) == 0:
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
                    for i in range(len(self._subscriptions)):
                        if self._subscriptions[i] == msg.message.address:
                            qos = self._subscriptions[i].qos
                            if qos == QOS_AT_LEAST_ONCE:
                                auto_confirm = self._subscriptions[
                                    i].auto_confirm

                        #TODO
                        def _auto_confirm(delivery):
                            log.entry(
                                'Client._check_for_messages._auto_confirm',
                                self._id)
                            log.data(self._id, 'data:', data)
                            log.exit(
                                'Client._check_for_messages._auto_confirm',
                                self._id,
                                None)

                        #TODO
                        def _no_auto_confirm(delivery, msg=None):
                            log.entry(
                                'Client._check_for_messages._no_auto_confirm',
                                self._id)
                            log.data(self._id, 'data:', data)
                            if msg:
                                self._messenger.settle(msg)
                                del msg
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
                        share = link_address.index(
                            'share:') if 'share:' in link_address else 1
                        if share == 0:
                            delivery['destination']['share'] = split[1]
                            delivery['destination']['topic_pattern'] = split[2]
                        else:
                            delivery['destination']['topic_pattern'] = split[1]

                    da = msg.message.annotations
                    malformed = {}
                    malformed['MQMD'] = {}
                    malformed['condition'] = None
                    if da is not None:
                        for an in range(len(da)):
                            if da[an] and da[an].key:
                                if da[an].key == 'x-opt-message-malformed-condition':
                                    malformed['condition'] = da[an].value
                                elif da[an].key == 'x-opt-message-malformed-description':
                                    malformed['description'] = da[an].value
                                elif da[an].key == 'x-opt-message-malformed-MQMD-CodedCharSetId':
                                    malformed['MQMD'][
                                        'CodedCharSetId'] = int(da[an].value)
                                elif da[an].key == 'x-opt-message-malformed-MQMD.Format':
                                    malformed['MQMD']['Format'] = da[an].value

                    if malformed['condition']:
                        if MALFORMED in self._callbacks:
                            delivery['malformed'] = malformed
                            self._emit(MALFORMED, msg.message.body, delivery)
                        else:
                            del msg
                            raise MQLightException(
                                'no listener for malformed event')
                    else:
                        log.emit(self._id, MESSAGE, data, delivery)
                        self._emit(MESSAGE, data, delivery)

                    if qos == QOS_AT_MOST_ONCE:
                        self._messenger.accept(msg)
                        self._messenger.settle(msg)
                        del msg
                    elif auto_confirm:
                        self._messenger.settle(msg)
                        del msg
            else:
                log.debug(self._id, 'no messages')
        except Exception as exc:
            log.error(self._id, exc)
            err = MQLightException(exc)
            self.disconnect()
            if err:
                log.emit(self._id, ERROR, err)
                self._emit(ERROR, err)

        if self.get_state() == CONNECTED:
            timer = threading.Timer(1, self._check_for_messages)
            timer.start()
        log.exit_often('Client._check_for_messages', self._id, None)

    def disconnect(self, callback=None):
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
        """
        log.entry('Client.disconnect', self._id)
        if (callback and not hasattr(callback, '__call__')):
            raise ValueError('callback must be a function')

        # just return if already disconnected or in the process of
        # disconnecting
        if (self.get_state() == DISCONNECTED or
                self.get_state() == DISCONNECTING):
            if callback:
                log.entry('Client.disconnect.callback', self._id)
                callback()
                log.exit('Client.disconnect.callback', self._id, None)
            log.exit('Client.disconnect', self._id, self)
            return self

        self._perform_disconnect(callback)
        log.exit('Client.disconnect', self._id, None)
        return self

    def _perform_disconnect(self, callback):
        """
        Performs the disconnection
        """
        log.entry('Client._perform_disconnect', self._id)
        self._set_state(DISCONNECTING)
        if self._messenger:
            self._messenger.stop()
            if self._heartbeat_timeout is not None:
                self._heartbeat_timeout.cancel()

        # Indicate that we've disconnected
        self._set_state(DISCONNECTED)
        self._first_connect = True
        log.emit(self._id, DISCONNECTED)
        self._emit(DISCONNECTED, True)

        if callback:
            log.entry('Client._perform_disconnect.callback', self._id)
            callback()
            log.exit('Client._perform_disconnect.callback', self._id, None)
        log.exit('Client._perform_disconnect', self._id, None)
        return

    def _connect_to_service(self, callback):
        """
        Function to connect to the service, tries each available service in turn.
        If none can connect it emits an error, waits and attempts to connect
        again. Callback happens once a successful connect/reconnect occurs
        """
        log.entry('Client._connect_to_service', self._id)
        if self.get_state() == DISCONNECTING or self.get_state == DISCONNECTED:
            if callback:
                log.entry('Client._connect_to_service.callback', self._id)
                callback(MQLightException('connect aborted due to disconnect'))
                log.exit('Client._connect_to_service.callback', self._id, None)

            log.exit('Client._connect_to_service', self._id, None)
            return

        error = None
        connected = False
        for service in self._service_list:
            try:
                log.data(self._id, 'attempting to connect to: ' + service)
                self._messenger.connect(service)
                error = self._messenger._last_connection_error
                if error is not None:
                    log.data(
                        self._id,
                        'failed to connect to:',
                        service,
                        ' due to error:',
                        error)
                else:
                    log.data(self._id, 'successfully connected to:', service)
                    self._service = service
                    connected = True
                    break
            except Exception as exc:
                # Should not get here
                # Means that messenger.connect has been called in an invalid
                # way
                log.ffdc('Client._connect_to_service', 'ffdc001', self._id, exc)
                raise MQLightException(exc)

        if connected:
            # Indicate that we're connected
            self._set_state(CONNECTED)
            status_connect = None
            if self._first_connect:
                status_connect = CONNECTED
                self._first_connect = False
            else:
                status_connect = RECONNECTED
            log.emit(self._id, status_connect)
            self._emit(status_connect)

            if callback:
                log.entry('Client._connect_to_service.callback', self._id)
                callback(None)
                log.exit('Client._connect_to_service.callback', self._id, None)

            # Setup heartbeat timer to ensure that while connected we send 
            # heartbeat frames to keep the connection alive, when required
            remote_idle_timeout = self._messenger.get_remote_idle_timeout(
                self._service)
            heartbeat_interval = remote_idle_timeout / \
                2 if remote_idle_timeout > 0 else remote_idle_timeout
            log.data(
                self._id,
                'set heartbeat_interval to:',
                heartbeat_interval)
            if heartbeat_interval > 0:
                def perform_heartbeat(client, heartbeat_interval):
                    log.entry(
                        'Client._connect_to_service.perform_heartbeat',
                        self._id)
                    if client._messenger:
                        client._messenger.work(0)
                        self._heartbeat_timeout = threading.Timer(
                            heartbeat_interval,
                            perform_heartbeat,
                            self,
                            heartbeat_interval)
                        self._heartbeat_timeout.start()
                    log.exit(
                        'Client._connect_to_service.perform_heartbeat',
                        self._id,
                        None)
                self._heartbeat_timeout = threading.Timer(
                    heartbeat_interval,
                    perform_heartbeat,
                    self,
                    heartbeat_interval)
                self._heartbeat_timeout.start()

        else:
            # We've tried all services without success. Pause for a while before
            # trying again
            # TODO 10 seconds is an arbitrary value, need to review
            log.emit(self._id, ERROR, error)
            self._emit(ERROR, error)
            self._set_state(RETRYING)
            log.data(self._id, 'trying to reconnect after 10 seconds')

            def retry():
                self._connect_to_service(callback)

            # If client is using service_function, re-generate the list of
            # services
            if hasattr(self._service_function, '__call__'):
                def function_callback(err, service):
                    if err:
                        log.emit(self._id, ERROR)
                        self._emit(self._id, ERROR, err)
                    else:
                        self._service_list = generate_service_list(service)
                        timer = threading.Timer(CONNECT_RETRY_INTERVAL, retry)
                        timer.start()
                self._service_function(function_callback)
            else:
                timer = threading.Timer(CONNECT_RETRY_INTERVAL, retry)
                timer.start()
        log.exit('Client._connect_to_service', self._id, None)

    def reconnect(self):
        """
        Reconnects the client to the MQ Light service, implicitly closing any
        subscriptions that the client has open. The 'reconnected' event will be
        emitted once the client has reconnected.
        """
        log.entry('Client.reconnect', self._id)
        if self.get_state() != CONNECTED:
            if self.get_state(
            ) == DISCONNECTED or self.get_state == DISCONNECTING:
                return None
            elif self.get_state() == RETRYING:
                return self
        self._set_state(RETRYING)

        # Stop the messenger to free the object then attempt a reconnect
        if self._messenger and not self._messenger.stopped:
            self._messenger.stop()
            if self._heartbeat_timeout:
                self._heartbeat_timeout.cancel()

        # Clear the subscriptions list, if the cause of the reconnect happens
        # during check for messages we need a 0 length so it will check once
        # reconnected.
        reestablish_subs_list = self._subscriptions[::-1]
        # Also clear any left over outstanding sends
        del self._outstanding_sends[:]

        def _resubscribe():
            log.entry('Client.reconnect._resubscribe', self._id)
            while len(reestablish_subs_list) > 0:
                sub = reestablish_subs_list.pop()

                def callback(err, pattern):
                    log.entry(
                        'Client.reconnect._resubscribe.callback',
                        self._id)
                    # if err we don't want to 'lose' subs in the reestablish
                    # list add to clients subscriptions list so the next
                    # reconnect picks them up
                    if err:
                        self._subscriptions.append(sub)
                        # rather than keep looping the rest of the loop to
                        # subscriptions here so we don't try another
                        # subscribe
                        self._subscriptions = reestablish_subs_list[::-1]
                    log.exit(
                        'Client.reconnect._resubscribe.callback',
                        self._id,
                        None)

                self.subscribe(
                    sub.topic_pattern,
                    sub.share,
                    sub.options,
                    callback)
            log.exit('Client.reconnect._resubscribe', self._id, None)

        # If client is using service_function, re-generate the list of services
        # TODO: merge these copy & paste
        if hasattr(self._service_function, '__call__'):
            def callback(err, service):
                if err:
                    log.emit(self._id, ERROR, err)
                    self._emit(ERROR, err)
                else:
                    self._service_list = generate_service_list(service)
                    self._connect_to_service([_resubscribe])
            self._service_function(callback)
        else:
            self._connect_to_service([_resubscribe])
        log.exit('Client.reconnect', self._id, self)
        return self

    def get_id(self):
        """
        Returns the client id
        """
        log.data('id', self._id)
        return self._id

    def get_service(self):
        """
        Returns the service if connected otherwise None
        """

        service = str(self._service) if (self.has_connected()) else None
        log.data('Client.get_service', self._id, 'service:', service)
        return service

    def get_state(self):
        """
        Returns the state of the client
        """
        log.data('Client.get_state', self._id, 'state:', self._state)
        return self._state

    def _set_state(self, state):
        """
        Sets the state of the client
        """
        log.data('Client._set_state', self._id, 'state:', state)
        if state in STATES:
            self._state = state
        else:
            raise ValueError('invalid state')

    def has_connected(self):
        """
        Returns True if the client is connected, otherwise False
        """
        log.data(self._id, 'state:', self._state)
        return self._state == CONNECTED

    def on(self, event, callback):
        """
        Registers a callback to be called when the event is emitted
        """
        log.entry('Client.on', self._id)
        if event in EVENTS:
            if event not in self._callbacks:
                self._callbacks[event] = []
            self._callbacks[event].append(callback)
        else:
            raise MQLightException('invalid event ' + str(event))
        log.exit('Client.on', self._id, None)

    def _emit(self, event, *value):
        """
        Calls all the callbacks registered with the events that is emitted
        """
        log.entry('Client._emit', self._id)
        log.parms(self._id, 'event:', event)
        log.parms(self._id, 'value:', value)
        if event in EVENTS:
            if event in self._callbacks:
                for callback in self._callbacks[event]:
                    callback(value)
        else:
            raise MQLightException('invalid event ' + str(event))
        log.exit('Client._emit', self._id, None)

    def send(self, topic, data, options=None, callback=None):
        """
        Sends a message to the MQLight service.
        """
        log.entry('Client.send', self._id)
        # Validate the passed parameters
        if topic is None:
            raise ValueError('Cannot send to undefined topic')
        else:
            topic = str(topic)
        log.parms(self._id, 'topic:', topic)

        if data is None:
            raise ValueError('Cannot send undefined data')
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
        if options:
            if hasattr(options, 'qos'):
                if options.qos in QOS:
                    qos = options.qos
                else:
                    raise ValueError(
                        'options.qos value ' +
                        options.qos +
                        ' is invalid must evaluate to 0 or 1')

        if callback:
            if not hasattr(callback, '__call__'):
                raise ValueError('callback must be a function type')
        elif qos == QOS_AT_LEAST_ONCE:
            raise ValueError(
                'callback must be specified when options.qos value of 1 ' + \
                '(at least once) is specified')

        # Ensure we have attempted a connect
        if not self.has_connected():
            raise MQLightException('not connected')

         # Send the data as a message to the specified topic
        msg = None
        try:
            msg = _MQLightMessage()
            address = self.get_service()
            if topic:
                # need to encode the topic component but / has meaning that
                # shouldn't be encoded
                topic_levels = topic.split('/')
                encoded_topic_levels = map(lambda x: quote(x), topic_levels)
                encoded_topic = '/'.join(encoded_topic_levels)
                address += '/' + encoded_topic
                msg.message.address = address

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

            # start the timer to trigger it to keep sending until msg has sent
            self._until_send_complete(msg, local_message_id, callback, options)
        except Exception as exc:
            err = MQLightException(exc)
            log.error('Client.send', self._id, err)
            if local_message_id:
                if local_message_id in self._outstanding_sends:
                    self._outstanding_sends.remove(local_message_id)

            if callback:
                log.entry('Client.send.callback', self._id)
                callback(err, msg)
                log.exit('Client.send.callback', self._id, None)

            log.emit(self._id, ERROR, err)
            self._emit(ERROR, err)
            self.reconnect()
        log.exit('Client.send', self._id, None)

    def _until_send_complete(
            self,
            msg,
            local_message_id,
            send_callback,
            options):
        """
        Setup a timer to trigger the callback once the msg has been sent, or
    immediately if no message to be sent
        """
        log.entry_often('Client._until_send_complete', self._id)

        try:
            complete = False
            err = None
            if (not self._messenger.stopped):
                status = self._messenger.status(msg)
                log.data(self._id, 'status:', status)
                if str(status) == 'ACCEPTED' or str(status) == 'SETTLED':
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
                    err = MQLightException('send failed - message was aborted')

                # if complete then invoke the callback when specified
                if complete:
                    if local_message_id in self._outstanding_sends:
                        self._outstanding_sends.remove(local_message_id)
                    if send_callback:
                        decoded = unquote(msg.message.address)
                        topic = urlparse(decoded).path[1:]
                        log.entry(
                            'Client._until_send_complete.callback',
                            self._id)
                        send_callback(err, topic, msg.message.body, options)
                        log.exit(
                            'Client._until_send_complete.callback',
                            self._id,
                            None)
                    del msg
                    log.exit_often(
                        'Client._until_send_complete',
                        self._id,
                        None)
                    return

                self._messenger.send()
                time.sleep(1)
                self._until_send_complete(
                    msg,
                    local_message_id,
                    send_callback,
                    options)
            else:
                # TODO Not sure we can actually get here (so FFDC?)
                if local_message_id:
                    if local_message_id in self._outstanding_sends:
                        self._outstanding_sends.remove(local_message_id)
                if send_callback:
                    err = MQLightException(
                        'send may have not complete due to disconnect')
                    log.entry('Client._until_send_complete.callback', self._id)
                    send_callback(err, topic, msg.message.body, options)
                    log.exit(
                        'Client._until_send_complete.callback',
                        self._id,
                        None)
            del msg
        except Exception as exc:
            log.error('Client._until_send_complete', self._id, exc)
            # Error condition so won't retry send remove from list of unsent
            if local_message_id in self._outstanding_sends:
                self._outstanding_sends.remove(local_message_id)
            self.disconnect()
            err = MQLightException(exc)
            if send_callback:
                log.entry('Client._until_send_complete.callback', self._id)
                send_callback(err, None, None)
                log.exit(
                    'Client._until_send_complete.callback',
                    self._id,
                    None)
            log.emit(self._id, ERROR, err)
            self._emit(ERROR, err)
        log.exit_often('Client._until_send_complete', self._id, None)

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
        """
        log.entry('Client.subscribe', self._id)
        if topic_pattern is None:
            raise ValueError('Cannot subscribe to an empty pattern')
        elif not isinstance(topic_pattern, str):
            raise ValueError('pattern must be a String')
        log.parms(self._id, 'topic_pattern:', topic_pattern)

        if options is None and callback is None:
            if hasattr(share, '__call__'):
                callback = share
                share = None
            elif not isinstance(share, str):
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

        if share:
            if ':' in share and share.index(':') >= 0:
                raise ValueError(
                    'share argument value ' +
                    share +
                    ' is invalid because it contains a colon character')
            share = 'share:' + share + ':'
        else:
            share = 'private:'

         # Validate the options parameter, when specified
        if options:
            if isinstance(options, 'dict'):
                log.parms(self._id, 'options:', options)
            else:
                raise ValueError('options must be a dict')

        qos = QOS_AT_MOST_ONCE
        auto_confirm = True
        if options:
            if options.qos:
                if options.qos == QOS_AT_MOST_ONCE:
                    qos = QOS_AT_MOST_ONCE
                elif options.qos == QOS_AT_LEAST_ONCE:
                    qos = QOS_AT_LEAST_ONCE
                else:
                    raise ValueError(
                        'options.qos value ' +
                        options.qos +
                        ' is invalid must evaluate to 0 or 1')
            if options.auto_confirm:
                auto_confirm = True
            elif options.auto_confirm == False:
                auto_confirm = False
            else:
                raise ValueError(
                    'options.auto_confirm value ' +
                    options.auto_confirm +
                    ' is invalid must evaluate to True or False')

        log.parms(self._id, 'share:', share)

        if callback and not hasattr(callback, '__call__'):
            raise ValueError('callback must be a function')

        # Ensure we have attempted a connect
        if not self.has_connected():
            raise MQLightException('not connected')

        # Subscribe using the specified pattern and share options
        address = self.get_service() + '/' + share + topic_pattern

        err = None
        try:
            self._messenger.subscribe(address, qos)

            # If this is the first subscription to be added, schedule a request
            # to start the polling loop to check for messages arriving
            if len(self._subscriptions) == 0:
                timer = threading.Timer(1, self._check_for_messages)
                timer.start()

            # Add address to list of subscriptions, replacing any existing
            # entry
            subscription_address = self.get_service() + '/' + topic_pattern
            for i in range(len(self._subscriptions)):
                if (self._subscriptions[i].address == subscription_address):
                    self._subscriptions.remove(subscription_address)
                    break

            sub = {
                'address': subscription_address,
                'qos': qos,
                'auto_confirm': auto_confirm
            }
            self._subscriptions.append(sub)

        except Exception as exc:
            log.error(self._id, exc)
            err = MQLightException(exc)

        if callback:
            callback(err, topic_pattern)

        if err:
            log.emit(self._id, ERROR, err)
            self._emit(ERROR, err)
            self.disconnect()
        log.exit('Client.subscribe', self._id, None)


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

    def __init__(self, name, username='', password=''):
        """
        MQLightMessenger constructor
        """
        log.entry('_MQLightMessenger.constructor', NO_CLIENT_ID)
        log.parms(NO_CLIENT_ID, 'name:', name)
        log.parms(NO_CLIENT_ID, 'username:', username)
        pwd = '*******' if password != '' else ''
        log.parms(NO_CLIENT_ID, 'password:', pwd)
        self._mng = Messenger(name)
        self._username = username
        self._password = password
        self._last_connection_error = None
        log.exit('_MQLightMessenger.constructor', NO_CLIENT_ID, None)

    def connect(self, service):
        """
        Connects to the specified service
        """
        log.entry('_MQLightMessenger.connect', NO_CLIENT_ID)
        log.parms(NO_CLIENT_ID, 'service:', service)

        self._mng.blocking = False
        self._mng.incoming_window = 2147483647
        self._mng.outgoing_window = 2147483647

        # If we have a username make sure we set a route to force auth
        index = service.find('//')
        host_and_port = service[index + 2:]
        #TODO
        if self._username != 'None':
            validation_address = 'amqp://' + self._username
            if self._password is not None:
                validation_address += ':' + self._password
            validation_address += '@' + host_and_port + '/$1'
        else:
            validation_address = service + '/$1'
        # Set the route so that when required any address starting with
        # amqp://<host>:<port> gets the supplied user and password added
        pattern = 'amqp://' + host_and_port + '/*'
        self._mng.route(pattern, validation_address)
        # Indicate that the route should be validated
        pn_messenger_set_flags(self.messenger, PN_FLAGS_CHECK_ROUTES)
        # Start the messenger. This will fail if the route is invalid
        error = self._mng.start()
        if error:
            self._last_connection_error = pn_error_text(
                pn_messenger_error(
                    self.messenger))
        else:
            self._last_connection_error = None
        log.exit('_MQLightMessenger.connect', NO_CLIENT_ID, None)

    def stop(self):
        log.entry('_MQLightMessenger.stop', NO_CLIENT_ID)
        self._mng.stop()
        log.exit('_MQLightMessenger.stop', NO_CLIENT_ID, None)

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

    def has_outgoing(self):
        """
        Returns True if there is a message on the outgoing queue
        """
        log.entry('_MQLightMessenger.has_outgoing', NO_CLIENT_ID)
        has_outgoing = False
        if self._mng:
            has_outgoing = (pn_messenger_outgoing(self.messenger) > 0)
        log.exit('_MQLightMessenger.has_outgoing', NO_CLIENT_ID, has_outgoing)
        return has_outgoing

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
        return remote_idle_timeout

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

        # XXX: for now, we're using the simplified messenger api, but long term
        # we may need to use the underlying engine directly here, or modify
        # proton
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

    def subscribe(self, source, qos):
        """
        Subscribes to a topic
        """
        log.entry('_MQLightMessenger.subscribe', NO_CLIENT_ID)
        log.parms(NO_CLIENT_ID, 'source:', source)
        log.parms(NO_CLIENT_ID, 'qos:', qos)
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
        self._mng.subscribe(source)
        self._mng.recv(-1)
        self._mng.work(50)
        log.exit('_MQLightMessenger.subscribe', NO_CLIENT_ID, None)
