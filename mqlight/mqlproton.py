# <copyright
# notice="lm-source-program"
# pids="5725-P60"
# years="2013,2015"
# crc="3568777996" >
# Licensed Materials - Property of IBM
#
# 5725-P60
#
# (C) Copyright IBM Corp. 2013, 2015
#
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
# </copyright>
from __future__ import division, absolute_import
import socket
import ssl
import select
import threading
import Queue
from . import cproton
from .exceptions import MQLightError, SecurityError, ReplacedError, \
    NetworkError, InvalidArgumentError, NotPermittedError
from .logging import get_logger, NO_CLIENT_ID
try:
    from urlparse import urlunparse
except ImportError:
    from urllib.parse import urlunparse

LOG = get_logger(__name__)

STATUSES = ['UNKNOWN', 'PENDING', 'ACCEPTED', 'REJECTED', 'RELEASED',
            'MODIFIED', 'ABORTED', 'SETTLED']

QOS_AT_MOST_ONCE = 0
QOS_AT_LEAST_ONCE = 1


class _MQLightMessage(object):
    """
    Wrapper for the Proton Message class
    """

    def __init__(self, message=None):
        """
        MQLight Message constructor
        """
        LOG.entry('_MQLightMessage.constructor', NO_CLIENT_ID)
        if message:
            LOG.parms(NO_CLIENT_ID, 'message:', message)
            self._msg = message
            self._body = None
            self._body = self._get_body()
        else:
            self._msg = cproton.pn_message()
            self._body = None
        self._tracker = None
        self._link_address = None
        self.connection_id = None
        LOG.exit('_MQLightMessage.constructor', NO_CLIENT_ID, None)

    def _set_body(self, value):
        """
        Handles body data type and encoding
        """
        LOG.entry('_MQLightMessage._set_body', NO_CLIENT_ID)
        LOG.parms(NO_CLIENT_ID, 'value:', value)
        if self._msg:
            body = cproton.pn_message_body(self._msg)
            if isinstance(value, str) or isinstance(value, unicode):
                LOG.data(NO_CLIENT_ID, 'setting the body format as text')
                cproton.pn_data_put_string(body, str(value))
            else:
                LOG.data(NO_CLIENT_ID, 'setting the body format as data')
                cproton.pn_data_put_binary(body, ''.join(chr(i % 256) for i in
                                                         value))
            self._body = self._get_body()
            LOG.data(NO_CLIENT_ID, 'body:', self._body)
        LOG.exit('_MQLightMessage._set_body', NO_CLIENT_ID, None)

    def _get_body(self):
        """
        Handles body data type and encoding
        """
        LOG.entry('_MQLightMessage._get_body', NO_CLIENT_ID)
        result = None
        if self._msg:
            if self._body is None:
                body = cproton.pn_message_body(self._msg)
                # inspect data to see if we have PN_STRING data
                cproton.pn_data_next(body)
                data_type = cproton.pn_data_type(body)
                LOG.data(NO_CLIENT_ID, 'data_type:', data_type)
                if data_type == cproton.PN_STRING:
                    result = cproton.pn_data_get_string(body).decode('utf8')
                else:
                    result = [ord(str(byte)) for byte in
                              list(cproton.pn_data_get_binary(body))]
            else:
                result = self._body
        LOG.exit('_MQLightMessage._get_body', NO_CLIENT_ID, result)
        return result

    body = property(_get_body, _set_body)

    def _get_delivery_annotations(self):
        """
        Gets the message delivery annotations
        """
        LOG.entry('_MQLightMessage._get_delivery_annotations', NO_CLIENT_ID)
        # instructions === delivery annotations
        anno = cproton.pn_message_instructions(self.message)

        # Count the number of delivery annotations that we are interested in
        # returning
        lval = cproton.pn_data_next(anno)     # Move to Map
        elements = 0
        result = []
        # Check it actually is a Map
        if lval and cproton.pn_data_type(anno) == cproton.PN_MAP:
            if lval:
                lval = cproton.pn_data_enter(anno)    # Enter the Map
            if lval:
                lval = cproton.pn_data_next(anno)     # Position on 1st map key
            if lval:
                while True:
                    if cproton.pn_data_type(anno) == cproton.PN_SYMBOL:
                        if cproton.pn_data_next(anno):
                            if cproton.pn_data_type(anno) in (
                                    cproton.PN_SYMBOL,
                                    cproton.PN_STRING,
                                    cproton.PN_INT):
                                elements += 1
                            else:
                                break
                            if not cproton.pn_data_next(anno):
                                break
                        else:
                            break
            cproton.pn_data_rewind(anno)

            # Return early if there are no (interesting) delivery annotations
            if elements == 0:
                LOG.exit(
                    '_MQLightMessage._get_delivery_annotations',
                    NO_CLIENT_ID,
                    result)
                return result

            cproton.pn_data_next(anno)    # Move to Map
            cproton.pn_data_enter(anno)   # Enter the Map
            cproton.pn_data_next(anno)    # Position on first map key

            # Build an array of objects, where each object has the following
            # four properties:
            #   key        : the key of the delivery annotation entry
            #   key_type   : the type of the delivery annotation key (always
            #   'symbol')
            #   value      : the value of the delivery annotation entry
            #   value_type : the type of the delivery annotation value
            #   ('symbol' ,'string', or 'int32')
            while True:
                if cproton.pn_data_type(anno) == cproton.PN_SYMBOL:
                    key = cproton.pn_data_get_symbol(anno).start

                    if cproton.pn_data_next(anno):
                        value = None
                        value_type = None
                        data_type = cproton.pn_data_type(anno)
                        add_entry = False

                        if data_type == cproton.PN_SYMBOL:
                            add_entry = True
                            value_type = 'symbol'
                            value = cproton.pn_data_get_symbol(anno).start
                        elif data_type == cproton.PN_STRING:
                            add_entry = True
                            value_type = 'string'
                            value = cproton.pn_data_get_string(anno).start
                        elif data_type == cproton.PN_INT:
                            add_entry = True
                            value_type = 'int32'
                            value = cproton.pn_data_get_atom(anno).u.as_int

                        if add_entry:
                            # e.g. { 'key': 'xopt-blah', 'key_type': 'symbol',
                            # 'value': 'blahblah', 'value_type': 'string' }
                            item = {
                                'key': key,
                                'key_type': 'symbol',
                                'value': value,
                                'value_type': value_type
                            }
                            result.append(item)

                        if not cproton.pn_data_next(anno):
                            break
                    else:
                        break

                cproton.pn_data_rewind(anno)
        LOG.exit(
            '_MQLightMessage._get_delivery_annotations',
            NO_CLIENT_ID,
            result)
        return result

    annotations = property(_get_delivery_annotations)

    def set_content_type(self, content_type):
        """
        Sets the message content type
        """
        cproton.pn_message_set_content_type(self.message, content_type)

    def _get_content_type(self):
        """
        Gets the message content type
        """
        content_type = cproton.pn_message_get_content_type(self.message)
        return content_type

    content_type = property(_get_content_type, set_content_type)

    def _set_ttl(self, ttl):
        """
        Sets the message time to live
        """
        cproton.pn_message_set_ttl(self.message, ttl)

    def _get_ttl(self):
        """
        Gets the message time to live
        """
        ttl = cproton.pn_message_get_ttl(self.message)
        return ttl

    ttl = property(_get_ttl, _set_ttl)

    def _set_address(self, address):
        """
        Sets the address
        """
        cproton.pn_message_set_address(self.message, address)

    def _get_address(self):
        """
        Gets the address
        """
        addr = cproton.pn_message_get_address(self.message)
        return addr

    address = property(_get_address, _set_address)

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
        cproton.pn_message_set_ttl(self.message, ttl)

    def _get_time_to_live(self):
        """
        Returns the ttl
        """
        return cproton.pn_message_get_ttl(self.message)

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

    def __init__(self, name):
        """
        MQLightMessenger constructor
        """
        LOG.entry('_MQLightMessenger.constructor', NO_CLIENT_ID)
        LOG.parms(NO_CLIENT_ID, 'name:', name)
        self.messenger = None
        self.connection = None
        self.sasl_outcome = cproton.PN_SASL_NONE
        self._name = name
        self._lock = threading.RLock()
        LOG.exit('_MQLightMessenger.constructor', NO_CLIENT_ID, None)

    @staticmethod
    def _raise_error(text):
        """
        Parses an error message from messenger and raises the corresponding
        Error
        """
        if 'sasl ' in text or 'SSL ' in text:
            raise SecurityError(text)

        if '_Takeover' in text:
            raise ReplacedError(text)

        if '_InvalidSourceTimeout' in text:
            raise NotPermittedError(text)

        raise NetworkError(text)

    def connect(self, service):
        """
        Connects to the specified service
        """
        LOG.entry('_MQLightMessenger.connect', NO_CLIENT_ID)

        # If the proton messenger already exists and has been stopped then free
        # it so that we can recreate a new instance.  This situation can arise
        # if the messenger link is closed by the remote end instead of a call
        # to stop()
        if self.messenger:
            stopped = cproton.pn_messenger_stopped(self.messenger)
            if stopped:
                self.messenger = None
                self.connection = None

        # throw exception if already connected
        if self.messenger:
            raise NetworkError('Already connected')

        # Create the messenger object and update the name in case messenger has
        # changed it
        self.messenger = cproton.pn_messenger(self._name)
        self._name = cproton.pn_messenger_name(self.messenger)

        cproton.pn_messenger_set_blocking(self.messenger, False)
        cproton.pn_messenger_set_incoming_window(self.messenger, 2147483647)
        cproton.pn_messenger_set_outgoing_window(self.messenger, 2147483647)

        # Set the route and enable PN_FLAGS_CHECK_ROUTES so that messenger
        # confirms that it can connect at startup.
        address = urlunparse(service)
        url_protocol = service.scheme
        host_and_port = service.hostname
        if service.port:
            host_and_port += ':{0}'.format(service.port)
        pattern = '{0}://{1}/*'.format(url_protocol, host_and_port)
        validation_address = '{0}/$1'.format(address)
        LOG.data(NO_CLIENT_ID, 'pattern:', pattern)
        error = cproton.pn_messenger_route(
            self.messenger, pattern, validation_address)
        LOG.data(NO_CLIENT_ID, 'pn_messenger_route:', error)
        if error:
            self.messenger = None
            raise MQLightError('Failed to set messenger route')
        # Indicate that the route should be validated
        if cproton.pn_messenger_set_flags(
                self.messenger, cproton.PN_FLAGS_CHECK_ROUTES
                | cproton.PN_FLAGS_ALLOW_INSECURE_MECHS):
            self.messenger = None
            raise TypeError('Invalid set flags call')

        # Indicate that an external socket is in use
        if cproton.pn_messenger_set_external_socket(self.messenger):
            self.messenger = None
            raise TypeError('Failed to set external socket')

        # Start the messenger. This will fail if the route is invalid
        error = cproton.pn_messenger_start(self.messenger)
        LOG.data(NO_CLIENT_ID, 'pn_messenger_start:', error)
        if error:
            text = cproton.pn_error_text(
                cproton.pn_messenger_error(self.messenger))
            self.messenger = None
            _MQLightMessenger._raise_error(text)

        # Get the proton connection by resolving the route
        self.connection = cproton.pn_messenger_resolve(self.messenger, pattern)
        LOG.data(NO_CLIENT_ID, 'connection:', self.connection)
        if not self.connection:
            self.messenger = None
            raise NetworkError('Unable to resolve connection')
        LOG.exit('_MQLightMessenger.connect', NO_CLIENT_ID, None)

    def stop(self, sock):
        """
        Calls stop() on the proton Messenger
        """
        LOG.entry('_MQLightMessenger.stop', NO_CLIENT_ID)
        # if already stopped then simply return True
        if self.messenger is None:
            LOG.exit('_MQLightMessenger.stop', NO_CLIENT_ID, True)
            return True
        cproton.pn_messenger_stop(self.messenger)
        self._write(sock, False)
        stopped = cproton.pn_messenger_stopped(self.messenger)
        if stopped:
            cproton.pn_messenger_free(self.messenger)
            self.messenger = None
            self.connection = None
        LOG.exit('_MQLightMessenger.stop', NO_CLIENT_ID, stopped)
        return stopped

    def _is_stopped(self):
        """
        Returns True if the messenger is currently stopped
        """
        LOG.entry('_MQLightMessenger._is_stopped', NO_CLIENT_ID)
        if self.messenger is not None:
            state = cproton.pn_messenger_stopped(self.messenger)
        else:
            state = True
        LOG.exit('_MQLightMessenger._is_stopped', NO_CLIENT_ID, state)
        return state
    stopped = property(_is_stopped)

    def started(self):
        """
        Returns True if the messenger is currently started
        """
        LOG.entry('_MQLightMessenger.started', NO_CLIENT_ID)
        if self.messenger is not None:
            if self.connection:
                transport = cproton.pn_connection_transport(self.connection)
                self._update_sasl_outcome(transport)

            error = cproton.pn_messenger_errno(self.messenger)

            if error:
                text = cproton.pn_error_text(
                    cproton.pn_messenger_error(
                        self.messenger))
                _MQLightMessenger._raise_error(text)
            # FIXME: these should really come from pn_messenger_error
            elif self.sasl_outcome == cproton.PN_SASL_AUTH:
                _MQLightMessenger._raise_error('sasl authentication failed')
            elif self.sasl_outcome > cproton.PN_SASL_AUTH:
                _MQLightMessenger._raise_error('sasl negotiation failed')

            started = cproton.pn_messenger_started(self.messenger) and \
                self.sasl_outcome != cproton.PN_SASL_NONE

        else:
            started = False
        LOG.exit('_MQLightMessenger.started', NO_CLIENT_ID, started)
        return started

    def set_snd_settle_mode(self, mode):
        """
        Sets the settle mode for sending messages
        """
        LOG.entry('_MQLightMessenger.set_snd_settle_mode', NO_CLIENT_ID)
        LOG.parms(NO_CLIENT_ID, 'mode:', mode)
        cproton.pn_messenger_set_snd_settle_mode(self.messenger, mode)
        LOG.exit('_MQLightMessenger.set_snd_settle_mode', NO_CLIENT_ID, None)

    def set_rcv_settle_mode(self, mode):
        """
        Sets the settle mode for receiving messages
        """
        LOG.entry('_MQLightMessenger.set_rcv_settle_mode', NO_CLIENT_ID)
        LOG.parms(NO_CLIENT_ID, 'mode:', mode)
        cproton.pn_messenger_set_rcv_settle_mode(self.messenger, mode)
        LOG.exit('_MQLightMessenger.set_rcv_settle_mode', NO_CLIENT_ID, None)

    def status_error(self, message):
        """
        Finds the reason why the message has been rejected
        """
        LOG.entry('_MQLightMessenger.status_error', NO_CLIENT_ID)
        LOG.parms(NO_CLIENT_ID, 'message:', message)
        if self.messenger is None:
            raise NetworkError('Not connected')

        delivery = cproton.pn_messenger_delivery(
            self.messenger, message.tracker)
        disposition = None
        condition = None
        description = None
        if delivery:
            disposition = cproton.pn_delivery_remote(delivery)
            if disposition:
                condition = cproton.pn_disposition_condition(disposition)
                if condition:
                    description = cproton.pn_condition_get_description(
                        condition)
        LOG.exit(
            '_MQLightMessenger.status_error',
            NO_CLIENT_ID,
            description)
        return description

    def get_remote_idle_timeout(self, address):
        """
        Returns the idle timeout of the Messenger
        """
        LOG.entry('_MQLightMessenger.get_remote_idle_timeout', NO_CLIENT_ID)
        LOG.parms(NO_CLIENT_ID, 'address:', address)
        if self.messenger is None:
            raise NetworkError('Not connected')

        remote_idle_timeout = cproton.pn_messenger_get_remote_idle_timeout(
            self.messenger,
            address) / 1000
        LOG.exit(
            '_MQLightMessenger.get_remote_idle_timeout',
            NO_CLIENT_ID,
            remote_idle_timeout)
        return remote_idle_timeout

    def flow(self, address, credit, sock):
        """
        Process messages based on the number of credit available
        """
        LOG.entry('_MQLightMessenger.flow', NO_CLIENT_ID)
        LOG.parms(NO_CLIENT_ID, 'address:', address)
        LOG.parms(NO_CLIENT_ID, 'credit:', credit)
        LOG.parms(NO_CLIENT_ID, 'sock:', sock)
        # throw exception if not connected
        if self.messenger is None:
            raise NetworkError('Not connected')

        # Find link based on address, and flow link credit.
        link = cproton.pn_messenger_get_link(self.messenger, address, False)
        if link:
            cproton.pn_link_flow(link, credit)
            self._write(sock, False)
        LOG.exit('_MQLightMessenger.flow', NO_CLIENT_ID, None)

    def put(self, msg, qos):
        """
        Puts a message on the outgoing queue
        """
        LOG.entry('_MQLightMessenger.put', NO_CLIENT_ID)
        LOG.parms(NO_CLIENT_ID, 'msg:', msg)
        LOG.parms(NO_CLIENT_ID, 'qos:', qos)
        # throw exception if not connected
        if self.messenger is None:
            raise NetworkError('Not connected')
        # Set the required QoS, by setting the sender settler mode to settled
        # (QoS = AMO) or unsettled (QoS = ALO). Note that the receiver settler
        # mode is always set to first, as the MQ Light listener will
        # negotiate down any receiver settler mode to first.
        if qos not in (QOS_AT_MOST_ONCE, QOS_AT_LEAST_ONCE):
            raise InvalidArgumentError('Invalid QoS')

        LOG.data(NO_CLIENT_ID, 'message:', msg.message)
        LOG.data(NO_CLIENT_ID, 'body:', msg.body)
        cproton.pn_messenger_put(self.messenger, msg.message)
        error = cproton.pn_messenger_errno(self.messenger)
        if error:
            text = cproton.pn_error_text(
                cproton.pn_messenger_error(
                    self.messenger))
            _MQLightMessenger._raise_error(text)

        tracker = cproton.pn_messenger_outgoing_tracker(self.messenger)
        LOG.data(NO_CLIENT_ID, 'tracker:', tracker)
        msg.tracker = tracker

        if qos == QOS_AT_MOST_ONCE:
            error = cproton.pn_messenger_settle(self.messenger, tracker, 0)
            if error:
                text = cproton.pn_error_text(
                    cproton.pn_messenger_error(
                        self.messenger))
                _MQLightMessenger._raise_error(text)
        LOG.exit('_MQLightMessenger.put', NO_CLIENT_ID, True)
        return True

    def send(self, sock):
        """
        Sends the messages on the outgoing queue
        """
        LOG.entry('_MQLightMessenger.send', NO_CLIENT_ID)
        # throw exception if not connected
        if self.messenger is None:
            raise NetworkError('Not connected')

        cproton.pn_messenger_send(self.messenger, -1)
        error = cproton.pn_messenger_errno(self.messenger)
        if error:
            raise MQLightError(
                cproton.pn_error_text(
                    cproton.pn_messenger_error(
                        self.messenger)))

        self._write(sock, False)

        LOG.exit('_MQLightMessenger.send', NO_CLIENT_ID, True)
        return True

    def receive(self, sock):
        """
        Retrieves messages from the incoming queue
        """
        LOG.entry('_MQLightMessenger.receive', NO_CLIENT_ID)
        LOG.parms(NO_CLIENT_ID, 'sock:', sock)
        # throw exception if not connected
        if self.messenger is None:
            raise NetworkError('Not connected')

        cproton.pn_messenger_recv(self.messenger, -2)
        error = cproton.pn_messenger_errno(self.messenger)
        if error:
            text = cproton.pn_error_text(
                cproton.pn_messenger_error(
                    self.messenger))
            _MQLightMessenger._raise_error(text)

        messages = []
        count = cproton.pn_messenger_incoming(self.messenger)
        LOG.data(NO_CLIENT_ID, 'messages count: {0}'.format(count))
        while cproton.pn_messenger_incoming(self.messenger) > 0:
            message = cproton.pn_message()
            rc = cproton.pn_messenger_get(self.messenger, message)
            # try again if message not yet available on incoming queue
            if rc == cproton.PN_EOS:
                continue
            error = cproton.pn_messenger_errno(self.messenger)
            if error:
                text = cproton.pn_error_text(
                    cproton.pn_messenger_error(
                        self.messenger))
                _MQLightMessenger._raise_error(text)
            msg = _MQLightMessage(message)
            tracker = cproton.pn_messenger_incoming_tracker(self.messenger)
            msg.tracker = tracker
            link = cproton.pn_messenger_tracker_link(
                self.messenger,
                tracker)
            if link:
                if cproton.pn_link_state(link) & cproton.PN_LOCAL_CLOSED:
                    LOG.data(
                        NO_CLIENT_ID,
                        'Link closed so ignoring received message for ' +
                        'address: {0}'.format(
                            cproton.pn_message_get_address(message)))
                else:
                    msg.link_address = cproton.pn_terminus_get_address(
                        cproton.pn_link_remote_target(link))
                    messages.append(msg)
            else:
                LOG.data(
                    NO_CLIENT_ID,
                    'No link associated with received message tracker for ' +
                    'address: {0}'.format(
                        cproton.pn_message_get_address(message)))

        self._write(sock, False)
        LOG.exit('_MQLightMessenger.receive', NO_CLIENT_ID, messages)
        return messages

    def settle(self, message, sock):
        """
        Settles a message
        """
        LOG.entry('_MQLightMessenger.settle', NO_CLIENT_ID)
        LOG.parms(NO_CLIENT_ID, 'message:', message)
        LOG.parms(NO_CLIENT_ID, 'sock:', sock)
        # throw exception if not connected
        if self.messenger is None:
            raise NetworkError('Not connected')

        tracker = message.tracker
        status = cproton.pn_messenger_settle(
            self.messenger,
            tracker,
            0)
        error = cproton.pn_messenger_errno(self.messenger)
        if error:
            text = cproton.pn_error_text(
                cproton.pn_messenger_error(
                    self.messenger))
            _MQLightMessenger._raise_error(text)
        elif status != 0:
            raise NetworkError('Failed to settle')

        self._write(sock, False)
        LOG.exit('_MQLightMessenger.settle', NO_CLIENT_ID, True)
        return True

    def settled(self, message):
        """
        Checks if a message has been settled
        """
        LOG.entry('_MQLightMessenger.settled', NO_CLIENT_ID)
        LOG.parms(NO_CLIENT_ID, 'message:', message)
        # throw exception if not connected
        if self.messenger is None:
            raise NetworkError('Not connected')

        delivery = cproton.pn_messenger_delivery(
            self.messenger,
            message.tracker)

        # For incoming messages, if we haven't already settled it, block for a
        # while until we *think* the settlement disposition has been
        # communicated over the network. We detect that by querying
        # pn_transport_quiesced which should return True once all pending
        # output has been written to the wire.
        settled = True
        if (delivery and
                cproton.pn_link_is_receiver(
                    cproton.pn_delivery_link(delivery))):
            session = cproton.pn_link_session(
                cproton.pn_delivery_link(delivery))
            if session:
                connection = cproton.pn_session_connection(session)
                if connection:
                    transport = cproton.pn_connection_transport(connection)
                    if transport:
                        if not cproton.pn_transport_quiesced(transport):
                            settled = False
        LOG.exit('_MQLightMessenger.settled', NO_CLIENT_ID, settled)
        return settled

    def accept(self, message):
        """
        Accepts a message
        """
        LOG.entry('_MQLightMessenger.accept', NO_CLIENT_ID)
        LOG.parms(NO_CLIENT_ID, 'message:', message)

        # throw exception if not connected
        if self.messenger is None:
            raise NetworkError('Not connected')

        tracker = message.tracker
        status = cproton.pn_messenger_accept(self.messenger, tracker, 0)
        error = cproton.pn_messenger_errno(self.messenger)
        if error:
            text = cproton.pn_error_text(
                cproton.pn_messenger_error(self.messenger))
            _MQLightMessenger._raise_error(text)
        elif status != 0:
            raise NetworkError('Failed to accept')

        LOG.exit('_MQLightMessenger.accept', NO_CLIENT_ID, True)
        return True

    def status(self, message):
        """
        Get the status of a message
        """
        LOG.entry('_MQLightMessenger.status', NO_CLIENT_ID)
        LOG.parms(NO_CLIENT_ID, 'message:', message)

        # throw exception if not connected
        if self.messenger is None:
            raise NetworkError('Not connected')

        tracker = message.tracker
        disp = cproton.pn_messenger_status(self.messenger, tracker)
        LOG.data(NO_CLIENT_ID, 'status: ', disp)
        status = STATUSES[disp]

        LOG.exit('_MQLightMessenger.status', NO_CLIENT_ID, status)
        return status

    def subscribe(self, address, qos, ttl, credit, sock):
        """
        Subscribes to a topic
        """
        LOG.entry('_MQLightMessenger.subscribe', NO_CLIENT_ID)
        if credit > 4294967295:
            credit = 4294967295
        LOG.parms(NO_CLIENT_ID, 'address:', address)
        LOG.parms(NO_CLIENT_ID, 'qos:', qos)
        LOG.parms(NO_CLIENT_ID, 'ttl:', ttl)
        LOG.parms(NO_CLIENT_ID, 'credit:', credit)
        LOG.parms(NO_CLIENT_ID, 'sock:', sock)

        # throw exception if not connected
        if self.messenger is None:
            raise NetworkError('Not connected')

        # Set the required QoS, by setting the sender settler mode to settled
        # (QoS = AMO) or unsettled (QoS = ALO).
        # Note that our API client implementation will always specify a value
        # of first - meaning "The Receiver will spontaneously settle all
        # incoming transfers" - this equates to a maximum QoS of "at least once
        # delivery".
        if qos == 0:
            self.set_snd_settle_mode(cproton.PN_SND_SETTLED)
            self.set_rcv_settle_mode(cproton.PN_RCV_FIRST)
        elif qos == 1:
            self.set_snd_settle_mode(cproton.PN_SND_UNSETTLED)
            self.set_rcv_settle_mode(cproton.PN_RCV_FIRST)
        else:
            raise InvalidArgumentError('Invalid QoS')
        cproton.pn_messenger_subscribe_ttl(self.messenger, address, ttl)
        cproton.pn_messenger_recv(self.messenger, -2)
        error = cproton.pn_messenger_errno(self.messenger)
        if error:
            text = cproton.pn_error_text(
                cproton.pn_messenger_error(self.messenger))
            _MQLightMessenger._raise_error(text)

        self._write(sock, False)

        LOG.exit('_MQLightMessenger.subscribe', NO_CLIENT_ID, True)
        return True

    def subscribed(self, address):
        """
        Return True if the client is subscribed to the specified topic
        """
        LOG.entry('_MQLightMessenger.subscribed', NO_CLIENT_ID)
        LOG.parms(NO_CLIENT_ID, 'address:', address)

        # throw Error if not connected
        if self.messenger is None:
            raise NetworkError('Not connected')

        link = cproton.pn_messenger_get_link(self.messenger, address, False)
        error = cproton.pn_messenger_errno(self.messenger)
        if error:
            text = cproton.pn_error_text(
                cproton.pn_messenger_error(self.messenger))
            _MQLightMessenger._raise_error(text)

        if link is None:
            # throw Error if unable to find a matching Link
            raise MQLightError(
                'Unable to locate link for {0}'.format(address))

        if not (cproton.pn_link_state(link) & cproton.PN_REMOTE_ACTIVE):
            subscribed = False
        else:
            subscribed = True

        LOG.exit('_MQLightMessenger.subscribed', NO_CLIENT_ID, subscribed)
        return subscribed

    def unsubscribe(self, address, ttl, sock):
        """
        Unsubscribes from a topic
        """
        LOG.entry('_MQLightMessenger.Unsubscribe', NO_CLIENT_ID)
        if ttl is None:
            ttl = -1
        LOG.parms(NO_CLIENT_ID, 'address:', address)
        LOG.parms(NO_CLIENT_ID, 'ttl:', ttl)
        LOG.parms(NO_CLIENT_ID, 'sock:', sock)

        # throw exception if not connected
        if self.messenger is None:
            raise NetworkError('Not connected')

        # find link based on address
        link = cproton.pn_messenger_get_link(self.messenger, address, False)

        if link is None:
            # throw Error if unable to find a matching Link
            raise MQLightError(
                'unable to locate link for {0}'.format(address))

        if ttl == 0:
            cproton.pn_terminus_set_expiry_policy(
                cproton.pn_link_target(link), cproton.PN_LINK_CLOSE)
            cproton.pn_terminus_set_expiry_policy(
                cproton.pn_link_source(link), cproton.PN_LINK_CLOSE)
            LOG.parms(NO_CLIENT_ID, 'ttl:', ttl)
            cproton.pn_terminus_set_timeout(cproton.pn_link_target(link), ttl)
            cproton.pn_terminus_set_timeout(cproton.pn_link_source(link), ttl)

        # Check if we are detaching with @closed=true
        closing = True
        expiry_policy = cproton.pn_terminus_get_expiry_policy(
            cproton.pn_link_target(link))
        timeout = cproton.pn_terminus_get_timeout(cproton.pn_link_target(link))
        if expiry_policy == cproton.PN_EXPIRE_NEVER or timeout > 0:
            closing = False
        LOG.data(NO_CLIENT_ID, 'closing:', closing)

        if closing:
            cproton.pn_link_close(link)
        else:
            cproton.pn_link_detach(link)
        self._write(sock, False)

        LOG.exit('_MQLightMessenger.Unsubscribe', NO_CLIENT_ID, True)
        return True

    def unsubscribed(self, address):
        """
        Return True if the client is not subscribed to the specified topic
        """
        LOG.entry('_MQLightMessenger.unsubscribed', NO_CLIENT_ID)
        LOG.parms(NO_CLIENT_ID, 'address:', address)

        # throw Error if not connected
        if self.messenger is None:
            raise NetworkError('Not connected')

        # find link based on address, in any state.
        link = cproton.pn_messenger_get_stated_link(
            self.messenger,
            address,
            False,
            0)

        if link is None:
            # throw Error if unable to find a matching Link
            raise MQLightError('Unable to locate link for {0}'.format(address))

        # Check if we are detaching with @closed=true
        closing = True
        expiry_policy = cproton.pn_terminus_get_expiry_policy(
            cproton.pn_link_target(link))
        timeout = cproton.pn_terminus_get_timeout(cproton.pn_link_target(link))
        if expiry_policy == cproton.PN_EXPIRE_NEVER or timeout > 0:
            closing = False
        LOG.data(NO_CLIENT_ID, 'closing:', closing)

        # check if the remote end has acknowledged the close or detach
        if closing:
            if not (cproton.pn_link_state(link) & cproton.PN_REMOTE_CLOSED):
                unsubscribed = False
            else:
                unsubscribed = True
        else:
            if not cproton.pn_link_remote_detached(link):
                unsubscribed = False
            else:
                unsubscribed = True
                cproton.pn_messenger_reclaim_link(self.messenger, link)
                cproton.pn_link_free(link)

        LOG.exit('_MQLightMessenger.unsubscribed', NO_CLIENT_ID, unsubscribed)
        return unsubscribed

    def pending_outbound(self, address):
        """
        Indicates if there are pending messages
        """
        LOG.entry('_MQLightMessenger.pending_outbound', NO_CLIENT_ID)
        LOG.parms(NO_CLIENT_ID, 'address:', address)

        # throw exception if not connected
        if self.messenger is None:
            raise NetworkError('Not connected')

        result = False
        pending = cproton.pn_messenger_pending_outbound(
            self.messenger,
            address)
        if pending < 0:
            raise NetworkError('Not connected')
        elif pending > 0:
            result = True

        LOG.exit('_MQLightMessenger.pending_outbound', NO_CLIENT_ID, result)
        return result

    def pop(self, sock, force):
        LOG.entry('_MQLightMessenger.pop', NO_CLIENT_ID)
        LOG.parms(NO_CLIENT_ID, 'sock:', sock)
        LOG.parms(NO_CLIENT_ID, 'force:', force)
        written = self._write(sock, force)
        LOG.exit('_MQLightMessenger.pop', NO_CLIENT_ID, written)
        return written

    def push(self, chunk):
        LOG.entry('_MQLightMessenger.push', NO_CLIENT_ID)
        with self._lock:
            if self.messenger and self.connection:
                pushed = cproton.pn_connection_push(
                    self.connection,
                    chunk,
                    len(chunk))
            else:
                # This connection has already been closed, so this data can
                # never be pushed in, so just return saying it has so the data
                # will be discarded.
                LOG.data(
                    NO_CLIENT_ID,
                    'connection already closed: discarding data')
                pushed = len(chunk)

        LOG.exit('_MQLightMessenger.push', NO_CLIENT_ID, pushed)
        return pushed

    def _update_sasl_outcome(self, transport):
        """
        Retrieves and stores the sasl outcome from the transport
        """
        LOG.entry('_update_sasl_outcome', NO_CLIENT_ID)
        if transport:
            self.sasl_outcome = cproton.pn_sasl_outcome(
                cproton.pn_sasl(transport))
            LOG.data(NO_CLIENT_ID, 'outcome:', self.sasl_outcome)
        else:
            LOG.data(NO_CLIENT_ID, 'connection is closed')

        LOG.exit('_update_sasl_outcome', NO_CLIENT_ID, None)

    def _write(self, sock, force):
        LOG.entry_often('_MQLightMessenger._write', NO_CLIENT_ID)
        LOG.parms(NO_CLIENT_ID, 'force:', force)

        with self._lock:
            # Checking for pending data requires a messenger connection
            n = 0
            if self.messenger and self.connection:
                transport = cproton.pn_connection_transport(self.connection)
                if transport:
                    n = cproton.pn_transport_pending(transport)
                    # Force a pop, causing a heartbeat to be generated, if
                    # necessary
                    if force:
                        closed = cproton.pn_connection_pop(self.connection, 0)
                        if closed:
                            LOG.data(NO_CLIENT_ID, 'connection is closed')
                            self._update_sasl_outcome(transport)
                            self.connection = None

                    if self.connection and n > 0:
                        # write n bytes to stream
                        buf = cproton.pn_transport_head(transport, n)
                        sock.send(buf)

                        closed = cproton.pn_connection_pop(self.connection, n)
                        if closed:
                            LOG.data(NO_CLIENT_ID, 'connection is closed')
                            self._update_sasl_outcome(transport)
                            self.connection = None
                    else:
                        n = 0
                else:
                    n = -1
            else:
                n = -1

        LOG.exit_often('_MQLightMessenger._write', NO_CLIENT_ID, n)
        return n

    def heartbeat(self, sock):
        LOG.entry('_MQLightMessenger.heartbeat', NO_CLIENT_ID)
        LOG.parms(NO_CLIENT_ID, 'sock:', sock)
        self._write(sock, True)
        LOG.exit('_MQLightMessenger.heartbeat', NO_CLIENT_ID, None)

    def closed(self):
        LOG.entry('_MQLightMessenger.closed', NO_CLIENT_ID)
        if self.messenger and self.connection:
            cproton.pn_connection_was_closed(self.messenger, self.connection)
            error = cproton.pn_messenger_errno(self.messenger)
            if error:
                text = cproton.pn_error_text(
                    cproton.pn_messenger_error(self.messenger))
                _MQLightMessenger._raise_error(text)
        LOG.exit('_MQLightMessenger.closed', NO_CLIENT_ID, None)


class _MQLightSocket(object):

    def __init__(self, address, tls, security_options, on_read, on_close):
        LOG.entry('_MQLightSocket.__init__', NO_CLIENT_ID)
        LOG.parms(NO_CLIENT_ID, 'address:', address)
        LOG.parms(NO_CLIENT_ID, 'tls:', tls)
        LOG.parms(NO_CLIENT_ID, 'security_options:', security_options)
        LOG.parms(NO_CLIENT_ID, 'on_read:', on_read)
        LOG.parms(NO_CLIENT_ID, 'on_close:', on_close)
        self.running = False
        self.on_read = on_read
        self.on_close = on_close
        self._data_queue = Queue.Queue()
        self._data_handler_thread = threading.Thread(target=self.queue_data)
        self._data_handler_thread.setDaemon(True)
        try:
            self.sock = socket.socket(
                socket.AF_INET,
                socket.SOCK_STREAM)
            if tls:
                LOG.data(NO_CLIENT_ID, 'wrapping the socket in an SSL context')
                ctx = ssl.create_default_context(
                    ssl.Purpose.SERVER_AUTH,
                    security_options.ssl_trust_certificate)
                ctx.check_hostname = security_options.ssl_verify_name
                self.sock = ctx.wrap_socket(self.sock, server_hostname=address[0])
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.sock.connect(address)

            self.running = True
            self.io_loop = threading.Thread(target=self.loop)
            self.io_loop.start()
        except (socket.error, ssl.SSLError) as exc:
            LOG.error('_MQLightSocket.__init__', NO_CLIENT_ID, exc)
            self.running = False
            raise exc
        LOG.exit('_MQLightSocket.__init__', NO_CLIENT_ID, None)

    def queue_data(self):
        LOG.entry('_MQLightSocket.queue_data', NO_CLIENT_ID)
        while self.running:
            args = self._data_queue.get()
            callback = args[0]
            if len(args) > 1:
                callback(*args[1:])
            # if we do not get arguments that means on_close was passed in
            else:
                callback()
                self.running = False
        LOG.exit('_MQLightSocket.queue_data', NO_CLIENT_ID, None)

    def loop(self):
        LOG.entry('_MQLightSocket.loop', NO_CLIENT_ID)
        self._data_handler_thread.start()
        exc = None
        while self.running and not exc:
            read, write, exc = select.select([self.sock], [], [self.sock])
            if read:
                data = self.sock.recv(4096)
                if data:
                    self._data_queue.put((self.on_read, data))
                else:
                    self._data_queue.put((self.on_close,))
        LOG.exit('_MQLightSocket.loop', NO_CLIENT_ID, None)

    def send(self, msg):
        LOG.entry('_MQLightSocket.send', NO_CLIENT_ID)
        if self.sock:
            sent = self.sock.send(msg)
        else:
            sent = 0
        LOG.exit('_MQLightSocket.send', NO_CLIENT_ID, sent)

    def close(self):
        LOG.entry('_MQLightSocket.close', NO_CLIENT_ID)
        self.running = False
        self.sock.shutdown(socket.SHUT_RD)
        self.io_loop.join()
        self.sock.close()
        LOG.exit('_MQLightSocket.close', NO_CLIENT_ID, None)
