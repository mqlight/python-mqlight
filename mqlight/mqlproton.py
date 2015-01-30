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
import cproton
import ast
from . import mqlightexceptions as mqlexc
import os
from .mqlightlog import get_logger, NO_CLIENT_ID
from urlparse import urlunparse

LOG = get_logger(__name__)

QOS_AT_MOST_ONCE = 0
QOS_AT_LEAST_ONCE = 1
STATUSES = ['UNKNOWN', 'PENDING', 'ACCEPTED', 'REJECTED', 'RELEASED',
            'MODIFIED', 'ABORTED', 'SETTLED']


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
            if type(value) in (unicode, str):
                LOG.data(NO_CLIENT_ID, 'setting the body format as text')
                cproton.pn_message_set_format(self._msg, cproton.PN_TEXT)
                cproton.pn_message_load_text(self._msg, str(value))
            else:
                LOG.data(NO_CLIENT_ID, 'setting the body format as data')
                cproton.pn_message_set_format(self._msg, cproton.PN_DATA)
                cproton.pn_message_load_data(
                    self._msg,
                    ''.join(str(i) for i in value))
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
                    cproton.pn_message_set_format(self._msg, cproton.PN_TEXT)
                else:
                    cproton.pn_message_set_format(self._msg, cproton.PN_DATA)

                size = 16
                while True:
                    err, result = cproton.pn_message_save(self._msg, size)
                    if err == cproton.PN_OVERFLOW:
                        size *= 2
                        continue
                    else:
                        break
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
        result = None
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
                    None)
                return

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
            result = []
            while True:
                if cproton.pn_data_type(anno) == cproton.PN_SYMBOL:
                    key = cproton.pn_data_get_symbol(anno).start

                    if cproton.pn_data_next(anno):
                        value = None
                        value_type = None
                        data_type = cproton.pn_data_type(anno)
                        add_entry = True

                        if data_type == cproton.PN_SYMBOL:
                            add_entry = True
                            value_type = 'symbol'
                            value = cproton.pn_data_get_symbol(anno).start
                            break
                        elif data_type == cproton.PN_STRING:
                            add_entry = True
                            value_type = 'string'
                            value = cproton.pn_data_get_string(anno).start
                            break
                        elif data_type == cproton.PN_INT:
                            add_entry = True
                            value_type = 'int32'
                            value = cproton.pn_data_get_atom(anno).u.as_int
                            break
                        else:
                            add_entry = False
                            break

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
            else:
                result = None
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
        self._name = name
        LOG.exit('_MQLightMessenger.constructor', NO_CLIENT_ID, None)

    @staticmethod
    def _raise_error(text):
        """
        Parses an error message from messenger and raises the corresponding
        Error
        """
        if ' sasl ' in text or 'SSL ' in text:
            raise mqlexc.SecurityError(text)
        elif '_Takeover' in text:
            raise mqlexc.ReplacedError(text)
        else:
            raise mqlexc.NetworkError(text)

    def connect(self, service, ssl_trust_certificate, ssl_verify_name):
        """
        Connects to the specified service
        """
        LOG.entry('_MQLightMessenger.connect', NO_CLIENT_ID)
        LOG.parms(
            NO_CLIENT_ID,
            'ssl_trust_certificate:',
            ssl_trust_certificate)
        LOG.parms(NO_CLIENT_ID, 'ssl_verify_name:', ssl_verify_name)

        if ssl_trust_certificate:
            if not os.path.isfile(ssl_trust_certificate):
                msg = 'The file specified for ssl_trust_certificate ' + \
                    ssl_trust_certificate + \
                    ' does not exist or is not accessible'
                raise mqlexc.SecurityError(msg)
        ssl_mode = cproton.PN_SSL_VERIFY_NULL
        if ssl_verify_name is not None:
            if ssl_verify_name:
                ssl_mode = cproton.PN_SSL_VERIFY_PEER_NAME
            else:
                ssl_mode = cproton.PN_SSL_VERIFY_PEER

        # If the proton messenger already exists and has been stopped then free
        # it so that we can recreate a new instance.  This situation can arise
        # if the messenger link is closed by the remote end instead of a call
        # to stop()
        if self.messenger:
            stopped = cproton.pn_messenger_stopped(self.messenger)
            if stopped:
                self.messenger = None

        # throw exception if already connected
        if self.messenger:
            raise mqlexc.NetworkError('already connected')

        # Create the messenger object and update the name in case messenger has
        # changed it
        self.messenger = cproton.pn_messenger(self._name)
        self._name = cproton.pn_messenger_name(self.messenger)

        cproton.pn_messenger_set_blocking(self.messenger, False)
        cproton.pn_messenger_set_incoming_window(self.messenger, 2147483647)
        cproton.pn_messenger_set_outgoing_window(self.messenger, 2147483647)

        # Set the SSL trust certificate when required
        if ssl_trust_certificate:
            error = cproton.pn_messenger_set_trusted_certificates(
                self.messenger,
                ssl_trust_certificate)
            LOG.data(
                NO_CLIENT_ID,
                'pn_messenger_set_trusted_certificates:',
                error)
            if error:
                self.messenger = None
                err = mqlexc.SecurityError(
                    'Failed to set trusted certificates')

        if ssl_mode != cproton.PN_SSL_VERIFY_NULL:
            error = cproton.pn_messenger_set_ssl_peer_authentication_mode(
                self.messenger, ssl_mode)
            LOG.data(
                NO_CLIENT_ID,
                'pn_messenger_set_ssl_peer_authentication_mode:',
                error)
            if error:
                self.messenger = None
                raise mqlexc.SecurityError(
                    'Failed to set SSL peer authentication mode')

        # Set the route and enable PN_FLAGS_CHECK_ROUTES so that messenger
        # confirms that it can connect at startup.
        address = urlunparse(service)
        url_protocol = service.scheme
        host_and_port = service.hostname
        if service.port:
            host_and_port += ':' + str(service.port)
        pattern = url_protocol + '://' + host_and_port + '/*'
        validation_address = address + '/$1'
        LOG.data(NO_CLIENT_ID, 'pattern:', pattern)
        error = cproton.pn_messenger_route(
            self.messenger, pattern, validation_address)
        LOG.data(NO_CLIENT_ID, 'pn_messenger_route:', error)
        if error:
            self.messenger = None
            err = mqlexc.MQLightError('Failed to set messenger route')
            raise err
        # Indicate that the route should be validated
        cproton.pn_messenger_set_flags(
            self.messenger,
            cproton.PN_FLAGS_CHECK_ROUTES)
        # Start the messenger. This will fail if the route is invalid
        error = cproton.pn_messenger_start(self.messenger)
        LOG.data(NO_CLIENT_ID, 'pn_messenger_start:', error)
        if error:
            text = cproton.pn_error_text(
                cproton.pn_messenger_error(self.messenger))
            self.messenger = None
            _MQLightMessenger._raise_error(text)
        LOG.exit('_MQLightMessenger.connect', NO_CLIENT_ID, None)

    def stop(self):
        """
        Calls stop() on the proton Messenger
        """
        LOG.entry('_MQLightMessenger.stop', NO_CLIENT_ID)
        # if already stopped then simply return True
        if self.messenger is None:
            LOG.exit('_MQLightMessenger.stop', NO_CLIENT_ID, True)
            return True
        cproton.pn_messenger_stop(self.messenger)
        stopped = cproton.pn_messenger_stopped(self.messenger)
        if stopped:
            cproton.pn_messenger_free(self.messenger)
            self.messenger = None
        LOG.exit('_MQLightMessenger.stop', NO_CLIENT_ID, stopped)
        return stopped

    def _is_stopped(self):
        """
        Returns True if the messenger if currently stopped
        """
        LOG.entry('_MQLightMessenger._is_stopped', NO_CLIENT_ID)
        if self.messenger is not None:
            state = cproton.pn_messenger_stopped(self.messenger)
        else:
            state = True
        LOG.exit('_MQLightMessenger._is_stopped', NO_CLIENT_ID, state)
        return state
    stopped = property(_is_stopped)

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
            raise mqlexc.NetworkError('not connected')

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
            description = cproton.pn_condition_get_description(condition)
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
            raise mqlexc.NetworkError('not connected')

        remote_idle_timeout = cproton.pn_messenger_get_remote_idle_timeout(
            self.messenger,
            address)
        LOG.exit(
            '_MQLightMessenger.get_remote_idle_timeout',
            NO_CLIENT_ID,
            remote_idle_timeout / 1000)
        return remote_idle_timeout / 1000

    def work(self, timeout):
        """
        Sends or receives any outstanding messages
        """
        LOG.entry('_MQLightMessenger.work', NO_CLIENT_ID)
        LOG.parms(NO_CLIENT_ID, 'timeout:', timeout)
        if self.messenger is None:
            raise mqlexc.NetworkError('not connected')

        status = cproton.pn_messenger_work(self.messenger, timeout)
        LOG.exit('_MQLightMessenger.work', NO_CLIENT_ID, status)
        return status

    def flow(self, address, credit):
        """
        Process messages based on the number of credit available
        """
        LOG.entry('_MQLightMessenger.flow', NO_CLIENT_ID)
        LOG.parms(NO_CLIENT_ID, 'address:', address)
        LOG.parms(NO_CLIENT_ID, 'credit:', credit)
        # throw exception if not connected
        if self.messenger is None:
            raise mqlexc.NetworkError('Not connected')
        # Find link based on address, and flow link credit.
        link = cproton.pn_messenger_get_link(self.messenger, address, False)
        if link:
            cproton.pn_link_flow(link, credit)
        else:
            LOG.parms(NO_CLIENT_ID, 'link:', None)
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
            raise mqlexc.NetworkError('Not connected')
        # Set the required QoS, by setting the sender settler mode to settled
        # (QoS = AMO) or unsettled (QoS = ALO). Note that the receiver settler
        # mode is always set to first, as the MQ Light listener will
        # negotiate down any receiver settler mode to first.
        if qos not in (QOS_AT_MOST_ONCE, QOS_AT_LEAST_ONCE):
            raise mqlexc.InvalidArgumentError('invalid qos argument')

        LOG.data(NO_CLIENT_ID, 'msg:', msg.message)
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

    def send(self):
        """
        Sends the messages on the outgoing queue
        """
        LOG.entry('_MQLightMessenger.send', NO_CLIENT_ID)
        # throw exception if not connected
        if self.messenger is None:
            raise mqlexc.NetworkError('Not connected')

        cproton.pn_messenger_send(self.messenger, -1)
        error = cproton.pn_messenger_errno(self.messenger)
        if error:
            raise mqlexc.MQLightError(
                cproton.pn_error_text(
                    cproton.pn_messenger_error(
                        self.messenger)))

        cproton.pn_messenger_work(self.messenger, 0)
        error = cproton.pn_messenger_errno(self.messenger)
        if error:
            raise mqlexc.MQLightError(
                cproton.pn_error_text(
                    cproton.pn_messenger_error(
                        self.messenger)))

        LOG.exit('_MQLightMessenger.send', NO_CLIENT_ID, True)
        return True

    def receive(self, timeout):
        """
        Retrieves messages from the incoming queue
        """
        LOG.entry('_MQLightMessenger.receive', NO_CLIENT_ID)
        LOG.parms(NO_CLIENT_ID, 'timeout:', timeout)
        # throw exception if not connected
        if self.messenger is None:
            raise mqlexc.NetworkError('Not connected')

        cproton.pn_messenger_recv(self.messenger, -2)
        error = cproton.pn_messenger_errno(self.messenger)
        if error:
            text = cproton.pn_error_text(
                cproton.pn_messenger_error(
                    self.messenger))
            _MQLightMessenger._raise_error(text)
        cproton.pn_messenger_work(self.messenger, timeout)
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
            cproton.pn_messenger_get(self.messenger, message)
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
                        'address: ' + cproton.pn_message_get_address(message))
                else:
                    msg.link_address = cproton.pn_terminus_get_address(
                        cproton.pn_link_remote_target(link))
                    messages.append(msg)
            else:
                LOG.data(
                    NO_CLIENT_ID,
                    'No link associated with received message tracker for ' +
                    'address: ' + cproton.pn_message_get_address(message))
        LOG.exit('_MQLightMessenger.receive', NO_CLIENT_ID, messages)
        return messages

    def settle(self, message):
        """
        Settles a message
        """
        LOG.entry('_MQLightMessenger.settle', NO_CLIENT_ID)
        LOG.parms(NO_CLIENT_ID, 'message:', message)
        # throw exception if not connected
        if self.messenger is None:
            raise mqlexc.NetworkError('Not connected')

        tracker = message.tracker
        delivery = cproton.pn_messenger_delivery(self.messenger, tracker)
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
            raise mqlexc.NetworkError('Failed to settle')

        # For incoming messages, if we haven't already settled it, block for a
        # while until we *think* the settlement disposition has been
        # communicated over the network. We detect that by querying
        # pn_transport_quiesced which should return True once all pending
        # output has been written to the wire.
        # (as per other comments, ideally we should wrap this in a callback...)
        is_receiver = cproton.pn_link_is_receiver(
            cproton.pn_delivery_link(delivery))
        if delivery is not None and is_receiver:
            session = cproton.pn_link_session(
                cproton.pn_delivery_link(delivery))
            if session:
                connection = cproton.pn_session_connection(session)
                if connection:
                    transport = cproton.pn_connection_transport(connection)
                    if transport:
                        while not cproton.pn_transport_quiesced(transport):
                            cproton.pn_messenger_work(self.messenger, 0)
                            error = cproton.pn_messenger_errno(self.messenger)
                            if error:
                                text = cproton.pn_error_text(
                                    cproton.pn_messenger_error(
                                        self.messenger))
                                _MQLightMessenger._raise_error(text)
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
            raise mqlexc.NetworkError('Not connected')

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
            raise mqlexc.NetworkError('Not connected')

        tracker = message.tracker
        status = cproton.pn_messenger_accept(self.messenger, tracker, 0)
        error = cproton.pn_messenger_errno(self.messenger)
        if error:
            text = cproton.pn_error_text(
                cproton.pn_messenger_error(self.messenger))
            _MQLightMessenger._raise_error(text)
        elif status != 0:
            raise mqlexc.NetworkError('Failed to accept')
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
            raise mqlexc.NetworkError('Not connected')

        tracker = message.tracker
        disp = cproton.pn_messenger_status(self.messenger, tracker)
        LOG.data(NO_CLIENT_ID, 'status: ', disp)
        status = STATUSES[disp]
        LOG.exit('_MQLightMessenger.status', NO_CLIENT_ID, status)
        return status

    def subscribe(self, address, qos, ttl, credit):
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

        # throw exception if not connected
        if self.messenger is None:
            raise mqlexc.NetworkError('Not connected')

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
            raise mqlexc.InvalidArgumentError('invalid qos')
        cproton.pn_messenger_subscribe_ttl(self.messenger, address, ttl)
        cproton.pn_messenger_recv(self.messenger, -2)
        error = cproton.pn_messenger_errno(self.messenger)
        if error:
            text = cproton.pn_error_text(
                cproton.pn_messenger_error(self.messenger))
            _MQLightMessenger._raise_error(text)

        link = cproton.pn_messenger_get_link(self.messenger, address, False)
        if not link:
            # throw Error if unable to find a matching Link
            raise mqlexc.MQLightError("unable to locate link for " + address)
        while not cproton.pn_link_state(link) & cproton.PN_REMOTE_ACTIVE:
            cproton.pn_messenger_work(self.messenger, 50)
            error = cproton.pn_messenger_errno(self.messenger)
            if error:
                text = cproton.pn_error_text(
                    cproton.pn_messenger_error(self.messenger))
                _MQLightMessenger._raise_error(text)

        if credit > 0:
            cproton.pn_link_flow(link, credit)

        LOG.exit('_MQLightMessenger.subscribe', NO_CLIENT_ID, True)
        return True

    def unsubscribe(self, address, ttl=None):
        """
        Unsubscribes from a topic
        """
        LOG.entry('_MQLightMessenger.Unsubscribe', NO_CLIENT_ID)
        if ttl is None:
            ttl = -1
        LOG.parms(NO_CLIENT_ID, 'address:', address)
        LOG.parms(NO_CLIENT_ID, 'ttl:', ttl)

        # throw exception if not connected
        if self.messenger is None:
            raise mqlexc.NetworkError('Not connected')

        # find link based on address
        link = cproton.pn_messenger_get_link(self.messenger, address, False)

        if link is None:
            # throw Error if unable to find a matching Link
            raise mqlexc.MQLightError('unable to locate link for ' + address)

        if ttl == 0:
            cproton.pn_terminus_set_expiry_policy(
                cproton.pn_link_target(link), cproton.PN_LINK_CLOSE)
            cproton.pn_terminus_set_expiry_policy(
                cproton.pn_link_source(link), cproton.PN_LINK_CLOSE)
            LOG.parms(NO_CLIENT_ID, 'ttl:', ttl)
            cproton.pn_terminus_set_timeout(cproton.pn_link_target(link), ttl)
            cproton.pn_terminus_set_timeout(cproton.pn_link_source(link), ttl)

        cproton.pn_link_close(link)

        # Check if we are detaching with @closed=true
        closed = True
        expiry_policy = cproton.pn_terminus_get_expiry_policy(
            cproton.pn_link_target(link))
        timeout = cproton.pn_terminus_get_timeout(cproton.pn_link_target(link))
        if expiry_policy == cproton.PN_NEVER or timeout > 0:
            closed = False
        LOG.data(NO_CLIENT_ID, 'closed:', closed)

        if closed:
            while not cproton.pn_link_state(link) & cproton.PN_REMOTE_CLOSED:
                cproton.pn_messenger_work(self.messenger, 50)
                error = cproton.pn_messenger_errno(self.messenger)
                LOG.data(NO_CLIENT_ID, 'error:', error)
                if error:
                    text = cproton.pn_error_text(
                        cproton.pn_messenger_error(
                            self.messenger))
                    _MQLightMessenger._raise_error(text)
        else:
            # Otherwise, all we can do is keep calling work until our close
            # request has been pushed over the network connection as we won't
            # get an ACK
            session = cproton.pn_link_session(link)
            if session:
                connection = cproton.pn_session_connection(session)
                if connection:
                    transport = cproton.pn_connection_transport(connection)
                    if transport:
                        while not cproton.pn_transport_quiesced(transport):
                            cproton.pn_messenger_work(self.messenger, 0)
                            error = cproton.pn_messenger_errno(self.messenger)
                            LOG.data(NO_CLIENT_ID, 'error:', error)
                            if error:
                                text = cproton.pn_error_text(
                                    cproton.pn_messenger_error(
                                        self.messenger))
                                _MQLightMessenger._raise_error(text)

        LOG.exit('_MQLightMessenger.Unsubscribe', NO_CLIENT_ID, True)
        return True

    def pending_outbound(self, address):
        """
        Indicates if there are pending messages
        """
        LOG.entry('_MQLightMessenger.pending_outbound', NO_CLIENT_ID)
        LOG.parms(NO_CLIENT_ID, 'address:', address)
        # throw exception if not connected
        if self.messenger is None:
            raise mqlexc.NetworkError('Not connected')

        result = False
        pending = cproton.pn_messenger_pending_outbound(
            self.messenger,
            address)
        if pending < 0:
            raise mqlexc.NetworkError('Not connected')
        elif pending > 0:
            result = True
        LOG.exit('_MQLightMessenger.pending_outbound', NO_CLIENT_ID, result)
        return result
