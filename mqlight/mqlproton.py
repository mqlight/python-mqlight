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
import proton
import cproton
import mqlightexceptions as mqlexc
import os
from mqlightlog import get_logger, NO_CLIENT_ID
from urlparse import urlunparse

LOG = get_logger(__name__)

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
            self._msg = message
        else:
            self._msg = cproton.pn_message()
        self._tracker = None
        self._link_address = None
        self.connection_id = None
        self._body = None
        LOG.exit('_MQLightMessage.constructor', NO_CLIENT_ID, None)

    def _pre_encode(self):
        """
        Handles body data type and encoding
        """
        body = proton.Data(cproton.pn_message_body(self.message))
        body.clear()
        if self._body is not None:
            body.put_object(self._body)

    def _post_decode(self):
        """
        Handles body data type and encoding
        """
        body = proton.Data(cproton.pn_message_body(self.message))
        if body.next():
            self._body = body.get_object()
        else:
            self._body = None

    def _set_body(self, body):
        """
        Sets the message body
        """
        self._body = body

    def _get_body(self):
        """
        Gets the message body
        """
        return self._body

    body = property(_get_body, _set_body)

    def _get_delivery_annotations(self):
        """
        Gets the message delivery annotations
        """
        # instructions === delivery annotations
        da = cproton.pn_message_instructions(self.message)

        # Count the number of delivery annotations that we are interested in
        # returning
        lval = cproton.pn_data_next(da)     # Move to Map
        elements = 0
        result = None
        # Check it actually is a Map
        if lval and cproton.pn_data_type(da) == cproton.PN_MAP:
            if lval:
                lval = cproton.pn_data_enter(da)    # Enter the Map
            if lval:
                lval = cproton.pn_data_next(da)     # Position on 1st map key
            if lval:
                while True:
                    if cproton.pn_data_type(da) == cproton.PN_SYMBOL:
                        if cproton.pn_data_next(da):
                            if cproton.pn_data_type(da) in (cproton.PN_SYMBOL, cproton.PN_STRING, cproton.PN_INT):
                                elements += 1
                            else:
                                break
                            if not cproton.pn_data_next(da):
                                break
                        else:
                            break
            cproton.pn_data_rewind(da)

            # Return early if there are no (interesting) delivery annotations
            if elements == 0:
                return

            cproton.pn_data_next(da)    # Move to Map
            cproton.pn_data_enter(da)   # Enter the Map
            cproton.pn_data_next(da)    # Position on first map key

            # Build an array of objects, where each object has the following
            # four properties:
            #   key        : the key of the delivery annotation entry
            #   key_type   : the type of the delivery annotation key (always
            #   'symbol')
            #   value      : the value of the delivery annotation entry
            #   value_type : the type of the delivery annotation value ('symbol'
            #   ,'string', or 'int32')
            result = []
            while True:
                if cproton.pn_data_type(da) == cproton.PN_SYMBOL:
                    key = cproton.pn_data_get_symbol(da).start

                    if cproton.pn_data_next(da):
                        value = None
                        value_type = None
                        data_type = cproton.pn_data_type(da)
                        add_entry = True

                        if data_type == cproton.PN_SYMBOL:
                            add_entry = True
                            value_type = 'symbol'
                            value = cproton.pn_data_get_symbol(da).start
                            break
                        elif data_type == cproton.PN_STRING:
                            add_entry = True
                            value_type = 'string'
                            value = cproton.pn_data_get_string(da).start
                            break
                        elif data_type == cproton.PN_INT:
                            add_entry = True
                            value_type = 'int32'
                            value = cproton.pn_data_get_atom(da).u.as_int
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

                        if not cproton.pn_data_next(da):
                            break
                    else:
                        break

                cproton.pn_data_rewind(da)
            else:
                result = None

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
        Parse an error message from messenger and raise the corresponding Error
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
        LOG.parms(NO_CLIENT_ID, 'service:', service)
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
            if error:
                err = mqlexc.SecurityError('Failed to set trusted certificates')

        if ssl_mode != cproton.PN_SSL_VERIFY_NULL:
            error = cproton.pn_messenger_set_ssl_peer_authentication_mode(self.messenger, ssl_mode)
            if error:
                raise mqlexc.SecurityError(
                    'Failed to set SSL peer authentication mode')

        # Set the route and enable PN_FLAGS_CHECK_ROUTES so that messenger
        # confirms that it can connect at startup.
        address = urlunparse(service)
        url_protocol = service.scheme
        port_and_host = service.netloc
        pattern = url_protocol + '://' + port_and_host
        validation_address = address + '/$1'
        error = cproton.pn_messenger_route(
            self.messenger, pattern, validation_address)
        if error:
            err = mqlexc.MQLightError('Failed to set messenger route')
            raise err
        # Indicate that the route should be validated
        cproton.pn_messenger_set_flags(
            self.messenger,
            cproton.PN_FLAGS_CHECK_ROUTES)
        # Start the messenger. This will fail if the route is invalid
        error = cproton.pn_messenger_start(self.messenger)
        if error:
            text = cproton.pn_error_text(
                cproton.pn_messenger_error(self.messenger))
            _MQLightMessenger._raise_error(text)
        LOG.exit('_MQLightMessenger.connect', NO_CLIENT_ID, None)

    def stop(self):
        """
        Calls stop() on the proton Messenger
        """
        LOG.entry('_MQLightMessenger.stop', NO_CLIENT_ID)
        # if already stopped then simply return True
        if self.messenger is None:
            return True
        cproton.pn_messenger_stop(self.messenger)
        stopped = cproton.pn_messenger_stopped(self.messenger)
        LOG.exit('_MQLightMessenger.stop', NO_CLIENT_ID, stopped)
        return stopped

    def _is_stopped(self):
        """
        Returns True if the messenger if currently stopped
        """
        if self.messenger is None:
            return True
        else:
            return False
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
            remote_idle_timeout)
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
        if qos == QOS_AT_MOST_ONCE:
            self.set_snd_settle_mode(cproton.PN_SND_SETTLED)
            self.set_rcv_settle_mode(cproton.PN_RCV_FIRST)
        elif qos == QOS_AT_LEAST_ONCE:
            self.set_snd_settle_mode(cproton.PN_SND_UNSETTLED)
            self.set_snd_settle_mode(cproton.PN_RCV_FIRST)
        else:
            raise mqlexc.InvalidArgumentError('invalid qos argument')

        LOG.data(NO_CLIENT_ID, 'msg:', msg.message)
        LOG.data(NO_CLIENT_ID, 'body:', msg.body)
        msg._pre_encode()
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

        cproton.pn_messenger_work(self.messenger, 50)
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
        LOG.data(NO_CLIENT_ID, 'messages count:', str(count))
        message = cproton.pn_message()
        while cproton.pn_messenger_incoming(self.messenger) > 0:
            cproton.pn_messenger_get(self.messenger, message)
            error = cproton.pn_messenger_errno(self.messenger)
            if error:
                text = cproton.pn_error_text(
                    cproton.pn_messenger_error(
                        self.messenger))
                _MQLightMessenger._raise_error(text)
            msg = _MQLightMessage(message)
            msg._post_decode()
            tracker = cproton.pn_messenger_incoming_tracker(self.messenger)
            msg.tracker = tracker
            link_address = cproton.pn_messenger_tracker_link(
                self.messenger,
                tracker)
            if link_address:
                msg.link_address = cproton.pn_terminus_get_address(
                    cproton.pn_link_remote_target(link_address))
            messages.append(msg)
            cproton.pn_messenger_accept(self.messenger, tracker, 0)
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
        status = cproton.pn_messenger_settle(self.messenger, message.tracker, 0)
        error = cproton.pn_messenger_errno(self.messenger)
        if error:
            text = cproton.pn_error_text(
                cproton.pn_messenger_error(
                    self.messenger))
            _MQLightMessenger._raise_error(text)
        elif status != 0:
            raise mqlexc.NetworkError('Failed to settle')
        LOG.exit('_MQLightMessenger.settle', NO_CLIENT_ID, True)
        return True

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
        status = proton.STATUSES.get(disp, disp)
        LOG.exit('_MQLightMessenger.status', NO_CLIENT_ID, status)
        return status

    def subscribe(self, address, qos, ttl, credit):
        """
        Subscribes to a topic
        """
        LOG.entry('_MQLightMessenger.subscribe', NO_CLIENT_ID)
        LOG.parms(NO_CLIENT_ID, 'address:', address)
        LOG.parms(NO_CLIENT_ID, 'qos:', qos)
        LOG.parms(NO_CLIENT_ID, 'ttl:', ttl)
        LOG.parms(NO_CLIENT_ID, 'credit:', credit)
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
        # XXX: this is less than ideal, but as a temporary fix we will block
        #      until we've received the @attach response back from the server
        #      and the link is marked as active. Ideally we should be passing
        #      callbacks around between JS and C++, so will fix better later
        while not (cproton.pn_link_state(link) & cproton.PN_REMOTE_ACTIVE):
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

    def unsubscribe(self, address, ttl):
        """
        Unsubscribes from a topic
        """
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
        cproton.pn_messenger_work(self.messenger, 50)
        error = cproton.pn_messenger_errno(self.messenger)

        if error:
            text = cproton.pn_error_text(
                cproton.pn_messenger_error(
                    self.messenger))
            _MQLightMessenger._raise_error(text)

        LOG.exit('_MQLightMessenger.Unsubscribe', NO_CLIENT_ID, True)
        return True
