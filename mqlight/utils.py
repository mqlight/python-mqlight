# <copyright
# notice="lm-source-program"
# pids="5725-P60"
# years="2013,2016"
# crc="3568777996" >
# Licensed Materials - Property of IBM
#
# 5725-P60
#
# (C) Copyright IBM Corp. 2016
#
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
# </copyright>

import sys
from .definitions import QOS, QOS_AT_MOST_ONCE, QOS_AT_LEAST_ONCE
from .logging import get_logger, NO_CLIENT_ID
from .exceptions import InvalidArgumentError, RangeError, \
    InternalError, NetworkError, SecurityError
import sys
import re
import inspect
import socket
import os
import traceback
import ssl
try:
    import httplib
    from urlparse import urlsplit, urlunsplit
except ImportError:
    import http.client as httplib
    from urllib.parse import urlsplit, urlunsplit
from json import loads, dumps
PYTHON2 = sys.version_info < (3, 0)
PYTHON3 = sys.version_info >= (3, 0)

# Set up logging (to stderr by default). The level of output is
# configured by the value of the MQLIGHT_NODE_LOG environment
# variable. The default is 'ffdc'.
LOG = get_logger(__name__)

CMD = ' '.join(sys.argv)
if 'setup.py test' in CMD or 'py.test' in CMD or \
        'unittest' in CMD or 'runfiles.py' in CMD:
    from tests import stub_httplib as httplib
    reportFFDC = False
    unittest = True
else:
    reportFFDC = True
    unittest = False
    try:
        import httplib
    except ImportError:
        import http.client as httplib


class BadItem(object):
    pass


class SubscriptionRecordBuildBase(object):
    """
    A base class holding the share build value for the Subscribe and
    Unsubscribe records.
    """
    def __init__(self, client_id, sub_items_defaults):
        self.sub_items = sub_items_defaults
        self.sub_items['client_id'] = client_id
        self._id = client_id

    def set_topic_pattern(self, topic_pattern):
        if topic_pattern is None or topic_pattern == '':
            raise InvalidArgumentError(
                'Cannot subscribe to an empty pattern')
        if isinstance(topic_pattern, str) or \
                isinstance(topic_pattern, unicode):
                self.sub_items['topic_pattern'] = str(topic_pattern)
        else:
            raise InvalidArgumentError(
                'topic_pattern argument of type {0} is not supported'
                .format(str(type(topic_pattern))))

    def set_share(self, share):
        if share is not None:
            if isinstance(share, str) or isinstance(share, unicode):
                share = str(share)
                if ':' in share:
                    raise InvalidArgumentError(
                        'share argument value {0} is invalid because it '
                        'contains a colon character'.format(share))
            else:
                raise InvalidArgumentError(
                    'share argument of type {0} is invalid because must be '
                    'of type str'.format(str(type(share))))
        self.sub_items['share'] = share


class SubscriptionRecordBase(object):
    """
    """
    def __init__(self, details):
        self.sub_items = details
        self._ignore = False

    def _get_ttl(self):
        return self.sub_items['ttl']
    ttl = property(_get_ttl)

    def _get_topic_pattern(self):
        return self.sub_items['topic_pattern']
    topic_pattern = property(_get_topic_pattern)

    def _get_share(self):
        return self.sub_items['share']
    share = property(_get_share)

    def _set_ignore(self, ignore_parm):
        self._ignore = ignore_parm

    def _get_ignore(self):
        return self._ignore
    ignore = property(_get_ignore, _set_ignore)

    def _get_client_id(self):
        return self.sub_items['client_id']
    client_id = property(_get_client_id)

    def generate_address_with_share(self, service):
        if 'share' in self.sub_items and self.sub_items['share'] is not None:
            share = 'share:{0}'.format(self.sub_items['share'])
        else:
            share = 'private'
        return '{0}/{1}:{2}'.format(
                                    service, share,
                                    self.sub_items['topic_pattern'])

    def generate_address_without_share(self, service):
        return '{0}/{1}'.format(service, self.sub_items['topic_pattern'])

    def is_same(self, sub):
        if isinstance(sub, SubscriptionRecord):
            if self.topic_pattern != sub.topic_pattern:
                return False
            if self.share != sub.share:
                return False
            return True
        else:
            LOG.ffdc(
                'Client.SubscriptionRecordBuild.build_scription_record',
                'ffdc005',
                self._id,
                err=InternalError(
                    'Invalid parameter - type {0}'.format(sub.__name__)))

    def does_match(self, topic_pattern, share):
        if topic_pattern != self.sub_items['topic_pattern']:
            return False
        if share != self.sub_items['share']:
            return False
        return True

    def __str__(self):
        """
        Described the information is string format for use
        with diagnostics and FFDC reports.
        """
        buff = list('SubscriptionRecordBase: ')
        client_id = self.sub_items['client_id'] \
            if 'client_id' in self.sub_items \
            else '<missing>'
        topic = self.sub_items['topic_pattern'] \
            if 'topic_pattern' in self.sub_items \
            else '<missing>'
        share = self.sub_items['share'] \
            if 'share' in self.sub_items \
            else '<empty>'
        ttl = self.sub_items['ttl'] \
            if 'ttl' in self.sub_items \
            else '<missing>'
        buff.append(
            'ClientId: {0} Topic: {1}, Share: {2} TTL: {3}, Ignore: {4}'.
            format(client_id, topic, share, ttl, self.ignore))
        return ''.join(buff)


class SubscriptionRecordBuild(SubscriptionRecordBuildBase):
    """
    """
    def __init__(self, client_id):
        self.default_sub_items = {
                    'qos': QOS_AT_MOST_ONCE,
                    'auto_confirm': True,
                    'ttl': 0,
                    'credit': 1024,
                    'topic_pattern': BadItem,
                    'share': None,
                    'options': None,
                    'on_subscribed': None,
                    'on_message': None,
                    'client_id': client_id
                }
        SubscriptionRecordBuildBase.__init__(
                                             self, client_id,
                                             self.default_sub_items)

    def set_options(self, options):
        if options is not None:
            if not isinstance(options, dict):
                raise TypeError('options must be a dict')
            if 'qos' in options:
                if options['qos'] in QOS:
                    self.sub_items['qos'] = options['qos']
                else:
                    raise RangeError(
                        'options[\'qos\'] value {0} is invalid must evaluate '
                        'to 0 or 1'.format(options['qos']))
            if 'auto_confirm' in options:
                if options['auto_confirm'] in (True, False):
                    self.sub_items['auto_confirm'] = options['auto_confirm']
                else:
                    raise TypeError(
                        'options[\'auto_confirm\'] value {0} is invalid must '
                        'evaluate to True or False'.format(
                            options['auto_confirm']))
            if 'ttl' in options:
                ttl = options['ttl']
                if not isinstance(ttl, int):
                    if PYTHON2 and not isinstance(ttl, long):
                        raise TypeError('options[\'ttl\'] must be of type '
                                        'integer or long')
                if ttl < 0:
                    raise RangeError(
                        'options[\'ttl\'] value {0} is invalid must be an '
                        'unsigned integer number'.format(options['ttl']))
                if ttl > 4294967295:
                    # Cap at max AMQP value for TTL (2^32-1)
                    ttl = 4294967295
                self.sub_items['ttl'] = int(ttl / 1000)

            if 'credit' in options:
                try:
                    credit = int(options['credit'])
                    if credit < 0:
                        raise TypeError()
                    self.sub_items['credit'] = credit
                except Exception:
                    raise RangeError(
                        'options[\'credit\'] value {0} is invalid. It must be '
                        'an unsigned integer number'.format(options['credit']))

    def set_on_subscribed(self, on_subscribed):
        if on_subscribed is not None:
            validate_callback_function('on_subscribed', on_subscribed, 4, 0)
        self.sub_items['on_subscribed'] = on_subscribed

    def set_on_message(self, on_message):
        if on_message is not None:
            validate_callback_function('on_message', on_message, 3, 0)
        self.sub_items['on_message'] = on_message

    def build_subscription_record(self):
        LOG.entry(
                  'SubscriptionRecordBuild.build_subscription_record',
                  self._id)
        try:
            for sub_item in self.sub_items:
                if isinstance(sub_item, BadItem):
                    raise InternalError(
                        'Mandatory subscription {0} has not been'
                        ' assigned a value')
            subscription_record = SubscriptionRecord(self.sub_items)
            LOG.exit('SubscriptionRecordBuild.build_subscription_record',
                     self._id,
                     subscription_record.sub_items)
            return subscription_record
        except Exception:
            LOG.ffdc(
                     'SubscriptionRecordBuild.build_scription_record',
                     'ffdc004', self._id, err=sys.exc_info())
            return None


class SubscriptionRecord(SubscriptionRecordBase):
    """
    The built record of the elements relating to a subscription
    """
    def __init__(self, details):
        SubscriptionRecordBase.__init__(self, details)
        self._confirmed = 0
        self._unconfirmed = 0

    def _get_qos(self):
        return self.sub_items['qos']
    qos = property(_get_qos)

    def _get_credit(self):
        return self.sub_items['credit']
    credit = property(_get_credit)

    def _get_auto_confirm(self):
        return self.sub_items['auto_confirm']
    auto_confirm = property(_get_auto_confirm)

    def _get_options(self):
        return self.sub_items['options']
    options = property(_get_options)

    def _get_on_subscribed(self):
        return self.sub_items['on_subscribed']
    on_subscribed = property(_get_on_subscribed)

    def _get_on_message(self):
        return self.sub_items['on_message']
    on_message = property(_get_on_message)

    def _set_confirmed(self, confirmed):
        self._confirmed = confirmed

    def _get_confirmed(self):
            return self._confirmed
    confirmed = property(_get_confirmed, _set_confirmed)

    def _set_unconfirmed(self, unconfirmed):
        self._unconfirmed = unconfirmed

    def _get_unconfirmed(self):
        return self._unconfirmed
    unconfirmed = property(_get_unconfirmed, _set_unconfirmed)

    def __str__(self):
        """
        Described the information is string format for use
        with diagnostics and FFDC reports.
        """
        buff = list(SubscriptionRecordBase.__str__(self))
        buff.append('SubscriptionRecord: ')
        buff.append('QOS: {0}, Credit:{2}, AutoConfirm: {3}'.format(
                self.sub_items['qos'],
                self.sub_items['ttl'],
                self.sub_items['credit'],
                self.sub_items['auto_confirm']))
        buff.append(', Confirmed: {0}, Unconfirmed: {1}'.format(
                self._confirmed, self._unconfirmed))
        return ''.join(buff)


class UnsubscriptionRecordBuild(SubscriptionRecordBuildBase):
    """
    """
    def __init__(self, client_id, service):
        self.sub_items_defaults = {
                    'topic_pattern': BadItem,
                    'share': None,
                    'ttl': None,
                    'on_unsubscribed': None,
                    'client_id': client_id,
                    'service': service
                }
        SubscriptionRecordBuildBase.__init__(
                                            self,
                                            client_id,
                                            self.sub_items_defaults)
        self._id = client_id

    def set_options(self, options):
        # Validate the options parameter, when specified
        if options is not None:
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
                except Exception:
                    raise RangeError(
                        'options[\'ttl\'] value {0} is invalid, only 0 is a '
                        'supported value for  an unsubscribe request'.format(
                            options['ttl']))
                self.sub_items['ttl'] = int(ttl / 1000)

    def set_on_unsubscribed(self, on_unsubscribed):
        if on_unsubscribed is not None:
            validate_callback_function('on_unsubscribed',
                                       on_unsubscribed, 4, 0)
        self.sub_items['on_unsubscribed'] = on_unsubscribed

    def build_unsubscription_record(self):
        LOG.entry('UnsubscriptionRecordBuild.build_subscription_record',
                  self._id)
        try:
            # Subscribe using the specified pattern and share options
            if self.sub_items['share']:
                self.sub_items['with_share_address'] = '{0}/{1}{2}'.format(
                                          self.sub_items['service'],
                                          self.sub_items['share'],
                                          self.sub_items['topic_pattern'])
            else:
                self.sub_items['with_share_address'] = \
                    '{0}/private:{1}'.format(
                                             self.sub_items['service'],
                                             self.sub_items['topic_pattern'])

            self.sub_items['without_share_address'] = '{0}/{1}'.format(
                self.sub_items['service'],
                self.sub_items['topic_pattern'])
            subscription_record = UnsubscriptionRecord(self.sub_items)
            LOG.exit('UnsubscriptionRecordBuild.build_subscription_record',
                     self._id,
                     self.sub_items)
            return subscription_record
        except Exception:
            LOG.ffdc(
                     'UnsubscriptionRecordBuild.build_scription_record',
                     'ffdc005', self._id, err=sys.exc_info())
            return None


class UnsubscriptionRecord(SubscriptionRecordBase):
    """
    """
    def __init__(self, details):
        SubscriptionRecordBase.__init__(self, details)

    def _get_on_unsubscribed(self):
        return self.sub_items['on_unsubscribed']
    on_unsubscribed = property(_get_on_unsubscribed)

    def _get_options(self):
        return {'ttl': self.sub_items['ttl']}
    options = property(_get_options)

    def __str__(self):
        """
        Described the information is string format for use
        with diagnostics and FFDC reports.
        """
        buff = list(SubscriptionRecordBase.__str__(self))
        return ''.join(buff)


class SendRecordBuild(object):
    """
    """
    def __init__(self, client_id):
        self.sub_items = {
                'topic': BadItem,
                'data': None,
                'qos': QOS_AT_MOST_ONCE,
                'ttl': None,
                'on_sent': None,
                'client_id': client_id,
                }
        self._id = client_id

    def set_topic(self, topic):
        if topic is None or topic == '':
            raise InvalidArgumentError('Cannot subscribe to an empty pattern')
        if isinstance(topic, str) or isinstance(topic, unicode):
            self.sub_items['topic'] = str(topic)
        else:
            raise InvalidArgumentError(
                'topic argument of type {0} is not supported'
                .format(str(type(topic))))

    def set_options(self, options):
        # Validate the options parameter, when specified
        if options is not None:
            if isinstance(options, dict):
                LOG.parms(self._id, 'options:', options)
            else:
                error = TypeError(
                    'options must be a dict type not a {0}'.format(
                        type(options)))
                LOG.error('Client.unsubscribe', self._id, error)
                raise error

            if 'qos' in options:
                if options['qos'] in QOS:
                    self.sub_items['qos'] = options['qos']
                else:
                    raise RangeError(
                        'options[\'qos\'] value {0} is invalid must evaluate '
                        'to 0 or 1'.format(options['qos']))

            if 'ttl' in options:
                ttl = options['ttl']
                if not isinstance(ttl, int):
                    if PYTHON2 and not isinstance(ttl, long):
                        raise TypeError('options[\'ttl\'] must be of type '
                                        'integer or long')
                if ttl <= 0:
                    raise RangeError(
                        'options[\'ttl\'] value {0} is invalid, must be an '
                        'unsigned non-zero integer number'
                        .format(options['ttl']))
                if ttl > 2592000000:
                    raise RangeError(
                        'options[\'ttl\'] value {0} is invalid, message has a '
                        'time to live value which exceeds the maximum value'
                        .format(options['ttl']))
                self.sub_items['ttl'] = ttl

    def set_data(self, data):
        if data is None:
            raise TypeError('Cannot send no data')
        elif hasattr(data, '__call__'):
            raise TypeError('Cannot send a function')
        self.sub_items['data'] = data

    def set_on_sent(self, on_sent):
        if on_sent is not None:
            validate_callback_function('on_sent', on_sent, 5, 0)
            self.sub_items['on_sent'] = on_sent

    def build_send_record(self):
        LOG.entry('SendRecordBuild.build_send_record', self._id)
        if self.sub_items['qos'] == QOS_AT_LEAST_ONCE and \
                self.sub_items['on_sent'] is None:
                raise InvalidArgumentError(
                    'on_sent must be specified when options[\'qos\'] value '
                    'of 1 (at least once) is specified')

        send_record = SendRecord(self.sub_items)
        LOG.exit('SendRecordBuild.build_send_record', self._id, send_record)
        return send_record


class SendRecord(object):
    """
    The built record of the elements relating to a send
    """
    def __init__(self, details):
        self.sub_items = details

    def _get_topic(self):
        return self.sub_items['topic']
    topic = property(_get_topic)

    def _get_qos(self):
        return self.sub_items['qos']
    qos = property(_get_qos)

    def _get_ttl(self):
        return self.sub_items['ttl']
    ttl = property(_get_ttl)

    def _get_on_sent(self):
        return self.sub_items['on_sent']
    on_sent = property(_get_on_sent)

    def _get_data(self):
        return self.sub_items['data']
    data = property(_get_data)

    def _set_error(self, error):
        self.sub_items['error'] = error

    def _get_error(self):
        return self.sub_items['error']
    error = property(_get_error, _set_error)

    def _get_options(self):
        return {'qos': self.qos, 'ttl': self.ttl}
    options = property(_get_options)

    def _set_message(self, message):
        self.sub_items['message'] = message

    def _get_message(self):
        return self.sub_items['message']
    message = property(_get_message, _set_message)

    def __str__(self):
        """
        Described the information is string format for use
        with diagnostics and FFDC reports.
        """
        buff = list('SendRecord: ')
        try:
            topic = self.sub_items['topic']
        except:
            topic = '<missing>'
        buff.append('Topic: {0},'.format(topic))

        try:
            share = self.sub_items['share']
        except:
            share = '<empty>'
        buff.append('Share: {0},'.format(share))

        try:
            message = self.sub_items['message']
        except:
            message = '<Empty>'
        buff.append('Message: {0},'.format(message))

        try:
            data = self._trim(self.sub_items['data'])
        except:
            data = '<Empty>'
        buff.append('data: {0},'.format(data))

        buff.append('QOS: {0}, TTL: {1}'.format(
                                                  self.sub_items['qos'],
                                                  self.sub_items['ttl']))
        return ''.join(buff)

    def _trim(self, value):
        if not isinstance(value, str):
            value = str(value)
        return (value[:200] + '...') if len(value) > 200 else value


class ManagedList(object):
    """
    comparison_func (thisItem, ** args)
    """
    def __init__(self, ident, object_type, comparison_func):
        self._id = ident
        if comparison_func is not None and \
                not hasattr(comparison_func, '__call__'):
            err = InternalError('ManagedList comparison argument must be a '
                                'function, but {0} given'.
                                format(str(type(comparison_func))))
            if reportFFDC:
                LOG.ffdc(
                         'ManagedQueues.validate',
                         'ffdc001',
                         self._id,
                         err=err)
        self._object_type = object_type
        self._comparison_func = comparison_func
        self._itemList = []

    def validate(self, p):
        if not isinstance(p, self._object_type):
            err = InternalError('ManagedList configured for type {0} but '
                                'given {1}'.format(
                                                   self._object_type,
                                                   str(type(p))))
            if reportFFDC:
                LOG.ffdc(
                         'ManagedQueues.validate',
                         'ffdc002',
                         self._id,
                         err=err)
            raise err

    def append(self, appendItem):
        """
        Add the given item to the end of thelist.
        """
        self.validate(appendItem)
        self._itemList.append(appendItem)

    def remove(self, *args):
        """
        Remove the item given in the arg. See the find method for
        argument options.
        """
        try:
            removeItem = self.find(*args)
            self._itemList.remove(removeItem)
        except ValueError:
            pass

    def clear(self):
        self._itemList = []

    def find(self, *args):
        """
        Locates the item in the list based on the arguments given.
        If the single argument is the same type then the list will be
        search for that item.
        If not of the same argument then all comparison is used to
        locate any matching items.
        If a match is found then that item is return otherwise None.
        """
        if isinstance(args[0], self._object_type):
            m = [item for item in self._itemList if item == args[0]]
            return args[0] if len(m) > 0 else None
        else:
            if self._comparison_func is None:
                LOG.ffdc(
                    'Utils.ManageRecord.find',
                    'ffdc006',
                    self._id,
                    err=InternalError(
                        'No comparison method supplied on construction'))
            matched_items = [item for item in self._itemList
                             if self._comparison_func(item, *args)]
            return matched_items[0] if len(matched_items) > 0 else None

    def pop(self):
        """
        Will remove the first item in the list and return it
        """
        if len(self._itemList) == 0:
            return None
        item = self._itemList[0]
        del self._itemList[0]
        return item

    def read(self):
        """
        Will read the next message without removing it
        """
        if len(self._itemList) == 0:
            return None
        item = self._itemList[0]
        return item

    def _empty(self):
        """
        Return true is the list is empty of items.
        """
        return len(self._itemList) == 0
    empty = property(_empty)

    def _list(self):
        """
        The list used for iterating over the values
        """
        return self._itemList
    list = property(_list)

    def __str__(self):
        return str(self._itemList)


class Security(object):
    def __init__(self, args):
        LOG.entry('Security.__init__', NO_CLIENT_ID)
        LOG.parms(NO_CLIENT_ID, 'args:', hide_password(args))
        if args is None:
            args = {}
        self._ssl_client_certificate = args.get('ssl_client_certificate', None)
        self._ssl_trust_certificate = args.get('ssl_trust_certificate', None)
        self._ssl_client_key = args.get('ssl_client_key', None)
        self._ssl_client_key_passphrase = \
            args.get('ssl_client_key_passphrase', None)
        self._ssl_verify_name = args.get('ssl_verify_name', True)
        self._user = args.get('user', None)
        self._password = args.get('password', None)

        def raise_error(msg):
            error = InvalidArgumentError(msg)
            LOG.error('Security.__init__', NO_CLIENT_ID, error)
            raise error

        def validate_string_argument(name, argument):
            if argument is not None:
                if not isinstance(argument, str) and \
                        not isinstance(argument, unicode):
                    raise InvalidArgumentError(
                        name + ' must be a string argument')

        def validate_file_argument(name, argument):
            if argument is not None:
                if not os.path.isfile(argument) and not unittest:
                    raise_error(name + ' is not a validate accessible file')

        def validate_key_passphrase(key_name, password_name,
                                    key, password):
            validate_string_argument(key_name, key)
            validate_string_argument(password_name, password)
            if key is not None:
                if password is None:
                    raise_error(password_name + ' is required for given ' +
                                key_name)
                validate_file_argument(key_name, key)
            elif password is not None:
                raise_error(key_name + ' is required for given ' +
                            password_name)

        def validate_user_passphrase(user_name, password_name,
                                     user, password):
            validate_string_argument(user_name, user)
            validate_string_argument(password_name, password)
            if user is not None:
                if password is None:
                    raise_error('Both {0} and {1} properties must be '
                                'specified together'.
                                format(user_name, password_name))
            elif password is not None:
                raise_error('Both {0} and {1} properties must be specified '
                            'together'.format(user_name, password_name))

        def get_first_in_group(args, group):
            for item in group:
                if item in args:
                    return item
            return None

        def validate_complete_group(args, group):
            missing = False
            present = False
            for item in group:
                if item in args:
                    present = True
                if item not in args:
                    missing = True
            if missing and present:
                raise_error('ssl_client_certificate, ssl_client_key '
                            'and ssl_client_key_passphrase options '
                            'must all be specified')
        """
        Validate of the given name against the known list.
        """
        names = [
                    'ssl_client_certificate',
                    'ssl_trust_certificate',
                    'ssl_client_key',
                    'ssl_client_key_passphrase',
                    'ssl_verify_name',
                    'user',
                    'password'
                 ]
        if args is not None:
            for arg in args:
                if arg not in names:
                    raise_error(arg + ' is not a valid name for the security '
                                'options.')

            first_in_client = get_first_in_group(
                                            args,
                                            ['ssl_client_certificate',
                                             'ssl_client_key',
                                             'ssl_client_key_passphrase'])
            first_in_trust = get_first_in_group(args,
                                                ['ssl_trust_certificate'])
            if first_in_client is None:
                if first_in_trust is None:
                    self._ssl = False
                    self._client_cert = False
                else:
                    self._ssl = True
                    self._client_cert = False
            else:
                if first_in_trust is None:
                    raise_error('Trust certificate is require when client '
                                'certificate is present')
                else:
                    self._ssl = True
                    self._client_cert = True
        else:
            self._ssl = False
            self._client_cert = False

        if self.ssl:
            if self._client_cert:
                validate_complete_group(args,
                                        ['ssl_client_certificate',
                                         'ssl_client_key',
                                         'ssl_client_key_passphrase'])
                validate_string_argument('Client certificate',
                                         self._ssl_client_certificate)
                validate_key_passphrase(
                                'ssl_client_key',
                                'ssl_client_key_passphrase',
                                self._ssl_client_key,
                                self._ssl_client_key_passphrase)

            validate_string_argument('Trust certificate',
                                     self._ssl_trust_certificate)
            validate_file_argument('Trust certificate',
                                   self._ssl_trust_certificate)

        if self._ssl_verify_name not in [True, False]:
            raise_error(
                'ssl_verify_name value {0} is invalid. '
                'Must evaluate to True of False'.format(self._ssl_verify_name))

        validate_user_passphrase('user', 'password',
                                 self._user, self._password)

        LOG.exit('Security.__init__', NO_CLIENT_ID, None)

    def _is_ssl(self):
        return self._ssl
    ssl = property(_is_ssl)

    def _is_client_cert(self):
        return self._client_cert
    client_cert = property(_is_client_cert)

    def _get_ssl_client_certificate(self):
        return self._ssl_client_certificate
    ssl_client_certificate = property(_get_ssl_client_certificate)

    def _get_ssl_trust_certificate(self):
        return self._ssl_trust_certificate
    ssl_trust_certificate = property(_get_ssl_trust_certificate)

    def _get_ssl_client_key(self):
        return self._ssl_client_key
    ssl_client_key = property(_get_ssl_client_key)

    def _get_ssl_client_key_passphrase(self):
        return self._ssl_client_key_passphrase
    ssl_client_key_passphrase = \
        property(_get_ssl_client_key_passphrase)

    def _get_ssl_verify_name(self):
        return self._ssl_verify_name
    ssl_verify_name = property(_get_ssl_verify_name)

    def _get_user(self):
        return self._user
    user = property(_get_user)

    def _get_password(self):
        return self._password
    password = property(_get_password)

    def _get_context(self):
        LOG.entry('Security._get_context', NO_CLIENT_ID)
        ctx = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
        # FIXME sort out the constant access
        ctx.options |= ssl.OP_NO_SSLv2 | ssl.OP_NO_SSLv3
# ?!?        ctx.options |= ssl.getattr(_ssl, "OP_NO_COMPRESSION", 0)

        def _load_trust_certificate():
            try:
                ctx.load_verify_locations(self._ssl_trust_certificate)
            except Exception as e:
                raise SecurityError('Failed to load the Trust certificate '
                                    'because {0}'.format(e))

        def _load_client_certificate():
            try:
                ctx.load_cert_chain(self._ssl_client_certificate,
                                    self._ssl_client_key,
                                    self._ssl_client_key_passphrase)
            except Exception as e:
                raise SecurityError('Failed to load the Client certificate & '
                                    'Key because {0}'.format(e))

        if self._client_cert:
            ctx.verify_mode = ssl.CERT_REQUIRED
            ctx.check_hostname = self._ssl_verify_name
            _load_trust_certificate()
            _load_client_certificate()
        elif self._ssl_trust_certificate:
            ctx.verify_mode = ssl.CERT_REQUIRED
            ctx.check_hostname = self._ssl_verify_name
            _load_trust_certificate()
        else:
            ctx.verify_mode = ssl.CERT_NONE
        LOG.exit('Security._get_context', NO_CLIENT_ID, ctx)
        return ctx
    context = property(_get_context)

    def __str__(self):
        return 'SecurityOptions[' \
            'user: {0}, ' \
            'password: {1}, ' \
            'ssl_trust_certificate: {2}, ' \
            'ssl_client_certificate: {3}, ' \
            'ssl_client_key: {4}, ' \
            'ssl_client_key_passphrase: {5}, ' \
            'ssl_verify_name: {6}, ' \
            'ssl : {7}, ' \
            'client_cert/Key: {8}, ' \
            ']'.format(
                self._user,
                ('****' if self._password else 'None'),
                self._ssl_trust_certificate,
                self._ssl_client_certificate,
                self._ssl_client_key,
                ('****' if self._ssl_client_key_passphrase else 'None'),
                self._ssl_verify_name,
                self._ssl,
                self._client_cert)


class ServiceGenerator(object):
    """
    This class handles the sourcing and organising of the list of services
    to be connected to.
    This client can be supplied with a static list of service connections or
    take a callout function to supplied a list of services.

    The single method for this class returns a list of Service[Class] via
    a callback function. For dynamic configuration the list will be refresh
    each time the method is called.
    """

    def __init__(self, service_parm, security_options):
        LOG.entry('ServiceGenerator.__init__', NO_CLIENT_ID)
        LOG.parms(NO_CLIENT_ID, 'service_parm:', service_parm)
        LOG.parms(NO_CLIENT_ID, 'security_options:', security_options)
        self._security_options = security_options
        # performed singleton
        if isinstance(service_parm, str) or \
                isinstance(service_parm, unicode):
            # Blue mix function to recall
            scheme = urlsplit(service_parm).scheme
            if scheme in ('http', 'https'):
                self._service_function = \
                    self._get_http_service_function(service_parm)
                self._services = None
            elif scheme in ('amqp', 'amqps'):
                self._services = self._wrap_services(service_parm)
                self._service_function = None
            else:
                err = InvalidArgumentError('Schema is an unsupported type')
                LOG.error('_http_service_function', NO_CLIENT_ID, err)
                raise err
        elif isinstance(service_parm, list):
            self._services = self._wrap_services(service_parm)
            self._service_function = None
        # User supplied function to call
        elif hasattr(service_parm, '__call__'):
            self._service_function = service_parm
            self._services = None
        else:
            err = InvalidArgumentError('Service is an unsupported type')
            LOG.error('_http_service_function', NO_CLIENT_ID, err)
            raise err
        LOG.exit('ServiceGenerator.__init__', NO_CLIENT_ID, None)

    def _wrap_services(self, service_parm):
        LOG.entry('ServiceGenerator._wrap_services', NO_CLIENT_ID)
        LOG.parms(NO_CLIENT_ID, 'service_parm:', service_parm)
        if isinstance(service_parm, str):
            services = [Service(service_parm, self._security_options)]
        # Performed multiple
        elif isinstance(service_parm, list):
            services = []
            for service in service_parm:
                services.append(Service(str(service), self._security_options))
        # unicode needs to be converted to str - python 2.x only
        elif PYTHON2 and isinstance(service_parm, unicode):
            services = [Service(str(service_parm), self._security_options)]
        LOG.exit('ServiceGenerator._wrap_services', NO_CLIENT_ID, None)
        return services

    def get_service_list(self, callback):
        LOG.entry('ServiceGenerator.get_service_list', NO_CLIENT_ID)
        LOG.parms(NO_CLIENT_ID, 'callback:', callback)

        def _get_service_list_callback(err, service_parm):
            services = [] if service_parm is None else \
                self._wrap_services(service_parm)
            callback(err, services)

        if self._services is not None:
            callback(None, self._services)
        else:
            self._service_function(_get_service_list_callback)
        LOG.exit('ServiceGenerator.get_service_list', NO_CLIENT_ID, None)

    def _get_http_service_function(self, http):
        """
        Function to take a single HTTP URL and using the JSON retrieved from
        it to return an array of service URLs.
        """
        LOG.entry('ServiceGenerator._get_http_service_function', NO_CLIENT_ID)
        LOG.parms(NO_CLIENT_ID, 'http:', http)
        http_url = urlsplit(http)

        def _http_service_function(callback):
            LOG.entry('ServiceGenerator._http_service_function', NO_CLIENT_ID)
            LOG.parms(NO_CLIENT_ID, 'callback:', callback)
            if http_url.scheme == 'https':
                func = httplib.HTTPSConnection
            else:
                func = httplib.HTTPConnection
            LOG.data(NO_CLIENT_ID, 'using :', func.__name__)
            host = http_url.netloc
            LOG.data(NO_CLIENT_ID, 'host:', host)
            path = http[http.index(http_url.netloc) + len(http_url.netloc):]
            LOG.data(NO_CLIENT_ID, 'path:', path)
            try:
                conn = func(host)
                conn.request('GET', path)
                res = conn.getresponse()
                if res.status == httplib.OK:
                    try:
                        json_msg = res.read()
                        if PYTHON3:
                            json_msg = json_msg.decode("utf-8")
                        json_obj = loads(json_msg)
                        if 'service' in json_obj:
                            service = json_obj['service']
                        else:
                            service = None
                        LOG.data(NO_CLIENT_ID, 'service:', service)
                        callback(None, service)
                    except Exception as exc:
                        err = TypeError(
                            '{0} request to {1} returned '
                            'unparseable JSON: {2}\nJSON={3}'.format(
                                http_url.scheme, http, exc, json_msg))
                        LOG.error('ServiceGenerator._http_service_function',
                                  NO_CLIENT_ID, err)
                        callback(err, None)
                else:
                    err = NetworkError(
                        '{0} request to {1} failed with a status code '
                        'of {2}'.format(http_url.scheme, http, res.status))
                    LOG.error('ServiceGenerator._http_service_function',
                              NO_CLIENT_ID, err)
                    callback(err, None)
            except (httplib.HTTPException, socket.error) as exc:
                err = NetworkError(
                    '{0} request to {1} failed: {2}'.format(
                        http_url.scheme, http, exc))
                LOG.error('ServiceGenerator._http_service_function',
                          NO_CLIENT_ID, err)
                callback(err, None)
            LOG.exit('ServiceGenerator._http_service_function',
                     NO_CLIENT_ID, None)

        LOG.exit(
            'ServiceGenerator._get_http_service_function',
            NO_CLIENT_ID,
            _http_service_function)
        return _http_service_function


class Service(object):
    """
    This class handling the data elements within a service connection.
    Whether necessary URL's will be updated with authentication information
    and supply methods to return selected information.
    Note: this class will include password information within some method
    return values
    """
    def __init__(self, service, security_options):
        LOG.entry('Service.__init__', NO_CLIENT_ID)
        LOG.parms(NO_CLIENT_ID, 'service:', service)
        LOG.parms(NO_CLIENT_ID, 'security_options:', security_options)
        unpacked = urlsplit(service)
        self._scheme = unpacked.scheme
        self._user = unpacked.username
        self._password = unpacked.password
        self._host = unpacked.hostname

        def raise_error(msg):
            error = InvalidArgumentError(msg)
            LOG.error('Service.__init__', NO_CLIENT_ID, error)
            raise error

        try:
            self._port = unpacked.port
        except:
            raise_error('Service {0} is not a valid URL'.format(service))
        self._path = unpacked.path

        def raise_error(msg):
            error = InvalidArgumentError(msg)
            LOG.error('Service.__init__', NO_CLIENT_ID, error)
            raise error

        def _validate_netloc(unpacked):
            buff = list()
            if unpacked.username is not None:
                buff.append(unpacked.username)
                buff.append(':')
            if unpacked.password is not None:
                buff.append(unpacked.password)
                buff.append('@')
            buff.append(unpacked.hostname)
            if unpacked.port is not None:
                buff.append(':')
                buff.append(str(unpacked.port))
            expected_netloc = ''.join(buff)
            if unpacked.netloc.lower() != expected_netloc.lower():
                raise_error('Service {0} is not a valid URL'.format(service))

        if (self._user is not None and self._password is None) or \
                (self._user is None and self._password is not None):
            raise_error('Both user and password must be specified together')
        if self._path != '/' and len(self._path) > 0:
            raise_error('Service {0} is not a valid URL'.format(service))
        if len(self._host) <= 0:
            raise_error('Service {0} is not a valid URL'.format(service))
        _validate_netloc(unpacked)

        def make_url(scheme, user, password, host, port):
            build_url = list(scheme)
            build_url.append('://')
            if self._user is not None and password is not None:
                build_url.append('{0}:{1}@'.format(user, password))
            build_url.append(host)
            build_url.append(':')
            build_url.append(port)
            return ''.join(build_url)
        if self._scheme is None:
            self._scheme = 'amqps' if security_options.ssl else 'amqp'
        if security_options.user is not None:
            self._user = security_options.user
            self._password = security_options.password
        if self._port is None:
            self._port = '5671' if self._scheme == 'amqps' else '5672'
        else:
            self._port = str(self._port)

        self._service = make_url(self._scheme,
                                 self._user,
                                 self._password,
                                 self._host,
                                 self._port)
        if self._password is None:
            self._log = self._service
        else:
            self._log = make_url(self._scheme,
                                 self._user,
                                 '****',
                                 self._host,
                                 self._port)
        self._pattern = make_url(self._scheme,
                                 None,
                                 None,
                                 self._host,
                                 self._port)
        LOG.exit('Service.__init__', NO_CLIENT_ID, None)

    def _get_address(self):
        return self._service
    address = property(_get_address)

    def _get_route_address(self):
        return '{0}/$1'.format(self._service)
    route_address = property(_get_route_address)

    def _get_route_pattern(self):
        return '{0}/*'.format(self._pattern)
    route_pattern = property(_get_route_pattern)

    def _get_host_port(self):
        return (self._host, int(self._port))
    host_port = property(_get_host_port)

    def _address_only(self):
        return self._pattern
    address_only = property(_address_only)

    def __str__(self):
        return str(self._log)

    def _is_ssl(self):
        return self._scheme == 'amqps'
    ssl = property(_is_ssl)


def decode_uri(uri):
    m = re.match('amqp:\/\/([^\/]*)(?:(\d+))?\/(private|share:(.*)):(.*)', uri)
    if m is None:
        raise InternalError('Invalid URI address {0}'.format(uri))
    return [m.group(5), m.group(4)]


def decode_link_address(uri):
    m = re.match('(?:private|share:(.*)):(.*)', uri)
    if m is None:
        raise InternalError('Invalid link address {0}'.format(uri))
    return [m.group(2), m.group(1)]


def is_text(value):
    if isinstance(value, str):
        return True
    if PYTHON2:
        return isinstance(value, unicode)
    return False


def validate_callback_function(name, callback, expected_args,
                               default_args, optional=True):
    if callback is None and optional:
        return
    if callback and not hasattr(callback, '__call__'):
                raise TypeError(name + ' must be a function')
    details = inspect.getargspec(callback)

    def _get_length(list):
        if list is None:
            return 0
        return len(list)

    num_args = _get_length(details[0])
    max_args = expected_args
    min_args = max_args - default_args
    hidden_args = 0
    if hasattr(callback, '__self__'):
        hidden_args += 1
    if num_args < min_args + hidden_args or num_args > max_args + hidden_args:
        s = '' if max_args == min_args else 'at least '
        actual_function = '{0}({1})'.format(callback.__name__, details[0])
        raise TypeError('{0} parameter is {1} this must be a function '
                        'with {2}{3} arguments'.
                        format(name, actual_function, s, min_args))


def hide_password(before):
    if before is None:
        return None

    after = before.copy()
    if PYTHON3:
        for key, value in after.items():
            if key == 'password':
                after[key] = '****'
    else:
        for key, value in after.iteritems():
            if key == 'password':
                after[key] = '****'
    return after
