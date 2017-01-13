"""
<copyright
notice="lm-source-program"
pids="5724-H72"
years="2013,2016"
crc="1615301132" >
Licensed Materials - Property of IBM

5725-P60

(C) Copyright IBM Corp. 2013, 2016

US Government Users Restricted Rights - Use, duplication or
disclosure restricted by GSA ADP Schedule Contract with
IBM Corp.
</copyright>
"""
# pylint: disable=bare-except,broad-except,invalid-name,no-self-use
# pylint: disable=too-many-public-methods,unused-argument
import unittest
import threading
import pytest
import traceback
from mqlight.utils import ManagedList, \
                        SubscriptionRecordBuild,SubscriptionRecord, \
                        UnsubscriptionRecordBuild,UnsubscriptionRecord, \
                        SendRecordBuild,SendRecord, decode_link_address, \
                        decode_uri, validate_callback_function, Security, \
                        Service, ServiceGenerator

from mqlight.exceptions import MQLightError, InvalidArgumentError, RangeError, \
    NetworkError, NotPermittedError, ReplacedError, StoppedError, \
    SubscribedError, UnsubscribedError, SecurityError, InternalError

from tests import stub_httplib as httplib


class Object001(object):
    def __init__(self,first,second):
        self._first = first
        self._second = second
        
    def _get_first(self):
        return self._first
    first = property(_get_first)
    
    def _get_second(self):
        return self._second
    second = property(_get_second)

class Object002(object):
    pass



class TestManagedList(unittest.TestCase):
    
    def comparison(self,item, first, second):
        if item.first != first:
            return False
        if item.second != second:
            return False
        return True
    
    item001 = Object001("aaa","bbb")
    item002 = Object001("aaa","ccc")
    item003 = Object001("ccc","bbb")
    item004 = Object001("ccc","ccc")
    

    
    def test_good_fill(self):
        managedList = ManagedList('id', Object001,self.comparison)
        managedList.append(self.item001)
        managedList.append(self.item002)
        managedList.append(self.item003)
        managedList.append(self.item004)

    def test_bad_fill(self):
        managedList = ManagedList('id', Object001,self.comparison)
        managedList.append(self.item001)
        managedList.append(self.item002)
        managedList.append(self.item003)
        with pytest.raises(InternalError):
            managedList.append(Object002())

    def test_find_present(self):
        managedList = ManagedList('id', Object001,self.comparison)
        managedList.append(self.item001)
        managedList.append(self.item002)
        managedList.append(self.item003)
        managedList.append(self.item004)
        assert managedList.find(self.item004) is not None

    def test_find_not_present(self):
        managedList = ManagedList('id', Object001,self.comparison)
        managedList.append(self.item001)
        managedList.append(self.item002)
        managedList.append(self.item003)
        assert managedList.find(self.item004) is None

    def test_mixed_fill(self):
        managedList = ManagedList('id', Object001,self.comparison)
        managedList.append(self.item001)
        with pytest.raises(InternalError):
            managedList.append(Object002())

    def test_fill_remove(self):
        managedList = ManagedList('id', Object001,self.comparison)
        managedList.append(self.item001)
        managedList.append(self.item002)
        managedList.append(self.item003)
        managedList.append(self.item004)
        assert len(managedList.list) == 4
        managedList.remove(self.item001)
        managedList.remove(self.item002)
        managedList.remove(self.item003)
        managedList.remove(self.item004)
        assert managedList.empty
        
    def test_comparison_find(self):
        managedList = ManagedList('id', Object001,self.comparison)
        managedList.append(self.item001)
        managedList.append(self.item002)
        managedList.append(self.item003)
        managedList.append(self.item004)
        assert managedList.find('aaa','ccc') is not None
        
    def test_comparison_not_find(self):
        managedList = ManagedList('id', Object001,self.comparison)
        managedList.append(self.item001)
        managedList.append(self.item002)
        managedList.append(self.item003)
        managedList.append(self.item004)
        assert managedList.find('aaa','zzz') is None


    def test_pop(self):
        managedList = ManagedList('id', Object001,self.comparison)
        managedList.append(self.item001)
        managedList.append(self.item002)
        managedList.append(self.item003)
        managedList.append(self.item004)
        item = managedList.pop()
        assert item == self.item001
        item = managedList.pop()
        assert item == self.item002
        item = managedList.pop()
        assert item == self.item003
        item = managedList.pop()
        assert item == self.item004
        assert managedList.empty



class TestSubscribe(unittest.TestCase):

    def func003(self, a,b,c):
        pass
    def func004(self, a,b,c,d):
        pass

    def test_basic_subscribe(self):
        sub_build = SubscriptionRecordBuild("client-id")
        sub_build.set_topic_pattern('pattern001')
        sub_build.set_share('share001')
        sub_build.set_options({
                   'auto_confirm' : True,
                   'qos' : 0,
                   'ttl' : 1000,
                   'credit' : 300})
        sub_build.set_on_subscribed(self.func004)
        sub_build.set_on_message(self.func003)
        
        sub = sub_build.build_subscription_record()
        assert sub.topic_pattern == 'pattern001'
        assert sub.share == 'share001'
        assert sub.qos == 0
        assert sub.ttl == 1
        assert sub.credit == 300
        assert sub.auto_confirm == True
        assert sub.on_subscribed == self.func004
        assert sub.on_message == self.func003

    def testbad_subscribe(self):
        sub_build = SubscriptionRecordBuild("client-id")
        # Bad topic_pattern
        with pytest.raises(InvalidArgumentError):
            sub_build.set_topic_pattern(None)
        with pytest.raises(InvalidArgumentError):
            sub_build.set_topic_pattern('')
        with pytest.raises(InvalidArgumentError):
            sub_build.set_topic_pattern(1234)
        # Bad Share
        with pytest.raises(InvalidArgumentError):
            sub_build.set_share(1234)
        with pytest.raises(InvalidArgumentError):
            sub_build.set_share('with:colon')
        # Bad options - qos
        with pytest.raises(RangeError):
            sub_build.set_options({'qos' : -1})
        with pytest.raises(RangeError):
            sub_build.set_options({'qos' : 2})
        # Bad options - ttl
        with pytest.raises(RangeError):
            sub_build.set_options({'ttl' : -1})
        # Bad options - credit
        with pytest.raises(RangeError):
            sub_build.set_options({'credit' : -1})
        # Bad options - auto confirm
        with pytest.raises(TypeError):
            sub_build.set_options({'auto_confirm' : 'True'})
        with pytest.raises(TypeError):
            sub_build.set_options({'auto_confirm' : 99})
        # Bad on_subscribed
        with pytest.raises(TypeError):
            sub_build.set_on_subscribed('func')
        with pytest.raises(TypeError):
            sub_build.set_on_subscribed(99)
        with pytest.raises(TypeError):
            sub_build.set_on_subscribed(self.func003)
                    # Bad on_subscribed
        with pytest.raises(TypeError):
            sub_build.set_on_message('func')
        with pytest.raises(TypeError):
            sub_build.set_on_message(99)
        with pytest.raises(TypeError):
            sub_build.set_on_message(self.func004)

    def testbad_support_methods(self):
        sub_build = SubscriptionRecordBuild("client-id")
        sub_build.set_topic_pattern('MyTopic')
        sub = sub_build.build_subscription_record()
        address = sub.generate_address_with_share('amqp://localhost')
        assert address == 'amqp://localhost/private:MyTopic'

        sub_build.set_share('Shared')
        sub = sub_build.build_subscription_record()
        address = sub.generate_address_with_share('amqp://localhost')
        assert address == 'amqp://localhost/share:Shared:MyTopic'


class TestUnsubscribe(unittest.TestCase):

    def func003(self, a,b,c):
        pass
    def func004(self, a,b,c,d):
        pass

    def test_basic_unsubscribe(self):
        sub_build = UnsubscriptionRecordBuild("client-id","amqp://localhost")
        sub_build.set_topic_pattern('pattern001')
        sub_build.set_share('share001')
        sub_build.set_options({'ttl' : 0})
        sub_build.set_on_unsubscribed(self.func004)
        
        sub = sub_build.build_unsubscription_record()
        assert sub.topic_pattern == 'pattern001'
        assert sub.share == 'share001'
        assert sub.ttl == 0
        assert sub.on_unsubscribed == self.func004

    def test_bad_unsubscribe(self):
        sub_build = UnsubscriptionRecordBuild("client-id","amqp://localhost")
        # Bad topic_pattern
        with pytest.raises(InvalidArgumentError):
            sub_build.set_topic_pattern(None)
        with pytest.raises(InvalidArgumentError):
            sub_build.set_topic_pattern('')
        with pytest.raises(InvalidArgumentError):
            sub_build.set_topic_pattern(1234)
        # Bad Share
        with pytest.raises(InvalidArgumentError):
            sub_build.set_share(1234)
        with pytest.raises(InvalidArgumentError):
            sub_build.set_share('with:colon')
        # Bad options - ttl
        with pytest.raises(RangeError):
            sub_build.set_options({'ttl' : -1})
        with pytest.raises(RangeError):
            sub_build.set_options({'ttl' : 1})
        # Bad on_subscribed
        with pytest.raises(TypeError):
            sub_build.set_on_unsubscribed('func')
        with pytest.raises(TypeError):
            sub_build.set_on_unsubscribed(99)
        with pytest.raises(TypeError):
            sub_build.set_on_unsubscribed(self.func003)


class TestSend(unittest.TestCase):

    def func004(self, a,b,c,d):
        pass
    def func005(self, a,b,c,d,e):
        pass

    def test_basic_send(self):
        sub_send = SendRecordBuild("client-id")
        sub_send.set_topic('pattern001')
        sub_send.set_data('Hello world')
        sub_send.set_options({'qos' : 0})
        sub_send.set_options({'ttl' : 5000})
        sub_send.set_on_sent(self.func005)
        send = sub_send.build_send_record()
        assert send.topic == 'pattern001'
        assert send.data == 'Hello world'
        assert send.ttl == 5000
        assert send.qos == 0
        assert send.on_sent == self.func005
        assert send.options['qos'] == 0
        assert send.options['ttl'] == 5000

    def test_bad_send(self):
        sub_send = SendRecordBuild("client-id")
        # Bad Data
        with pytest.raises(TypeError):
            sub_send.set_data(None)
        with pytest.raises(TypeError):
            sub_send.set_data(self.func005)
        # Bad topic
        with pytest.raises(InvalidArgumentError):
            sub_send.set_topic(None)
        # Bad options - qos
        with pytest.raises(RangeError):
            sub_send.set_options({'qos' : -1})
        with pytest.raises(RangeError):
            sub_send.set_options({'qos' : 2})
        # Bad options - ttl
        with pytest.raises(RangeError):
            sub_send.set_options({'ttl' : -1})
        # Bad options - ttl
        with pytest.raises(RangeError):
            sub_send.set_options({'ttl' : 0})
        # Bad options - ttl
        with pytest.raises(RangeError):
            sub_send.set_options({'ttl' : 2592000001})
        # Bad on_sent
        with pytest.raises(TypeError):
            sub_send.set_on_sent('func')
        with pytest.raises(TypeError):
            sub_send.set_on_sent(99)
        with pytest.raises(TypeError):
            sub_send.set_on_sent(self.func004)

class TestSecurity(unittest.TestCase):
    def test_golden(self):

        opt_no_ssl = dict([])
        s = Security(opt_no_ssl)
        assert not s.ssl

        opt_with_client = dict([
            ('ssl_client_certificate','certificate'),
            ('ssl_client_key','key'),
            ('ssl_client_key_passphrase','passphrase'),
            ('ssl_trust_certificate','certificate')
            ])
        s = Security(opt_with_client)
        assert s.ssl
        assert s.ssl_client_certificate == 'certificate'
        assert s.ssl_client_key == 'key'
        assert s.ssl_client_key_passphrase == 'passphrase'
        assert s.ssl_trust_certificate == 'certificate'
        assert s.ssl_verify_name

        opt_trust_only = dict([
                        ('ssl_verify_name', False),
                        ('ssl_trust_certificate','certificate')
                      ])
        s = Security(opt_trust_only)
        assert s.ssl
        assert s.ssl_client_certificate is None
        assert s.ssl_client_key is None
        assert s.ssl_client_key_passphrase is None
        assert s.ssl_trust_certificate  == 'certificate'
        assert not s.ssl_verify_name

        opt_none = dict([
                        ('ssl_verify_name', False),
                      ])
        s = Security(opt_none)
        assert not s.ssl
        assert s.ssl_client_certificate is None
        assert s.ssl_client_key is None
        assert s.ssl_client_key_passphrase is None
        assert s.ssl_trust_certificate is None
        assert not s.ssl_verify_name

        opt_user_password = dict([
                              ('user','user'),
                              ('password', 'password')
                              ])
        s = Security(opt_user_password)
        assert not s.ssl
        assert s.user == 'user'
        assert s.password == 'password'
 
    def test_bad_types(self):
        def func():
            pass

        def sub_type_test(var_type, var_name):
            with pytest.raises(InvalidArgumentError):
                Security(dict([(var_name, 1)]))
            with pytest.raises(InvalidArgumentError):
                Security(dict([(var_name, {'user}'})]))
            with pytest.raises(InvalidArgumentError):
                Security(dict([(var_name, func)]))
            if var_type != 'str':
                with pytest.raises(InvalidArgumentError):
                    Security(dict([(var_name, 'string')]))
            if var_type != 'bool':
                with pytest.raises(InvalidArgumentError):
                    Security(dict([(var_name, True)]))

        sub_type_test('str', 'user')
        sub_type_test('str', 'password')
        sub_type_test('str', 'ssl_client_key')
        sub_type_test('str', 'ssl_client_key_passphrase')
        sub_type_test('str', 'ssl_trust_certificate')
        sub_type_test('str', 'ssl_client_certificate')
        sub_type_test('bool', 'ssl_verifty_name')


    def test_store_and_password(self):
        name_list = ['ssl_client_key'
                     'ssl_clinet_key_passphrase',
                     'user',
                     'password',
                     ]
        for name in name_list:
            with pytest.raises(InvalidArgumentError):
                Security(dict([
                               (name,'value')
                               ]))
                
class TestService(unittest.TestCase):

    no_ssl = Security({})
    with_ssl = Security(dict([('ssl_trust_certificate','cert')]))
    no_user_pass = Security({})
    user_pass = Security(dict([('user','user'),('password','password')]))
    
    
    def test_golden(self):
        s = Service('amqp://localhost', self.no_ssl)
        assert s.address == 'amqp://localhost:5672'

        s = Service('amqps://localhost', self.with_ssl)
        assert s.address == 'amqps://localhost:5671'

        s = Service('amqps://user:password@localhost', self.with_ssl)
        assert s.address == 'amqps://user:password@localhost:5671'

    def test_golden_getters(self):
        s = Service('amqps://user:password@localhost', self.with_ssl)
        assert s.host_port == ('localhost',5671)

    def test_scheme(self):
        s = Service('amqp://localhost:5672', self.with_ssl)
        assert s.address == 'amqp://localhost:5672'

        s = Service('amqps://localhost:5671', self.no_ssl)
        assert s.address == 'amqps://localhost:5671'

        s = Service('http://localhost:5671', self.no_ssl)
        assert s.address == 'http://localhost:5671'

    def test_port(self):
        s = Service('amqp://localhost', self.no_ssl)
        assert s.address == 'amqp://localhost:5672'

        s = Service('amqps://localhost', self.with_ssl)
        assert s.address == 'amqps://localhost:5671'

        s = Service('amqp://localhost:9999', self.no_ssl)
        assert s.address == 'amqp://localhost:9999'

        s = Service('amqps://localhost:9999', self.with_ssl)
        assert s.address == 'amqps://localhost:9999'

    def test_user_pass(self):
        s = Service('amqp://localhost', self.user_pass)
        assert s.address == 'amqp://user:password@localhost:5672'
        
        s = Service('amqp://myuser:mypassword@localhost', self.no_user_pass)
        assert s.address == 'amqp://myuser:mypassword@localhost:5672'
        
        s = Service('amqp://myuser:mypassword@localhost', self.user_pass)
        assert s.address == 'amqp://user:password@localhost:5672'

        with pytest.raises(InvalidArgumentError):
            s = Service('amqp://myuser@localhost', self.no_user_pass)

    def test_logging(self):
        s = Service('amqp://localhost', self.user_pass)
        assert str(s) == 'amqp://user:****@localhost:5672'

    def test_attributes(self):
        s = Service('amqp://localhost', self.user_pass)
        assert s.address == 'amqp://user:password@localhost:5672'
        assert s.route_pattern == 'amqp://localhost:5672/*'
        assert s.route_address == 'amqp://user:password@localhost:5672/$1'
        assert s.host_port == ('localhost',5672)


class TestServiceGenerator(unittest.TestCase):

    TEST_TIMEOUT = 10.0

    no_ssl = Security({})
    single_service = 'amqp://localhost:5672'
    bad_service = 12
    multiple_services = ['amqp://localhost:5672',
                         'amqp://localhost:5673',
                         'amqp://localhost:5674']
    as_http = 'http://localhost:5672'
    bad_schema='amq://localhost'
    multi_schema='amqp://amqps://localhost'
    bad_port = 'amqp://localhost:-5672'
    two_ports = 'amqp://localhost:5672:5672'
    missing_host = 'amqp://:5672'

    def _user_function_single(self, callback):
        callback(None, self.single_service)

    def _user_function_error(self, callback):
        callback(TypeError('Unitest generated'), self.single_service)

    def test_golden_path_single(self):
        test_is_done = threading.Event()
        def _callback(err, services):
            assert err is None,'Error: {0}'.format(err)
            assert isinstance(services, list),'Type: {0}'.format(str(type(services)))
            assert len(services) == 1,'Size: {0}'.format(len(services))
            assert services[0].address == 'amqp://localhost:5672','Address: {0}'.format(services[0].address)
            test_is_done.set()
        sg = ServiceGenerator(self.single_service, self.no_ssl) 
        sg.get_service_list(_callback)
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()

    def test_golden_path_multi(self):
        test_is_done = threading.Event()
        def _callback(err, services):
            assert err is None,'Error: {0}'.format(err)
            assert isinstance(services, list),'Type: {0}'.format(str(type(services)))
            assert len(services) == 3,'Size: {0}'.format(len(services))
            assert services[0].address == 'amqp://localhost:5672','Address: {0}'.format(services[0].address)
            assert services[1].address == 'amqp://localhost:5673','Address: {0}'.format(services[0].address)
            assert services[2].address == 'amqp://localhost:5674','Address: {0}'.format(services[0].address)
            test_is_done.set()
        sg = ServiceGenerator(self.multiple_services, self.no_ssl) 
        sg.get_service_list(_callback)
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()

    def test_golden_path_user_function(self):
        test_is_done = threading.Event()
        def _callback(err, services):
            assert err is None,'Error: {0}'.format(err)
            assert isinstance(services, list),'Type: {0}'.format(str(type(services)))
            assert len(services) == 1,'Size: {0}'.format(len(services))
            assert services[0].address == 'amqp://localhost:5672','Address: {0}'.format(services[0].address)
            test_is_done.set()
        sg = ServiceGenerator(self._user_function_single, self.no_ssl) 
        sg.get_service_list(_callback)
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()

    def test_golden_path_http(self):
        test_is_done = threading.Event()
        httplib.HTTPConnection.set_response(200,
                '{"service":["amqp://localhost:5672"]}')
        def _callback(err, services):
            assert err is None,'Error: {0}'.format(err)
            assert isinstance(services, list),'Type: {0}'.format(str(type(services)))
            assert len(services) == 1,'Size: {0}'.format(len(services))
            assert services[0].address == 'amqp://localhost:5672','Address: {0}'.format(services[0].address)
            test_is_done.set()
        sg = ServiceGenerator(self.as_http, self.no_ssl) 
        sg.get_service_list(_callback)
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()

    def test_error_single(self):
        with pytest.raises(InvalidArgumentError):
            sg = ServiceGenerator(self.bad_service, self.no_ssl) 

    def test_error_bad_schema(self):
        with pytest.raises(InvalidArgumentError):
            sg = ServiceGenerator(self.bad_schema, self.no_ssl) 

    def test_error_multiple_schema(self):
        with pytest.raises(InvalidArgumentError):
            sg = ServiceGenerator(self.multi_schema, self.no_ssl) 

    def test_error_missing_host(self):
        with pytest.raises(InvalidArgumentError):
            sg = ServiceGenerator(self.missing_host, self.no_ssl) 

    def test_error_bad_port(self):
        with pytest.raises(InvalidArgumentError):
            sg = ServiceGenerator(self.bad_port, self.no_ssl) 

    def test_error_two_ports(self):
        with pytest.raises(InvalidArgumentError):
            sg = ServiceGenerator(self.two_ports, self.no_ssl) 

    def test_error_user_function(self):
        test_is_done = threading.Event()
        def _callback(err, services):
            assert err is not None
            test_is_done.set()
        sg = ServiceGenerator(self._user_function_error, self.no_ssl) 
        sg.get_service_list(_callback)
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()

    def test_error_http(self):
        test_is_done = threading.Event()
        httplib.HTTPConnection.set_response(0,'')
        def _callback(err, services):
            assert err is not None
            test_is_done.set()
        sg = ServiceGenerator(self.as_http, self.no_ssl) 
        sg.get_service_list(_callback)
        test_is_done.wait(self.TEST_TIMEOUT)
        assert test_is_done.is_set()


class TestMisc(unittest.TestCase):

    def bound001(self, arg):
        pass

    def test_decode_uri(self):
        data = [
            {'uri': 'amqp://localhost:5672/private:public1', 'topic': 'public1', 'share': None},
            {'uri': 'amqp://localhost:5672/share:MyShare:public1', 'topic': 'public1', 'share': 'MyShare'},
            {'uri': 'localhost:5672/public2', 'topic': None},
            {'uri': 'amqp://localhost/private:public3', 'topic': 'public3', 'share': None},
            {'uri': 'amqp://localhost:5672*private:public', 'topic': None},
        ]
        for test in data:
            if test['topic'] is None:
                with pytest.raises(InternalError):
                    topic, share = decode_uri(test['uri'])
            else:
                topic, share = decode_uri(test['uri'])
                assert topic == test['topic']
                assert share == test['share']


    def test_decode_link_address(self):
        data = [
            {'link': 'private:public1', 'topic': 'public1', 'share': None},
            {'link': 'share:MyShare:public1', 'topic': 'public1', 'share': 'MyShare'},
        ]
        for test in data:
            if test['topic'] is None:
                with pytest.raises(InternalError):
                    topic, share = decode_link_address(test['link'])
            else:
                topic, share = decode_link_address(test['link'])
                assert topic == test['topic']
                assert share == test['share']

    def test_validate_callback_function_golden(self):
        def one_arg(arg):
            pass
        try:
            validate_callback_function('on_started', one_arg, 1, 0)
        except Exception as e:
            pytest.fail('Unexpected exception of ' + e)

    def test_validate_callback_function_bad(self):
        def one_arg(arg):
            pass
        with pytest.raises(TypeError):
            validate_callback_function('on_started', one_arg, 2, 0)

    def test_validate_callback_function_bound(self):
        try:
            validate_callback_function('on_started', self.bound001, 1, 0)
        except Exception as e:
            pytest.fail('Unexpected exception of ' + e)
