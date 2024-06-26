import logging
import json
import tempfile
import random
import threading
import subprocess
import socket
import time
import os
import io
import string
# XXX this should be converted to use boto3
import boto
from botocore.exceptions import ClientError
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from random import randint
import hashlib
# XXX this should be converted to use pytest
from nose.plugins.attrib import attr
import boto3
from boto.s3.connection import S3Connection
import datetime
from cloudevents.http import from_http
from dateutil import parser
import requests

from . import(
    get_config_host,
    get_config_port,
    get_config_zonegroup,
    get_config_cluster,
    get_access_key,
    get_secret_key
    )

from .api import PSTopicS3, \
    PSNotificationS3, \
    delete_all_objects, \
    delete_all_topics, \
    put_object_tagging, \
    admin

from nose import SkipTest
from nose.tools import assert_not_equal, assert_equal, assert_in, assert_true
import boto.s3.tagging

# configure logging for the tests module
class LogWrapper:
    def __init__(self):
        self.log = logging.getLogger(__name__)
        self.errors = 0

    def info(self, msg, *args, **kwargs):
        try:
            self.log.info(msg, *args, **kwargs)
        except BlockingIOError:
            self.errors += 1

    def warning(self, msg, *args, **kwargs):
        try:
            self.log.warning(msg, *args, **kwargs)
        except BlockingIOError:
            self.errors += 1

    def error(self, msg, *args, **kwargs):
        try:
            self.log.error(msg, *args, **kwargs)
        except BlockingIOError:
            self.errors += 1

    def __del__(self):
        if self.errors > 0:
            self.log.error("%d logs were lost", self.errors)


log = LogWrapper()

TOPIC_SUFFIX = "_topic"
NOTIFICATION_SUFFIX = "_notif"
UID_PREFIX = "superman"

num_buckets = 0
run_prefix=''.join(random.choice(string.ascii_lowercase) for _ in range(6))

def gen_bucket_name():
    global num_buckets

    num_buckets += 1
    return run_prefix + '-' + str(num_buckets)


def set_contents_from_string(key, content):
    try:
        key.set_contents_from_string(content)
    except Exception as e:
        print('Error: ' + str(e))


def verify_s3_records_by_elements(records, keys, exact_match=False, deletions=False, expected_sizes={}, etags=[]):
    """ verify there is at least one record per element """
    err = ''
    for key in keys:
        key_found = False
        object_size = 0
        if type(records) is list:
            for record_list in records:
                if key_found:
                    break
                for record in record_list['Records']:
                    assert_in('eTag', record['s3']['object'])
                    if record['s3']['bucket']['name'] == key.bucket.name and \
                        record['s3']['object']['key'] == key.name:
                        # Assertion Error needs to be fixed
                        #assert_equal(key.etag[1:-1], record['s3']['object']['eTag'])
                        if etags:
                            assert_in(key.etag[1:-1], etags)
                        if len(record['s3']['object']['metadata']) > 0:
                            for meta in record['s3']['object']['metadata']:
                                assert(meta['key'].startswith(META_PREFIX))
                        if deletions and record['eventName'].startswith('ObjectRemoved'):
                            key_found = True
                            object_size = record['s3']['object']['size']
                            break
                        elif not deletions and record['eventName'].startswith('ObjectCreated'):
                            key_found = True
                            object_size = record['s3']['object']['size']
                            break
        else:
            for record in records['Records']:
                assert_in('eTag', record['s3']['object'])
                if record['s3']['bucket']['name'] == key.bucket.name and \
                    record['s3']['object']['key'] == key.name:
                    assert_equal(key.etag, record['s3']['object']['eTag'])
                    if etags:
                        assert_in(key.etag[1:-1], etags)
                    if len(record['s3']['object']['metadata']) > 0:
                        for meta in record['s3']['object']['metadata']:
                            assert(meta['key'].startswith(META_PREFIX))
                    if deletions and record['eventName'].startswith('ObjectRemoved'):
                        key_found = True
                        object_size = record['s3']['object']['size']
                        break
                    elif not deletions and record['eventName'].startswith('ObjectCreated'):
                        key_found = True
                        object_size = record['s3']['object']['size']
                        break

        if not key_found:
            err = 'no ' + ('deletion' if deletions else 'creation') + ' event found for key: ' + str(key)
            assert False, err
        elif expected_sizes:
            assert_equal(object_size, expected_sizes.get(key.name))

    if not len(records) == len(keys):
        err = 'superfluous records are found'
        log.warning(err)
        if exact_match:
            for record_list in records:
                for record in record_list['Records']:
                    log.error(str(record['s3']['bucket']['name']) + ',' + str(record['s3']['object']['key']))
            assert False, err


class HTTPPostHandler(BaseHTTPRequestHandler):
    """HTTP POST hanler class storing the received events in its http server"""
    def do_POST(self):
        """implementation of POST handler"""
        content_length = int(self.headers['Content-Length'])
        if content_length == 0:
            log.info('HTTP Server received iempty event')
            self.send_response(200)
            self.end_headers()
            return
        body = self.rfile.read(content_length)
        if self.server.cloudevents:
            event = from_http(self.headers, body) 
            record = json.loads(body)['Records'][0]
            assert_equal(event['specversion'], '1.0')
            assert_equal(event['id'], record['responseElements']['x-amz-request-id'] + '.' + record['responseElements']['x-amz-id-2'])
            assert_equal(event['source'], 'ceph:s3.' + record['awsRegion'] + '.' + record['s3']['bucket']['name'])
            assert_equal(event['type'], 'com.amazonaws.' + record['eventName'])
            assert_equal(event['datacontenttype'], 'application/json') 
            assert_equal(event['subject'], record['s3']['object']['key'])
            assert_equal(parser.parse(event['time']), parser.parse(record['eventTime']))
        log.info('HTTP Server received event: %s', str(body))
        self.server.append(json.loads(body))
        if self.headers.get('Expect') == '100-continue':
            self.send_response(100)
        else:
            self.send_response(200)
        if self.server.delay > 0:
            time.sleep(self.server.delay)
        self.end_headers()

import requests

class HTTPServerWithEvents(ThreadingHTTPServer):
    """multithreaded HTTP server used by the handler to store events"""
    def __init__(self, addr, delay=0, cloudevents=False):
        self.events = []
        self.delay = delay
        self.cloudevents = cloudevents
        self.addr = addr
        self.request_queue_size = 100
        self.lock = threading.Lock()
        ThreadingHTTPServer.__init__(self, addr, HTTPPostHandler)
        log.info('http server created on %s', self.addr)
        self.proc = threading.Thread(target=self.run)
        self.proc.daemon = True
        self.proc.start()
        retries = 0
        while self.proc.is_alive() == False and retries < 5:
            retries += 1
            time.sleep(5)
            log.warning('http server on %s did not start yet', str(self.addr))
        if not self.proc.is_alive():
            log.error('http server on %s failed to start. closing...', str(self.addr))
            self.close()
            assert False
        # make sure that http handler is able to consume requests
        url = 'http://{}:{}'.format(self.addr[0], self.addr[1])
        response = requests.post(url, {})
        assert response.status_code == 200


    def run(self):
        log.info('http server started on %s', str(self.addr))
        self.serve_forever()
        self.server_close()
        log.info('http server ended on %s', str(self.addr))

    def acquire_lock(self):
        if self.lock.acquire(timeout=5) == False:
            self.close()
            raise AssertionError('failed to acquire lock in HTTPServerWithEvents')

    def append(self, event):
        self.acquire_lock()
        self.events.append(event)
        self.lock.release()
    
    def verify_s3_events(self, keys, exact_match=False, deletions=False, expected_sizes={}, etags=[]):
        """verify stored s3 records agains a list of keys"""
        self.acquire_lock()
        log.info('verify_s3_events: http server has %d events', len(self.events))
        try:
            verify_s3_records_by_elements(self.events, keys, exact_match=exact_match, deletions=deletions, expected_sizes=expected_sizes, etags=etags)
        except AssertionError as err:
            self.close()
            raise err
        finally:
            self.lock.release()
            self.events = []

    def get_and_reset_events(self):
        self.acquire_lock()
        log.info('get_and_reset_events: http server has %d events', len(self.events))
        events = self.events
        self.events = []
        self.lock.release()
        return events

    def close(self, task=None):
        log.info('http server on %s starting shutdown', str(self.addr))
        t = threading.Thread(target=self.shutdown)
        t.start()
        t.join(5)
        retries = 0
        while self.proc.is_alive() and retries < 5:
            retries += 1
            t.join(5)
            log.warning('http server on %s still alive', str(self.addr))
        if self.proc.is_alive():
            log.error('http server on %s failed to shutdown', str(self.addr))
            self.server_close()
        else:
            log.info('http server on %s shutdown ended', str(self.addr))

# AMQP endpoint functions

class AMQPReceiver(object):
    """class for receiving and storing messages on a topic from the AMQP broker"""
    def __init__(self, exchange, topic, external_endpoint_address=None, ca_location=None):
        import pika
        import ssl

        if ca_location:
            ssl_context = ssl.create_default_context()
            ssl_context.load_verify_locations(cafile=ca_location)
            ssl_options = pika.SSLOptions(ssl_context)
            rabbitmq_port = 5671
        else:
            rabbitmq_port = 5672
            ssl_options = None

        if external_endpoint_address:
            if ssl_options:
                # this is currently not working due to: https://github.com/pika/pika/issues/1192
                params = pika.URLParameters(external_endpoint_address, ssl_options=ssl_options)
            else:
                params = pika.URLParameters(external_endpoint_address)
        else:
            hostname = get_ip()
            params = pika.ConnectionParameters(host=hostname, port=rabbitmq_port, ssl_options=ssl_options)

        remaining_retries = 10
        while remaining_retries > 0:
            try:
                connection = pika.BlockingConnection(params)
                break
            except Exception as error:
                remaining_retries -= 1
                print('failed to connect to rabbitmq (remaining retries '
                    + str(remaining_retries) + '): ' + str(error))
                time.sleep(1)

        if remaining_retries == 0:
            raise Exception('failed to connect to rabbitmq - no retries left')

        self.channel = connection.channel()
        self.channel.exchange_declare(exchange=exchange, exchange_type='topic', durable=True)
        result = self.channel.queue_declare('', exclusive=True)
        queue_name = result.method.queue
        self.channel.queue_bind(exchange=exchange, queue=queue_name, routing_key=topic)
        self.channel.basic_consume(queue=queue_name,
                                   on_message_callback=self.on_message,
                                   auto_ack=True)
        self.events = []
        self.topic = topic

    def on_message(self, ch, method, properties, body):
        """callback invoked when a new message arrive on the topic"""
        log.info('AMQP received event for topic %s:\n %s', self.topic, body)
        self.events.append(json.loads(body))

    # TODO create a base class for the AMQP and HTTP cases
    def verify_s3_events(self, keys, exact_match=False, deletions=False, expected_sizes={}, etags=[]):
        """verify stored s3 records agains a list of keys"""
        verify_s3_records_by_elements(self.events, keys, exact_match=exact_match, deletions=deletions, expected_sizes=expected_sizes, etags=etags)
        self.events = []

    def get_and_reset_events(self):
        tmp = self.events
        self.events = []
        return tmp

    def close(self, task):
        stop_amqp_receiver(self, task)


def amqp_receiver_thread_runner(receiver):
    """main thread function for the amqp receiver"""
    try:
        log.info('AMQP receiver started')
        receiver.channel.start_consuming()
        log.info('AMQP receiver ended')
    except Exception as error:
        log.info('AMQP receiver ended unexpectedly: %s', str(error))


def create_amqp_receiver_thread(exchange, topic, external_endpoint_address=None, ca_location=None):
    """create amqp receiver and thread"""
    receiver = AMQPReceiver(exchange, topic, external_endpoint_address, ca_location)
    task = threading.Thread(target=amqp_receiver_thread_runner, args=(receiver,))
    task.daemon = True
    return task, receiver

def stop_amqp_receiver(receiver, task):
    """stop the receiver thread and wait for it to finis"""
    try:
        receiver.channel.stop_consuming()
        log.info('stopping AMQP receiver')
    except Exception as error:
        log.info('failed to gracefully stop AMQP receiver: %s', str(error))
    task.join(5)


def init_rabbitmq():
    """ start a rabbitmq broker """
    hostname = get_ip()
    try:
        # first try to stop any existing process
        subprocess.call(['sudo', 'rabbitmqctl', 'stop'])
        time.sleep(5)
        proc = subprocess.Popen(['sudo', '--preserve-env=RABBITMQ_CONFIG_FILE', 'rabbitmq-server'])
    except Exception as error:
        log.info('failed to execute rabbitmq-server: %s', str(error))
        print('failed to execute rabbitmq-server: %s' % str(error))
        return None
    # TODO add rabbitmq checkpoint instead of sleep
    time.sleep(5)
    return proc


def clean_rabbitmq(proc):
    """ stop the rabbitmq broker """
    try:
        subprocess.call(['sudo', 'rabbitmqctl', 'stop'])
        time.sleep(5)
        proc.terminate()
    except:
        log.info('rabbitmq server already terminated')


META_PREFIX = 'x-amz-meta-'

# Kafka endpoint functions

kafka_server = 'localhost'

class KafkaReceiver(object):
    """class for receiving and storing messages on a topic from the kafka broker"""
    def __init__(self, topic, security_type):
        from kafka import KafkaConsumer
        remaining_retries = 10
        port = 9092
        if security_type != 'PLAINTEXT':
            security_type = 'SSL'
            port = 9093
        while remaining_retries > 0:
            try:
                self.consumer = KafkaConsumer(topic, 
                        bootstrap_servers = kafka_server+':'+str(port), 
                        security_protocol=security_type,
                        consumer_timeout_ms=16000,
                        auto_offset_reset='earliest')
                print('Kafka consumer created on topic: '+topic)
                break
            except Exception as error:
                remaining_retries -= 1
                print('failed to connect to kafka (remaining retries '
                    + str(remaining_retries) + '): ' + str(error))
                time.sleep(1)

        if remaining_retries == 0:
            raise Exception('failed to connect to kafka - no retries left')

        self.events = []
        self.topic = topic
        self.stop = False

    def verify_s3_events(self, keys, exact_match=False, deletions=False, expected_sizes={}, etags=[]):
        """verify stored s3 records agains a list of keys"""
        verify_s3_records_by_elements(self.events, keys, exact_match=exact_match, deletions=deletions, expected_sizes=expected_sizes, etags=etags)
        self.events = []

    def get_and_reset_events(self):
        tmp = self.events
        self.events = []
        return tmp

    def close(self, task):
        stop_kafka_receiver(self, task)

def kafka_receiver_thread_runner(receiver):
    """main thread function for the kafka receiver"""
    try:
        log.info('Kafka receiver started')
        print('Kafka receiver started')
        while not receiver.stop:
            for msg in receiver.consumer:
                receiver.events.append(json.loads(msg.value))
            time.sleep(0.1)
        log.info('Kafka receiver ended')
        print('Kafka receiver ended')
    except Exception as error:
        log.info('Kafka receiver ended unexpectedly: %s', str(error))
        print('Kafka receiver ended unexpectedly: ' + str(error))


def create_kafka_receiver_thread(topic, security_type='PLAINTEXT'):
    """create kafka receiver and thread"""
    receiver = KafkaReceiver(topic, security_type)
    task = threading.Thread(target=kafka_receiver_thread_runner, args=(receiver,))
    task.daemon = True
    return task, receiver

def stop_kafka_receiver(receiver, task):
    """stop the receiver thread and wait for it to finish"""
    receiver.stop = True
    task.join(1)
    try:
        receiver.consumer.unsubscribe()
        receiver.consumer.close()
    except Exception as error:
        log.info('failed to gracefully stop Kafka receiver: %s', str(error))


def get_ip():
    return 'localhost'


def get_ip_http():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # address should not be reachable
        s.connect(('10.255.255.255', 1))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip


def connection():
    hostname = get_config_host()
    port_no = get_config_port()
    vstart_access_key = get_access_key()
    vstart_secret_key = get_secret_key()

    conn = S3Connection(aws_access_key_id=vstart_access_key,
                  aws_secret_access_key=vstart_secret_key,
                      is_secure=False, port=port_no, host=hostname, 
                      calling_format='boto.s3.connection.OrdinaryCallingFormat')

    return conn


def connection2():
    hostname = get_config_host()
    port_no = 8001
    vstart_access_key = get_access_key()
    vstart_secret_key = get_secret_key()

    conn = S3Connection(aws_access_key_id=vstart_access_key,
                  aws_secret_access_key=vstart_secret_key,
                      is_secure=False, port=port_no, host=hostname, 
                      calling_format='boto.s3.connection.OrdinaryCallingFormat')

    return conn


def another_user(user=None, tenant=None, account=None):
    access_key = str(time.time())
    secret_key = str(time.time())
    uid = user or UID_PREFIX + str(time.time())
    cmd = ['user', 'create', '--uid', uid, '--access-key', access_key, '--secret-key', secret_key, '--display-name', 'Superman']
    arn = f'arn:aws:iam:::user/{uid}'
    if tenant:
        cmd += ['--tenant', tenant]
        arn = f'arn:aws:iam::{tenant}:user/{uid}'
    if account:
        cmd += ['--account-id', account, '--account-root']
        arn = f'arn:aws:iam::{account}:user/Superman'

    _, result = admin(cmd, get_config_cluster())
    assert_equal(result, 0)

    conn = S3Connection(aws_access_key_id=access_key,
                  aws_secret_access_key=secret_key,
                      is_secure=False, port=get_config_port(), host=get_config_host(), 
                      calling_format='boto.s3.connection.OrdinaryCallingFormat')
    return conn, arn


def list_topics(assert_len=None, tenant=''):
    if tenant == '':
        result = admin(['topic', 'list'], get_config_cluster())
    else:
        result = admin(['topic', 'list', '--tenant', tenant], get_config_cluster())
    parsed_result = json.loads(result[0])
    if assert_len:
        assert_equal(len(parsed_result['topics']), assert_len)
    return parsed_result


def get_stats_persistent_topic(topic_name, assert_entries_number=None):
    result = admin(['topic', 'stats', '--topic', topic_name], get_config_cluster())
    assert_equal(result[1], 0)
    parsed_result = json.loads(result[0])
    if assert_entries_number:
        assert_equal(parsed_result['Topic Stats']['Entries'], assert_entries_number)
    return parsed_result


def get_topic(topic_name, tenant='', allow_failure=False):
    if tenant == '':
        result = admin(['topic', 'get', '--topic', topic_name], get_config_cluster())
    else:
        result = admin(['topic', 'get', '--topic', topic_name, '--tenant', tenant], get_config_cluster())
    if allow_failure:
        return result
    assert_equal(result[1], 0)
    parsed_result = json.loads(result[0])
    return parsed_result


def remove_topic(topic_name, tenant='', allow_failure=False):
    if tenant == '':
        result = admin(['topic', 'rm', '--topic', topic_name], get_config_cluster())
    else:
        result = admin(['topic', 'rm', '--topic', topic_name, '--tenant', tenant], get_config_cluster())
    if not allow_failure:
        assert_equal(result[1], 0)
    return result[1]


def list_notifications(bucket_name, assert_len=None, tenant=''):
    if tenant == '':
        result = admin(['notification', 'list', '--bucket', bucket_name], get_config_cluster())
    else:
        result = admin(['notification', 'list', '--bucket', bucket_name, '--tenant', tenant], get_config_cluster())
    parsed_result = json.loads(result[0])
    if assert_len:
        assert_equal(len(parsed_result['notifications']), assert_len)
    return parsed_result


def get_notification(bucket_name,  notification_name, tenant=''):
    if tenant == '':
        result = admin(['notification', 'get', '--bucket', bucket_name, '--notification-id', notification_name], get_config_cluster())
    else:
        result = admin(['notification', 'get', '--bucket', bucket_name, '--notification-id', notification_name, '--tenant', tenant], get_config_cluster())
    assert_equal(result[1], 0)
    parsed_result = json.loads(result[0])
    assert_equal(parsed_result['Id'], notification_name)
    return parsed_result


def remove_notification(bucket_name, notification_name='', tenant='', allow_failure=False):
    args = ['notification', 'rm', '--bucket', bucket_name]
    if notification_name != '':
        args.extend(['--notification-id', notification_name])
    if tenant != '':
        args.extend(['--tenant', tenant])
    result = admin(args, get_config_cluster())
    if not allow_failure:
        assert_equal(result[1], 0)
    return result[1]


zonegroup_feature_notification_v2 = 'notification_v2'


def zonegroup_modify_feature(enable, feature_name):
    if enable:
        command = '--enable-feature='+feature_name
    else:
        command = '--disable-feature='+feature_name
    result = admin(['zonegroup', 'modify', command], get_config_cluster())
    assert_equal(result[1], 0)
    result = admin(['period', 'update'], get_config_cluster())
    assert_equal(result[1], 0)
    result = admin(['period', 'commit'], get_config_cluster())
    assert_equal(result[1], 0)


def connect_random_user(tenant=''):
    access_key = str(time.time())
    secret_key = str(time.time())
    uid = UID_PREFIX + str(time.time())
    if tenant == '':
        _, result = admin(['user', 'create', '--uid', uid, '--access-key', access_key, '--secret-key', secret_key, '--display-name', '"Super Man"'], get_config_cluster())
    else:
        _, result = admin(['user', 'create', '--uid', uid, '--tenant', tenant, '--access-key', access_key, '--secret-key', secret_key, '--display-name', '"Super Man"'], get_config_cluster())
    assert_equal(result, 0)
    conn = S3Connection(aws_access_key_id=access_key,
                        aws_secret_access_key=secret_key,
                        is_secure=False, port=get_config_port(), host=get_config_host(),
                        calling_format='boto.s3.connection.OrdinaryCallingFormat')
    return conn

##############
# bucket notifications tests
##############


@attr('basic_test')
def test_ps_s3_topic_on_master():
    """ test s3 topics set/get/delete on master """
    
    tenant = 'kaboom'
    conn = connect_random_user(tenant)
    
    # make sure there are no leftover topics
    delete_all_topics(conn, tenant, get_config_cluster())
    
    zonegroup = get_config_zonegroup()
    bucket_name = gen_bucket_name()
    topic_name = bucket_name + TOPIC_SUFFIX

    # create s3 topics
    endpoint_address = 'amqp://127.0.0.1:7001/vhost_1'
    endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=amqp.direct&amqp-ack-level=none'
    topic_conf1 = PSTopicS3(conn, topic_name+'_1', zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf1.set_config()
    assert_equal(topic_arn,
                 'arn:aws:sns:' + zonegroup + ':' + tenant + ':' + topic_name + '_1')

    endpoint_address = 'http://127.0.0.1:9001'
    endpoint_args = 'push-endpoint='+endpoint_address
    topic_conf2 = PSTopicS3(conn, topic_name+'_2', zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf2.set_config()
    assert_equal(topic_arn,
                 'arn:aws:sns:' + zonegroup + ':' + tenant + ':' + topic_name + '_2')
    endpoint_address = 'http://127.0.0.1:9002'
    endpoint_args = 'push-endpoint='+endpoint_address
    topic_conf3 = PSTopicS3(conn, topic_name+'_3', zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf3.set_config()
    assert_equal(topic_arn,
                 'arn:aws:sns:' + zonegroup + ':' + tenant + ':' + topic_name + '_3')

    # get topic 3
    result, status = topic_conf3.get_config()
    assert_equal(status, 200)
    assert_equal(topic_arn, result['GetTopicResponse']['GetTopicResult']['Topic']['TopicArn'])
    assert_equal(endpoint_address, result['GetTopicResponse']['GetTopicResult']['Topic']['EndPoint']['EndpointAddress'])

    # Note that endpoint args may be ordered differently in the result
    # delete topic 1
    result = topic_conf1.del_config()
    assert_equal(status, 200)

    # try to get a deleted topic
    _, status = topic_conf1.get_config()
    assert_equal(status, 404)

    # get the remaining 2 topics
    result, status = topic_conf1.get_list()
    assert_equal(status, 200)
    assert_equal(len(result['ListTopicsResponse']['ListTopicsResult']['Topics']['member']), 2)

    # delete topics
    result = topic_conf2.del_config()
    assert_equal(status, 200)
    result = topic_conf3.del_config()
    assert_equal(status, 200)

    # get topic list, make sure it is empty
    result, status = topic_conf1.get_list()
    assert_equal(result['ListTopicsResponse']['ListTopicsResult']['Topics'], None)


@attr('basic_test')
def test_ps_s3_topic_admin_on_master():
    """ test s3 topics set/get/delete on master """
    
    tenant = 'kaboom'
    conn = connect_random_user(tenant)
    
    # make sure there are no leftover topics
    delete_all_topics(conn, tenant, get_config_cluster())
    
    zonegroup = get_config_zonegroup()
    bucket_name = gen_bucket_name()
    topic_name = bucket_name + TOPIC_SUFFIX

    # create s3 topics
    endpoint_address = 'amqp://127.0.0.1:7001/vhost_1'
    endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=amqp.direct&amqp-ack-level=none'
    topic_conf1 = PSTopicS3(conn, topic_name+'_1', zonegroup, endpoint_args=endpoint_args)
    topic_arn1 = topic_conf1.set_config()
    assert_equal(topic_arn1,
                 'arn:aws:sns:' + zonegroup + ':' + tenant + ':' + topic_name + '_1')

    endpoint_address = 'http://127.0.0.1:9001'
    endpoint_args = 'push-endpoint='+endpoint_address+'&persistent=true'
    topic_conf2 = PSTopicS3(conn, topic_name+'_2', zonegroup, endpoint_args=endpoint_args)
    topic_arn2 = topic_conf2.set_config()
    assert_equal(topic_arn2,
                 'arn:aws:sns:' + zonegroup + ':' + tenant + ':' + topic_name + '_2')
    endpoint_address = 'http://127.0.0.1:9002'
    endpoint_args = 'push-endpoint=' + endpoint_address + '&persistent=true'
    topic_conf3 = PSTopicS3(conn, topic_name+'_3', zonegroup, endpoint_args=endpoint_args)
    topic_arn3 = topic_conf3.set_config()
    assert_equal(topic_arn3,
                 'arn:aws:sns:' + zonegroup + ':' + tenant + ':' + topic_name + '_3')

    # get topic 3 via commandline
    parsed_result = get_topic(topic_name+'_3', tenant)
    assert_equal(parsed_result['arn'], topic_arn3)
    matches = [tenant, UID_PREFIX]
    assert_true( all([x in parsed_result['owner'] for x in matches]))
    assert_equal(parsed_result['dest']['persistent_queue'],
                 tenant + ":" + topic_name + '_3')

    # recall CreateTopic and verify the owner and persistent_queue remain same.
    topic_conf3 = PSTopicS3(conn, topic_name + '_3', zonegroup,
                            endpoint_args=endpoint_args)
    topic_arn3 = topic_conf3.set_config()
    assert_equal(topic_arn3,
                 'arn:aws:sns:' + zonegroup + ':' + tenant + ':' + topic_name + '_3')
    # get topic 3 via commandline
    result = admin(
      ['topic', 'get', '--topic', topic_name + '_3', '--tenant', tenant],
      get_config_cluster())
    parsed_result = json.loads(result[0])
    assert_equal(parsed_result['arn'], topic_arn3)
    assert_true(all([x in parsed_result['owner'] for x in matches]))
    assert_equal(parsed_result['dest']['persistent_queue'],
                 tenant + ":" + topic_name + '_3')

    # delete topic 3
    remove_topic(topic_name + '_3', tenant)

    # try to get a deleted topic
    _, result = get_topic(topic_name + '_3', tenant, allow_failure=True)
    print('"topic not found" error is expected')
    assert_equal(result, 2)

    # get the remaining 2 topics
    list_topics(2, tenant)

    # delete topics
    remove_topic(topic_name + '_1', tenant)
    remove_topic(topic_name + '_2', tenant)

    # get topic list, make sure it is empty
    list_topics(0, tenant)


def notification_configuration(with_cli):
    conn = connection()
    zonegroup = get_config_zonegroup()
    bucket_name = gen_bucket_name()
    # create bucket
    bucket = conn.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX

    # make sure there are no leftover topics
    delete_all_topics(conn, '', get_config_cluster())

    # create s3 topics
    endpoint_address = 'amqp://127.0.0.1:7001'
    endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=amqp.direct&amqp-ack-level=none'
    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()
    assert_equal(topic_arn,
                 'arn:aws:sns:' + zonegroup + '::' + topic_name)
    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name+'_1',
                        'TopicArn': topic_arn,
                        'Events': ['s3:ObjectCreated:*']
                        },
                       {'Id': notification_name+'_2',
                        'TopicArn': topic_arn,
                        'Events': ['s3:ObjectRemoved:*']
                        },
                       {'Id': notification_name+'_3',
                        'TopicArn': topic_arn,
                        'Events': []
                        }]
    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    _, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)

    # get notification 1
    if with_cli:
        get_notification(bucket_name, notification_name+'_1')
    else:
        response, status = s3_notification_conf.get_config(notification=notification_name+'_1')
        assert_equal(status/100, 2)
        assert_equal(response['NotificationConfiguration']['TopicConfiguration']['Topic'], topic_arn)

    # list notification
    if with_cli:
        list_notifications(bucket_name, 3)
    else:
        result, status = s3_notification_conf.get_config()
        assert_equal(status, 200)
        assert_equal(len(result['TopicConfigurations']), 3)

    # delete notification 2
    if with_cli:
        remove_notification(bucket_name, notification_name + '_2')
    else:
        _, status = s3_notification_conf.del_config(notification=notification_name+'_2')
        assert_equal(status/100, 2)

    # list notification
    if with_cli:
        list_notifications(bucket_name, 2)
    else:
        result, status = s3_notification_conf.get_config()
        assert_equal(status, 200)
        assert_equal(len(result['TopicConfigurations']), 2)

    # delete notifications
    if with_cli:
        remove_notification(bucket_name)
    else:
        _, status = s3_notification_conf.del_config()
        assert_equal(status/100, 2)

    # list notification, make sure it is empty
    list_notifications(bucket_name, 0)

    # cleanup
    topic_conf.del_config()
    # delete the bucket
    conn.delete_bucket(bucket_name)


@attr('basic_test')
def test_notification_configuration_admin():
    """ test notification list/set/get/delete, with admin cli """
    notification_configuration(True)


@attr('modification_required')
def test_ps_s3_topic_with_secret_on_master():
    """ test s3 topics with secret set/get/delete on master """
    return SkipTest('secure connection is needed to test topic with secrets')

    conn = connection1()
    if conn.secure_conn is None:
        return SkipTest('secure connection is needed to test topic with secrets')

    zonegroup = get_config_zonegroup()
    bucket_name = gen_bucket_name()
    topic_name = bucket_name + TOPIC_SUFFIX

    # clean all topics
    delete_all_s3_topics(conn, zonegroup)

    # create s3 topics
    endpoint_address = 'amqp://user:password@127.0.0.1:7001'
    endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=amqp.direct&amqp-ack-level=none'
    bad_topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    try:
        result = bad_topic_conf.set_config()
    except Exception as err:
        print('Error is expected: ' + str(err))
    else:
        assert False, 'user password configuration set allowed only over HTTPS'
    topic_conf = PSTopicS3(conn.secure_conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()

    assert_equal(topic_arn,
                 'arn:aws:sns:' + zonegroup + ':' + get_tenant() + ':' + topic_name)

    _, status = bad_topic_conf.get_config()
    assert_equal(status/100, 4)

    # get topic
    result, status = topic_conf.get_config()
    assert_equal(status, 200)
    assert_equal(topic_arn, result['GetTopicResponse']['GetTopicResult']['Topic']['TopicArn'])
    assert_equal(endpoint_address, result['GetTopicResponse']['GetTopicResult']['Topic']['EndPoint']['EndpointAddress'])

    _, status = bad_topic_conf.get_config()
    assert_equal(status/100, 4)

    _, status = topic_conf.get_list()
    assert_equal(status/100, 2)

    # delete topics
    result = topic_conf.del_config()


@attr('basic_test')
def test_notification_configuration():
    """ test s3 notification set/get/deleter """
    notification_configuration(False)


@attr('basic_test')
def test_ps_s3_notification_on_master_empty_config():
    """ test s3 notification set/get/delete on master with empty config """
    hostname = get_ip()

    conn = connection()

    zonegroup = get_config_zonegroup()

    # create bucket
    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX

    # create s3 topic
    endpoint_address = 'amqp://127.0.0.1:7001'
    endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=amqp.direct&amqp-ack-level=none'
    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()

    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name+'_1',
                        'TopicArn': topic_arn,
                        'Events': []
                       }]
    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    _, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)

    # get notifications on a bucket
    response, status = s3_notification_conf.get_config(notification=notification_name+'_1')
    assert_equal(status/100, 2)
    assert_equal(response['NotificationConfiguration']['TopicConfiguration']['Topic'], topic_arn)

    # create s3 notification again with empty configuration to check if it deletes or not
    topic_conf_list = []

    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    _, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)

    # make sure that the notification is now deleted
    response, status = s3_notification_conf.get_config()
    try:
        check = response['NotificationConfiguration']
    except KeyError as e:
        assert_equal(status/100, 2)
    else:
        assert False

    # cleanup
    topic_conf.del_config()
    # delete the bucket
    conn.delete_bucket(bucket_name)


@attr('amqp_test')
def test_ps_s3_notification_filter_on_master():
    """ test s3 notification filter on master """

    hostname = get_ip()
    
    conn = connection()
    ps_zone = conn

    zonegroup = get_config_zonegroup()

    # create bucket
    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX

    # start amqp receivers
    exchange = 'ex1'
    task, receiver = create_amqp_receiver_thread(exchange, topic_name)
    task.start()

    # create s3 topic
    endpoint_address = 'amqp://' + hostname
    endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=' + exchange +'&amqp-ack-level=broker'
        
    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()

    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name+'_1',
                        'TopicArn': topic_arn,
                        'Events': ['s3:ObjectCreated:*'],
                        'Filter': {
                          'Key': {
                            'FilterRules': [{'Name': 'prefix', 'Value': 'hello'}]
                          }
                        }
                       },
                       {'Id': notification_name+'_2',
                        'TopicArn': topic_arn,
                        'Events': ['s3:ObjectCreated:*'],
                        'Filter': {
                          'Key': {
                            'FilterRules': [{'Name': 'prefix', 'Value': 'world'},
                                            {'Name': 'suffix', 'Value': 'log'}]
                          }
                        }
                       },
                       {'Id': notification_name+'_3',
                        'TopicArn': topic_arn,
                        'Events': [],
                        'Filter': {
                          'Key': {
                            'FilterRules': [{'Name': 'regex', 'Value': '([a-z]+)\\.txt'}]
                         }
                        }
                       }]

    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    result, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)

    topic_conf_list = [{'Id': notification_name+'_4',
                        'TopicArn': topic_arn,
                        'Events': ['s3:ObjectCreated:*', 's3:ObjectRemoved:*'],
                        'Filter': {
                            'Metadata': {
                                'FilterRules': [{'Name': 'x-amz-meta-foo', 'Value': 'bar'},
                                                {'Name': 'x-amz-meta-hello', 'Value': 'world'}]
                            },
                            'Key': {
                                'FilterRules': [{'Name': 'regex', 'Value': '([a-z]+)'}]
                            }
                        }
                        }]

    try:
        s3_notification_conf4 = PSNotificationS3(conn, bucket_name, topic_conf_list)
        _, status = s3_notification_conf4.set_config()
        assert_equal(status/100, 2)
        skip_notif4 = False
    except Exception as error:
        print('note: metadata filter is not supported by boto3 - skipping test')
        skip_notif4 = True


    # get all notifications
    result, status = s3_notification_conf.get_config()
    assert_equal(status/100, 2)
    for conf in result['TopicConfigurations']:
        filter_name = conf['Filter']['Key']['FilterRules'][0]['Name']
        assert filter_name == 'prefix' or filter_name == 'suffix' or filter_name == 'regex', filter_name

    if not skip_notif4:
        result, status = s3_notification_conf4.get_config(notification=notification_name+'_4')
        assert_equal(status/100, 2)
        filter_name = result['NotificationConfiguration']['TopicConfiguration']['Filter']['S3Metadata']['FilterRule'][0]['Name']
        assert filter_name == 'x-amz-meta-foo' or filter_name == 'x-amz-meta-hello'

    expected_in1 = ['hello.kaboom', 'hello.txt', 'hello123.txt', 'hello']
    expected_in2 = ['world1.log', 'world2log', 'world3.log']
    expected_in3 = ['hello.txt', 'hell.txt', 'worldlog.txt']
    expected_in4 = ['foo', 'bar', 'hello', 'world']
    filtered = ['hell.kaboom', 'world.og', 'world.logg', 'he123ll.txt', 'wo', 'log', 'h', 'txt', 'world.log.txt']
    filtered_with_attr = ['nofoo', 'nobar', 'nohello', 'noworld']
    # create objects in bucket
    for key_name in expected_in1:
        key = bucket.new_key(key_name)
        key.set_contents_from_string('bar')
    for key_name in expected_in2:
        key = bucket.new_key(key_name)
        key.set_contents_from_string('bar')
    for key_name in expected_in3:
        key = bucket.new_key(key_name)
        key.set_contents_from_string('bar')
    if not skip_notif4:
        for key_name in expected_in4:
            key = bucket.new_key(key_name)
            key.set_metadata('foo', 'bar')
            key.set_metadata('hello', 'world')
            key.set_metadata('goodbye', 'cruel world')
            key.set_contents_from_string('bar')
    for key_name in filtered:
        key = bucket.new_key(key_name)
        key.set_contents_from_string('bar')
    for key_name in filtered_with_attr:
        key.set_metadata('foo', 'nobar')
        key.set_metadata('hello', 'noworld')
        key.set_metadata('goodbye', 'cruel world')
        key = bucket.new_key(key_name)
        key.set_contents_from_string('bar')

    print('wait for 5sec for the messages...')
    time.sleep(5)

    found_in1 = []
    found_in2 = []
    found_in3 = []
    found_in4 = []

    for event in receiver.get_and_reset_events():
        notif_id = event['Records'][0]['s3']['configurationId']
        key_name = event['Records'][0]['s3']['object']['key']
        awsRegion = event['Records'][0]['awsRegion']
        assert_equal(awsRegion, zonegroup)
        bucket_arn = event['Records'][0]['s3']['bucket']['arn']
        assert_equal(bucket_arn, "arn:aws:s3:"+awsRegion+"::"+bucket_name)
        if notif_id == notification_name+'_1':
            found_in1.append(key_name)
        elif notif_id == notification_name+'_2':
            found_in2.append(key_name)
        elif notif_id == notification_name+'_3':
            found_in3.append(key_name)
        elif not skip_notif4 and notif_id == notification_name+'_4':
            found_in4.append(key_name)
        else:
            assert False, 'invalid notification: ' + notif_id

    assert_equal(set(found_in1), set(expected_in1))
    assert_equal(set(found_in2), set(expected_in2))
    assert_equal(set(found_in3), set(expected_in3))
    if not skip_notif4:
        assert_equal(set(found_in4), set(expected_in4))

    # cleanup
    s3_notification_conf.del_config()
    if not skip_notif4:
        s3_notification_conf4.del_config()
    topic_conf.del_config()
    # delete the bucket
    for key in bucket.list():
        key.delete()
    conn.delete_bucket(bucket_name)
    stop_amqp_receiver(receiver, task)


@attr('basic_test')
def test_ps_s3_notification_errors_on_master():
    """ test s3 notification set/get/delete on master """
    conn = connection()
    zonegroup = get_config_zonegroup()
    bucket_name = gen_bucket_name()
    # create bucket
    bucket = conn.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX
    # create s3 topic
    endpoint_address = 'amqp://127.0.0.1:7001'
    endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=amqp.direct&amqp-ack-level=none'
    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()

    # create s3 notification with invalid event name
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name,
                        'TopicArn': topic_arn,
                        'Events': ['s3:ObjectCreated:Kaboom']
                       }]
    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    try:
      result, status = s3_notification_conf.set_config()
    except Exception as error:
      print(str(error) + ' - is expected')
    else:
      assert False, 'invalid event name is expected to fail'

    # create s3 notification with missing name
    topic_conf_list = [{'Id': '',
                        'TopicArn': topic_arn,
                        'Events': ['s3:ObjectCreated:Put']
                       }]
    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    try:
      _, _ = s3_notification_conf.set_config()
    except Exception as error:
      print(str(error) + ' - is expected')
    else:
      assert False, 'missing notification name is expected to fail'

    # create s3 notification with invalid topic ARN
    invalid_topic_arn = 'kaboom'
    topic_conf_list = [{'Id': notification_name,
                        'TopicArn': invalid_topic_arn,
                        'Events': ['s3:ObjectCreated:Put']
                       }]
    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    try:
      _, _ = s3_notification_conf.set_config()
    except Exception as error:
      print(str(error) + ' - is expected')
    else:
      assert False, 'invalid ARN is expected to fail'

    # create s3 notification with unknown topic ARN
    invalid_topic_arn = 'arn:aws:sns:a::kaboom'
    topic_conf_list = [{'Id': notification_name,
                        'TopicArn': invalid_topic_arn ,
                        'Events': ['s3:ObjectCreated:Put']
                       }]
    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    try:
      _, _ = s3_notification_conf.set_config()
    except Exception as error:
      print(str(error) + ' - is expected')
    else:
      assert False, 'unknown topic is expected to fail'

    # create s3 notification with wrong bucket
    topic_conf_list = [{'Id': notification_name,
                        'TopicArn': topic_arn,
                        'Events': ['s3:ObjectCreated:Put']
                       }]
    s3_notification_conf = PSNotificationS3(conn, 'kaboom', topic_conf_list)
    try:
      _, _ = s3_notification_conf.set_config()
    except Exception as error:
      print(str(error) + ' - is expected')
    else:
      assert False, 'unknown bucket is expected to fail'

    topic_conf.del_config()

    status = topic_conf.del_config()
    # deleting an unknown notification is not considered an error
    assert_equal(status, 200)

    _, status = topic_conf.get_config()
    assert_equal(status, 404)

    # cleanup
    # delete the bucket
    conn.delete_bucket(bucket_name)


def notification_push(endpoint_type, conn, account=None, cloudevents=False):
    """ test pushinging notification """
    zonegroup = get_config_zonegroup()
    # create bucket
    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX

    host = get_ip()
    task = None
    if endpoint_type == 'http':
        # create random port for the http server
        host = get_ip_http()
        port = random.randint(10000, 20000)
        # start an http server in a separate thread
        receiver = HTTPServerWithEvents((host, port), cloudevents=cloudevents)
        endpoint_address = 'http://'+host+':'+str(port)
        if cloudevents:
            endpoint_args = 'push-endpoint='+endpoint_address+'&cloudevents=true'
        else:
            endpoint_args = 'push-endpoint='+endpoint_address
        topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
        topic_arn = topic_conf.set_config()
        # create s3 notification
        notification_name = bucket_name + NOTIFICATION_SUFFIX
        topic_conf_list = [{'Id': notification_name,
                            'TopicArn': topic_arn,
                            'Events': []
                            }]
        s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
        response, status = s3_notification_conf.set_config()
        assert_equal(status/100, 2)
    elif endpoint_type == 'amqp':
        # start amqp receiver
        exchange = 'ex1'
        task, receiver = create_amqp_receiver_thread(exchange, topic_name)
        task.start()
        endpoint_address = 'amqp://' + host
        # with acks from broker
        exchange = 'ex1'
        endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=' + exchange +'&amqp-ack-level=broker'
        # create two s3 topic
        topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
        topic_arn = topic_conf.set_config()
        # create s3 notification
        notification_name = bucket_name + NOTIFICATION_SUFFIX
        topic_conf_list = [{'Id': notification_name,
                            'TopicArn': topic_arn,
                            'Events': []
                            }]
        s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
        response, status = s3_notification_conf.set_config()
        assert_equal(status/100, 2)
    elif endpoint_type == 'kafka':
        # start amqp receiver
        task, receiver = create_kafka_receiver_thread(topic_name)
        task.start()
        endpoint_address = 'kafka://' + kafka_server
        # without acks from broker
        endpoint_args = 'push-endpoint='+endpoint_address+'&kafka-ack-level=broker'
        # create s3 topic
        topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
        topic_arn = topic_conf.set_config()
        # create s3 notification
        notification_name = bucket_name + NOTIFICATION_SUFFIX
        topic_conf_list = [{'Id': notification_name,
                            'TopicArn': topic_arn,
                            'Events': []
                            }]
        s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
        response, status = s3_notification_conf.set_config()
        assert_equal(status/100, 2)
    else:
        return SkipTest('Unknown endpoint type: ' + endpoint_type)

    # create objects in the bucket
    number_of_objects = 100
    if cloudevents:
        number_of_objects = 10
    client_threads = []
    etags = []
    objects_size = {}
    start_time = time.time()
    for i in range(number_of_objects):
        content = str(os.urandom(1024*1024))
        etag = hashlib.md5(content.encode()).hexdigest()
        etags.append(etag)
        object_size = len(content)
        key = bucket.new_key(str(i))
        objects_size[key.name] = object_size
        thr = threading.Thread(target=set_contents_from_string, args=(key, content,))
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]

    time_diff = time.time() - start_time
    print('average time for creation + ' + endpoint_type + ' notification is: ' + str(time_diff*1000/number_of_objects) + ' milliseconds')

    print('wait for 5sec for the messages...')
    time.sleep(5)

    # check receiver
    keys = list(bucket.list())
    receiver.verify_s3_events(keys, exact_match=True, deletions=False, expected_sizes=objects_size, etags=etags)

    # delete objects from the bucket
    client_threads = []
    start_time = time.time()
    for key in bucket.list():
        thr = threading.Thread(target=key.delete, args=())
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]

    time_diff = time.time() - start_time
    print('average time for deletion + ' + endpoint_type + ' notification is: ' + str(time_diff*1000/number_of_objects) + ' milliseconds')

    print('wait for 5sec for the messages...')
    time.sleep(5)

    # check receiver
    receiver.verify_s3_events(keys, exact_match=True, deletions=True, expected_sizes=objects_size, etags=etags)

    # cleanup
    s3_notification_conf.del_config()
    topic_conf.del_config()
    # delete the bucket
    conn.delete_bucket(bucket_name)
    receiver.close(task)


@attr('amqp_test')
def test_notification_push_amqp():
    """ test pushing amqp notification """
    return SkipTest("Running into an issue with amqp when we make exact_match=true")
    conn = connection()
    notification_push('amqp', conn)


@attr('manual_test')
def test_ps_s3_notification_push_amqp_idleness_check():
    """ test pushing amqp s3 notification and checking for connection idleness """
    return SkipTest("only used in manual testing")
    hostname = get_ip()
    conn = connection()
    zonegroup = get_config_zonegroup()

    # create bucket
    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(bucket_name)
    topic_name1 = bucket_name + TOPIC_SUFFIX + '_1'

    # start amqp receivers
    exchange = 'ex1'
    task1, receiver1 = create_amqp_receiver_thread(exchange, topic_name1)
    task1.start()

    # create two s3 topic
    endpoint_address = 'amqp://' + hostname
    # with acks from broker
    endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=' + exchange +'&amqp-ack-level=broker'
    topic_conf1 = PSTopicS3(conn, topic_name1, zonegroup, endpoint_args=endpoint_args)
    topic_arn1 = topic_conf1.set_config()
    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name+'_1', 'TopicArn': topic_arn1,
                         'Events': []
                       }]

    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)

    # create objects in the bucket (async)
    number_of_objects = 10
    client_threads = []
    start_time = time.time()
    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        content = str(os.urandom(1024*1024))
        thr = threading.Thread(target = set_contents_from_string, args=(key, content,))
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]

    time_diff = time.time() - start_time
    print('average time for creation + amqp notification is: ' + str(time_diff*1000/number_of_objects) + ' milliseconds')

    print('wait for 5sec for the messages...')
    time.sleep(5)

    # check amqp receiver
    keys = list(bucket.list())
    print('total number of objects: ' + str(len(keys)))
    receiver1.verify_s3_events(keys, exact_match=True)

    # delete objects from the bucket
    client_threads = []
    start_time = time.time()
    for key in bucket.list():
        thr = threading.Thread(target = key.delete, args=())
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]

    time_diff = time.time() - start_time
    print('average time for deletion + amqp notification is: ' + str(time_diff*1000/number_of_objects) + ' milliseconds')

    print('wait for 5sec for the messages...')
    time.sleep(5)

    # check amqp receiver 1 for deletions
    receiver1.verify_s3_events(keys, exact_match=True, deletions=True)

    print('waiting for 40sec for checking idleness')
    time.sleep(40)

    os.system("netstat -nnp | grep 5672");

    # do the process of uploading an object and checking for notification again
    number_of_objects = 10
    client_threads = []
    start_time = time.time()
    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        content = str(os.urandom(1024*1024))
        thr = threading.Thread(target = set_contents_from_string, args=(key, content,))
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]

    time_diff = time.time() - start_time
    print('average time for creation + amqp notification is: ' + str(time_diff*1000/number_of_objects) + ' milliseconds')

    print('wait for 5sec for the messages...')
    time.sleep(5)

    # check amqp receiver
    keys = list(bucket.list())
    print('total number of objects: ' + str(len(keys)))
    receiver1.verify_s3_events(keys, exact_match=True)

    # delete objects from the bucket
    client_threads = []
    start_time = time.time()
    for key in bucket.list():
        thr = threading.Thread(target = key.delete, args=())
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]

    time_diff = time.time() - start_time
    print('average time for deletion + amqp notification is: ' + str(time_diff*1000/number_of_objects) + ' milliseconds')

    print('wait for 5sec for the messages...')
    time.sleep(5)

    # check amqp receiver 1 for deletions
    receiver1.verify_s3_events(keys, exact_match=True, deletions=True)

    os.system("netstat -nnp | grep 5672");

    # cleanup
    stop_amqp_receiver(receiver1, task1)
    s3_notification_conf.del_config()
    topic_conf1.del_config()
    # delete the bucket
    conn.delete_bucket(bucket_name)


@attr('kafka_test')
def test_notification_push_kafka():
    """ test pushing kafka s3 notification on master """
    conn = connection()
    notification_push('kafka', conn)


@attr('http_test')
def test_ps_s3_notification_multi_delete_on_master():
    """ test deletion of multiple keys on master """
    hostname = get_ip()
    conn = connection()
    zonegroup = get_config_zonegroup()

    # create random port for the http server
    host = get_ip()
    port = random.randint(10000, 20000)
    # start an http server in a separate thread
    number_of_objects = 10
    http_server = HTTPServerWithEvents((host, port))

    # create bucket
    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX

    # create s3 topic
    endpoint_address = 'http://'+host+':'+str(port)
    endpoint_args = 'push-endpoint='+endpoint_address
    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()
    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name,
                        'TopicArn': topic_arn,
                        'Events': ['s3:ObjectRemoved:*']
                       }]
    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)

    # create objects in the bucket
    client_threads = []
    objects_size = {}
    for i in range(number_of_objects):
        content = str(os.urandom(randint(1, 1024)))
        object_size = len(content)
        key = bucket.new_key(str(i))
        objects_size[key.name] = object_size
        thr = threading.Thread(target = set_contents_from_string, args=(key, content,))
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]

    keys = list(bucket.list())

    start_time = time.time()
    delete_all_objects(conn, bucket_name)
    time_diff = time.time() - start_time
    print('average time for deletion + http notification is: ' + str(time_diff*1000/number_of_objects) + ' milliseconds')

    print('wait for 5sec for the messages...')
    time.sleep(5)

    # check http receiver
    http_server.verify_s3_events(keys, exact_match=True, deletions=True, expected_sizes=objects_size)

    # cleanup
    topic_conf.del_config()
    s3_notification_conf.del_config(notification=notification_name)
    # delete the bucket
    conn.delete_bucket(bucket_name)
    http_server.close()


@attr('http_test')
def test_notification_push_http():
    """ test pushing http s3 notification """
    conn = connection()
    notification_push('http', conn)


@attr('http_test')
def test_notification_push_cloudevents():
    """ test pushing cloudevents notification """
    conn = connection()
    notification_push('http', conn, cloudevents=True)



@attr('http_test')
def test_ps_s3_opaque_data_on_master():
    """ test that opaque id set in topic, is sent in notification on master """
    hostname = get_ip()
    conn = connection()
    zonegroup = get_config_zonegroup()

    # create random port for the http server
    host = get_ip()
    port = random.randint(10000, 20000)
    # start an http server in a separate thread
    number_of_objects = 10
    http_server = HTTPServerWithEvents((host, port))

    # create bucket
    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX

    # create s3 topic
    endpoint_address = 'http://'+host+':'+str(port)
    endpoint_args = 'push-endpoint='+endpoint_address
    opaque_data = 'http://1.2.3.4:8888'
    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args, opaque_data=opaque_data)
    topic_arn = topic_conf.set_config()
    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name,
                        'TopicArn': topic_arn,
                        'Events': []
                       }]
    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)

    # create objects in the bucket
    client_threads = []
    start_time = time.time()
    content = 'bar'
    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        thr = threading.Thread(target = set_contents_from_string, args=(key, content,))
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]

    time_diff = time.time() - start_time
    print('average time for creation + http notification is: ' + str(time_diff*1000/number_of_objects) + ' milliseconds')

    print('wait for 5sec for the messages...')
    time.sleep(5)

    # check http receiver
    keys = list(bucket.list())
    print('total number of objects: ' + str(len(keys)))
    events = http_server.get_and_reset_events()
    for event in events:
        assert_equal(event['Records'][0]['opaqueData'], opaque_data)

    # cleanup
    for key in keys:
        key.delete()
    [thr.join() for thr in client_threads]
    topic_conf.del_config()
    s3_notification_conf.del_config(notification=notification_name)
    # delete the bucket
    conn.delete_bucket(bucket_name)
    http_server.close()


def lifecycle(endpoint_type, conn, number_of_objects, topic_events, create_thread, rules_creator, record_events,
              expected_abortion=False):
    zonegroup = get_config_zonegroup()

    # create bucket
    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX

    host = get_ip()
    task = None
    port = None
    if endpoint_type == 'http':
        # create random port for the http server
        port = random.randint(10000, 20000)
        # start an http server in a separate thread
        receiver = HTTPServerWithEvents((host, port))
        endpoint_address = 'http://'+host+':'+str(port)
        endpoint_args = 'push-endpoint='+endpoint_address+'&persistent=true'
    elif endpoint_type == 'amqp':
        # start amqp receiver
        exchange = 'ex1'
        task, receiver = create_amqp_receiver_thread(exchange, topic_name)
        task.start()
        endpoint_address = 'amqp://' + host
        endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange='+exchange+'&amqp-ack-level=broker&persistent=true'
    elif endpoint_type == 'kafka':
        # start kafka receiver
        task, receiver = create_kafka_receiver_thread(topic_name)
        task.start()
        endpoint_address = 'kafka://' + host
        endpoint_args = 'push-endpoint='+endpoint_address+'&kafka-ack-level=broker&persistent=true'
    else:
        return SkipTest('Unknown endpoint type: ' + endpoint_type)

    # create s3 topic
    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()
    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name,
                        'TopicArn': topic_arn,
                        'Events': topic_events
                        }]
    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)

    # create objects in the bucket
    obj_prefix = 'ooo'
    client_threads = []
    start_time = time.time()
    content = 'bar'
    for i in range(number_of_objects):
        thr = create_thread(bucket, obj_prefix, i, content)
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]

    time_diff = time.time() - start_time
    print('average time for creation + '+endpoint_type+' notification is: ' + str(time_diff*1000/number_of_objects) + ' milliseconds')

    keys = list(bucket.list())

    # create lifecycle policy
    client = boto3.client('s3',
                          endpoint_url='http://'+conn.host+':'+str(conn.port),
                          aws_access_key_id=conn.aws_access_key_id,
                          aws_secret_access_key=conn.aws_secret_access_key)
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    response = client.put_bucket_lifecycle_configuration(Bucket=bucket_name,
                                                         LifecycleConfiguration={'Rules': rules_creator(yesterday, obj_prefix)}
                                                         )

    # start lifecycle processing
    admin(['lc', 'process'], get_config_cluster())
    print('wait for 20s for the lifecycle...')
    time.sleep(20)
    print('wait for sometime for the messages...')

    no_keys = list(bucket.list())
    wait_for_queue_to_drain(topic_name, http_port=port)
    assert_equal(len(no_keys), 0)
    event_keys = []
    events = receiver.get_and_reset_events()
    if not expected_abortion:
        assert number_of_objects * 2 <= len(events)
    for event in events:
        assert_in(event['Records'][0]['eventName'], record_events)
        event_keys.append(event['Records'][0]['s3']['object']['key'])
    for key in keys:
        key_found = False
        for event_key in event_keys:
            if event_key == key.name:
                key_found = True
                break
        if not key_found:
            err = 'no lifecycle event found for key: ' + str(key)
            log.error(events)
            assert False, err

    # cleanup
    for key in keys:
        key.delete()
    if not expected_abortion:
        [thr.join() for thr in client_threads]
    topic_conf.del_config()
    s3_notification_conf.del_config(notification=notification_name)
    # delete the bucket
    conn.delete_bucket(bucket_name)
    receiver.close(task)


def rules_creator(yesterday, obj_prefix):
    return [
        {
            'ID': 'rule1',
            'Expiration': {'Date': yesterday.isoformat()},
            'Filter': {'Prefix': obj_prefix},
            'Status': 'Enabled',
        }
    ]


def create_thread(bucket, obj_prefix, i, content):
    key = bucket.new_key(obj_prefix + str(i))
    return threading.Thread(target = set_contents_from_string, args=(key, content,))


@attr('http_test')
def test_lifecycle_http():
    """ test that when object is deleted due to lifecycle policy, http endpoint """

    conn = connection()
    lifecycle('http', conn, 10, ['s3:ObjectLifecycle:Expiration:*', 's3:LifecycleExpiration:*'], create_thread,
              rules_creator, ['LifecycleExpiration:Delete', 'ObjectLifecycle:Expiration:Current'])


@attr('kafka_test')
def test_lifecycle_kafka():
    """ test that when object is deleted due to lifecycle policy, kafka endpoint """

    conn = connection()
    lifecycle('kafka', conn, 10, ['s3:ObjectLifecycle:Expiration:*', 's3:LifecycleExpiration:*'], create_thread,
              rules_creator, ['LifecycleExpiration:Delete', 'ObjectLifecycle:Expiration:Current'])


def start_and_abandon_multipart_upload(bucket, key_name, content):
    try:
        mp = bucket.initiate_multipart_upload(key_name)
        part_data = io.StringIO(content)
        mp.upload_part_from_file(part_data, 1)
    except Exception as e:
        print('Error: ' + str(e))


@attr('http_test')
def test_lifecycle_abort_mpu():
    """ test that when a multipart upload is aborted by lifecycle policy, http endpoint """

    def rules_creator(yesterday, obj_prefix):
        return [
            {
                'ID': 'abort1',
                'Filter': {'Prefix': obj_prefix},
                'Status': 'Enabled',
                'AbortIncompleteMultipartUpload': {'DaysAfterInitiation': 1},
            }
        ]

    def create_thread(bucket, obj_prefix, i, content):
        key_name = obj_prefix + str(i)
        return threading.Thread(target = start_and_abandon_multipart_upload, args=(bucket, key_name, content,))

    conn = connection()
    lifecycle('http', conn, 1, ['s3:ObjectLifecycle:Expiration:*'], create_thread, rules_creator,
              ['ObjectLifecycle:Expiration:AbortMultipartUpload'], True)


def ps_s3_creation_triggers_on_master(external_endpoint_address=None, ca_location=None, verify_ssl='true'):
    """ test object creation s3 notifications in using put/copy/post on master"""
    
    if not external_endpoint_address:
        hostname = get_ip()
        proc = init_rabbitmq()
        if proc is  None:
            return SkipTest('end2end amqp tests require rabbitmq-server installed')
    else:
        proc = None

    conn = connection()
    zonegroup = get_config_zonegroup()

    # create bucket
    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX

    # start amqp receiver
    exchange = 'ex1'
    task, receiver = create_amqp_receiver_thread(exchange, topic_name, external_endpoint_address, ca_location)
    task.start()

    # create s3 topic
    if external_endpoint_address:
        endpoint_address = external_endpoint_address
    elif ca_location:
        endpoint_address = 'amqps://' + hostname
    else:
        endpoint_address = 'amqp://' + hostname
    endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=' + exchange +'&amqp-ack-level=broker&verify-ssl='+verify_ssl
    if ca_location:
        endpoint_args += '&ca-location={}'.format(ca_location)
    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()
    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name,'TopicArn': topic_arn,
                        'Events': ['s3:ObjectCreated:Put', 's3:ObjectCreated:Copy', 's3:ObjectCreated:CompleteMultipartUpload']
                       }]

    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)

    objects_size = {}
    # create objects in the bucket using PUT
    content = str(os.urandom(randint(1, 1024)))
    key_name = 'put'
    key = bucket.new_key(key_name)
    objects_size[key_name] = len(content)
    key.set_contents_from_string(content)
    # create objects in the bucket using COPY
    key_name = 'copy'
    bucket.copy_key(key_name, bucket.name, key.name)
    objects_size[key_name] = len(content)

    # create objects in the bucket using multi-part upload
    fp = tempfile.NamedTemporaryFile(mode='w+b')
    content = bytearray(os.urandom(10*1024*1024))
    key_name = 'multipart'
    objects_size[key_name] = len(content)
    fp.write(content)
    fp.flush()
    fp.seek(0)
    uploader = bucket.initiate_multipart_upload(key_name)
    uploader.upload_part_from_file(fp, 1)
    uploader.complete_upload()
    fp.close()

    print('wait for 5sec for the messages...')
    time.sleep(5)

    # check amqp receiver
    keys = list(bucket.list())
    receiver.verify_s3_events(keys, exact_match=True, expected_sizes=objects_size)

    # cleanup
    stop_amqp_receiver(receiver, task)
    s3_notification_conf.del_config()
    topic_conf.del_config()
    for key in bucket.list():
        key.delete()
    # delete the bucket
    conn.delete_bucket(bucket_name)
    if proc:
        clean_rabbitmq(proc)


@attr('amqp_test')
def test_ps_s3_creation_triggers_on_master():
    ps_s3_creation_triggers_on_master(external_endpoint_address="amqp://localhost:5672")


@attr('amqp_ssl_test')
def test_ps_s3_creation_triggers_on_master_external():

    from distutils.util import strtobool

    if 'AMQP_EXTERNAL_ENDPOINT' in os.environ:
        try:
            if strtobool(os.environ['AMQP_VERIFY_SSL']):
                verify_ssl = 'true'
            else:
                verify_ssl = 'false'
        except Exception as e:
            verify_ssl = 'true'

        ps_s3_creation_triggers_on_master(
            external_endpoint_address=os.environ['AMQP_EXTERNAL_ENDPOINT'],
            verify_ssl=verify_ssl)
    else:
        return SkipTest("Set AMQP_EXTERNAL_ENDPOINT to a valid external AMQP endpoint url for this test to run")


def generate_private_key(tempdir):

    import datetime
    import stat
    from cryptography import x509
    from cryptography.x509.oid import NameOID
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    # modify permissions to ensure that the broker user can access them
    os.chmod(tempdir, mode=stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)
    CACERTFILE = os.path.join(tempdir, 'ca_certificate.pem')
    CERTFILE = os.path.join(tempdir, 'server_certificate.pem')
    KEYFILE = os.path.join(tempdir, 'server_key.pem')

    root_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
        backend=default_backend()
    )
    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COUNTRY_NAME, u"UK"),
        x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, u"Oxfordshire"),
        x509.NameAttribute(NameOID.LOCALITY_NAME, u"Harwell"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, u"Rosalind Franklin Institute"),
        x509.NameAttribute(NameOID.COMMON_NAME, u"RFI CA"),
    ])
    root_cert = x509.CertificateBuilder().subject_name(
        subject
    ).issuer_name(
        issuer
    ).public_key(
        root_key.public_key()
    ).serial_number(
        x509.random_serial_number()
    ).not_valid_before(
        datetime.datetime.utcnow()
    ).not_valid_after(
        datetime.datetime.utcnow() + datetime.timedelta(days=3650)
    ).add_extension(
        x509.BasicConstraints(ca=True, path_length=None), critical=True
    ).sign(root_key, hashes.SHA256(), default_backend())
    with open(CACERTFILE, "wb") as f:
        f.write(root_cert.public_bytes(serialization.Encoding.PEM))

    # Now we want to generate a cert from that root
    cert_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
        backend=default_backend(),
    )
    with open(KEYFILE, "wb") as f:
        f.write(cert_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        ))
    new_subject = x509.Name([
        x509.NameAttribute(NameOID.COUNTRY_NAME, u"UK"),
        x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, u"Oxfordshire"),
        x509.NameAttribute(NameOID.LOCALITY_NAME, u"Harwell"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, u"Rosalind Franklin Institute"),
    ])
    cert = x509.CertificateBuilder().subject_name(
        new_subject
    ).issuer_name(
        root_cert.issuer
    ).public_key(
        cert_key.public_key()
    ).serial_number(
        x509.random_serial_number()
    ).not_valid_before(
        datetime.datetime.utcnow()
    ).not_valid_after(
    datetime.datetime.utcnow() + datetime.timedelta(days=30)
    ).add_extension(
        x509.SubjectAlternativeName([x509.DNSName(u"localhost")]),
        critical=False,
    ).sign(root_key, hashes.SHA256(), default_backend())
    # Write our certificate out to disk.
    with open(CERTFILE, "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))

    print("\n\n********private key generated********")
    print(CACERTFILE, CERTFILE, KEYFILE)
    print("\n\n")
    return CACERTFILE, CERTFILE, KEYFILE


@attr('amqp_ssl_test')
def test_ps_s3_creation_triggers_on_master_ssl():

    import textwrap
    from tempfile import TemporaryDirectory

    with TemporaryDirectory() as tempdir:
        CACERTFILE, CERTFILE, KEYFILE = generate_private_key(tempdir)
        RABBITMQ_CONF_FILE = os.path.join(tempdir, 'rabbitmq.config')
        with open(RABBITMQ_CONF_FILE, "w") as f:
            # use the old style config format to ensure it also runs on older RabbitMQ versions.
            f.write(textwrap.dedent(f'''
                [
                    {{rabbit, [
                        {{ssl_listeners, [5671]}},
                        {{ssl_options, [{{cacertfile,           "{CACERTFILE}"}},
                                        {{certfile,             "{CERTFILE}"}},
                                        {{keyfile,              "{KEYFILE}"}},
                                        {{verify,               verify_peer}},
                                        {{fail_if_no_peer_cert, false}}]}}]}}
                ].
            '''))
        os.environ['RABBITMQ_CONFIG_FILE'] = os.path.splitext(RABBITMQ_CONF_FILE)[0]

        ps_s3_creation_triggers_on_master(ca_location=CACERTFILE)

        del os.environ['RABBITMQ_CONFIG_FILE']


@attr('amqp_test')
def test_http_post_object_upload():
    """ test that uploads object using HTTP POST """

    import boto3
    from collections import OrderedDict
    import requests

    hostname = get_ip()
    zonegroup = get_config_zonegroup()
    conn = connection()

    endpoint = "http://%s:%d" % (get_config_host(), get_config_port())

    conn1 = boto3.client(service_name='s3',
                         aws_access_key_id=get_access_key(),
                         aws_secret_access_key=get_secret_key(),
                         endpoint_url=endpoint,
                        )

    bucket_name = gen_bucket_name()
    topic_name = bucket_name + TOPIC_SUFFIX

    key_name = 'foo.txt'

    resp = conn1.generate_presigned_post(Bucket=bucket_name, Key=key_name,)

    url = resp['url']

    bucket = conn1.create_bucket(ACL='public-read-write', Bucket=bucket_name)

    # start amqp receivers
    exchange = 'ex1'
    task1, receiver1 = create_amqp_receiver_thread(exchange, topic_name+'_1')
    task1.start()

    # create s3 topics
    endpoint_address = 'amqp://' + hostname
    endpoint_args = 'push-endpoint=' + endpoint_address + '&amqp-exchange=' + exchange + '&amqp-ack-level=broker'
    topic_conf1 = PSTopicS3(conn, topic_name+'_1', zonegroup, endpoint_args=endpoint_args)
    topic_arn1 = topic_conf1.set_config()

    # create s3 notifications
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name+'_1', 'TopicArn': topic_arn1,
                        'Events': ['s3:ObjectCreated:Post']
                       }]
    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)

    payload = OrderedDict([("key" , "foo.txt"),("acl" , "public-read"),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    # POST upload
    r = requests.post(url, files=payload, verify=True)
    assert_equal(r.status_code, 204)

    # check amqp receiver
    events = receiver1.get_and_reset_events()
    assert_equal(len(events), 1)

    # cleanup
    stop_amqp_receiver(receiver1, task1)
    s3_notification_conf.del_config()
    topic_conf1.del_config()
    conn1.delete_object(Bucket=bucket_name, Key=key_name)
    # delete the bucket
    conn1.delete_bucket(Bucket=bucket_name)


def multipart_endpoint_agnostic(endpoint_type, conn):
    hostname = get_ip()
    zonegroup = get_config_zonegroup()

    # create bucket
    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX

    host = get_ip()
    task = None
    if endpoint_type == 'http':
        # create random port for the http server
        port = random.randint(10000, 20000)
        # start an http server in a separate thread
        receiver = HTTPServerWithEvents((hostname, port))
        endpoint_address = 'http://'+host+':'+str(port)
        endpoint_args = 'push-endpoint='+endpoint_address
    elif endpoint_type == 'amqp':
        # start amqp receiver
        exchange = 'ex1'
        task, receiver = create_amqp_receiver_thread(exchange, topic_name)
        task.start()
        endpoint_address = 'amqp://' + host
        endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange='+exchange+'&amqp-ack-level=broker'
    elif endpoint_type == 'kafka':
        # start amqp receiver
        task, receiver = create_kafka_receiver_thread(topic_name)
        task.start()
        endpoint_address = 'kafka://' + host
        endpoint_args = 'push-endpoint='+endpoint_address+'&kafka-ack-level=broker'
    else:
        return SkipTest('Unknown endpoint type: ' + endpoint_type)

    # create s3 topic
    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()
    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name, 'TopicArn': topic_arn,
                        'Events': []
                        }]

    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)

    # create objects in the bucket
    client_threads = []
    content = str(os.urandom(20*1024*1024))
    key = bucket.new_key('obj')
    thr = threading.Thread(target=set_contents_from_string, args=(key, content,))
    thr.start()
    client_threads.append(thr)
    [thr.join() for thr in client_threads]

    print('wait for 5sec for the messages...')
    time.sleep(5)

    # check http receiver
    keys = list(bucket.list())
    receiver.verify_s3_events(keys, exact_match=True, deletions=False)

    # cleanup
    s3_notification_conf.del_config()
    topic_conf.del_config()
    # delete objects
    for key in keys:
        key.delete()
    # delete the bucket
    conn.delete_bucket(bucket_name)
    receiver.close(task)


@attr('http_test')
def test_multipart_http():
    """ test http multipart object upload """
    conn = connection()
    multipart_endpoint_agnostic('http', conn)


@attr('kafka_test')
def test_multipart_kafka():
    """ test kafka multipart object upload """
    conn = connection()
    multipart_endpoint_agnostic('kafka', conn)


@attr('amqp_test')
def test_multipart_ampq():
    """ test ampq multipart object upload """
    conn = connection()
    multipart_endpoint_agnostic('ampq', conn)


def metadata_filter(endpoint_type, conn):
    # create bucket
    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX 

    # start endpoint receiver
    host = get_ip()
    task = None
    port = None
    if endpoint_type == 'http':
        # create random port for the http server
        port = random.randint(10000, 20000)
        # start an http server in a separate thread
        receiver = HTTPServerWithEvents((host, port))
        endpoint_address = 'http://'+host+':'+str(port)
        endpoint_args = 'push-endpoint='+endpoint_address+'&persistent=true'
    elif endpoint_type == 'amqp':
        # start amqp receiver
        exchange = 'ex1'
        task, receiver = create_amqp_receiver_thread(exchange, topic_name)
        task.start()
        endpoint_address = 'amqp://' + host
        endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=' + exchange +'&amqp-ack-level=routable&persistent=true'
    elif endpoint_type == 'kafka':
        # start kafka receiver
        task, receiver = create_kafka_receiver_thread(topic_name)
        task.start()
        endpoint_address = 'kafka://' + host
        endpoint_args = 'push-endpoint='+endpoint_address+'&kafka-ack-level=broker&persistent=true'
    else:
        return SkipTest('Unknown endpoint type: ' + endpoint_type)

    # create s3 topic
    zonegroup = get_config_zonegroup()
    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()
    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    meta_key = 'meta1'
    meta_value = 'This is my metadata value'
    topic_conf_list = [{'Id': notification_name, 'TopicArn': topic_arn,
        'Events': ['s3:ObjectCreated:*', 's3:ObjectRemoved:*'],
        'Filter': {
            'Metadata': {
                'FilterRules': [{'Name': META_PREFIX+meta_key, 'Value': meta_value}]
            }
        }
    }]

    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    _, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)

    expected_keys = []
    # create objects in the bucket
    key_name = 'foo'
    key = bucket.new_key(key_name)
    key.set_metadata(meta_key, meta_value)
    key.set_contents_from_string('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')
    expected_keys.append(key_name)

    # create objects in the bucket using COPY
    key_name = 'copy_of_foo'
    bucket.copy_key(key_name, bucket.name, key.name)
    expected_keys.append(key_name)
    
    # create another objects in the bucket using COPY
    # but override the metadata value
    key_name = 'another_copy_of_foo'
    bucket.copy_key(key_name, bucket.name, key.name, metadata={meta_key: 'kaboom'})
    # this key is not in the expected keys due to the different meta value

    # create objects in the bucket using multi-part upload
    fp = tempfile.NamedTemporaryFile(mode='w+b')
    chunk_size = 1024*1024*5 # 5MB
    object_size = 10*chunk_size
    content = bytearray(os.urandom(object_size))
    fp.write(content)
    fp.flush()
    fp.seek(0)
    key_name = 'multipart_foo'
    uploader = bucket.initiate_multipart_upload(key_name,
            metadata={meta_key: meta_value})
    for i in range(1,5):
        uploader.upload_part_from_file(fp, i, size=chunk_size)
        fp.seek(i*chunk_size)
    uploader.complete_upload()
    fp.close()
    expected_keys.append(key_name)

    print('wait for the messages...')
    wait_for_queue_to_drain(topic_name, http_port=port)
    # check amqp receiver
    events = receiver.get_and_reset_events()
    assert_equal(len(events), len(expected_keys))
    for event in events:
        assert(event['Records'][0]['s3']['object']['key'] in expected_keys)

    # delete objects
    for key in bucket.list():
        key.delete()
    print('wait for the messages...')
    wait_for_queue_to_drain(topic_name, http_port=port)
    # check endpoint receiver
    events = receiver.get_and_reset_events()
    assert_equal(len(events), len(expected_keys))
    for event in events:
        assert(event['Records'][0]['s3']['object']['key'] in expected_keys)

    # cleanup
    receiver.close(task)
    s3_notification_conf.del_config()
    topic_conf.del_config()
    # delete the bucket
    conn.delete_bucket(bucket_name)


@attr('kafka_test')
def test_metadata_filter_kafka():
    """ test notification of filtering metadata, kafka endpoint """
    conn = connection()
    metadata_filter('kafka', conn)


@attr('http_test')
def test_metadata_filter_http():
    """ test notification of filtering metadata, http endpoint """
    conn = connection()
    metadata_filter('http', conn)


@attr('amqp_test')
def test_metadata_filter_ampq():
    """ test notification of filtering metadata, ampq endpoint """
    conn = connection()
    metadata_filter('amqp', conn)


@attr('amqp_test')
def test_ps_s3_metadata_on_master():
    """ test s3 notification of metadata on master """

    hostname = get_ip()
    conn = connection()
    zonegroup = get_config_zonegroup()

    # create bucket
    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX

    # start amqp receivers
    exchange = 'ex1'
    task, receiver = create_amqp_receiver_thread(exchange, topic_name)
    task.start()

    # create s3 topic
    endpoint_address = 'amqp://' + hostname
    endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=' + exchange +'&amqp-ack-level=routable'
    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()
    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    meta_key = 'meta1'
    meta_value = 'This is my metadata value'
    meta_prefix = META_PREFIX
    topic_conf_list = [{'Id': notification_name, 'TopicArn': topic_arn,
        'Events': ['s3:ObjectCreated:*', 's3:ObjectRemoved:*'],
    }]

    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    _, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)

    # create objects in the bucket
    key_name = 'foo'
    key = bucket.new_key(key_name)
    key.set_metadata(meta_key, meta_value)
    key.set_contents_from_string('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')
    # update the object
    another_meta_key = 'meta2'
    key.set_metadata(another_meta_key, meta_value)
    key.set_contents_from_string('bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb')

    # create objects in the bucket using COPY
    key_name = 'copy_of_foo'
    bucket.copy_key(key_name, bucket.name, key.name)
    
    # create objects in the bucket using multi-part upload
    fp = tempfile.NamedTemporaryFile(mode='w+b')
    chunk_size = 1024*1024*5 # 5MB
    object_size = 10*chunk_size
    content = bytearray(os.urandom(object_size))
    fp.write(content)
    fp.flush()
    fp.seek(0)
    key_name = 'multipart_foo'
    uploader = bucket.initiate_multipart_upload(key_name,
            metadata={meta_key: meta_value})
    for i in range(1,5):
        uploader.upload_part_from_file(fp, i, size=chunk_size)
        fp.seek(i*chunk_size)
    uploader.complete_upload()
    fp.close()

    print('wait for 5sec for the messages...')
    time.sleep(5)
    # check amqp receiver
    events = receiver.get_and_reset_events()
    for event in events:
        value = [x['val'] for x in event['Records'][0]['s3']['object']['metadata'] if x['key'] == META_PREFIX+meta_key]
        assert_equal(value[0], meta_value)

    # delete objects
    for key in bucket.list():
        key.delete()
    print('wait for 5sec for the messages...')
    time.sleep(5)
    # check amqp receiver
    events = receiver.get_and_reset_events()
    for event in events:
        value = [x['val'] for x in event['Records'][0]['s3']['object']['metadata'] if x['key'] == META_PREFIX+meta_key]
        assert_equal(value[0], meta_value)

    # cleanup
    stop_amqp_receiver(receiver, task)
    s3_notification_conf.del_config()
    topic_conf.del_config()
    # delete the bucket
    conn.delete_bucket(bucket_name)


@attr('amqp_test')
def test_ps_s3_tags_on_master():
    """ test s3 notification of tags on master """

    hostname = get_ip()
    conn = connection()
    zonegroup = get_config_zonegroup()

    # create bucket
    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX

    # start amqp receiver
    exchange = 'ex1'
    task, receiver = create_amqp_receiver_thread(exchange, topic_name)
    task.start()

    # create s3 topic
    endpoint_address = 'amqp://' + hostname
    endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=' + exchange +'&amqp-ack-level=routable'
    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()
    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name,'TopicArn': topic_arn,
        'Events': ['s3:ObjectCreated:*', 's3:ObjectRemoved:*'],
        'Filter': {
            'Tags': {
                'FilterRules': [{'Name': 'hello', 'Value': 'world'}, {'Name': 'ka', 'Value': 'boom'}]
            }
        }
    }]

    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)

    expected_keys = []
    # create objects in the bucket with tags
    # key 1 has all the tags in the filter
    tags = 'hello=world&ka=boom&hello=helloworld'
    key_name1 = 'key1'
    put_object_tagging(conn, bucket_name, key_name1, tags)
    expected_keys.append(key_name1)
    # key 2 has an additional tag not in the filter
    tags = 'hello=world&foo=bar&ka=boom&hello=helloworld'
    key_name = 'key2'
    put_object_tagging(conn, bucket_name, key_name, tags)
    expected_keys.append(key_name)
    # key 3 has no tags
    key_name3 = 'key3'
    key = bucket.new_key(key_name3)
    key.set_contents_from_string('bar')
    # key 4 has the wrong of the multi value tags
    tags = 'hello=helloworld&ka=boom'
    key_name = 'key4'
    put_object_tagging(conn, bucket_name, key_name, tags)
    # key 5 has the right of the multi value tags
    tags = 'hello=world&ka=boom'
    key_name = 'key5'
    put_object_tagging(conn, bucket_name, key_name, tags)
    expected_keys.append(key_name)
    # key 6 is missing a tag
    tags = 'hello=world'
    key_name = 'key6'
    put_object_tagging(conn, bucket_name, key_name, tags)
    # create objects in the bucket using COPY
    key_name = 'copy_of_'+key_name1
    bucket.copy_key(key_name, bucket.name, key_name1)
    expected_keys.append(key_name)

    print('wait for 5sec for the messages...')
    time.sleep(5)
    event_count = 0
    expected_tags1 = [{'key': 'hello', 'val': 'world'}, {'key': 'hello', 'val': 'helloworld'}, {'key': 'ka', 'val': 'boom'}]
    expected_tags1 = sorted(expected_tags1, key=lambda k: k['key']+k['val'])
    for event in receiver.get_and_reset_events():
        key = event['Records'][0]['s3']['object']['key']
        if (key == key_name1):
            obj_tags =  sorted(event['Records'][0]['s3']['object']['tags'], key=lambda k: k['key']+k['val'])
            assert_equal(obj_tags, expected_tags1)
        event_count += 1
        assert(key in expected_keys)

    assert_equal(event_count, len(expected_keys))

    # delete the objects
    for key in bucket.list():
        key.delete()
    print('wait for 5sec for the messages...')
    time.sleep(5)
    event_count = 0
    # check amqp receiver
    for event in receiver.get_and_reset_events():
        key = event['Records'][0]['s3']['object']['key']
        if (key == key_name1):
            obj_tags =  sorted(event['Records'][0]['s3']['object']['tags'], key=lambda k: k['key']+k['val'])
            assert_equal(obj_tags, expected_tags1)
        event_count += 1
        assert(key in expected_keys)

    assert(event_count == len(expected_keys))

    # cleanup
    stop_amqp_receiver(receiver, task)
    s3_notification_conf.del_config()
    topic_conf.del_config()
    # delete the bucket
    conn.delete_bucket(bucket_name)

@attr('amqp_test')
def test_ps_s3_versioning_on_master():
    """ test s3 notification of object versions """

    hostname = get_ip()
    conn = connection()
    zonegroup = get_config_zonegroup()

    # create bucket
    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(bucket_name)
    bucket.configure_versioning(True)
    topic_name = bucket_name + TOPIC_SUFFIX

    # start amqp receiver
    exchange = 'ex1'
    task, receiver = create_amqp_receiver_thread(exchange, topic_name)
    task.start()

    # create s3 topic
    endpoint_address = 'amqp://' + hostname
    endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=' + exchange +'&amqp-ack-level=broker'
    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()
    # create notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name, 'TopicArn': topic_arn,
                        'Events': []
                       }]
    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    _, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)

    # create objects in the bucket
    key_name = 'foo'
    key = bucket.new_key(key_name)
    key.set_contents_from_string('hello')
    ver1 = key.version_id
    key.set_contents_from_string('world')
    ver2 = key.version_id
    copy_of_key = bucket.copy_key('copy_of_foo', bucket.name, key_name, src_version_id=ver1)
    ver3 = copy_of_key.version_id
    versions = [ver1, ver2, ver3]

    print('wait for 5sec for the messages...')
    time.sleep(5)

    # check amqp receiver
    events = receiver.get_and_reset_events()
    num_of_versions = 0
    for event_list in events:
        for event in event_list['Records']:
            assert event['s3']['object']['key'] in (key_name, copy_of_key.name)
            version = event['s3']['object']['versionId']
            num_of_versions += 1
            if version not in versions:
                print('version mismatch: '+version+' not in: '+str(versions))
                # TODO: copy_key() does not return the version of the copied object
                #assert False 
            else:
                print('version ok: '+version+' in: '+str(versions))

    assert_equal(num_of_versions, 3)

    # cleanup
    stop_amqp_receiver(receiver, task)
    s3_notification_conf.del_config()
    topic_conf.del_config()
    # delete the bucket
    bucket.delete_key(copy_of_key, version_id=ver3)
    bucket.delete_key(key.name, version_id=ver2)
    bucket.delete_key(key.name, version_id=ver1)
    #conn.delete_bucket(bucket_name)


@attr('amqp_test')
def test_ps_s3_versioned_deletion_on_master():
    """ test s3 notification of deletion markers on master """

    hostname = get_ip()
    conn = connection()
    zonegroup = get_config_zonegroup()

    # create bucket
    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(bucket_name)
    bucket.configure_versioning(True)
    topic_name = bucket_name + TOPIC_SUFFIX

    # start amqp receiver
    exchange = 'ex1'
    task, receiver = create_amqp_receiver_thread(exchange, topic_name)
    task.start()

    # create s3 topic
    endpoint_address = 'amqp://' + hostname
    endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=' + exchange +'&amqp-ack-level=broker'
    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()
    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name+'_1', 'TopicArn': topic_arn,
                        'Events': ['s3:ObjectRemoved:*']
                       },
                       {'Id': notification_name+'_2', 'TopicArn': topic_arn,
                        'Events': ['s3:ObjectRemoved:DeleteMarkerCreated']
                       },
                       {'Id': notification_name+'_3', 'TopicArn': topic_arn,
                         'Events': ['s3:ObjectRemoved:Delete']
                       }]
    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)

    # create objects in the bucket
    key = bucket.new_key('foo')
    content = str(os.urandom(512))
    size1 = len(content)
    key.set_contents_from_string(content)
    ver1 = key.version_id
    content = str(os.urandom(511))
    size2 = len(content)
    key.set_contents_from_string(content)
    ver2 = key.version_id
    # create delete marker (non versioned deletion)
    delete_marker_key = bucket.delete_key(key.name)
    versions = [ver1, ver2, delete_marker_key.version_id]

    time.sleep(1)

    # versioned deletion
    bucket.delete_key(key.name, version_id=ver2)
    bucket.delete_key(key.name, version_id=ver1)

    print('wait for 5sec for the messages...')
    time.sleep(5)

    # check amqp receiver
    events = receiver.get_and_reset_events()
    delete_events = 0
    delete_marker_create_events = 0
    for event_list in events:
        for event in event_list['Records']:
            version = event['s3']['object']['versionId']
            size = event['s3']['object']['size']
            if version not in versions:
                print('version mismatch: '+version+' not in: '+str(versions))
                assert False 
            else:
                print('version ok: '+version+' in: '+str(versions))
            if event['eventName'] == 'ObjectRemoved:Delete':
                delete_events += 1
                assert size in [size1, size2]
                assert event['s3']['configurationId'] in [notification_name+'_1', notification_name+'_3']
            if event['eventName'] == 'ObjectRemoved:DeleteMarkerCreated':
                delete_marker_create_events += 1
                assert size == size2
                assert event['s3']['configurationId'] in [notification_name+'_1', notification_name+'_2']

    # 2 key versions were deleted
    # notified over the same topic via 2 notifications (1,3)
    assert_equal(delete_events, 2*2)
    # 1 deletion marker was created
    # notified over the same topic over 2 notifications (1,2)
    assert_equal(delete_marker_create_events, 1*2)

    # cleanup
    delete_marker_key.delete()
    stop_amqp_receiver(receiver, task)
    s3_notification_conf.del_config()
    topic_conf.del_config()
    # delete the bucket
    conn.delete_bucket(bucket_name)


@attr('manual_test')
def test_ps_s3_persistent_cleanup():
    """ test reservation cleanup after gateway crash """
    return SkipTest("only used in manual testing")
    conn = connection()
    zonegroup = get_config_zonegroup()

    # create random port for the http server
    host = get_ip()
    port = random.randint(10000, 20000)
    # start an http server in a separate thread
    number_of_objects = 200
    http_server = HTTPServerWithEvents((host, port))

    gw = conn

    # create bucket
    bucket_name = gen_bucket_name()
    bucket = gw.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX

    # create s3 topic
    endpoint_address = 'http://'+host+':'+str(port)
    endpoint_args = 'push-endpoint='+endpoint_address+'&persistent=true'
    topic_conf = PSTopicS3(gw, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()

    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name, 'TopicArn': topic_arn,
        'Events': ['s3:ObjectCreated:Put']
        }]
    s3_notification_conf = PSNotificationS3(gw, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)

    client_threads = []
    start_time = time.time()
    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        content = str(os.urandom(1024*1024))
        thr = threading.Thread(target = set_contents_from_string, args=(key, content,))
        thr.start()
        client_threads.append(thr)
    # stop gateway while clients are sending
    os.system("killall -9 radosgw");
    print('wait for 10 sec for before restarting the gateway')
    time.sleep(10)
    # TODO: start the radosgw
    [thr.join() for thr in client_threads]

    keys = list(bucket.list())

    # delete objects from the bucket
    client_threads = []
    start_time = time.time()
    for key in bucket.list():
        thr = threading.Thread(target = key.delete, args=())
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]

    # check http receiver
    events = http_server.get_and_reset_events()

    print(str(len(events) ) + " events found out of " + str(number_of_objects))

    # make sure that things are working now
    client_threads = []
    start_time = time.time()
    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        content = str(os.urandom(1024*1024))
        thr = threading.Thread(target = set_contents_from_string, args=(key, content,))
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]

    keys = list(bucket.list())

    # delete objects from the bucket
    client_threads = []
    start_time = time.time()
    for key in bucket.list():
        thr = threading.Thread(target = key.delete, args=())
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]

    print('wait for 180 sec for reservations to be stale before queue deletion')
    time.sleep(180)

    # check http receiver
    events = http_server.get_and_reset_events()

    print(str(len(events)) + " events found out of " + str(number_of_objects))

    # cleanup
    s3_notification_conf.del_config()
    topic_conf.del_config()
    gw.delete_bucket(bucket_name)
    http_server.close()


def check_http_server(http_port):
    str_port = str(http_port)
    cmd = 'netstat -tlnnp | grep python | grep '+str_port
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
    out = proc.communicate()[0]
    assert len(out) > 0, 'http python server NOT listening on port '+str_port
    log.info("http python server listening on port "+str_port)
    log.info(out.decode('utf-8'))


def wait_for_queue_to_drain(topic_name, tenant=None, account=None, http_port=None):
    retries = 0
    entries = 1
    start_time = time.time()
    # topic stats
    cmd = ['topic', 'stats', '--topic', topic_name]
    if tenant:
        cmd += ['--tenant', tenant]
    if account:
        cmd += ['--account-id', account]
    while entries > 0:
        if http_port:
            check_http_server(http_port)
        result = admin(cmd, get_config_cluster())
        assert_equal(result[1], 0)
        parsed_result = json.loads(result[0])
        entries = parsed_result['Topic Stats']['Entries']
        retries += 1
        time_diff = time.time() - start_time
        log.info('queue %s has %d entries after %ds', topic_name, entries, time_diff)
        if retries > 30:
            log.warning('queue %s still has %d entries after %ds', topic_name, entries, time_diff)
            assert_equal(entries, 0)
        time.sleep(5)
    time_diff = time.time() - start_time
    log.info('waited for %ds for queue %s to drain', time_diff, topic_name)


@attr('basic_test')
def test_ps_s3_persistent_topic_stats():
    """ test persistent topic stats """
    conn = connection()
    zonegroup = get_config_zonegroup()

    # create random port for the http server
    host = get_ip()
    port = random.randint(10000, 20000)

    # create bucket
    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX

    # create s3 topic
    endpoint_address = 'http://'+host+':'+str(port)
    endpoint_args = 'push-endpoint='+endpoint_address+'&persistent=true'+ \
            '&retry_sleep_duration=1'
    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()
    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name, 'TopicArn': topic_arn,
                        'Events': []
                        }]

    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)

    # topic stats
    get_stats_persistent_topic(topic_name, 0)

    # create objects in the bucket (async)
    number_of_objects = 20
    client_threads = []
    start_time = time.time()
    for i in range(number_of_objects):
        key = bucket.new_key('key-'+str(i))
        content = str(os.urandom(1024*1024))
        thr = threading.Thread(target = set_contents_from_string, args=(key, content,))
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]
    time_diff = time.time() - start_time
    print('average time for creation + async http notification is: ' + str(time_diff*1000/number_of_objects) + ' milliseconds')

    # topic stats
    get_stats_persistent_topic(topic_name, number_of_objects)

    # delete objects from the bucket
    client_threads = []
    start_time = time.time()
    for key in bucket.list():
        thr = threading.Thread(target = key.delete, args=())
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]
    time_diff = time.time() - start_time
    print('average time for deletion + async http notification is: ' + str(time_diff*1000/number_of_objects) + ' milliseconds')

    # topic stats
    get_stats_persistent_topic(topic_name, 2 * number_of_objects)

    # start an http server in a separate thread
    http_server = HTTPServerWithEvents((host, port))

    wait_for_queue_to_drain(topic_name, http_port=port)

    # cleanup
    s3_notification_conf.del_config()
    topic_conf.del_config()
    # delete the bucket
    conn.delete_bucket(bucket_name)
    http_server.close()

@attr('basic_test')
def test_persistent_topic_dump():
    """ test persistent topic dump """
    conn = connection()
    zonegroup = get_config_zonegroup()

    # create random port for the http server
    host = get_ip()
    port = random.randint(10000, 20000)

    # create bucket
    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX

    # create s3 topic
    endpoint_address = 'http://'+host+':'+str(port)
    endpoint_args = 'push-endpoint='+endpoint_address+'&persistent=true'+ \
            '&retry_sleep_duration=1'
    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()
    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name, 'TopicArn': topic_arn,
                        'Events': []
                        }]

    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)

    # create objects in the bucket (async)
    number_of_objects = 20
    client_threads = []
    start_time = time.time()
    for i in range(number_of_objects):
        key = bucket.new_key('key-'+str(i))
        content = str(os.urandom(1024*1024))
        thr = threading.Thread(target = set_contents_from_string, args=(key, content,))
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]
    time_diff = time.time() - start_time
    print('average time for creation + async http notification is: ' + str(time_diff*1000/number_of_objects) + ' milliseconds')

    # topic dump
    result = admin(['topic', 'dump', '--topic', topic_name], get_config_cluster())
    assert_equal(result[1], 0)
    parsed_result = json.loads(result[0])
    assert_equal(len(parsed_result), number_of_objects)

    # delete objects from the bucket
    client_threads = []
    start_time = time.time()
    for key in bucket.list():
        thr = threading.Thread(target = key.delete, args=())
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]
    time_diff = time.time() - start_time
    print('average time for deletion + async http notification is: ' + str(time_diff*1000/number_of_objects) + ' milliseconds')

    # topic stats
    result = admin(['topic', 'dump', '--topic', topic_name], get_config_cluster())
    assert_equal(result[1], 0)
    print(result[0])
    parsed_result = json.loads(result[0])
    assert_equal(len(parsed_result), 2*number_of_objects)

    # start an http server in a separate thread
    http_server = HTTPServerWithEvents((host, port))

    wait_for_queue_to_drain(topic_name, http_port=port)

    result = admin(['topic', 'dump', '--topic', topic_name], get_config_cluster())
    assert_equal(result[1], 0)
    parsed_result = json.loads(result[0])
    assert_equal(len(parsed_result), 0)

    # cleanup
    s3_notification_conf.del_config()
    topic_conf.del_config()
    # delete the bucket
    conn.delete_bucket(bucket_name)
    http_server.close()


def ps_s3_persistent_topic_configs(persistency_time, config_dict):
    conn = connection()
    zonegroup = get_config_zonegroup()

    # create random port for the http server
    host = get_ip()
    port = random.randint(10000, 20000)

    # start an http server in a separate thread
    http_server = HTTPServerWithEvents((host, port))

    # create bucket
    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX

    # create s3 topic
    endpoint_address = 'http://'+host+':'+str(port)
    endpoint_args = 'push-endpoint='+endpoint_address+'&persistent=true&'+create_persistency_config_string(config_dict)
    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()
    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name, 'TopicArn': topic_arn,
                        'Events': []
                        }]

    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)

    delay = 20
    time.sleep(delay)
    http_server.close()
    # topic get
    parsed_result = get_topic(topic_name)
    parsed_result_dest = parsed_result["dest"]
    for key, value in config_dict.items():
        assert_equal(parsed_result_dest[key], str(value))

    # topic stats
    get_stats_persistent_topic(topic_name, 0)

    # create objects in the bucket (async)
    number_of_objects = 10
    client_threads = []
    start_time = time.time()
    for i in range(number_of_objects):
        key = bucket.new_key('key-'+str(i))
        content = str(os.urandom(1024*1024))
        thr = threading.Thread(target = set_contents_from_string, args=(key, content,))
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]
    time_diff = time.time() - start_time
    print('average time for creation + async http notification is: ' + str(time_diff*1000/number_of_objects) + ' milliseconds')

    # topic stats
    get_stats_persistent_topic(topic_name, number_of_objects)

    # wait as much as ttl and check if the persistent topics have expired
    time.sleep(persistency_time)
    get_stats_persistent_topic(topic_name, 0)

    # delete objects from the bucket
    client_threads = []
    start_time = time.time()
    count = 0
    for key in bucket.list():
        count += 1
        thr = threading.Thread(target = key.delete, args=())
        thr.start()
        client_threads.append(thr)
        if count%100 == 0:
            [thr.join() for thr in client_threads]
            time_diff = time.time() - start_time
            print('average time for deletion + async http notification is: ' + str(time_diff*1000/number_of_objects) + ' milliseconds')
            client_threads = []
            start_time = time.time()

    # topic stats
    get_stats_persistent_topic(topic_name, number_of_objects)

    # wait as much as ttl and check if the persistent topics have expired
    time.sleep(persistency_time)
    get_stats_persistent_topic(topic_name, 0)

    # cleanup
    s3_notification_conf.del_config()
    topic_conf.del_config()
    # delete the bucket
    conn.delete_bucket(bucket_name)
    time.sleep(delay)

def create_persistency_config_string(config_dict):
    str_ret = ""
    for key, value in config_dict.items():
        if key != "None":
            str_ret += key + "=" + str(value) + '&'

    return str_ret[:-1]

@attr('basic_test')
def test_ps_s3_persistent_topic_configs_ttl():
    """ test persistent topic configurations with time_to_live """
    config_dict = {"time_to_live": 30, "max_retries": "None", "retry_sleep_duration": "None"}
    buffer = 10
    persistency_time =config_dict["time_to_live"] + buffer

    ps_s3_persistent_topic_configs(persistency_time, config_dict)

@attr('basic_test')
def test_ps_s3_persistent_topic_configs_max_retries():
    """ test persistent topic configurations with max_retries and retry_sleep_duration """
    config_dict = {"time_to_live": "None", "max_retries": 10, "retry_sleep_duration": 1}
    buffer = 30
    persistency_time = config_dict["max_retries"]*config_dict["retry_sleep_duration"] + buffer

    ps_s3_persistent_topic_configs(persistency_time, config_dict)

@attr('manual_test')
def test_ps_s3_persistent_notification_pushback():
    """ test pushing persistent notification pushback """
    return SkipTest("only used in manual testing")
    conn = connection()
    zonegroup = get_config_zonegroup()

    # create random port for the http server
    host = get_ip()
    port = random.randint(10000, 20000)
    # start an http server in a separate thread
    http_server = HTTPServerWithEvents((host, port), delay=0.5)

    # create bucket
    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX

    # create s3 topic
    endpoint_address = 'http://'+host+':'+str(port)
    endpoint_args = 'push-endpoint='+endpoint_address+'&persistent=true'
    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()
    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name, 'TopicArn': topic_arn,
                         'Events': []
                       }]

    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)

    # create objects in the bucket (async)
    for j in range(100):
        number_of_objects = randint(500, 1000)
        client_threads = []
        start_time = time.time()
        for i in range(number_of_objects):
            key = bucket.new_key(str(j)+'-'+str(i))
            content = str(os.urandom(1024*1024))
            thr = threading.Thread(target = set_contents_from_string, args=(key, content,))
            thr.start()
            client_threads.append(thr)
        [thr.join() for thr in client_threads]
        time_diff = time.time() - start_time
        print('average time for creation + async http notification is: ' + str(time_diff*1000/number_of_objects) + ' milliseconds')

    keys = list(bucket.list())

    delay = 30
    print('wait for '+str(delay)+'sec for the messages...')
    time.sleep(delay)

    # delete objects from the bucket
    client_threads = []
    start_time = time.time()
    count = 0
    for key in bucket.list():
        count += 1
        thr = threading.Thread(target = key.delete, args=())
        thr.start()
        client_threads.append(thr)
        if count%100 == 0:
            [thr.join() for thr in client_threads]
            time_diff = time.time() - start_time
            print('average time for deletion + async http notification is: ' + str(time_diff*1000/number_of_objects) + ' milliseconds')
            client_threads = []
            start_time = time.time()

    print('wait for '+str(delay)+'sec for the messages...')
    time.sleep(delay)

    # cleanup
    s3_notification_conf.del_config()
    topic_conf.del_config()
    # delete the bucket
    conn.delete_bucket(bucket_name)
    time.sleep(delay)
    http_server.close()


@attr('kafka_test')
def test_ps_s3_notification_kafka_idle_behaviour():
    """ test pushing kafka s3 notification idle behaviour check """
    conn = connection()
    zonegroup = get_config_zonegroup()

    # create bucket
    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(bucket_name)
    # name is constant for manual testing
    topic_name = bucket_name+'_topic'
    # create consumer on the topic
   
    task, receiver = create_kafka_receiver_thread(topic_name+'_1')
    task.start()

    # create s3 topic
    endpoint_address = 'kafka://' + kafka_server
    # with acks from broker
    endpoint_args = 'push-endpoint='+endpoint_address+'&kafka-ack-level=broker'
    topic_conf1 = PSTopicS3(conn, topic_name+'_1', zonegroup, endpoint_args=endpoint_args)
    topic_arn1 = topic_conf1.set_config()
    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name + '_1', 'TopicArn': topic_arn1,
                     'Events': []
                   }]

    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)

    # create objects in the bucket (async)
    number_of_objects = 10
    client_threads = []
    etags = []
    start_time = time.time()
    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        content = str(os.urandom(1024*1024))
        etag = hashlib.md5(content.encode()).hexdigest()
        etags.append(etag)
        thr = threading.Thread(target = set_contents_from_string, args=(key, content,))
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]

    time_diff = time.time() - start_time
    print('average time for creation + kafka notification is: ' + str(time_diff*1000/number_of_objects) + ' milliseconds')

    print('wait for 5sec for the messages...')
    time.sleep(5)
    keys = list(bucket.list())
    receiver.verify_s3_events(keys, exact_match=True, etags=etags)

    # delete objects from the bucket
    client_threads = []
    start_time = time.time()
    for key in bucket.list():
        thr = threading.Thread(target = key.delete, args=())
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]

    time_diff = time.time() - start_time
    print('average time for deletion + kafka notification is: ' + str(time_diff*1000/number_of_objects) + ' milliseconds')

    print('wait for 5sec for the messages...')
    time.sleep(5)
    receiver.verify_s3_events(keys, exact_match=True, deletions=True, etags=etags)

    is_idle = False

    while not is_idle:
        print('waiting for 10sec for checking idleness')
        time.sleep(10)
        cmd = "netstat -nnp | grep 9092 | grep radosgw"
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
        out = proc.communicate()[0]
        if len(out) == 0:
            is_idle = True
        else:
            print("radosgw<->kafka connection is not idle")
            print(out.decode('utf-8'))

    # do the process of uploading an object and checking for notification again
    number_of_objects = 10
    client_threads = []
    etags = []
    start_time = time.time()
    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        content = str(os.urandom(1024*1024))
        etag = hashlib.md5(content.encode()).hexdigest()
        etags.append(etag)
        thr = threading.Thread(target = set_contents_from_string, args=(key, content,))
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]

    time_diff = time.time() - start_time
    print('average time for creation + kafka notification is: ' + str(time_diff*1000/number_of_objects) + ' milliseconds')

    print('wait for 5sec for the messages...')
    time.sleep(5)
    keys = list(bucket.list())
    receiver.verify_s3_events(keys, exact_match=True, etags=etags)

    # delete objects from the bucket
    client_threads = []
    start_time = time.time()
    for key in bucket.list():
        thr = threading.Thread(target = key.delete, args=())
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]

    time_diff = time.time() - start_time
    print('average time for deletion + kafka notification is: ' + str(time_diff*1000/number_of_objects) + ' milliseconds')

    print('wait for 5sec for the messages...')
    time.sleep(5)
    receiver.verify_s3_events(keys, exact_match=True, deletions=True, etags=etags)

    # cleanup
    s3_notification_conf.del_config()
    topic_conf1.del_config()
    # delete the bucket
    conn.delete_bucket(bucket_name)
    stop_kafka_receiver(receiver, task)


@attr('modification_required')
def test_ps_s3_persistent_gateways_recovery():
    """ test gateway recovery of persistent notifications """
    return SkipTest('This test requires two gateways.')

    conn = connection()
    zonegroup = get_config_zonegroup()
    # create random port for the http server
    host = get_ip()
    port = random.randint(10000, 20000)
    # start an http server in a separate thread
    number_of_objects = 10
    http_server = StreamingHTTPServer(host, port, num_workers=number_of_objects)
    gw1 = conn
    gw2 = connection2()
    # create bucket
    bucket_name = gen_bucket_name()
    bucket = gw1.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX
    # create two s3 topics
    endpoint_address = 'http://'+host+':'+str(port)
    endpoint_args = 'push-endpoint='+endpoint_address+'&persistent=true'
    topic_conf1 = PSTopicS3(gw1, topic_name+'_1', zonegroup, endpoint_args=endpoint_args+'&OpaqueData=fromgw1')
    topic_arn1 = topic_conf1.set_config()
    topic_conf2 = PSTopicS3(gw2, topic_name+'_2', zonegroup, endpoint_args=endpoint_args+'&OpaqueData=fromgw2')
    topic_arn2 = topic_conf2.set_config()
    # create two s3 notifications
    notification_name = bucket_name + NOTIFICATION_SUFFIX+'_1'
    topic_conf_list = [{'Id': notification_name, 'TopicArn': topic_arn1,
        'Events': ['s3:ObjectCreated:Put']
        }]
    s3_notification_conf1 = PSNotificationS3(gw1, bucket_name, topic_conf_list)
    response, status = s3_notification_conf1.set_config()
    assert_equal(status/100, 2)
    notification_name = bucket_name + NOTIFICATION_SUFFIX+'_2'
    topic_conf_list = [{'Id': notification_name, 'TopicArn': topic_arn2,
        'Events': ['s3:ObjectRemoved:Delete']
        }]
    s3_notification_conf2 = PSNotificationS3(gw2, bucket_name, topic_conf_list)
    response, status = s3_notification_conf2.set_config()
    assert_equal(status/100, 2)
    # stop gateway 2
    print('stopping gateway2...')
    client_threads = []
    start_time = time.time()
    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        content = str(os.urandom(1024*1024))
        thr = threading.Thread(target = set_contents_from_string, args=(key, content,))
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]
    keys = list(bucket.list())
    # delete objects from the bucket
    client_threads = []
    start_time = time.time()
    for key in bucket.list():
        thr = threading.Thread(target = key.delete, args=())
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]
    print('wait for 60 sec for before restarting the gateway')
    time.sleep(60)
    # check http receiver
    events = http_server.get_and_reset_events()
    for key in keys:
        creations = 0
        deletions = 0
        for event in events:
            if event['Records'][0]['eventName'] == 'ObjectCreated:Put' and \
                    key.name == event['Records'][0]['s3']['object']['key']:
                creations += 1
            elif event['Records'][0]['eventName'] == 'ObjectRemoved:Delete' and \
                    key.name == event['Records'][0]['s3']['object']['key']:
                deletions += 1
        assert_equal(creations, 1)
        assert_equal(deletions, 1)
    # cleanup
    s3_notification_conf1.del_config()
    topic_conf1.del_config()
    gw1.delete_bucket(bucket_name)
    time.sleep(10)
    s3_notification_conf2.del_config()
    topic_conf2.del_config()
    http_server.close()


@attr('modification_required')
def test_ps_s3_persistent_multiple_gateways():
    """ test pushing persistent notification via two gateways """
    return SkipTest('This test requires two gateways.')

    conn = connection()
    zonegroup = get_config_zonegroup()
    # create random port for the http server
    host = get_ip()
    port = random.randint(10000, 20000)
    # start an http server in a separate thread
    number_of_objects = 10
    http_server = StreamingHTTPServer(host, port, num_workers=number_of_objects)
    gw1 = conn
    gw2 = connection2()
    # create bucket
    bucket_name = gen_bucket_name()
    bucket1 = gw1.create_bucket(bucket_name)
    bucket2 = gw2.get_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX
    # create two s3 topics
    endpoint_address = 'http://'+host+':'+str(port)
    endpoint_args = 'push-endpoint='+endpoint_address+'&persistent=true'
    topic1_opaque = 'fromgw1'
    topic_conf1 = PSTopicS3(gw1, topic_name+'_1', zonegroup, endpoint_args=endpoint_args+'&OpaqueData='+topic1_opaque)
    topic_arn1 = topic_conf1.set_config()
    topic2_opaque = 'fromgw2'
    topic_conf2 = PSTopicS3(gw2, topic_name+'_2', zonegroup, endpoint_args=endpoint_args+'&OpaqueData='+topic2_opaque)
    topic_arn2 = topic_conf2.set_config()
    # create two s3 notifications
    notification_name = bucket_name + NOTIFICATION_SUFFIX+'_1'
    topic_conf_list = [{'Id': notification_name, 'TopicArn': topic_arn1,
                         'Events': []
                       }]
    s3_notification_conf1 = PSNotificationS3(gw1, bucket_name, topic_conf_list)
    response, status = s3_notification_conf1.set_config()
    assert_equal(status/100, 2)
    notification_name = bucket_name + NOTIFICATION_SUFFIX+'_2'
    topic_conf_list = [{'Id': notification_name, 'TopicArn': topic_arn2,
                         'Events': []
                       }]
    s3_notification_conf2 = PSNotificationS3(gw2, bucket_name, topic_conf_list)
    response, status = s3_notification_conf2.set_config()
    assert_equal(status/100, 2)
    client_threads = []
    start_time = time.time()
    for i in range(number_of_objects):
        key = bucket1.new_key('gw1_'+str(i))
        content = str(os.urandom(1024*1024))
        thr = threading.Thread(target = set_contents_from_string, args=(key, content,))
        thr.start()
        client_threads.append(thr)
        key = bucket2.new_key('gw2_'+str(i))
        content = str(os.urandom(1024*1024))
        thr = threading.Thread(target = set_contents_from_string, args=(key, content,))
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]
    keys = list(bucket1.list())
    delay = 30
    print('wait for '+str(delay)+'sec for the messages...')
    time.sleep(delay)
    events = http_server.get_and_reset_events()
    for key in keys:
        topic1_count = 0
        topic2_count = 0
        for event in events:
            if event['Records'][0]['eventName'] == 'ObjectCreated:Put' and \
                    key.name == event['Records'][0]['s3']['object']['key'] and \
                    topic1_opaque == event['Records'][0]['opaqueData']:
                topic1_count += 1
            elif event['Records'][0]['eventName'] == 'ObjectCreated:Put' and \
                    key.name == event['Records'][0]['s3']['object']['key'] and \
                    topic2_opaque == event['Records'][0]['opaqueData']:
                topic2_count += 1
        assert_equal(topic1_count, 1)
        assert_equal(topic2_count, 1)
    # delete objects from the bucket
    client_threads = []
    start_time = time.time()
    for key in bucket1.list():
        thr = threading.Thread(target = key.delete, args=())
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]
    print('wait for '+str(delay)+'sec for the messages...')
    time.sleep(delay)
    events = http_server.get_and_reset_events()
    for key in keys:
        topic1_count = 0
        topic2_count = 0
        for event in events:
            if event['Records'][0]['eventName'] == 'ObjectRemoved:Delete' and \
                    key.name == event['Records'][0]['s3']['object']['key'] and \
                    topic1_opaque == event['Records'][0]['opaqueData']:
                topic1_count += 1
            elif event['Records'][0]['eventName'] == 'ObjectRemoved:Delete' and \
                    key.name == event['Records'][0]['s3']['object']['key'] and \
                    topic2_opaque == event['Records'][0]['opaqueData']:
                topic2_count += 1
        assert_equal(topic1_count, 1)
        assert_equal(topic2_count, 1)
    # cleanup
    s3_notification_conf1.del_config()
    topic_conf1.del_config()
    s3_notification_conf2.del_config()
    topic_conf2.del_config()
    gw1.delete_bucket(bucket_name)
    http_server.close()


@attr('http_test')
def test_ps_s3_persistent_multiple_endpoints():
    """ test pushing persistent notification when one of the endpoints has error """
    conn = connection()
    zonegroup = get_config_zonegroup()

    # create random port for the http server
    host = get_ip()
    port = random.randint(10000, 20000)
    # start an http server in a separate thread
    number_of_objects = 10
    http_server = HTTPServerWithEvents((host, port))

    # create bucket
    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX

    # create two s3 topics
    endpoint_address = 'http://'+host+':'+str(port)
    endpoint_args = 'push-endpoint='+endpoint_address+'&persistent=true'+ \
            '&retry_sleep_duration=1'
    topic_conf1 = PSTopicS3(conn, topic_name+'_1', zonegroup, endpoint_args=endpoint_args)
    topic_arn1 = topic_conf1.set_config()
    endpoint_address = 'http://kaboom:9999'
    endpoint_args = 'push-endpoint='+endpoint_address+'&persistent=true'+ \
            '&retry_sleep_duration=1'
    topic_conf2 = PSTopicS3(conn, topic_name+'_2', zonegroup, endpoint_args=endpoint_args)
    topic_arn2 = topic_conf2.set_config()

    # create two s3 notifications
    notification_name = bucket_name + NOTIFICATION_SUFFIX+'_1'
    topic_conf_list = [{'Id': notification_name, 'TopicArn': topic_arn1,
                         'Events': []
                       }]
    s3_notification_conf1 = PSNotificationS3(conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf1.set_config()
    assert_equal(status/100, 2)
    notification_name = bucket_name + NOTIFICATION_SUFFIX+'_2'
    topic_conf_list = [{'Id': notification_name, 'TopicArn': topic_arn2,
                         'Events': []
                       }]
    s3_notification_conf2 = PSNotificationS3(conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf2.set_config()
    assert_equal(status/100, 2)

    client_threads = []
    start_time = time.time()
    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        content = str(os.urandom(1024*1024))
        thr = threading.Thread(target = set_contents_from_string, args=(key, content,))
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]

    keys = list(bucket.list())

    wait_for_queue_to_drain(topic_name+'_1')

    http_server.verify_s3_events(keys, exact_match=True, deletions=False)

    # delete objects from the bucket
    client_threads = []
    start_time = time.time()
    for key in bucket.list():
        thr = threading.Thread(target = key.delete, args=())
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]

    wait_for_queue_to_drain(topic_name+'_1')

    http_server.verify_s3_events(keys, exact_match=True, deletions=True)

    # cleanup
    s3_notification_conf1.del_config()
    topic_conf1.del_config()
    s3_notification_conf2.del_config()
    topic_conf2.del_config()
    conn.delete_bucket(bucket_name)
    http_server.close()

def persistent_notification(endpoint_type, conn, account=None):
    """ test pushing persistent notification """
    zonegroup = get_config_zonegroup()

    # create bucket
    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX

    host = get_ip()
    task = None
    if endpoint_type == 'http':
        # create random port for the http server
        host = get_ip_http()
        port = random.randint(10000, 20000)
        # start an http server in a separate thread
        receiver = HTTPServerWithEvents((host, port))
        endpoint_address = 'http://'+host+':'+str(port)
        endpoint_args = 'push-endpoint='+endpoint_address+'&persistent=true'
    elif endpoint_type == 'amqp':
        # start amqp receiver
        exchange = 'ex1'
        task, receiver = create_amqp_receiver_thread(exchange, topic_name)
        task.start()
        endpoint_address = 'amqp://' + host
        endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange='+exchange+'&amqp-ack-level=broker'+'&persistent=true'
    elif endpoint_type == 'kafka':
        # start kafka receiver
        task, receiver = create_kafka_receiver_thread(topic_name)
        task.start()
        endpoint_address = 'kafka://' + host
        endpoint_args = 'push-endpoint='+endpoint_address+'&kafka-ack-level=broker'+'&persistent=true'
    else:
        return SkipTest('Unknown endpoint type: ' + endpoint_type)


    # create s3 topic
    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()
    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name, 'TopicArn': topic_arn,
                         'Events': []
                       }]

    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)

    # create objects in the bucket (async)
    number_of_objects = 100
    client_threads = []
    start_time = time.time()
    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        content = str(os.urandom(1024*1024))
        thr = threading.Thread(target = set_contents_from_string, args=(key, content,))
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]

    time_diff = time.time() - start_time
    print('average time for creation + async http notification is: ' + str(time_diff*1000/number_of_objects) + ' milliseconds')

    keys = list(bucket.list())

    wait_for_queue_to_drain(topic_name, account=account)

    receiver.verify_s3_events(keys, exact_match=False, deletions=False)

    # delete objects from the bucket
    client_threads = []
    start_time = time.time()
    for key in bucket.list():
        thr = threading.Thread(target = key.delete, args=())
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]

    time_diff = time.time() - start_time
    print('average time for deletion + async http notification is: ' + str(time_diff*1000/number_of_objects) + ' milliseconds')

    wait_for_queue_to_drain(topic_name, account=account)

    receiver.verify_s3_events(keys, exact_match=False, deletions=True)

    # cleanup
    s3_notification_conf.del_config()
    topic_conf.del_config()
    # delete the bucket
    conn.delete_bucket(bucket_name)
    receiver.close(task)


@attr('http_test')
def test_ps_s3_persistent_notification_http():
    """ test pushing persistent notification http """
    conn = connection()
    persistent_notification('http', conn)

@attr('http_test')
def test_ps_s3_persistent_notification_http_account():
    """ test pushing persistent notification via http for account user """

    account = 'RGW77777777777777777'
    user = UID_PREFIX + 'test'

    _, result = admin(['account', 'create', '--account-id', account, '--account-name', 'testacct'], get_config_cluster())
    assert_true(result in [0, 17]) # EEXIST okay if we rerun

    conn, _ = another_user(user=user, account=account)
    try:
        persistent_notification('http', conn, account)
    finally:
        admin(['user', 'rm', '--uid', user], get_config_cluster())
        admin(['account', 'rm', '--account-id', account], get_config_cluster())

@attr('amqp_test')
def test_ps_s3_persistent_notification_amqp():
    """ test pushing persistent notification amqp """
    conn = connection()
    persistent_notification('amqp', conn)


@attr('kafka_test')
def test_ps_s3_persistent_notification_kafka():
    """ test pushing persistent notification kafka """
    conn = connection()
    persistent_notification('kafka', conn)


def random_string(length):
    import string
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(length))


@attr('amqp_test')
def test_ps_s3_persistent_notification_large():
    """ test pushing persistent notification of large notifications """

    conn = connection()
    zonegroup = get_config_zonegroup()

    # create bucket
    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX

    receiver = {}
    host = get_ip()
    # start amqp receiver
    exchange = 'ex1'
    task, receiver = create_amqp_receiver_thread(exchange, topic_name)
    task.start()
    endpoint_address = 'amqp://' + host
    opaque_data = random_string(1024*2)
    endpoint_args = 'push-endpoint='+endpoint_address+'&OpaqueData='+opaque_data+'&amqp-exchange='+exchange+'&amqp-ack-level=broker'+'&persistent=true'
    # amqp broker guarantee ordering
    exact_match = True

    # create s3 topic
    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()
    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name, 'TopicArn': topic_arn,
                         'Events': []
                       }]

    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)

    # create objects in the bucket (async)
    number_of_objects = 100
    client_threads = []
    start_time = time.time()
    for i in range(number_of_objects):
        key_value = random_string(63)
        key = bucket.new_key(key_value)
        content = str(os.urandom(1024*1024))
        thr = threading.Thread(target = set_contents_from_string, args=(key, content,))
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]

    time_diff = time.time() - start_time
    print('average time for creation + async http notification is: ' + str(time_diff*1000/number_of_objects) + ' milliseconds')

    keys = list(bucket.list())

    delay = 40
    print('wait for '+str(delay)+'sec for the messages...')
    time.sleep(delay)

    receiver.verify_s3_events(keys, exact_match=exact_match, deletions=False)

    # delete objects from the bucket
    client_threads = []
    start_time = time.time()
    for key in bucket.list():
        thr = threading.Thread(target = key.delete, args=())
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]

    time_diff = time.time() - start_time
    print('average time for deletion + async http notification is: ' + str(time_diff*1000/number_of_objects) + ' milliseconds')

    print('wait for '+str(delay)+'sec for the messages...')
    time.sleep(delay)

    receiver.verify_s3_events(keys, exact_match=exact_match, deletions=True)

    # cleanup
    s3_notification_conf.del_config()
    topic_conf.del_config()
    # delete the bucket
    conn.delete_bucket(bucket_name)
    stop_amqp_receiver(receiver, task)


@attr('modification_required')
def test_ps_s3_topic_update():
    """ test updating topic associated with a notification"""
    return SkipTest('This test is yet to be modified.')

    conn = connection()
    ps_zone = None
    bucket_name = gen_bucket_name()
    topic_name = bucket_name+TOPIC_SUFFIX
    # create amqp topic
    hostname = get_ip()
    exchange = 'ex1'
    amqp_task, receiver = create_amqp_receiver_thread(exchange, topic_name)
    amqp_task.start()
    #topic_conf = PSTopic(ps_zone.conn, topic_name,endpoint='amqp://' + hostname,endpoint_args='amqp-exchange=' + exchange + '&amqp-ack-level=none')
    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args='amqp-exchange=' + exchange + '&amqp-ack-level=none')
    
    topic_arn = topic_conf.set_config()
    #result, status = topic_conf.set_config()
    #assert_equal(status/100, 2)
    parsed_result = json.loads(result)
    topic_arn = parsed_result['arn']
    # get topic
    result, _ = topic_conf.get_config()
    # verify topic content
    parsed_result = json.loads(result)
    assert_equal(parsed_result['topic']['name'], topic_name)
    assert_equal(parsed_result['topic']['dest']['push_endpoint'], topic_conf.parameters['push-endpoint'])
    # create http server
    port = random.randint(10000, 20000)
    # start an http server in a separate thread
    http_server = StreamingHTTPServer(hostname, port)
    # create bucket on the first of the rados zones
    bucket = conn.create_bucket(bucket_name)
    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name,
                        'TopicArn': topic_arn,
                        'Events': ['s3:ObjectCreated:*']
                       }]
    s3_notification_conf = PSNotificationS3(ps_zone.conn, bucket_name, topic_conf_list)
    _, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)
    # create objects in the bucket
    number_of_objects = 10
    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        key.set_contents_from_string('bar')
    # wait for sync
    #zone_bucket_checkpoint(ps_zone.zone, master_zone.zone, bucket_name)
    keys = list(bucket.list())
    # TODO: use exact match
    receiver.verify_s3_events(keys, exact_match=True)
    # update the same topic with new endpoint
    #topic_conf = PSTopic(ps_zone.conn, topic_name,endpoint='http://'+ hostname + ':' + str(port))
    topic_conf = PSTopicS3(conn, topic_name, endpoint_args='http://'+ hostname + ':' + str(port))
    _, status = topic_conf.set_config()
    assert_equal(status/100, 2)
    # get topic
    result, _ = topic_conf.get_config()
    # verify topic content
    parsed_result = json.loads(result)
    assert_equal(parsed_result['topic']['name'], topic_name)
    assert_equal(parsed_result['topic']['dest']['push_endpoint'], topic_conf.parameters['push-endpoint'])
    # delete current objects and create new objects in the bucket
    for key in bucket.list():
        key.delete()
    for i in range(number_of_objects):
        key = bucket.new_key(str(i+100))
        key.set_contents_from_string('bar')
    # wait for sync
    #zone_meta_checkpoint(ps_zone.zone)
    #zone_bucket_checkpoint(ps_zone.zone, master_zone.zone, bucket_name)
    keys = list(bucket.list())
    # verify that notifications are still sent to amqp
    # TODO: use exact match
    receiver.verify_s3_events(keys, exact_match=False)
    # update notification to update the endpoint from the topic
    topic_conf_list = [{'Id': notification_name,
                        'TopicArn': topic_arn,
                        'Events': ['s3:ObjectCreated:*']
                       }]
    s3_notification_conf = PSNotificationS3(ps_zone.conn, bucket_name, topic_conf_list)
    _, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)
    # delete current objects and create new objects in the bucket
    for key in bucket.list():
        key.delete()
    for i in range(number_of_objects):
        key = bucket.new_key(str(i+200))
        key.set_contents_from_string('bar')
    # wait for sync
    #zone_meta_checkpoint(ps_zone.zone)
    #zone_bucket_checkpoint(ps_zone.zone, master_zone.zone, bucket_name)
    keys = list(bucket.list())
    # check that updates switched to http
    # TODO: use exact match
    http_server.verify_s3_events(keys, exact_match=False)
    # cleanup
    # delete objects from the bucket
    stop_amqp_receiver(receiver, amqp_task)
    for key in bucket.list():
        key.delete()
    s3_notification_conf.del_config()
    topic_conf.del_config()
    conn.delete_bucket(bucket_name)
    http_server.close()


@attr('modification_required')
def test_ps_s3_notification_update():
    """ test updating the topic of a notification"""
    return SkipTest('This test is yet to be modified.')

    hostname = get_ip()
    conn = connection()
    ps_zone = None
    bucket_name = gen_bucket_name()
    topic_name1 = bucket_name+'amqp'+TOPIC_SUFFIX
    topic_name2 = bucket_name+'http'+TOPIC_SUFFIX
    zonegroup = get_config_zonegroup()
    # create topics
    # start amqp receiver in a separate thread
    exchange = 'ex1'
    amqp_task, receiver = create_amqp_receiver_thread(exchange, topic_name1)
    amqp_task.start()
    # create random port for the http server
    http_port = random.randint(10000, 20000)
    # start an http server in a separate thread
    http_server = StreamingHTTPServer(hostname, http_port)
    #topic_conf1 = PSTopic(ps_zone.conn, topic_name1,endpoint='amqp://' + hostname,endpoint_args='amqp-exchange=' + exchange + '&amqp-ack-level=none')
    topic_conf1 = PSTopicS3(conn, topic_name1, zonegroup, endpoint_args='amqp-exchange=' + exchange + '&amqp-ack-level=none')
    result, status = topic_conf1.set_config()
    parsed_result = json.loads(result)
    topic_arn1 = parsed_result['arn']
    assert_equal(status/100, 2)
    #topic_conf2 = PSTopic(ps_zone.conn, topic_name2,endpoint='http://'+hostname+':'+str(http_port))
    topic_conf2 = PSTopicS3(conn, topic_name2, endpoint_args='http://'+hostname+':'+str(http_port))
    result, status = topic_conf2.set_config()
    parsed_result = json.loads(result)
    topic_arn2 = parsed_result['arn']
    assert_equal(status/100, 2)
    # create bucket on the first of the rados zones
    bucket = conn.create_bucket(bucket_name)
    # wait for sync
    #zone_meta_checkpoint(ps_zone.zone)
    # create s3 notification with topic1
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name,
                        'TopicArn': topic_arn1,
                        'Events': ['s3:ObjectCreated:*']
                       }]
    s3_notification_conf = PSNotificationS3(ps_zone.conn, bucket_name, topic_conf_list)
    _, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)
    # create objects in the bucket
    number_of_objects = 10
    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        key.set_contents_from_string('bar')
    # wait for sync
    #zone_bucket_checkpoint(ps_zone.zone, master_zone.zone, bucket_name)
    keys = list(bucket.list())
    # TODO: use exact match
    receiver.verify_s3_events(keys, exact_match=False);
    # update notification to use topic2
    topic_conf_list = [{'Id': notification_name,
                        'TopicArn': topic_arn2,
                        'Events': ['s3:ObjectCreated:*']
                       }]
    s3_notification_conf = PSNotificationS3(ps_zone.conn, bucket_name, topic_conf_list)
    _, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)
    # delete current objects and create new objects in the bucket
    for key in bucket.list():
        key.delete()
    for i in range(number_of_objects):
        key = bucket.new_key(str(i+100))
        key.set_contents_from_string('bar')
    # wait for sync
    #zone_meta_checkpoint(ps_zone.zone)
    #zone_bucket_checkpoint(ps_zone.zone, master_zone.zone, bucket_name)
    keys = list(bucket.list())
    # check that updates switched to http
    # TODO: use exact match
    http_server.verify_s3_events(keys, exact_match=False)
    # cleanup
    # delete objects from the bucket
    stop_amqp_receiver(receiver, amqp_task)
    for key in bucket.list():
        key.delete()
    s3_notification_conf.del_config()
    topic_conf1.del_config()
    topic_conf2.del_config()
    conn.delete_bucket(bucket_name)
    http_server.close()


@attr('modification_required')
def test_ps_s3_multiple_topics_notification():
    """ test notification creation with multiple topics"""
    return SkipTest('This test is yet to be modified.')

    hostname = get_ip()
    zonegroup = get_config_zonegroup()
    conn = connection()
    ps_zone = None
    bucket_name = gen_bucket_name()
    topic_name1 = bucket_name+'amqp'+TOPIC_SUFFIX
    topic_name2 = bucket_name+'http'+TOPIC_SUFFIX
    # create topics
    # start amqp receiver in a separate thread
    exchange = 'ex1'
    amqp_task, receiver = create_amqp_receiver_thread(exchange, topic_name1)
    amqp_task.start()
    # create random port for the http server
    http_port = random.randint(10000, 20000)
    # start an http server in a separate thread
    http_server = StreamingHTTPServer(hostname, http_port)
    #topic_conf1 = PSTopic(ps_zone.conn, topic_name1,endpoint='amqp://' + hostname,endpoint_args='amqp-exchange=' + exchange + '&amqp-ack-level=none')
    topic_conf1 = PSTopicS3(conn, topic_name1, zonegroup, endpoint_args='amqp-exchange=' + exchange + '&amqp-ack-level=none')
    result, status = topic_conf1.set_config()
    parsed_result = json.loads(result)
    topic_arn1 = parsed_result['arn']
    assert_equal(status/100, 2)
    #topic_conf2 = PSTopic(ps_zone.conn, topic_name2,endpoint='http://'+hostname+':'+str(http_port))
    topic_conf2 = PSTopicS3(conn, topic_name2, zonegroup, endpoint_args='http://'+hostname+':'+str(http_port))
    result, status = topic_conf2.set_config()
    parsed_result = json.loads(result)
    topic_arn2 = parsed_result['arn']
    assert_equal(status/100, 2)
    # create bucket on the first of the rados zones
    bucket = conn.create_bucket(bucket_name)
    # wait for sync
    #zone_meta_checkpoint(ps_zone.zone)
    # create s3 notification
    notification_name1 = bucket_name + NOTIFICATION_SUFFIX + '_1'
    notification_name2 = bucket_name + NOTIFICATION_SUFFIX + '_2'
    topic_conf_list = [
        {
            'Id': notification_name1,
            'TopicArn': topic_arn1,
            'Events': ['s3:ObjectCreated:*']
        },
        {
            'Id': notification_name2,
            'TopicArn': topic_arn2,
            'Events': ['s3:ObjectCreated:*']
        }]
    s3_notification_conf = PSNotificationS3(ps_zone.conn, bucket_name, topic_conf_list)
    _, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)
    result, _ = s3_notification_conf.get_config()
    assert_equal(len(result['TopicConfigurations']), 2)
    assert_equal(result['TopicConfigurations'][0]['Id'], notification_name1)
    assert_equal(result['TopicConfigurations'][1]['Id'], notification_name2)
    # get auto-generated subscriptions
    sub_conf1 = PSSubscription(ps_zone.conn, notification_name1,
                               topic_name1)
    _, status = sub_conf1.get_config()
    assert_equal(status/100, 2)
    sub_conf2 = PSSubscription(ps_zone.conn, notification_name2,
                               topic_name2)
    _, status = sub_conf2.get_config()
    assert_equal(status/100, 2)
    # create objects in the bucket
    number_of_objects = 10
    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        key.set_contents_from_string('bar')
    # wait for sync
    #zone_bucket_checkpoint(ps_zone.zone, master_zone.zone, bucket_name)
    # get the events from both of the subscription
    result, _ = sub_conf1.get_events()
    records = json.loads(result)
    for record in records['Records']:
        log.debug(record)
    keys = list(bucket.list())
    # TODO: use exact match
    verify_s3_records_by_elements(records, keys, exact_match=False)
    receiver.verify_s3_events(keys, exact_match=False)  
    result, _ = sub_conf2.get_events()
    parsed_result = json.loads(result)
    for record in parsed_result['Records']:
        log.debug(record)
    keys = list(bucket.list())
    # TODO: use exact match
    verify_s3_records_by_elements(records, keys, exact_match=False)
    http_server.verify_s3_events(keys, exact_match=False)
    # cleanup
    stop_amqp_receiver(receiver, amqp_task)
    s3_notification_conf.del_config()
    topic_conf1.del_config()
    topic_conf2.del_config()
    # delete objects from the bucket
    for key in bucket.list():
        key.delete()
    conn.delete_bucket(bucket_name)
    http_server.close()


@attr('basic_test')
def test_ps_s3_topic_permissions():
    """ test s3 topic set/get/delete permissions """
    conn1 = connection()
    conn2, arn2 = another_user()
    zonegroup = get_config_zonegroup()
    bucket_name = gen_bucket_name()
    topic_name = bucket_name + TOPIC_SUFFIX
    topic_policy = json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "Statement",
                "Effect": "Deny",
                "Principal": {"AWS": arn2},
                "Action": ["sns:Publish", "sns:SetTopicAttributes", "sns:GetTopicAttributes", "sns:DeleteTopic", "sns:CreateTopic"],
                "Resource": f"arn:aws:sns:{zonegroup}::{topic_name}"
            }
        ]
    })
    # create s3 topic with DENY policy
    endpoint_address = 'amqp://127.0.0.1:7001'
    endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=amqp.direct&amqp-ack-level=none'
    topic_conf = PSTopicS3(conn1, topic_name, zonegroup, endpoint_args=endpoint_args, policy_text=topic_policy)
    topic_arn = topic_conf.set_config()

    topic_conf2 = PSTopicS3(conn2, topic_name, zonegroup, endpoint_args=endpoint_args)
    try:
        # 2nd user tries to override the topic
        topic_arn = topic_conf2.set_config()
        assert False, "'AuthorizationError' error is expected"
    except ClientError as err:
        if 'Error' in err.response:
            assert_equal(err.response['Error']['Code'], 'AuthorizationError')
        else:
            assert_equal(err.response['Code'], 'AuthorizationError')
    except Exception as err:
        print('unexpected error type: '+type(err).__name__)

    # 2nd user tries to fetch the topic
    _, status = topic_conf2.get_config(topic_arn=topic_arn)
    assert_equal(status, 403)

    try:
        # 2nd user tries to set the attribute
        status = topic_conf2.set_attributes(attribute_name="persistent", attribute_val="false", topic_arn=topic_arn)
        assert False, "'AuthorizationError' error is expected"
    except ClientError as err:
        if 'Error' in err.response:
            assert_equal(err.response['Error']['Code'], 'AuthorizationError')
        else:
            assert_equal(err.response['Code'], 'AuthorizationError')
    except Exception as err:
        print('unexpected error type: '+type(err).__name__)

    # create bucket for conn2 and try publishing notification to topic
    _ = conn2.create_bucket(bucket_name)
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name, 'TopicArn': topic_arn,
                         'Events': []
                       }]
    try:
        s3_notification_conf2 = PSNotificationS3(conn2, bucket_name, topic_conf_list)
        _, status = s3_notification_conf2.set_config()
        assert False, "'AccessDenied' error is expected"
    except ClientError as err:
        if 'Error' in err.response:
            assert_equal(err.response['Error']['Code'], 'AccessDenied')
        else:
            assert_equal(err.response['Code'], 'AccessDenied')
    except Exception as err:
        print('unexpected error type: '+type(err).__name__)

    try:
        # 2nd user tries to delete the topic
        status = topic_conf2.del_config(topic_arn=topic_arn)
        assert False, "'AuthorizationError' error is expected"
    except ClientError as err:
        if 'Error' in err.response:
            assert_equal(err.response['Error']['Code'], 'AuthorizationError')
        else:
            assert_equal(err.response['Code'], 'AuthorizationError')
    except Exception as err:
        print('unexpected error type: '+type(err).__name__)

    # Topic policy is now added by the 1st user to allow 2nd user.
    topic_policy  = topic_policy.replace("Deny", "Allow")
    topic_conf = PSTopicS3(conn1, topic_name, zonegroup, endpoint_args=endpoint_args, policy_text=topic_policy)
    topic_arn = topic_conf.set_config()
    # 2nd user try to fetch topic again
    _, status = topic_conf2.get_config(topic_arn=topic_arn)
    assert_equal(status, 200)
    # 2nd user tries to set the attribute again
    status = topic_conf2.set_attributes(attribute_name="persistent", attribute_val="false", topic_arn=topic_arn)
    assert_equal(status, 200)
    # 2nd user tries to publish notification again
    s3_notification_conf2 = PSNotificationS3(conn2, bucket_name, topic_conf_list)
    _, status = s3_notification_conf2.set_config()
    assert_equal(status, 200)
    # 2nd user tries to delete the topic again
    status = topic_conf2.del_config(topic_arn=topic_arn)
    assert_equal(status, 200)

    # cleanup
    s3_notification_conf2.del_config()
    # delete the bucket
    conn2.delete_bucket(bucket_name)


@attr('basic_test')
def test_ps_s3_topic_no_permissions():
    """ test s3 topic set/get/delete permissions """
    conn1 = connection()
    conn2, _ = another_user()
    zonegroup = 'default'
    bucket_name = gen_bucket_name()
    topic_name = bucket_name + TOPIC_SUFFIX
    
    # create s3 topic without policy
    endpoint_address = 'amqp://127.0.0.1:7001'
    endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=amqp.direct&amqp-ack-level=none'
    topic_conf = PSTopicS3(conn1, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()

    topic_conf2 = PSTopicS3(conn2, topic_name, zonegroup, endpoint_args=endpoint_args)
    try:
        # 2nd user tries to override the topic
        topic_arn = topic_conf2.set_config()
        assert False, "'AuthorizationError' error is expected"
    except ClientError as err:
        if 'Error' in err.response:
            assert_equal(err.response['Error']['Code'], 'AuthorizationError')
        else:
            assert_equal(err.response['Code'], 'AuthorizationError')
    except Exception as err:
        print('unexpected error type: '+type(err).__name__)

    # 2nd user tries to fetch the topic
    _, status = topic_conf2.get_config(topic_arn=topic_arn)
    assert_equal(status, 403)

    try:
        # 2nd user tries to set the attribute
        status = topic_conf2.set_attributes(attribute_name="persistent", attribute_val="false", topic_arn=topic_arn)
        assert False, "'AuthorizationError' error is expected"
    except ClientError as err:
        if 'Error' in err.response:
            assert_equal(err.response['Error']['Code'], 'AuthorizationError')
        else:
            assert_equal(err.response['Code'], 'AuthorizationError')
    except Exception as err:
        print('unexpected error type: '+type(err).__name__)

    # create bucket for conn2 publish notification to topic
    # should be allowed based on the default value of rgw_topic_require_publish_policy=false
    _ = conn2.create_bucket(bucket_name)
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name, 'TopicArn': topic_arn,
                         'Events': []
                       }]
    s3_notification_conf2 = PSNotificationS3(conn2, bucket_name, topic_conf_list)
    _, status = s3_notification_conf2.set_config()
    assert_equal(status, 200)
    
    try:
        # 2nd user tries to delete the topic
        status = topic_conf2.del_config(topic_arn=topic_arn)
        assert False, "'AuthorizationError' error is expected"
    except ClientError as err:
        if 'Error' in err.response:
            assert_equal(err.response['Error']['Code'], 'AuthorizationError')
        else:
            assert_equal(err.response['Code'], 'AuthorizationError')
    except Exception as err:
        print('unexpected error type: '+type(err).__name__)

    # cleanup
    s3_notification_conf2.del_config()
    topic_conf.del_config()
    # delete the bucket
    conn2.delete_bucket(bucket_name)


def kafka_security(security_type, mechanism='PLAIN', use_topic_attrs_for_creds=False):
    """ test pushing kafka s3 notification securly to master """
    conn = connection()
    zonegroup = get_config_zonegroup()
    # create bucket
    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(bucket_name)
    # name is constant for manual testing
    topic_name = bucket_name+'_topic'
    # create s3 topic
    if security_type == 'SASL_SSL':
        if not use_topic_attrs_for_creds:
            endpoint_address = 'kafka://alice:alice-secret@' + kafka_server + ':9094'
        else:
            endpoint_address = 'kafka://' + kafka_server + ':9094'
    elif security_type == 'SSL':
        endpoint_address = 'kafka://' + kafka_server + ':9093'
    elif security_type == 'SASL_PLAINTEXT':
        endpoint_address = 'kafka://alice:alice-secret@' + kafka_server + ':9095'
    else:
        assert False, 'unknown security method '+security_type

    if security_type == 'SASL_PLAINTEXT':
        endpoint_args = 'push-endpoint='+endpoint_address+'&kafka-ack-level=broker&use-ssl=false&mechanism='+mechanism
    elif security_type == 'SASL_SSL':
        KAFKA_DIR = os.environ['KAFKA_DIR']
        endpoint_args = 'push-endpoint='+endpoint_address+'&kafka-ack-level=broker&use-ssl=true&ca-location='+KAFKA_DIR+'/y-ca.crt&mechanism='+mechanism
        if use_topic_attrs_for_creds:
            endpoint_args += '&user-name=alice&password=alice-secret'
    else:
        KAFKA_DIR = os.environ['KAFKA_DIR']
        endpoint_args = 'push-endpoint='+endpoint_address+'&kafka-ack-level=broker&use-ssl=true&ca-location='+KAFKA_DIR+'/y-ca.crt'

    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    
    # create consumer on the topic
    task, receiver = create_kafka_receiver_thread(topic_name)
    task.start()
    
    topic_arn = topic_conf.set_config()
    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name, 'TopicArn': topic_arn,
                         'Events': []
                       }]
    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    s3_notification_conf.set_config()
    # create objects in the bucket (async)
    number_of_objects = 10
    client_threads = []
    start_time = time.time()
    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        content = str(os.urandom(1024*1024))
        thr = threading.Thread(target = set_contents_from_string, args=(key, content,))
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]
    time_diff = time.time() - start_time
    print('average time for creation + kafka notification is: ' + str(time_diff*1000/number_of_objects) + ' milliseconds')
    try:
        print('wait for 5sec for the messages...')
        time.sleep(5)
        keys = list(bucket.list())
        receiver.verify_s3_events(keys, exact_match=True)
        # delete objects from the bucket
        client_threads = []
        start_time = time.time()
        for key in bucket.list():
            thr = threading.Thread(target = key.delete, args=())
            thr.start()
            client_threads.append(thr)
        [thr.join() for thr in client_threads]
        time_diff = time.time() - start_time
        print('average time for deletion + kafka notification is: ' + str(time_diff*1000/number_of_objects) + ' milliseconds')
        print('wait for 5sec for the messages...')
        time.sleep(5)
        receiver.verify_s3_events(keys, exact_match=True, deletions=True)
    except Exception as err:
        assert False, str(err)
    finally:
        # cleanup
        s3_notification_conf.del_config()
        topic_conf.del_config()
        # delete the bucket
        for key in bucket.list():
            key.delete()
        conn.delete_bucket(bucket_name)
        stop_kafka_receiver(receiver, task)


@attr('kafka_security_test')
def test_ps_s3_notification_push_kafka_security_ssl():
    kafka_security('SSL')


@attr('kafka_security_test')
def test_ps_s3_notification_push_kafka_security_ssl_sasl():
    kafka_security('SASL_SSL')


@attr('kafka_security_test')
def test_ps_s3_notification_push_kafka_security_ssl_sasl_attrs():
    kafka_security('SASL_SSL', use_topic_attrs_for_creds=True)


@attr('kafka_security_test')
def test_ps_s3_notification_push_kafka_security_sasl():
    kafka_security('SASL_PLAINTEXT')


@attr('kafka_security_test')
def test_ps_s3_notification_push_kafka_security_ssl_sasl_scram():
    kafka_security('SASL_SSL', mechanism='SCRAM-SHA-256')


@attr('kafka_security_test')
def test_ps_s3_notification_push_kafka_security_sasl_scram():
    kafka_security('SASL_PLAINTEXT', mechanism='SCRAM-SHA-256')


@attr('http_test')
def test_persistent_ps_s3_reload():
    """ do a realm reload while we send notifications """
    if get_config_cluster() == 'noname':
        return SkipTest('realm is needed for reload test')

    conn = connection()
    zonegroup = get_config_zonegroup()

    # make sure there is nothing to migrate
    print('delete all topics')
    delete_all_topics(conn, '', get_config_cluster())

    # disable v2 notification
    zonegroup_modify_feature(enable=False, feature_name=zonegroup_feature_notification_v2)

    # create random port for the http server
    host = get_ip()
    http_port = random.randint(10000, 20000)
    print('start http server')
    http_server = HTTPServerWithEvents((host, http_port), delay=2)

    # create bucket
    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(bucket_name)
    topic_name1 = bucket_name + TOPIC_SUFFIX + '_1'

    # create s3 topics
    endpoint_address = 'http://'+host+':'+str(http_port)
    endpoint_args = 'push-endpoint='+endpoint_address+'&persistent=true'+ \
            '&retry_sleep_duration=1'
    topic_conf1 = PSTopicS3(conn, topic_name1, zonegroup, endpoint_args=endpoint_args)
    topic_arn1 = topic_conf1.set_config()
    # 2nd topic is unused
    topic_name2 = bucket_name + TOPIC_SUFFIX + '_2'
    topic_conf2 = PSTopicS3(conn, topic_name2, zonegroup, endpoint_args=endpoint_args)
    topic_arn2 = topic_conf2.set_config()

    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name, 'TopicArn': topic_arn1,
                        'Events': []
                        }]

    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)

    # topic stats
    get_stats_persistent_topic(topic_name1, 0)

    # create objects in the bucket (async)
    number_of_objects = 10
    client_threads = []
    start_time = time.time()
    for i in range(number_of_objects):
        key = bucket.new_key('key-'+str(i))
        content = str(os.urandom(1024*1024))
        thr = threading.Thread(target = set_contents_from_string, args=(key, content,))
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]
    time_diff = time.time() - start_time
    print('average time for creation + async http notification is: ' + str(time_diff*1000/number_of_objects) + ' milliseconds')

    wait_for_queue_to_drain(topic_name1, http_port=http_port)

    client_threads = []
    start_time = time.time()
    for i in range(number_of_objects):
        key = bucket.new_key('another-key-'+str(i))
        content = str(os.urandom(1024*1024))
        thr = threading.Thread(target = set_contents_from_string, args=(key, content,))
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]
    time_diff = time.time() - start_time
    print('average time for creation + async http notification is: ' + str(time_diff*1000/number_of_objects) + ' milliseconds')

    # do a reload
    print('do reload')
    zonegroup_modify_feature(enable=True, feature_name=zonegroup_feature_notification_v2)

    wait_for_queue_to_drain(topic_name1, http_port=http_port)
    # verify events
    keys = list(bucket.list())
    http_server.verify_s3_events(keys, exact_match=False)

    # cleanup
    s3_notification_conf.del_config()
    topic_conf1.del_config()
    topic_conf2.del_config()
    # delete objects from the bucket
    client_threads = []
    for key in bucket.list():
        thr = threading.Thread(target = key.delete, args=())
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]
    # delete the bucket
    conn.delete_bucket(bucket_name)
    http_server.close()


@attr('data_path_v2_test')
def test_persistent_ps_s3_data_path_v2_migration():
    """ test data path v2 persistent migration """
    if get_config_cluster() == 'noname':
        return SkipTest('realm is needed for migration test')
    conn = connection()
    zonegroup = get_config_zonegroup()

    # create random port for the http server
    host = get_ip()
    http_port = random.randint(10000, 20000)

    # disable v2 notification
    zonegroup_modify_feature(enable=False, feature_name=zonegroup_feature_notification_v2)

    # create bucket
    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX

    # create s3 topic
    endpoint_address = 'http://'+host+':'+str(http_port)
    endpoint_args = 'push-endpoint='+endpoint_address+'&persistent=true'+ \
            '&retry_sleep_duration=1'
    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()
    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name, 'TopicArn': topic_arn,
                        'Events': []
                        }]

    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)

    # topic stats
    get_stats_persistent_topic(topic_name, 0)

    # create objects in the bucket (async)
    number_of_objects = 10
    client_threads = []
    start_time = time.time()
    for i in range(number_of_objects):
        key = bucket.new_key('key-'+str(i))
        content = str(os.urandom(1024*1024))
        thr = threading.Thread(target = set_contents_from_string, args=(key, content,))
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]
    time_diff = time.time() - start_time
    print('average time for creation + async http notification is: ' + str(time_diff*1000/number_of_objects) + ' milliseconds')

    http_server = None
    try:
        # topic stats
        get_stats_persistent_topic(topic_name, number_of_objects)

        # create topic to poll on
        topic_name_1 = topic_name + '_1'
        topic_conf_1 = PSTopicS3(conn, topic_name_1, zonegroup, endpoint_args=endpoint_args)

        # enable v2 notification
        zonegroup_modify_feature(enable=True, feature_name=zonegroup_feature_notification_v2)

        # poll on topic_1
        result = 1
        while result != 0:
            time.sleep(1)
            result = remove_topic(topic_name_1, allow_failure=True)

        # topic stats
        get_stats_persistent_topic(topic_name, number_of_objects)

        # create more objects in the bucket (async)
        client_threads = []
        start_time = time.time()
        for i in range(number_of_objects):
            key = bucket.new_key('key-'+str(i))
            content = str(os.urandom(1024*1024))
            thr = threading.Thread(target = set_contents_from_string, args=(key, content,))
            thr.start()
            client_threads.append(thr)
        [thr.join() for thr in client_threads]
        time_diff = time.time() - start_time
        print('average time for creation + async http notification is: ' + str(time_diff*1000/number_of_objects) + ' milliseconds')

        # topic stats
        get_stats_persistent_topic(topic_name, 2 * number_of_objects)

        # start an http server in a separate thread
        http_server = HTTPServerWithEvents((host, http_port))

        wait_for_queue_to_drain(topic_name, http_port=http_port)
        # verify events
        keys = list(bucket.list())
        # exact match is false because the notifications are persistent.
        http_server.verify_s3_events(keys, exact_match=False)

    except Exception as e:
        assert False, str(e)
    finally:
        # cleanup
        s3_notification_conf.del_config()
        topic_conf.del_config()
        # delete objects from the bucket
        client_threads = []
        for key in bucket.list():
            thr = threading.Thread(target = key.delete, args=())
            thr.start()
            client_threads.append(thr)
        [thr.join() for thr in client_threads]
        # delete the bucket
        conn.delete_bucket(bucket_name)
        if http_server:
            http_server.close()


@attr('data_path_v2_test')
def test_ps_s3_data_path_v2_migration():
    """ test data path v2 migration """
    if get_config_cluster() == 'noname':
        return SkipTest('realm is needed for migration test')
    conn = connection()
    zonegroup = get_config_zonegroup()

    # create random port for the http server
    host = get_ip()
    http_port = random.randint(10000, 20000)

    # start an http server in a separate thread
    http_server = HTTPServerWithEvents((host, http_port))

    # disable v2 notification
    zonegroup_modify_feature(enable=False, feature_name=zonegroup_feature_notification_v2)

    # create bucket
    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX

    # create s3 topic
    endpoint_address = 'http://'+host+':'+str(http_port)
    endpoint_args = 'push-endpoint='+endpoint_address
    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()
    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name, 'TopicArn': topic_arn,
                        'Events': []
                        }]

    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)

    # create objects in the bucket (async)
    number_of_objects = 10
    client_threads = []
    start_time = time.time()
    for i in range(number_of_objects):
        key = bucket.new_key('key-'+str(i))
        content = str(os.urandom(1024*1024))
        thr = threading.Thread(target = set_contents_from_string, args=(key, content,))
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]
    time_diff = time.time() - start_time
    print('average time for creation + http notification is: ' + str(time_diff*1000/number_of_objects) + ' milliseconds')

    try:
        # verify events
        keys = list(bucket.list())
        http_server.verify_s3_events(keys, exact_match=True)

        # create topic to poll on
        topic_name_1 = topic_name + '_1'
        topic_conf_1 = PSTopicS3(conn, topic_name_1, zonegroup, endpoint_args=endpoint_args)

        # enable v2 notification
        zonegroup_modify_feature(enable=True, feature_name=zonegroup_feature_notification_v2)

        # poll on topic_1
        result = 1
        while result != 0:
            time.sleep(1)
            result = remove_topic(topic_name_1, allow_failure=True)

        # create more objects in the bucket (async)
        client_threads = []
        start_time = time.time()
        for i in range(number_of_objects):
            key = bucket.new_key('key-'+str(i))
            content = str(os.urandom(1024*1024))
            thr = threading.Thread(target = set_contents_from_string, args=(key, content,))
            thr.start()
            client_threads.append(thr)
        [thr.join() for thr in client_threads]
        time_diff = time.time() - start_time
        print('average time for creation + http notification is: ' + str(time_diff*1000/number_of_objects) + ' milliseconds')

        # verify events
        keys = list(bucket.list())
        http_server.verify_s3_events(keys, exact_match=True)

    except Exception as e:
        assert False, str(e)
    finally:
        # cleanup
        s3_notification_conf.del_config()
        topic_conf.del_config()
        # delete objects from the bucket
        client_threads = []
        for key in bucket.list():
            thr = threading.Thread(target = key.delete, args=())
            thr.start()
            client_threads.append(thr)
        [thr.join() for thr in client_threads]
        # delete the bucket
        conn.delete_bucket(bucket_name)
        http_server.close()


@attr('data_path_v2_test')
def test_ps_s3_data_path_v2_large_migration():
    """ test data path v2 large migration """
    if get_config_cluster() == 'noname':
        return SkipTest('realm is needed for migration test')
    conn = connection()
    connections_list = []
    connections_list.append(conn)
    zonegroup = get_config_zonegroup()
    tenants_list = []
    tenants_list.append('')
    # make sure there are no leftover topics
    delete_all_topics(conn, '', get_config_cluster())
    for i in ['1', '2']:
        tenant_id = 'kaboom_' + i
        tenants_list.append(tenant_id)
        conn = connect_random_user(tenant_id)
        connections_list.append(conn)
        # make sure there are no leftover topics
        delete_all_topics(conn, tenant_id, get_config_cluster())

    # disable v2 notification
    zonegroup_modify_feature(enable=False, feature_name=zonegroup_feature_notification_v2)

    # create random port for the http server
    host = get_ip()
    http_port = random.randint(10000, 20000)

    # create s3 topic
    buckets_list = []
    topics_conf_list = []
    s3_notification_conf_list = []
    num_of_s3_notifications = 110
    for conn in connections_list:
        # create bucket
        bucket_name = gen_bucket_name()
        bucket = conn.create_bucket(bucket_name)
        buckets_list.append(bucket)
        topic_name = bucket_name + TOPIC_SUFFIX
        # create s3 topic
        endpoint_address = 'http://' + host + ':' + str(http_port)
        endpoint_args = 'push-endpoint=' + endpoint_address + '&persistent=true'
        topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
        topics_conf_list.append(topic_conf)
        topic_arn = topic_conf.set_config()
        # create s3 110 notifications
        s3_notification_list = []
        for i in range(num_of_s3_notifications):
            notification_name = bucket_name + NOTIFICATION_SUFFIX + '_' + str(i + 1)
            s3_notification_list.append({'Id': notification_name, 'TopicArn': topic_arn,
                                    'Events': []
                                    })

        s3_notification_conf = PSNotificationS3(conn, bucket_name, s3_notification_list)
        s3_notification_conf_list.append(s3_notification_conf)
        response, status = s3_notification_conf.set_config()
        assert_equal(status / 100, 2)

    # create topic to poll on
    polling_topics_conf = []
    for conn, bucket in zip(connections_list, buckets_list):
        topic_name = bucket.name + TOPIC_SUFFIX + '_1'
        topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
        polling_topics_conf.append(topic_conf)

    # enable v2 notification
    zonegroup_modify_feature(enable=True, feature_name=zonegroup_feature_notification_v2)

    # poll on topic_1
    for tenant, topic_conf in zip(tenants_list, polling_topics_conf):
        while True:
            result = remove_topic(topic_conf.topic_name, tenant, allow_failure=True)

            if result != 0:
                print('migration in process... error: '+str(result))
            else:
                break

            time.sleep(1)

    # check if we migrated all the topics
    for tenant in tenants_list:
        list_topics(1, tenant)

    # check if we migrated all the notifications
    for tenant, bucket in zip(tenants_list, buckets_list):
        list_notifications(bucket.name, num_of_s3_notifications)

    # cleanup
    for s3_notification_conf in s3_notification_conf_list:
        s3_notification_conf.del_config()
    for topic_conf in topics_conf_list:
        topic_conf.del_config()
    # delete the bucket
    for conn, bucket in zip(connections_list, buckets_list):
        conn.delete_bucket(bucket.name)


@attr('data_path_v2_test')
def test_ps_s3_data_path_v2_mixed_migration():
    """ test data path v2 mixed migration """
    if get_config_cluster() == 'noname':
        return SkipTest('realm is needed for migration test')
    conn = connection()
    connections_list = []
    connections_list.append(conn)
    zonegroup = get_config_zonegroup()
    tenants_list = []
    tenants_list.append('')
    # make sure there are no leftover topics
    delete_all_topics(conn, '', get_config_cluster())
    
    # make sure that we start at v2
    zonegroup_modify_feature(enable=True, feature_name=zonegroup_feature_notification_v2)

    for i in ['1', '2']:
        tenant_id = 'kaboom_' + i
        tenants_list.append(tenant_id)
        conn = connect_random_user(tenant_id)
        connections_list.append(conn)
        # make sure there are no leftover topics
        delete_all_topics(conn, tenant_id, get_config_cluster())

    # create random port for the http server
    host = get_ip()
    http_port = random.randint(10000, 20000)

    # create s3 topic
    buckets_list = []
    topics_conf_list = []
    s3_notification_conf_list = []
    topic_arn_list = []
    created_version = '_created_v2'
    for conn in connections_list:
        # create bucket
        bucket_name = gen_bucket_name()
        bucket = conn.create_bucket(bucket_name)
        buckets_list.append(bucket)
        topic_name = bucket_name + TOPIC_SUFFIX + created_version
        # create s3 topic
        endpoint_address = 'http://' + host + ':' + str(http_port)
        endpoint_args = 'push-endpoint=' + endpoint_address + '&persistent=true'
        topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
        topics_conf_list.append(topic_conf)
        topic_arn = topic_conf.set_config()
        topic_arn_list.append(topic_arn)
        # create s3 notification
        notification_name = bucket_name + NOTIFICATION_SUFFIX + created_version
        s3_notification_list = [{'Id': notification_name, 'TopicArn': topic_arn,
                                 'Events': []
                                 }]

        s3_notification_conf = PSNotificationS3(conn, bucket_name, s3_notification_list)
        s3_notification_conf_list.append(s3_notification_conf)
        response, status = s3_notification_conf.set_config()
        assert_equal(status / 100, 2)

    # disable v2 notification
    zonegroup_modify_feature(enable=False, feature_name=zonegroup_feature_notification_v2)

    # create s3 topic
    created_version = '_created_v1'
    for conn, bucket in zip(connections_list, buckets_list):
        # create bucket
        bucket_name = bucket.name
        topic_name = bucket_name + TOPIC_SUFFIX + created_version
        # create s3 topic
        endpoint_address = 'http://' + host + ':' + str(http_port)
        endpoint_args = 'push-endpoint=' + endpoint_address + '&persistent=true'
        topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
        topics_conf_list.append(topic_conf)
        topic_arn = topic_conf.set_config()
        # create s3 notification
        notification_name = bucket_name + NOTIFICATION_SUFFIX + created_version
        s3_notification_list = [{'Id': notification_name, 'TopicArn': topic_arn,
                                 'Events': []
                                 }]

        s3_notification_conf = PSNotificationS3(conn, bucket_name, s3_notification_list)
        s3_notification_conf_list.append(s3_notification_conf)
        response, status = s3_notification_conf.set_config()
        assert_equal(status / 100, 2)

    # create topic to poll on
    polling_topics_conf = []
    for conn, bucket in zip(connections_list, buckets_list):
        topic_name = bucket.name + TOPIC_SUFFIX + '_1'
        topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
        polling_topics_conf.append(topic_conf)

    # enable v2 notification
    zonegroup_modify_feature(enable=True, feature_name=zonegroup_feature_notification_v2)

    # poll on topic_1
    for tenant, topic_conf in zip(tenants_list, polling_topics_conf):
        while True:
            result = remove_topic(topic_conf.topic_name, tenant, allow_failure=True)

            if result != 0:
                print(result)
            else:
                break

            time.sleep(1)

    # check if we migrated all the topics
    for tenant in tenants_list:
        list_topics(2, tenant)

    # check if we migrated all the notifications
    for tenant, bucket in zip(tenants_list, buckets_list):
        list_notifications(bucket.name, 2)

    # cleanup
    for s3_notification_conf in s3_notification_conf_list:
        s3_notification_conf.del_config()
    for topic_conf in topics_conf_list:
        topic_conf.del_config()
    # delete the bucket
    for conn, bucket in zip(connections_list, buckets_list):
        conn.delete_bucket(bucket.name)

