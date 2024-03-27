import logging
import json
import tempfile
import random
import threading
import subprocess
import socket
import time
import os
import string
import boto
from botocore.exceptions import ClientError
from http import server as http_server
from random import randint
import hashlib
from nose.plugins.attrib import attr
import boto3
import datetime
from cloudevents.http import from_http
from dateutil import parser

from boto.s3.connection import S3Connection

from . import(
    get_config_host,
    get_config_port,
    get_access_key,
    get_secret_key
    )

from .api import PSTopicS3, \
    PSNotificationS3, \
    delete_all_objects, \
    put_object_tagging, \
    admin

from nose import SkipTest
from nose.tools import assert_not_equal, assert_equal, assert_in
import boto.s3.tagging

# configure logging for the tests module
log = logging.getLogger(__name__)

TOPIC_SUFFIX = "_topic"
NOTIFICATION_SUFFIX = "_notif"


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


class HTTPPostHandler(http_server.BaseHTTPRequestHandler):
    """HTTP POST hanler class storing the received events in its http server"""
    def do_POST(self):
        """implementation of POST handler"""
        content_length = int(self.headers['Content-Length'])
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
        log.info('HTTP Server (%d) received event: %s', self.server.worker_id, str(body))
        self.server.append(json.loads(body))
        if self.headers.get('Expect') == '100-continue':
            self.send_response(100)
        else:
            self.send_response(200)
        if self.server.delay > 0:
            time.sleep(self.server.delay)
        self.end_headers()


class HTTPServerWithEvents(http_server.HTTPServer):
    """HTTP server used by the handler to store events"""
    def __init__(self, addr, handler, worker_id, delay=0, cloudevents=False):
        http_server.HTTPServer.__init__(self, addr, handler, False)
        self.worker_id = worker_id
        self.events = []
        self.delay = delay
        self.cloudevents = cloudevents

    def append(self, event):
        self.events.append(event)

class HTTPServerThread(threading.Thread):
    """thread for running the HTTP server. reusing the same socket for all threads"""
    def __init__(self, i, sock, addr, delay=0, cloudevents=False):
        threading.Thread.__init__(self)
        self.i = i
        self.daemon = True
        self.httpd = HTTPServerWithEvents(addr, HTTPPostHandler, i, delay, cloudevents)
        self.httpd.socket = sock
        # prevent the HTTP server from re-binding every handler
        self.httpd.server_bind = self.server_close = lambda self: None
        self.start()

    def run(self):
        try:
            log.info('HTTP Server (%d) started on: %s', self.i, self.httpd.server_address)
            self.httpd.serve_forever()
            log.info('HTTP Server (%d) ended', self.i)
        except Exception as error:
            # could happen if the server r/w to a closing socket during shutdown
            log.info('HTTP Server (%d) ended unexpectedly: %s', self.i, str(error))

    def close(self):
        self.httpd.shutdown()

    def get_events(self):
        return self.httpd.events

    def reset_events(self):
        self.httpd.events = []

class StreamingHTTPServer:
    """multi-threaded http server class also holding list of events received into the handler
    each thread has its own server, and all servers share the same socket"""
    def __init__(self, host, port, num_workers=100, delay=0, cloudevents=False):
        addr = (host, port)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(addr)
        self.sock.listen(num_workers)
        self.workers = [HTTPServerThread(i, self.sock, addr, delay, cloudevents) for i in range(num_workers)]

    def verify_s3_events(self, keys, exact_match=False, deletions=False, expected_sizes={}):
        """verify stored s3 records agains a list of keys"""
        events = []
        for worker in self.workers:
            events += worker.get_events()
            worker.reset_events()
        verify_s3_records_by_elements(events, keys, exact_match=exact_match, deletions=deletions, expected_sizes=expected_sizes)

    def verify_events(self, keys, exact_match=False, deletions=False):
        """verify stored events agains a list of keys"""
        events = []
        for worker in self.workers:
            events += worker.get_events()
            worker.reset_events()
        verify_events_by_elements(events, keys, exact_match=exact_match, deletions=deletions)

    def get_and_reset_events(self):
        events = []
        for worker in self.workers:
            events += worker.get_events()
            worker.reset_events()
        return events

    def close(self):
        """close all workers in the http server and wait for it to finish"""
        # make sure that the shared socket is closed
        # this is needed in case that one of the threads is blocked on the socket
        self.sock.shutdown(socket.SHUT_RDWR)
        self.sock.close()
        # wait for server threads to finish
        for worker in self.workers:
            worker.close()
            worker.join()

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
    def verify_s3_events(self, keys, exact_match=False, deletions=False, expected_sizes={}):
        """verify stored s3 records agains a list of keys"""
        verify_s3_records_by_elements(self.events, keys, exact_match=exact_match, deletions=deletions, expected_sizes=expected_sizes)
        self.events = []

    def verify_events(self, keys, exact_match=False, deletions=False):
        """verify stored events agains a list of keys"""
        verify_events_by_elements(self.events, keys, exact_match=exact_match, deletions=deletions)
        self.events = []

    def get_and_reset_events(self):
        tmp = self.events
        self.events = []
        return tmp


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
        log.info('failed to gracefuly stop AMQP receiver: %s', str(error))
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


def verify_events_by_elements(events, keys, exact_match=False, deletions=False):
    """ verify there is at least one event per element """
    err = ''
    for key in keys:
        key_found = False
        if type(events) is list:
            for event_list in events:
                if key_found:
                    break
                for event in event_list['events']:
                    if event['info']['bucket']['name'] == key.bucket.name and \
                        event['info']['key']['name'] == key.name:
                        if deletions and event['event'] == 'OBJECT_DELETE':
                            key_found = True
                            break
                        elif not deletions and event['event'] == 'OBJECT_CREATE':
                            key_found = True
                            break
        else:
            for event in events['events']:
                if event['info']['bucket']['name'] == key.bucket.name and \
                    event['info']['key']['name'] == key.name:
                    if deletions and event['event'] == 'OBJECT_DELETE':
                        key_found = True
                        break
                    elif not deletions and event['event'] == 'OBJECT_CREATE':
                        key_found = True
                        break

        if not key_found:
            err = 'no ' + ('deletion' if deletions else 'creation') + ' event found for key: ' + str(key)
            log.error(events)
            assert False, err

    if not len(events) == len(keys):
        err = 'superfluous events are found'
        log.debug(err)
        if exact_match:
            log.error(events)
            assert False, err

META_PREFIX = 'x-amz-meta-'

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
                        consumer_timeout_ms=16000)
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

    def verify_s3_events(self, keys, exact_match=False, deletions=False, etags=[]):
        """verify stored s3 records agains a list of keys"""
        verify_s3_records_by_elements(self.events, keys, exact_match=exact_match, deletions=deletions, etags=etags)
        self.events = []

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
    """stop the receiver thread and wait for it to finis"""
    receiver.stop = True
    task.join(1)
    try:
        receiver.consumer.unsubscribe()
        receiver.consumer.close()
    except Exception as error:
        log.info('failed to gracefuly stop Kafka receiver: %s', str(error))


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


def another_user(tenant=None):
    access_key = str(time.time())
    secret_key = str(time.time())
    uid = 'superman' + str(time.time())
    if tenant:
        _, result = admin(['user', 'create', '--uid', uid, '--tenant', tenant, '--access-key', access_key, '--secret-key', secret_key, '--display-name', '"Super Man"'])  
    else:
        _, result = admin(['user', 'create', '--uid', uid, '--access-key', access_key, '--secret-key', secret_key, '--display-name', '"Super Man"'])  

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
    conn = another_user(tenant)
    zonegroup = 'default' 
    bucket_name = gen_bucket_name()
    topic_name = bucket_name + TOPIC_SUFFIX

    # create s3 topics
    endpoint_address = 'amqp://127.0.0.1:7001/vhost_1'
    endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=amqp.direct&amqp-ack-level=none'
    topic_conf1 = PSTopicS3(conn, topic_name+'_1', zonegroup, endpoint_args=endpoint_args)
    # clean all topics
    try:
        result = topic_conf1.get_list()[0]['ListTopicsResponse']['ListTopicsResult']['Topics']
        topics = []
        if result is not None:
            topics = result['member']
        for topic in topics:
            topic_conf1.del_config(topic_arn=topic['TopicArn'])
    except Exception as err:
        print('failed to do topic cleanup: ' + str(err))

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
    conn = another_user(tenant)
    zonegroup = 'default' 
    bucket_name = gen_bucket_name()
    topic_name = bucket_name + TOPIC_SUFFIX

    # create s3 topics
    endpoint_address = 'amqp://127.0.0.1:7001/vhost_1'
    endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=amqp.direct&amqp-ack-level=none'
    topic_conf1 = PSTopicS3(conn, topic_name+'_1', zonegroup, endpoint_args=endpoint_args)
    # clean all topics
    try:
        result = topic_conf1.get_list()[0]['ListTopicsResponse']['ListTopicsResult']['Topics']
        topics = []
        if result is not None:
            topics = result['member']
        for topic in topics:
            topic_conf1.del_config(topic_arn=topic['TopicArn'])
    except Exception as err:
        print('failed to do topic cleanup: ' + str(err))

    topic_arn1 = topic_conf1.set_config()
    assert_equal(topic_arn1,
                 'arn:aws:sns:' + zonegroup + ':' + tenant + ':' + topic_name + '_1')

    endpoint_address = 'http://127.0.0.1:9001'
    endpoint_args = 'push-endpoint='+endpoint_address
    topic_conf2 = PSTopicS3(conn, topic_name+'_2', zonegroup, endpoint_args=endpoint_args)
    topic_arn2 = topic_conf2.set_config()
    assert_equal(topic_arn2,
                 'arn:aws:sns:' + zonegroup + ':' + tenant + ':' + topic_name + '_2')
    endpoint_address = 'http://127.0.0.1:9002'
    endpoint_args = 'push-endpoint='+endpoint_address
    topic_conf3 = PSTopicS3(conn, topic_name+'_3', zonegroup, endpoint_args=endpoint_args)
    topic_arn3 = topic_conf3.set_config()
    assert_equal(topic_arn3,
                 'arn:aws:sns:' + zonegroup + ':' + tenant + ':' + topic_name + '_3')

    # get topic 3 via commandline
    result = admin(['topic', 'get', '--topic', topic_name+'_3', '--tenant', tenant])  
    parsed_result = json.loads(result[0])
    assert_equal(parsed_result['arn'], topic_arn3)

    # delete topic 3
    _, result = admin(['topic', 'rm', '--topic', topic_name+'_3', '--tenant', tenant])  
    assert_equal(result, 0)

    # try to get a deleted topic
    _, result = admin(['topic', 'get', '--topic', topic_name+'_3', '--tenant', tenant])  
    print('"topic not found" error is expected')
    assert_equal(result, 2)

    # get the remaining 2 topics
    result = admin(['topic', 'list', '--tenant', tenant])  
    parsed_result = json.loads(result[0])
    assert_equal(len(parsed_result['topics']), 2)

    # delete topics
    _, result = admin(['topic', 'rm', '--topic', topic_name+'_1', '--tenant', tenant])  
    assert_equal(result, 0)
    _, result = admin(['topic', 'rm', '--topic', topic_name+'_2', '--tenant', tenant])  
    assert_equal(result, 0)

    # get topic list, make sure it is empty
    result = admin(['topic', 'list', '--tenant', tenant])  
    parsed_result = json.loads(result[0])
    assert_equal(len(parsed_result['topics']), 0)


@attr('basic_test')
def test_ps_s3_notification_configuration_admin_on_master():
    """ test s3 notification list/get/delete on master """
    conn = connection()
    zonegroup = 'default'
    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX

    # create s3 topics
    endpoint_address = 'amqp://127.0.0.1:7001/vhost_1'
    endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=amqp.direct&amqp-ack-level=none'
    topic_conf = PSTopicS3(conn, topic_name+'_1', zonegroup, endpoint_args=endpoint_args)
    # clean all topics
    try:
        result = topic_conf.get_list()[0]['ListTopicsResponse']['ListTopicsResult']['Topics']
        topics = []
        if result is not None:
            topics = result['member']
        for topic in topics:
            topic_conf.del_config(topic_arn=topic['TopicArn'])
    except Exception as err:
        print('failed to do topic cleanup: ' + str(err))

    topic_arn = topic_conf.set_config()
    assert_equal(topic_arn,
                 'arn:aws:sns:' + zonegroup + '::' + topic_name + '_1')
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

    # list notification
    result = admin(['notification', 'list', '--bucket', bucket_name])
    parsed_result = json.loads(result[0])
    assert_equal(len(parsed_result['notifications']), 3)
    assert_equal(result[1], 0)

    # get notification 1
    result = admin(['notification', 'get', '--bucket', bucket_name, '--notification-id', notification_name+'_1'])
    parsed_result = json.loads(result[0])
    assert_equal(parsed_result['Id'], notification_name+'_1')
    assert_equal(result[1], 0)

    # remove notification 3
    _, result = admin(['notification', 'rm', '--bucket', bucket_name, '--notification-id', notification_name+'_3'])
    assert_equal(result, 0)

    # list notification
    result = admin(['notification', 'list', '--bucket', bucket_name])
    parsed_result = json.loads(result[0])
    assert_equal(len(parsed_result['notifications']), 2)
    assert_equal(result[1], 0)

    # delete notifications
    _, result = admin(['notification', 'rm', '--bucket', bucket_name])
    assert_equal(result, 0)

    # list notification, make sure it is empty
    result = admin(['notification', 'list', '--bucket', bucket_name])
    parsed_result = json.loads(result[0])
    assert_equal(len(parsed_result['notifications']), 0)
    assert_equal(result[1], 0)


@attr('modification_required')
def test_ps_s3_topic_with_secret_on_master():
    """ test s3 topics with secret set/get/delete on master """
    return SkipTest('secure connection is needed to test topic with secrets')

    conn = connection1()
    if conn.secure_conn is None:
        return SkipTest('secure connection is needed to test topic with secrets')

    zonegroup = 'default' 
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
def test_ps_s3_notification_on_master():
    """ test s3 notification set/get/delete on master """
    conn = connection()
    zonegroup = 'default'
    bucket_name = gen_bucket_name()
    # create bucket
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

    # get notifications on a bucket
    response, status = s3_notification_conf.get_config(notification=notification_name+'_1')
    assert_equal(status/100, 2)
    assert_equal(response['NotificationConfiguration']['TopicConfiguration']['Topic'], topic_arn)

    # delete specific notifications
    _, status = s3_notification_conf.del_config(notification=notification_name+'_1')
    assert_equal(status/100, 2)

    # get the remaining 2 notifications on a bucket
    response, status = s3_notification_conf.get_config()
    assert_equal(status/100, 2)
    assert_equal(len(response['TopicConfigurations']), 2)
    assert_equal(response['TopicConfigurations'][0]['TopicArn'], topic_arn)
    assert_equal(response['TopicConfigurations'][1]['TopicArn'], topic_arn)

    # delete remaining notifications
    _, status = s3_notification_conf.del_config()
    assert_equal(status/100, 2)

    # make sure that the notifications are now deleted
    _, status = s3_notification_conf.get_config()

    # cleanup
    topic_conf.del_config()
    # delete the bucket
    conn.delete_bucket(bucket_name)


@attr('basic_test')
def test_ps_s3_notification_on_master_empty_config():
    """ test s3 notification set/get/delete on master with empty config """
    hostname = get_ip()

    conn = connection()

    zonegroup = 'default'

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

    zonegroup = 'default'

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
    zonegroup = 'default'
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

@attr('basic_test')
def test_ps_s3_notification_permissions():
    """ test s3 notification set/get/delete permissions """
    conn1 = connection()
    conn2 = another_user()
    zonegroup = 'default'
    bucket_name = gen_bucket_name()
    # create bucket
    bucket = conn1.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX
    # create s3 topic
    endpoint_address = 'amqp://127.0.0.1:7001'
    endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=amqp.direct&amqp-ack-level=none'
    topic_conf = PSTopicS3(conn1, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()

    # one user create a notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name,
                        'TopicArn': topic_arn,
                        'Events': []
                       }]
    s3_notification_conf1 = PSNotificationS3(conn1, bucket_name, topic_conf_list)
    _, status = s3_notification_conf1.set_config()
    assert_equal(status, 200)
    # another user try to fetch it
    s3_notification_conf2 = PSNotificationS3(conn2, bucket_name, topic_conf_list)
    try:
        _, _ = s3_notification_conf2.get_config()
        assert False, "'AccessDenied' error is expected"
    except ClientError as error:
        assert_equal(error.response['Error']['Code'], 'AccessDenied')
    # other user try to delete the notification
    _, status = s3_notification_conf2.del_config()
    assert_equal(status, 403)

    # bucket policy is added by the 1st user
    client = boto3.client('s3',
            endpoint_url='http://'+conn1.host+':'+str(conn1.port),
            aws_access_key_id=conn1.aws_access_key_id,
            aws_secret_access_key=conn1.aws_secret_access_key)
    bucket_policy = json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "Statement",
                "Effect": "Allow",
                "Principal": "*",
                "Action": ["s3:GetBucketNotification", "s3:PutBucketNotification"],
                "Resource": f"arn:aws:s3:::{bucket_name}"
            }
        ]
    })
    response = client.put_bucket_policy(Bucket=bucket_name, Policy=bucket_policy)
    assert_equal(int(response['ResponseMetadata']['HTTPStatusCode']/100), 2) 
    result = client.get_bucket_policy(Bucket=bucket_name)
    print(result['Policy'])
    
    # 2nd user try to fetch it again
    _, status = s3_notification_conf2.get_config()
    assert_equal(status, 200)

    # 2nd user try to delete it again
    result, status = s3_notification_conf2.del_config()
    assert_equal(status, 200)

    # 2nd user try to add another notification
    topic_conf_list = [{'Id': notification_name+"2",
                        'TopicArn': topic_arn,
                        'Events': []
                       }]
    s3_notification_conf2 = PSNotificationS3(conn2, bucket_name, topic_conf_list)
    result, status = s3_notification_conf2.set_config()
    assert_equal(status, 200)

    # cleanup
    s3_notification_conf1.del_config()
    s3_notification_conf2.del_config()
    topic_conf.del_config()
    # delete the bucket
    conn1.delete_bucket(bucket_name)

@attr('amqp_test')
def test_ps_s3_notification_push_amqp_on_master():
    """ test pushing amqp s3 notification on master """

    hostname = get_ip()
    conn = connection()
    zonegroup = 'default'

    # create bucket
    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(bucket_name)
    topic_name1 = bucket_name + TOPIC_SUFFIX + '_1'
    topic_name2 = bucket_name + TOPIC_SUFFIX + '_2'

    # start amqp receivers
    exchange = 'ex1'
    task1, receiver1 = create_amqp_receiver_thread(exchange, topic_name1)
    task2, receiver2 = create_amqp_receiver_thread(exchange, topic_name2)
    task1.start()
    task2.start()

    # create two s3 topic
    endpoint_address = 'amqp://' + hostname
    # with acks from broker
    endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=' + exchange +'&amqp-ack-level=broker'
    topic_conf1 = PSTopicS3(conn, topic_name1, zonegroup, endpoint_args=endpoint_args)
    topic_arn1 = topic_conf1.set_config()
    # without acks from broker
    endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=' + exchange +'&amqp-ack-level=routable'
    topic_conf2 = PSTopicS3(conn, topic_name2, zonegroup, endpoint_args=endpoint_args)
    topic_arn2 = topic_conf2.set_config()
    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name+'_1', 'TopicArn': topic_arn1,
                         'Events': []
                       },
                       {'Id': notification_name+'_2', 'TopicArn': topic_arn2,
                         'Events': ['s3:ObjectCreated:*']
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
    print('average time for creation + qmqp notification is: ' + str(time_diff*1000/number_of_objects) + ' milliseconds')

    print('wait for 5sec for the messages...')
    time.sleep(5)

    # check amqp receiver
    keys = list(bucket.list())
    print('total number of objects: ' + str(len(keys)))
    receiver1.verify_s3_events(keys, exact_match=True)
    receiver2.verify_s3_events(keys, exact_match=True)

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
    # check amqp receiver 2 has no deletions
    try:
        receiver1.verify_s3_events(keys, exact_match=False, deletions=True)
    except:
        pass
    else:
        err = 'amqp receiver 2 should have no deletions'
        assert False, err

    # cleanup
    stop_amqp_receiver(receiver1, task1)
    stop_amqp_receiver(receiver2, task2)
    s3_notification_conf.del_config()
    topic_conf1.del_config()
    topic_conf2.del_config()
    # delete the bucket
    conn.delete_bucket(bucket_name)


@attr('manual_test')
def test_ps_s3_notification_push_amqp_idleness_check():
    """ test pushing amqp s3 notification and checking for connection idleness """
    return SkipTest("only used in manual testing")
    hostname = get_ip()
    conn = connection()
    zonegroup = 'default'

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
def test_ps_s3_notification_push_kafka_on_master():
    """ test pushing kafka s3 notification on master """
    conn = connection()
    zonegroup = 'default'

    # create bucket
    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(bucket_name)
    # name is constant for manual testing
    topic_name = bucket_name+'_topic'
    # create consumer on the topic

    try:
        s3_notification_conf = None
        topic_conf1 = None
        topic_conf2 = None
        receiver = None
        task, receiver = create_kafka_receiver_thread(topic_name+'_1')
        task.start()

        # create s3 topic
        endpoint_address = 'kafka://' + kafka_server
        # without acks from broker
        endpoint_args = 'push-endpoint='+endpoint_address+'&kafka-ack-level=broker'
        topic_conf1 = PSTopicS3(conn, topic_name+'_1', zonegroup, endpoint_args=endpoint_args)
        topic_arn1 = topic_conf1.set_config()
        endpoint_args = 'push-endpoint='+endpoint_address+'&kafka-ack-level=none'
        topic_conf2 = PSTopicS3(conn, topic_name+'_2', zonegroup, endpoint_args=endpoint_args)
        topic_arn2 = topic_conf2.set_config()
        # create s3 notification
        notification_name = bucket_name + NOTIFICATION_SUFFIX
        topic_conf_list = [{'Id': notification_name + '_1', 'TopicArn': topic_arn1,
                         'Events': []
                       },
                       {'Id': notification_name + '_2', 'TopicArn': topic_arn2,
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
    except Exception as e:
        print(e)
        assert False
    finally:
        # cleanup
        if s3_notification_conf is not None:
            s3_notification_conf.del_config()
        if topic_conf1 is not None:
            topic_conf1.del_config()
        if topic_conf2 is not None:
            topic_conf2.del_config()
        # delete the bucket
        for key in bucket.list():
            key.delete()
        conn.delete_bucket(bucket_name)
        if receiver is not None:
            stop_kafka_receiver(receiver, task)


@attr('http_test')
def test_ps_s3_notification_multi_delete_on_master():
    """ test deletion of multiple keys on master """
    hostname = get_ip()
    conn = connection()
    zonegroup = 'default'

    # create random port for the http server
    host = get_ip()
    port = random.randint(10000, 20000)
    # start an http server in a separate thread
    number_of_objects = 10
    http_server = StreamingHTTPServer(host, port, num_workers=number_of_objects)

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
def test_ps_s3_notification_push_http_on_master():
    """ test pushing http s3 notification on master """
    hostname = get_ip_http()
    conn = connection()
    zonegroup = 'default'

    # create random port for the http server
    host = get_ip()
    port = random.randint(10000, 20000)
    # start an http server in a separate thread
    number_of_objects = 10
    http_server = StreamingHTTPServer(host, port, num_workers=number_of_objects)

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
                        'Events': []
                       }]
    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)

    # create objects in the bucket
    client_threads = []
    objects_size = {}
    start_time = time.time()
    for i in range(number_of_objects):
        content = str(os.urandom(randint(1, 1024)))
        object_size = len(content)
        key = bucket.new_key(str(i))
        objects_size[key.name] = object_size
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
    http_server.verify_s3_events(keys, exact_match=True, deletions=False, expected_sizes=objects_size)

    # delete objects from the bucket
    client_threads = []
    start_time = time.time()
    for key in bucket.list():
        thr = threading.Thread(target = key.delete, args=())
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]

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
def test_ps_s3_notification_push_cloudevents_on_master():
    """ test pushing cloudevents notification on master """
    hostname = get_ip_http()
    conn = connection()
    zonegroup = 'default'

    # create random port for the http server
    host = get_ip()
    port = random.randint(10000, 20000)
    # start an http server in a separate thread
    number_of_objects = 10
    http_server = StreamingHTTPServer(host, port, num_workers=number_of_objects, cloudevents=True)

    # create bucket
    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX

    # create s3 topic
    endpoint_address = 'http://'+host+':'+str(port)
    endpoint_args = 'push-endpoint='+endpoint_address+'&cloudevents=true'
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

    # create objects in the bucket
    client_threads = []
    objects_size = {}
    start_time = time.time()
    for i in range(number_of_objects):
        content = str(os.urandom(randint(1, 1024)))
        object_size = len(content)
        key = bucket.new_key(str(i))
        objects_size[key.name] = object_size
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
    http_server.verify_s3_events(keys, exact_match=True, deletions=False, expected_sizes=objects_size)

    # delete objects from the bucket
    client_threads = []
    start_time = time.time()
    for key in bucket.list():
        thr = threading.Thread(target = key.delete, args=())
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]

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
def test_ps_s3_opaque_data_on_master():
    """ test that opaque id set in topic, is sent in notification on master """
    hostname = get_ip()
    conn = connection()
    zonegroup = 'default'

    # create random port for the http server
    host = get_ip()
    port = random.randint(10000, 20000)
    # start an http server in a separate thread
    number_of_objects = 10
    http_server = StreamingHTTPServer(host, port, num_workers=number_of_objects)

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

@attr('http_test')
def test_ps_s3_lifecycle_on_master():
    """ test that when object is deleted due to lifecycle policy, notification is sent on master """
    hostname = get_ip()
    conn = connection()
    zonegroup = 'default'

    # create random port for the http server
    host = get_ip()
    port = random.randint(10000, 20000)
    # start an http server in a separate thread
    number_of_objects = 10
    http_server = StreamingHTTPServer(host, port, num_workers=number_of_objects)

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
                        'Events': ['s3:ObjectLifecycle:Expiration:*']
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
        key = bucket.new_key(obj_prefix + str(i))
        thr = threading.Thread(target = set_contents_from_string, args=(key, content,))
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]

    time_diff = time.time() - start_time
    print('average time for creation + http notification is: ' + str(time_diff*1000/number_of_objects) + ' milliseconds')
    
    # create lifecycle policy
    client = boto3.client('s3',
            endpoint_url='http://'+conn.host+':'+str(conn.port),
            aws_access_key_id=conn.aws_access_key_id,
            aws_secret_access_key=conn.aws_secret_access_key)
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    response = client.put_bucket_lifecycle_configuration(Bucket=bucket_name, 
            LifecycleConfiguration={'Rules': [
                {
                    'ID': 'rule1',
                    'Expiration': {'Date': yesterday.isoformat()},
                    'Filter': {'Prefix': obj_prefix},
                    'Status': 'Enabled',
                }
            ]
        }
    )

    # start lifecycle processing
    admin(['lc', 'process'])
    print('wait for 5sec for the messages...')
    time.sleep(5)

    # check http receiver does not have messages
    keys = list(bucket.list())
    print('total number of objects: ' + str(len(keys)))
    event_keys = []
    events = http_server.get_and_reset_events()
    for event in events:
        assert_equal(event['Records'][0]['eventName'], 'ObjectLifecycle:Expiration:Current')
        event_keys.append(event['Records'][0]['s3']['object']['key'])
    for key in keys:
        key_found = False
        for event_key in event_keys:
            if event_key == key:
                key_found = True
                break
        if not key_found:
            err = 'no lifecycle event found for key: ' + str(key)
            log.error(events)
            assert False, err

    # cleanup
    for key in keys:
        key.delete()
    [thr.join() for thr in client_threads]
    topic_conf.del_config()
    s3_notification_conf.del_config(notification=notification_name)
    # delete the bucket
    conn.delete_bucket(bucket_name)
    http_server.close()


def ps_s3_creation_triggers_on_master(external_endpoint_address=None, ca_location=None, verify_ssl='true'):
    """ test object creation s3 notifications in using put/copy/post on master"""
    
    if not external_endpoint_address:
        hostname = 'localhost'
        proc = init_rabbitmq()
        if proc is  None:
            return SkipTest('end2end amqp tests require rabbitmq-server installed')
    else:
        proc = None

    conn = connection()
    hostname = 'localhost'
    zonegroup = 'default'

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
    zonegroup = 'default'
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


@attr('mpu_test')
def test_ps_s3_multipart_on_master_http():
    """ test http multipart object upload on master"""
    conn = connection()
    zonegroup = 'default'

    # create random port for the http server
    host = get_ip()
    port = random.randint(10000, 20000)
    # start an http server in a separate thread
    http_server = StreamingHTTPServer(host, port, num_workers=10)

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
    content = str(os.urandom(20*1024*1024))
    key = bucket.new_key('obj')
    thr = threading.Thread(target = set_contents_from_string, args=(key, content,))
    thr.start()
    client_threads.append(thr)
    [thr.join() for thr in client_threads]

    print('wait for 5sec for the messages...')
    time.sleep(5)

    # check http receiver
    keys = list(bucket.list())
    print('total number of objects: ' + str(len(keys)))
    events = http_server.get_and_reset_events()
    for event in events:
        assert_equal(event['Records'][0]['opaqueData'], opaque_data)
        assert_equal(event['Records'][0]['s3']['object']['eTag'] != '', True)
        print(event['Records'][0]['s3']['object'])

    # cleanup
    for key in keys:
        key.delete()
    [thr.join() for thr in client_threads]
    topic_conf.del_config()
    s3_notification_conf.del_config(notification=notification_name)
    # delete the bucket
    conn.delete_bucket(bucket_name)
    http_server.close()


@attr('amqp_test')
def test_ps_s3_multipart_on_master():
    """ test multipart object upload on master"""

    hostname = get_ip()
    conn = connection()
    zonegroup = 'default'

    # create bucket
    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX

    # start amqp receivers
    exchange = 'ex1'
    task1, receiver1 = create_amqp_receiver_thread(exchange, topic_name+'_1')
    task1.start()
    task2, receiver2 = create_amqp_receiver_thread(exchange, topic_name+'_2')
    task2.start()
    task3, receiver3 = create_amqp_receiver_thread(exchange, topic_name+'_3')
    task3.start()

    # create s3 topics
    endpoint_address = 'amqp://' + hostname
    endpoint_args = 'push-endpoint=' + endpoint_address + '&amqp-exchange=' + exchange + '&amqp-ack-level=broker'
    topic_conf1 = PSTopicS3(conn, topic_name+'_1', zonegroup, endpoint_args=endpoint_args)
    topic_arn1 = topic_conf1.set_config()
    topic_conf2 = PSTopicS3(conn, topic_name+'_2', zonegroup, endpoint_args=endpoint_args)
    topic_arn2 = topic_conf2.set_config()
    topic_conf3 = PSTopicS3(conn, topic_name+'_3', zonegroup, endpoint_args=endpoint_args)
    topic_arn3 = topic_conf3.set_config()

    # create s3 notifications
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name+'_1', 'TopicArn': topic_arn1,
                        'Events': ['s3:ObjectCreated:*']
                       },
                       {'Id': notification_name+'_2', 'TopicArn': topic_arn2,
                        'Events': ['s3:ObjectCreated:Post']
                       },
                       {'Id': notification_name+'_3', 'TopicArn': topic_arn3,
                        'Events': ['s3:ObjectCreated:CompleteMultipartUpload']
                       }]
    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)

    # create objects in the bucket using multi-part upload
    fp = tempfile.NamedTemporaryFile(mode='w+b')
    object_size = 1024
    content = bytearray(os.urandom(object_size))
    fp.write(content)
    fp.flush()
    fp.seek(0)
    uploader = bucket.initiate_multipart_upload('multipart')
    uploader.upload_part_from_file(fp, 1)
    uploader.complete_upload()
    fp.close()

    print('wait for 5sec for the messages...')
    time.sleep(5)

    # check amqp receiver
    events = receiver1.get_and_reset_events()
    assert_equal(len(events), 1)

    events = receiver2.get_and_reset_events()
    assert_equal(len(events), 0)

    events = receiver3.get_and_reset_events()
    assert_equal(len(events), 1)
    assert_equal(events[0]['Records'][0]['eventName'], 'ObjectCreated:CompleteMultipartUpload')
    assert_equal(events[0]['Records'][0]['s3']['configurationId'], notification_name+'_3')
    assert_equal(events[0]['Records'][0]['s3']['object']['size'], object_size)
    assert events[0]['Records'][0]['eventTime'] != '0.000000', 'invalid eventTime'

    # cleanup
    stop_amqp_receiver(receiver1, task1)
    stop_amqp_receiver(receiver2, task2)
    stop_amqp_receiver(receiver3, task3)
    s3_notification_conf.del_config()
    topic_conf1.del_config()
    topic_conf2.del_config()
    topic_conf3.del_config()
    for key in bucket.list():
        key.delete()
    # delete the bucket
    conn.delete_bucket(bucket_name)

@attr('amqp_test')
def test_ps_s3_metadata_filter_on_master():
    """ test s3 notification of metadata on master """

    hostname = get_ip()
    conn = connection()
    zonegroup = 'default'

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

    print('wait for 5sec for the messages...')
    time.sleep(5)
    # check amqp receiver
    events = receiver.get_and_reset_events()
    assert_equal(len(events), len(expected_keys))
    for event in events:
        assert(event['Records'][0]['s3']['object']['key'] in expected_keys)

    # delete objects
    for key in bucket.list():
        key.delete()
    print('wait for 5sec for the messages...')
    time.sleep(5)
    # check amqp receiver
    events = receiver.get_and_reset_events()
    assert_equal(len(events), len(expected_keys))
    for event in events:
        assert(event['Records'][0]['s3']['object']['key'] in expected_keys)

    # cleanup
    stop_amqp_receiver(receiver, task)
    s3_notification_conf.del_config()
    topic_conf.del_config()
    # delete the bucket
    conn.delete_bucket(bucket_name)


@attr('amqp_test')
def test_ps_s3_metadata_on_master():
    """ test s3 notification of metadata on master """

    hostname = get_ip()
    conn = connection()
    zonegroup = 'default'

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
    zonegroup = 'default'

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
    zonegroup = 'default'

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
    zonegroup = 'default'

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
    zonegroup = 'default'

    # create random port for the http server
    host = get_ip()
    port = random.randint(10000, 20000)
    # start an http server in a separate thread
    number_of_objects = 200
    http_server = StreamingHTTPServer(host, port, num_workers=number_of_objects)

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


@attr('manual_test')
def test_ps_s3_persistent_notification_pushback():
    """ test pushing persistent notification pushback """
    return SkipTest("only used in manual testing")
    conn = connection()
    zonegroup = 'default'

    # create random port for the http server
    host = get_ip()
    port = random.randint(10000, 20000)
    # start an http server in a separate thread
    http_server = StreamingHTTPServer(host, port, num_workers=10, delay=0.5)

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
    # TODO convert this test to actual running test by changing
    # os.system call to verify the process idleness
    conn = connection()
    zonegroup = 'default'

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
    zonegroup = 'default'
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
    zonegroup = 'default'
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
    zonegroup = 'default'

    # create random port for the http server
    host = get_ip()
    port = random.randint(10000, 20000)
    # start an http server in a separate thread
    number_of_objects = 10
    http_server = StreamingHTTPServer(host, port, num_workers=number_of_objects)

    # create bucket
    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX

    # create two s3 topics
    endpoint_address = 'http://'+host+':'+str(port)
    endpoint_args = 'push-endpoint='+endpoint_address+'&persistent=true'
    topic_conf1 = PSTopicS3(conn, topic_name+'_1', zonegroup, endpoint_args=endpoint_args)
    topic_arn1 = topic_conf1.set_config()
    endpoint_address = 'http://kaboom:9999'
    endpoint_args = 'push-endpoint='+endpoint_address+'&persistent=true'
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

    delay = 30
    print('wait for '+str(delay)+'sec for the messages...')
    time.sleep(delay)

    http_server.verify_s3_events(keys, exact_match=False, deletions=False)

    # delete objects from the bucket
    client_threads = []
    start_time = time.time()
    for key in bucket.list():
        thr = threading.Thread(target = key.delete, args=())
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]

    print('wait for '+str(delay)+'sec for the messages...')
    time.sleep(delay)

    http_server.verify_s3_events(keys, exact_match=False, deletions=True)

    # cleanup
    s3_notification_conf1.del_config()
    topic_conf1.del_config()
    s3_notification_conf2.del_config()
    topic_conf2.del_config()
    conn.delete_bucket(bucket_name)
    http_server.close()

def persistent_notification(endpoint_type):
    """ test pushing persistent notification """
    conn = connection()
    zonegroup = 'default'

    # create bucket
    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX

    receiver = {}
    host = get_ip()
    if endpoint_type == 'http':
        # create random port for the http server
        host = get_ip_http()
        port = random.randint(10000, 20000)
        # start an http server in a separate thread
        receiver = StreamingHTTPServer(host, port, num_workers=10)
        endpoint_address = 'http://'+host+':'+str(port)
        endpoint_args = 'push-endpoint='+endpoint_address+'&persistent=true'
        # the http server does not guarantee order, so duplicates are expected
        exact_match = False
    elif endpoint_type == 'amqp':
        # start amqp receiver
        exchange = 'ex1'
        task, receiver = create_amqp_receiver_thread(exchange, topic_name)
        task.start()
        endpoint_address = 'amqp://' + host
        endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange='+exchange+'&amqp-ack-level=broker'+'&persistent=true'
        # amqp broker guarantee ordering
        exact_match = True
    elif endpoint_type == 'kafka':
        # start amqp receiver
        task, receiver = create_kafka_receiver_thread(topic_name)
        task.start()
        endpoint_address = 'kafka://' + host
        endpoint_args = 'push-endpoint='+endpoint_address+'&kafka-ack-level=broker'+'&persistent=true'
        # amqp broker guarantee ordering
        exact_match = True
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
    if endpoint_type == 'http':
        receiver.close()
    else:
        stop_amqp_receiver(receiver, task)


@attr('http_test')
def test_ps_s3_persistent_notification_http():
    """ test pushing persistent notification http """
    persistent_notification('http')


@attr('amqp_test')
def test_ps_s3_persistent_notification_amqp():
    """ test pushing persistent notification amqp """
    persistent_notification('amqp')


@attr('kafka_test')
def test_ps_s3_persistent_notification_kafka():
    """ test pushing persistent notification kafka """
    persistent_notification('kafka')


def random_string(length):
    import string
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(length))


@attr('amqp_test')
def test_ps_s3_persistent_notification_large():
    """ test pushing persistent notification of large notifications """

    conn = connection()
    zonegroup = 'default'

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
    receiver.verify_s3_events(keys, exact_match=False)
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
    zonegroup = 'default'
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
    zonegroup = 'default'
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


def kafka_security(security_type):
    """ test pushing kafka s3 notification securly to master """
    conn = connection()
    zonegroup = 'default'
    # create bucket
    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(bucket_name)
    # name is constant for manual testing
    topic_name = bucket_name+'_topic'
    # create s3 topic
    if security_type == 'SSL_SASL':
        endpoint_address = 'kafka://alice:alice-secret@' + kafka_server + ':9094'
    elif security_type == 'SSL':
        endpoint_address = 'kafka://' + kafka_server + ':9093'
    else:
        assert False, 'unknown security method '+security_type

    KAFKA_DIR = os.environ['KAFKA_DIR']
    endpoint_args = 'push-endpoint='+endpoint_address+'&kafka-ack-level=broker&use-ssl=true&ca-location='+KAFKA_DIR+"/y-ca.crt"

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


@attr('kafka_ssl_test')
def test_ps_s3_notification_push_kafka_security_ssl():
    kafka_security('SSL')


@attr('kafka_ssl_test')
def test_ps_s3_notification_push_kafka_security_ssl_sasl():
    kafka_security('SSL_SASL')

