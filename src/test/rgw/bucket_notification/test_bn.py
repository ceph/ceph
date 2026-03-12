import logging
import json
import tempfile
import random
import threading
from concurrent.futures import ThreadPoolExecutor
import subprocess
import socket
import time
import os
import io
import string
import sys
from botocore.client import Config
from botocore.exceptions import ClientError
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from random import randint
import hashlib
import pytest
import boto3
import datetime
from cloudevents.http import from_http
from dateutil import parser
import requests

from . import(
    configfile,
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
    admin, \
    set_rgw_config_option, \
    bash, \
    S3Connection, \
    S3Bucket, \
    S3Key, \
    MultipartUpload


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


def get_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # address should not be reachable
        s.connect(('10.255.255.255', 1))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip


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
                    assert 'eTag' in record['s3']['object']
                    if record['s3']['bucket']['name'] == key.bucket.name and \
                        record['s3']['object']['key'] == key.name:
                        # Assertion Error needs to be fixed
                        #assert key.etag[1:-1] == record['s3']['object']['eTag']
                        if etags:
                            assert key.etag[1:-1] in etags
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
                assert 'eTag' in record['s3']['object']
                if record['s3']['bucket']['name'] == key.bucket.name and \
                    record['s3']['object']['key'] == key.name:
                    assert key.etag == record['s3']['object']['eTag']
                    if etags:
                        assert key.etag[1:-1] in etags
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
            assert object_size == expected_sizes.get(key.name)

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
            log.info('HTTP Server received empty event')
            self.send_response(200)
            self.end_headers()
            return
        body = self.rfile.read(content_length)
        if self.server.cloudevents:
            event = from_http(self.headers, body)
            record = json.loads(body)['Records'][0]
            assert event['specversion'] == '1.0'
            assert event['id'] == record['responseElements']['x-amz-request-id'] + '.' + record['responseElements']['x-amz-id-2']
            assert event['source'] == 'ceph:s3.' + record['awsRegion'] + '.' + record['s3']['bucket']['name']
            assert event['type'] == 'com.amazonaws.' + record['eventName']
            assert event['datacontenttype'] == 'application/json'
            assert event['subject'] == record['s3']['object']['key']
            assert parser.parse(event['time']) == parser.parse(record['eventTime'])
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
        """verify stored records agains a list of keys"""
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
        """verify stored records agains a list of keys"""
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

default_kafka_server = get_ip()
KAFKA_TEST_USER = 'alice'
KAFKA_TEST_PASSWORD = 'alice-secret'

def setup_scram_users_via_kafka_configs(mechanism: str) -> None:
    """to setup SCRAM users using kafka-configs.sh after Kafka is running."""
    if not mechanism.startswith('SCRAM'):
        return

    log.info(f"Setting up {mechanism} users via kafka-configs.sh...")

    kafka_dir = os.environ.get('KAFKA_DIR')
    if not kafka_dir:
        log.warning("KAFKA_DIR not set, skipping SCRAM setup")
        return
    
    kafka_configs = os.path.join(kafka_dir, 'bin/kafka-configs.sh')
    if not os.path.exists(kafka_configs):
        log.warning(f"kafka-configs.sh not found at {kafka_configs}")
        return
    
    scram_mechanism = 'SCRAM-SHA-512' if 'SHA-512' in mechanism else 'SCRAM-SHA-256'
    zk_connect = 'localhost:2181'
    
    try:
        # delete existing SCRAM credentials first
        subprocess.run(
            [kafka_configs,
             '--zookeeper', zk_connect,
             '--alter',
             '--entity-type', 'users',
             '--entity-name', KAFKA_TEST_USER,
             '--delete-config', 'scram-sha-256,scram-sha-512'],
            capture_output=True,
            timeout=15,
            check=False
        )
        time.sleep(1)
        
        # adding SCRAM credentials
        add_config_value = f'{scram_mechanism}=[password={KAFKA_TEST_PASSWORD}]'
        result = subprocess.run(
            [kafka_configs,
             '--zookeeper', zk_connect,
             '--alter',
             '--entity-type', 'users',
             '--entity-name', KAFKA_TEST_USER,
             '--add-config', add_config_value],
            capture_output=True,
            text=True,
            timeout=15,
            check=False
        )
        
        if result.returncode == 0:
            log.info(f"SCRAM user configured: {KAFKA_TEST_USER} ({scram_mechanism})")
        else:
            raise RuntimeError(f"Failed to create SCRAM user {KAFKA_TEST_USER} with {scram_mechanism}")
    except Exception as e:
        log.error(f"Failed to setup SCRAM users via kafka-configs: {e}")
        raise

def _kafka_ca_cert_path():
    kafka_dir = os.environ.get('KAFKA_DIR')
    if kafka_dir:
        ca_path = os.path.join(kafka_dir, 'y-ca.crt')
        if os.path.exists(ca_path):
            return ca_path
    return None

class KafkaReceiver(object):
    """class for receiving and storing messages on a topic from the kafka broker"""
    def __init__(self, topic_name, security_type, kafka_server, mechanism='PLAIN'):
        from kafka import KafkaConsumer
        from kafka.admin import KafkaAdminClient, NewTopic
        from kafka.errors import TopicAlreadyExistsError
        self.status = 'init'
        ca_cert = _kafka_ca_cert_path()
        base_config = {}
        port = 9092
        effective_protocol = 'PLAINTEXT'
        if security_type == 'SSL':
            port = 9093
            effective_protocol = 'SSL'
        elif security_type == 'SASL_SSL':
            port = 9094
            effective_protocol = 'SASL_SSL'
        elif security_type == 'SASL_PLAINTEXT':
            port = 9095
            effective_protocol = 'SASL_PLAINTEXT'

        if kafka_server is None:
            endpoint = default_kafka_server + ":" + str(port)
        elif ":" not in kafka_server and len(kafka_server.split(",")) == 1:
            endpoint = kafka_server + ":" + str(port)
        else:
            endpoint = kafka_server

        base_config['bootstrap_servers'] = endpoint
        if effective_protocol == 'SSL':
            base_config['security_protocol'] = 'SSL'
            if ca_cert:
                base_config['ssl_cafile'] = ca_cert
            kafka_dir = os.environ.get('KAFKA_DIR', '/opt/kafka')
            client_cert = os.path.join(kafka_dir, 'config/client.crt')
            client_key = os.path.join(kafka_dir, 'config/client.key')
            if os.path.exists(client_cert) and os.path.exists(client_key):
                base_config['ssl_certfile'] = client_cert
                base_config['ssl_keyfile'] = client_key
        elif effective_protocol == 'SASL_SSL':
            base_config['security_protocol'] = 'SASL_SSL'
            if ca_cert:
                base_config['ssl_cafile'] = ca_cert
            base_config['sasl_mechanism'] = mechanism
            base_config.update({
                'sasl_plain_username': KAFKA_TEST_USER,
                'sasl_plain_password': KAFKA_TEST_PASSWORD,
            })
        elif effective_protocol == 'SASL_PLAINTEXT':
            base_config['security_protocol'] = 'SASL_PLAINTEXT'
            base_config['sasl_mechanism'] = mechanism
            base_config.update({
                'sasl_plain_username': KAFKA_TEST_USER,
                'sasl_plain_password': KAFKA_TEST_PASSWORD,
            })

        remaining_retries = 10
        while remaining_retries > 0:
            try:
                admin_client = KafkaAdminClient(
                        request_timeout_ms=16000,
                        **base_config)
                topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
                admin_client.create_topics([topic])
                log.info('Kafka admin created topic: %s on broker/s: %s', topic_name, endpoint)
                break
            except Exception as error:
                if type(error) == TopicAlreadyExistsError:
                    log.info('Kafka admin topic %s already exists on broker/s: %s', topic_name, endpoint)
                    break
                remaining_retries -= 1
                log.warning('Kafka admin failed to create topic: %s on broker/s: %s. remaining reties: %d. error: %s',
                            topic_name, endpoint , remaining_retries, str(error))
                time.sleep(1)

        if remaining_retries == 0:
            raise Exception('Kafka admin failed to create topic: %s. no retries left', topic_name)

        remaining_retries = 10
        consumer_config = dict(base_config)
        consumer_config.update({
            'metadata_max_age_ms': 5000,
            'consumer_timeout_ms': 5000,
            'auto_offset_reset': 'earliest'
        })
        while remaining_retries > 0:
            try:
                self.consumer = KafkaConsumer(topic_name, **consumer_config)
                log.info('Kafka consumer connected to broker/s: %s for topic: %s', endpoint , topic_name)
                # This forces the consumer to fetch metadata immediately
                partitions = self.consumer.partitions_for_topic(topic)
                log.info('Kafka consumer partitions for topic: %s are: %s', topic_name, str(partitions))
                self.consumer.poll(timeout_ms=1000, max_records=1)
                break
            except Exception as error:
                remaining_retries -= 1
                log.warning('Kafka consumer failed to connect to broker/s: %s. for topic: %. remaining reties: %d. error: %s',
                            endpoint, topic_name,  remaining_retries, str(error))
                time.sleep(1)

        if remaining_retries == 0:
            raise Exception('Kafka consumer failed to connect to kafka for topic: %s. no retries left', topic_name)

        self.status = 'connected'
        self.events = []
        self.topic = topic_name
        self.stop = False
        self.producer_config = base_config

    def verify_s3_events(self, keys, exact_match=False, deletions=False, expected_sizes={}, etags=[]):
        """verify stored records agains a list of keys"""
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
        log.info('Kafka receiver for topic: %s started', receiver.topic)
        receiver.status = 'running'
        while not receiver.stop:
            for msg in receiver.consumer:
                receiver.events.append(json.loads(msg.value))
            time.sleep(0.1)
        log.info('Kafka receiver for topic: %s ended', receiver.topic)
    except Exception as error:
        log.info('Kafka receiver for topic: %s ended unexpectedly. error: %s', receiver.topic,  str(error))
    receiver.status = 'ended'


def create_kafka_receiver_thread(topic, security_type='PLAINTEXT', kafka_brokers=None, mechanism='PLAIN'):
    """create kafka receiver and thread"""
    receiver = KafkaReceiver(topic, security_type, kafka_server=kafka_brokers, mechanism=mechanism)
    task = threading.Thread(target=kafka_receiver_thread_runner, args=(receiver,))
    task.daemon = True
    return task, receiver

def stop_kafka_receiver(receiver, task):
    """stop the receiver thread and wait for it to finish"""
    receiver.stop = True
    task.join(5)
    try:
        receiver.consumer.unsubscribe()
        receiver.consumer.close()
        log.info('Kafka receiver on topic: %s gracefully stopped', receiver.topic)
    except Exception as error:
        log.info('Kafka receiver on topic: %s failed to gracefully stop. error: %s', receiver.topic, str(error))

def verify_kafka_receiver(receiver):
    """test the kafka receiver"""
    from kafka import KafkaProducer
    producer = KafkaProducer(**receiver.producer_config)
    producer.send(receiver.topic, value=json.dumps({'test': 'message'}).encode('utf-8'))
    producer.flush()
    events = []
    remaining_retries = 10
    while len(events) == 0:
        log.info('Kafka receiver (in "%s" state) waiting for test event (at: %s). remaining retries: %d',
                 receiver.status, datetime.datetime.now(), remaining_retries)
        time.sleep(1)
        events = receiver.get_and_reset_events()
        remaining_retries -= 1
        if remaining_retries == 0:
            raise Exception('kafka receiver on topic: %s did not receive test event in time', receiver.topic)
    assert len(events) == 1
    assert 'test' in events[0]
    log.info('Kafka receiver on topic: %s tested ok', receiver.topic)


def connection(no_retries=False):
    hostname = get_config_host()
    port_no = get_config_port()
    vstart_access_key = get_access_key()
    vstart_secret_key = get_secret_key()
    conn = S3Connection(aws_access_key_id=vstart_access_key,
                        aws_secret_access_key=vstart_secret_key,
                        is_secure=False, port=port_no, host=hostname)
    if no_retries:
        conn.num_retries = 0
    return conn


def connection2():
    hostname = get_config_host()
    port_no = 8001
    vstart_access_key = get_access_key()
    vstart_secret_key = get_secret_key()

    conn = S3Connection(aws_access_key_id=vstart_access_key,
                        aws_secret_access_key=vstart_secret_key,
                        is_secure=False, port=port_no, host=hostname)

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

    _, rc = admin(cmd, get_config_cluster())
    assert rc == 0

    conn = S3Connection(aws_access_key_id=access_key,
                        aws_secret_access_key=secret_key,
                        is_secure=False, port=get_config_port(), host=get_config_host())
    return conn, arn


def list_topics(assert_len=None, tenant=''):
    if tenant == '':
        result = admin(['topic', 'list'], get_config_cluster())
    else:
        result = admin(['topic', 'list', '--tenant', tenant], get_config_cluster())
    assert result[1] == 0
    parsed_result = json.loads(result[0])
    try:
        actual_len = len(parsed_result['topics'])
    except TypeError:
        actual_len = len(parsed_result)
    if assert_len and assert_len != actual_len:
        log.error(parsed_result)
        assert 'expected %d topics, got %d' % (assert_len, actual_len)
    return parsed_result


def get_stats_persistent_topic(topic_name, assert_entries_number=None):
    result = admin(['topic', 'stats', '--topic', topic_name], get_config_cluster())
    assert result[1] == 0
    parsed_result = json.loads(result[0])
    if assert_entries_number:
        actual_number = parsed_result['Topic Stats']['Entries']
        if actual_number != assert_entries_number:
            log.warning('Topic stats: %s', parsed_result)
            result = admin(['topic', 'dump', '--topic', topic_name], get_config_cluster())
            parsed_result = json.loads(result[0])
            log.warning('Topic dump:')
            for entry in parsed_result:
                log.warning(entry)
            assert 'expected %d entries, got %d' % (assert_entries_number, actual_number)
    return parsed_result


def get_topic(topic_name, tenant='', allow_failure=False):
    if tenant == '':
        result = admin(['topic', 'get', '--topic', topic_name], get_config_cluster())
    else:
        result = admin(['topic', 'get', '--topic', topic_name, '--tenant', tenant], get_config_cluster())
    if allow_failure:
        return result
    assert result[1] == 0
    parsed_result = json.loads(result[0])
    return parsed_result


def remove_topic(topic_name, tenant='', allow_failure=False):
    if tenant == '':
        result = admin(['topic', 'rm', '--topic', topic_name], get_config_cluster())
    else:
        result = admin(['topic', 'rm', '--topic', topic_name, '--tenant', tenant], get_config_cluster())
    if not allow_failure:
        assert result[1] == 0
    return result[1]


def list_notifications(bucket_name, assert_len=None, tenant=''):
    if tenant == '':
        result = admin(['notification', 'list', '--bucket', bucket_name], get_config_cluster())
    else:
        result = admin(['notification', 'list', '--bucket', bucket_name, '--tenant', tenant], get_config_cluster())
    assert result[1] == 0
    parsed_result = json.loads(result[0])
    actual_len = len(parsed_result['notifications'])
    if assert_len and assert_len != actual_len:
        log.error(parsed_result)
        assert 'expected %d notifications, got %d' % (assert_len, actual_len)
    return parsed_result


def get_notification(bucket_name, notification_name, tenant=''):
    if tenant == '':
        result = admin(['notification', 'get', '--bucket', bucket_name, '--notification-id', notification_name], get_config_cluster())
    else:
        result = admin(['notification', 'get', '--bucket', bucket_name, '--notification-id', notification_name, '--tenant', tenant], get_config_cluster())
    assert result[1] == 0
    parsed_result = json.loads(result[0])
    assert parsed_result['Id'] == notification_name
    return parsed_result


def remove_notification(bucket_name, notification_name='', tenant='', allow_failure=False):
    args = ['notification', 'rm', '--bucket', bucket_name]
    if notification_name != '':
        args.extend(['--notification-id', notification_name])
    if tenant != '':
        args.extend(['--tenant', tenant])
    result = admin(args, get_config_cluster())
    if not allow_failure:
        assert result[1] == 0
    return result[1]


zonegroup_feature_notification_v2 = 'notification_v2'


def zonegroup_modify_feature(enable, feature_name):
    if enable:
        command = '--enable-feature='+feature_name
    else:
        command = '--disable-feature='+feature_name
    result = admin(['zonegroup', 'modify', command], get_config_cluster())
    assert result[1] == 0
    result = admin(['period', 'update'], get_config_cluster())
    assert result[1] == 0
    result = admin(['period', 'commit'], get_config_cluster())
    assert result[1] == 0


def connect_random_user(tenant=''):
    access_key = str(time.time())
    secret_key = str(time.time())
    uid = UID_PREFIX + str(time.time())
    if tenant == '':
        _, rc = admin(['user', 'create', '--uid', uid, '--access-key', access_key, '--secret-key', secret_key, '--display-name', '"Super Man"'], get_config_cluster())
    else:
        _, rc = admin(['user', 'create', '--uid', uid, '--tenant', tenant, '--access-key', access_key, '--secret-key', secret_key, '--display-name', '"Super Man"'], get_config_cluster())
    assert rc == 0
    conn = S3Connection(aws_access_key_id=access_key,
                        aws_secret_access_key=secret_key,
                        is_secure=False, port=get_config_port(), host=get_config_host())
    return conn

##############
# bucket notifications tests
##############


@pytest.mark.basic_test
def test_topic():
    """ test topics set/get/delete """

    tenant = 'kaboom'
    conn = connect_random_user(tenant)

    # make sure there are no leftover topics
    delete_all_topics(conn, '', get_config_cluster())
    delete_all_topics(conn, tenant, get_config_cluster())

    zonegroup = get_config_zonegroup()
    bucket_name = gen_bucket_name()
    topic_name = bucket_name + TOPIC_SUFFIX

    # create topics
    endpoint_address = 'amqp://127.0.0.1:7001/vhost_1'
    endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=amqp.direct&amqp-ack-level=none'
    topic_conf1 = PSTopicS3(conn, topic_name+'_1', zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf1.set_config()
    assert topic_arn == 'arn:aws:sns:' + zonegroup + ':' + tenant + ':' + topic_name + '_1'

    endpoint_address = 'http://127.0.0.1:9001'
    endpoint_args = 'push-endpoint='+endpoint_address
    topic_conf2 = PSTopicS3(conn, topic_name+'_2', zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf2.set_config()
    assert topic_arn == 'arn:aws:sns:' + zonegroup + ':' + tenant + ':' + topic_name + '_2'
    endpoint_address = 'http://127.0.0.1:9002'
    endpoint_args = 'push-endpoint='+endpoint_address
    topic_conf3 = PSTopicS3(conn, topic_name+'_3', zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf3.set_config()
    assert topic_arn == 'arn:aws:sns:' + zonegroup + ':' + tenant + ':' + topic_name + '_3'

    # get topic 3
    result, status = topic_conf3.get_config()
    assert status == 200
    assert topic_arn == result['GetTopicResponse']['GetTopicResult']['Topic']['TopicArn']
    assert endpoint_address == result['GetTopicResponse']['GetTopicResult']['Topic']['EndPoint']['EndpointAddress']

    # Note that endpoint args may be ordered differently in the result
    # delete topic 1
    result = topic_conf1.del_config()
    assert status == 200

    # try to get a deleted topic
    _, status = topic_conf1.get_config()
    assert status == 404

    # get the remaining 2 topics
    list_topics(2, tenant)

    # delete topics
    status = topic_conf2.del_config()
    assert status == 200
    status = topic_conf3.del_config()
    assert status == 200

    # get topic list, make sure it is empty
    list_topics(0, tenant)


@pytest.mark.basic_test
def test_topic_admin():
    """ test topics set/get/delete """

    tenant = 'kaboom'
    conn = connect_random_user(tenant)

    # make sure there are no leftover topics
    delete_all_topics(conn, '', get_config_cluster())
    delete_all_topics(conn, tenant, get_config_cluster())

    zonegroup = get_config_zonegroup()
    bucket_name = gen_bucket_name()
    topic_name = bucket_name + TOPIC_SUFFIX

    # create topics
    endpoint_address = 'amqp://127.0.0.1:7001/vhost_1'
    endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=amqp.direct&amqp-ack-level=none'
    topic_conf1 = PSTopicS3(conn, topic_name+'_1', zonegroup, endpoint_args=endpoint_args)
    topic_arn1 = topic_conf1.set_config()
    assert topic_arn1 == 'arn:aws:sns:' + zonegroup + ':' + tenant + ':' + topic_name + '_1'

    endpoint_address = 'http://127.0.0.1:9001'
    endpoint_args = 'push-endpoint='+endpoint_address+'&persistent=true'
    topic_conf2 = PSTopicS3(conn, topic_name+'_2', zonegroup, endpoint_args=endpoint_args)
    topic_arn2 = topic_conf2.set_config()
    assert topic_arn2 == 'arn:aws:sns:' + zonegroup + ':' + tenant + ':' + topic_name + '_2'
    endpoint_address = 'http://127.0.0.1:9002'
    endpoint_args = 'push-endpoint=' + endpoint_address + '&persistent=true'
    topic_conf3 = PSTopicS3(conn, topic_name+'_3', zonegroup, endpoint_args=endpoint_args)
    topic_arn3 = topic_conf3.set_config()
    assert topic_arn3 == 'arn:aws:sns:' + zonegroup + ':' + tenant + ':' + topic_name + '_3'

    # get topic 3 via commandline
    parsed_result = get_topic(topic_name+'_3', tenant)
    assert parsed_result['arn'] == topic_arn3
    matches = [tenant, UID_PREFIX]
    assert all([x in parsed_result['owner'] for x in matches])
    assert parsed_result['dest']['persistent_queue'] == tenant + ":" + topic_name + '_3'

    # recall CreateTopic and verify the owner and persistent_queue remain same.
    topic_conf3 = PSTopicS3(conn, topic_name + '_3', zonegroup,
                            endpoint_args=endpoint_args)
    topic_arn3 = topic_conf3.set_config()
    assert topic_arn3 == 'arn:aws:sns:' + zonegroup + ':' + tenant + ':' + topic_name + '_3'
    # get topic 3 via commandline
    result = admin(
      ['topic', 'get', '--topic', topic_name + '_3', '--tenant', tenant],
      get_config_cluster())
    assert result[1] == 0
    parsed_result = json.loads(result[0])
    assert parsed_result['arn'] == topic_arn3
    assert all([x in parsed_result['owner'] for x in matches])
    assert parsed_result['dest']['persistent_queue'] == tenant + ":" + topic_name + '_3'

    # delete topic 3
    remove_topic(topic_name + '_3', tenant)

    # try to get a deleted topic
    _, result = get_topic(topic_name + '_3', tenant, allow_failure=True)
    print('"topic not found" error is expected')
    assert result == 2

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

    # create topics
    endpoint_address = 'amqp://127.0.0.1:7001'
    endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=amqp.direct&amqp-ack-level=none'
    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()
    assert topic_arn == 'arn:aws:sns:' + zonegroup + '::' + topic_name
    # create notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [
                    {
                        'Id': notification_name+'_1',
                        'TopicArn': topic_arn,
                        'Events': ['s3:ObjectCreated:*'],
                        'Filter': {
                            'Key': {
                                'FilterRules': [
                                    {'Name': 'prefix', 'Value': 'test'},
                                    {'Name': 'suffix', 'Value': 'txt'}
                                ]
                            }
                        }
                    },
                    {
                        'Id': notification_name+'_2',
                        'TopicArn': topic_arn,
                        'Events': ['s3:ObjectRemoved:*'],
                        'Filter': {
                            'Metadata': {
                                'FilterRules': [
                                    {'Name': 'x-amz-meta-foo', 'Value': 'bar'},
                                    {'Name': 'x-amz-meta-hello', 'Value': 'world'}]
                            },
                        }
                    },
                    {
                        'Id': notification_name+'_3',
                        'TopicArn': topic_arn,
                        'Events': [],
                        'Filter': {
                            'Tags': {
                                'FilterRules': [
                                    {'Name': 'tag1', 'Value': 'value1'},
                                    {'Name': 'tag2', 'Value': 'value2'}]
                            }
                        }
                    }]
    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    _, status = s3_notification_conf.set_config()
    assert status/100 == 2

    # get notification 1
    if with_cli:
        get_notification(bucket_name, notification_name+'_1')
    else:
        response, status = s3_notification_conf.get_config(notification=notification_name+'_1')
        assert status/100 == 2
        assert response['NotificationConfiguration']['TopicConfiguration']['Topic'] == topic_arn

    # list notification
    if with_cli:
        list_notifications(bucket_name, 3)
    else:
        result, status = s3_notification_conf.get_config()
        assert status == 200
        assert len(result['TopicConfigurations']) == 3

    # delete notification 2
    if with_cli:
        remove_notification(bucket_name, notification_name + '_2')
    else:
        _, status = s3_notification_conf.del_config(notification=notification_name+'_2')
        assert status/100 == 2

    # list notification
    if with_cli:
        list_notifications(bucket_name, 2)
    else:
        result, status = s3_notification_conf.get_config()
        assert status == 200
        assert len(result['TopicConfigurations']) == 2

    # delete notifications
    if with_cli:
        remove_notification(bucket_name)
    else:
        _, status = s3_notification_conf.del_config()
        assert status/100 == 2

    # list notification, make sure it is empty
    list_notifications(bucket_name, 0)

    # cleanup
    topic_conf.del_config()
    # delete the bucket
    conn.delete_bucket(bucket_name)


@pytest.mark.basic_test
def test_notification_configuration_admin():
    """ test notification list/set/get/delete, with admin cli """
    notification_configuration(True)


@pytest.mark.not_implemented
def test_topic_with_secret():
    """ test topics with secret set/get/delete """
    pytest.skip('This test is yet to be implemented')


@pytest.mark.basic_test
def test_notification_configuration():
    """ test notification set/get/deleter """
    notification_configuration(False)


@pytest.mark.basic_test
def test_notification_empty_config():
    """ test notification set/get/delete with empty config """
    hostname = get_ip()

    conn = connection()

    zonegroup = get_config_zonegroup()

    # create bucket
    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX

    # create topic
    endpoint_address = 'amqp://127.0.0.1:7001'
    endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=amqp.direct&amqp-ack-level=none'
    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()

    # create notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name+'_1',
                        'TopicArn': topic_arn,
                        'Events': []
                       }]
    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    _, status = s3_notification_conf.set_config()
    assert status/100 == 2

    # get notifications on a bucket
    response, status = s3_notification_conf.get_config(notification=notification_name+'_1')
    assert status/100 == 2
    assert response['NotificationConfiguration']['TopicConfiguration']['Topic'] == topic_arn

    # create notification again with empty configuration to check if it deletes or not
    topic_conf_list = []

    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    _, status = s3_notification_conf.set_config()
    assert status/100 == 2

    # make sure that the notification is now deleted
    response, status = s3_notification_conf.get_config()
    try:
        check = response['NotificationConfiguration']
    except KeyError as e:
        assert status/100 == 2
    else:
        assert False

    # cleanup
    topic_conf.del_config()
    # delete the bucket
    conn.delete_bucket(bucket_name)


@pytest.mark.amqp_test
def test_notification_filter_amqp():
    """ test notification filter """

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

    # create topic
    endpoint_address = 'amqp://' + hostname
    endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=' + exchange +'&amqp-ack-level=broker'

    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()

    # create notification
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
    assert status/100 == 2

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
        assert status/100 == 2
        skip_notif4 = False
    except Exception as error:
        print('note: metadata filter is not supported by boto3 - skipping test')
        skip_notif4 = True


    # get all notifications
    result, status = s3_notification_conf.get_config()
    assert status/100 == 2
    for conf in result['TopicConfigurations']:
        filter_name = conf['Filter']['Key']['FilterRules'][0]['Name']
        assert filter_name == 'prefix' or filter_name == 'suffix' or filter_name == 'regex', filter_name

    if not skip_notif4:
        result, status = s3_notification_conf4.get_config(notification=notification_name+'_4')
        assert status/100 == 2
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
        assert awsRegion == zonegroup
        bucket_arn = event['Records'][0]['s3']['bucket']['arn']
        assert bucket_arn == "arn:aws:s3:"+awsRegion+"::"+bucket_name
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

    assert set(found_in1) == set(expected_in1)
    assert set(found_in2) == set(expected_in2)
    assert set(found_in3) == set(expected_in3)
    if not skip_notif4:
        assert set(found_in4) == set(expected_in4)

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


@pytest.mark.basic_test
def test_notification_errors():
    """ test notification set/get/delete """
    conn = connection()
    zonegroup = get_config_zonegroup()
    bucket_name = gen_bucket_name()
    # create bucket
    bucket = conn.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX
    # create topic
    endpoint_address = 'amqp://127.0.0.1:7001'
    endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=amqp.direct&amqp-ack-level=none'
    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()

    # create notification with invalid event name
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

    # create notification with missing name
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

    # create notification with invalid topic ARN
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

    # create notification with unknown topic ARN
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

    # create notification with wrong bucket
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
    assert status == 200

    _, status = topic_conf.get_config()
    assert status == 404

    # cleanup
    # delete the bucket
    conn.delete_bucket(bucket_name)


def notification(endpoint_type, conn, account=None, cloudevents=False, kafka_brokers=None):
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
        host = get_ip()
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
        # create notification
        notification_name = bucket_name + NOTIFICATION_SUFFIX
        topic_conf_list = [{'Id': notification_name,
                            'TopicArn': topic_arn,
                            'Events': []
                            }]
        s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
        response, status = s3_notification_conf.set_config()
        assert status/100 == 2
    elif endpoint_type == 'amqp':
        # start amqp receiver
        exchange = 'ex1'
        task, receiver = create_amqp_receiver_thread(exchange, topic_name)
        task.start()
        endpoint_address = 'amqp://' + host
        # with acks from broker
        exchange = 'ex1'
        endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=' + exchange +'&amqp-ack-level=broker'
        # create two topic
        topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
        topic_arn = topic_conf.set_config()
        # create notification
        notification_name = bucket_name + NOTIFICATION_SUFFIX
        topic_conf_list = [{'Id': notification_name,
                            'TopicArn': topic_arn,
                            'Events': []
                            }]
        s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
        response, status = s3_notification_conf.set_config()
        assert status/100 == 2
    elif endpoint_type == 'kafka':
        # start kafka receiver
        default_kafka_server_and_port = default_kafka_server + ':9092'
        if kafka_brokers is not None:
            kafka_brokers = kafka_brokers + ',' + default_kafka_server_and_port
        task, receiver = create_kafka_receiver_thread(topic_name, kafka_brokers=kafka_brokers)
        task.start()
        verify_kafka_receiver(receiver)
        endpoint_address = 'kafka://' + default_kafka_server_and_port
        # without acks from broker
        endpoint_args = 'push-endpoint='+endpoint_address+'&kafka-ack-level=broker'
        if kafka_brokers is not None:
            endpoint_args += '&kafka-brokers=' + kafka_brokers
        # create topic
        topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
        topic_arn = topic_conf.set_config()
        # create notification
        notification_name = bucket_name + NOTIFICATION_SUFFIX
        topic_conf_list = [{'Id': notification_name,
                            'TopicArn': topic_arn,
                            'Events': []
                            }]
        s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
        response, status = s3_notification_conf.set_config()
        assert status/100 == 2
    else:
        pytest.skip('Unknown endpoint type: ' + endpoint_type)

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


@pytest.mark.amqp_test
def test_notification_amqp():
    """ test pushing amqp notification """
    conn = connection()
    notification('amqp', conn)


@pytest.mark.manual_test
def test_notification_amqp_idleness_check():
    """ test pushing amqp notification and checking for connection idleness """
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

    # create two topic
    endpoint_address = 'amqp://' + hostname
    # with acks from broker
    endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=' + exchange +'&amqp-ack-level=broker'
    topic_conf1 = PSTopicS3(conn, topic_name1, zonegroup, endpoint_args=endpoint_args)
    topic_arn1 = topic_conf1.set_config()
    # create notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name+'_1', 'TopicArn': topic_arn1,
                         'Events': []
                       }]

    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert status/100 == 2

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

    os.system("ss -tnp | grep 5672")

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

    os.system("ss -tnp | grep 5672")

    # cleanup
    stop_amqp_receiver(receiver1, task1)
    s3_notification_conf.del_config()
    topic_conf1.del_config()
    # delete the bucket
    conn.delete_bucket(bucket_name)


@pytest.mark.kafka_test
def test_notification_kafka():
    """ test pushing kafka notification """
    conn = connection()
    notification('kafka', conn)


@pytest.mark.kafka_failover
def test_notification_kafka_multiple_brokers_override():
    """ test pushing kafka notification """
    conn = connection()
    notification('kafka', conn, kafka_brokers='{host}:9091,{host}:9092'.format(host=default_kafka_server))


@pytest.mark.kafka_failover
def test_notification_kafka_multiple_brokers_append():
    """ test pushing kafka notification """
    conn = connection()
    notification('kafka', conn, kafka_brokers='{host}:9091'.format(host=default_kafka_server))


@pytest.mark.manual_test
def test_1K_topics():
    """ test creation of moe than 1K topics """
    conn = connection()
    zonegroup = get_config_zonegroup()
    base_bucket_name = gen_bucket_name()
    host = get_ip()
    endpoint_address = 'kafka://' + host
    endpoint_args = 'push-endpoint='+endpoint_address+'&kafka-ack-level=broker&persistent=true'
    topic_count = 1200
    thread_pool_size = 30
    topics = []
    notifications = []
    buckets = []
    log.info(f"creating {topic_count} topics, buckets and notifications")
    print(f"creating {topic_count} topics, buckets and notifications")
    # create topics buckets and notifications
    def create_topic_bucket_notification(i):
        bucket_name = base_bucket_name + str(i)
        topic_name = bucket_name + TOPIC_SUFFIX
        topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
        topic_arn = topic_conf.set_config()
        bucket = conn.create_bucket(bucket_name)
        notification_name = bucket_name + NOTIFICATION_SUFFIX
        topic_conf_list = [{'Id': notification_name,
                            'TopicArn': topic_arn,
                            'Events': []
                            }]
        s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
        response, status = s3_notification_conf.set_config()
        return topic_conf, bucket, s3_notification_conf

    with ThreadPoolExecutor(max_workers=thread_pool_size) as executor:
        futures = [executor.submit(create_topic_bucket_notification, i) for i in range(topic_count)]
        for i, future in enumerate(futures):
            try:
                topic_conf, bucket, s3_notification_conf = future.result()
                topics.append(topic_conf)
                buckets.append(bucket)
                notifications.append(s3_notification_conf)
            except Exception as e:
                log.error(f"error creating topic/bucket/notification {i}: {e}")

    log.info("creating objects in buckets")
    print("creating objects in buckets")
    # create objects in the buckets
    def create_object_in_bucket(bucket_idx):
        bucket = buckets[bucket_idx]
        content = str(os.urandom(32))
        key = bucket.new_key(str(bucket_idx))
        set_contents_from_string(key, content)

    with ThreadPoolExecutor(max_workers=thread_pool_size) as executor:
        futures = [executor.submit(create_object_in_bucket, i) for i in range(len(buckets))]
        for future in futures:
            try:
                future.result()
            except Exception as e:
                log.error(f"error creating object in bucket: {e}")

    log.info("deleting objects from buckets")
    print("deleting objects from buckets")
    # delete objects in the buckets
    def delete_object_in_bucket(bucket_idx):
        bucket = buckets[bucket_idx]
        key = bucket.new_key(str(bucket_idx))
        key.delete()

    with ThreadPoolExecutor(max_workers=thread_pool_size) as executor:
        futures = [executor.submit(delete_object_in_bucket, i) for i in range(len(buckets))]
        for future in futures:
            try:
                future.result()
            except Exception as e:
                log.error(f"error deleting object in bucket: {e}")

    # cleanup
    def cleanup_notification(notification):
        notification.del_config()

    def cleanup_topic(topic):
        topic.del_config()

    def cleanup_bucket(i):
        bucket_name = base_bucket_name + str(i)
        conn.delete_bucket(bucket_name)

    with ThreadPoolExecutor(max_workers=thread_pool_size) as executor:
        # cleanup notifications
        notification_futures = [executor.submit(cleanup_notification, notification) for notification in notifications]
        # cleanup topics
        topic_futures = [executor.submit(cleanup_topic, topic) for topic in topics]
        # cleanup buckets
        bucket_futures = [executor.submit(cleanup_bucket, i) for i in range(topic_count)]
        # wait for all cleanup operations to complete
        all_futures = notification_futures + topic_futures + bucket_futures
        for future in all_futures:
            try:
                future.result()
            except Exception as e:
                log.error(f"error during cleanup: {e}")


@pytest.mark.http_test
def test_notification_multi_delete():
    """ test deletion of multiple keys """
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

    # create topic
    endpoint_address = 'http://'+host+':'+str(port)
    endpoint_args = 'push-endpoint='+endpoint_address
    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()
    # create notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name,
                        'TopicArn': topic_arn,
                        'Events': ['s3:ObjectRemoved:*']
                       }]
    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert status/100 == 2

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


@pytest.mark.http_test
def test_notification_http():
    """ test pushing http notification """
    conn = connection()
    notification('http', conn)


@pytest.mark.http_test
def test_notification_cloudevents():
    """ test pushing cloudevents notification """
    conn = connection()
    notification('http', conn, cloudevents=True)



@pytest.mark.http_test
def test_opaque_data():
    """ test that opaque id set in topic, is sent in notification """
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

    # create topic
    endpoint_address = 'http://'+host+':'+str(port)
    endpoint_args = 'push-endpoint='+endpoint_address
    opaque_data = 'http://1.2.3.4:8888'
    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args, opaque_data=opaque_data)
    topic_arn = topic_conf.set_config()
    # create notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name,
                        'TopicArn': topic_arn,
                        'Events': []
                       }]
    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert status/100 == 2

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
        assert event['Records'][0]['opaqueData'] == opaque_data

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
        verify_kafka_receiver(receiver)
        endpoint_address = 'kafka://' + host
        endpoint_args = 'push-endpoint='+endpoint_address+'&kafka-ack-level=broker&persistent=true'
    else:
        pytest.skip('Unknown endpoint type: ' + endpoint_type)

    # create topic
    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()
    # create notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name,
                        'TopicArn': topic_arn,
                        'Events': topic_events
                        }]
    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert status/100 == 2

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
                          aws_secret_access_key=conn.aws_secret_access_key,
                          config=Config(s3={'addressing_style': 'path'}))
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    response = client.put_bucket_lifecycle_configuration(Bucket=bucket_name,
                                                         LifecycleConfiguration={'Rules': rules_creator(yesterday, obj_prefix)}
                                                         )

    # start lifecycle processing
    admin(['lc', 'process'], get_config_cluster())
    print('polling on bucket object to check if lifecycle deleted them...')
    max_loops = 100
    no_keys = list(bucket.list())
    while len(no_keys) > 0 and max_loops > 0:
        print('waiting 5 secs to check if lifecycle kicked in')
        time.sleep(5)
        no_keys = list(bucket.list())
        max_loops = max_loops - 1

    assert len(no_keys) == 0, "lifecycle didn't delete the objects after 500 seconds"
    wait_for_queue_to_drain(topic_name, http_port=port)
    assert len(no_keys) == 0
    event_keys = []
    events = receiver.get_and_reset_events()
    if not expected_abortion:
        assert number_of_objects * 2 <= len(events)
    for event in events:
        assert event['Records'][0]['eventName'] in record_events
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


@pytest.mark.http_test
def test_lifecycle_http():
    """ test that when object is deleted due to lifecycle policy, http endpoint """

    conn = connection()
    lifecycle('http', conn, 10, ['s3:ObjectLifecycle:Expiration:*', 's3:LifecycleExpiration:*'], create_thread,
              rules_creator, ['LifecycleExpiration:Delete', 'ObjectLifecycle:Expiration:Current'])


@pytest.mark.kafka_test
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


@pytest.mark.http_test
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


def creation_triggers(external_endpoint_address=None, ca_location=None, verify_ssl='true'):
    """ test object creation notifications in using put/copy/post"""

    if not external_endpoint_address:
        hostname = get_ip()
        proc = init_rabbitmq()
        if proc is  None:
            pytest.skip('end2end amqp tests require rabbitmq-server installed')
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

    # create topic
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
    # create notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name,'TopicArn': topic_arn,
                        'Events': ['s3:ObjectCreated:Put', 's3:ObjectCreated:Copy', 's3:ObjectCreated:CompleteMultipartUpload']
                       }]

    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert status/100 == 2

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


@pytest.mark.amqp_test
def test_creation_triggers_amqp():
    creation_triggers(external_endpoint_address="amqp://localhost:5672")


@pytest.mark.amqp_ssl_test
def test_creation_triggers_external():

    from distutils.util import strtobool

    if 'AMQP_EXTERNAL_ENDPOINT' in os.environ:
        try:
            if strtobool(os.environ['AMQP_VERIFY_SSL']):
                verify_ssl = 'true'
            else:
                verify_ssl = 'false'
        except Exception as e:
            verify_ssl = 'true'

        creation_triggers(
            external_endpoint_address=os.environ['AMQP_EXTERNAL_ENDPOINT'],
            verify_ssl=verify_ssl)
    else:
        pytest.skip("Set AMQP_EXTERNAL_ENDPOINT to a valid external AMQP endpoint url for this test to run")


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


@pytest.mark.amqp_ssl_test
def test_creation_triggers_ssl():

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

        creation_triggers(ca_location=CACERTFILE)

        del os.environ['RABBITMQ_CONFIG_FILE']


@pytest.mark.amqp_test
def test_post_object_upload_amqp():
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
                         config=Config(s3={'addressing_style': 'path'})
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

    # create topics
    endpoint_address = 'amqp://' + hostname
    endpoint_args = 'push-endpoint=' + endpoint_address + '&amqp-exchange=' + exchange + '&amqp-ack-level=broker'
    topic_conf1 = PSTopicS3(conn, topic_name+'_1', zonegroup, endpoint_args=endpoint_args)
    topic_arn1 = topic_conf1.set_config()

    # create notifications
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name+'_1', 'TopicArn': topic_arn1,
                        'Events': ['s3:ObjectCreated:Post']
                       }]
    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert status/100 == 2

    payload = OrderedDict([("key" , "foo.txt"),("acl" , "public-read"),\
    ("Content-Type" , "text/plain"),('file', ('bar'))])

    # POST upload
    r = requests.post(url, files=payload, verify=True)
    assert r.status_code == 204

    # check amqp receiver
    events = receiver1.get_and_reset_events()
    assert len(events) == 1

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
        verify_kafka_receiver(receiver)
        endpoint_address = 'kafka://' + host
        endpoint_args = 'push-endpoint='+endpoint_address+'&kafka-ack-level=broker'
    else:
        pytest.skip('Unknown endpoint type: ' + endpoint_type)

    # create topic
    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()
    # create notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name, 'TopicArn': topic_arn,
                        'Events': []
                        }]

    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert status/100 == 2

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


@pytest.mark.http_test
def test_multipart_http():
    """ test http multipart object upload """
    conn = connection()
    multipart_endpoint_agnostic('http', conn)


@pytest.mark.kafka_test
def test_multipart_kafka():
    """ test kafka multipart object upload """
    conn = connection()
    multipart_endpoint_agnostic('kafka', conn)


@pytest.mark.amqp_test
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
        verify_kafka_receiver(receiver)
        endpoint_address = 'kafka://' + host
        endpoint_args = 'push-endpoint='+endpoint_address+'&kafka-ack-level=broker&persistent=true'
    else:
        pytest.skip('Unknown endpoint type: ' + endpoint_type)

    # create topic
    zonegroup = get_config_zonegroup()
    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()
    # create notification
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
    assert status/100 == 2

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
    assert len(events) == len(expected_keys)
    for event in events:
        assert(event['Records'][0]['s3']['object']['key'] in expected_keys)

    # delete objects
    for key in bucket.list():
        key.delete()
    print('wait for the messages...')
    wait_for_queue_to_drain(topic_name, http_port=port)
    # check endpoint receiver
    events = receiver.get_and_reset_events()
    assert len(events) == len(expected_keys)
    for event in events:
        assert(event['Records'][0]['s3']['object']['key'] in expected_keys)

    # cleanup
    receiver.close(task)
    s3_notification_conf.del_config()
    topic_conf.del_config()
    # delete the bucket
    conn.delete_bucket(bucket_name)


@pytest.mark.kafka_test
def test_metadata_filter_kafka():
    """ test notification of filtering metadata, kafka endpoint """
    conn = connection()
    metadata_filter('kafka', conn)


@pytest.mark.http_test
def test_metadata_filter_http():
    """ test notification of filtering metadata, http endpoint """
    conn = connection()
    metadata_filter('http', conn)


@pytest.mark.amqp_test
def test_metadata_filter_ampq():
    """ test notification of filtering metadata, ampq endpoint """
    conn = connection()
    metadata_filter('amqp', conn)


@pytest.mark.amqp_test
def test_metadata_amqp():
    """ test notification of metadata """

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

    # create topic
    endpoint_address = 'amqp://' + hostname
    endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=' + exchange +'&amqp-ack-level=routable'
    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()
    # create notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    meta_key = 'meta1'
    meta_value = 'This is my metadata value'
    meta_prefix = META_PREFIX
    topic_conf_list = [{'Id': notification_name, 'TopicArn': topic_arn,
        'Events': ['s3:ObjectCreated:*', 's3:ObjectRemoved:*'],
    }]

    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    _, status = s3_notification_conf.set_config()
    assert status/100 == 2

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
        assert value[0] == meta_value

    # delete objects
    for key in bucket.list():
        key.delete()
    print('wait for 5sec for the messages...')
    time.sleep(5)
    # check amqp receiver
    events = receiver.get_and_reset_events()
    for event in events:
        value = [x['val'] for x in event['Records'][0]['s3']['object']['metadata'] if x['key'] == META_PREFIX+meta_key]
        assert value[0] == meta_value

    # cleanup
    stop_amqp_receiver(receiver, task)
    s3_notification_conf.del_config()
    topic_conf.del_config()
    # delete the bucket
    conn.delete_bucket(bucket_name)


@pytest.mark.amqp_test
def test_tags_amqp():
    """ test notification of tags """

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

    # create topic
    endpoint_address = 'amqp://' + hostname
    endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=' + exchange +'&amqp-ack-level=routable'
    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()
    # create notification
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
    assert status/100 == 2

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
            assert obj_tags == expected_tags1
        event_count += 1
        assert(key in expected_keys)

    assert event_count == len(expected_keys)

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
            assert obj_tags == expected_tags1
        event_count += 1
        assert(key in expected_keys)

    assert(event_count == len(expected_keys))

    # cleanup
    stop_amqp_receiver(receiver, task)
    s3_notification_conf.del_config()
    topic_conf.del_config()
    # delete the bucket
    conn.delete_bucket(bucket_name)

@pytest.mark.amqp_test
def test_versioning_amqp():
    """ test notification of object versions """

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

    # create topic
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
    assert status/100 == 2

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

    assert num_of_versions == 3

    # cleanup
    stop_amqp_receiver(receiver, task)
    s3_notification_conf.del_config()
    topic_conf.del_config()
    # delete the bucket
    bucket.delete_key(copy_of_key.name, version_id=ver3)
    bucket.delete_key(key.name, version_id=ver2)
    bucket.delete_key(key.name, version_id=ver1)
    #conn.delete_bucket(bucket_name)


@pytest.mark.amqp_test
def test_versioned_deletion_amqp():
    """ test notification of deletion markers """

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

    # create topic
    endpoint_address = 'amqp://' + hostname
    endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=' + exchange +'&amqp-ack-level=broker'
    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()
    # create notification
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
    assert status/100 == 2

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
    assert delete_events == 2*2
    # 1 deletion marker was created
    # notified over the same topic over 2 notifications (1,2)
    assert delete_marker_create_events == 1*2

    # cleanup
    delete_marker_key.delete()
    stop_amqp_receiver(receiver, task)
    s3_notification_conf.del_config()
    topic_conf.del_config()
    # delete the bucket
    conn.delete_bucket(bucket_name)


@pytest.mark.manual_test
def test_persistent_cleanup():
    """ test reservation cleanup after gateway crash """
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

    # create topic
    endpoint_address = 'http://'+host+':'+str(port)
    endpoint_args = 'push-endpoint='+endpoint_address+'&persistent=true'
    topic_conf = PSTopicS3(gw, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()

    # create notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name, 'TopicArn': topic_arn,
        'Events': ['s3:ObjectCreated:Put']
        }]
    s3_notification_conf = PSNotificationS3(gw, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert status/100 == 2

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
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.settimeout(1)
        s.connect((get_ip(), http_port))
        log.info("http server listening on port "+str_port)
    except (socket.error, socket.timeout):
        assert False, 'http server NOT listening on port '+str_port
    finally:
        s.close()


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
        assert result[1] == 0
        parsed_result = json.loads(result[0])
        entries = parsed_result['Topic Stats']['Entries']
        retries += 1
        time_diff = time.time() - start_time
        log.info('shards for %s has %d entries after %ds', topic_name, entries, time_diff)
        if retries > 100:
            log.warning('shards for %s still has %d entries after %ds', topic_name, entries, time_diff)
            assert entries == 0
        time.sleep(5)
    time_diff = time.time() - start_time
    log.info('waited for %ds for shards of %s to drain', time_diff, topic_name)


def persistent_topic_stats(conn, endpoint_type):
    zonegroup = get_config_zonegroup()

    # create bucket
    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX

    host = get_ip()
    task = None
    port = None
    wrong_port = 1234
    endpoint_address = endpoint_type+'://'+host+':'+str(wrong_port)
    if endpoint_type == 'http':
        # create random port for the http server
        port = random.randint(10000, 20000)
        # start an http server in a separate thread
        receiver = HTTPServerWithEvents((host, port))
        endpoint_args = 'push-endpoint='+endpoint_address+'&persistent=true'+ \
                        '&retry_sleep_duration=1'
    elif endpoint_type == 'amqp':
        # start amqp receiver
        exchange = 'ex1'
        task, receiver = create_amqp_receiver_thread(exchange, topic_name)
        task.start()
        endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange='+exchange+'&amqp-ack-level=broker&persistent=true'+ \
                        '&retry_sleep_duration=1'
    elif endpoint_type == 'kafka':
        # start kafka receiver
        task, receiver = create_kafka_receiver_thread(topic_name)
        task.start()
        verify_kafka_receiver(receiver)
        endpoint_args = 'push-endpoint='+endpoint_address+'&kafka-ack-level=broker&persistent=true'+ \
                        '&retry_sleep_duration=1'
    else:
        pytest.skip('Unknown endpoint type: ' + endpoint_type)

    # create topic
    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()
    # create notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name, 'TopicArn': topic_arn,
                        'Events': []
                        }]

    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert status/100 == 2

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

    # change the endpoint port
    if endpoint_type == 'http':
        endpoint_address = endpoint_type+'://'+host+':'+str(port)
        endpoint_args = 'push-endpoint='+endpoint_address+'&persistent=true'+ \
                        '&retry_sleep_duration=1'
    elif endpoint_type == 'amqp':
        endpoint_address = endpoint_type+'://'+host
        endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange='+exchange+'&amqp-ack-level=broker&persistent=true'+ \
                        '&retry_sleep_duration=1'
    elif endpoint_type == 'kafka':
        endpoint_address = endpoint_type+'://'+host
        endpoint_args = 'push-endpoint='+endpoint_address+'&kafka-ack-level=broker&persistent=true'+ \
                        '&retry_sleep_duration=1'
    
    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()

    wait_for_queue_to_drain(topic_name, http_port=port)

    # cleanup
    s3_notification_conf.del_config()
    topic_conf.del_config()
    # delete the bucket
    conn.delete_bucket(bucket_name)
    receiver.close(task)


@pytest.mark.http_test
def test_persistent_topic_stats_http():
    """ test persistent topic stats, http endpoint """
    conn = connection()
    persistent_topic_stats(conn, 'http')


@pytest.mark.kafka_test
def test_persistent_topic_stats_kafka():
    """ test persistent topic stats, kafka endpoint """
    conn = connection()
    persistent_topic_stats(conn, 'kafka')


@pytest.mark.amqp_test
def test_persistent_topic_stats_amqp():
    """ test persistent topic stats, amqp endpoint """
    conn = connection()
    persistent_topic_stats(conn, 'amqp')


@pytest.mark.kafka_test
def test_persistent_topic_dump():
    """ test persistent topic dump """
    conn = connection()
    zonegroup = get_config_zonegroup()

    # create bucket
    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX

    # start kafka receiver
    host = get_ip()
    task, receiver = create_kafka_receiver_thread(topic_name)
    task.start()
    verify_kafka_receiver(receiver)

    # create topic
    endpoint_address = 'kafka://WrongHost' # wrong port
    endpoint_args = 'push-endpoint='+endpoint_address+'&kafka-ack-level=broker&persistent=true'+ \
                    '&retry_sleep_duration=1'
    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()
    # create notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name, 'TopicArn': topic_arn,
                        'Events': []
                        }]

    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert status/100 == 2

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
    assert result[1] == 0
    parsed_result = json.loads(result[0])
    assert len(parsed_result) == number_of_objects

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
    assert result[1] == 0
    parsed_result = json.loads(result[0])
    assert len(parsed_result) == 2*number_of_objects

    # change the endpoint port
    endpoint_address = 'kafka://' + host
    endpoint_args = 'push-endpoint='+endpoint_address+'&kafka-ack-level=broker&persistent=true'+ \
                    '&retry_sleep_duration=1'
    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()

    wait_for_queue_to_drain(topic_name,)

    result = admin(['topic', 'dump', '--topic', topic_name], get_config_cluster())
    assert result[1] == 0
    parsed_result = json.loads(result[0])
    assert len(parsed_result) == 0

    # cleanup
    s3_notification_conf.del_config()
    topic_conf.del_config()
    # delete the bucket
    conn.delete_bucket(bucket_name)
    receiver.close(task)


def persistent_topic_configs(persistency_time, config_dict):
    # create connection with no retries
    conn = connection(no_retries=True)
    zonegroup = get_config_zonegroup()

    # create random port for the http server
    host = get_ip()
    port = random.randint(10000, 20000)

    # create bucket
    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX

    # create topic
    endpoint_address = 'http://'+host+':'+str(port)
    endpoint_args = 'push-endpoint='+endpoint_address+'&persistent=true&'+create_persistency_config_string(config_dict)
    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()
    # create notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name, 'TopicArn': topic_arn,
                        'Events': []
                        }]

    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert status/100 == 2

    # topic get
    parsed_result = get_topic(topic_name)
    parsed_result_dest = parsed_result["dest"]
    for key, value in config_dict.items():
        assert parsed_result_dest[key] == str(value)

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
    log.info('creation of %d objects took %f seconds', number_of_objects, time_diff)

    time_buffer = 20

    # topic stats
    if time_diff > persistency_time:
        log.warning('persistency time for topic %s already passed. not possible to check object in the queue, as some may have expired', topic_name)
    else:
        get_stats_persistent_topic(topic_name, number_of_objects)
        # wait as much as ttl and check if the persistent topics have expired
        time.sleep(persistency_time+time_buffer)

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
    [thr.join() for thr in client_threads]
    time_diff = time.time() - start_time
    log.info('deletion of %d objects took %f seconds', count, time_diff)
    assert count == number_of_objects

    # topic stats
    if time_diff > persistency_time:
        log.warning('persistency time for topic %s already passed. not possible to check object in the queue, as some may have expired', topic_name)
    else:
        get_stats_persistent_topic(topic_name, number_of_objects)
        # wait as much as ttl and check if the persistent topics have expired
        time.sleep(persistency_time+time_buffer)

    get_stats_persistent_topic(topic_name, 0)

    # cleanup
    s3_notification_conf.del_config()
    topic_conf.del_config()
    # delete the bucket
    conn.delete_bucket(bucket_name)

def create_persistency_config_string(config_dict):
    str_ret = ""
    for key, value in config_dict.items():
        if key != "None":
            str_ret += key + "=" + str(value) + '&'

    return str_ret[:-1]

@pytest.mark.basic_test
def test_persistent_topic_configs_ttl():
    """ test persistent topic configurations with time_to_live """
    config_dict = {"time_to_live": 30, "max_retries": "None", "retry_sleep_duration": "None"}
    persistency_time = config_dict["time_to_live"]

    persistent_topic_configs(persistency_time, config_dict)

@pytest.mark.basic_test
def test_persistent_topic_configs_max_retries():
    """ test persistent topic configurations with max_retries and retry_sleep_duration """
    config_dict = {"time_to_live": "None", "max_retries": 10, "retry_sleep_duration": 1}
    persistency_time = config_dict["max_retries"]*config_dict["retry_sleep_duration"]

    persistent_topic_configs(persistency_time, config_dict)

@pytest.mark.manual_test
def test_persistent_notificationback():
    """ test pushing persistent notification pushback """
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

    # create topic
    endpoint_address = 'http://'+host+':'+str(port)
    endpoint_args = 'push-endpoint='+endpoint_address+'&persistent=true'
    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()
    # create notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name, 'TopicArn': topic_arn,
                         'Events': []
                       }]

    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert status/100 == 2

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


@pytest.mark.kafka_test
def test_notification_kafka_idle_behaviour():
    """ test pushing kafka notification idle behaviour check """
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
    verify_kafka_receiver(receiver)

    # create topic
    endpoint_address = 'kafka://' + default_kafka_server
    # with acks from broker
    endpoint_args = 'push-endpoint='+endpoint_address+'&kafka-ack-level=broker'
    topic_conf1 = PSTopicS3(conn, topic_name+'_1', zonegroup, endpoint_args=endpoint_args)
    topic_arn1 = topic_conf1.set_config()
    # create notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name + '_1', 'TopicArn': topic_arn1,
                     'Events': []
                   }]

    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert status/100 == 2

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
        cmd = "ss -tnp | grep 9092 | grep radosgw"
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


@pytest.mark.not_implemented
def test_persistent_gateways_recovery():
    """ test gateway recovery of persistent notifications """
    pytest.skip('This test is yet to be implemented')


@pytest.mark.not_implemented
def test_persistent_multiple_gateways():
    """ test pushing persistent notification via two gateways """
    pytest.skip('This test is yet to be implemented')


def persistent_topic_multiple_endpoints(conn, endpoint_type):
    zonegroup = get_config_zonegroup()

    # create bucket
    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX
    topic_name_1 = topic_name+'_1'

    host = get_ip()
    task = None
    port = None
    if endpoint_type == 'http':
        # create random port for the http server
        port = random.randint(10000, 20000)
        # start an http server in a separate thread
        receiver = HTTPServerWithEvents((host, port))
        endpoint_address = 'http://'+host+':'+str(port)
        endpoint_args = 'push-endpoint='+endpoint_address+'&persistent=true'+ \
                        '&retry_sleep_duration=1'
    elif endpoint_type == 'amqp':
        # start amqp receiver
        exchange = 'ex1'
        task, receiver = create_amqp_receiver_thread(exchange, topic_name_1)
        task.start()
        endpoint_address = 'amqp://' + host
        endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange='+exchange+'&amqp-ack-level=broker&persistent=true'+ \
                        '&retry_sleep_duration=1'
    elif endpoint_type == 'kafka':
        # start kafka receiver
        task, receiver = create_kafka_receiver_thread(topic_name_1)
        task.start()
        verify_kafka_receiver(receiver)
        endpoint_address = 'kafka://' + host
        endpoint_args = 'push-endpoint='+endpoint_address+'&kafka-ack-level=broker&persistent=true'+ \
                        '&retry_sleep_duration=1'
    else:
        pytest.skip('Unknown endpoint type: ' + endpoint_type)

    # create two topics
    topic_conf1 = PSTopicS3(conn, topic_name_1, zonegroup, endpoint_args=endpoint_args)
    topic_arn1 = topic_conf1.set_config()
    endpoint_address = 'http://kaboom:9999'
    endpoint_args = 'push-endpoint='+endpoint_address+'&persistent=true'+ \
                    '&retry_sleep_duration=1'
    topic_conf2 = PSTopicS3(conn, topic_name+'_2', zonegroup, endpoint_args=endpoint_args)
    topic_arn2 = topic_conf2.set_config()

    # create two notifications
    notification_name = bucket_name + NOTIFICATION_SUFFIX+'_1'
    topic_conf_list = [{'Id': notification_name, 'TopicArn': topic_arn1,
                         'Events': []
                       }]
    s3_notification_conf1 = PSNotificationS3(conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf1.set_config()
    assert status/100 == 2
    notification_name = bucket_name + NOTIFICATION_SUFFIX+'_2'
    topic_conf_list = [{'Id': notification_name, 'TopicArn': topic_arn2,
                         'Events': []
                       }]
    s3_notification_conf2 = PSNotificationS3(conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf2.set_config()
    assert status/100 == 2

    client_threads = []
    start_time = time.time()
    number_of_objects = 10
    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        content = str(os.urandom(1024*1024))
        thr = threading.Thread(target = set_contents_from_string, args=(key, content,))
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]

    keys = list(bucket.list())

    wait_for_queue_to_drain(topic_name_1, http_port=port)
    receiver.verify_s3_events(keys, exact_match=True, deletions=False)

    # delete objects from the bucket
    client_threads = []
    start_time = time.time()
    for key in bucket.list():
        thr = threading.Thread(target = key.delete, args=())
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]

    wait_for_queue_to_drain(topic_name_1, http_port=port)
    receiver.verify_s3_events(keys, exact_match=True, deletions=True)

    # cleanup
    s3_notification_conf1.del_config()
    topic_conf1.del_config()
    s3_notification_conf2.del_config()
    topic_conf2.del_config()
    conn.delete_bucket(bucket_name)
    receiver.close(task)


@pytest.mark.http_test
def test_persistent_multiple_endpoints_http():
    """ test pushing persistent notification when one of the endpoints has error, http endpoint """
    conn = connection()
    persistent_topic_multiple_endpoints(conn, 'http')


@pytest.mark.kafka_test
def test_persistent_multiple_endpoints_kafka():
    """ test pushing persistent notification when one of the endpoints has error, kafka endpoint """
    conn = connection()
    persistent_topic_multiple_endpoints(conn, 'kafka')


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
        host = get_ip()
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
        verify_kafka_receiver(receiver)
        endpoint_address = 'kafka://' + host
        endpoint_args = 'push-endpoint='+endpoint_address+'&kafka-ack-level=broker'+'&persistent=true'
    else:
        pytest.skip('Unknown endpoint type: ' + endpoint_type)


    # create topic
    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()
    # create notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name, 'TopicArn': topic_arn,
                         'Events': []
                       }]

    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert status/100 == 2

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


@pytest.mark.http_test
def test_persistent_notification_http():
    """ test pushing persistent notification http """
    conn = connection()
    persistent_notification('http', conn)

@pytest.mark.http_test
def test_persistent_notification_http_account():
    """ test pushing persistent notification via http for account user """

    account = 'RGW77777777777777777'
    user = UID_PREFIX + 'test'

    _, rc = admin(['account', 'create', '--account-id', account, '--account-name', 'testacct'], get_config_cluster())
    assert rc in [0, 17] # EEXIST okay if we rerun

    conn, _ = another_user(user=user, account=account)
    try:
        persistent_notification('http', conn, account)
    finally:
        admin(['user', 'rm', '--uid', user], get_config_cluster())
        admin(['account', 'rm', '--account-id', account], get_config_cluster())

@pytest.mark.amqp_test
def test_persistent_notification_amqp():
    """ test pushing persistent notification amqp """
    conn = connection()
    persistent_notification('amqp', conn)


@pytest.mark.kafka_test
def test_persistent_notification_kafka():
    """ test pushing persistent notification kafka """
    conn = connection()
    persistent_notification('kafka', conn)


def random_string(length):
    import string
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(length))


@pytest.mark.amqp_test
def test_persistent_notification_large_amqp():
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

    # create topic
    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()
    # create notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name, 'TopicArn': topic_arn,
                         'Events': []
                       }]

    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert status/100 == 2

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


@pytest.mark.not_implemented
def test_topic_update():
    """ test updating topic associated with a notification"""
    pytest.skip('This test is yet to be implemented')


@pytest.mark.not_implemented
def test_notification_update():
    """ test updating the topic of a notification"""
    pytest.skip('This test is yet to be implemented')


@pytest.mark.not_implemented
def test_multiple_topics_notification():
    """ test notification creation with multiple topics"""
    pytest.skip('This test is yet to be implemented')


@pytest.mark.basic_test
def test_list_topics_migration():
    """ test list topics on migration"""
    if get_config_cluster() == 'noname':
        pytest.skip('realm is needed for migration test')
    
    # Initialize connections and configurations
    conn1 = connection()
    tenant = 'kaboom1'
    conn2 = connect_random_user(tenant)
    bucket_name = gen_bucket_name()
    topics = [f"{bucket_name}{TOPIC_SUFFIX}{i}" for i in range(1, 7)]
    tenant_topics = [f"{tenant}_{topic}" for topic in topics]
    
    # Define topic names with version
    topic_versions = {
        "topic1_v2": f"{topics[0]}_v2",
        "topic2_v2": f"{topics[1]}_v2",
        "topic3_v1": f"{topics[2]}_v1",
        "topic4_v1": f"{topics[3]}_v1",
        "topic5_v1": f"{topics[4]}_v1",
        "topic6_v1": f"{topics[5]}_v1",
        "tenant_topic1_v2": f"{tenant_topics[0]}_v2",
        "tenant_topic2_v1": f"{tenant_topics[1]}_v1",
        "tenant_topic3_v1": f"{tenant_topics[2]}_v1"
    }
    
    # Get necessary configurations
    host = get_ip()
    http_port = random.randint(10000, 20000)
    endpoint_address = 'http://' + host + ':' + str(http_port)
    endpoint_args = 'push-endpoint=' + endpoint_address + '&persistent=true'
    zonegroup = get_config_zonegroup()
    conf_cluster = get_config_cluster()
    
    # Make sure there are no leftover topics on v2
    zonegroup_modify_feature(enable=True, feature_name=zonegroup_feature_notification_v2)
    delete_all_topics(conn1, '', conf_cluster)
    delete_all_topics(conn2, tenant, conf_cluster)

    # Start v1 notification
    # Make sure there are no leftover topics on v1
    zonegroup_modify_feature(enable=False, feature_name=zonegroup_feature_notification_v2)
    delete_all_topics(conn1, '', conf_cluster)
    delete_all_topics(conn2, tenant, conf_cluster)
    
    # Create - v1 topics
    topic_conf = PSTopicS3(conn1, topic_versions['topic3_v1'], zonegroup, endpoint_args=endpoint_args)
    topic_arn3 = topic_conf.set_config()
    topic_conf = PSTopicS3(conn1, topic_versions['topic4_v1'], zonegroup, endpoint_args=endpoint_args)
    topic_arn4 = topic_conf.set_config()
    topic_conf = PSTopicS3(conn1, topic_versions['topic5_v1'], zonegroup, endpoint_args=endpoint_args)
    topic_arn5 = topic_conf.set_config()
    topic_conf = PSTopicS3(conn1, topic_versions['topic6_v1'], zonegroup, endpoint_args=endpoint_args)
    topic_arn6 = topic_conf.set_config()
    tenant_topic_conf = PSTopicS3(conn2, topic_versions['tenant_topic2_v1'], zonegroup, endpoint_args=endpoint_args)
    tenant_topic_arn2 = tenant_topic_conf.set_config()
    tenant_topic_conf = PSTopicS3(conn2, topic_versions['tenant_topic3_v1'], zonegroup, endpoint_args=endpoint_args)
    tenant_topic_arn3 = tenant_topic_conf.set_config()
    
    # Start v2 notification
    zonegroup_modify_feature(enable=True, feature_name=zonegroup_feature_notification_v2) 
    
    # Create - v2 topics
    topic_conf = PSTopicS3(conn1, topic_versions['topic1_v2'], zonegroup, endpoint_args=endpoint_args)
    topic_arn1 = topic_conf.set_config()
    topic_conf = PSTopicS3(conn1, topic_versions['topic2_v2'], zonegroup, endpoint_args=endpoint_args)
    topic_arn2 = topic_conf.set_config()
    tenant_topic_conf = PSTopicS3(conn2, topic_versions['tenant_topic1_v2'], zonegroup, endpoint_args=endpoint_args)
    tenant_topic_arn1 = tenant_topic_conf.set_config()
    
    # Verify topics list
    try:
        # Verify no tenant topics
        res, status = topic_conf.get_list()
        assert status // 100 == 2
        listTopicsResponse = res.get('ListTopicsResponse', {})
        listTopicsResult = listTopicsResponse.get('ListTopicsResult', {})
        topics = listTopicsResult.get('Topics', {})
        member = topics['member'] if topics else []
        assert len(member) == 6
        
        # Verify tenant topics
        res, status = tenant_topic_conf.get_list()
        assert status // 100 == 2
        listTopicsResponse = res.get('ListTopicsResponse', {})
        listTopicsResult = listTopicsResponse.get('ListTopicsResult', {})
        topics = listTopicsResult.get('Topics', {})
        member = topics['member'] if topics else []
        assert len(member) == 3
    finally:
        # Cleanup created topics
        topic_conf.del_config(topic_arn1)
        topic_conf.del_config(topic_arn2)
        topic_conf.del_config(topic_arn3)
        topic_conf.del_config(topic_arn4)
        topic_conf.del_config(topic_arn5)
        topic_conf.del_config(topic_arn6)
        tenant_topic_conf.del_config(tenant_topic_arn1)
        tenant_topic_conf.del_config(tenant_topic_arn2)
        tenant_topic_conf.del_config(tenant_topic_arn3)


@pytest.mark.basic_test
def test_list_topics():
    """ test list topics"""
    
    # Initialize connections, topic names and configurations
    conn1 = connection()
    tenant = 'kaboom1'
    conn2 = connect_random_user(tenant)
    bucket_name = gen_bucket_name()
    topic_name1 = bucket_name + TOPIC_SUFFIX + '1'
    topic_name2 = bucket_name + TOPIC_SUFFIX + '2'
    topic_name3 = bucket_name + TOPIC_SUFFIX + '3'
    tenant_topic_name1 = tenant + "_" + topic_name1
    tenant_topic_name2 = tenant + "_" + topic_name2
    host = get_ip()
    http_port = random.randint(10000, 20000)
    endpoint_address = 'http://' + host + ':' + str(http_port)
    endpoint_args = 'push-endpoint=' + endpoint_address + '&persistent=true'
    zonegroup = get_config_zonegroup()
    
    # Make sure there are no leftover topics
    delete_all_topics(conn1, '', get_config_cluster())
    delete_all_topics(conn2, tenant, get_config_cluster())
    
    # Create - v2 topics
    topic_conf = PSTopicS3(conn1, topic_name1, zonegroup, endpoint_args=endpoint_args)
    topic_arn1 = topic_conf.set_config()
    topic_conf = PSTopicS3(conn1, topic_name2, zonegroup, endpoint_args=endpoint_args)
    topic_arn2 = topic_conf.set_config()
    topic_conf = PSTopicS3(conn1, topic_name3, zonegroup, endpoint_args=endpoint_args)
    topic_arn3 = topic_conf.set_config()
    tenant_topic_conf = PSTopicS3(conn2, tenant_topic_name1, zonegroup, endpoint_args=endpoint_args)
    tenant_topic_arn1 = tenant_topic_conf.set_config()
    tenant_topic_conf = PSTopicS3(conn2, tenant_topic_name2, zonegroup, endpoint_args=endpoint_args)
    tenant_topic_arn2 = tenant_topic_conf.set_config()
    
    # Verify topics list
    try:
        # Verify no tenant topics
        res, status = topic_conf.get_list()
        assert status // 100 == 2
        listTopicsResponse = res.get('ListTopicsResponse', {})
        listTopicsResult = listTopicsResponse.get('ListTopicsResult', {})
        topics = listTopicsResult.get('Topics', {})
        member = topics['member'] if topics else [] # version 2
        assert len(member) == 3
        	
        # Verify topics for tenant
        res, status = tenant_topic_conf.get_list()
        assert status // 100 == 2
        listTopicsResponse = res.get('ListTopicsResponse', {})
        listTopicsResult = listTopicsResponse.get('ListTopicsResult', {})
        topics = listTopicsResult.get('Topics', {})
        member = topics['member'] if topics else []
        assert len(member) == 2
    finally:
        # Cleanup created topics
        topic_conf.del_config(topic_arn1)
        topic_conf.del_config(topic_arn2)
        topic_conf.del_config(topic_arn3)
        tenant_topic_conf.del_config(tenant_topic_arn1)
        tenant_topic_conf.del_config(tenant_topic_arn2)

@pytest.mark.basic_test
def test_list_topics_v1():
    """ test list topics on v1"""
    if get_config_cluster() == 'noname':
        pytest.skip('realm is needed')
    
    # Initialize connections and configurations
    conn1 = connection()
    tenant = 'kaboom1'
    conn2 = connect_random_user(tenant)
    bucket_name = gen_bucket_name()
    topic_name1 = bucket_name + TOPIC_SUFFIX + '1'
    topic_name2 = bucket_name + TOPIC_SUFFIX + '2'
    topic_name3 = bucket_name + TOPIC_SUFFIX + '3'
    tenant_topic_name1 = tenant + "_" + topic_name1
    tenant_topic_name2 = tenant + "_" + topic_name2
    host = get_ip()
    http_port = random.randint(10000, 20000)
    endpoint_address = 'http://' + host + ':' + str(http_port)
    endpoint_args = 'push-endpoint=' + endpoint_address + '&persistent=true'
    zonegroup = get_config_zonegroup()
    conf_cluster = get_config_cluster()
    
    # Make sure there are no leftover topics
    delete_all_topics(conn1, '', conf_cluster)
    delete_all_topics(conn2, tenant, conf_cluster)
    
    # Make sure that we disable v2
    zonegroup_modify_feature(enable=False, feature_name=zonegroup_feature_notification_v2)
    
    # Create - v1 topics
    topic_conf = PSTopicS3(conn1, topic_name1, zonegroup, endpoint_args=endpoint_args)
    topic_arn1 = topic_conf.set_config()
    topic_conf = PSTopicS3(conn1, topic_name2, zonegroup, endpoint_args=endpoint_args)
    topic_arn2 = topic_conf.set_config()
    topic_conf = PSTopicS3(conn1, topic_name3, zonegroup, endpoint_args=endpoint_args)
    topic_arn3 = topic_conf.set_config()
    tenant_topic_conf = PSTopicS3(conn2, tenant_topic_name1, zonegroup, endpoint_args=endpoint_args)
    tenant_topic_arn1 = tenant_topic_conf.set_config()
    tenant_topic_conf = PSTopicS3(conn2, tenant_topic_name2, zonegroup, endpoint_args=endpoint_args)
    tenant_topic_arn2 = tenant_topic_conf.set_config()
    
    # Verify topics list
    try:
        # Verify no tenant topics
        res, status = topic_conf.get_list()
        assert status // 100 == 2
        listTopicsResponse = res.get('ListTopicsResponse', {})
        listTopicsResult = listTopicsResponse.get('ListTopicsResult', {})
        topics = listTopicsResult.get('Topics', {})
        member = topics['member'] if topics else []
        assert len(member) == 3
        
        # Verify tenant topics
        res, status = tenant_topic_conf.get_list()
        assert status // 100 == 2
        listTopicsResponse = res.get('ListTopicsResponse', {})
        listTopicsResult = listTopicsResponse.get('ListTopicsResult', {})
        topics = listTopicsResult.get('Topics', {})
        member = topics['member'] if topics else []
        assert len(member) == 2
    finally:
        # Cleanup created topics
        topic_conf.del_config(topic_arn1)
        topic_conf.del_config(topic_arn2)
        topic_conf.del_config(topic_arn3)
        tenant_topic_conf.del_config(tenant_topic_arn1)
        tenant_topic_conf.del_config(tenant_topic_arn2)


def topic_permissions(another_tenant=""):
    """ test topic set/get/delete permissions """
    conn1 = connection()
    conn2, arn2 = another_user(tenant=another_tenant)
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
    # create topic with DENY policy
    endpoint_address = 'amqp://127.0.0.1:7001'
    endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=amqp.direct&amqp-ack-level=none'
    topic_conf = PSTopicS3(conn1, topic_name, zonegroup, endpoint_args=endpoint_args, policy_text=topic_policy)
    topic_arn = topic_conf.set_config()

    topic_conf2 = PSTopicS3(conn2, topic_name, zonegroup, endpoint_args=endpoint_args)
    # only on the same tenant we can try to override the topic
    if another_tenant == "":
        try:
            # 2nd user tries to override the topic
            topic_arn = topic_conf2.set_config()
            assert False, "'AuthorizationError' error is expected"
        except ClientError as err:
            if 'Error' in err.response:
                assert err.response['Error']['Code'] == 'AuthorizationError'
            else:
                assert err.response['Code'] == 'AuthorizationError'
        except Exception as err:
            print('unexpected error type: '+type(err).__name__)
            assert False, "'AuthorizationError' error is expected"

    # 2nd user tries to fetch the topic
    _, status = topic_conf2.get_config(topic_arn=topic_arn)
    assert status == 403

    try:
        # 2nd user tries to set the attribute
        status = topic_conf2.set_attributes(attribute_name="persistent", attribute_val="false", topic_arn=topic_arn)
        assert False, "'AuthorizationError' error is expected"
    except ClientError as err:
        if 'Error' in err.response:
            assert err.response['Error']['Code'] == 'AuthorizationError'
        else:
            assert err.response['Code'] == 'AuthorizationError'
    except Exception as err:
        print('unexpected error type: '+type(err).__name__)
        assert False, "'AuthorizationError' error is expected"

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
            assert err.response['Error']['Code'] == 'AccessDenied'
        else:
            assert err.response['Code'] == 'AccessDenied'
    except Exception as err:
        print('unexpected error type: '+type(err).__name__)
        assert False, "'AuthorizationError' error is expected"

    try:
        # 2nd user tries to delete the topic
        status = topic_conf2.del_config(topic_arn=topic_arn)
        assert False, "'AuthorizationError' error is expected"
    except ClientError as err:
        if 'Error' in err.response:
            assert err.response['Error']['Code'] == 'AuthorizationError'
        else:
            assert err.response['Code'] == 'AuthorizationError'
    except Exception as err:
        print('unexpected error type: '+type(err).__name__)
        assert False, "'AuthorizationError' error is expected"

    # Topic policy is now added by the 1st user to allow 2nd user.
    topic_policy = topic_policy.replace("Deny", "Allow")
    topic_conf = PSTopicS3(conn1, topic_name, zonegroup, endpoint_args=endpoint_args, policy_text=topic_policy)
    topic_arn = topic_conf.set_config()
    # 2nd user try to fetch topic again
    _, status = topic_conf2.get_config(topic_arn=topic_arn)
    assert status == 200
    # 2nd user tries to set the attribute again
    status = topic_conf2.set_attributes(attribute_name="persistent", attribute_val="false", topic_arn=topic_arn)
    assert status == 200
    # 2nd user tries to publish notification again
    s3_notification_conf2 = PSNotificationS3(conn2, bucket_name, topic_conf_list)
    _, status = s3_notification_conf2.set_config()
    assert status == 200
    # 2nd user tries to delete the topic again
    status = topic_conf2.del_config(topic_arn=topic_arn)
    assert status == 200

    # cleanup
    s3_notification_conf2.del_config()
    # delete the bucket
    conn2.delete_bucket(bucket_name)


@pytest.mark.basic_test
def test_topic_permissions_same_tenant():
    topic_permissions()


@pytest.mark.basic_test
def test_topic_permissions_cross_tenant():
    topic_permissions(another_tenant="boom")


@pytest.mark.basic_test
def test_topic_no_permissions():
    """ test topic set/get/delete permissions """
    conn1 = connection()
    conn2, _ = another_user()
    zonegroup = 'default'
    bucket_name = gen_bucket_name()
    topic_name = bucket_name + TOPIC_SUFFIX

    # create topic without policy
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
            assert err.response['Error']['Code'] == 'AuthorizationError'
        else:
            assert err.response['Code'] == 'AuthorizationError'
    except Exception as err:
        print('unexpected error type: '+type(err).__name__)

    # 2nd user tries to fetch the topic
    _, status = topic_conf2.get_config(topic_arn=topic_arn)
    assert status == 403

    try:
        # 2nd user tries to set the attribute
        status = topic_conf2.set_attributes(attribute_name="persistent", attribute_val="false", topic_arn=topic_arn)
        assert False, "'AuthorizationError' error is expected"
    except ClientError as err:
        if 'Error' in err.response:
            assert err.response['Error']['Code'] == 'AuthorizationError'
        else:
            assert err.response['Code'] == 'AuthorizationError'
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
    assert status == 200

    try:
        # 2nd user tries to delete the topic
        status = topic_conf2.del_config(topic_arn=topic_arn)
        assert False, "'AuthorizationError' error is expected"
    except ClientError as err:
        if 'Error' in err.response:
            assert err.response['Error']['Code'] == 'AuthorizationError'
        else:
            assert err.response['Code'] == 'AuthorizationError'
    except Exception as err:
        print('unexpected error type: '+type(err).__name__)

    # cleanup
    s3_notification_conf2.del_config()
    topic_conf.del_config()
    # delete the bucket
    conn2.delete_bucket(bucket_name)


def kafka_security(security_type, mechanism='PLAIN', use_topic_attrs_for_creds=False):
    """ test pushing kafka notification securly to master """
    # Setup SCRAM users if needed
    if mechanism.startswith('SCRAM'):
        setup_scram_users_via_kafka_configs(mechanism)
        time.sleep(2)  # Allow time for SCRAM config to propagate
    
    conn = connection()
    zonegroup = get_config_zonegroup()
    # create bucket
    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(bucket_name)
    # name is constant for manual testing
    topic_name = bucket_name+'_topic'
    # create topic
    if security_type == 'SASL_SSL':
        if not use_topic_attrs_for_creds:
            endpoint_address = 'kafka://alice:alice-secret@' + default_kafka_server + ':9094'
        else:
            endpoint_address = 'kafka://' + default_kafka_server + ':9094'
    elif security_type == 'SSL':
        endpoint_address = 'kafka://' + default_kafka_server + ':9093'
    elif security_type == 'SASL_PLAINTEXT':
        endpoint_address = 'kafka://alice:alice-secret@' + default_kafka_server + ':9095'
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
    task, receiver = create_kafka_receiver_thread(topic_name, security_type=security_type, mechanism=mechanism)
    task.start()
    verify_kafka_receiver(receiver)

    topic_arn = topic_conf.set_config()
    # create notification
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


@pytest.mark.kafka_security_test
def test_notification_kafka_security_ssl():
    kafka_security('SSL')


@pytest.mark.kafka_security_test
def test_notification_kafka_security_ssl_sasl():
    kafka_security('SASL_SSL')


@pytest.mark.kafka_security_test
def test_notification_kafka_security_ssl_sasl_attrs():
    kafka_security('SASL_SSL', use_topic_attrs_for_creds=True)


@pytest.mark.kafka_security_test
def test_notification_kafka_security_sasl():
    kafka_security('SASL_PLAINTEXT')


@pytest.mark.kafka_security_test
def test_notification_kafka_security_ssl_sasl_scram():
    kafka_security('SASL_SSL', mechanism='SCRAM-SHA-256')


@pytest.mark.kafka_security_test
def test_notification_kafka_security_sasl_scram():
    kafka_security('SASL_PLAINTEXT', mechanism='SCRAM-SHA-256')


@pytest.mark.kafka_security_test
def test_notification_kafka_security_sasl_scram_512():
    kafka_security('SASL_PLAINTEXT', mechanism='SCRAM-SHA-512')


@pytest.mark.kafka_security_test
def test_notification_kafka_security_ssl_sasl_scram_512():
    kafka_security('SASL_SSL', mechanism='SCRAM-SHA-512')


@pytest.mark.http_test
def test_persistent_reload():
    """ do a realm reload while we send notifications """
    if get_config_cluster() == 'noname':
        pytest.skip('realm is needed for reload test')

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

    # create topics
    endpoint_address = 'http://'+host+':'+str(http_port)
    endpoint_args = 'push-endpoint='+endpoint_address+'&persistent=true'+ \
            '&retry_sleep_duration=1'
    topic_conf1 = PSTopicS3(conn, topic_name1, zonegroup, endpoint_args=endpoint_args)
    topic_arn1 = topic_conf1.set_config()
    # 2nd topic is unused
    topic_name2 = bucket_name + TOPIC_SUFFIX + '_2'
    topic_conf2 = PSTopicS3(conn, topic_name2, zonegroup, endpoint_args=endpoint_args)
    topic_arn2 = topic_conf2.set_config()

    # create notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name, 'TopicArn': topic_arn1,
                        'Events': []
                        }]

    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert status/100 == 2

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


def poll_on_topic(topic_name, tenant=''):
    remaining_retries = 10
    start_time = time.time()
    while True:
        result = remove_topic(topic_name, tenant, allow_failure=True)
        time_diff = time.time() - start_time
        if result == 0:
            log.info('migration took %d seconds', time_diff)
            return
        elif result == 154:
            if remaining_retries == 0:
                assert False, 'migration did not end after %d seconds' % time_diff
            remaining_retries -= 1
            log.info('migration in process. remaining retries: %d', remaining_retries)
            time.sleep(2)
        else:
            assert False, 'unexpected error (%d) trying to remove topic when waiting for migration to end' % result

def persistent_data_path_v2_migration(conn, endpoint_type):
    """ test data path v2 persistent migration """
    if get_config_cluster() == 'noname':
        pytest.skip('realm is needed for migration test')
    zonegroup = get_config_zonegroup()

    # disable v2 notification
    zonegroup_modify_feature(enable=False, feature_name=zonegroup_feature_notification_v2)

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
        endpoint_args = 'push-endpoint='+endpoint_address+'&persistent=true'+ \
                        '&retry_sleep_duration=1'
    elif endpoint_type == 'amqp':
        # start amqp receiver
        exchange = 'ex1'
        task, receiver = create_amqp_receiver_thread(exchange, topic_name)
        task.start()
        endpoint_address = 'amqp://' + host
        endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange='+exchange+'&amqp-ack-level=broker&persistent=true'+ \
                        '&retry_sleep_duration=1'
    elif endpoint_type == 'kafka':
        # start kafka receiver
        task, receiver = create_kafka_receiver_thread(topic_name)
        task.start()
        verify_kafka_receiver(receiver)
        endpoint_address = 'kafka://' + host
        endpoint_args = 'push-endpoint='+endpoint_address+'&kafka-ack-level=broker&persistent=true'+ \
                        '&retry_sleep_duration=1'
    else:
        pytest.skip('Unknown endpoint type: ' + endpoint_type)

    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()
    # create notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name, 'TopicArn': topic_arn,
                        'Events': []
                        }]

    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert status/100 == 2

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
        poll_on_topic(topic_name_1)

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

        wait_for_queue_to_drain(topic_name)
        # verify events
        keys = list(bucket.list())
        # exact match is false because the notifications are persistent.
        receiver.verify_s3_events(keys, exact_match=False)

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
        receiver.close(task)


@pytest.mark.http_test
def persistent_data_path_v2_migration_http():
    """ test data path v2 persistent migration, http endpoint """
    conn = connection()
    persistent_data_path_v2_migration(conn, 'http')


@pytest.mark.kafka_test
def persistent_data_path_v2_migration_kafka():
    """ test data path v2 persistent migration, kafka endpoint """
    conn = connection()
    persistent_data_path_v2_migration(conn, 'kafka')


@pytest.mark.http_test
def test_data_path_v2_migration():
    """ test data path v2 migration """
    if get_config_cluster() == 'noname':
        pytest.skip('realm is needed for migration test')
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

    # create topic
    endpoint_address = 'http://'+host+':'+str(http_port)
    endpoint_args = 'push-endpoint='+endpoint_address
    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()
    # create notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name, 'TopicArn': topic_arn,
                        'Events': []
                        }]

    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert status/100 == 2

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

    # verify events
    keys = list(bucket.list())
    http_server.verify_s3_events(keys, exact_match=True)

    # create topic to poll on
    topic_name_1 = topic_name + '_1'
    topic_conf_1 = PSTopicS3(conn, topic_name_1, zonegroup, endpoint_args=endpoint_args)

    # enable v2 notification
    zonegroup_modify_feature(enable=True, feature_name=zonegroup_feature_notification_v2)

    # poll on topic_1
    poll_on_topic(topic_name_1)

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


@pytest.mark.basic_test
def test_data_path_v2_large_migration():
    """ test data path v2 large migration """
    if get_config_cluster() == 'noname':
        pytest.skip('realm is needed for migration test')
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

    # create topic
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
        # create topic
        endpoint_address = 'http://' + host + ':' + str(http_port)
        endpoint_args = 'push-endpoint=' + endpoint_address + '&persistent=true'
        topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
        topics_conf_list.append(topic_conf)
        topic_arn = topic_conf.set_config()
        # create 110 notifications
        s3_notification_list = []
        for i in range(num_of_s3_notifications):
            notification_name = bucket_name + NOTIFICATION_SUFFIX + '_' + str(i + 1)
            s3_notification_list.append({'Id': notification_name, 'TopicArn': topic_arn,
                                    'Events': []
                                    })

        s3_notification_conf = PSNotificationS3(conn, bucket_name, s3_notification_list)
        s3_notification_conf_list.append(s3_notification_conf)
        response, status = s3_notification_conf.set_config()
        assert status / 100 == 2

    # create topic to poll on
    polling_topics_conf = []
    for conn, bucket in zip(connections_list, buckets_list):
        topic_name = bucket.name + TOPIC_SUFFIX + '_1'
        topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
        polling_topics_conf.append(topic_conf)

    # enable v2 notification
    zonegroup_modify_feature(enable=True, feature_name=zonegroup_feature_notification_v2)

    for tenant in tenants_list:
        list_topics(1, tenant)

    # poll on topic
    for tenant, topic_conf in zip(tenants_list, polling_topics_conf):
        poll_on_topic(topic_conf.topic_name, tenant)

    # check if we migrated all the topics
    for tenant in tenants_list:
        list_topics(1, tenant)

    # check if we migrated all the notifications
    for tenant, bucket in zip(tenants_list, buckets_list):
        list_notifications(bucket.name, num_of_s3_notifications, tenant)

    # cleanup
    for s3_notification_conf in s3_notification_conf_list:
        s3_notification_conf.del_config()
    for topic_conf in topics_conf_list:
        topic_conf.del_config()
    # delete the bucket
    for conn, bucket in zip(connections_list, buckets_list):
        conn.delete_bucket(bucket.name)


@pytest.mark.basic_test
def test_data_path_v2_mixed_migration():
    """ test data path v2 mixed migration """
    if get_config_cluster() == 'noname':
        pytest.skip('realm is needed for migration test')
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

    # create topic
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
        # create topic
        endpoint_address = 'http://' + host + ':' + str(http_port)
        endpoint_args = 'push-endpoint=' + endpoint_address + '&persistent=true'
        topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
        topics_conf_list.append(topic_conf)
        topic_arn = topic_conf.set_config()
        topic_arn_list.append(topic_arn)
        # create notification
        notification_name = bucket_name + NOTIFICATION_SUFFIX + created_version
        s3_notification_list = [{'Id': notification_name, 'TopicArn': topic_arn,
                                 'Events': []
                                 }]

        s3_notification_conf = PSNotificationS3(conn, bucket_name, s3_notification_list)
        s3_notification_conf_list.append(s3_notification_conf)
        response, status = s3_notification_conf.set_config()
        assert status / 100 == 2

    # disable v2 notification
    zonegroup_modify_feature(enable=False, feature_name=zonegroup_feature_notification_v2)

    # create topic
    created_version = '_created_v1'
    for conn, bucket in zip(connections_list, buckets_list):
        # create bucket
        bucket_name = bucket.name
        topic_name = bucket_name + TOPIC_SUFFIX + created_version
        # create topic
        endpoint_address = 'http://' + host + ':' + str(http_port)
        endpoint_args = 'push-endpoint=' + endpoint_address + '&persistent=true'
        topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
        topics_conf_list.append(topic_conf)
        topic_arn = topic_conf.set_config()
        # create notification
        notification_name = bucket_name + NOTIFICATION_SUFFIX + created_version
        s3_notification_list = [{'Id': notification_name, 'TopicArn': topic_arn,
                                 'Events': []
                                 }]

        s3_notification_conf = PSNotificationS3(conn, bucket_name, s3_notification_list)
        s3_notification_conf_list.append(s3_notification_conf)
        response, status = s3_notification_conf.set_config()
        assert status / 100 == 2

    # create topic to poll on
    polling_topics_conf = []
    for conn, bucket in zip(connections_list, buckets_list):
        topic_name = bucket.name + TOPIC_SUFFIX + '_1'
        topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
        polling_topics_conf.append(topic_conf)

    # enable v2 notification
    zonegroup_modify_feature(enable=True, feature_name=zonegroup_feature_notification_v2)

    # poll on topic
    for tenant, topic_conf in zip(tenants_list, polling_topics_conf):
        poll_on_topic(topic_conf.topic_name, tenant)

    # check if we migrated all the topics
    for tenant in tenants_list:
        list_topics(2, tenant)

    # check if we migrated all the notifications
    for tenant, bucket in zip(tenants_list, buckets_list):
        list_notifications(bucket.name, 2, tenant)

    # cleanup
    for s3_notification_conf in s3_notification_conf_list:
        s3_notification_conf.del_config()
    for topic_conf in topics_conf_list:
        topic_conf.del_config()
    # delete the bucket
    for conn, bucket in zip(connections_list, buckets_list):
        conn.delete_bucket(bucket.name)


@pytest.mark.kafka_test
def test_notification_caching():
    """ test notification caching """
    conn = connection()
    # create bucket
    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX

    # start kafka receiver
    task, receiver = create_kafka_receiver_thread(topic_name)
    task.start()
    verify_kafka_receiver(receiver)
    incorrect_port = 9999
    endpoint_address = 'kafka://' + default_kafka_server + ':' + str(incorrect_port)
    endpoint_args = 'push-endpoint=' + endpoint_address + '&kafka-ack-level=broker' + '&persistent=true'

    # create topic
    zonegroup = get_config_zonegroup()
    topic_conf = PSTopicS3(conn, topic_name, zonegroup,
                           endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()
    # create notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name, 'TopicArn': topic_arn,
                        'Events': []
                        }]

    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert status / 100 == 2

    # create objects in the bucket (async)
    number_of_objects = 10
    client_threads = []
    start_time = time.time()
    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        content = str(os.urandom(1024 * 1024))
        thr = threading.Thread(target=set_contents_from_string,
                               args=(key, content,))
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]

    time_diff = time.time() - start_time
    print('average time for creation + async notification is: ' + str(
      time_diff * 1000 / number_of_objects) + ' milliseconds')

    # delete objects from the bucket
    client_threads = []
    start_time = time.time()
    for key in bucket.list():
        thr = threading.Thread(target=key.delete, args=())
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]

    time_diff = time.time() - start_time
    print('average time for deletion + async notification is: ' + str(
      time_diff * 1000 / number_of_objects) + ' milliseconds')

    time.sleep(30)
    # topic stats
    result = admin(['topic', 'stats', '--topic', topic_name],
                   get_config_cluster())
    assert result[1] == 0
    parsed_result = json.loads(result[0])
    assert parsed_result['Topic Stats']['Entries'] == 2 * number_of_objects

    # remove the port and update the topic, so its pointing to correct endpoint.
    endpoint_address = 'kafka://' + default_kafka_server
    # update topic
    topic_conf.set_attributes(attribute_name="push-endpoint",
                              attribute_val=endpoint_address)
    keys = list(bucket.list())
    wait_for_queue_to_drain(topic_name)
    receiver.verify_s3_events(keys, deletions=True)

    # cleanup
    s3_notification_conf.del_config()
    topic_conf.del_config()
    # delete the bucket
    conn.delete_bucket(bucket_name)
    receiver.close(task)


@pytest.mark.kafka_test
def test_connection_caching():
    """ test connection caching """
    conn = connection()
    # create bucket
    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(bucket_name)
    topic_name_1 = bucket_name + TOPIC_SUFFIX + "-without-ssl"
    topic_name_2 = bucket_name + TOPIC_SUFFIX + "-with-ssl"

    # start kafka receiver
    task_1, receiver_1 = create_kafka_receiver_thread(topic_name_1)
    task_1.start()
    verify_kafka_receiver(receiver_1)
    task_2, receiver_2 = create_kafka_receiver_thread(topic_name_2)
    task_2.start()
    verify_kafka_receiver(receiver_2)
    endpoint_address = 'kafka://' + default_kafka_server
    endpoint_args = 'push-endpoint=' + endpoint_address + '&kafka-ack-level=broker&use-ssl=true' + '&persistent=true'

    # initially create both topics with `use-ssl=true`
    zonegroup = get_config_zonegroup()
    topic_conf_1 = PSTopicS3(conn, topic_name_1, zonegroup,
                             endpoint_args=endpoint_args)
    topic_arn_1 = topic_conf_1.set_config()
    topic_conf_2 = PSTopicS3(conn, topic_name_2, zonegroup,
                             endpoint_args=endpoint_args)
    topic_arn_2 = topic_conf_2.set_config()
    # create notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name + '_1', 'TopicArn': topic_arn_1,
                        'Events': []
                        },
                       {'Id': notification_name + '_2', 'TopicArn': topic_arn_2,
                        'Events': []}]

    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert status / 100 == 2

    # create objects in the bucket (async)
    number_of_objects = 10
    client_threads = []
    start_time = time.time()
    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        content = str(os.urandom(1024))
        thr = threading.Thread(target=set_contents_from_string,
                               args=(key, content,))
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]

    time_diff = time.time() - start_time
    print('average time for creation + async notification is: ' + str(
        time_diff * 1000 / number_of_objects) + ' milliseconds')

    # delete objects from the bucket
    client_threads = []
    start_time = time.time()
    for key in bucket.list():
        thr = threading.Thread(target=key.delete, args=())
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]

    time_diff = time.time() - start_time
    print('average time for deletion + async notification is: ' + str(
        time_diff * 1000 / number_of_objects) + ' milliseconds')

    time.sleep(30)
    # topic stats
    result = admin(['topic', 'stats', '--topic', topic_name_1],
                   get_config_cluster())
    assert result[1] == 0
    parsed_result = json.loads(result[0])
    assert parsed_result['Topic Stats']['Entries'] == 2 * number_of_objects

    # remove the ssl from topic1 and update the topic.
    endpoint_address = 'kafka://' + default_kafka_server
    topic_conf_1.set_attributes(attribute_name="use-ssl",
                                attribute_val="false")
    keys = list(bucket.list())
    wait_for_queue_to_drain(topic_name_1)
    receiver_1.verify_s3_events(keys, deletions=True)
    # topic stats for 2nd topic will still reflect entries
    result = admin(['topic', 'stats', '--topic', topic_name_2],
                   get_config_cluster())
    assert result[1] == 0
    parsed_result = json.loads(result[0])
    assert parsed_result['Topic Stats']['Entries'] == 2 * number_of_objects

    # cleanup
    s3_notification_conf.del_config()
    topic_conf_1.del_config()
    topic_conf_2.del_config()
    # delete the bucket
    conn.delete_bucket(bucket_name)
    receiver_1.close(task_1)
    receiver_2.close(task_2)


@pytest.mark.http_test
def test_topic_migration_to_an_account():
    """test the topic migration procedure described at
    https://docs.ceph.com/en/latest/radosgw/account/#migrate-an-existing-user-into-an-account
    """
    # Initialize variables for cleanup in finally block
    user1_bucket_name = None
    user2_bucket_name = None
    user1_id = None
    account_id = None

    try:
        # create an http server for notification delivery
        host = get_ip()
        port = random.randint(10000, 20000)
        http_server = HTTPServerWithEvents((host, port))

        # start with two non-account users
        # create a new user for "user1" which is going to be migrated to an account
        user1_conn, user1_arn = another_user()
        user1_id = user1_arn.split("/")[1]
        user2_conn = connection()
        log.info(
            f"two non-account users with acckeys user1 {user1_conn.access_key} and user2 {user2_conn.access_key}"
        )

        # create a bucket per user
        user1_bucket_name = gen_bucket_name()
        user2_bucket_name = gen_bucket_name()
        user1_bucket = user1_conn.create_bucket(user1_bucket_name)
        user2_bucket = user2_conn.create_bucket(user2_bucket_name)
        log.info(
            f"one bucket per user {user1_conn.access_key}: {user1_bucket_name} and {user2_conn.access_key}: {user2_bucket_name}"
        )

        # create an topic owned by the first user
        topic_name = user1_bucket_name + TOPIC_SUFFIX
        zonegroup = get_config_zonegroup()
        endpoint_address = "http://" + host + ":" + str(port)
        endpoint_args = "push-endpoint=" + endpoint_address + "&persistent=true"
        expected_topic_arn = "arn:aws:sns:" + zonegroup + "::" + topic_name
        topic_conf = PSTopicS3(
            user1_conn, topic_name, zonegroup, endpoint_args=endpoint_args
        )
        topic_arn = topic_conf.set_config()
        assert topic_arn == expected_topic_arn
        log.info(
            f"{user1_conn.access_key} created the topic {topic_arn} with args {endpoint_args}"
        )

        # both buckets subscribe to the same and only topic using the same notification id
        notification_name = user1_bucket_name + NOTIFICATION_SUFFIX
        topic_conf_list = [
            {
                "Id": notification_name,
                "TopicArn": topic_arn,
                "Events": ["s3:ObjectCreated:*"],
            }
        ]
        s3_notification_conf1 = PSNotificationS3(
            user1_conn, user1_bucket_name, topic_conf_list
        )
        s3_notification_conf2 = PSNotificationS3(
            user2_conn, user2_bucket_name, topic_conf_list
        )
        _, status = s3_notification_conf1.set_config()
        assert status / 100 == 2
        _, status = s3_notification_conf2.set_config()
        assert status / 100 == 2
        # verify both buckets are subscribed to the topic
        rgw_topic_entry = [
            t for t in list_topics() if t["name"] == topic_name
        ]
        assert len(rgw_topic_entry) == 1
        subscribed_buckets = rgw_topic_entry[0]["subscribed_buckets"]
        assert len(subscribed_buckets) == 2
        assert user1_bucket_name in subscribed_buckets
        assert user2_bucket_name in subscribed_buckets
        log.info(
            "buckets {user1_bucket_name} and {user2_bucket_name} are subscribed to {topic_arn}"
        )

        # move user1 to an account
        account_id = "RGW98765432101234567"
        cmd = ["account", "create", "--account-id", account_id]
        _, rc = admin(cmd, get_config_cluster())
        assert rc == 0, f"failed to create {account_id}: {rc}"
        cmd = [
            "user",
            "modify",
            "--uid",
            user1_id,
            "--account-id",
            account_id,
            "--account-root",
        ]
        _, rc = admin(cmd, get_config_cluster())
        assert rc == 0, f"failed to modify {user1_id}: {rc}"
        log.info(
            f"{user1_conn.access_key}/{user1_id} is migrated to account {account_id} as root user"
        )

        # verify the topic is functional
        user1_bucket.new_key("user1obj1").set_contents_from_string("object content")
        user2_bucket.new_key("user2obj1").set_contents_from_string("object content")
        wait_for_queue_to_drain(topic_name, http_port=port)
        http_server.verify_s3_events(
            list(user1_bucket.list()) + list(user2_bucket.list()), exact_match=True
        )

        # create a new account topic with the same name as the existing topic
        # note that the expected arn now contains the account ID
        expected_topic_arn = (
            "arn:aws:sns:" + zonegroup + ":" + account_id + ":" + topic_name
        )
        topic_conf = PSTopicS3(
            user1_conn, topic_name, zonegroup, endpoint_args=endpoint_args
        )
        account_topic_arn = topic_conf.set_config()
        assert account_topic_arn == expected_topic_arn
        log.info(
            f"{user1_conn.access_key} created the account topic {account_topic_arn} with args {endpoint_args}"
        )

        # verify that the old topic is still functional
        user1_bucket.new_key("user1obj1").set_contents_from_string("object content")
        user2_bucket.new_key("user2obj1").set_contents_from_string("object content")
        wait_for_queue_to_drain(topic_name, http_port=port)
        # wait_for_queue_to_drain(topic_name, tenant=account_id, http_port=port)
        http_server.verify_s3_events(
            list(user1_bucket.list()) + list(user2_bucket.list()), exact_match=True
        )

        # change user1 bucket's subscription to the account topic - using the same notification ID but with the new account_topic_arn
        topic_conf_list = [
            {
                "Id": notification_name,
                "TopicArn": account_topic_arn,
                "Events": ["s3:ObjectCreated:*"],
            }
        ]
        s3_notification_conf1 = PSNotificationS3(
            user1_conn, user1_bucket_name, topic_conf_list
        )
        _, status = s3_notification_conf1.set_config()
        assert status / 100 == 2
        # verify subscriptions to the account topic
        rgw_topic_entry = [
            t
            for t in list_topics(tenant=account_id)
            if t["name"] == topic_name
        ]
        assert len(rgw_topic_entry) == 1
        subscribed_buckets = rgw_topic_entry[0]["subscribed_buckets"]
        assert len(subscribed_buckets) == 1
        assert user1_bucket_name in subscribed_buckets
        assert user2_bucket_name not in subscribed_buckets
        # verify bucket notifications while 2 test buckets are in the mixed mode
        notification_list = list_notifications(user1_bucket_name, assert_len=1)
        assert notification_list["notifications"][0]["TopicArn"] == account_topic_arn
        notification_list = list_notifications(user2_bucket_name, assert_len=1)
        assert notification_list["notifications"][0]["TopicArn"] == topic_arn

        # verify both topics are functional at the same time with no duplicate notifications
        user1_bucket.new_key("user1obj1").set_contents_from_string("object content")
        user2_bucket.new_key("user2obj1").set_contents_from_string("object content")
        wait_for_queue_to_drain(topic_name, http_port=port)
        wait_for_queue_to_drain(topic_name, tenant=account_id, http_port=port)
        http_server.verify_s3_events(
            list(user1_bucket.list()) + list(user2_bucket.list()), exact_match=True
        )

        # also change user2 bucket's subscription to the account topic
        s3_notification_conf2 = PSNotificationS3(
            user2_conn, user2_bucket_name, topic_conf_list
        )
        _, status = s3_notification_conf2.set_config()
        assert status / 100 == 2
        # remove old topic
        # note that, although account topic has the same name, it has to be scoped by account/tenant id to be removed
        # so below command will only remove the old topic
        _, rc = admin(["topic", "rm", "--topic", topic_name], get_config_cluster())
        assert rc == 0

        # now verify account topic serves both buckets
        rgw_topic_entry = [
            t
            for t in list_topics(tenant=account_id)
            if t["name"] == topic_name
        ]
        assert len(rgw_topic_entry) == 1
        subscribed_buckets = rgw_topic_entry[0]["subscribed_buckets"]
        assert len(subscribed_buckets) == 2
        assert user1_bucket_name in subscribed_buckets
        assert user2_bucket_name in subscribed_buckets
        # verify bucket notifications after 2 test buckets are updated to use the account topic
        notification_list = list_notifications(user1_bucket_name, assert_len=1)
        assert notification_list["notifications"][0]["TopicArn"] == account_topic_arn
        notification_list = list_notifications(user2_bucket_name, assert_len=1)
        assert notification_list["notifications"][0]["TopicArn"] == account_topic_arn

        # finally, make sure that notifications are going thru via the new account topic
        user1_bucket.new_key("user1obj1").set_contents_from_string("object content")
        user2_bucket.new_key("user2obj1").set_contents_from_string("object content")
        wait_for_queue_to_drain(topic_name, tenant=account_id, http_port=port)
        http_server.verify_s3_events(
            list(user1_bucket.list()) + list(user2_bucket.list()), exact_match=True
        )
        log.info("topic migration test has completed successfully")
    finally:
        if user1_id is not None:
            admin(["user", "rm", "--uid", user1_id, "--purge-data"], get_config_cluster())
        if user1_bucket_name is not None:
            admin(
                ["bucket", "rm", "--bucket", user1_bucket_name, "--purge-data"],
                get_config_cluster(),
            )
        if user2_bucket_name is not None:
            admin(
                ["bucket", "rm", "--bucket", user2_bucket_name, "--purge-data"],
                get_config_cluster(),
            )
        if account_id is not None:
            admin(["account", "rm", "--account-id", account_id], get_config_cluster())

def persistent_notification_shard_config_change(endpoint_type, conn, new_num_shards, old_num_shards=11): 
    """ test persistent notification shard config change """
    """ test to check if notifications work when config value for determining num_shards is changed..."""
    
    default_num_shards = 11
    rgw_client = f'client.rgw.{get_config_port()}'
    if (old_num_shards != default_num_shards):
        set_rgw_config_option(rgw_client, 'rgw_bucket_persistent_notif_num_shards', old_num_shards, get_config_cluster())

    bucket_name = gen_bucket_name()
    bucket = conn.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX

    #start receiver thread based on conn type
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
    elif endpoint_type == 'kafka':
        # start kafka receiver
        task, receiver = create_kafka_receiver_thread(topic_name)
        task.start()
        endpoint_address = 'kafka://' + host
        endpoint_args = 'push-endpoint='+endpoint_address+'&kafka-ack-level=broker&persistent=true'
    else:
        pytest.skip('Unknown endpoint type: ' + endpoint_type)

    zonegroup = get_config_zonegroup()
    topic_conf = PSTopicS3(conn, topic_name, zonegroup, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()

    # create notification
    notif_1 = bucket_name +  '_notif_1'
    topic_conf_list = [{'Id': notif_1, 'TopicArn': topic_arn,
        'Events': ['s3:ObjectCreated:*', 's3:ObjectRemoved:*']
    }]

    s3_notification_conf = PSNotificationS3(conn, bucket_name, topic_conf_list)
    _, status = s3_notification_conf.set_config()
    assert status/100 == 2

    ## create objects in the bucket (async)
    expected_keys = []
    create_object_and_verify_events(bucket, 'foo', topic_name, receiver, expected_keys, deletions=True)

    ## change config value for num_shards to new_num_shards
    set_rgw_config_option(rgw_client, 'rgw_bucket_persistent_notif_num_shards', new_num_shards, get_config_cluster())
    
    ## create objects in the bucket (async)
    expected_keys = []
    create_object_and_verify_events(bucket, 'bar', topic_name, receiver, expected_keys, deletions=True)

    # cleanup
    receiver.close(task)
    s3_notification_conf.del_config()
    topic_conf.del_config()
    # delete the bucket
    conn.delete_bucket(bucket_name)

    ##revert config value for num_shards to default
    if (new_num_shards != default_num_shards):
        set_rgw_config_option(rgw_client, 'rgw_bucket_persistent_notif_num_shards', default_num_shards, get_config_cluster())


def create_object_and_verify_events(bucket, key_name, topic_name, receiver, expected_keys, deletions=False):
    key = bucket.new_key(key_name)
    key.set_contents_from_string('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')
    expected_keys.append(key_name)

    print('wait for the messages...')
    wait_for_queue_to_drain(topic_name)
    events = receiver.get_and_reset_events()
    assert len(events) == len(expected_keys)
    for event in events:
        assert(event['Records'][0]['s3']['object']['key'] in expected_keys)

    if deletions:
        # delete objects
        for key in bucket.list():
            key.delete()
        print('wait for the messages...')
        wait_for_queue_to_drain(topic_name)
        # check endpoint receiver
        events = receiver.get_and_reset_events()
        assert len(events) == len(expected_keys)
        for event in events:
            assert(event['Records'][0]['s3']['object']['key'] in expected_keys)

@pytest.mark.http_test
def test_backward_compatibility_persistent_sharded_topic_http(): 
    conn = connection()
    persistent_notification_shard_config_change('http', conn, new_num_shards=11, old_num_shards=1)

@pytest.mark.kafka_test
def test_backward_compatibility_persistent_sharded_topic_kafka(): 
    conn = connection()
    persistent_notification_shard_config_change('kafka', conn, new_num_shards=11, old_num_shards=1)

@pytest.mark.http_test
def test_persistent_sharded_topic_config_change_http():
    conn = connection()
    new_num_shards = random.randint(2, 10)
    default_num_shards = 11
    persistent_notification_shard_config_change('http', conn, new_num_shards, default_num_shards)

@pytest.mark.kafka_test
def test_persistent_sharded_topic_config_change_kafka():
    conn = connection()
    new_num_shards = random.randint(2, 10)
    default_num_shards = 11
    persistent_notification_shard_config_change('kafka', conn, new_num_shards, default_num_shards)
