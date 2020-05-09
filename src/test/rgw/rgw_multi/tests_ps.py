import logging
import json
import tempfile
from six.moves import BaseHTTPServer
import random
import threading
import subprocess
import socket
import time
import os
from random import randint
from .tests import get_realm, \
    ZonegroupConns, \
    zonegroup_meta_checkpoint, \
    zone_meta_checkpoint, \
    zone_bucket_checkpoint, \
    zone_data_checkpoint, \
    zonegroup_bucket_checkpoint, \
    check_bucket_eq, \
    gen_bucket_name, \
    get_user, \
    get_tenant
from .zone_ps import PSTopic, \
    PSTopicS3, \
    PSNotification, \
    PSSubscription, \
    PSNotificationS3, \
    print_connection_info, \
    delete_all_s3_topics, \
    put_object_tagging, \
    get_object_tagging, \
    delete_all_objects
from .multisite import User
from nose import SkipTest
from nose.tools import assert_not_equal, assert_equal
import boto.s3.tagging

# configure logging for the tests module
log = logging.getLogger(__name__)

skip_push_tests = True

####################################
# utility functions for pubsub tests
####################################

def set_contents_from_string(key, content):
    try:
        key.set_contents_from_string(content)
    except Exception as e:
        print('Error: ' + str(e))


# HTTP endpoint functions
# multithreaded streaming server, based on: https://stackoverflow.com/questions/46210672/

class HTTPPostHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    """HTTP POST hanler class storing the received events in its http server"""
    def do_POST(self):
        """implementation of POST handler"""
        try:
            content_length = int(self.headers['Content-Length'])
            body = self.rfile.read(content_length)
            log.info('HTTP Server (%d) received event: %s', self.server.worker_id, str(body))
            self.server.append(json.loads(body))
        except:
            log.error('HTTP Server received empty event')
            self.send_response(400)
        else:
            self.send_response(100)
        finally:
            self.end_headers()


class HTTPServerWithEvents(BaseHTTPServer.HTTPServer):
    """HTTP server used by the handler to store events"""
    def __init__(self, addr, handler, worker_id):
        BaseHTTPServer.HTTPServer.__init__(self, addr, handler, False)
        self.worker_id = worker_id
        self.events = []

    def append(self, event):
        self.events.append(event)


class HTTPServerThread(threading.Thread):
    """thread for running the HTTP server. reusing the same socket for all threads"""
    def __init__(self, i, sock, addr):
        threading.Thread.__init__(self)
        self.i = i
        self.daemon = True
        self.httpd = HTTPServerWithEvents(addr, HTTPPostHandler, i)
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
    def __init__(self, host, port, num_workers=100):
        addr = (host, port)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(addr)
        self.sock.listen(num_workers)
        self.workers = [HTTPServerThread(i, self.sock, addr) for i in range(num_workers)]

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

rabbitmq_port = 5672

class AMQPReceiver(object):
    """class for receiving and storing messages on a topic from the AMQP broker"""
    def __init__(self, exchange, topic):
        import pika
        hostname = get_ip()
        remaining_retries = 10
        while remaining_retries > 0:
            try:
                connection = pika.BlockingConnection(pika.ConnectionParameters(host=hostname, port=rabbitmq_port))
                break
            except Exception as error:
                remaining_retries -= 1
                print('failed to connect to rabbitmq (remaining retries '
                    + str(remaining_retries) + '): ' + str(error))

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
    def verify_s3_events(self, keys, exact_match=False, deletions=False):
        """verify stored s3 records agains a list of keys"""
        verify_s3_records_by_elements(self.events, keys, exact_match=exact_match, deletions=deletions)
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


def create_amqp_receiver_thread(exchange, topic):
    """create amqp receiver and thread"""
    receiver = AMQPReceiver(exchange, topic)
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

def check_ps_configured():
    """check if at least one pubsub zone exist"""
    realm = get_realm()
    zonegroup = realm.master_zonegroup()

    ps_zones = zonegroup.zones_by_type.get("pubsub")
    if not ps_zones:
        raise SkipTest("Requires at least one PS zone")


def is_ps_zone(zone_conn):
    """check if a specific zone is pubsub zone"""
    if not zone_conn:
        return False
    return zone_conn.zone.tier_type() == "pubsub"


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


def verify_s3_records_by_elements(records, keys, exact_match=False, deletions=False, expected_sizes={}):
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
                    if record['s3']['bucket']['name'] == key.bucket.name and \
                        record['s3']['object']['key'] == key.name:
                        if deletions and 'ObjectRemoved' in record['eventName']:
                            key_found = True
                            object_size = record['s3']['object']['size']
                            break
                        elif not deletions and 'ObjectCreated' in record['eventName']:
                            key_found = True
                            object_size = record['s3']['object']['size']
                            break
        else:
            for record in records['Records']:
                if record['s3']['bucket']['name'] == key.bucket.name and \
                    record['s3']['object']['key'] == key.name:
                    if deletions and 'ObjectRemoved' in record['eventName']:
                        key_found = True
                        object_size = record['s3']['object']['size']
                        break
                    elif not deletions and 'ObjectCreated' in record['eventName']:
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


def init_rabbitmq():
    """ start a rabbitmq broker """
    hostname = get_ip()
    #port = str(random.randint(20000, 30000))
    #data_dir = './' + port + '_data'
    #log_dir = './' + port + '_log'
    #print('')
    #try:
    #    os.mkdir(data_dir)
    #    os.mkdir(log_dir)
    #except:
    #    print('rabbitmq directories already exists')
    #env = {'RABBITMQ_NODE_PORT': port,
    #       'RABBITMQ_NODENAME': 'rabbit'+ port + '@' + hostname,
    #       'RABBITMQ_USE_LONGNAME': 'true',
    #       'RABBITMQ_MNESIA_BASE': data_dir,
    #       'RABBITMQ_LOG_BASE': log_dir}
    # TODO: support multiple brokers per host using env
    # make sure we don't collide with the default
    try:
        proc = subprocess.Popen('rabbitmq-server')
    except Exception as error:
        log.info('failed to execute rabbitmq-server: %s', str(error))
        print('failed to execute rabbitmq-server: %s' % str(error))
        return None
    # TODO add rabbitmq checkpoint instead of sleep
    time.sleep(5)
    return proc #, port, data_dir, log_dir


def clean_rabbitmq(proc): #, data_dir, log_dir)
    """ stop the rabbitmq broker """
    try:
        subprocess.call(['rabbitmqctl', 'stop'])
        time.sleep(5)
        proc.terminate()
    except:
        log.info('rabbitmq server already terminated')
    # TODO: add directory cleanup once multiple brokers are supported
    #try:
    #    os.rmdir(data_dir)
    #    os.rmdir(log_dir)
    #except:
    #    log.info('rabbitmq directories already removed')


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
                self.consumer = KafkaConsumer(topic, bootstrap_servers = kafka_server+':'+str(port), security_protocol=security_type)
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

    def verify_s3_events(self, keys, exact_match=False, deletions=False):
        """verify stored s3 records agains a list of keys"""
        verify_s3_records_by_elements(self.events, keys, exact_match=exact_match, deletions=deletions)
        self.events = []


def kafka_receiver_thread_runner(receiver):
    """main thread function for the kafka receiver"""
    try:
        log.info('Kafka receiver started')
        print('Kafka receiver started')
        while not receiver.stop:
            for msg in receiver.consumer:
                receiver.events.append(json.loads(msg.value))
            timer.sleep(0.1)
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
        receiver.consumer.close()
    except Exception as error:
        log.info('failed to gracefuly stop Kafka receiver: %s', str(error))


# follow the instruction here to create and sign a broker certificate:
# https://github.com/edenhill/librdkafka/wiki/Using-SSL-with-librdkafka

# the generated broker certificate should be stored in the java keystore for the use of the server
# assuming the jks files were copied to $KAFKA_DIR and broker name is "localhost"
# following lines must be added to $KAFKA_DIR/config/server.properties
# listeners=PLAINTEXT://localhost:9092,SSL://localhost:9093,SASL_SSL://localhost:9094
# sasl.enabled.mechanisms=PLAIN
# ssl.keystore.location = $KAFKA_DIR/server.keystore.jks
# ssl.keystore.password = abcdefgh
# ssl.key.password = abcdefgh
# ssl.truststore.location = $KAFKA_DIR/server.truststore.jks
# ssl.truststore.password = abcdefgh

# notes:
# (1) we dont test client authentication, hence, no need to generate client keys
# (2) our client is not using the keystore, and the "rootCA.crt" file generated in the process above
# should be copied to: $KAFKA_DIR

def init_kafka():
    """ start kafka/zookeeper """
    try:
        KAFKA_DIR = os.environ['KAFKA_DIR']
    except:
        KAFKA_DIR = ''

    if KAFKA_DIR == '':
        log.info('KAFKA_DIR must be set to where kafka is installed')
        print('KAFKA_DIR must be set to where kafka is installed')
        return None, None, None
    
    DEVNULL = open(os.devnull, 'wb')

    print('\nStarting zookeeper...')
    try:
        zk_proc = subprocess.Popen([KAFKA_DIR+'bin/zookeeper-server-start.sh', KAFKA_DIR+'config/zookeeper.properties'], stdout=DEVNULL)
    except Exception as error:
        log.info('failed to execute zookeeper: %s', str(error))
        print('failed to execute zookeeper: %s' % str(error))
        return None, None, None

    time.sleep(5)
    if zk_proc.poll() is not None:
        print('zookeeper failed to start')
        return None, None, None
    print('Zookeeper started')
    print('Starting kafka...')
    kafka_log = open('./kafka.log', 'w')
    try:
        kafka_env = os.environ.copy()
        kafka_env['KAFKA_OPTS']='-Djava.security.auth.login.config='+KAFKA_DIR+'config/kafka_server_jaas.conf'
        kafka_proc = subprocess.Popen([
            KAFKA_DIR+'bin/kafka-server-start.sh', 
            KAFKA_DIR+'config/server.properties'], 
            stdout=kafka_log,
            env=kafka_env)
    except Exception as error:
        log.info('failed to execute kafka: %s', str(error))
        print('failed to execute kafka: %s' % str(error))
        zk_proc.terminate()
        kafka_log.close()
        return None, None, None

    # TODO add kafka checkpoint instead of sleep
    time.sleep(15)
    if kafka_proc.poll() is not None:
        zk_proc.terminate()
        print('kafka failed to start. details in: ./kafka.log')
        kafka_log.close()
        return None, None, None

    print('Kafka started')
    return kafka_proc, zk_proc, kafka_log


def clean_kafka(kafka_proc, zk_proc, kafka_log):
    """ stop kafka/zookeeper """
    try:
        kafka_log.close()
        print('Shutdown Kafka...')
        kafka_proc.terminate()
        time.sleep(5)
        if kafka_proc.poll() is None:
            print('Failed to shutdown Kafka... killing')
            kafka_proc.kill()
        print('Shutdown zookeeper...')
        zk_proc.terminate()
        time.sleep(5)
        if zk_proc.poll() is None:
            print('Failed to shutdown zookeeper... killing')
            zk_proc.kill()
    except:
        log.info('kafka/zookeeper already terminated')


def init_env(require_ps=True):
    """initialize the environment"""
    if require_ps:
        check_ps_configured()

    realm = get_realm()
    zonegroup = realm.master_zonegroup()
    zonegroup_conns = ZonegroupConns(zonegroup)

    zonegroup_meta_checkpoint(zonegroup)

    ps_zone = None
    master_zone = None
    for conn in zonegroup_conns.zones:
        if conn.zone == zonegroup.master_zone:
            master_zone = conn
        if is_ps_zone(conn):
            zone_meta_checkpoint(conn.zone)
            ps_zone = conn

    assert_not_equal(master_zone, None)
    if require_ps:
        assert_not_equal(ps_zone, None)
    return master_zone, ps_zone


def get_ip():
    """ This method returns the "primary" IP on the local box (the one with a default route)
    source: https://stackoverflow.com/a/28950776/711085
    this is needed because on the teuthology machines: socket.getfqdn()/socket.gethostname() return 127.0.0.1 """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # address should not be reachable
        s.connect(('10.255.255.255', 1))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip


TOPIC_SUFFIX = "_topic"
SUB_SUFFIX = "_sub"
NOTIFICATION_SUFFIX = "_notif"

##############
# pubsub tests
##############

def test_ps_info():
    """ log information for manual testing """
    return SkipTest("only used in manual testing")
    master_zone, ps_zone = init_env()
    realm = get_realm()
    zonegroup = realm.master_zonegroup()
    bucket_name = gen_bucket_name()
    # create bucket on the first of the rados zones
    bucket = master_zone.create_bucket(bucket_name)
    # create objects in the bucket
    number_of_objects = 10
    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        key.set_contents_from_string('bar')
    print('Zonegroup: ' + zonegroup.name)
    print('user: ' + get_user())
    print('tenant: ' + get_tenant())
    print('Master Zone')
    print_connection_info(master_zone.conn)
    print('PubSub Zone')
    print_connection_info(ps_zone.conn)
    print('Bucket: ' + bucket_name)


def test_ps_s3_notification_low_level():
    """ test low level implementation of s3 notifications """
    master_zone, ps_zone = init_env()
    bucket_name = gen_bucket_name()
    # create bucket on the first of the rados zones
    master_zone.create_bucket(bucket_name)
    # wait for sync
    zone_meta_checkpoint(ps_zone.zone)
    # create topic
    topic_name = bucket_name + TOPIC_SUFFIX
    topic_conf = PSTopic(ps_zone.conn, topic_name)
    result, status = topic_conf.set_config()
    assert_equal(status/100, 2)
    parsed_result = json.loads(result)
    topic_arn = parsed_result['arn']
    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    generated_topic_name = notification_name+'_'+topic_name
    topic_conf_list = [{'Id': notification_name,
                        'TopicArn': topic_arn,
                        'Events': ['s3:ObjectCreated:*']
                       }]
    s3_notification_conf = PSNotificationS3(ps_zone.conn, bucket_name, topic_conf_list)
    _, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)
    zone_meta_checkpoint(ps_zone.zone)
    # get auto-generated topic
    generated_topic_conf = PSTopic(ps_zone.conn, generated_topic_name)
    result, status = generated_topic_conf.get_config()
    parsed_result = json.loads(result)
    assert_equal(status/100, 2)
    assert_equal(parsed_result['topic']['name'], generated_topic_name)
    # get auto-generated notification
    notification_conf = PSNotification(ps_zone.conn, bucket_name,
                                       generated_topic_name)
    result, status = notification_conf.get_config()
    parsed_result = json.loads(result)
    assert_equal(status/100, 2)
    assert_equal(len(parsed_result['topics']), 1)
    # get auto-generated subscription
    sub_conf = PSSubscription(ps_zone.conn, notification_name,
                              generated_topic_name)
    result, status = sub_conf.get_config()
    parsed_result = json.loads(result)
    assert_equal(status/100, 2)
    assert_equal(parsed_result['topic'], generated_topic_name)
    # delete s3 notification
    _, status = s3_notification_conf.del_config(notification=notification_name)
    assert_equal(status/100, 2)
    # delete topic
    _, status = topic_conf.del_config()
    assert_equal(status/100, 2)

    # verify low-level cleanup
    _, status = generated_topic_conf.get_config()
    assert_equal(status, 404)
    result, status = notification_conf.get_config()
    parsed_result = json.loads(result)
    assert_equal(len(parsed_result['topics']), 0)
    # TODO should return 404
    # assert_equal(status, 404)
    result, status = sub_conf.get_config()
    parsed_result = json.loads(result)
    assert_equal(parsed_result['topic'], '')
    # TODO should return 404
    # assert_equal(status, 404)

    # cleanup
    topic_conf.del_config()
    # delete the bucket
    master_zone.delete_bucket(bucket_name)


def test_ps_s3_notification_records():
    """ test s3 records fetching """
    master_zone, ps_zone = init_env()
    bucket_name = gen_bucket_name()
    # create bucket on the first of the rados zones
    bucket = master_zone.create_bucket(bucket_name)
    # wait for sync
    zone_meta_checkpoint(ps_zone.zone)
    # create topic
    topic_name = bucket_name + TOPIC_SUFFIX
    topic_conf = PSTopic(ps_zone.conn, topic_name)
    result, status = topic_conf.set_config()
    assert_equal(status/100, 2)
    parsed_result = json.loads(result)
    topic_arn = parsed_result['arn']
    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name,
                        'TopicArn': topic_arn,
                        'Events': ['s3:ObjectCreated:*']
                       }]
    s3_notification_conf = PSNotificationS3(ps_zone.conn, bucket_name, topic_conf_list)
    _, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)
    zone_meta_checkpoint(ps_zone.zone)
    # get auto-generated subscription
    sub_conf = PSSubscription(ps_zone.conn, notification_name,
                              topic_name)
    _, status = sub_conf.get_config()
    assert_equal(status/100, 2)
    # create objects in the bucket
    number_of_objects = 10
    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        key.set_contents_from_string('bar')
    # wait for sync
    zone_bucket_checkpoint(ps_zone.zone, master_zone.zone, bucket_name)

    # get the events from the subscription
    result, _ = sub_conf.get_events()
    records = json.loads(result)
    for record in records['Records']:
        log.debug(record)
    keys = list(bucket.list())
    # TODO: use exact match
    verify_s3_records_by_elements(records, keys, exact_match=False)

    # cleanup
    _, status = s3_notification_conf.del_config()
    topic_conf.del_config()
    # delete the keys
    for key in bucket.list():
        key.delete()
    master_zone.delete_bucket(bucket_name)


def test_ps_s3_notification():
    """ test s3 notification set/get/delete """
    master_zone, ps_zone = init_env()
    bucket_name = gen_bucket_name()
    # create bucket on the first of the rados zones
    master_zone.create_bucket(bucket_name)
    # wait for sync
    zone_meta_checkpoint(ps_zone.zone)
    topic_name = bucket_name + TOPIC_SUFFIX
    # create topic
    topic_name = bucket_name + TOPIC_SUFFIX
    topic_conf = PSTopic(ps_zone.conn, topic_name)
    response, status = topic_conf.set_config()
    assert_equal(status/100, 2)
    parsed_result = json.loads(response)
    topic_arn = parsed_result['arn']
    # create one s3 notification
    notification_name1 = bucket_name + NOTIFICATION_SUFFIX + '_1'
    topic_conf_list = [{'Id': notification_name1,
                        'TopicArn': topic_arn,
                        'Events': ['s3:ObjectCreated:*']
                       }]
    s3_notification_conf1 = PSNotificationS3(ps_zone.conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf1.set_config()
    assert_equal(status/100, 2)
    # create another s3 notification with the same topic
    notification_name2 = bucket_name + NOTIFICATION_SUFFIX + '_2'
    topic_conf_list = [{'Id': notification_name2,
                        'TopicArn': topic_arn,
                        'Events': ['s3:ObjectCreated:*', 's3:ObjectRemoved:*']
                       }]
    s3_notification_conf2 = PSNotificationS3(ps_zone.conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf2.set_config()
    assert_equal(status/100, 2)
    zone_meta_checkpoint(ps_zone.zone)

    # get all notification on a bucket
    response, status = s3_notification_conf1.get_config()
    assert_equal(status/100, 2)
    assert_equal(len(response['TopicConfigurations']), 2)
    assert_equal(response['TopicConfigurations'][0]['TopicArn'], topic_arn)
    assert_equal(response['TopicConfigurations'][1]['TopicArn'], topic_arn)

    # get specific notification on a bucket
    response, status = s3_notification_conf1.get_config(notification=notification_name1)
    assert_equal(status/100, 2)
    assert_equal(response['NotificationConfiguration']['TopicConfiguration']['Topic'], topic_arn)
    assert_equal(response['NotificationConfiguration']['TopicConfiguration']['Id'], notification_name1)
    response, status = s3_notification_conf2.get_config(notification=notification_name2)
    assert_equal(status/100, 2)
    assert_equal(response['NotificationConfiguration']['TopicConfiguration']['Topic'], topic_arn)
    assert_equal(response['NotificationConfiguration']['TopicConfiguration']['Id'], notification_name2)

    # delete specific notifications
    _, status = s3_notification_conf1.del_config(notification=notification_name1)
    assert_equal(status/100, 2)
    _, status = s3_notification_conf2.del_config(notification=notification_name2)
    assert_equal(status/100, 2)

    # cleanup
    topic_conf.del_config()
    # delete the bucket
    master_zone.delete_bucket(bucket_name)


def test_ps_s3_topic_on_master():
    """ test s3 topics set/get/delete on master """
    master_zone, _ = init_env(require_ps=False)
    realm = get_realm()
    zonegroup = realm.master_zonegroup()
    bucket_name = gen_bucket_name()
    topic_name = bucket_name + TOPIC_SUFFIX

    # clean all topics
    delete_all_s3_topics(master_zone, zonegroup.name)
   
    # create s3 topics
    endpoint_address = 'amqp://127.0.0.1:7001'
    endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=amqp.direct&amqp-ack-level=none'
    topic_conf1 = PSTopicS3(master_zone.conn, topic_name+'_1', zonegroup.name, endpoint_args=endpoint_args)
    topic_arn = topic_conf1.set_config()
    assert_equal(topic_arn,
                 'arn:aws:sns:' + zonegroup.name + ':' + get_tenant() + ':' + topic_name + '_1')

    endpoint_address = 'http://127.0.0.1:9001'
    endpoint_args = 'push-endpoint='+endpoint_address
    topic_conf2 = PSTopicS3(master_zone.conn, topic_name+'_2', zonegroup.name, endpoint_args=endpoint_args)
    topic_arn = topic_conf2.set_config()
    assert_equal(topic_arn,
                 'arn:aws:sns:' + zonegroup.name + ':' + get_tenant() + ':' + topic_name + '_2')
    endpoint_address = 'http://127.0.0.1:9002'
    endpoint_args = 'push-endpoint='+endpoint_address
    topic_conf3 = PSTopicS3(master_zone.conn, topic_name+'_3', zonegroup.name, endpoint_args=endpoint_args)
    topic_arn = topic_conf3.set_config()
    assert_equal(topic_arn,
                 'arn:aws:sns:' + zonegroup.name + ':' + get_tenant() + ':' + topic_name + '_3')

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
    # TODO: should be 200OK
    # assert_equal(status, 200)
    result = topic_conf3.del_config()
    # TODO: should be 200OK
    # assert_equal(status, 200)

    # get topic list, make sure it is empty
    result, status = topic_conf1.get_list()
    assert_equal(result['ListTopicsResponse']['ListTopicsResult']['Topics'], None)


def test_ps_s3_topic_with_secret_on_master():
    """ test s3 topics with secret set/get/delete on master """
    master_zone, _ = init_env(require_ps=False)
    if master_zone.secure_conn is None:
        return SkipTest('secure connection is needed to test topic with secrets')

    realm = get_realm()
    zonegroup = realm.master_zonegroup()
    bucket_name = gen_bucket_name()
    topic_name = bucket_name + TOPIC_SUFFIX

    # clean all topics
    delete_all_s3_topics(master_zone, zonegroup.name)
   
    # create s3 topics
    endpoint_address = 'amqp://user:password@127.0.0.1:7001'
    endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=amqp.direct&amqp-ack-level=none'
    bad_topic_conf = PSTopicS3(master_zone.conn, topic_name, zonegroup.name, endpoint_args=endpoint_args)
    try:
        result = bad_topic_conf.set_config()
    except Exception as err:
        print('Error is expected: ' + str(err))
    else:
        assert False, 'user password configuration set allowed only over HTTPS'
    
    topic_conf = PSTopicS3(master_zone.secure_conn, topic_name, zonegroup.name, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()

    assert_equal(topic_arn,
                 'arn:aws:sns:' + zonegroup.name + ':' + get_tenant() + ':' + topic_name)

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


def test_ps_s3_notification_on_master():
    """ test s3 notification set/get/delete on master """
    master_zone, _  = init_env(require_ps=False)
    realm = get_realm()
    zonegroup = realm.master_zonegroup()
    bucket_name = gen_bucket_name()
    # create bucket
    bucket = master_zone.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX
    # create s3 topic
    endpoint_address = 'amqp://127.0.0.1:7001'
    endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=amqp.direct&amqp-ack-level=none'
    topic_conf = PSTopicS3(master_zone.conn, topic_name, zonegroup.name, endpoint_args=endpoint_args)
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
    s3_notification_conf = PSNotificationS3(master_zone.conn, bucket_name, topic_conf_list)
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
    master_zone.delete_bucket(bucket_name)


def ps_s3_notification_filter(on_master):
    """ test s3 notification filter on master """
    if skip_push_tests:
        return SkipTest("PubSub push tests don't run in teuthology")
    hostname = get_ip()
    proc = init_rabbitmq()
    if proc is  None:
        return SkipTest('end2end amqp tests require rabbitmq-server installed')
    if on_master:
        master_zone, _ = init_env(require_ps=False)
        ps_zone = master_zone
    else:
        master_zone, ps_zone = init_env(require_ps=True)
        ps_zone = ps_zone

    realm = get_realm()
    zonegroup = realm.master_zonegroup()
    
    # create bucket
    bucket_name = gen_bucket_name()
    bucket = master_zone.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX

    # start amqp receivers
    exchange = 'ex1'
    task, receiver = create_amqp_receiver_thread(exchange, topic_name)
    task.start()

    # create s3 topic
    endpoint_address = 'amqp://' + hostname
    endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=' + exchange +'&amqp-ack-level=broker'
    if on_master:
        topic_conf = PSTopicS3(ps_zone.conn, topic_name, zonegroup.name, endpoint_args=endpoint_args)
        topic_arn = topic_conf.set_config()
    else:
        topic_conf = PSTopic(ps_zone.conn, topic_name, endpoint=endpoint_address, endpoint_args=endpoint_args)
        result, _ = topic_conf.set_config()
        parsed_result = json.loads(result)
        topic_arn = parsed_result['arn']
        zone_meta_checkpoint(ps_zone.zone)

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

    s3_notification_conf = PSNotificationS3(ps_zone.conn, bucket_name, topic_conf_list)
    result, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)

    if on_master:
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
            s3_notification_conf4 = PSNotificationS3(ps_zone.conn, bucket_name, topic_conf_list)
            _, status = s3_notification_conf4.set_config()
            assert_equal(status/100, 2)
            skip_notif4 = False
        except Exception as error:
            print('note: metadata filter is not supported by boto3 - skipping test')
            skip_notif4 = True
    else:
        print('filtering by attributes only supported on master zone')
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

    if on_master:
        print('wait for 5sec for the messages...')
        time.sleep(5)
    else:
        zone_bucket_checkpoint(ps_zone.zone, master_zone.zone, bucket_name)

    found_in1 = []
    found_in2 = []
    found_in3 = []
    found_in4 = []

    for event in receiver.get_and_reset_events():
        notif_id = event['Records'][0]['s3']['configurationId']
        key_name = event['Records'][0]['s3']['object']['key']
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
    master_zone.delete_bucket(bucket_name)
    stop_amqp_receiver(receiver, task)
    clean_rabbitmq(proc)


def test_ps_s3_notification_filter_on_master():
    ps_s3_notification_filter(on_master=True)


def test_ps_s3_notification_filter():
    ps_s3_notification_filter(on_master=False)


def test_ps_s3_notification_errors_on_master():
    """ test s3 notification set/get/delete on master """
    master_zone, _ = init_env(require_ps=False)
    realm = get_realm()
    zonegroup = realm.master_zonegroup()
    bucket_name = gen_bucket_name()
    # create bucket
    bucket = master_zone.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX
    # create s3 topic
    endpoint_address = 'amqp://127.0.0.1:7001'
    endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=amqp.direct&amqp-ack-level=none'
    topic_conf = PSTopicS3(master_zone.conn, topic_name, zonegroup.name, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()

    # create s3 notification with invalid event name
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name,
                        'TopicArn': topic_arn,
                        'Events': ['s3:ObjectCreated:Kaboom']
                       }]
    s3_notification_conf = PSNotificationS3(master_zone.conn, bucket_name, topic_conf_list)
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
    s3_notification_conf = PSNotificationS3(master_zone.conn, bucket_name, topic_conf_list)
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
    s3_notification_conf = PSNotificationS3(master_zone.conn, bucket_name, topic_conf_list)
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
    s3_notification_conf = PSNotificationS3(master_zone.conn, bucket_name, topic_conf_list)
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
    s3_notification_conf = PSNotificationS3(master_zone.conn, 'kaboom', topic_conf_list)
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
    master_zone.delete_bucket(bucket_name)


def test_objcet_timing():
    return SkipTest("only used in manual testing")
    master_zone, _ = init_env(require_ps=False)
    
    # create bucket
    bucket_name = gen_bucket_name()
    bucket = master_zone.create_bucket(bucket_name)
    # create objects in the bucket (async)
    print('creating objects...')
    number_of_objects = 1000
    client_threads = []
    start_time = time.time()
    content = str(bytearray(os.urandom(1024*1024)))
    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        thr = threading.Thread(target = set_contents_from_string, args=(key, content,))
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads] 

    time_diff = time.time() - start_time
    print('average time for object creation: ' + str(time_diff*1000/number_of_objects) + ' milliseconds')
    
    print('total number of objects: ' + str(len(list(bucket.list()))))

    print('deleting objects...')
    client_threads = []
    start_time = time.time()
    for key in bucket.list():
        thr = threading.Thread(target = key.delete, args=())
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads] 
    
    time_diff = time.time() - start_time
    print('average time for object deletion: ' + str(time_diff*1000/number_of_objects) + ' milliseconds')
    
    # cleanup
    master_zone.delete_bucket(bucket_name)


def test_ps_s3_notification_push_amqp_on_master():
    """ test pushing amqp s3 notification on master """
    if skip_push_tests:
        return SkipTest("PubSub push tests don't run in teuthology")
    hostname = get_ip()
    proc = init_rabbitmq()
    if proc is  None:
        return SkipTest('end2end amqp tests require rabbitmq-server installed')
    master_zone, _ = init_env(require_ps=False)
    realm = get_realm()
    zonegroup = realm.master_zonegroup()
    
    # create bucket
    bucket_name = gen_bucket_name()
    bucket = master_zone.create_bucket(bucket_name)
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
    topic_conf1 = PSTopicS3(master_zone.conn, topic_name1, zonegroup.name, endpoint_args=endpoint_args)
    topic_arn1 = topic_conf1.set_config()
    # without acks from broker
    endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=' + exchange +'&amqp-ack-level=routable'
    topic_conf2 = PSTopicS3(master_zone.conn, topic_name2, zonegroup.name, endpoint_args=endpoint_args)
    topic_arn2 = topic_conf2.set_config()
    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name+'_1', 'TopicArn': topic_arn1,
                         'Events': []
                       },
                       {'Id': notification_name+'_2', 'TopicArn': topic_arn2,
                         'Events': ['s3:ObjectCreated:*']
                       }]

    s3_notification_conf = PSNotificationS3(master_zone.conn, bucket_name, topic_conf_list)
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
    master_zone.delete_bucket(bucket_name)
    clean_rabbitmq(proc)


def test_ps_s3_notification_push_kafka():
    """ test pushing kafka s3 notification on master """
    if skip_push_tests:
        return SkipTest("PubSub push tests don't run in teuthology")
    kafka_proc, zk_proc, kafka_log = init_kafka()
    if kafka_proc is  None or zk_proc is None:
        return SkipTest('end2end kafka tests require kafka/zookeeper installed')

    master_zone, ps_zone = init_env()
    realm = get_realm()
    zonegroup = realm.master_zonegroup()
    
    # create bucket
    bucket_name = gen_bucket_name()
    bucket = master_zone.create_bucket(bucket_name)
    # wait for sync
    zone_meta_checkpoint(ps_zone.zone)
    # name is constant for manual testing
    topic_name = bucket_name+'_topic'
    # create consumer on the topic
    task, receiver = create_kafka_receiver_thread(topic_name)
    task.start()

    # create topic
    topic_conf = PSTopic(ps_zone.conn, topic_name,
                         endpoint='kafka://' + kafka_server,
                         endpoint_args='kafka-ack-level=broker')
    result, status = topic_conf.set_config()
    assert_equal(status/100, 2)
    parsed_result = json.loads(result)
    topic_arn = parsed_result['arn']
    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name, 'TopicArn': topic_arn,
                         'Events': []
                       }]

    s3_notification_conf = PSNotificationS3(ps_zone.conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)

    # create objects in the bucket (async)
    number_of_objects = 10
    client_threads = []
    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        content = str(os.urandom(1024*1024))
        thr = threading.Thread(target = set_contents_from_string, args=(key, content,))
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads] 

    # wait for sync
    zone_bucket_checkpoint(ps_zone.zone, master_zone.zone, bucket_name)
    keys = list(bucket.list())
    receiver.verify_s3_events(keys, exact_match=True)

    # delete objects from the bucket
    client_threads = []
    for key in bucket.list():
        thr = threading.Thread(target = key.delete, args=())
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads] 
    
    zone_bucket_checkpoint(ps_zone.zone, master_zone.zone, bucket_name)
    receiver.verify_s3_events(keys, exact_match=True, deletions=True)
    
    # cleanup
    s3_notification_conf.del_config()
    topic_conf.del_config()
    # delete the bucket
    master_zone.delete_bucket(bucket_name)
    stop_kafka_receiver(receiver, task)
    clean_kafka(kafka_proc, zk_proc, kafka_log)


def test_ps_s3_notification_push_kafka_on_master():
    """ test pushing kafka s3 notification on master """
    if skip_push_tests:
        return SkipTest("PubSub push tests don't run in teuthology")
    kafka_proc, zk_proc, kafka_log = init_kafka()
    if kafka_proc is None or zk_proc is None:
        return SkipTest('end2end kafka tests require kafka/zookeeper installed')
    master_zone, _ = init_env(require_ps=False)
    realm = get_realm()
    zonegroup = realm.master_zonegroup()
    
    # create bucket
    bucket_name = gen_bucket_name()
    bucket = master_zone.create_bucket(bucket_name)
    # name is constant for manual testing
    topic_name = bucket_name+'_topic'
    # create consumer on the topic
    task, receiver = create_kafka_receiver_thread(topic_name+'_1')
    task.start()

    # create s3 topic
    endpoint_address = 'kafka://' + kafka_server
    # without acks from broker
    endpoint_args = 'push-endpoint='+endpoint_address+'&kafka-ack-level=broker'
    topic_conf1 = PSTopicS3(master_zone.conn, topic_name+'_1', zonegroup.name, endpoint_args=endpoint_args)
    topic_arn1 = topic_conf1.set_config()
    endpoint_args = 'push-endpoint='+endpoint_address+'&kafka-ack-level=none'
    topic_conf2 = PSTopicS3(master_zone.conn, topic_name+'_2', zonegroup.name, endpoint_args=endpoint_args)
    topic_arn2 = topic_conf2.set_config()
    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name + '_1', 'TopicArn': topic_arn1,
                         'Events': []
                       },
                       {'Id': notification_name + '_2', 'TopicArn': topic_arn2,
                         'Events': []
                       }]

    s3_notification_conf = PSNotificationS3(master_zone.conn, bucket_name, topic_conf_list)
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
    print('average time for creation + kafka notification is: ' + str(time_diff*1000/number_of_objects) + ' milliseconds')

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
    
    # cleanup
    s3_notification_conf.del_config()
    topic_conf1.del_config()
    topic_conf2.del_config()
    # delete the bucket
    master_zone.delete_bucket(bucket_name)
    stop_kafka_receiver(receiver, task)
    clean_kafka(kafka_proc, zk_proc, kafka_log)


def kafka_security(security_type):
    """ test pushing kafka s3 notification on master """
    if skip_push_tests:
        return SkipTest("PubSub push tests don't run in teuthology")
    master_zone, _ = init_env(require_ps=False)
    if security_type == 'SSL_SASL' and master_zone.secure_conn is None:
        return SkipTest("secure connection is needed to test SASL_SSL security")
    kafka_proc, zk_proc, kafka_log = init_kafka()
    if kafka_proc is None or zk_proc is None:
        return SkipTest('end2end kafka tests require kafka/zookeeper installed')
    realm = get_realm()
    zonegroup = realm.master_zonegroup()
    
    # create bucket
    bucket_name = gen_bucket_name()
    bucket = master_zone.create_bucket(bucket_name)
    # name is constant for manual testing
    topic_name = bucket_name+'_topic'
    # create consumer on the topic
    task, receiver = create_kafka_receiver_thread(topic_name)
    task.start()

    # create s3 topic
    if security_type == 'SSL_SASL':
        endpoint_address = 'kafka://alice:alice-secret@' + kafka_server + ':9094'
    else:
        # ssl only
        endpoint_address = 'kafka://' + kafka_server + ':9093'

    KAFKA_DIR = os.environ['KAFKA_DIR']

    # without acks from broker, with root CA
    endpoint_args = 'push-endpoint='+endpoint_address+'&kafka-ack-level=none&use-ssl=true&ca-location='+KAFKA_DIR+'rootCA.crt'

    if security_type == 'SSL_SASL':
        topic_conf = PSTopicS3(master_zone.secure_conn, topic_name, zonegroup.name, endpoint_args=endpoint_args)
    else:
        topic_conf = PSTopicS3(master_zone.conn, topic_name, zonegroup.name, endpoint_args=endpoint_args)

    topic_arn = topic_conf.set_config()
    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name, 'TopicArn': topic_arn,
                         'Events': []
                       }]

    s3_notification_conf = PSNotificationS3(master_zone.conn, bucket_name, topic_conf_list)
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
        master_zone.delete_bucket(bucket_name)
        stop_kafka_receiver(receiver, task)
        clean_kafka(kafka_proc, zk_proc, kafka_log)


def test_ps_s3_notification_push_kafka_security_ssl():
    kafka_security('SSL')

def test_ps_s3_notification_push_kafka_security_ssl_sasl():
    kafka_security('SSL_SASL')


def test_ps_s3_notification_multi_delete_on_master():
    """ test deletion of multiple keys on master """
    if skip_push_tests:
        return SkipTest("PubSub push tests don't run in teuthology")
    hostname = get_ip()
    master_zone, _  = init_env(require_ps=False)
    realm = get_realm()
    zonegroup = realm.master_zonegroup()
    
    # create random port for the http server
    host = get_ip()
    port = random.randint(10000, 20000)
    # start an http server in a separate thread
    number_of_objects = 10
    http_server = StreamingHTTPServer(host, port, num_workers=number_of_objects)
    
    # create bucket
    bucket_name = gen_bucket_name()
    bucket = master_zone.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX

    # create s3 topic
    endpoint_address = 'http://'+host+':'+str(port)
    endpoint_args = 'push-endpoint='+endpoint_address
    topic_conf = PSTopicS3(master_zone.conn, topic_name, zonegroup.name, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()
    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name,
                        'TopicArn': topic_arn,
                        'Events': ['s3:ObjectRemoved:*']
                       }]
    s3_notification_conf = PSNotificationS3(master_zone.conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)

    # create objects in the bucket
    client_threads = []
    objects_size = {}
    for i in range(number_of_objects):
        object_size = randint(1, 1024)
        content = str(os.urandom(object_size))
        key = bucket.new_key(str(i))
        objects_size[key.name] = object_size
        thr = threading.Thread(target = set_contents_from_string, args=(key, content,))
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads] 
    
    keys = list(bucket.list())

    start_time = time.time()
    delete_all_objects(master_zone.conn, bucket_name)
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
    master_zone.delete_bucket(bucket_name)
    http_server.close()


def test_ps_s3_notification_push_http_on_master():
    """ test pushing http s3 notification on master """
    if skip_push_tests:
        return SkipTest("PubSub push tests don't run in teuthology")
    hostname = get_ip()
    master_zone, _ = init_env(require_ps=False)
    realm = get_realm()
    zonegroup = realm.master_zonegroup()
    
    # create random port for the http server
    host = get_ip()
    port = random.randint(10000, 20000)
    # start an http server in a separate thread
    number_of_objects = 10
    http_server = StreamingHTTPServer(host, port, num_workers=number_of_objects)
    
    # create bucket
    bucket_name = gen_bucket_name()
    bucket = master_zone.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX

    # create s3 topic
    endpoint_address = 'http://'+host+':'+str(port)
    endpoint_args = 'push-endpoint='+endpoint_address
    topic_conf = PSTopicS3(master_zone.conn, topic_name, zonegroup.name, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()
    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name,
                        'TopicArn': topic_arn,
                        'Events': []
                       }]
    s3_notification_conf = PSNotificationS3(master_zone.conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)

    # create objects in the bucket
    client_threads = []
    objects_size = {}
    start_time = time.time()
    for i in range(number_of_objects):
        object_size = randint(1, 1024)
        content = str(os.urandom(object_size))
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
    master_zone.delete_bucket(bucket_name)
    http_server.close()


def test_ps_s3_opaque_data():
    """ test that opaque id set in topic, is sent in notification """
    if skip_push_tests:
        return SkipTest("PubSub push tests don't run in teuthology")
    hostname = get_ip()
    master_zone, ps_zone = init_env()
    realm = get_realm()
    zonegroup = realm.master_zonegroup()
    
    # create random port for the http server
    host = get_ip()
    port = random.randint(10000, 20000)
    # start an http server in a separate thread
    number_of_objects = 10
    http_server = StreamingHTTPServer(host, port, num_workers=number_of_objects)
    
    # create bucket
    bucket_name = gen_bucket_name()
    bucket = master_zone.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX
    # wait for sync
    zone_meta_checkpoint(ps_zone.zone)

    # create s3 topic
    endpoint_address = 'http://'+host+':'+str(port)
    opaque_data = 'http://1.2.3.4:8888'
    endpoint_args = 'push-endpoint='+endpoint_address+'&OpaqueData='+opaque_data
    topic_conf = PSTopic(ps_zone.conn, topic_name, endpoint=endpoint_address, endpoint_args=endpoint_args)
    result, status = topic_conf.set_config()
    assert_equal(status/100, 2)
    parsed_result = json.loads(result)
    topic_arn = parsed_result['arn']
    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name,
                        'TopicArn': topic_arn,
                        'Events': []
                       }]
    s3_notification_conf = PSNotificationS3(ps_zone.conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)

    # create objects in the bucket
    client_threads = []
    content = 'bar'
    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        thr = threading.Thread(target = set_contents_from_string, args=(key, content,))
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads] 

    # wait for sync
    zone_bucket_checkpoint(ps_zone.zone, master_zone.zone, bucket_name)

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
    master_zone.delete_bucket(bucket_name)
    http_server.close()


def test_ps_s3_opaque_data_on_master():
    """ test that opaque id set in topic, is sent in notification on master """
    if skip_push_tests:
        return SkipTest("PubSub push tests don't run in teuthology")
    hostname = get_ip()
    master_zone, _ = init_env(require_ps=False)
    realm = get_realm()
    zonegroup = realm.master_zonegroup()
    
    # create random port for the http server
    host = get_ip()
    port = random.randint(10000, 20000)
    # start an http server in a separate thread
    number_of_objects = 10
    http_server = StreamingHTTPServer(host, port, num_workers=number_of_objects)
    
    # create bucket
    bucket_name = gen_bucket_name()
    bucket = master_zone.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX

    # create s3 topic
    endpoint_address = 'http://'+host+':'+str(port)
    endpoint_args = 'push-endpoint='+endpoint_address
    opaque_data = 'http://1.2.3.4:8888'
    topic_conf = PSTopicS3(master_zone.conn, topic_name, zonegroup.name, endpoint_args=endpoint_args, opaque_data=opaque_data)
    topic_arn = topic_conf.set_config()
    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name,
                        'TopicArn': topic_arn,
                        'Events': []
                       }]
    s3_notification_conf = PSNotificationS3(master_zone.conn, bucket_name, topic_conf_list)
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
    master_zone.delete_bucket(bucket_name)
    http_server.close()

def test_ps_topic():
    """ test set/get/delete of topic """
    _, ps_zone = init_env()
    realm = get_realm()
    zonegroup = realm.master_zonegroup()
    bucket_name = gen_bucket_name()
    topic_name = bucket_name+TOPIC_SUFFIX

    # create topic
    topic_conf = PSTopic(ps_zone.conn, topic_name)
    _, status = topic_conf.set_config()
    assert_equal(status/100, 2)
    # get topic
    result, _ = topic_conf.get_config()
    # verify topic content
    parsed_result = json.loads(result)
    assert_equal(parsed_result['topic']['name'], topic_name)
    assert_equal(len(parsed_result['subs']), 0)
    assert_equal(parsed_result['topic']['arn'],
                 'arn:aws:sns:' + zonegroup.name + ':' + get_tenant() + ':' + topic_name)
    # delete topic
    _, status = topic_conf.del_config()
    assert_equal(status/100, 2)
    # verift topic is deleted
    result, status = topic_conf.get_config()
    assert_equal(status, 404)
    parsed_result = json.loads(result)
    assert_equal(parsed_result['Code'], 'NoSuchKey')


def test_ps_topic_with_endpoint():
    """ test set topic with endpoint"""
    _, ps_zone = init_env()
    bucket_name = gen_bucket_name()
    topic_name = bucket_name+TOPIC_SUFFIX

    # create topic
    dest_endpoint = 'amqp://localhost:7001'
    dest_args = 'amqp-exchange=amqp.direct&amqp-ack-level=none'
    topic_conf = PSTopic(ps_zone.conn, topic_name,
                         endpoint=dest_endpoint,
                         endpoint_args=dest_args)
    _, status = topic_conf.set_config()
    assert_equal(status/100, 2)
    # get topic
    result, _ = topic_conf.get_config()
    # verify topic content
    parsed_result = json.loads(result)
    assert_equal(parsed_result['topic']['name'], topic_name)
    assert_equal(parsed_result['topic']['dest']['push_endpoint'], dest_endpoint)
    # cleanup
    topic_conf.del_config()


def test_ps_notification():
    """ test set/get/delete of notification """
    master_zone, ps_zone = init_env()
    bucket_name = gen_bucket_name()
    topic_name = bucket_name+TOPIC_SUFFIX

    # create topic
    topic_conf = PSTopic(ps_zone.conn, topic_name)
    topic_conf.set_config()
    # create bucket on the first of the rados zones
    master_zone.create_bucket(bucket_name)
    # wait for sync
    zone_meta_checkpoint(ps_zone.zone)
    # create notifications
    notification_conf = PSNotification(ps_zone.conn, bucket_name,
                                       topic_name)
    _, status = notification_conf.set_config()
    assert_equal(status/100, 2)
    # get notification
    result, _ = notification_conf.get_config()
    parsed_result = json.loads(result)
    assert_equal(len(parsed_result['topics']), 1)
    assert_equal(parsed_result['topics'][0]['topic']['name'],
                 topic_name)
    # delete notification
    _, status = notification_conf.del_config()
    assert_equal(status/100, 2)
    result, status = notification_conf.get_config()
    parsed_result = json.loads(result)
    assert_equal(len(parsed_result['topics']), 0)
    # TODO should return 404
    # assert_equal(status, 404)

    # cleanup
    topic_conf.del_config()
    master_zone.delete_bucket(bucket_name)


def test_ps_notification_events():
    """ test set/get/delete of notification on specific events"""
    master_zone, ps_zone = init_env()
    bucket_name = gen_bucket_name()
    topic_name = bucket_name+TOPIC_SUFFIX

    # create topic
    topic_conf = PSTopic(ps_zone.conn, topic_name)
    topic_conf.set_config()
    # create bucket on the first of the rados zones
    master_zone.create_bucket(bucket_name)
    # wait for sync
    zone_meta_checkpoint(ps_zone.zone)
    # create notifications
    events = "OBJECT_CREATE,OBJECT_DELETE"
    notification_conf = PSNotification(ps_zone.conn, bucket_name,
                                       topic_name,
                                       events)
    _, status = notification_conf.set_config()
    assert_equal(status/100, 2)
    # get notification
    result, _ = notification_conf.get_config()
    parsed_result = json.loads(result)
    assert_equal(len(parsed_result['topics']), 1)
    assert_equal(parsed_result['topics'][0]['topic']['name'],
                 topic_name)
    assert_not_equal(len(parsed_result['topics'][0]['events']), 0)
    # TODO add test for invalid event name

    # cleanup
    notification_conf.del_config()
    topic_conf.del_config()
    master_zone.delete_bucket(bucket_name)


def test_ps_subscription():
    """ test set/get/delete of subscription """
    master_zone, ps_zone = init_env()
    bucket_name = gen_bucket_name()
    topic_name = bucket_name+TOPIC_SUFFIX

    # create topic
    topic_conf = PSTopic(ps_zone.conn, topic_name)
    topic_conf.set_config()
    # create bucket on the first of the rados zones
    bucket = master_zone.create_bucket(bucket_name)
    # wait for sync
    zone_meta_checkpoint(ps_zone.zone)
    # create notifications
    notification_conf = PSNotification(ps_zone.conn, bucket_name,
                                       topic_name)
    _, status = notification_conf.set_config()
    assert_equal(status/100, 2)
    # create subscription
    sub_conf = PSSubscription(ps_zone.conn, bucket_name+SUB_SUFFIX,
                              topic_name)
    _, status = sub_conf.set_config()
    assert_equal(status/100, 2)
    # get the subscription
    result, _ = sub_conf.get_config()
    parsed_result = json.loads(result)
    assert_equal(parsed_result['topic'], topic_name)
    # create objects in the bucket
    number_of_objects = 10
    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        key.set_contents_from_string('bar')
    # wait for sync
    zone_bucket_checkpoint(ps_zone.zone, master_zone.zone, bucket_name)

    # get the create events from the subscription
    result, _ = sub_conf.get_events()
    events = json.loads(result)
    for event in events['events']:
        log.debug('Event: objname: "' + str(event['info']['key']['name']) + '" type: "' + str(event['event']) + '"')
    keys = list(bucket.list())
    # TODO: use exact match
    verify_events_by_elements(events, keys, exact_match=False)
    # delete objects from the bucket
    for key in bucket.list():
        key.delete()
    # wait for sync
    zone_bucket_checkpoint(ps_zone.zone, master_zone.zone, bucket_name)

    # get the delete events from the subscriptions
    #result, _ = sub_conf.get_events()
    #for event in events['events']:
    #    log.debug('Event: objname: "' + str(event['info']['key']['name']) + '" type: "' + str(event['event']) + '"')
    # TODO: check deletions
    # TODO: use exact match
    # verify_events_by_elements(events, keys, exact_match=False, deletions=True)
    # we should see the creations as well as the deletions
    # delete subscription
    _, status = sub_conf.del_config()
    assert_equal(status/100, 2)
    result, status = sub_conf.get_config()
    parsed_result = json.loads(result)
    assert_equal(parsed_result['topic'], '')
    # TODO should return 404
    # assert_equal(status, 404)

    # cleanup
    notification_conf.del_config()
    topic_conf.del_config()
    master_zone.delete_bucket(bucket_name)

def test_ps_incremental_sync():
    """ test that events are only sent on incremental sync """
    master_zone, ps_zone = init_env()
    bucket_name = gen_bucket_name()
    topic_name = bucket_name+TOPIC_SUFFIX

    # create topic
    topic_conf = PSTopic(ps_zone.conn, topic_name)
    topic_conf.set_config()
    # create bucket on the first of the rados zones
    bucket = master_zone.create_bucket(bucket_name)
    # create objects in the bucket
    number_of_objects = 10
    for i in range(0, number_of_objects):
        key = bucket.new_key(str(i))
        key.set_contents_from_string('foo')
    # wait for sync
    zone_bucket_checkpoint(ps_zone.zone, master_zone.zone, bucket_name)
    # create notifications
    notification_conf = PSNotification(ps_zone.conn, bucket_name,
                                       topic_name)
    _, status = notification_conf.set_config()
    assert_equal(status/100, 2)
    # create subscription
    sub_conf = PSSubscription(ps_zone.conn, bucket_name+SUB_SUFFIX,
                              topic_name)
    _, status = sub_conf.set_config()
    assert_equal(status/100, 2)
    
    # create more objects in the bucket
    for i in range(number_of_objects, 2*number_of_objects):
        key = bucket.new_key(str(i))
        key.set_contents_from_string('bar')
    # wait for sync
    zone_bucket_checkpoint(ps_zone.zone, master_zone.zone, bucket_name)

    # get the create events from the subscription
    result, _ = sub_conf.get_events()
    events = json.loads(result)
    count = 0
    for event in events['events']:
        log.debug('Event: objname: "' + str(event['info']['key']['name']) + '" type: "' + str(event['event']) + '"')
        count += 1
   
    # make sure we have 10 and not 20 events
    assert_equal(count, number_of_objects)

    # cleanup
    for key in bucket.list():
        key.delete()
    sub_conf.del_config()
    notification_conf.del_config()
    topic_conf.del_config()
    master_zone.delete_bucket(bucket_name)

def test_ps_event_type_subscription():
    """ test subscriptions for different events """
    master_zone, ps_zone = init_env()
    bucket_name = gen_bucket_name()

    # create topic for objects creation
    topic_create_name = bucket_name+TOPIC_SUFFIX+'_create'
    topic_create_conf = PSTopic(ps_zone.conn, topic_create_name)
    topic_create_conf.set_config()
    # create topic for objects deletion
    topic_delete_name = bucket_name+TOPIC_SUFFIX+'_delete'
    topic_delete_conf = PSTopic(ps_zone.conn, topic_delete_name)
    topic_delete_conf.set_config()
    # create topic for all events
    topic_name = bucket_name+TOPIC_SUFFIX+'_all'
    topic_conf = PSTopic(ps_zone.conn, topic_name)
    topic_conf.set_config()
    # create bucket on the first of the rados zones
    bucket = master_zone.create_bucket(bucket_name)
    # wait for sync
    zone_meta_checkpoint(ps_zone.zone)
    zone_bucket_checkpoint(ps_zone.zone, master_zone.zone, bucket_name)
    # create notifications for objects creation
    notification_create_conf = PSNotification(ps_zone.conn, bucket_name,
                                              topic_create_name, "OBJECT_CREATE")
    _, status = notification_create_conf.set_config()
    assert_equal(status/100, 2)
    # create notifications for objects deletion
    notification_delete_conf = PSNotification(ps_zone.conn, bucket_name,
                                              topic_delete_name, "OBJECT_DELETE")
    _, status = notification_delete_conf.set_config()
    assert_equal(status/100, 2)
    # create notifications for all events
    notification_conf = PSNotification(ps_zone.conn, bucket_name,
                                       topic_name, "OBJECT_DELETE,OBJECT_CREATE")
    _, status = notification_conf.set_config()
    assert_equal(status/100, 2)
    # create subscription for objects creation
    sub_create_conf = PSSubscription(ps_zone.conn, bucket_name+SUB_SUFFIX+'_create',
                                     topic_create_name)
    _, status = sub_create_conf.set_config()
    assert_equal(status/100, 2)
    # create subscription for objects deletion
    sub_delete_conf = PSSubscription(ps_zone.conn, bucket_name+SUB_SUFFIX+'_delete',
                                     topic_delete_name)
    _, status = sub_delete_conf.set_config()
    assert_equal(status/100, 2)
    # create subscription for all events
    sub_conf = PSSubscription(ps_zone.conn, bucket_name+SUB_SUFFIX+'_all',
                              topic_name)
    _, status = sub_conf.set_config()
    assert_equal(status/100, 2)
    # create objects in the bucket
    number_of_objects = 10
    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        key.set_contents_from_string('bar')
    # wait for sync
    zone_bucket_checkpoint(ps_zone.zone, master_zone.zone, bucket_name)

    # get the events from the creation subscription
    result, _ = sub_create_conf.get_events()
    events = json.loads(result)
    for event in events['events']:
        log.debug('Event (OBJECT_CREATE): objname: "' + str(event['info']['key']['name']) +
                  '" type: "' + str(event['event']) + '"')
    keys = list(bucket.list())
    # TODO: use exact match
    verify_events_by_elements(events, keys, exact_match=False)
    # get the events from the deletions subscription
    result, _ = sub_delete_conf.get_events()
    events = json.loads(result)
    for event in events['events']:
        log.debug('Event (OBJECT_DELETE): objname: "' + str(event['info']['key']['name']) +
                  '" type: "' + str(event['event']) + '"')
    assert_equal(len(events['events']), 0)
    # get the events from the all events subscription
    result, _ = sub_conf.get_events()
    events = json.loads(result)
    for event in events['events']:
        log.debug('Event (OBJECT_CREATE,OBJECT_DELETE): objname: "' +
                  str(event['info']['key']['name']) + '" type: "' + str(event['event']) + '"')
    # TODO: use exact match
    verify_events_by_elements(events, keys, exact_match=False)
    # delete objects from the bucket
    for key in bucket.list():
        key.delete()
    # wait for sync
    zone_bucket_checkpoint(ps_zone.zone, master_zone.zone, bucket_name)
    log.debug("Event (OBJECT_DELETE) synced")

    # get the events from the creations subscription
    result, _ = sub_create_conf.get_events()
    events = json.loads(result)
    for event in events['events']:
        log.debug('Event (OBJECT_CREATE): objname: "' + str(event['info']['key']['name']) +
                  '" type: "' + str(event['event']) + '"')
    # deletions should not change the creation events
    # TODO: use exact match
    verify_events_by_elements(events, keys, exact_match=False)
    # get the events from the deletions subscription
    result, _ = sub_delete_conf.get_events()
    events = json.loads(result)
    for event in events['events']:
        log.debug('Event (OBJECT_DELETE): objname: "' + str(event['info']['key']['name']) +
                  '" type: "' + str(event['event']) + '"')
    # only deletions should be listed here
    # TODO: use exact match
    verify_events_by_elements(events, keys, exact_match=False, deletions=True)
    # get the events from the all events subscription
    result, _ = sub_create_conf.get_events()
    events = json.loads(result)
    for event in events['events']:
        log.debug('Event (OBJECT_CREATE,OBJECT_DELETE): objname: "' + str(event['info']['key']['name']) +
                  '" type: "' + str(event['event']) + '"')
    # both deletions and creations should be here
    # TODO: use exact match
    verify_events_by_elements(events, keys, exact_match=False, deletions=False)
    # verify_events_by_elements(events, keys, exact_match=False, deletions=True)
    # TODO: (1) test deletions (2) test overall number of events

    # test subscription deletion when topic is specified
    _, status = sub_create_conf.del_config(topic=True)
    assert_equal(status/100, 2)
    _, status = sub_delete_conf.del_config(topic=True)
    assert_equal(status/100, 2)
    _, status = sub_conf.del_config(topic=True)
    assert_equal(status/100, 2)

    # cleanup
    notification_create_conf.del_config()
    notification_delete_conf.del_config()
    notification_conf.del_config()
    topic_create_conf.del_config()
    topic_delete_conf.del_config()
    topic_conf.del_config()
    master_zone.delete_bucket(bucket_name)


def test_ps_event_fetching():
    """ test incremental fetching of events from a subscription """
    master_zone, ps_zone = init_env()
    bucket_name = gen_bucket_name()
    topic_name = bucket_name+TOPIC_SUFFIX

    # create topic
    topic_conf = PSTopic(ps_zone.conn, topic_name)
    topic_conf.set_config()
    # create bucket on the first of the rados zones
    bucket = master_zone.create_bucket(bucket_name)
    # wait for sync
    zone_meta_checkpoint(ps_zone.zone)
    # create notifications
    notification_conf = PSNotification(ps_zone.conn, bucket_name,
                                       topic_name)
    _, status = notification_conf.set_config()
    assert_equal(status/100, 2)
    # create subscription
    sub_conf = PSSubscription(ps_zone.conn, bucket_name+SUB_SUFFIX,
                              topic_name)
    _, status = sub_conf.set_config()
    assert_equal(status/100, 2)
    # create objects in the bucket
    number_of_objects = 100
    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        key.set_contents_from_string('bar')
    # wait for sync
    zone_bucket_checkpoint(ps_zone.zone, master_zone.zone, bucket_name)
    max_events = 15
    total_events_count = 0
    next_marker = None
    all_events = []
    while True:
        # get the events from the subscription
        result, _ = sub_conf.get_events(max_events, next_marker)
        events = json.loads(result)
        total_events_count += len(events['events'])
        all_events.extend(events['events'])
        next_marker = events['next_marker']
        for event in events['events']:
            log.debug('Event: objname: "' + str(event['info']['key']['name']) + '" type: "' + str(event['event']) + '"')
        if next_marker == '':
            break
    keys = list(bucket.list())
    # TODO: use exact match
    verify_events_by_elements({'events': all_events}, keys, exact_match=False)

    # cleanup
    sub_conf.del_config()
    notification_conf.del_config()
    topic_conf.del_config()
    for key in bucket.list():
        key.delete()
    master_zone.delete_bucket(bucket_name)


def test_ps_event_acking():
    """ test acking of some events in a subscription """
    master_zone, ps_zone = init_env()
    bucket_name = gen_bucket_name()
    topic_name = bucket_name+TOPIC_SUFFIX

    # create topic
    topic_conf = PSTopic(ps_zone.conn, topic_name)
    topic_conf.set_config()
    # create bucket on the first of the rados zones
    bucket = master_zone.create_bucket(bucket_name)
    # wait for sync
    zone_meta_checkpoint(ps_zone.zone)
    # create notifications
    notification_conf = PSNotification(ps_zone.conn, bucket_name,
                                       topic_name)
    _, status = notification_conf.set_config()
    assert_equal(status/100, 2)
    # create subscription
    sub_conf = PSSubscription(ps_zone.conn, bucket_name+SUB_SUFFIX,
                              topic_name)
    _, status = sub_conf.set_config()
    assert_equal(status/100, 2)
    # create objects in the bucket
    number_of_objects = 10
    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        key.set_contents_from_string('bar')
    # wait for sync
    zone_bucket_checkpoint(ps_zone.zone, master_zone.zone, bucket_name)

    # get the create events from the subscription
    result, _ = sub_conf.get_events()
    events = json.loads(result)
    original_number_of_events = len(events)
    for event in events['events']:
        log.debug('Event (before ack)  id: "' + str(event['id']) + '"')
    keys = list(bucket.list())
    # TODO: use exact match
    verify_events_by_elements(events, keys, exact_match=False)
    # ack half of the  events
    events_to_ack = number_of_objects/2
    for event in events['events']:
        if events_to_ack == 0:
            break
        _, status = sub_conf.ack_events(event['id'])
        assert_equal(status/100, 2)
        events_to_ack -= 1

    # verify that acked events are gone
    result, _ = sub_conf.get_events()
    events = json.loads(result)
    for event in events['events']:
        log.debug('Event (after ack) id: "' + str(event['id']) + '"')
    assert len(events) >= (original_number_of_events - number_of_objects/2)

    # cleanup
    sub_conf.del_config()
    notification_conf.del_config()
    topic_conf.del_config()
    for key in bucket.list():
        key.delete()
    master_zone.delete_bucket(bucket_name)


def test_ps_creation_triggers():
    """ test object creation notifications in using put/copy/post """
    master_zone, ps_zone = init_env()
    bucket_name = gen_bucket_name()
    topic_name = bucket_name+TOPIC_SUFFIX

    # create topic
    topic_conf = PSTopic(ps_zone.conn, topic_name)
    topic_conf.set_config()
    # create bucket on the first of the rados zones
    bucket = master_zone.create_bucket(bucket_name)
    # wait for sync
    zone_meta_checkpoint(ps_zone.zone)
    # create notifications
    notification_conf = PSNotification(ps_zone.conn, bucket_name,
                                       topic_name)
    _, status = notification_conf.set_config()
    assert_equal(status/100, 2)
    # create subscription
    sub_conf = PSSubscription(ps_zone.conn, bucket_name+SUB_SUFFIX,
                              topic_name)
    _, status = sub_conf.set_config()
    assert_equal(status/100, 2)
    # create objects in the bucket using PUT
    key = bucket.new_key('put')
    key.set_contents_from_string('bar')
    # create objects in the bucket using COPY
    bucket.copy_key('copy', bucket.name, key.name)
    # create objects in the bucket using multi-part upload
    fp = tempfile.TemporaryFile(mode='w')
    fp.write('bar')
    fp.close()
    uploader = bucket.initiate_multipart_upload('multipart')
    fp = tempfile.NamedTemporaryFile(mode='r')
    uploader.upload_part_from_file(fp, 1)
    uploader.complete_upload()
    fp.close()
    # wait for sync
    zone_bucket_checkpoint(ps_zone.zone, master_zone.zone, bucket_name)

    # get the create events from the subscription
    result, _ = sub_conf.get_events()
    events = json.loads(result)
    for event in events['events']:
        log.debug('Event key: "' + str(event['info']['key']['name']) + '" type: "' + str(event['event']) + '"')

    # TODO: verify the specific 3 keys: 'put', 'copy' and 'multipart'
    assert len(events['events']) >= 3
    # cleanup
    sub_conf.del_config()
    notification_conf.del_config()
    topic_conf.del_config()
    for key in bucket.list():
        key.delete()
    master_zone.delete_bucket(bucket_name)


def test_ps_s3_creation_triggers_on_master():
    """ test object creation s3 notifications in using put/copy/post on master"""
    if skip_push_tests:
        return SkipTest("PubSub push tests don't run in teuthology")
    hostname = get_ip()
    proc = init_rabbitmq()
    if proc is  None:
        return SkipTest('end2end amqp tests require rabbitmq-server installed')
    master_zone, _ = init_env(require_ps=False)
    realm = get_realm()
    zonegroup = realm.master_zonegroup()
    
    # create bucket
    bucket_name = gen_bucket_name()
    bucket = master_zone.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX

    # start amqp receiver
    exchange = 'ex1'
    task, receiver = create_amqp_receiver_thread(exchange, topic_name)
    task.start()

    # create s3 topic
    endpoint_address = 'amqp://' + hostname
    endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=' + exchange +'&amqp-ack-level=broker'
    topic_conf = PSTopicS3(master_zone.conn, topic_name, zonegroup.name, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()
    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name,'TopicArn': topic_arn,
                        'Events': ['s3:ObjectCreated:Put', 's3:ObjectCreated:Copy']
                       }]

    s3_notification_conf = PSNotificationS3(master_zone.conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)

    # create objects in the bucket using PUT
    key = bucket.new_key('put')
    key.set_contents_from_string('bar')
    # create objects in the bucket using COPY
    bucket.copy_key('copy', bucket.name, key.name)
    # create objects in the bucket using multi-part upload
    fp = tempfile.TemporaryFile(mode='w')
    fp.write('bar')
    fp.close()
    uploader = bucket.initiate_multipart_upload('multipart')
    fp = tempfile.NamedTemporaryFile(mode='r')
    uploader.upload_part_from_file(fp, 1)
    uploader.complete_upload()
    fp.close()

    print('wait for 5sec for the messages...')
    time.sleep(5)

    # check amqp receiver
    keys = list(bucket.list())
    receiver.verify_s3_events(keys, exact_match=True)

    # cleanup
    stop_amqp_receiver(receiver, task)
    s3_notification_conf.del_config()
    topic_conf.del_config()
    for key in bucket.list():
        key.delete()
    # delete the bucket
    master_zone.delete_bucket(bucket_name)
    clean_rabbitmq(proc)


def test_ps_s3_multipart_on_master():
    """ test multipart object upload on master"""
    if skip_push_tests:
        return SkipTest("PubSub push tests don't run in teuthology")
    hostname = get_ip()
    proc = init_rabbitmq()
    if proc is  None:
        return SkipTest('end2end amqp tests require rabbitmq-server installed')
    master_zone, _ = init_env(require_ps=False)
    realm = get_realm()
    zonegroup = realm.master_zonegroup()
    
    # create bucket
    bucket_name = gen_bucket_name()
    bucket = master_zone.create_bucket(bucket_name)
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
    topic_conf1 = PSTopicS3(master_zone.conn, topic_name+'_1', zonegroup.name, endpoint_args=endpoint_args)
    topic_arn1 = topic_conf1.set_config()
    topic_conf2 = PSTopicS3(master_zone.conn, topic_name+'_2', zonegroup.name, endpoint_args=endpoint_args)
    topic_arn2 = topic_conf2.set_config()
    topic_conf3 = PSTopicS3(master_zone.conn, topic_name+'_3', zonegroup.name, endpoint_args=endpoint_args)
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
    s3_notification_conf = PSNotificationS3(master_zone.conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)

    # create objects in the bucket using multi-part upload
    fp = tempfile.NamedTemporaryFile(mode='w+b')
    object_size = 10*1024*1024
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
    assert_equal(len(events), 3)

    events = receiver2.get_and_reset_events()
    assert_equal(len(events), 1)
    assert_equal(events[0]['Records'][0]['eventName'], 's3:ObjectCreated:Post')
    assert_equal(events[0]['Records'][0]['s3']['configurationId'], notification_name+'_2')

    events = receiver3.get_and_reset_events()
    assert_equal(len(events), 1)
    assert_equal(events[0]['Records'][0]['eventName'], 's3:ObjectCreated:CompleteMultipartUpload')
    assert_equal(events[0]['Records'][0]['s3']['configurationId'], notification_name+'_3')
    print(events[0]['Records'][0]['s3']['object']['size'])

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
    master_zone.delete_bucket(bucket_name)
    clean_rabbitmq(proc)


def test_ps_versioned_deletion():
    """ test notification of deletion markers """
    master_zone, ps_zone = init_env()
    bucket_name = gen_bucket_name()
    topic_name = bucket_name+TOPIC_SUFFIX

    # create topics
    topic_conf1 = PSTopic(ps_zone.conn, topic_name+'_1')
    _, status = topic_conf1.set_config()
    assert_equal(status/100, 2)
    topic_conf2 = PSTopic(ps_zone.conn, topic_name+'_2')
    _, status = topic_conf2.set_config()
    assert_equal(status/100, 2)
    
    # create bucket on the first of the rados zones
    bucket = master_zone.create_bucket(bucket_name)
    bucket.configure_versioning(True)
    
    # wait for sync
    zone_meta_checkpoint(ps_zone.zone)
    
    # create notifications
    event_type1 = 'OBJECT_DELETE'
    notification_conf1 = PSNotification(ps_zone.conn, bucket_name,
                                        topic_name+'_1',
                                        event_type1)
    _, status = notification_conf1.set_config()
    assert_equal(status/100, 2)
    event_type2 = 'DELETE_MARKER_CREATE'
    notification_conf2 = PSNotification(ps_zone.conn, bucket_name,
                                        topic_name+'_2',
                                        event_type2)
    _, status = notification_conf2.set_config()
    assert_equal(status/100, 2)
    
    # create subscriptions
    sub_conf1 = PSSubscription(ps_zone.conn, bucket_name+SUB_SUFFIX+'_1',
                               topic_name+'_1')
    _, status = sub_conf1.set_config()
    assert_equal(status/100, 2)
    sub_conf2 = PSSubscription(ps_zone.conn, bucket_name+SUB_SUFFIX+'_2',
                               topic_name+'_2')
    _, status = sub_conf2.set_config()
    assert_equal(status/100, 2)
    
    # create objects in the bucket
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    v1 = key.version_id
    key.set_contents_from_string('kaboom')
    v2 = key.version_id
    # create deletion marker
    delete_marker_key = bucket.delete_key(key.name)
    
    # wait for sync
    zone_bucket_checkpoint(ps_zone.zone, master_zone.zone, bucket_name)

    # delete the deletion marker
    delete_marker_key.delete()
    # delete versions
    bucket.delete_key(key.name, version_id=v2)
    bucket.delete_key(key.name, version_id=v1)
    
    # wait for sync
    zone_bucket_checkpoint(ps_zone.zone, master_zone.zone, bucket_name)

    # get the delete events from the subscription
    result, _ = sub_conf1.get_events()
    events = json.loads(result)
    for event in events['events']:
        log.debug('Event key: "' + str(event['info']['key']['name']) + '" type: "' + str(event['event']) + '"')
        assert_equal(str(event['event']), event_type1)

    result, _ = sub_conf2.get_events()
    events = json.loads(result)
    for event in events['events']:
        log.debug('Event key: "' + str(event['info']['key']['name']) + '" type: "' + str(event['event']) + '"')
        assert_equal(str(event['event']), event_type2)

    # cleanup
    # follwing is needed for the cleanup in the case of 3-zones
    # see: http://tracker.ceph.com/issues/39142
    realm = get_realm()
    zonegroup = realm.master_zonegroup()
    zonegroup_conns = ZonegroupConns(zonegroup)
    try:
        zonegroup_bucket_checkpoint(zonegroup_conns, bucket_name)
        master_zone.delete_bucket(bucket_name)
    except:
        log.debug('zonegroup_bucket_checkpoint failed, cannot delete bucket')
    sub_conf1.del_config()
    sub_conf2.del_config()
    notification_conf1.del_config()
    notification_conf2.del_config()
    topic_conf1.del_config()
    topic_conf2.del_config()


def test_ps_s3_metadata_on_master():
    """ test s3 notification of metadata on master """
    if skip_push_tests:
        return SkipTest("PubSub push tests don't run in teuthology")
    hostname = get_ip()
    proc = init_rabbitmq()
    if proc is  None:
        return SkipTest('end2end amqp tests require rabbitmq-server installed')
    master_zone, _ = init_env(require_ps=False)
    realm = get_realm()
    zonegroup = realm.master_zonegroup()
    
    # create bucket
    bucket_name = gen_bucket_name()
    bucket = master_zone.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX

    # start amqp receiver
    exchange = 'ex1'
    task, receiver = create_amqp_receiver_thread(exchange, topic_name)
    task.start()

    # create s3 topic
    endpoint_address = 'amqp://' + hostname
    endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=' + exchange +'&amqp-ack-level=routable'
    topic_conf = PSTopicS3(master_zone.conn, topic_name, zonegroup.name, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()
    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    meta_key = 'meta1'
    meta_value = 'This is my metadata value'
    meta_prefix = 'x-amz-meta-'
    topic_conf_list = [{'Id': notification_name,'TopicArn': topic_arn,
        'Events': ['s3:ObjectCreated:*', 's3:ObjectRemoved:*'],
        'Filter': {
            'Metadata': {
                'FilterRules': [{'Name': meta_prefix+meta_key, 'Value': meta_value}]
            }
        }
    }]

    s3_notification_conf = PSNotificationS3(master_zone.conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)

    # create objects in the bucket
    key_name = 'foo'
    key = bucket.new_key(key_name)
    key.set_metadata(meta_key, meta_value)
    key.set_contents_from_string('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')
    
    # create objects in the bucket using COPY
    bucket.copy_key('copy_of_foo', bucket.name, key.name)
    # create objects in the bucket using multi-part upload
    fp = tempfile.NamedTemporaryFile(mode='w')
    fp.write('bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb')
    fp.close()
    uploader = bucket.initiate_multipart_upload('multipart_foo', 
            metadata={meta_key: meta_value})
    fp = tempfile.TemporaryFile(mode='r')
    uploader.upload_part_from_file(fp, 1)
    uploader.complete_upload()
    fp.close()
    print('wait for 5sec for the messages...')
    time.sleep(5)
    # check amqp receiver
    event_count = 0
    for event in receiver.get_and_reset_events():
        s3_event = event['Records'][0]['s3']
        assert_equal(s3_event['object']['metadata'][0]['key'], meta_prefix+meta_key)
        assert_equal(s3_event['object']['metadata'][0]['val'], meta_value)
        event_count +=1

    # only PUT and POST has the metadata value
    assert_equal(event_count, 2)

    # delete objects
    for key in bucket.list():
        key.delete()
    print('wait for 5sec for the messages...')
    time.sleep(5)
    # check amqp receiver
    event_count = 0
    for event in receiver.get_and_reset_events():
        s3_event = event['Records'][0]['s3']
        assert_equal(s3_event['object']['metadata'][0]['key'], meta_prefix+meta_key)
        assert_equal(s3_event['object']['metadata'][0]['val'], meta_value)
        event_count +=1

    # all 3 object has metadata when deleted
    assert_equal(event_count, 3)

    # cleanup
    stop_amqp_receiver(receiver, task)
    s3_notification_conf.del_config()
    topic_conf.del_config()
    # delete the bucket
    master_zone.delete_bucket(bucket_name)
    clean_rabbitmq(proc)


def test_ps_s3_tags_on_master():
    """ test s3 notification of tags on master """
    if skip_push_tests:
        return SkipTest("PubSub push tests don't run in teuthology")
    hostname = get_ip()
    proc = init_rabbitmq()
    if proc is  None:
        return SkipTest('end2end amqp tests require rabbitmq-server installed')
    master_zone, _ = init_env(require_ps=False)
    realm = get_realm()
    zonegroup = realm.master_zonegroup()
    
    # create bucket
    bucket_name = gen_bucket_name()
    bucket = master_zone.create_bucket(bucket_name)
    topic_name = bucket_name + TOPIC_SUFFIX

    # start amqp receiver
    exchange = 'ex1'
    task, receiver = create_amqp_receiver_thread(exchange, topic_name)
    task.start()

    # create s3 topic
    endpoint_address = 'amqp://' + hostname
    endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=' + exchange +'&amqp-ack-level=routable'
    topic_conf = PSTopicS3(master_zone.conn, topic_name, zonegroup.name, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()
    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name,'TopicArn': topic_arn,
        'Events': ['s3:ObjectCreated:*', 's3:ObjectRemoved:*'],
        'Filter': {
            'Tags': {
                'FilterRules': [{'Name': 'hello', 'Value': 'world'}]
            }
        }
    }]

    s3_notification_conf = PSNotificationS3(master_zone.conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)

    # create objects in the bucket with tags
    tags = 'hello=world&ka=boom'
    key_name1 = 'key1'
    put_object_tagging(master_zone.conn, bucket_name, key_name1, tags)
    tags = 'foo=bar&ka=boom'
    key_name2 = 'key2'
    put_object_tagging(master_zone.conn, bucket_name, key_name2, tags)
    key_name3 = 'key3'
    key = bucket.new_key(key_name3)
    key.set_contents_from_string('bar')
    # create objects in the bucket using COPY
    bucket.copy_key('copy_of_'+key_name1, bucket.name, key_name1)
    print('wait for 5sec for the messages...')
    time.sleep(5)
    expected_tags = [{'val': 'world', 'key': 'hello'}, {'val': 'boom', 'key': 'ka'}]
    # check amqp receiver
    for event in receiver.get_and_reset_events():
        obj_tags =  event['Records'][0]['s3']['object']['tags']
        assert_equal(obj_tags[0], expected_tags[0])

    # delete the objects
    for key in bucket.list():
        key.delete()
    print('wait for 5sec for the messages...')
    time.sleep(5)
    # check amqp receiver
    for event in receiver.get_and_reset_events():
        obj_tags =  event['Records'][0]['s3']['object']['tags']
        assert_equal(obj_tags[0], expected_tags[0])

    # cleanup
    stop_amqp_receiver(receiver, task)
    s3_notification_conf.del_config()
    topic_conf.del_config()
    # delete the bucket
    master_zone.delete_bucket(bucket_name)
    clean_rabbitmq(proc)


def test_ps_s3_versioning_on_master():
    """ test s3 notification of object versions """
    if skip_push_tests:
        return SkipTest("PubSub push tests don't run in teuthology")
    hostname = get_ip()
    proc = init_rabbitmq()
    if proc is None:
        return SkipTest('end2end amqp tests require rabbitmq-server installed')
    master_zone, _ = init_env(require_ps=False)
    realm = get_realm()
    zonegroup = realm.master_zonegroup()

    # create bucket
    bucket_name = gen_bucket_name()
    bucket = master_zone.create_bucket(bucket_name)
    bucket.configure_versioning(True)
    topic_name = bucket_name + TOPIC_SUFFIX

    # start amqp receiver
    exchange = 'ex1'
    task, receiver = create_amqp_receiver_thread(exchange, topic_name)
    task.start()

    # create s3 topic
    endpoint_address = 'amqp://' + hostname
    endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=' + exchange +'&amqp-ack-level=broker'
    topic_conf = PSTopicS3(master_zone.conn, topic_name, zonegroup.name, endpoint_args=endpoint_args)
    topic_arn = topic_conf.set_config()
    # create notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name, 'TopicArn': topic_arn,
                        'Events': []
                       }]
    s3_notification_conf = PSNotificationS3(master_zone.conn, bucket_name, topic_conf_list)
    _, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)

    # create objects in the bucket
    key_value = 'foo'
    key = bucket.new_key(key_value)
    key.set_contents_from_string('hello')
    ver1 = key.version_id
    key.set_contents_from_string('world')
    ver2 = key.version_id

    print('wait for 5sec for the messages...')
    time.sleep(5)

    # check amqp receiver
    events = receiver.get_and_reset_events()
    num_of_versions = 0
    for event_list in events:
        for event in event_list['Records']:
            assert_equal(event['s3']['object']['key'], key_value)
            version = event['s3']['object']['versionId']
            num_of_versions += 1
            if version not in (ver1, ver2):
                print('version mismatch: '+version+' not in: ('+ver1+', '+ver2+')')
                assert_equal(1, 0)
            else:
                print('version ok: '+version+' in: ('+ver1+', '+ver2+')')

    assert_equal(num_of_versions, 2)

    # cleanup
    stop_amqp_receiver(receiver, task)
    s3_notification_conf.del_config()
    topic_conf.del_config()
    # delete the bucket
    bucket.delete_key(key.name, version_id=ver2)
    bucket.delete_key(key.name, version_id=ver1)
    master_zone.delete_bucket(bucket_name)
    clean_rabbitmq(proc)


def test_ps_s3_versioned_deletion_on_master():
    """ test s3 notification of deletion markers on master """
    if skip_push_tests:
        return SkipTest("PubSub push tests don't run in teuthology")
    hostname = get_ip()
    proc = init_rabbitmq()
    if proc is  None:
        return SkipTest('end2end amqp tests require rabbitmq-server installed')
    master_zone, _ = init_env(require_ps=False)
    realm = get_realm()
    zonegroup = realm.master_zonegroup()
    
    # create bucket
    bucket_name = gen_bucket_name()
    bucket = master_zone.create_bucket(bucket_name)
    bucket.configure_versioning(True)
    topic_name = bucket_name + TOPIC_SUFFIX

    # start amqp receiver
    exchange = 'ex1'
    task, receiver = create_amqp_receiver_thread(exchange, topic_name)
    task.start()

    # create s3 topic
    endpoint_address = 'amqp://' + hostname
    endpoint_args = 'push-endpoint='+endpoint_address+'&amqp-exchange=' + exchange +'&amqp-ack-level=broker'
    topic_conf = PSTopicS3(master_zone.conn, topic_name, zonegroup.name, endpoint_args=endpoint_args)
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
    s3_notification_conf = PSNotificationS3(master_zone.conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)

    # create objects in the bucket
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    v1 = key.version_id
    key.set_contents_from_string('kaboom')
    v2 = key.version_id
    # create delete marker (non versioned deletion)
    delete_marker_key = bucket.delete_key(key.name)
    
    time.sleep(1)
    
    # versioned deletion
    bucket.delete_key(key.name, version_id=v2)
    bucket.delete_key(key.name, version_id=v1)
    delete_marker_key.delete()

    print('wait for 5sec for the messages...')
    time.sleep(5)

    # check amqp receiver
    events = receiver.get_and_reset_events()
    delete_events = 0
    delete_marker_create_events = 0
    for event_list in events:
        for event in event_list['Records']:
            if event['eventName'] == 's3:ObjectRemoved:Delete':
                delete_events += 1
                assert event['s3']['configurationId'] in [notification_name+'_1', notification_name+'_3']
            if event['eventName'] == 's3:ObjectRemoved:DeleteMarkerCreated':
                delete_marker_create_events += 1
                assert event['s3']['configurationId'] in [notification_name+'_1', notification_name+'_2']
   
    # 3 key versions were deleted (v1, v2 and the deletion marker)
    # notified over the same topic via 2 notifications (1,3)
    assert_equal(delete_events, 3*2)
    # 1 deletion marker was created
    # notified over the same topic over 2 notifications (1,2)
    assert_equal(delete_marker_create_events, 1*2)

    # cleanup
    stop_amqp_receiver(receiver, task)
    s3_notification_conf.del_config()
    topic_conf.del_config()
    # delete the bucket
    master_zone.delete_bucket(bucket_name)
    clean_rabbitmq(proc)


def test_ps_push_http():
    """ test pushing to http endpoint """
    if skip_push_tests:
        return SkipTest("PubSub push tests don't run in teuthology")
    master_zone, ps_zone = init_env()
    bucket_name = gen_bucket_name()
    topic_name = bucket_name+TOPIC_SUFFIX

    # create random port for the http server
    host = get_ip()
    port = random.randint(10000, 20000)
    # start an http server in a separate thread
    http_server = StreamingHTTPServer(host, port)

    # create topic
    topic_conf = PSTopic(ps_zone.conn, topic_name)
    _, status = topic_conf.set_config()
    assert_equal(status/100, 2)
    # create bucket on the first of the rados zones
    bucket = master_zone.create_bucket(bucket_name)
    # wait for sync
    zone_meta_checkpoint(ps_zone.zone)
    # create notifications
    notification_conf = PSNotification(ps_zone.conn, bucket_name,
                                       topic_name)
    _, status = notification_conf.set_config()
    assert_equal(status/100, 2)
    # create subscription
    sub_conf = PSSubscription(ps_zone.conn, bucket_name+SUB_SUFFIX,
                              topic_name, endpoint='http://'+host+':'+str(port))
    _, status = sub_conf.set_config()
    assert_equal(status/100, 2)
    # create objects in the bucket
    number_of_objects = 10
    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        key.set_contents_from_string('bar')
    # wait for sync
    zone_bucket_checkpoint(ps_zone.zone, master_zone.zone, bucket_name)
    # check http server
    keys = list(bucket.list())
    # TODO: use exact match
    http_server.verify_events(keys, exact_match=False)

    # delete objects from the bucket
    for key in bucket.list():
        key.delete()
    # wait for sync
    zone_meta_checkpoint(ps_zone.zone)
    zone_bucket_checkpoint(ps_zone.zone, master_zone.zone, bucket_name)
    # check http server
    # TODO: use exact match
    http_server.verify_events(keys, deletions=True, exact_match=False)

    # cleanup
    sub_conf.del_config()
    notification_conf.del_config()
    topic_conf.del_config()
    master_zone.delete_bucket(bucket_name)
    http_server.close()


def test_ps_s3_push_http():
    """ test pushing to http endpoint s3 record format"""
    if skip_push_tests:
        return SkipTest("PubSub push tests don't run in teuthology")
    master_zone, ps_zone = init_env()
    bucket_name = gen_bucket_name()
    topic_name = bucket_name+TOPIC_SUFFIX

    # create random port for the http server
    host = get_ip()
    port = random.randint(10000, 20000)
    # start an http server in a separate thread
    http_server = StreamingHTTPServer(host, port)

    # create topic
    topic_conf = PSTopic(ps_zone.conn, topic_name,
                         endpoint='http://'+host+':'+str(port))
    result, status = topic_conf.set_config()
    assert_equal(status/100, 2)
    parsed_result = json.loads(result)
    topic_arn = parsed_result['arn']
    # create bucket on the first of the rados zones
    bucket = master_zone.create_bucket(bucket_name)
    # wait for sync
    zone_meta_checkpoint(ps_zone.zone)
    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name,
                        'TopicArn': topic_arn,
                        'Events': ['s3:ObjectCreated:*', 's3:ObjectRemoved:*']
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
    zone_bucket_checkpoint(ps_zone.zone, master_zone.zone, bucket_name)
    # check http server
    keys = list(bucket.list())
    # TODO: use exact match
    http_server.verify_s3_events(keys, exact_match=False)

    # delete objects from the bucket
    for key in bucket.list():
        key.delete()
    # wait for sync
    zone_meta_checkpoint(ps_zone.zone)
    zone_bucket_checkpoint(ps_zone.zone, master_zone.zone, bucket_name)
    # check http server
    # TODO: use exact match
    http_server.verify_s3_events(keys, deletions=True, exact_match=False)

    # cleanup
    s3_notification_conf.del_config()
    topic_conf.del_config()
    master_zone.delete_bucket(bucket_name)
    http_server.close()


def test_ps_push_amqp():
    """ test pushing to amqp endpoint """
    if skip_push_tests:
        return SkipTest("PubSub push tests don't run in teuthology")
    hostname = get_ip()
    proc = init_rabbitmq()
    if proc is  None:
        return SkipTest('end2end amqp tests require rabbitmq-server installed')
    master_zone, ps_zone = init_env()
    bucket_name = gen_bucket_name()
    topic_name = bucket_name+TOPIC_SUFFIX

    # create topic
    exchange = 'ex1'
    task, receiver = create_amqp_receiver_thread(exchange, topic_name)
    task.start()
    topic_conf = PSTopic(ps_zone.conn, topic_name)
    _, status = topic_conf.set_config()
    assert_equal(status/100, 2)
    # create bucket on the first of the rados zones
    bucket = master_zone.create_bucket(bucket_name)
    # wait for sync
    zone_meta_checkpoint(ps_zone.zone)
    # create notifications
    notification_conf = PSNotification(ps_zone.conn, bucket_name,
                                       topic_name)
    _, status = notification_conf.set_config()
    assert_equal(status/100, 2)
    # create subscription
    sub_conf = PSSubscription(ps_zone.conn, bucket_name+SUB_SUFFIX,
                              topic_name, endpoint='amqp://'+hostname,
                              endpoint_args='amqp-exchange='+exchange+'&amqp-ack-level=broker')
    _, status = sub_conf.set_config()
    assert_equal(status/100, 2)
    # create objects in the bucket
    number_of_objects = 10
    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        key.set_contents_from_string('bar')
    # wait for sync
    zone_bucket_checkpoint(ps_zone.zone, master_zone.zone, bucket_name)
    # check amqp receiver
    keys = list(bucket.list())
    # TODO: use exact match
    receiver.verify_events(keys, exact_match=False)

    # delete objects from the bucket
    for key in bucket.list():
        key.delete()
    # wait for sync
    zone_meta_checkpoint(ps_zone.zone)
    zone_bucket_checkpoint(ps_zone.zone, master_zone.zone, bucket_name)
    # check amqp receiver
    # TODO: use exact match
    receiver.verify_events(keys, deletions=True, exact_match=False)

    # cleanup
    stop_amqp_receiver(receiver, task)
    sub_conf.del_config()
    notification_conf.del_config()
    topic_conf.del_config()
    master_zone.delete_bucket(bucket_name)
    clean_rabbitmq(proc)


def test_ps_s3_push_amqp():
    """ test pushing to amqp endpoint s3 record format"""
    if skip_push_tests:
        return SkipTest("PubSub push tests don't run in teuthology")
    hostname = get_ip()
    proc = init_rabbitmq()
    if proc is  None:
        return SkipTest('end2end amqp tests require rabbitmq-server installed')
    master_zone, ps_zone = init_env()
    bucket_name = gen_bucket_name()
    topic_name = bucket_name+TOPIC_SUFFIX

    # create topic
    exchange = 'ex1'
    task, receiver = create_amqp_receiver_thread(exchange, topic_name)
    task.start()
    topic_conf = PSTopic(ps_zone.conn, topic_name,
                         endpoint='amqp://' + hostname,
                         endpoint_args='amqp-exchange=' + exchange + '&amqp-ack-level=none')
    result, status = topic_conf.set_config()
    assert_equal(status/100, 2)
    parsed_result = json.loads(result)
    topic_arn = parsed_result['arn']
    # create bucket on the first of the rados zones
    bucket = master_zone.create_bucket(bucket_name)
    # wait for sync
    zone_meta_checkpoint(ps_zone.zone)
    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name,
                        'TopicArn': topic_arn,
                        'Events': ['s3:ObjectCreated:*', 's3:ObjectRemoved:*']
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
    zone_bucket_checkpoint(ps_zone.zone, master_zone.zone, bucket_name)
    # check amqp receiver
    keys = list(bucket.list())
    # TODO: use exact match
    receiver.verify_s3_events(keys, exact_match=False)

    # delete objects from the bucket
    for key in bucket.list():
        key.delete()
    # wait for sync
    zone_meta_checkpoint(ps_zone.zone)
    zone_bucket_checkpoint(ps_zone.zone, master_zone.zone, bucket_name)
    # check amqp receiver
    # TODO: use exact match
    receiver.verify_s3_events(keys, deletions=True, exact_match=False)

    # cleanup
    stop_amqp_receiver(receiver, task)
    s3_notification_conf.del_config()
    topic_conf.del_config()
    master_zone.delete_bucket(bucket_name)
    clean_rabbitmq(proc)


def test_ps_delete_bucket():
    """ test notification status upon bucket deletion """
    master_zone, ps_zone = init_env()
    bucket_name = gen_bucket_name()
    # create bucket on the first of the rados zones
    bucket = master_zone.create_bucket(bucket_name)
    # wait for sync
    zone_meta_checkpoint(ps_zone.zone)
    topic_name = bucket_name + TOPIC_SUFFIX
    # create topic
    topic_name = bucket_name + TOPIC_SUFFIX
    topic_conf = PSTopic(ps_zone.conn, topic_name)
    response, status = topic_conf.set_config()
    assert_equal(status/100, 2)
    parsed_result = json.loads(response)
    topic_arn = parsed_result['arn']
    # create one s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name,
                        'TopicArn': topic_arn,
                        'Events': ['s3:ObjectCreated:*']
                       }]
    s3_notification_conf = PSNotificationS3(ps_zone.conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)

    # create non-s3 notification
    notification_conf = PSNotification(ps_zone.conn, bucket_name,
                                       topic_name)
    _, status = notification_conf.set_config()
    assert_equal(status/100, 2)

    # create objects in the bucket
    number_of_objects = 10
    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        key.set_contents_from_string('bar')
    # wait for bucket sync
    zone_bucket_checkpoint(ps_zone.zone, master_zone.zone, bucket_name)
    keys = list(bucket.list())
    # delete objects from the bucket
    for key in bucket.list():
        key.delete()
    # wait for bucket sync
    zone_bucket_checkpoint(ps_zone.zone, master_zone.zone, bucket_name)
    # delete the bucket
    master_zone.delete_bucket(bucket_name)
    # wait for meta sync
    zone_meta_checkpoint(ps_zone.zone)

    # get the events from the auto-generated subscription
    sub_conf = PSSubscription(ps_zone.conn, notification_name,
                              topic_name)
    result, _ = sub_conf.get_events()
    records = json.loads(result)
    # TODO: use exact match
    verify_s3_records_by_elements(records, keys, exact_match=False)

    # s3 notification is deleted with bucket
    _, status = s3_notification_conf.get_config(notification=notification_name)
    assert_equal(status, 404)
    # non-s3 notification is deleted with bucket
    _, status = notification_conf.get_config()
    assert_equal(status, 404)
    # cleanup
    sub_conf.del_config()
    topic_conf.del_config()


def test_ps_missing_topic():
    """ test creating a subscription when no topic info exists"""
    master_zone, ps_zone = init_env()
    bucket_name = gen_bucket_name()
    topic_name = bucket_name+TOPIC_SUFFIX

    # create bucket on the first of the rados zones
    master_zone.create_bucket(bucket_name)
    # wait for sync
    zone_meta_checkpoint(ps_zone.zone)
    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_arn = 'arn:aws:sns:::' + topic_name
    topic_conf_list = [{'Id': notification_name,
                        'TopicArn': topic_arn,
                        'Events': ['s3:ObjectCreated:*']
                       }]
    s3_notification_conf = PSNotificationS3(ps_zone.conn, bucket_name, topic_conf_list)
    try:
        s3_notification_conf.set_config()
    except:
        log.info('missing topic is expected')
    else:
        assert 'missing topic is expected'

    # cleanup
    master_zone.delete_bucket(bucket_name)


def test_ps_s3_topic_update():
    """ test updating topic associated with a notification"""
    if skip_push_tests:
        return SkipTest("PubSub push tests don't run in teuthology")
    rabbit_proc = init_rabbitmq()
    if rabbit_proc is  None:
        return SkipTest('end2end amqp tests require rabbitmq-server installed')
    master_zone, ps_zone = init_env()
    bucket_name = gen_bucket_name()
    topic_name = bucket_name+TOPIC_SUFFIX

    # create amqp topic
    hostname = get_ip()
    exchange = 'ex1'
    amqp_task, receiver = create_amqp_receiver_thread(exchange, topic_name)
    amqp_task.start()
    topic_conf = PSTopic(ps_zone.conn, topic_name,
                          endpoint='amqp://' + hostname,
                          endpoint_args='amqp-exchange=' + exchange + '&amqp-ack-level=none')
    result, status = topic_conf.set_config()
    assert_equal(status/100, 2)
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
    bucket = master_zone.create_bucket(bucket_name)
    # wait for sync
    zone_meta_checkpoint(ps_zone.zone)
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
    zone_bucket_checkpoint(ps_zone.zone, master_zone.zone, bucket_name)

    keys = list(bucket.list())
    # TODO: use exact match
    receiver.verify_s3_events(keys, exact_match=False)

    # update the same topic with new endpoint
    topic_conf = PSTopic(ps_zone.conn, topic_name,
                         endpoint='http://'+ hostname + ':' + str(port))
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
    zone_meta_checkpoint(ps_zone.zone)
    zone_bucket_checkpoint(ps_zone.zone, master_zone.zone, bucket_name)

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
    zone_meta_checkpoint(ps_zone.zone)
    zone_bucket_checkpoint(ps_zone.zone, master_zone.zone, bucket_name)

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
    master_zone.delete_bucket(bucket_name)
    http_server.close()
    clean_rabbitmq(rabbit_proc)


def test_ps_s3_notification_update():
    """ test updating the topic of a notification"""
    if skip_push_tests:
        return SkipTest("PubSub push tests don't run in teuthology")
    hostname = get_ip()
    rabbit_proc = init_rabbitmq()
    if rabbit_proc is  None:
        return SkipTest('end2end amqp tests require rabbitmq-server installed')

    master_zone, ps_zone = init_env()
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

    topic_conf1 = PSTopic(ps_zone.conn, topic_name1,
                          endpoint='amqp://' + hostname,
                          endpoint_args='amqp-exchange=' + exchange + '&amqp-ack-level=none')
    result, status = topic_conf1.set_config()
    parsed_result = json.loads(result)
    topic_arn1 = parsed_result['arn']
    assert_equal(status/100, 2)
    topic_conf2 = PSTopic(ps_zone.conn, topic_name2,
                          endpoint='http://'+hostname+':'+str(http_port))
    result, status = topic_conf2.set_config()
    parsed_result = json.loads(result)
    topic_arn2 = parsed_result['arn']
    assert_equal(status/100, 2)

    # create bucket on the first of the rados zones
    bucket = master_zone.create_bucket(bucket_name)
    # wait for sync
    zone_meta_checkpoint(ps_zone.zone)
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
    zone_bucket_checkpoint(ps_zone.zone, master_zone.zone, bucket_name)

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
    zone_meta_checkpoint(ps_zone.zone)
    zone_bucket_checkpoint(ps_zone.zone, master_zone.zone, bucket_name)

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
    master_zone.delete_bucket(bucket_name)
    http_server.close()
    clean_rabbitmq(rabbit_proc)


def test_ps_s3_multiple_topics_notification():
    """ test notification creation with multiple topics"""
    if skip_push_tests:
        return SkipTest("PubSub push tests don't run in teuthology")
    hostname = get_ip()
    rabbit_proc = init_rabbitmq()
    if rabbit_proc is  None:
        return SkipTest('end2end amqp tests require rabbitmq-server installed')

    master_zone, ps_zone = init_env()
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

    topic_conf1 = PSTopic(ps_zone.conn, topic_name1,
                          endpoint='amqp://' + hostname,
                          endpoint_args='amqp-exchange=' + exchange + '&amqp-ack-level=none')
    result, status = topic_conf1.set_config()
    parsed_result = json.loads(result)
    topic_arn1 = parsed_result['arn']
    assert_equal(status/100, 2)
    topic_conf2 = PSTopic(ps_zone.conn, topic_name2,
                          endpoint='http://'+hostname+':'+str(http_port))
    result, status = topic_conf2.set_config()
    parsed_result = json.loads(result)
    topic_arn2 = parsed_result['arn']
    assert_equal(status/100, 2)

    # create bucket on the first of the rados zones
    bucket = master_zone.create_bucket(bucket_name)
    # wait for sync
    zone_meta_checkpoint(ps_zone.zone)
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
    zone_bucket_checkpoint(ps_zone.zone, master_zone.zone, bucket_name)

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
    master_zone.delete_bucket(bucket_name)
    http_server.close()
    clean_rabbitmq(rabbit_proc)
