import logging
import json
import tempfile
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
from .zone_ps import PSTopic, PSNotification, PSSubscription, PSNotificationS3, print_connection_info
from multisite import User
from nose import SkipTest
from nose.tools import assert_not_equal, assert_equal

# configure logging for the tests module
log = logging.getLogger(__name__)

####################################
# utility functions for pubsub tests
####################################


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
        for event in events:
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


def verify_s3_records_by_elements(records, keys, exact_match=False, deletions=False):
    """ verify there is at least one record per element """
    err = ''
    for key in keys:
        key_found = False
        for record in records:
            if record['s3']['bucket']['name'] == key.bucket.name and \
               record['s3']['object']['key'] == key.name:
                if deletions and record['eventName'] == 'ObjectRemoved':
                    key_found = True
                    break
                elif not deletions and record['eventName'] == 'ObjectCreated':
                    key_found = True
                    break
        if not key_found:
            err = 'no ' + ('deletion' if deletions else 'creation') + ' event found for key: ' + str(key)
            for record in records:
                log.error(str(record['s3']['bucket']['name']) + ',' + str(record['s3']['object']['key']))
            assert False, err

    if not len(records) == len(keys):
        err = 'superfluous records are found'
        log.warning(err)
        if exact_match:
            for record in records:
                log.error(str(record['s3']['bucket']['name']) + ',' + str(record['s3']['object']['key']))
            assert False, err


def init_env():
    """initialize the environment"""
    check_ps_configured()

    realm = get_realm()
    zonegroup = realm.master_zonegroup()
    zonegroup_conns = ZonegroupConns(zonegroup)

    zonegroup_meta_checkpoint(zonegroup)

    ps_zones = []
    zones = []
    for conn in zonegroup_conns.zones:
        if is_ps_zone(conn):
            zone_meta_checkpoint(conn.zone)
            ps_zones.append(conn)
        elif not conn.zone.is_read_only():
            zones.append(conn)

    assert_not_equal(len(zones), 0)
    assert_not_equal(len(ps_zones), 0)
    return zones, ps_zones


TOPIC_SUFFIX = "_topic"
SUB_SUFFIX = "_sub"
NOTIFICATION_SUFFIX = "_notif"

##############
# pubsub tests
##############

def test_ps_info():
    """ log information for manual testing """
    return SkipTest("only used in manual testing")
    zones, ps_zones = init_env()
    realm = get_realm()
    zonegroup = realm.master_zonegroup()
    bucket_name = gen_bucket_name()
    # create bucket on the first of the rados zones
    bucket = zones[0].create_bucket(bucket_name)
    # create objects in the bucket
    number_of_objects = 10
    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        key.set_contents_from_string('bar')
    print('Zonegroup: ' + zonegroup.name)
    print('user: ' + get_user())
    print('tenant: ' + get_tenant())
    print('Master Zone')
    print_connection_info(zones[0].conn)
    print('PubSub Zone')
    print_connection_info(ps_zones[0].conn)
    print('Bucket: ' + bucket_name)


def test_ps_s3_notification_low_level():
    """ test low level implementation of s3 notifications """
    zones, ps_zones = init_env()
    bucket_name = gen_bucket_name()
    # create bucket on the first of the rados zones
    zones[0].create_bucket(bucket_name)
    # wait for sync
    zone_meta_checkpoint(ps_zones[0].zone)
    # create topic
    topic_name = bucket_name + TOPIC_SUFFIX
    topic_conf = PSTopic(ps_zones[0].conn, topic_name)
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
    s3_notification_conf = PSNotificationS3(ps_zones[0].conn, bucket_name, topic_conf_list)
    _, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)
    zone_meta_checkpoint(ps_zones[0].zone)
    # get auto-generated topic
    generated_topic_conf = PSTopic(ps_zones[0].conn, generated_topic_name)
    result, status = generated_topic_conf.get_config()
    parsed_result = json.loads(result)
    assert_equal(status/100, 2)
    assert_equal(parsed_result['topic']['name'], generated_topic_name)
    # get auto-generated notification
    notification_conf = PSNotification(ps_zones[0].conn, bucket_name,
                                       generated_topic_name)
    result, status = notification_conf.get_config()
    parsed_result = json.loads(result)
    assert_equal(status/100, 2)
    assert_equal(len(parsed_result['topics']), 1)
    # get auto-generated subscription
    sub_conf = PSSubscription(ps_zones[0].conn, notification_name,
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
    zones[0].delete_bucket(bucket_name)


def test_ps_s3_notification_records():
    """ test s3 records fetching """
    zones, ps_zones = init_env()
    bucket_name = gen_bucket_name()
    # create bucket on the first of the rados zones
    bucket = zones[0].create_bucket(bucket_name)
    # wait for sync
    zone_meta_checkpoint(ps_zones[0].zone)
    # create topic
    topic_name = bucket_name + TOPIC_SUFFIX
    topic_conf = PSTopic(ps_zones[0].conn, topic_name)
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
    s3_notification_conf = PSNotificationS3(ps_zones[0].conn, bucket_name, topic_conf_list)
    _, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)
    zone_meta_checkpoint(ps_zones[0].zone)
    # get auto-generated subscription
    sub_conf = PSSubscription(ps_zones[0].conn, notification_name,
                              topic_name)
    _, status = sub_conf.get_config()
    assert_equal(status/100, 2)
    # create objects in the bucket
    number_of_objects = 10
    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        key.set_contents_from_string('bar')
    # wait for sync
    zone_bucket_checkpoint(ps_zones[0].zone, zones[0].zone, bucket_name)

    # get the events from the subscription
    result, _ = sub_conf.get_events()
    parsed_result = json.loads(result)
    for record in parsed_result['Records']:
        log.debug(record)
    keys = list(bucket.list())
    # TODO: use exact match
    verify_s3_records_by_elements(parsed_result['Records'], keys, exact_match=False)

    # cleanup
    _, status = s3_notification_conf.del_config()
    topic_conf.del_config()
    # delete the keys
    for key in bucket.list():
        key.delete()
    zones[0].delete_bucket(bucket_name)


def test_ps_s3_notification():
    """ test s3 notification set/get/delete """
    zones, ps_zones = init_env()
    bucket_name = gen_bucket_name()
    # create bucket on the first of the rados zones
    zones[0].create_bucket(bucket_name)
    # wait for sync
    zone_meta_checkpoint(ps_zones[0].zone)
    topic_name = bucket_name + TOPIC_SUFFIX
    # create topic
    topic_name = bucket_name + TOPIC_SUFFIX
    topic_conf = PSTopic(ps_zones[0].conn, topic_name)
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
    s3_notification_conf1 = PSNotificationS3(ps_zones[0].conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf1.set_config()
    assert_equal(status/100, 2)
    # create another s3 notification with the same topic
    notification_name2 = bucket_name + NOTIFICATION_SUFFIX + '_2'
    topic_conf_list = [{'Id': notification_name2,
                        'TopicArn': topic_arn,
                        'Events': ['s3:ObjectCreated:*', 's3:ObjectRemoved:*']
                        }]
    s3_notification_conf2 = PSNotificationS3(ps_zones[0].conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf2.set_config()
    assert_equal(status/100, 2)
    zone_meta_checkpoint(ps_zones[0].zone)

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
    zones[0].delete_bucket(bucket_name)


def test_ps_topic():
    """ test set/get/delete of topic """
    _, ps_zones = init_env()
    realm = get_realm()
    zonegroup = realm.master_zonegroup()
    bucket_name = gen_bucket_name()
    topic_name = bucket_name+TOPIC_SUFFIX

    # create topic
    topic_conf = PSTopic(ps_zones[0].conn, topic_name)
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
    _, ps_zones = init_env()
    bucket_name = gen_bucket_name()
    topic_name = bucket_name+TOPIC_SUFFIX

    # create topic
    dest_endpoint = 'amqp://localhost:7001'
    dest_args = 'amqp-exchange=amqp.direct&amqp-ack-level=none'
    topic_conf = PSTopic(ps_zones[0].conn, topic_name, 
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
    zones, ps_zones = init_env()
    bucket_name = gen_bucket_name()
    topic_name = bucket_name+TOPIC_SUFFIX

    # create topic
    topic_conf = PSTopic(ps_zones[0].conn, topic_name)
    topic_conf.set_config()
    # create bucket on the first of the rados zones
    zones[0].create_bucket(bucket_name)
    # wait for sync
    zone_meta_checkpoint(ps_zones[0].zone)
    # create notifications
    notification_conf = PSNotification(ps_zones[0].conn, bucket_name,
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
    zones[0].delete_bucket(bucket_name)


def test_ps_notification_events():
    """ test set/get/delete of notification on specific events"""
    zones, ps_zones = init_env()
    bucket_name = gen_bucket_name()
    topic_name = bucket_name+TOPIC_SUFFIX

    # create topic
    topic_conf = PSTopic(ps_zones[0].conn, topic_name)
    topic_conf.set_config()
    # create bucket on the first of the rados zones
    zones[0].create_bucket(bucket_name)
    # wait for sync
    zone_meta_checkpoint(ps_zones[0].zone)
    # create notifications
    events = "OBJECT_CREATE,OBJECT_DELETE"
    notification_conf = PSNotification(ps_zones[0].conn, bucket_name,
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
    zones[0].delete_bucket(bucket_name)


def test_ps_subscription():
    """ test set/get/delete of subscription """
    zones, ps_zones = init_env()
    bucket_name = gen_bucket_name()
    topic_name = bucket_name+TOPIC_SUFFIX

    # create topic
    topic_conf = PSTopic(ps_zones[0].conn, topic_name)
    topic_conf.set_config()
    # create bucket on the first of the rados zones
    bucket = zones[0].create_bucket(bucket_name)
    # wait for sync
    zone_meta_checkpoint(ps_zones[0].zone)
    # create notifications
    notification_conf = PSNotification(ps_zones[0].conn, bucket_name,
                                       topic_name)
    _, status = notification_conf.set_config()
    assert_equal(status/100, 2)
    # create subscription
    sub_conf = PSSubscription(ps_zones[0].conn, bucket_name+SUB_SUFFIX,
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
    zone_bucket_checkpoint(ps_zones[0].zone, zones[0].zone, bucket_name)

    # get the create events from the subscription
    result, _ = sub_conf.get_events()
    parsed_result = json.loads(result)
    for event in parsed_result['events']:
        log.debug('Event: objname: "' + str(event['info']['key']['name']) + '" type: "' + str(event['event']) + '"')
    keys = list(bucket.list())
    # TODO: use exact match
    verify_events_by_elements(parsed_result['events'], keys, exact_match=False)
    # delete objects from the bucket
    for key in bucket.list():
        key.delete()
    # wait for sync
    zone_meta_checkpoint(ps_zones[0].zone)
    zone_bucket_checkpoint(ps_zones[0].zone, zones[0].zone, bucket_name)

    # get the delete events from the subscriptions
    result, _ = sub_conf.get_events()
    for event in parsed_result['events']:
        log.debug('Event: objname: "' + str(event['info']['key']['name']) + '" type: "' + str(event['event']) + '"')
    # TODO: check deletions
    # TODO: use exact match
    # verify_events_by_elements(parsed_result['events'], keys, exact_match=False, deletions=True)
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
    zones[0].delete_bucket(bucket_name)


def test_ps_event_type_subscription():
    """ test subscriptions for different events """
    zones, ps_zones = init_env()
    bucket_name = gen_bucket_name()

    # create topic for objects creation
    topic_create_name = bucket_name+TOPIC_SUFFIX+'_create'
    topic_create_conf = PSTopic(ps_zones[0].conn, topic_create_name)
    topic_create_conf.set_config()
    # create topic for objects deletion
    topic_delete_name = bucket_name+TOPIC_SUFFIX+'_delete'
    topic_delete_conf = PSTopic(ps_zones[0].conn, topic_delete_name)
    topic_delete_conf.set_config()
    # create topic for all events
    topic_name = bucket_name+TOPIC_SUFFIX+'_all'
    topic_conf = PSTopic(ps_zones[0].conn, topic_name)
    topic_conf.set_config()
    # create bucket on the first of the rados zones
    bucket = zones[0].create_bucket(bucket_name)
    # wait for sync
    zone_meta_checkpoint(ps_zones[0].zone)
    zone_bucket_checkpoint(ps_zones[0].zone, zones[0].zone, bucket_name)
    # create notifications for objects creation
    notification_create_conf = PSNotification(ps_zones[0].conn, bucket_name,
                                              topic_create_name, "OBJECT_CREATE")
    _, status = notification_create_conf.set_config()
    assert_equal(status/100, 2)
    # create notifications for objects deletion
    notification_delete_conf = PSNotification(ps_zones[0].conn, bucket_name,
                                              topic_delete_name, "OBJECT_DELETE")
    _, status = notification_delete_conf.set_config()
    assert_equal(status/100, 2)
    # create notifications for all events
    notification_conf = PSNotification(ps_zones[0].conn, bucket_name,
                                       topic_name, "OBJECT_DELETE,OBJECT_CREATE")
    _, status = notification_conf.set_config()
    assert_equal(status/100, 2)
    # create subscription for objects creation
    sub_create_conf = PSSubscription(ps_zones[0].conn, bucket_name+SUB_SUFFIX+'_create',
                                     topic_create_name)
    _, status = sub_create_conf.set_config()
    assert_equal(status/100, 2)
    # create subscription for objects deletion
    sub_delete_conf = PSSubscription(ps_zones[0].conn, bucket_name+SUB_SUFFIX+'_delete',
                                     topic_delete_name)
    _, status = sub_delete_conf.set_config()
    assert_equal(status/100, 2)
    # create subscription for all events
    sub_conf = PSSubscription(ps_zones[0].conn, bucket_name+SUB_SUFFIX+'_all',
                              topic_name)
    _, status = sub_conf.set_config()
    assert_equal(status/100, 2)
    # create objects in the bucket
    number_of_objects = 10
    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        key.set_contents_from_string('bar')
    # wait for sync
    zone_bucket_checkpoint(ps_zones[0].zone, zones[0].zone, bucket_name)

    # get the events from the creation subscription
    result, _ = sub_create_conf.get_events()
    parsed_result = json.loads(result)
    for event in parsed_result['events']:
        log.debug('Event (OBJECT_CREATE): objname: "' + str(event['info']['key']['name']) +
                  '" type: "' + str(event['event']) + '"')
    keys = list(bucket.list())
    # TODO: use exact match
    verify_events_by_elements(parsed_result['events'], keys, exact_match=False)
    # get the events from the deletions subscription
    result, _ = sub_delete_conf.get_events()
    parsed_result = json.loads(result)
    for event in parsed_result['events']:
        log.debug('Event (OBJECT_DELETE): objname: "' + str(event['info']['key']['name']) +
                  '" type: "' + str(event['event']) + '"')
    assert_equal(len(parsed_result['events']), 0)
    # get the events from the all events subscription
    result, _ = sub_conf.get_events()
    parsed_result = json.loads(result)
    for event in parsed_result['events']:
        log.debug('Event (OBJECT_CREATE,OBJECT_DELETE): objname: "' +
                  str(event['info']['key']['name']) + '" type: "' + str(event['event']) + '"')
    # TODO: use exact match
    verify_events_by_elements(parsed_result['events'], keys, exact_match=False)
    # delete objects from the bucket
    for key in bucket.list():
        key.delete()
    # wait for sync
    zone_bucket_checkpoint(ps_zones[0].zone, zones[0].zone, bucket_name)
    log.debug("Event (OBJECT_DELETE) synced")
    
    # get the events from the creations subscription
    result, _ = sub_create_conf.get_events()
    parsed_result = json.loads(result)
    for event in parsed_result['events']:
        log.debug('Event (OBJECT_CREATE): objname: "' + str(event['info']['key']['name']) +
                  '" type: "' + str(event['event']) + '"')
    # deletions should not change the creation events
    # TODO: use exact match
    verify_events_by_elements(parsed_result['events'], keys, exact_match=False)
    # get the events from the deletions subscription
    result, _ = sub_delete_conf.get_events()
    parsed_result = json.loads(result)
    for event in parsed_result['events']:
        log.debug('Event (OBJECT_DELETE): objname: "' + str(event['info']['key']['name']) +
                  '" type: "' + str(event['event']) + '"')
    # only deletions should be listed here
    # TODO: use exact match
    verify_events_by_elements(parsed_result['events'], keys, exact_match=False, deletions=True)
    # get the events from the all events subscription
    result, _ = sub_create_conf.get_events()
    parsed_result = json.loads(result)
    for event in parsed_result['events']:
        log.debug('Event (OBJECT_CREATE,OBJECT_DELETE): objname: "' + str(event['info']['key']['name']) +
                  '" type: "' + str(event['event']) + '"')
    # both deletions and creations should be here
    # TODO: use exact match
    verify_events_by_elements(parsed_result['events'], keys, exact_match=False, deletions=False)
    # verify_events_by_elements(parsed_result['events'], keys, exact_match=False, deletions=True)
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
    zones[0].delete_bucket(bucket_name)


def test_ps_event_fetching():
    """ test incremental fetching of events from a subscription """
    zones, ps_zones = init_env()
    bucket_name = gen_bucket_name()
    topic_name = bucket_name+TOPIC_SUFFIX

    # create topic
    topic_conf = PSTopic(ps_zones[0].conn, topic_name)
    topic_conf.set_config()
    # create bucket on the first of the rados zones
    bucket = zones[0].create_bucket(bucket_name)
    # wait for sync
    zone_meta_checkpoint(ps_zones[0].zone)
    # create notifications
    notification_conf = PSNotification(ps_zones[0].conn, bucket_name,
                                       topic_name)
    _, status = notification_conf.set_config()
    assert_equal(status/100, 2)
    # create subscription
    sub_conf = PSSubscription(ps_zones[0].conn, bucket_name+SUB_SUFFIX,
                              topic_name)
    _, status = sub_conf.set_config()
    assert_equal(status/100, 2)
    # create objects in the bucket
    number_of_objects = 100
    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        key.set_contents_from_string('bar')
    # wait for sync
    zone_bucket_checkpoint(ps_zones[0].zone, zones[0].zone, bucket_name)
    max_events = 15
    total_events_count = 0
    next_marker = None
    all_events = []
    while True:
        # get the events from the subscription
        result, _ = sub_conf.get_events(max_events, next_marker)
        parsed_result = json.loads(result)
        events = parsed_result['events']
        total_events_count += len(events)
        all_events.extend(events)
        next_marker = parsed_result['next_marker']
        for event in events:
            log.debug('Event: objname: "' + str(event['info']['key']['name']) + '" type: "' + str(event['event']) + '"')
        if next_marker == '':
            break
    keys = list(bucket.list())
    # TODO: use exact match
    verify_events_by_elements(all_events, keys, exact_match=False)

    # cleanup
    sub_conf.del_config()
    notification_conf.del_config()
    topic_conf.del_config()
    for key in bucket.list():
        key.delete()
    zones[0].delete_bucket(bucket_name)


def test_ps_event_acking():
    """ test acking of some events in a subscription """
    zones, ps_zones = init_env()
    bucket_name = gen_bucket_name()
    topic_name = bucket_name+TOPIC_SUFFIX

    # create topic
    topic_conf = PSTopic(ps_zones[0].conn, topic_name)
    topic_conf.set_config()
    # create bucket on the first of the rados zones
    bucket = zones[0].create_bucket(bucket_name)
    # wait for sync
    zone_meta_checkpoint(ps_zones[0].zone)
    # create notifications
    notification_conf = PSNotification(ps_zones[0].conn, bucket_name,
                                       topic_name)
    _, status = notification_conf.set_config()
    assert_equal(status/100, 2)
    # create subscription
    sub_conf = PSSubscription(ps_zones[0].conn, bucket_name+SUB_SUFFIX,
                              topic_name)
    _, status = sub_conf.set_config()
    assert_equal(status/100, 2)
    # create objects in the bucket
    number_of_objects = 10
    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        key.set_contents_from_string('bar')
    # wait for sync
    zone_bucket_checkpoint(ps_zones[0].zone, zones[0].zone, bucket_name)

    # get the create events from the subscription
    result, _ = sub_conf.get_events()
    parsed_result = json.loads(result)
    events = parsed_result['events']
    original_number_of_events = len(events)
    for event in events:
        log.debug('Event (before ack)  id: "' + str(event['id']) + '"')
    keys = list(bucket.list())
    # TODO: use exact match
    verify_events_by_elements(events, keys, exact_match=False)
    # ack half of the  events
    events_to_ack = number_of_objects/2
    for event in events:
        if events_to_ack == 0:
            break
        _, status = sub_conf.ack_events(event['id'])
        assert_equal(status/100, 2)
        events_to_ack -= 1

    # verify that acked events are gone
    result, _ = sub_conf.get_events()
    parsed_result = json.loads(result)
    for event in parsed_result['events']:
        log.debug('Event (after ack) id: "' + str(event['id']) + '"')
    assert len(parsed_result['events']) >= (original_number_of_events - number_of_objects/2)

    # cleanup
    sub_conf.del_config()
    notification_conf.del_config()
    topic_conf.del_config()
    for key in bucket.list():
        key.delete()
    zones[0].delete_bucket(bucket_name)


def test_ps_creation_triggers():
    """ test object creation notifications in using put/copy/post """
    zones, ps_zones = init_env()
    bucket_name = gen_bucket_name()
    topic_name = bucket_name+TOPIC_SUFFIX

    # create topic
    topic_conf = PSTopic(ps_zones[0].conn, topic_name)
    topic_conf.set_config()
    # create bucket on the first of the rados zones
    bucket = zones[0].create_bucket(bucket_name)
    # wait for sync
    zone_meta_checkpoint(ps_zones[0].zone)
    # create notifications
    notification_conf = PSNotification(ps_zones[0].conn, bucket_name,
                                       topic_name)
    _, status = notification_conf.set_config()
    assert_equal(status/100, 2)
    # create subscription
    sub_conf = PSSubscription(ps_zones[0].conn, bucket_name+SUB_SUFFIX,
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
    fp = tempfile.TemporaryFile(mode='r')
    uploader.upload_part_from_file(fp, 1)
    uploader.complete_upload()
    fp.close()
    # wait for sync
    zone_bucket_checkpoint(ps_zones[0].zone, zones[0].zone, bucket_name)

    # get the create events from the subscription
    result, _ = sub_conf.get_events()
    parsed_result = json.loads(result)
    for event in parsed_result['events']:
        log.debug('Event key: "' + str(event['info']['key']['name']) + '" type: "' + str(event['event']) + '"')

    # TODO: verify the specific 3 keys: 'put', 'copy' and 'multipart'
    assert len(parsed_result['events']) >= 3
    # cleanup
    sub_conf.del_config()
    notification_conf.del_config()
    topic_conf.del_config()
    for key in bucket.list():
        key.delete()
    zones[0].delete_bucket(bucket_name)


def test_ps_versioned_deletion():
    """ test notification of deletion markers """
    zones, ps_zones = init_env()
    bucket_name = gen_bucket_name()
    topic_name = bucket_name+TOPIC_SUFFIX

    # create topic
    topic_conf = PSTopic(ps_zones[0].conn, topic_name)
    topic_conf.set_config()
    # create bucket on the first of the rados zones
    bucket = zones[0].create_bucket(bucket_name)
    bucket.configure_versioning(True)
    # wait for sync
    zone_meta_checkpoint(ps_zones[0].zone)
    # create notifications
    notification_conf = PSNotification(ps_zones[0].conn, bucket_name,
                                       topic_name, "OBJECT_DELETE")
    _, status = notification_conf.set_config()
    assert_equal(status/100, 2)
    # create subscription
    sub_conf = PSSubscription(ps_zones[0].conn, bucket_name+SUB_SUFFIX,
                              topic_name)
    _, status = sub_conf.set_config()
    assert_equal(status/100, 2)
    # create objects in the bucket
    key = bucket.new_key('foo')
    key.set_contents_from_string('bar')
    v1 = key.version_id
    key.set_contents_from_string('kaboom')
    v2 = key.version_id
    # wait for sync
    zone_bucket_checkpoint(ps_zones[0].zone, zones[0].zone, bucket_name)
    # set delete markers
    bucket.delete_key(key.name, version_id=v2)
    bucket.delete_key(key.name, version_id=v1)
    # wait for sync
    zone_bucket_checkpoint(ps_zones[0].zone, zones[0].zone, bucket_name)

    # get the delete events from the subscription
    result, _ = sub_conf.get_events()
    parsed_result = json.loads(result)
    for event in parsed_result['events']:
        log.debug('Event key: "' + str(event['info']['key']['name']) + '" type: "' + str(event['event']) + '"')
        assert_equal(str(event['event']), 'OBJECT_DELETE')

    # TODO: verify we have exactly 2 events
    assert len(parsed_result['events']) >= 2

    # cleanup
    # follwing is needed for the cleanup in the case of 3-zones
    # see: http://tracker.ceph.com/issues/39142
    realm = get_realm()
    zonegroup = realm.master_zonegroup()
    zonegroup_conns = ZonegroupConns(zonegroup)
    try:
        zonegroup_bucket_checkpoint(zonegroup_conns, bucket_name)
        zones[0].delete_bucket(bucket_name)
    except:
        log.debug('zonegroup_bucket_checkpoint failed, cannot delete bucket')
    sub_conf.del_config()
    notification_conf.del_config()
    topic_conf.del_config()


def test_ps_push_http():
    """ test pushing to http endpoint """
    return SkipTest("PubSub push tests are only manual")
    zones, ps_zones = init_env()
    bucket_name = gen_bucket_name()
    topic_name = bucket_name+TOPIC_SUFFIX

    # create topic
    topic_conf = PSTopic(ps_zones[0].conn, topic_name)
    _, status = topic_conf.set_config()
    assert_equal(status/100, 2)
    # create bucket on the first of the rados zones
    bucket = zones[0].create_bucket(bucket_name)
    # wait for sync
    zone_meta_checkpoint(ps_zones[0].zone)
    # create notifications
    notification_conf = PSNotification(ps_zones[0].conn, bucket_name,
                                       topic_name)
    _, status = notification_conf.set_config()
    assert_equal(status/100, 2)
    # create subscription
    sub_conf = PSSubscription(ps_zones[0].conn, bucket_name+SUB_SUFFIX,
                              topic_name, endpoint='http://localhost:9001')
    _, status = sub_conf.set_config()
    assert_equal(status/100, 2)
    # create objects in the bucket
    number_of_objects = 10
    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        key.set_contents_from_string('bar')
    # wait for sync
    zone_bucket_checkpoint(ps_zones[0].zone, zones[0].zone, bucket_name)

    # delete objects from the bucket
    for key in bucket.list():
        key.delete()
    # wait for sync
    zone_meta_checkpoint(ps_zones[0].zone)
    zone_bucket_checkpoint(ps_zones[0].zone, zones[0].zone, bucket_name)

    # cleanup
    sub_conf.del_config()
    notification_conf.del_config()
    topic_conf.del_config()
    zones[0].delete_bucket(bucket_name)


def test_ps_s3_push_http():
    """ test pushing to http endpoint n s3 record format"""
    return SkipTest("PubSub push tests are only manual")
    zones, ps_zones = init_env()
    bucket_name = gen_bucket_name()
    topic_name = bucket_name+TOPIC_SUFFIX

    # create topic
    topic_conf = PSTopic(ps_zones[0].conn, topic_name,
                         endpoint='http://localhost:9001')
    result, status = topic_conf.set_config()
    assert_equal(status/100, 2)
    parsed_result = json.loads(result)
    topic_arn = parsed_result['arn']
    # create bucket on the first of the rados zones
    bucket = zones[0].create_bucket(bucket_name)
    # wait for sync
    zone_meta_checkpoint(ps_zones[0].zone)
    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name,
                        'TopicArn': topic_arn,
                        'Events': ['s3:ObjectCreated:*']
                        }]
    s3_notification_conf = PSNotificationS3(ps_zones[0].conn, bucket_name, topic_conf_list)
    _, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)
    # create objects in the bucket
    number_of_objects = 10
    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        key.set_contents_from_string('bar')
    # wait for sync
    zone_bucket_checkpoint(ps_zones[0].zone, zones[0].zone, bucket_name)
    # TODO check http server

    # delete objects from the bucket
    for key in bucket.list():
        key.delete()
    # wait for sync
    zone_meta_checkpoint(ps_zones[0].zone)
    zone_bucket_checkpoint(ps_zones[0].zone, zones[0].zone, bucket_name)
    # TODO check http server

    # cleanup
    s3_notification_conf.del_config()
    topic_conf.del_config()
    zones[0].delete_bucket(bucket_name)


def test_ps_push_amqp():
    """ test pushing to amqp endpoint """
    return SkipTest("PubSub push tests are only manual")
    zones, ps_zones = init_env()
    bucket_name = gen_bucket_name()
    topic_name = bucket_name+TOPIC_SUFFIX

    # create topic
    topic_conf = PSTopic(ps_zones[0].conn, topic_name)
    _, status = topic_conf.set_config()
    assert_equal(status/100, 2)
    # create bucket on the first of the rados zones
    bucket = zones[0].create_bucket(bucket_name)
    # wait for sync
    zone_meta_checkpoint(ps_zones[0].zone)
    # create notifications
    notification_conf = PSNotification(ps_zones[0].conn, bucket_name,
                                       topic_name)
    _, status = notification_conf.set_config()
    assert_equal(status/100, 2)
    # create subscription
    sub_conf = PSSubscription(ps_zones[0].conn, bucket_name+SUB_SUFFIX,
                              topic_name, endpoint='amqp://localhost',
                              endpoint_args='amqp-exchange=ex1&amqp-ack-level=none')
    _, status = sub_conf.set_config()
    assert_equal(status/100, 2)
    # create objects in the bucket
    number_of_objects = 10
    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        key.set_contents_from_string('bar')
    # wait for sync
    zone_bucket_checkpoint(ps_zones[0].zone, zones[0].zone, bucket_name)
    # TODO check amqp receiver

    # delete objects from the bucket
    for key in bucket.list():
        key.delete()
    # wait for sync
    zone_meta_checkpoint(ps_zones[0].zone)
    zone_bucket_checkpoint(ps_zones[0].zone, zones[0].zone, bucket_name)
    # TODO check amqp receiver

    # cleanup
    sub_conf.del_config()
    notification_conf.del_config()
    topic_conf.del_config()
    zones[0].delete_bucket(bucket_name)


def test_ps_s3_push_amqp():
    """ test pushing to amqp endpoint s3 record format"""
    return SkipTest("PubSub push tests are only manual")
    zones, ps_zones = init_env()
    bucket_name = gen_bucket_name()
    topic_name = bucket_name+TOPIC_SUFFIX

    # create topic
    topic_conf = PSTopic(ps_zones[0].conn, topic_name,
                         endpoint='amqp://localhost',
                         endpoint_args='amqp-exchange=ex1&amqp-ack-level=none')
    result, status = topic_conf.set_config()
    assert_equal(status/100, 2)
    parsed_result = json.loads(result)
    topic_arn = parsed_result['arn']
    # create bucket on the first of the rados zones
    bucket = zones[0].create_bucket(bucket_name)
    # wait for sync
    zone_meta_checkpoint(ps_zones[0].zone)
    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name,
                        'TopicArn': topic_arn,
                        'Events': ['s3:ObjectCreated:*']
                        }]
    s3_notification_conf = PSNotificationS3(ps_zones[0].conn, bucket_name, topic_conf_list)
    _, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)
    # create objects in the bucket
    number_of_objects = 10
    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        key.set_contents_from_string('bar')
    # wait for sync
    zone_bucket_checkpoint(ps_zones[0].zone, zones[0].zone, bucket_name)
    # TODO check amqp receiver

    # delete objects from the bucket
    for key in bucket.list():
        key.delete()
    # wait for sync
    zone_meta_checkpoint(ps_zones[0].zone)
    zone_bucket_checkpoint(ps_zones[0].zone, zones[0].zone, bucket_name)
    # TODO check amqp receiver

    # cleanup
    s3_notification_conf.del_config()
    topic_conf.del_config()
    zones[0].delete_bucket(bucket_name)


def test_ps_delete_bucket():
    """ test notification status upon bucket deletion """
    zones, ps_zones = init_env()
    bucket_name = gen_bucket_name()
    # create bucket on the first of the rados zones
    bucket = zones[0].create_bucket(bucket_name)
    # wait for sync
    zone_meta_checkpoint(ps_zones[0].zone)
    topic_name = bucket_name + TOPIC_SUFFIX
    # create topic
    topic_name = bucket_name + TOPIC_SUFFIX
    topic_conf = PSTopic(ps_zones[0].conn, topic_name)
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
    s3_notification_conf = PSNotificationS3(ps_zones[0].conn, bucket_name, topic_conf_list)
    response, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)
   
    # create non-s3 notification
    notification_conf = PSNotification(ps_zones[0].conn, bucket_name,
                                       topic_name)
    _, status = notification_conf.set_config()
    assert_equal(status/100, 2)

    # create objects in the bucket
    number_of_objects = 10
    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        key.set_contents_from_string('bar')
    # wait for bucket sync
    zone_bucket_checkpoint(ps_zones[0].zone, zones[0].zone, bucket_name)
    keys = list(bucket.list())
    # delete objects from the bucket
    for key in bucket.list():
        key.delete()
    # wait for bucket sync
    zone_bucket_checkpoint(ps_zones[0].zone, zones[0].zone, bucket_name)
    # delete the bucket
    zones[0].delete_bucket(bucket_name)
    # wait for meta sync
    zone_meta_checkpoint(ps_zones[0].zone)

    # get the events from the auto-generated subscription
    sub_conf = PSSubscription(ps_zones[0].conn, notification_name,
                              topic_name)
    result, _ = sub_conf.get_events()
    parsed_result = json.loads(result)
    # TODO: use exact match
    verify_s3_records_by_elements(parsed_result['Records'], keys, exact_match=False)

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
    zones, ps_zones = init_env()
    bucket_name = gen_bucket_name()
    topic_name = bucket_name+TOPIC_SUFFIX

    # create bucket on the first of the rados zones
    zones[0].create_bucket(bucket_name)
    # wait for sync
    zone_meta_checkpoint(ps_zones[0].zone)
    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_arn = 'arn:aws:sns:::' + topic_name
    topic_conf_list = [{'Id': notification_name,
                        'TopicArn': topic_arn,
                        'Events': ['s3:ObjectCreated:*']
                        }]
    s3_notification_conf = PSNotificationS3(ps_zones[0].conn, bucket_name, topic_conf_list)
    try:
        s3_notification_conf.set_config()
    except:
        print('missing topic is expected')
    else:
        assert 'missing topic is expected'

    # cleanup
    zones[0].delete_bucket(bucket_name)


def test_ps_s3_topic_update():
    """ test updating topic associated with a notification"""
    return SkipTest("PubSub push tests are only manual")
    zones, ps_zones = init_env()
    bucket_name = gen_bucket_name()
    topic_name = bucket_name+TOPIC_SUFFIX

    # create topic
    dest_endpoint1 = 'amqp://localhost'
    dest_args1 = 'amqp-exchange=ex1&amqp-ack-level=none'
    dest_endpoint2 = 'http://localhost:9001'
    topic_conf = PSTopic(ps_zones[0].conn, topic_name, 
                         endpoint=dest_endpoint1,
                         endpoint_args=dest_args1)
    result, status = topic_conf.set_config()
    parsed_result = json.loads(result)
    topic_arn = parsed_result['arn']
    assert_equal(status/100, 2)
    # get topic
    result, _ = topic_conf.get_config()
    # verify topic content
    parsed_result = json.loads(result)
    assert_equal(parsed_result['topic']['name'], topic_name)
    assert_equal(parsed_result['topic']['dest']['push_endpoint'], dest_endpoint1)

    # create bucket on the first of the rados zones
    bucket = zones[0].create_bucket(bucket_name)
    # wait for sync
    zone_meta_checkpoint(ps_zones[0].zone)
    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name,
                        'TopicArn': topic_arn,
                        'Events': ['s3:ObjectCreated:*']
                        }]
    s3_notification_conf = PSNotificationS3(ps_zones[0].conn, bucket_name, topic_conf_list)
    _, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)
    # create objects in the bucket
    number_of_objects = 10
    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        key.set_contents_from_string('bar')
    # wait for sync
    zone_bucket_checkpoint(ps_zones[0].zone, zones[0].zone, bucket_name)

    # TODO: check update to amqp

    # update the same topic
    topic_conf = PSTopic(ps_zones[0].conn, topic_name, 
                         endpoint=dest_endpoint2)
    _, status = topic_conf.set_config()
    assert_equal(status/100, 2)
    # get topic
    result, _ = topic_conf.get_config()
    # verify topic content
    parsed_result = json.loads(result)
    assert_equal(parsed_result['topic']['name'], topic_name)
    assert_equal(parsed_result['topic']['dest']['push_endpoint'], dest_endpoint2)

    # create more objects in the bucket
    number_of_objects = 10
    for i in range(number_of_objects):
        key = bucket.new_key(str(i+100))
        key.set_contents_from_string('bar')
    # wait for sync
    zone_bucket_checkpoint(ps_zones[0].zone, zones[0].zone, bucket_name)

    # TODO: check it is still updating amqp

    # update notification to update the endpoint from the topic
    topic_conf_list = [{'Id': notification_name,
                        'TopicArn': topic_arn,
                        'Events': ['s3:ObjectCreated:*']
                        }]
    s3_notification_conf = PSNotificationS3(ps_zones[0].conn, bucket_name, topic_conf_list)
    _, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)
    # create even more objects in the bucket
    number_of_objects = 10
    for i in range(number_of_objects):
        key = bucket.new_key(str(i+200))
        key.set_contents_from_string('bar')
    # wait for sync
    zone_bucket_checkpoint(ps_zones[0].zone, zones[0].zone, bucket_name)

    # TODO: check that updates switched to http

    # cleanup
    # delete objects from the bucket
    for key in bucket.list():
        key.delete()
    s3_notification_conf.del_config()
    topic_conf.del_config()
    zones[0].delete_bucket(bucket_name)


def test_ps_s3_notification_update():
    """ test updating the topic of a notification"""
    return SkipTest("PubSub push tests are only manual")
    zones, ps_zones = init_env()
    bucket_name = gen_bucket_name()
    topic_name1 = bucket_name+'amqp'+TOPIC_SUFFIX

    # create first topic
    dest_endpoint1 = 'amqp://localhost'
    dest_args1 = 'amqp-exchange=ex1&amqp-ack-level=none'
    topic_conf1 = PSTopic(ps_zones[0].conn, topic_name1, 
                         endpoint=dest_endpoint1,
                         endpoint_args=dest_args1)
    result, status = topic_conf1.set_config()
    parsed_result = json.loads(result)
    topic_arn1 = parsed_result['arn']
    assert_equal(status/100, 2)

    # create bucket on the first of the rados zones
    bucket = zones[0].create_bucket(bucket_name)
    # wait for sync
    zone_meta_checkpoint(ps_zones[0].zone)
    # create s3 notification
    notification_name = bucket_name + NOTIFICATION_SUFFIX
    topic_conf_list = [{'Id': notification_name,
                        'TopicArn': topic_arn1,
                        'Events': ['s3:ObjectCreated:*']
                        }]
    s3_notification_conf = PSNotificationS3(ps_zones[0].conn, bucket_name, topic_conf_list)
    _, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)
    # create objects in the bucket
    number_of_objects = 10
    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        key.set_contents_from_string('bar')
    # wait for sync
    zone_bucket_checkpoint(ps_zones[0].zone, zones[0].zone, bucket_name)
    result, _ = s3_notification_conf.get_config()

    # TODO: check updates to amqp

    # create another topic
    topic_name2 = bucket_name+'http'+TOPIC_SUFFIX
    dest_endpoint2 = 'http://localhost:9001'
    topic_conf2 = PSTopic(ps_zones[0].conn, topic_name2, 
                         endpoint=dest_endpoint2)
    result, status = topic_conf2.set_config()
    parsed_result = json.loads(result)
    topic_arn2 = parsed_result['arn']
    assert_equal(status/100, 2)

    # update notification to the new topic
    topic_conf_list = [{'Id': notification_name,
                        'TopicArn': topic_arn2,
                        'Events': ['s3:ObjectCreated:*']
                        }]
    s3_notification_conf = PSNotificationS3(ps_zones[0].conn, bucket_name, topic_conf_list)
    _, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)
    # create more objects in the bucket
    number_of_objects = 10
    for i in range(number_of_objects):
        key = bucket.new_key(str(i+200))
        key.set_contents_from_string('bar')
    # wait for sync
    zone_bucket_checkpoint(ps_zones[0].zone, zones[0].zone, bucket_name)

    # TODO: check uodate to http
    result, _ = s3_notification_conf.get_config()

    # cleanup
    # delete objects from the bucket
    for key in bucket.list():
        key.delete()
    s3_notification_conf.del_config()
    topic_conf1.del_config()
    topic_conf2.del_config()
    zones[0].delete_bucket(bucket_name)


def test_ps_s3_multiple_topics_notification():
    """ test notification creation with multiple topics"""
    zones, ps_zones = init_env()
    bucket_name = gen_bucket_name()
    # TODO: test via push endpoint (amqp+http)
    topic_name1 = bucket_name+'amqp'+TOPIC_SUFFIX
    topic_name2 = bucket_name+'http'+TOPIC_SUFFIX

    # create topics
    topic_conf1 = PSTopic(ps_zones[0].conn, topic_name1)
    result, status = topic_conf1.set_config()
    parsed_result = json.loads(result)
    topic_arn1 = parsed_result['arn']
    assert_equal(status/100, 2)
    topic_conf2 = PSTopic(ps_zones[0].conn, topic_name2)
    result, status = topic_conf2.set_config()
    parsed_result = json.loads(result)
    topic_arn2 = parsed_result['arn']
    assert_equal(status/100, 2)

    # create bucket on the first of the rados zones
    bucket = zones[0].create_bucket(bucket_name)
    # wait for sync
    zone_meta_checkpoint(ps_zones[0].zone)
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
    s3_notification_conf = PSNotificationS3(ps_zones[0].conn, bucket_name, topic_conf_list)
    _, status = s3_notification_conf.set_config()
    assert_equal(status/100, 2)
    result, _ = s3_notification_conf.get_config()
    assert_equal(len(result['TopicConfigurations']), 2)
    assert_equal(result['TopicConfigurations'][0]['Id'], notification_name1)
    assert_equal(result['TopicConfigurations'][1]['Id'], notification_name2)

    # get auto-generated subscriptions
    sub_conf1 = PSSubscription(ps_zones[0].conn, notification_name1,
                              topic_name1)
    _, status = sub_conf1.get_config()
    assert_equal(status/100, 2)
    sub_conf2 = PSSubscription(ps_zones[0].conn, notification_name2,
                              topic_name2)
    _, status = sub_conf2.get_config()
    assert_equal(status/100, 2)
    
    # create objects in the bucket
    number_of_objects = 10
    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        key.set_contents_from_string('bar')
    # wait for sync
    zone_bucket_checkpoint(ps_zones[0].zone, zones[0].zone, bucket_name)

    # get the events from both of the subscription
    result, _ = sub_conf1.get_events()
    parsed_result = json.loads(result)
    for record in parsed_result['Records']:
        log.debug(record)
    keys = list(bucket.list())
    # TODO: use exact match
    verify_s3_records_by_elements(parsed_result['Records'], keys, exact_match=False)
    
    result, _ = sub_conf2.get_events()
    parsed_result = json.loads(result)
    for record in parsed_result['Records']:
        log.debug(record)
    keys = list(bucket.list())
    # TODO: use exact match
    verify_s3_records_by_elements(parsed_result['Records'], keys, exact_match=False)
    
    # cleanup
    s3_notification_conf.del_config()
    topic_conf1.del_config()
    topic_conf2.del_config()
    # delete objects from the bucket
    for key in bucket.list():
        key.delete()
    zones[0].delete_bucket(bucket_name)
