import logging
import json
from rgw_multi.tests import get_realm, \
    ZonegroupConns, \
    zonegroup_meta_checkpoint, \
    zone_meta_checkpoint, \
    gen_bucket_name
from rgw_multi.zone_ps import PSTopic, PSNotification, PSSubscription
from nose import SkipTest
from nose.tools import assert_not_equal, assert_equal

# configure logging for the tests module
log = logging.getLogger('rgw_multi.tests')

def check_ps_configured():
    """check if at least one pubsub zone exist"""
    realm = get_realm()
    zonegroup = realm.master_zonegroup()

    es_zones = zonegroup.zones_by_type.get("pubsub")
    if not es_zones:
        raise SkipTest("Requires at least one PS zone")


def is_ps_zone(zone_conn):
    """check if a specific zone is pubsub zone"""
    if not zone_conn:
        return False
    return zone_conn.zone.tier_type() == "pubsub"


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


def test_ps_topic():
    """ test set/get/delete of topic """
    _, ps_zones = init_env()
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
    # delete topic
    _, status = topic_conf.del_config()
    assert_equal(status/100, 2)
    # verift topic is deleted
    result, _ = topic_conf.get_config()
    parsed_result = json.loads(result)
    assert_equal(parsed_result['Code'], 'NoSuchKey')


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
    # TODO: deletion cannot be verified via GET
    # result, _ = notification_conf.get_config()
    # parsed_result = json.loads(result)
    # assert_equal(parsed_result['Code'], 'NoSuchKey')

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
    zone_meta_checkpoint(ps_zones[0].zone)
    log.debug("Event (OBJECT_CREATE) synced")
    # get the events from the subscriptions
    result, _ = sub_conf.get_events()
    parsed_result = json.loads(result)
    for event in parsed_result['events']:
        log.debug("Event: " + str(event))
    # TODO use the exact assert
    assert_not_equal(len(parsed_result['events']), 0)
    if len(parsed_result['events']) != number_of_objects:
        log.error('wrong number of events: ' + str(len(parsed_result['events'])) + ' should be: ' + str(number_of_objects))
    # assert_equal(len(parsed_result['events']), number_of_objects)
    # delete objects from the bucket
    for key in bucket.list():
        key.delete()
    # wait for sync
    zone_meta_checkpoint(ps_zones[0].zone)
    log.debug("Event (OBJECT_DELETE) synced")
    # get the events from the subscriptions
    result, _ = sub_conf.get_events()
    for event in parsed_result['events']:
        log.debug("Event: " + str(event))
    # TODO use the exact assert
    assert_not_equal(len(parsed_result['events']), 0)
    if len(parsed_result['events']) != 2*number_of_objects:
        log.error('wrong number of events: ' + str(len(parsed_result['events'])) + ' should be: ' + str(2*number_of_objects))
    # we should see the creations as well as the deletions
    # assert_equal(len(parsed_result['events']), number_of_objects*2)
    # delete subscription
    _, status = sub_conf.del_config()
    assert_equal(status/100, 2)
    result, _ = sub_conf.get_config()
    parsed_result = json.loads(result)
    assert_equal(parsed_result['topic'], '')
    # TODO should return "no-key" instead
    # assert_equal(parsed_result['Code'], 'NoSuchKey')

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
    zone_meta_checkpoint(ps_zones[0].zone)
    log.debug("Event (OBJECT_CREATE) synced")
    # get the events from the creations subscription
    result, _ = sub_create_conf.get_events()
    parsed_result = json.loads(result)
    for event in parsed_result['events']:
        log.debug("Event (OBJECT_CREATE): " + str(event))
    # TODO use the exact assert
    assert_not_equal(len(parsed_result['events']), 0)
    if len(parsed_result['events']) != number_of_objects:
        log.error('wrong number of OBJECT_CREATE events: ' + str(len(parsed_result['events'])) + ' should be: ' + str(number_of_objects))
    # assert_equal(len(parsed_result['events']), number_of_objects)
    # get the events from the deletions subscription
    result, _ = sub_delete_conf.get_events()
    parsed_result = json.loads(result)
    for event in parsed_result['events']:
        log.debug("Event (OBJECT_DELETE): " + str(event))
    assert_equal(len(parsed_result['events']), 0)
    # get the events from the all events subscription
    result, _ = sub_conf.get_events()
    parsed_result = json.loads(result)
    for event in parsed_result['events']:
        log.debug("Event (OBJECT_CREATE,OBJECT_DELETE): " + str(event))
    # TODO use the exact assert
    assert_not_equal(len(parsed_result['events']), 0)
    if len(parsed_result['events']) != number_of_objects:
        log.error('wrong number of OBJECT_CREATE,OBJECT_DELETE events: ' + str(len(parsed_result['events'])) + ' should be: ' + str(number_of_objects))
    # assert_equal(len(parsed_result['events']), number_of_objects)
    # delete objects from the bucket
    for key in bucket.list():
        key.delete()
    # wait for sync
    zone_meta_checkpoint(ps_zones[0].zone)
    log.debug("Event (OBJECT_DELETE) synced")
    # get the events from the creations subscription
    result, _ = sub_create_conf.get_events()
    parsed_result = json.loads(result)
    for event in parsed_result['events']:
        log.debug("Event (OBJECT_CREATE): " + str(event))
    # TODO use the exact assert
    assert_not_equal(len(parsed_result['events']), 0)
    if len(parsed_result['events']) != number_of_objects:
        log.error('wrong number of OBJECT_CREATE events: ' + str(len(parsed_result['events'])) + ' should be: ' + str(number_of_objects))
    # deletions should not change the number of events
    # assert_equal(len(parsed_result['events']), number_of_objects)
    # get the events from the deletions subscription
    result, _ = sub_delete_conf.get_events()
    parsed_result = json.loads(result)
    for event in parsed_result['events']:
        log.debug("Event (OBJECT_DELETE): " + str(event))
    # TODO use the exact assert
    assert_not_equal(len(parsed_result['events']), 0)
    if len(parsed_result['events']) != number_of_objects:
        log.error('wrong number of OBJECT_DELETE events: ' + str(len(parsed_result['events'])) + ' should be: ' + str(number_of_objects))
    # only deletions should be counted here
    # assert_equal(len(parsed_result['events']), number_of_objects)
    # get the events from the all events subscription
    result, _ = sub_create_conf.get_events()
    parsed_result = json.loads(result)
    for event in parsed_result['events']:
        log.debug("Event (OBJECT_CREATE,OBJECT_DELETE): " + str(event))
    # TODO use the exact assert
    assert_not_equal(len(parsed_result['events']), 0)
    if len(parsed_result['events']) != 2*number_of_objects:
        log.error('wrong number of OBJECT_CREATE,OBJECT_DELETE events: ' + str(len(parsed_result['events'])) + ' should be: ' + str(2*number_of_objects))
    # we should see the creations as well as the deletions
    # assert_equal(len(parsed_result['events']), number_of_objects*2)

    # cleanup
    sub_create_conf.del_config()
    sub_delete_conf.del_config()
    sub_conf.del_config()
    notification_create_conf.del_config()
    notification_delete_conf.del_config()
    notification_conf.del_config()
    topic_create_conf.del_config()
    topic_delete_conf.del_config()
    topic_conf.del_config()
    zones[0].delete_bucket(bucket_name)
