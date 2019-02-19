import logging
import json
import tempfile
from rgw_multi.tests import get_realm, \
    ZonegroupConns, \
    zonegroup_meta_checkpoint, \
    zone_meta_checkpoint, \
    zone_bucket_checkpoint, \
    zone_data_checkpoint, \
    check_bucket_eq, \
    gen_bucket_name
from rgw_multi.zone_ps import PSTopic, PSNotification, PSSubscription
from nose import SkipTest
from nose.tools import assert_not_equal, assert_equal

# configure logging for the tests module
log = logging.getLogger('rgw_multi.tests')

####################################
# utility functions for pubsub tests
####################################


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

##############
# pubsub tests
##############


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
    zone_bucket_checkpoint(ps_zones[0].zone, zones[0].zone, bucket_name)

    # get the create events from the subscription
    result, _ = sub_conf.get_events()
    parsed_result = json.loads(result)
    for event in parsed_result['events']:
        log.debug('Event: objname: "' + str(event['info']['key']['name']) + '" type: "' + str(event['event']) + '"')
    keys = list(bucket.list())
    # TODO: set exact_match to true
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
    # verify_events_by_elements(parsed_result['events'], keys, exact_match=False, deletions=True)
    # we should see the creations as well as the deletions
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
        log.debug('Event (OBJECT_CREATE): objname: "' + str(event['info']['key']['name']) + \
                  '" type: "' + str(event['event']) + '"')
    keys = list(bucket.list())
    # TODO: set exact_match to true
    verify_events_by_elements(parsed_result['events'], keys, exact_match=False)
    # get the events from the deletions subscription
    result, _ = sub_delete_conf.get_events()
    parsed_result = json.loads(result)
    for event in parsed_result['events']:
        log.debug('Event (OBJECT_DELETE): objname: "' + str(event['info']['key']['name']) + \
                  '" type: "' + str(event['event']) + '"')
    assert_equal(len(parsed_result['events']), 0)
    # get the events from the all events subscription
    result, _ = sub_conf.get_events()
    parsed_result = json.loads(result)
    for event in parsed_result['events']:
        log.debug('Event (OBJECT_CREATE,OBJECT_DELETE): objname: "' + \
                  str(event['info']['key']['name']) + '" type: "' + str(event['event']) + '"')
    # TODO: set exact_match to true
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
        log.debug('Event (OBJECT_CREATE): objname: "' + str(event['info']['key']['name']) + \
                  '" type: "' + str(event['event']) + '"')
    # deletions should not change the creation events
    # TODO: set exact_match to true
    verify_events_by_elements(parsed_result['events'], keys, exact_match=False)
    # get the events from the deletions subscription
    result, _ = sub_delete_conf.get_events()
    parsed_result = json.loads(result)
    for event in parsed_result['events']:
        log.debug('Event (OBJECT_DELETE): objname: "' + str(event['info']['key']['name']) + \
                  '" type: "' + str(event['event']) + '"')
    # only deletions should be listed here
    # TODO: set exact_match to true
    verify_events_by_elements(parsed_result['events'], keys, exact_match=False, deletions=True)
    # get the events from the all events subscription
    result, _ = sub_create_conf.get_events()
    parsed_result = json.loads(result)
    for event in parsed_result['events']:
        log.debug('Event (OBJECT_CREATE,OBJECT_DELETE): objname: "' + str(event['info']['key']['name']) + \
                  '" type: "' + str(event['event']) + '"')
    # both deletions and creations should be here
    verify_events_by_elements(parsed_result['events'], keys, exact_match=False, deletions=False)
    # verify_events_by_elements(parsed_result['events'], keys, exact_match=False, deletions=True)
    # TODO: (1) test deletions (2) test overall number of events

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
    # TODO: set exact_match to true
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
    # TODO: set exact_match to true
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
    assert_equal(len(parsed_result['events']), original_number_of_events - number_of_objects/2)

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

    # get the create events from the subscription
    result, _ = sub_conf.get_events()
    parsed_result = json.loads(result)
    for event in parsed_result['events']:
        log.debug('Event key: "' + str(event['info']['key']['name']) + '" type: "' + str(event['event']) + '"')

    # TODO: verify the specific events
    assert len(parsed_result['events']) >=  2
    
    # cleanup
    sub_conf.del_config()
    notification_conf.del_config()
    topic_conf.del_config()
    zones[0].delete_bucket(bucket_name)
