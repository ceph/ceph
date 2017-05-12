import json
import random
import string
import sys
import time
import logging
try:
    from itertools import izip_longest as zip_longest
except ImportError:
    from itertools import zip_longest
from itertools import combinations

import boto
import boto.s3.connection

from nose.tools import eq_ as eq
from nose.plugins.attrib import attr
from nose.plugins.skip import SkipTest

from .multisite import Zone

# rgw multisite tests, written against the interfaces provided in rgw_multi.
# these tests must be initialized and run by another module that provides
# implementations of these interfaces by calling init_multi()
realm = None
user = None
def init_multi(_realm, _user):
    global realm
    realm = _realm
    global user
    user = _user

log = logging.getLogger(__name__)

num_buckets = 0
run_prefix=''.join(random.choice(string.ascii_lowercase) for _ in range(6))

def get_gateway_connection(gateway, credentials):
    """ connect to the given gateway """
    if gateway.connection is None:
        gateway.connection = boto.connect_s3(
                aws_access_key_id = credentials.access_key,
                aws_secret_access_key = credentials.secret,
                host = gateway.host,
                port = gateway.port,
                is_secure = False,
                calling_format = boto.s3.connection.OrdinaryCallingFormat())
    return gateway.connection

def get_zone_connection(zone, credentials):
    """ connect to the zone's first gateway """
    if isinstance(credentials, list):
        credentials = credentials[0]
    return get_gateway_connection(zone.gateways[0], credentials)

def mdlog_list(zone, period = None):
    cmd = ['mdlog', 'list']
    if period:
        cmd += ['--period', period]
    (mdlog_json, _) = zone.cluster.admin(cmd, read_only=True)
    mdlog_json = mdlog_json.decode('utf-8')
    return json.loads(mdlog_json)

def mdlog_autotrim(zone):
    zone.cluster.admin(['mdlog', 'autotrim'])

def meta_sync_status(zone):
    while True:
        cmd = ['metadata', 'sync', 'status'] + zone.zone_args()
        meta_sync_status_json, retcode = zone.cluster.admin(cmd, check_retcode=False, read_only=True)
        if retcode == 0:
            break
        assert(retcode == 2) # ENOENT
        time.sleep(5)

    meta_sync_status_json = meta_sync_status_json.decode('utf-8')
    log.debug('current meta sync status=%s', meta_sync_status_json)
    sync_status = json.loads(meta_sync_status_json)

    sync_info = sync_status['sync_status']['info']
    global_sync_status = sync_info['status']
    num_shards = sync_info['num_shards']
    period = sync_info['period']
    realm_epoch = sync_info['realm_epoch']

    sync_markers=sync_status['sync_status']['markers']
    log.debug('sync_markers=%s', sync_markers)
    assert(num_shards == len(sync_markers))

    markers={}
    for i in range(num_shards):
        # get marker, only if it's an incremental marker for the same realm epoch
        if realm_epoch > sync_markers[i]['val']['realm_epoch'] or sync_markers[i]['val']['state'] == 0:
            markers[i] = ''
        else:
            markers[i] = sync_markers[i]['val']['marker']

    return period, realm_epoch, num_shards, markers

def meta_master_log_status(master_zone):
    cmd = ['mdlog', 'status'] + master_zone.zone_args()
    mdlog_status_json, retcode = master_zone.cluster.admin(cmd, read_only=True)
    mdlog_status = json.loads(mdlog_status_json.decode('utf-8'))

    markers = {i: s['marker'] for i, s in enumerate(mdlog_status)}
    log.debug('master meta markers=%s', markers)
    return markers

def compare_meta_status(zone, log_status, sync_status):
    if len(log_status) != len(sync_status):
        log.error('len(log_status)=%d, len(sync_status)=%d', len(log_status), len(sync_status))
        return False

    msg = ''
    for i, l, s in zip(log_status, log_status.values(), sync_status.values()):
        if l > s:
            if len(msg):
                msg += ', '
            msg += 'shard=' + str(i) + ' master=' + l + ' target=' + s

    if len(msg) > 0:
        log.warning('zone %s behind master: %s', zone.name, msg)
        return False

    return True

def zone_meta_checkpoint(zone, meta_master_zone = None, master_status = None):
    if not meta_master_zone:
        meta_master_zone = zone.realm().meta_master_zone()
    if not master_status:
        master_status = meta_master_log_status(meta_master_zone)

    current_realm_epoch = realm.current_period.data['realm_epoch']

    log.info('starting meta checkpoint for zone=%s', zone.name)

    while True:
        period, realm_epoch, num_shards, sync_status = meta_sync_status(zone)
        if realm_epoch < current_realm_epoch:
            log.warning('zone %s is syncing realm epoch=%d, behind current realm epoch=%d',
                        zone.name, realm_epoch, current_realm_epoch)
        else:
            log.debug('log_status=%s', master_status)
            log.debug('sync_status=%s', sync_status)
            if compare_meta_status(zone, master_status, sync_status):
                break

        time.sleep(5)

    log.info('finish meta checkpoint for zone=%s', zone.name)

def zonegroup_meta_checkpoint(zonegroup, meta_master_zone = None, master_status = None):
    if not meta_master_zone:
        meta_master_zone = zonegroup.realm().meta_master_zone()
    if not master_status:
        master_status = meta_master_log_status(meta_master_zone)

    for zone in zonegroup.zones:
        if zone == meta_master_zone:
            continue
        zone_meta_checkpoint(zone, meta_master_zone, master_status)

def realm_meta_checkpoint(realm):
    log.info('meta checkpoint')

    meta_master_zone = realm.meta_master_zone()
    master_status = meta_master_log_status(meta_master_zone)

    for zonegroup in realm.current_period.zonegroups:
        zonegroup_meta_checkpoint(zonegroup, meta_master_zone, master_status)

def data_sync_status(target_zone, source_zone):
    if target_zone == source_zone:
        return None

    while True:
        cmd = ['data', 'sync', 'status'] + target_zone.zone_args()
        cmd += ['--source-zone', source_zone.name]
        data_sync_status_json, retcode = target_zone.cluster.admin(cmd, check_retcode=False, read_only=True)
        if retcode == 0:
            break

        assert(retcode == 2) # ENOENT

    data_sync_status_json = data_sync_status_json.decode('utf-8')
    log.debug('current data sync status=%s', data_sync_status_json)
    sync_status = json.loads(data_sync_status_json)

    global_sync_status=sync_status['sync_status']['info']['status']
    num_shards=sync_status['sync_status']['info']['num_shards']

    sync_markers=sync_status['sync_status']['markers']
    log.debug('sync_markers=%s', sync_markers)
    assert(num_shards == len(sync_markers))

    markers={}
    for i in range(num_shards):
        markers[i] = sync_markers[i]['val']['marker']

    return (num_shards, markers)

def bucket_sync_status(target_zone, source_zone, bucket_name):
    if target_zone == source_zone:
        return None

    cmd = ['bucket', 'sync', 'status'] + target_zone.zone_args()
    cmd += ['--source-zone', source_zone.name]
    cmd += ['--bucket', bucket_name]
    while True:
        bucket_sync_status_json, retcode = target_zone.cluster.admin(cmd, check_retcode=False, read_only=True)
        if retcode == 0:
            break

        assert(retcode == 2) # ENOENT

    bucket_sync_status_json = bucket_sync_status_json.decode('utf-8')
    log.debug('current bucket sync status=%s', bucket_sync_status_json)
    sync_status = json.loads(bucket_sync_status_json)

    markers={}
    for entry in sync_status:
        val = entry['val']
        if val['status'] == 'incremental-sync':
            pos = val['inc_marker']['position'].split('#')[-1] # get rid of shard id; e.g., 6#00000000002.132.3 -> 00000000002.132.3
        else:
            pos = ''
        markers[entry['key']] = pos

    return markers

def data_source_log_status(source_zone):
    source_cluster = source_zone.cluster
    cmd = ['datalog', 'status'] + source_zone.zone_args()
    datalog_status_json, retcode = source_cluster.rgw_admin(cmd, read_only=True)
    datalog_status = json.loads(datalog_status_json.decode('utf-8'))

    markers = {i: s['marker'] for i, s in enumerate(datalog_status)}
    log.debug('data markers for zone=%s markers=%s', source_zone.name, markers)
    return markers

def bucket_source_log_status(source_zone, bucket_name):
    cmd = ['bilog', 'status'] + source_zone.zone_args()
    cmd += ['--bucket', bucket_name]
    source_cluster = source_zone.cluster
    bilog_status_json, retcode = source_cluster.admin(cmd, read_only=True)
    bilog_status = json.loads(bilog_status_json.decode('utf-8'))

    m={}
    markers={}
    try:
        m = bilog_status['markers']
    except:
        pass

    for s in m:
        key = s['key']
        val = s['val']
        markers[key] = val

    log.debug('bilog markers for zone=%s bucket=%s markers=%s', source_zone.name, bucket_name, markers)
    return markers

def compare_data_status(target_zone, source_zone, log_status, sync_status):
    if len(log_status) != len(sync_status):
        log.error('len(log_status)=%d len(sync_status)=%d', len(log_status), len(sync_status))
        return False

    msg =  ''
    for i, l, s in zip(log_status, log_status.values(), sync_status.values()):
        if l > s:
            if len(msg):
                msg += ', '
            msg += 'shard=' + str(i) + ' master=' + l + ' target=' + s

    if len(msg) > 0:
        log.warning('data of zone %s behind zone %s: %s', target_zone.name, source_zone.name, msg)
        return False

    return True

def compare_bucket_status(target_zone, source_zone, bucket_name, log_status, sync_status):
    if len(log_status) != len(sync_status):
        log.error('len(log_status)=%d len(sync_status)=%d', len(log_status), len(sync_status))
        return False

    msg =  ''
    for i, l, s in zip(log_status, log_status.values(), sync_status.values()):
        if l > s:
            if len(msg):
                msg += ', '
            msg += 'shard=' + str(i) + ' master=' + l + ' target=' + s

    if len(msg) > 0:
        log.warning('bucket %s zone %s behind zone %s: %s', bucket_name, target_zone.name, source_zone.name, msg)
        return False

    return True

def zone_data_checkpoint(target_zone, source_zone):
    if target_zone == source_zone:
        return

    log.info('starting data checkpoint for target_zone=%s source_zone=%s', target_zone.name, source_zone.name)

    while True:
        log_status = data_source_log_status(source_zone)
        num_shards, sync_status = data_sync_status(target_zone, source_zone)

        log.debug('log_status=%s', log_status)
        log.debug('sync_status=%s', sync_status)

        if compare_data_status(target_zone, source_zone, log_status, sync_status):
            break

        time.sleep(5)

    log.info('finished data checkpoint for target_zone=%s source_zone=%s', target_zone.name, source_zone.name)

def zone_bucket_checkpoint(target_zone, source_zone, bucket_name):
    if target_zone == source_zone:
        return

    log.info('starting bucket checkpoint for target_zone=%s source_zone=%s bucket=%s', target_zone.name, source_zone.name, bucket_name)

    while True:
        log_status = bucket_source_log_status(source_zone, bucket_name)
        sync_status = bucket_sync_status(target_zone, source_zone, bucket_name)

        log.debug('log_status=%s', log_status)
        log.debug('sync_status=%s', sync_status)

        if compare_bucket_status(target_zone, source_zone, bucket_name, log_status, sync_status):
            break

        time.sleep(5)

    log.info('finished bucket checkpoint for target_zone=%s source_zone=%s bucket=%s', target_zone.name, source_zone.name, bucket_name)

def set_master_zone(zone):
    zone.modify(zone.cluster, ['--master'])
    zonegroup = zone.zonegroup
    zonegroup.period.update(zone, commit=True)
    zonegroup.master_zone = zone
    # wait for reconfiguration, so that later metadata requests go to the new master
    time.sleep(5)

def gen_bucket_name():
    global num_buckets

    num_buckets += 1
    return run_prefix + '-' + str(num_buckets)

def check_all_buckets_exist(zone, buckets):
    conn = get_zone_connection(zone, user.credentials)
    for b in buckets:
        try:
            conn.get_bucket(b)
        except:
            log.critical('zone %s does not contain bucket %s', zone.name, b)
            return False

    return True

def check_all_buckets_dont_exist(zone, buckets):
    conn = get_zone_connection(zone, user.credentials)
    for b in buckets:
        try:
            conn.get_bucket(b)
        except:
            continue

        log.critical('zone %s contains bucket %s', zone.zone, b)
        return False

    return True

def create_bucket_per_zone(zonegroup):
    buckets = []
    zone_bucket = {}
    for zone in zonegroup.zones:
        conn = get_zone_connection(zone, user.credentials)
        bucket_name = gen_bucket_name()
        log.info('create bucket zone=%s name=%s', zone.name, bucket_name)
        bucket = conn.create_bucket(bucket_name)
        buckets.append(bucket_name)
        zone_bucket[zone] = bucket

    return buckets, zone_bucket

def create_bucket_per_zone_in_realm():
    buckets = []
    zone_bucket = {}
    for zonegroup in realm.current_period.zonegroups:
        b, z = create_bucket_per_zone(zonegroup)
        buckets.extend(b)
        zone_bucket.update(z)
    return buckets, zone_bucket

def test_bucket_create():
    zonegroup = realm.master_zonegroup()
    buckets, _ = create_bucket_per_zone(zonegroup)
    zonegroup_meta_checkpoint(zonegroup)

    for zone in zonegroup.zones:
        assert check_all_buckets_exist(zone, buckets)

def test_bucket_recreate():
    zonegroup = realm.master_zonegroup()
    buckets, _ = create_bucket_per_zone(zonegroup)
    zonegroup_meta_checkpoint(zonegroup)

    for zone in zonegroup.zones:
        assert check_all_buckets_exist(zone, buckets)

    # recreate buckets on all zones, make sure they weren't removed
    for zone in zonegroup.zones:
        for bucket_name in buckets:
            conn = get_zone_connection(zone, user.credentials)
            bucket = conn.create_bucket(bucket_name)

    for zone in zonegroup.zones:
        assert check_all_buckets_exist(zone, buckets)

    zonegroup_meta_checkpoint(zonegroup)

    for zone in zonegroup.zones:
        assert check_all_buckets_exist(zone, buckets)

def test_bucket_remove():
    zonegroup = realm.master_zonegroup()
    buckets, zone_bucket = create_bucket_per_zone(zonegroup)
    zonegroup_meta_checkpoint(zonegroup)

    for zone in zonegroup.zones:
        assert check_all_buckets_exist(zone, buckets)

    for zone, bucket_name in zone_bucket.items():
        conn = get_zone_connection(zone, user.credentials)
        conn.delete_bucket(bucket_name)

    zonegroup_meta_checkpoint(zonegroup)

    for zone in zonegroup.zones:
        assert check_all_buckets_dont_exist(zone, buckets)

def get_bucket(zone, bucket_name):
    conn = get_zone_connection(zone, user.credentials)
    return conn.get_bucket(bucket_name)

def get_key(zone, bucket_name, obj_name):
    b = get_bucket(zone, bucket_name)
    return b.get_key(obj_name)

def new_key(zone, bucket_name, obj_name):
    b = get_bucket(zone, bucket_name)
    return b.new_key(obj_name)

def check_object_eq(k1, k2, check_extra = True):
    assert k1
    assert k2
    log.debug('comparing key name=%s', k1.name)
    eq(k1.name, k2.name)
    eq(k1.get_contents_as_string(), k2.get_contents_as_string())
    eq(k1.metadata, k2.metadata)
    eq(k1.cache_control, k2.cache_control)
    eq(k1.content_type, k2.content_type)
    eq(k1.content_encoding, k2.content_encoding)
    eq(k1.content_disposition, k2.content_disposition)
    eq(k1.content_language, k2.content_language)
    eq(k1.etag, k2.etag)
    eq(k1.last_modified, k2.last_modified)
    if check_extra:
        eq(k1.owner.id, k2.owner.id)
        eq(k1.owner.display_name, k2.owner.display_name)
    eq(k1.storage_class, k2.storage_class)
    eq(k1.size, k2.size)
    eq(k1.version_id, k2.version_id)
    eq(k1.encrypted, k2.encrypted)

def check_bucket_eq(zone1, zone2, bucket_name):
    log.info('comparing bucket=%s zones={%s, %s}', bucket_name, zone1.name, zone2.name)
    b1 = get_bucket(zone1, bucket_name)
    b2 = get_bucket(zone2, bucket_name)

    log.debug('bucket1 objects:')
    for o in b1.get_all_versions():
        log.debug('o=%s', o.name)
    log.debug('bucket2 objects:')
    for o in b2.get_all_versions():
        log.debug('o=%s', o.name)

    for k1, k2 in zip_longest(b1.get_all_versions(), b2.get_all_versions()):
        if k1 is None:
            log.critical('key=%s is missing from zone=%s', k2.name, zone1.name)
            assert False
        if k2 is None:
            log.critical('key=%s is missing from zone=%s', k1.name, zone2.name)
            assert False

        check_object_eq(k1, k2)

        # now get the keys through a HEAD operation, verify that the available data is the same
        k1_head = b1.get_key(k1.name)
        k2_head = b2.get_key(k2.name)

        check_object_eq(k1_head, k2_head, False)

    log.info('success, bucket identical: bucket=%s zones={%s, %s}', bucket_name, zone1.name, zone2.name)


def test_object_sync():
    zonegroup = realm.master_zonegroup()
    buckets, zone_bucket = create_bucket_per_zone(zonegroup)

    objnames = [ 'myobj', '_myobj', ':', '&' ]
    content = 'asdasd'

    # don't wait for meta sync just yet
    for zone, bucket_name in zone_bucket.items():
        for objname in objnames:
            k = new_key(zone, bucket_name, objname)
            k.set_contents_from_string(content)

    zonegroup_meta_checkpoint(zonegroup)

    for source_zone, bucket in zone_bucket.items():
        for target_zone in zonegroup.zones:
            if source_zone == target_zone:
                continue

            zone_bucket_checkpoint(target_zone, source_zone, bucket.name)
            check_bucket_eq(source_zone, target_zone, bucket)

def test_object_delete():
    zonegroup = realm.master_zonegroup()
    buckets, zone_bucket = create_bucket_per_zone(zonegroup)

    objname = 'myobj'
    content = 'asdasd'

    # don't wait for meta sync just yet
    for zone, bucket in zone_bucket.items():
        k = new_key(zone, bucket, objname)
        k.set_contents_from_string(content)

    zonegroup_meta_checkpoint(zonegroup)

    # check object exists
    for source_zone, bucket in zone_bucket.items():
        for target_zone in zonegroup.zones:
            if source_zone == target_zone:
                continue

            zone_bucket_checkpoint(target_zone, source_zone, bucket.name)
            check_bucket_eq(source_zone, target_zone, bucket)

    # check object removal
    for source_zone, bucket in zone_bucket.items():
        k = get_key(source_zone, bucket, objname)
        k.delete()
        for target_zone in zonegroup.zones:
            if source_zone == target_zone:
                continue

            zone_bucket_checkpoint(target_zone, source_zone, bucket.name)
            check_bucket_eq(source_zone, target_zone, bucket)

def get_latest_object_version(key):
    for k in key.bucket.list_versions(key.name):
        if k.is_latest:
            return k
    return None

def test_versioned_object_incremental_sync():
    zonegroup = realm.master_zonegroup()
    buckets, zone_bucket = create_bucket_per_zone(zonegroup)

    # enable versioning
    for zone, bucket in zone_bucket.items():
        bucket.configure_versioning(True)

    zonegroup_meta_checkpoint(zonegroup)

    # upload a dummy object to each bucket and wait for sync. this forces each
    # bucket to finish a full sync and switch to incremental
    for source_zone, bucket in zone_bucket.items():
        new_key(source_zone, bucket, 'dummy').set_contents_from_string('')
        for target_zone in zonegroup.zones:
            if source_zone == target_zone:
                continue
            zone_bucket_checkpoint(target_zone, source_zone, bucket.name)

    for _, bucket in zone_bucket.items():
        # create and delete multiple versions of an object from each zone
        for zone in zonegroup.zones:
            obj = 'obj-' + zone.name
            k = new_key(zone, bucket, obj)

            k.set_contents_from_string('version1')
            v = get_latest_object_version(k)
            log.debug('version1 id=%s', v.version_id)
            # don't delete version1 - this tests that the initial version
            # doesn't get squashed into later versions

            # create and delete the following object versions to test that
            # the operations don't race with each other during sync
            k.set_contents_from_string('version2')
            v = get_latest_object_version(k)
            log.debug('version2 id=%s', v.version_id)
            k.bucket.delete_key(obj, version_id=v.version_id)

            k.set_contents_from_string('version3')
            v = get_latest_object_version(k)
            log.debug('version3 id=%s', v.version_id)
            k.bucket.delete_key(obj, version_id=v.version_id)

    for source_zone, bucket in zone_bucket.items():
        for target_zone in zonegroup.zones:
            if source_zone == target_zone:
                continue
            zone_bucket_checkpoint(target_zone, source_zone, bucket.name)
            check_bucket_eq(source_zone, target_zone, bucket)

def test_bucket_versioning():
    buckets, zone_bucket = create_bucket_per_zone_in_realm()
    for zone, bucket in zone_bucket.items():
        bucket.configure_versioning(True)
        res = bucket.get_versioning_status()
        key = 'Versioning'
        assert(key in res and res[key] == 'Enabled')

def test_bucket_acl():
    buckets, zone_bucket = create_bucket_per_zone_in_realm()
    for zone, bucket in zone_bucket.items():
        assert(len(bucket.get_acl().acl.grants) == 1) # single grant on owner
        bucket.set_acl('public-read')
        assert(len(bucket.get_acl().acl.grants) == 2) # new grant on AllUsers

def test_bucket_delete_notempty():
    zonegroup = realm.master_zonegroup()
    buckets, zone_bucket = create_bucket_per_zone(zonegroup)
    zonegroup_meta_checkpoint(zonegroup)

    for zone, bucket_name in zone_bucket.items():
        # upload an object to each bucket on its own zone
        conn = get_zone_connection(zone, user.credentials)
        bucket = conn.get_bucket(bucket_name)
        k = bucket.new_key('foo')
        k.set_contents_from_string('bar')
        # attempt to delete the bucket before this object can sync
        try:
            conn.delete_bucket(bucket_name)
        except boto.exception.S3ResponseError as e:
            assert(e.error_code == 'BucketNotEmpty')
            continue
        assert False # expected 409 BucketNotEmpty

    # assert that each bucket still exists on the master
    c1 = get_zone_connection(zonegroup.master_zone, user.credentials)
    for _, bucket_name in zone_bucket.items():
        assert c1.get_bucket(bucket_name)

def test_multi_period_incremental_sync():
    zonegroup = realm.master_zonegroup()
    if len(zonegroup.zones) < 3:
        raise SkipTest("test_multi_period_incremental_sync skipped. Requires 3 or more zones in master zonegroup.")

    # periods to include in mdlog comparison
    mdlog_periods = [realm.current_period.id]

    # create a bucket in each zone
    buckets = []
    for zone in zonegroup.zones:
        conn = get_zone_connection(zone, user.credentials)
        bucket_name = gen_bucket_name()
        log.info('create bucket zone=%s name=%s', zone.name, bucket_name)
        bucket = conn.create_bucket(bucket_name)
        buckets.append(bucket_name)

    zonegroup_meta_checkpoint(zonegroup)

    z1, z2, z3 = zonegroup.zones[0:3]
    assert(z1 == zonegroup.master_zone)

    # kill zone 3 gateways to freeze sync status to incremental in first period
    z3.stop()

    # change master to zone 2 -> period 2
    set_master_zone(z2)
    mdlog_periods += [realm.current_period.id]

    # create another bucket in each zone, except for z3
    for zone in zonegroup.zones:
        if zone == z3:
            continue
        conn = get_zone_connection(zone, user.credentials)
        bucket_name = gen_bucket_name()
        log.info('create bucket zone=%s name=%s', zone.name, bucket_name)
        bucket = conn.create_bucket(bucket_name)
        buckets.append(bucket_name)

    # wait for zone 1 to sync
    zone_meta_checkpoint(z1)

    # change master back to zone 1 -> period 3
    set_master_zone(z1)
    mdlog_periods += [realm.current_period.id]

    # create another bucket in each zone, except for z3
    for zone in zonegroup.zones:
        if zone == z3:
            continue
        conn = get_zone_connection(zone, user.credentials)
        bucket_name = gen_bucket_name()
        log.info('create bucket zone=%s name=%s', zone.name, bucket_name)
        bucket = conn.create_bucket(bucket_name)
        buckets.append(bucket_name)

    # restart zone 3 gateway and wait for sync
    z3.start()
    zonegroup_meta_checkpoint(zonegroup)

    # verify that we end up with the same buckets
    for bucket_name in buckets:
        for source_zone, target_zone in combinations(zonegroup.zones, 2):
            check_bucket_eq(source_zone, target_zone, bucket_name)

    # verify that mdlogs are not empty and match for each period
    for period in mdlog_periods:
        master_mdlog = mdlog_list(z1, period)
        assert len(master_mdlog) > 0
        for zone in zonegroup.zones:
            if zone == z1:
                continue
            mdlog = mdlog_list(zone, period)
            assert len(mdlog) == len(master_mdlog)

    # autotrim mdlogs for master zone
    mdlog_autotrim(z1)

    # autotrim mdlogs for peers
    for zone in zonegroup.zones:
        if zone == z1:
            continue
        mdlog_autotrim(zone)

    # verify that mdlogs are empty for each period
    for period in mdlog_periods:
        for zone in zonegroup.zones:
            mdlog = mdlog_list(zone, period)
            assert len(mdlog) == 0

def test_zonegroup_remove():
    zonegroup = realm.master_zonegroup()
    if len(zonegroup.zones) < 2:
        raise SkipTest("test_zonegroup_remove skipped. Requires 2 or more zones in master zonegroup.")

    zonegroup_meta_checkpoint(zonegroup)
    z1, z2 = zonegroup.zones[0:2]
    c1, c2 = (z1.cluster, z2.cluster)

    # create a new zone in zonegroup on c2 and commit
    zone = Zone('remove', zonegroup, c2)
    zone.create(c2)
    zonegroup.zones.append(zone)
    zonegroup.period.update(zone, commit=True)

    # try to 'zone delete' the new zone from cluster 1
    # must fail with ENOENT because the zone is local to cluster 2
    retcode = zone.delete(c1, check_retcode=False)
    assert(retcode == 2) # ENOENT

    # use 'zonegroup remove', expecting success
    zonegroup.remove(c1, zone)

    # another 'zonegroup remove' should fail with ENOENT
    _, retcode = zonegroup.remove(c1, zone, check_retcode=False)
    assert(retcode == 2) # ENOENT

    # delete the new zone
    zone.delete(c2)

    # validate the resulting period
    zonegroup.period.update(z1, commit=True)
