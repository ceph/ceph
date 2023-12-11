import json
import random
import string
import sys
import time
import logging
import errno
import dateutil.parser

from itertools import combinations
from itertools import zip_longest
from io import StringIO

import boto
import boto.s3.connection
from boto.s3.website import WebsiteConfiguration
from boto.s3.cors import CORSConfiguration

from nose.tools import eq_ as eq
from nose.tools import assert_not_equal, assert_equal, assert_true, assert_false
from nose.plugins.attrib import attr
from nose.plugins.skip import SkipTest

from .multisite import Zone, ZoneGroup, Credentials

from .conn import get_gateway_connection
from .tools import assert_raises

class Config:
    """ test configuration """
    def __init__(self, **kwargs):
        # by default, wait up to 5 minutes before giving up on a sync checkpoint
        self.checkpoint_retries = kwargs.get('checkpoint_retries', 60)
        self.checkpoint_delay = kwargs.get('checkpoint_delay', 5)
        # allow some time for realm reconfiguration after changing master zone
        self.reconfigure_delay = kwargs.get('reconfigure_delay', 5)
        self.tenant = kwargs.get('tenant', '')

# rgw multisite tests, written against the interfaces provided in rgw_multi.
# these tests must be initialized and run by another module that provides
# implementations of these interfaces by calling init_multi()
realm = None
user = None
config = None
def init_multi(_realm, _user, _config=None):
    global realm
    realm = _realm
    global user
    user = _user
    global config
    config = _config or Config()
    realm_meta_checkpoint(realm)

def get_user():
    return user.id if user is not None else ''

def get_tenant():
    return config.tenant if config is not None and config.tenant is not None else ''

def get_realm():
    return realm

log = logging.getLogger('rgw_multi.tests')

num_buckets = 0
run_prefix=''.join(random.choice(string.ascii_lowercase) for _ in range(6))

num_roles = 0
num_topic = 0

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
    return json.loads(mdlog_json)

def mdlog_autotrim(zone):
    zone.cluster.admin(['mdlog', 'autotrim'])

def datalog_list(zone, args = None):
    cmd = ['datalog', 'list'] + (args or [])
    (datalog_json, _) = zone.cluster.admin(cmd, read_only=True)
    return json.loads(datalog_json)

def datalog_status(zone):
    cmd = ['datalog', 'status']
    (datalog_json, _) = zone.cluster.admin(cmd, read_only=True)
    return json.loads(datalog_json)

def datalog_autotrim(zone):
    zone.cluster.admin(['datalog', 'autotrim'])

def bilog_list(zone, bucket, args = None):
    cmd = ['bilog', 'list', '--bucket', bucket] + (args or [])
    cmd += ['--tenant', config.tenant, '--uid', user.name] if config.tenant else []
    bilog, _ = zone.cluster.admin(cmd, read_only=True)
    return json.loads(bilog)

def bilog_autotrim(zone, args = None):
    zone.cluster.admin(['bilog', 'autotrim'] + (args or []))

def bucket_layout(zone, bucket, args = None):
    (bl_output,_) = zone.cluster.admin(['bucket', 'layout', '--bucket', bucket] + (args or []))
    return json.loads(bl_output)

def parse_meta_sync_status(meta_sync_status_json):
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

    return global_sync_status, period, realm_epoch, num_shards, markers

def meta_sync_status(zone):
    for _ in range(config.checkpoint_retries):
        cmd = ['metadata', 'sync', 'status'] + zone.zone_args()
        meta_sync_status_json, retcode = zone.cluster.admin(cmd, check_retcode=False, read_only=True)
        if retcode == 0:
            return parse_meta_sync_status(meta_sync_status_json)
        assert(retcode == 2) # ENOENT
        time.sleep(config.checkpoint_delay)

    assert False, 'failed to read metadata sync status for zone=%s' % zone.name

def meta_master_log_status(master_zone):
    cmd = ['mdlog', 'status'] + master_zone.zone_args()
    mdlog_status_json, retcode = master_zone.cluster.admin(cmd, read_only=True)
    mdlog_status = json.loads(mdlog_status_json)

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

    for _ in range(config.checkpoint_retries):
        global_status, period, realm_epoch, num_shards, sync_status = meta_sync_status(zone)
        if global_status != 'sync':
            log.warning('zone %s has not started sync yet, state=%s', zone.name, global_status)
        elif realm_epoch < current_realm_epoch:
            log.warning('zone %s is syncing realm epoch=%d, behind current realm epoch=%d',
                        zone.name, realm_epoch, current_realm_epoch)
        else:
            log.debug('log_status=%s', master_status)
            log.debug('sync_status=%s', sync_status)
            if compare_meta_status(zone, master_status, sync_status):
                log.info('finish meta checkpoint for zone=%s', zone.name)
                return

        time.sleep(config.checkpoint_delay)
    assert False, 'failed meta checkpoint for zone=%s' % zone.name

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

def parse_data_sync_status(data_sync_status_json):
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

def data_sync_status(target_zone, source_zone):
    if target_zone == source_zone:
        return None

    for _ in range(config.checkpoint_retries):
        cmd = ['data', 'sync', 'status'] + target_zone.zone_args()
        cmd += ['--source-zone', source_zone.name]
        data_sync_status_json, retcode = target_zone.cluster.admin(cmd, check_retcode=False, read_only=True)
        if retcode == 0:
            return parse_data_sync_status(data_sync_status_json)

        assert(retcode == 2) # ENOENT
        time.sleep(config.checkpoint_delay)

    assert False, 'failed to read data sync status for target_zone=%s source_zone=%s' % \
                  (target_zone.name, source_zone.name)

def bucket_sync_status(target_zone, source_zone, bucket_name):
    if target_zone == source_zone:
        return None

    cmd = ['bucket', 'sync', 'markers'] + target_zone.zone_args()
    cmd += ['--source-zone', source_zone.name]
    cmd += ['--bucket', bucket_name]
    cmd += ['--tenant', config.tenant, '--uid', user.name] if config.tenant else []
    while True:
        bucket_sync_status_json, retcode = target_zone.cluster.admin(cmd, check_retcode=False, read_only=True)
        if retcode == 0:
            break

        assert(retcode == 2) # ENOENT

    sync_status = json.loads(bucket_sync_status_json)

    markers={}
    for entry in sync_status:
        val = entry['val']
        pos = val['inc_marker']['position'].split('#')[-1] # get rid of shard id; e.g., 6#00000000002.132.3 -> 00000000002.132.3
        markers[entry['key']] = pos

    return markers

def data_source_log_status(source_zone):
    source_cluster = source_zone.cluster
    cmd = ['datalog', 'status'] + source_zone.zone_args()
    datalog_status_json, retcode = source_cluster.admin(cmd, read_only=True)
    datalog_status = json.loads(datalog_status_json)

    markers = {i: s['marker'] for i, s in enumerate(datalog_status)}
    log.debug('data markers for zone=%s markers=%s', source_zone.name, markers)
    return markers

def bucket_source_log_status(source_zone, bucket_name):
    cmd = ['bilog', 'status'] + source_zone.zone_args()
    cmd += ['--bucket', bucket_name]
    cmd += ['--tenant', config.tenant, '--uid', user.name] if config.tenant else []
    source_cluster = source_zone.cluster
    bilog_status_json, retcode = source_cluster.admin(cmd, read_only=True)
    bilog_status = json.loads(bilog_status_json)

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
    if not target_zone.syncs_from(source_zone.name):
        return

    log_status = data_source_log_status(source_zone)
    log.info('starting data checkpoint for target_zone=%s source_zone=%s', target_zone.name, source_zone.name)

    for _ in range(config.checkpoint_retries):
        num_shards, sync_status = data_sync_status(target_zone, source_zone)

        log.debug('log_status=%s', log_status)
        log.debug('sync_status=%s', sync_status)

        if compare_data_status(target_zone, source_zone, log_status, sync_status):
            log.info('finished data checkpoint for target_zone=%s source_zone=%s',
                     target_zone.name, source_zone.name)
            return
        time.sleep(config.checkpoint_delay)

    assert False, 'failed data checkpoint for target_zone=%s source_zone=%s' % \
                  (target_zone.name, source_zone.name)

def zonegroup_data_checkpoint(zonegroup_conns):
    for source_conn in zonegroup_conns.rw_zones:
        for target_conn in zonegroup_conns.zones:
            if source_conn.zone == target_conn.zone:
                continue
            log.debug('data checkpoint: source=%s target=%s', source_conn.zone.name, target_conn.zone.name)
            zone_data_checkpoint(target_conn.zone, source_conn.zone)

def zone_bucket_checkpoint(target_zone, source_zone, bucket_name):
    if not target_zone.syncs_from(source_zone.name):
        return

    cmd = ['bucket', 'sync', 'checkpoint']
    cmd += ['--bucket', bucket_name, '--source-zone', source_zone.name]
    retry_delay_ms = config.checkpoint_delay * 1000
    timeout_sec = config.checkpoint_retries * config.checkpoint_delay
    cmd += ['--retry-delay-ms', str(retry_delay_ms), '--timeout-sec', str(timeout_sec)]
    cmd += target_zone.zone_args()
    target_zone.cluster.admin(cmd, debug_rgw=1)

def zonegroup_bucket_checkpoint(zonegroup_conns, bucket_name):
    for source_conn in zonegroup_conns.rw_zones:
        for target_conn in zonegroup_conns.zones:
            if source_conn.zone == target_conn.zone:
                continue
            log.debug('bucket checkpoint: source=%s target=%s bucket=%s', source_conn.zone.name, target_conn.zone.name, bucket_name)
            zone_bucket_checkpoint(target_conn.zone, source_conn.zone, bucket_name)
    for source_conn, target_conn in combinations(zonegroup_conns.zones, 2):
        if target_conn.zone.has_buckets():
            target_conn.check_bucket_eq(source_conn, bucket_name)

def set_master_zone(zone):
    zone.modify(zone.cluster, ['--master'])
    zonegroup = zone.zonegroup
    zonegroup.period.update(zone, commit=True)
    zonegroup.master_zone = zone
    log.info('Set master zone=%s, waiting %ds for reconfiguration..', zone.name, config.reconfigure_delay)
    time.sleep(config.reconfigure_delay)

def set_sync_from_all(zone, flag):
    s = 'true' if flag else 'false'
    zone.modify(zone.cluster, ['--sync-from-all={}'.format(s)])
    zonegroup = zone.zonegroup
    zonegroup.period.update(zone, commit=True)
    log.info('Set sync_from_all flag on zone %s to %s', zone.name, s)
    time.sleep(config.reconfigure_delay)

def set_redirect_zone(zone, redirect_zone):
    id_str = redirect_zone.id if redirect_zone else ''
    zone.modify(zone.cluster, ['--redirect-zone={}'.format(id_str)])
    zonegroup = zone.zonegroup
    zonegroup.period.update(zone, commit=True)
    log.info('Set redirect_zone zone %s to "%s"', zone.name, id_str)
    time.sleep(config.reconfigure_delay)

def enable_bucket_sync(zone, bucket_name):
    cmd = ['bucket', 'sync', 'enable', '--bucket', bucket_name] + zone.zone_args()
    zone.cluster.admin(cmd)

def disable_bucket_sync(zone, bucket_name):
    cmd = ['bucket', 'sync', 'disable', '--bucket', bucket_name] + zone.zone_args()
    zone.cluster.admin(cmd)

def check_buckets_sync_status_obj_not_exist(zone, buckets):
    for _ in range(config.checkpoint_retries):
        cmd = ['log', 'list'] + zone.zone_arg()
        log_list, ret = zone.cluster.admin(cmd, check_retcode=False, read_only=True)
        for bucket in buckets:
            if log_list.find(':'+bucket+":") >= 0:
                break
        else:
            return
        time.sleep(config.checkpoint_delay)
    assert False

def gen_bucket_name():
    global num_buckets

    num_buckets += 1
    return run_prefix + '-' + str(num_buckets)

def gen_role_name():
    global num_roles

    num_roles += 1
    return "roles" + '-' + run_prefix + '-' + str(num_roles)


def gen_topic_name():
    global num_topic

    num_topic += 1
    return "topic" + '-' + run_prefix + '-' + str(num_topic)

class ZonegroupConns:
    def __init__(self, zonegroup):
        self.zonegroup = zonegroup
        self.zones = []
        self.ro_zones = []
        self.rw_zones = []
        self.master_zone = None

        for z in zonegroup.zones:
            zone_conn = z.get_conn(user.credentials)
            self.zones.append(zone_conn)
            if z.is_read_only():
                self.ro_zones.append(zone_conn)
            else:
                self.rw_zones.append(zone_conn)

            if z == zonegroup.master_zone:
                self.master_zone = zone_conn

def check_all_buckets_exist(zone_conn, buckets):
    if not zone_conn.zone.has_buckets():
        return True

    for b in buckets:
        try:
            zone_conn.get_bucket(b)
        except:
            log.critical('zone %s does not contain bucket %s', zone_conn.zone.name, b)
            return False

    return True

def check_all_buckets_dont_exist(zone_conn, buckets):
    if not zone_conn.zone.has_buckets():
        return True

    for b in buckets:
        try:
            zone_conn.get_bucket(b)
        except:
            continue

        log.critical('zone %s contains bucket %s', zone.zone, b)
        return False

    return True


def get_topics(zone):
    """
    Get list of topics in cluster.
    """
    cmd = ['topic', 'list'] + zone.zone_args()
    topics_json, _ = zone.cluster.admin(cmd, read_only=True)
    topics = json.loads(topics_json)
    return topics['topics']


def create_topic_per_zone(zonegroup_conns, topics_per_zone=1):
    topics = []
    zone_topic = []
    for zone in zonegroup_conns.rw_zones:
        for _ in range(topics_per_zone):
            topic_name = gen_topic_name()
            log.info('create topic zone=%s name=%s', zone.name, topic_name)
            attributes = {
                "push-endpoint": "http://kaboom:9999",
                "persistent": "true",
            }
            topic_arn = zone.create_topic(topic_name, attributes)
            topics.append(topic_arn)
            zone_topic.append((zone, topic_arn))

    return topics, zone_topic

def create_role_per_zone(zonegroup_conns, roles_per_zone = 1):
    roles = []
    zone_role = []
    for zone in zonegroup_conns.rw_zones:
        for i in range(roles_per_zone):
            role_name = gen_role_name()
            log.info('create role zone=%s name=%s', zone.name, role_name)
            policy_document = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":[\"arn:aws:iam:::user/testuser\"]},\"Action\":[\"sts:AssumeRole\"]}]}"
            role = zone.create_role("", role_name, policy_document, "")
            roles.append(role_name)
            zone_role.append((zone, role))

    return roles, zone_role

def create_bucket_per_zone(zonegroup_conns, buckets_per_zone = 1):
    buckets = []
    zone_bucket = []
    for zone in zonegroup_conns.rw_zones:
        for i in range(buckets_per_zone):
            bucket_name = gen_bucket_name()
            log.info('create bucket zone=%s name=%s', zone.name, bucket_name)
            bucket = zone.create_bucket(bucket_name)
            buckets.append(bucket_name)
            zone_bucket.append((zone, bucket))

    return buckets, zone_bucket

def create_bucket_per_zone_in_realm():
    buckets = []
    zone_bucket = []
    for zonegroup in realm.current_period.zonegroups:
        zg_conn = ZonegroupConns(zonegroup)
        b, z = create_bucket_per_zone(zg_conn)
        buckets.extend(b)
        zone_bucket.extend(z)
    return buckets, zone_bucket

def test_bucket_create():
    zonegroup = realm.master_zonegroup()
    zonegroup_conns = ZonegroupConns(zonegroup)
    buckets, _ = create_bucket_per_zone(zonegroup_conns)
    zonegroup_meta_checkpoint(zonegroup)

    for zone in zonegroup_conns.zones:
        assert check_all_buckets_exist(zone, buckets)

def test_bucket_recreate():
    zonegroup = realm.master_zonegroup()
    zonegroup_conns = ZonegroupConns(zonegroup)
    buckets, _ = create_bucket_per_zone(zonegroup_conns)
    zonegroup_meta_checkpoint(zonegroup)


    for zone in zonegroup_conns.zones:
        assert check_all_buckets_exist(zone, buckets)

    # recreate buckets on all zones, make sure they weren't removed
    for zone in zonegroup_conns.rw_zones:
        for bucket_name in buckets:
            bucket = zone.create_bucket(bucket_name)

    for zone in zonegroup_conns.zones:
        assert check_all_buckets_exist(zone, buckets)

    zonegroup_meta_checkpoint(zonegroup)

    for zone in zonegroup_conns.zones:
        assert check_all_buckets_exist(zone, buckets)

def test_bucket_remove():
    zonegroup = realm.master_zonegroup()
    zonegroup_conns = ZonegroupConns(zonegroup)
    buckets, zone_bucket = create_bucket_per_zone(zonegroup_conns)
    zonegroup_meta_checkpoint(zonegroup)

    for zone in zonegroup_conns.zones:
        assert check_all_buckets_exist(zone, buckets)

    for zone, bucket_name in zone_bucket:
        zone.conn.delete_bucket(bucket_name)

    zonegroup_meta_checkpoint(zonegroup)

    for zone in zonegroup_conns.zones:
        assert check_all_buckets_dont_exist(zone, buckets)

def get_bucket(zone, bucket_name):
    return zone.conn.get_bucket(bucket_name)

def get_key(zone, bucket_name, obj_name):
    b = get_bucket(zone, bucket_name)
    return b.get_key(obj_name)

def new_key(zone, bucket_name, obj_name):
    b = get_bucket(zone, bucket_name)
    return b.new_key(obj_name)

def check_bucket_eq(zone_conn1, zone_conn2, bucket):
    if zone_conn2.zone.has_buckets():
        zone_conn2.check_bucket_eq(zone_conn1, bucket.name)

def check_role_eq(zone_conn1, zone_conn2, role):
    if zone_conn2.zone.has_roles():
        zone_conn2.check_role_eq(zone_conn1, role['create_role_response']['create_role_result']['role']['role_name'])

def test_object_sync():
    zonegroup = realm.master_zonegroup()
    zonegroup_conns = ZonegroupConns(zonegroup)
    buckets, zone_bucket = create_bucket_per_zone(zonegroup_conns)

    objnames = [ 'myobj', '_myobj', ':', '&' ]
    content = 'asdasd'

    # don't wait for meta sync just yet
    for zone, bucket_name in zone_bucket:
        for objname in objnames:
            k = new_key(zone, bucket_name, objname)
            k.set_contents_from_string(content)

    zonegroup_meta_checkpoint(zonegroup)

    for source_conn, bucket in zone_bucket:
        for target_conn in zonegroup_conns.zones:
            if source_conn.zone == target_conn.zone:
                continue

            zone_bucket_checkpoint(target_conn.zone, source_conn.zone, bucket.name)
            check_bucket_eq(source_conn, target_conn, bucket)

def test_object_delete():
    zonegroup = realm.master_zonegroup()
    zonegroup_conns = ZonegroupConns(zonegroup)
    buckets, zone_bucket = create_bucket_per_zone(zonegroup_conns)

    objname = 'myobj'
    content = 'asdasd'

    # don't wait for meta sync just yet
    for zone, bucket in zone_bucket:
        k = new_key(zone, bucket, objname)
        k.set_contents_from_string(content)

    zonegroup_meta_checkpoint(zonegroup)

    # check object exists
    for source_conn, bucket in zone_bucket:
        for target_conn in zonegroup_conns.zones:
            if source_conn.zone == target_conn.zone:
                continue

            zone_bucket_checkpoint(target_conn.zone, source_conn.zone, bucket.name)
            check_bucket_eq(source_conn, target_conn, bucket)

    # check object removal
    for source_conn, bucket in zone_bucket:
        k = get_key(source_conn, bucket, objname)
        k.delete()
        for target_conn in zonegroup_conns.zones:
            if source_conn.zone == target_conn.zone:
                continue

            zone_bucket_checkpoint(target_conn.zone, source_conn.zone, bucket.name)
            check_bucket_eq(source_conn, target_conn, bucket)

def get_latest_object_version(key):
    for k in key.bucket.list_versions(key.name):
        if k.is_latest:
            return k
    return None

def test_versioned_object_incremental_sync():
    zonegroup = realm.master_zonegroup()
    zonegroup_conns = ZonegroupConns(zonegroup)
    buckets, zone_bucket = create_bucket_per_zone(zonegroup_conns)

    # enable versioning
    for _, bucket in zone_bucket:
        bucket.configure_versioning(True)

    zonegroup_meta_checkpoint(zonegroup)

    # upload a dummy object to each bucket and wait for sync. this forces each
    # bucket to finish a full sync and switch to incremental
    for source_conn, bucket in zone_bucket:
        new_key(source_conn, bucket, 'dummy').set_contents_from_string('')
        for target_conn in zonegroup_conns.zones:
            if source_conn.zone == target_conn.zone:
                continue
            zone_bucket_checkpoint(target_conn.zone, source_conn.zone, bucket.name)

    for _, bucket in zone_bucket:
        # create and delete multiple versions of an object from each zone
        for zone_conn in zonegroup_conns.rw_zones:
            obj = 'obj-' + zone_conn.name
            k = new_key(zone_conn, bucket, obj)

            k.set_contents_from_string('version1')
            log.debug('version1 id=%s', k.version_id)
            # don't delete version1 - this tests that the initial version
            # doesn't get squashed into later versions

            # create and delete the following object versions to test that
            # the operations don't race with each other during sync
            k.set_contents_from_string('version2')
            log.debug('version2 id=%s', k.version_id)
            k.bucket.delete_key(obj, version_id=k.version_id)

            k.set_contents_from_string('version3')
            log.debug('version3 id=%s', k.version_id)
            k.bucket.delete_key(obj, version_id=k.version_id)

    for _, bucket in zone_bucket:
        zonegroup_bucket_checkpoint(zonegroup_conns, bucket.name)

    for _, bucket in zone_bucket:
        # overwrite the acls to test that metadata-only entries are applied
        for zone_conn in zonegroup_conns.rw_zones:
            obj = 'obj-' + zone_conn.name
            k = new_key(zone_conn, bucket.name, obj)
            v = get_latest_object_version(k)
            v.make_public()

    for _, bucket in zone_bucket:
        zonegroup_bucket_checkpoint(zonegroup_conns, bucket.name)

def test_concurrent_versioned_object_incremental_sync():
    zonegroup = realm.master_zonegroup()
    zonegroup_conns = ZonegroupConns(zonegroup)
    zone = zonegroup_conns.rw_zones[0]

    # create a versioned bucket
    bucket = zone.create_bucket(gen_bucket_name())
    log.debug('created bucket=%s', bucket.name)
    bucket.configure_versioning(True)

    zonegroup_meta_checkpoint(zonegroup)

    # upload a dummy object and wait for sync. this forces each zone to finish
    # a full sync and switch to incremental
    new_key(zone, bucket, 'dummy').set_contents_from_string('')
    zonegroup_bucket_checkpoint(zonegroup_conns, bucket.name)

    # create several concurrent versions on each zone and let them race to sync
    obj = 'obj'
    for i in range(10):
        for zone_conn in zonegroup_conns.rw_zones:
            k = new_key(zone_conn, bucket, obj)
            k.set_contents_from_string('version1')
            log.debug('zone=%s version=%s', zone_conn.zone.name, k.version_id)

    zonegroup_bucket_checkpoint(zonegroup_conns, bucket.name)
    zonegroup_data_checkpoint(zonegroup_conns)

def test_version_suspended_incremental_sync():
    zonegroup = realm.master_zonegroup()
    zonegroup_conns = ZonegroupConns(zonegroup)

    zone = zonegroup_conns.rw_zones[0]

    # create a non-versioned bucket
    bucket = zone.create_bucket(gen_bucket_name())
    log.debug('created bucket=%s', bucket.name)
    zonegroup_meta_checkpoint(zonegroup)

    # upload an initial object
    key1 = new_key(zone, bucket, 'obj')
    key1.set_contents_from_string('')
    log.debug('created initial version id=%s', key1.version_id)
    zonegroup_bucket_checkpoint(zonegroup_conns, bucket.name)

    # enable versioning
    bucket.configure_versioning(True)
    zonegroup_meta_checkpoint(zonegroup)

    # re-upload the object as a new version
    key2 = new_key(zone, bucket, 'obj')
    key2.set_contents_from_string('')
    log.debug('created new version id=%s', key2.version_id)
    zonegroup_bucket_checkpoint(zonegroup_conns, bucket.name)

    # suspend versioning
    bucket.configure_versioning(False)
    zonegroup_meta_checkpoint(zonegroup)

    # re-upload the object as a 'null' version
    key3 = new_key(zone, bucket, 'obj')
    key3.set_contents_from_string('')
    log.debug('created null version id=%s', key3.version_id)
    zonegroup_bucket_checkpoint(zonegroup_conns, bucket.name)

def test_delete_marker_full_sync():
    zonegroup = realm.master_zonegroup()
    zonegroup_conns = ZonegroupConns(zonegroup)
    buckets, zone_bucket = create_bucket_per_zone(zonegroup_conns)

    # enable versioning
    for _, bucket in zone_bucket:
        bucket.configure_versioning(True)
    zonegroup_meta_checkpoint(zonegroup)

    for zone, bucket in zone_bucket:
        # upload an initial object
        key1 = new_key(zone, bucket, 'obj')
        key1.set_contents_from_string('')

        # create a delete marker
        key2 = new_key(zone, bucket, 'obj')
        key2.delete()

    # wait for full sync
    for _, bucket in zone_bucket:
        zonegroup_bucket_checkpoint(zonegroup_conns, bucket.name)

def test_suspended_delete_marker_full_sync():
    zonegroup = realm.master_zonegroup()
    zonegroup_conns = ZonegroupConns(zonegroup)
    buckets, zone_bucket = create_bucket_per_zone(zonegroup_conns)

    # enable/suspend versioning
    for _, bucket in zone_bucket:
        bucket.configure_versioning(True)
        bucket.configure_versioning(False)
    zonegroup_meta_checkpoint(zonegroup)

    for zone, bucket in zone_bucket:
        # upload an initial object
        key1 = new_key(zone, bucket, 'obj')
        key1.set_contents_from_string('')

        # create a delete marker
        key2 = new_key(zone, bucket, 'obj')
        key2.delete()

    # wait for full sync
    for _, bucket in zone_bucket:
        zonegroup_bucket_checkpoint(zonegroup_conns, bucket.name)

def test_bucket_versioning():
    buckets, zone_bucket = create_bucket_per_zone_in_realm()
    for _, bucket in zone_bucket:
        bucket.configure_versioning(True)
        res = bucket.get_versioning_status()
        key = 'Versioning'
        assert(key in res and res[key] == 'Enabled')

def test_bucket_acl():
    buckets, zone_bucket = create_bucket_per_zone_in_realm()
    for _, bucket in zone_bucket:
        assert(len(bucket.get_acl().acl.grants) == 1) # single grant on owner
        bucket.set_acl('public-read')
        assert(len(bucket.get_acl().acl.grants) == 2) # new grant on AllUsers

def test_bucket_cors():
    buckets, zone_bucket = create_bucket_per_zone_in_realm()
    for _, bucket in zone_bucket:
        cors_cfg = CORSConfiguration()
        cors_cfg.add_rule(['DELETE'], 'https://www.example.com', allowed_header='*', max_age_seconds=3000)
        bucket.set_cors(cors_cfg)
        assert(bucket.get_cors().to_xml() == cors_cfg.to_xml())

def test_bucket_delete_notempty():
    zonegroup = realm.master_zonegroup()
    zonegroup_conns = ZonegroupConns(zonegroup)
    buckets, zone_bucket = create_bucket_per_zone(zonegroup_conns)
    zonegroup_meta_checkpoint(zonegroup)

    for zone_conn, bucket_name in zone_bucket:
        # upload an object to each bucket on its own zone
        conn = zone_conn.get_connection()
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
    c1 = zonegroup_conns.master_zone.conn
    for _, bucket_name in zone_bucket:
        assert c1.get_bucket(bucket_name)

def test_multi_period_incremental_sync():
    zonegroup = realm.master_zonegroup()
    if len(zonegroup.zones) < 3:
        raise SkipTest("test_multi_period_incremental_sync skipped. Requires 3 or more zones in master zonegroup.")

    # periods to include in mdlog comparison
    mdlog_periods = [realm.current_period.id]

    # create a bucket in each zone
    zonegroup_conns = ZonegroupConns(zonegroup)
    buckets, zone_bucket = create_bucket_per_zone(zonegroup_conns)

    zonegroup_meta_checkpoint(zonegroup)

    z1, z2, z3 = zonegroup.zones[0:3]
    assert(z1 == zonegroup.master_zone)

    # kill zone 3 gateways to freeze sync status to incremental in first period
    z3.stop()

    # change master to zone 2 -> period 2
    set_master_zone(z2)
    mdlog_periods += [realm.current_period.id]

    for zone_conn, _ in zone_bucket:
        if zone_conn.zone == z3:
            continue
        bucket_name = gen_bucket_name()
        log.info('create bucket zone=%s name=%s', zone_conn.name, bucket_name)
        bucket = zone_conn.conn.create_bucket(bucket_name)
        buckets.append(bucket_name)

    # wait for zone 1 to sync
    zone_meta_checkpoint(z1)

    # change master back to zone 1 -> period 3
    set_master_zone(z1)
    mdlog_periods += [realm.current_period.id]

    for zone_conn, bucket_name in zone_bucket:
        if zone_conn.zone == z3:
            continue
        bucket_name = gen_bucket_name()
        log.info('create bucket zone=%s name=%s', zone_conn.name, bucket_name)
        zone_conn.conn.create_bucket(bucket_name)
        buckets.append(bucket_name)

    # restart zone 3 gateway and wait for sync
    z3.start()
    zonegroup_meta_checkpoint(zonegroup)

    # verify that we end up with the same objects
    for bucket_name in buckets:
        for source_conn, _ in zone_bucket:
            for target_conn in zonegroup_conns.zones:
                if source_conn.zone == target_conn.zone:
                    continue

                if target_conn.zone.has_buckets():
                    target_conn.check_bucket_eq(source_conn, bucket_name)

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

def test_datalog_autotrim():
    zonegroup = realm.master_zonegroup()
    zonegroup_conns = ZonegroupConns(zonegroup)
    buckets, zone_bucket = create_bucket_per_zone(zonegroup_conns)

    # upload an object to each zone to generate a datalog entry
    for zone, bucket in zone_bucket:
        k = new_key(zone, bucket.name, 'key')
        k.set_contents_from_string('body')

    # wait for metadata and data sync to catch up
    zonegroup_meta_checkpoint(zonegroup)
    zonegroup_data_checkpoint(zonegroup_conns)

    # trim each datalog
    for zone, _ in zone_bucket:
        # read max markers for each shard
        status = datalog_status(zone.zone)

        datalog_autotrim(zone.zone)

        for shard_id, shard_status in enumerate(status):
            try:
                before_trim = dateutil.parser.isoparse(shard_status['last_update'])
            except: # empty timestamps look like "0.000000" and will fail here
                continue
            entries = datalog_list(zone.zone, ['--shard-id', str(shard_id), '--max-entries', '1'])
            if not len(entries):
                continue
            after_trim = dateutil.parser.isoparse(entries[0]['timestamp'])
            assert before_trim < after_trim, "any datalog entries must be newer than trim"

def test_multi_zone_redirect():
    zonegroup = realm.master_zonegroup()
    if len(zonegroup.rw_zones) < 2:
        raise SkipTest("test_multi_period_incremental_sync skipped. Requires 3 or more zones in master zonegroup.")

    zonegroup_conns = ZonegroupConns(zonegroup)
    (zc1, zc2) = zonegroup_conns.rw_zones[0:2]

    z1, z2 = (zc1.zone, zc2.zone)

    set_sync_from_all(z2, False)

    # create a bucket on the first zone
    bucket_name = gen_bucket_name()
    log.info('create bucket zone=%s name=%s', z1.name, bucket_name)
    bucket = zc1.conn.create_bucket(bucket_name)
    obj = 'testredirect'

    key = bucket.new_key(obj)
    data = 'A'*512
    key.set_contents_from_string(data)

    zonegroup_meta_checkpoint(zonegroup)

    # try to read object from second zone (should fail)
    bucket2 = get_bucket(zc2, bucket_name)
    assert_raises(boto.exception.S3ResponseError, bucket2.get_key, obj)

    set_redirect_zone(z2, z1)

    key2 = bucket2.get_key(obj)

    eq(data, key2.get_contents_as_string(encoding='ascii'))

    key = bucket.new_key(obj)

    for x in ['a', 'b', 'c', 'd']:
        data = x*512
        key.set_contents_from_string(data)
        eq(data, key2.get_contents_as_string(encoding='ascii'))

    # revert config changes
    set_sync_from_all(z2, True)
    set_redirect_zone(z2, None)

def test_zonegroup_remove():
    zonegroup = realm.master_zonegroup()
    zonegroup_conns = ZonegroupConns(zonegroup)
    if len(zonegroup.zones) < 2:
        raise SkipTest("test_zonegroup_remove skipped. Requires 2 or more zones in master zonegroup.")

    zonegroup_meta_checkpoint(zonegroup)
    z1, z2 = zonegroup.zones[0:2]
    c1, c2 = (z1.cluster, z2.cluster)

    # get admin credentials out of existing zone
    system_key = z1.data['system_key']
    admin_creds = Credentials(system_key['access_key'], system_key['secret_key'])

    # create a new zone in zonegroup on c2 and commit
    zone = Zone('remove', zonegroup, c2)
    zone.create(c2, admin_creds.credential_args())
    zonegroup.zones.append(zone)
    zonegroup.period.update(zone, commit=True)

    zonegroup.remove(c1, zone)

    # another 'zonegroup remove' should fail with ENOENT
    _, retcode = zonegroup.remove(c1, zone, check_retcode=False)
    assert(retcode == 2) # ENOENT

    # delete the new zone
    zone.delete(c2)

    # validate the resulting period
    zonegroup.period.update(z1, commit=True)


def test_zg_master_zone_delete():

    master_zg = realm.master_zonegroup()
    master_zone = master_zg.master_zone

    assert(len(master_zg.zones) >= 1)
    master_cluster = master_zg.zones[0].cluster

    rm_zg = ZoneGroup('remove_zg')
    rm_zg.create(master_cluster)

    rm_zone = Zone('remove', rm_zg, master_cluster)
    rm_zone.create(master_cluster)
    master_zg.period.update(master_zone, commit=True)


    rm_zone.delete(master_cluster)
    # Period update: This should now fail as the zone will be the master zone
    # in that zg
    _, retcode = master_zg.period.update(master_zone, check_retcode=False)
    assert(retcode == errno.EINVAL)

    # Proceed to delete the zonegroup as well, previous period now does not
    # contain a dangling master_zone, this must succeed
    rm_zg.delete(master_cluster)
    master_zg.period.update(master_zone, commit=True)

def test_set_bucket_website():
    buckets, zone_bucket = create_bucket_per_zone_in_realm()
    for _, bucket in zone_bucket:
        website_cfg = WebsiteConfiguration(suffix='index.html',error_key='error.html')
        try:
            bucket.set_website_configuration(website_cfg)
        except boto.exception.S3ResponseError as e:
            if e.error_code == 'MethodNotAllowed':
                raise SkipTest("test_set_bucket_website skipped. Requires rgw_enable_static_website = 1.")
        assert(bucket.get_website_configuration_with_xml()[1] == website_cfg.to_xml())

def test_set_bucket_policy():
    policy = '''{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": "*"
  }]
}'''
    buckets, zone_bucket = create_bucket_per_zone_in_realm()
    for _, bucket in zone_bucket:
        bucket.set_policy(policy)
        assert(bucket.get_policy().decode('ascii') == policy)

@attr('bucket_sync_disable')
def test_bucket_sync_disable():
    zonegroup = realm.master_zonegroup()
    zonegroup_conns = ZonegroupConns(zonegroup)
    buckets, zone_bucket = create_bucket_per_zone(zonegroup_conns)
    zonegroup_meta_checkpoint(zonegroup)

    for bucket_name in buckets:
        disable_bucket_sync(realm.meta_master_zone(), bucket_name)

    for zone in zonegroup.zones:
        check_buckets_sync_status_obj_not_exist(zone, buckets)

    zonegroup_data_checkpoint(zonegroup_conns)

@attr('bucket_sync_disable')
def test_bucket_sync_enable_right_after_disable():
    zonegroup = realm.master_zonegroup()
    zonegroup_conns = ZonegroupConns(zonegroup)
    buckets, zone_bucket = create_bucket_per_zone(zonegroup_conns)

    objnames = ['obj1', 'obj2', 'obj3', 'obj4']
    content = 'asdasd'

    for zone, bucket in zone_bucket:
        for objname in objnames:
            k = new_key(zone, bucket.name, objname)
            k.set_contents_from_string(content)

    zonegroup_meta_checkpoint(zonegroup)

    for bucket_name in buckets:
        zonegroup_bucket_checkpoint(zonegroup_conns, bucket_name)

    for bucket_name in buckets:
        disable_bucket_sync(realm.meta_master_zone(), bucket_name)
        enable_bucket_sync(realm.meta_master_zone(), bucket_name)

    objnames_2 = ['obj5', 'obj6', 'obj7', 'obj8']

    for zone, bucket in zone_bucket:
        for objname in objnames_2:
            k = new_key(zone, bucket.name, objname)
            k.set_contents_from_string(content)

    for bucket_name in buckets:
        zonegroup_bucket_checkpoint(zonegroup_conns, bucket_name)

    zonegroup_data_checkpoint(zonegroup_conns)

@attr('bucket_sync_disable')
def test_bucket_sync_disable_enable():
    zonegroup = realm.master_zonegroup()
    zonegroup_conns = ZonegroupConns(zonegroup)
    buckets, zone_bucket = create_bucket_per_zone(zonegroup_conns)

    objnames = [ 'obj1', 'obj2', 'obj3', 'obj4' ]
    content = 'asdasd'

    for zone, bucket in zone_bucket:
        for objname in objnames:
            k = new_key(zone, bucket.name, objname)
            k.set_contents_from_string(content)

    zonegroup_meta_checkpoint(zonegroup)

    for bucket_name in buckets:
        zonegroup_bucket_checkpoint(zonegroup_conns, bucket_name)

    for bucket_name in buckets:
        disable_bucket_sync(realm.meta_master_zone(), bucket_name)

    zonegroup_meta_checkpoint(zonegroup)

    objnames_2 = [ 'obj5', 'obj6', 'obj7', 'obj8' ]

    for zone, bucket in zone_bucket:
        for objname in objnames_2:
            k = new_key(zone, bucket.name, objname)
            k.set_contents_from_string(content)

    for bucket_name in buckets:
        enable_bucket_sync(realm.meta_master_zone(), bucket_name)

    for bucket_name in buckets:
        zonegroup_bucket_checkpoint(zonegroup_conns, bucket_name)

    zonegroup_data_checkpoint(zonegroup_conns)

def test_multipart_object_sync():
    zonegroup = realm.master_zonegroup()
    zonegroup_conns = ZonegroupConns(zonegroup)
    buckets, zone_bucket = create_bucket_per_zone(zonegroup_conns)

    _, bucket = zone_bucket[0]

    # initiate a multipart upload
    upload = bucket.initiate_multipart_upload('MULTIPART')
    mp = boto.s3.multipart.MultiPartUpload(bucket)
    mp.key_name = upload.key_name
    mp.id = upload.id
    part_size = 5 * 1024 * 1024 # 5M min part size
    mp.upload_part_from_file(StringIO('a' * part_size), 1)
    mp.upload_part_from_file(StringIO('b' * part_size), 2)
    mp.upload_part_from_file(StringIO('c' * part_size), 3)
    mp.upload_part_from_file(StringIO('d' * part_size), 4)
    mp.complete_upload()

    zonegroup_meta_checkpoint(zonegroup)
    zonegroup_bucket_checkpoint(zonegroup_conns, bucket.name)

def test_encrypted_object_sync():
    zonegroup = realm.master_zonegroup()
    zonegroup_conns = ZonegroupConns(zonegroup)

    if len(zonegroup.rw_zones) < 2:
        raise SkipTest("test_zonegroup_remove skipped. Requires 2 or more zones in master zonegroup.")

    (zone1, zone2) = zonegroup_conns.rw_zones[0:2]

    # create a bucket on the first zone
    bucket_name = gen_bucket_name()
    log.info('create bucket zone=%s name=%s', zone1.name, bucket_name)
    bucket = zone1.conn.create_bucket(bucket_name)

    # upload an object with sse-c encryption
    sse_c_headers = {
        'x-amz-server-side-encryption-customer-algorithm': 'AES256',
        'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
        'x-amz-server-side-encryption-customer-key-md5': 'DWygnHRtgiJ77HCm+1rvHw=='
    }
    key = bucket.new_key('testobj-sse-c')
    data = 'A'*512
    key.set_contents_from_string(data, headers=sse_c_headers)

    # upload an object with sse-kms encryption
    sse_kms_headers = {
        'x-amz-server-side-encryption': 'aws:kms',
        # testkey-1 must be present in 'rgw crypt s3 kms encryption keys' (vstart.sh adds this)
        'x-amz-server-side-encryption-aws-kms-key-id': 'testkey-1',
    }
    key = bucket.new_key('testobj-sse-kms')
    key.set_contents_from_string(data, headers=sse_kms_headers)

    # wait for the bucket metadata and data to sync
    zonegroup_meta_checkpoint(zonegroup)
    zone_bucket_checkpoint(zone2.zone, zone1.zone, bucket_name)

    # read the encrypted objects from the second zone
    bucket2 = get_bucket(zone2, bucket_name)
    key = bucket2.get_key('testobj-sse-c', headers=sse_c_headers)
    eq(data, key.get_contents_as_string(headers=sse_c_headers, encoding='ascii'))

    key = bucket2.get_key('testobj-sse-kms')
    eq(data, key.get_contents_as_string(encoding='ascii'))

def test_bucket_index_log_trim():
    zonegroup = realm.master_zonegroup()
    zonegroup_conns = ZonegroupConns(zonegroup)

    zone = zonegroup_conns.rw_zones[0]

    # create a test bucket, upload some objects, and wait for sync
    def make_test_bucket():
        name = gen_bucket_name()
        log.info('create bucket zone=%s name=%s', zone.name, name)
        bucket = zone.conn.create_bucket(name)
        for objname in ('a', 'b', 'c', 'd'):
            k = new_key(zone, name, objname)
            k.set_contents_from_string('foo')
        zonegroup_meta_checkpoint(zonegroup)
        zonegroup_bucket_checkpoint(zonegroup_conns, name)
        return bucket

    # create a 'cold' bucket
    cold_bucket = make_test_bucket()

    # trim with max-buckets=0 to clear counters for cold bucket. this should
    # prevent it from being considered 'active' by the next autotrim
    bilog_autotrim(zone.zone, [
        '--rgw-sync-log-trim-max-buckets', '0',
    ])

    # create an 'active' bucket
    active_bucket = make_test_bucket()

    # trim with max-buckets=1 min-cold-buckets=0 to trim active bucket only
    bilog_autotrim(zone.zone, [
        '--rgw-sync-log-trim-max-buckets', '1',
        '--rgw-sync-log-trim-min-cold-buckets', '0',
    ])

    # verify active bucket has empty bilog
    active_bilog = bilog_list(zone.zone, active_bucket.name)
    assert(len(active_bilog) == 0)

    # verify cold bucket has nonempty bilog
    cold_bilog = bilog_list(zone.zone, cold_bucket.name)
    assert(len(cold_bilog) > 0)

    # trim with min-cold-buckets=999 to trim all buckets
    bilog_autotrim(zone.zone, [
        '--rgw-sync-log-trim-max-buckets', '999',
        '--rgw-sync-log-trim-min-cold-buckets', '999',
    ])

    # verify cold bucket has empty bilog
    cold_bilog = bilog_list(zone.zone, cold_bucket.name)
    assert(len(cold_bilog) == 0)

def test_bucket_reshard_index_log_trim():
    zonegroup = realm.master_zonegroup()
    zonegroup_conns = ZonegroupConns(zonegroup)

    zone = zonegroup_conns.rw_zones[0]

    # create a test bucket, upload some objects, and wait for sync
    def make_test_bucket():
        name = gen_bucket_name()
        log.info('create bucket zone=%s name=%s', zone.name, name)
        bucket = zone.conn.create_bucket(name)
        for objname in ('a', 'b', 'c', 'd'):
            k = new_key(zone, name, objname)
            k.set_contents_from_string('foo')
        zonegroup_meta_checkpoint(zonegroup)
        zonegroup_bucket_checkpoint(zonegroup_conns, name)
        return bucket

    # create a 'test' bucket
    test_bucket = make_test_bucket()

    # checking bucket layout before resharding
    json_obj_1 = bucket_layout(zone.zone, test_bucket.name)
    assert(len(json_obj_1['layout']['logs']) == 1)

    first_gen = json_obj_1['layout']['current_index']['gen']

    before_reshard_bilog = bilog_list(zone.zone, test_bucket.name, ['--gen', str(first_gen)])
    assert(len(before_reshard_bilog) == 4)

    # Resharding the bucket
    zone.zone.cluster.admin(['bucket', 'reshard',
        '--bucket', test_bucket.name,
        '--num-shards', '3',
        '--yes-i-really-mean-it'])

    # checking bucket layout after 1st resharding
    json_obj_2 = bucket_layout(zone.zone, test_bucket.name)
    assert(len(json_obj_2['layout']['logs']) == 2)

    second_gen = json_obj_2['layout']['current_index']['gen']

    after_reshard_bilog = bilog_list(zone.zone, test_bucket.name, ['--gen', str(second_gen)])
    assert(len(after_reshard_bilog) == 0)

    # upload more objects
    for objname in ('e', 'f', 'g', 'h'):
        k = new_key(zone, test_bucket.name, objname)
        k.set_contents_from_string('foo')
    zonegroup_bucket_checkpoint(zonegroup_conns, test_bucket.name)

    # Resharding the bucket again
    zone.zone.cluster.admin(['bucket', 'reshard',
        '--bucket', test_bucket.name,
        '--num-shards', '3',
        '--yes-i-really-mean-it'])

    # checking bucket layout after 2nd resharding
    json_obj_3 = bucket_layout(zone.zone, test_bucket.name)
    assert(len(json_obj_3['layout']['logs']) == 3)

    zonegroup_bucket_checkpoint(zonegroup_conns, test_bucket.name)

    bilog_autotrim(zone.zone)

    # checking bucket layout after 1st bilog autotrim
    json_obj_4 = bucket_layout(zone.zone, test_bucket.name)
    assert(len(json_obj_4['layout']['logs']) == 2)

    bilog_autotrim(zone.zone)

    # checking bucket layout after 2nd bilog autotrim
    json_obj_5 = bucket_layout(zone.zone, test_bucket.name)
    assert(len(json_obj_5['layout']['logs']) == 1)

    bilog_autotrim(zone.zone)

    # upload more objects
    for objname in ('i', 'j', 'k', 'l'):
        k = new_key(zone, test_bucket.name, objname)
        k.set_contents_from_string('foo')
    zonegroup_bucket_checkpoint(zonegroup_conns, test_bucket.name)

    # verify the bucket has non-empty bilog
    test_bilog = bilog_list(zone.zone, test_bucket.name)
    assert(len(test_bilog) > 0)

@attr('bucket_reshard')
def test_bucket_reshard_incremental():
    zonegroup = realm.master_zonegroup()
    zonegroup_conns = ZonegroupConns(zonegroup)
    zone = zonegroup_conns.rw_zones[0]

    # create a bucket
    bucket = zone.create_bucket(gen_bucket_name())
    log.debug('created bucket=%s', bucket.name)
    zonegroup_meta_checkpoint(zonegroup)

    # upload some objects
    for objname in ('a', 'b', 'c', 'd'):
        k = new_key(zone, bucket.name, objname)
        k.set_contents_from_string('foo')
    zonegroup_bucket_checkpoint(zonegroup_conns, bucket.name)

    # reshard in each zone
    for z in zonegroup_conns.rw_zones:
        z.zone.cluster.admin(['bucket', 'reshard',
            '--bucket', bucket.name,
            '--num-shards', '3',
            '--yes-i-really-mean-it'])

    # upload more objects
    for objname in ('e', 'f', 'g', 'h'):
        k = new_key(zone, bucket.name, objname)
        k.set_contents_from_string('foo')
    zonegroup_bucket_checkpoint(zonegroup_conns, bucket.name)

@attr('bucket_reshard')
def test_bucket_reshard_full():
    zonegroup = realm.master_zonegroup()
    zonegroup_conns = ZonegroupConns(zonegroup)
    zone = zonegroup_conns.rw_zones[0]

    # create a bucket
    bucket = zone.create_bucket(gen_bucket_name())
    log.debug('created bucket=%s', bucket.name)
    zonegroup_meta_checkpoint(zonegroup)

    # stop gateways in other zones so we can force the bucket to full sync
    for z in zonegroup_conns.rw_zones[1:]:
        z.zone.stop()

    # use try-finally to restart gateways even if something fails
    try:
        # upload some objects
        for objname in ('a', 'b', 'c', 'd'):
            k = new_key(zone, bucket.name, objname)
            k.set_contents_from_string('foo')

        # reshard on first zone
        zone.zone.cluster.admin(['bucket', 'reshard',
            '--bucket', bucket.name,
            '--num-shards', '3',
            '--yes-i-really-mean-it'])

        # upload more objects
        for objname in ('e', 'f', 'g', 'h'):
            k = new_key(zone, bucket.name, objname)
            k.set_contents_from_string('foo')
    finally:
        for z in zonegroup_conns.rw_zones[1:]:
            z.zone.start()

    zonegroup_bucket_checkpoint(zonegroup_conns, bucket.name)

def test_bucket_creation_time():
    zonegroup = realm.master_zonegroup()
    zonegroup_conns = ZonegroupConns(zonegroup)

    zonegroup_meta_checkpoint(zonegroup)

    zone_buckets = [zone.get_connection().get_all_buckets() for zone in zonegroup_conns.rw_zones]
    for z1, z2 in combinations(zone_buckets, 2):
        for a, b in zip(z1, z2):
            eq(a.name, b.name)
            eq(a.creation_date, b.creation_date)

def get_bucket_shard_objects(zone, num_shards):
    """
    Get one object for each shard of the bucket index log
    """
    cmd = ['bucket', 'shard', 'objects'] + zone.zone_args()
    cmd += ['--num-shards', str(num_shards)]
    shardobjs_json, ret = zone.cluster.admin(cmd, read_only=True)
    assert ret == 0
    shardobjs = json.loads(shardobjs_json)
    return shardobjs['objs']

def write_most_shards(zone, bucket_name, num_shards):
    """
    Write one object to most (but not all) bucket index shards.
    """
    objs = get_bucket_shard_objects(zone.zone, num_shards)
    random.shuffle(objs)
    del objs[-(len(objs)//10):]
    for obj in objs:
        k = new_key(zone, bucket_name, obj)
        k.set_contents_from_string('foo')

def reshard_bucket(zone, bucket_name, num_shards):
    """
    Reshard a bucket
    """
    cmd = ['bucket', 'reshard'] + zone.zone_args()
    cmd += ['--bucket', bucket_name]
    cmd += ['--num-shards', str(num_shards)]
    cmd += ['--yes-i-really-mean-it']
    zone.cluster.admin(cmd)

def get_obj_names(zone, bucket_name, maxobjs):
    """
    Get names of objects in a bucket.
    """
    cmd = ['bucket', 'list'] + zone.zone_args()
    cmd += ['--bucket', bucket_name]
    cmd += ['--max-entries', str(maxobjs)]
    objs_json, _ = zone.cluster.admin(cmd, read_only=True)
    objs = json.loads(objs_json)
    return [o['name'] for o in objs]

def bucket_keys_eq(zone1, zone2, bucket_name):
    """
    Ensure that two buckets have the same keys, but get the lists through
    radosgw-admin rather than S3 so it can be used when radosgw isn't running.
    Only works for buckets of 10,000 objects since the tests calling it don't
    need more, and the output from bucket list doesn't have an obvious marker
    with which to continue.
    """
    keys1 = get_obj_names(zone1, bucket_name, 10000)
    keys2 = get_obj_names(zone2, bucket_name, 10000)
    for key1, key2 in zip_longest(keys1, keys2):
        if key1 is None:
            log.critical('key=%s is missing from zone=%s', key1.name,
                         zone1.name)
            assert False
        if key2 is None:
            log.critical('key=%s is missing from zone=%s', key2.name,
                         zone2.name)
            assert False

@attr('bucket_reshard')
def test_bucket_sync_run_basic_incremental():
    """
    Create several generations of objects, then run bucket sync
    run to ensure they're all processed.
    """
    zonegroup = realm.master_zonegroup()
    zonegroup_conns = ZonegroupConns(zonegroup)
    primary = zonegroup_conns.rw_zones[0]

    # create a bucket write objects to it and wait for them to sync, ensuring
    # we are in incremental.
    bucket = primary.create_bucket(gen_bucket_name())
    log.debug('created bucket=%s', bucket.name)
    zonegroup_meta_checkpoint(zonegroup)
    write_most_shards(primary, bucket.name, 11)
    zonegroup_bucket_checkpoint(zonegroup_conns, bucket.name)

    try:
        # stop gateways in other zones so we can rely on bucket sync run
        for secondary in zonegroup_conns.rw_zones[1:]:
            secondary.zone.stop()

        # build up multiple generations each with some objects written to
        # them.
        generations = [17, 19, 23, 29, 31, 37]
        for num_shards in generations:
            reshard_bucket(primary.zone, bucket.name, num_shards)
            write_most_shards(primary, bucket.name, num_shards)

        # bucket sync run on every secondary
        for secondary in zonegroup_conns.rw_zones[1:]:
            cmd = ['bucket', 'sync', 'run'] + secondary.zone.zone_args()
            cmd += ['--bucket', bucket.name, '--source-zone', primary.name]
            secondary.zone.cluster.admin(cmd)

            bucket_keys_eq(primary.zone, secondary.zone, bucket.name)

    finally:
        # Restart so bucket_checkpoint can actually fetch things from the
        # secondaries. Put this in a finally block so they restart even on
        # error.
        for secondary in zonegroup_conns.rw_zones[1:]:
            secondary.zone.start()

    zonegroup_bucket_checkpoint(zonegroup_conns, bucket.name)

def trash_bucket(zone, bucket_name):
    """
    Remove objects through radosgw-admin, zapping bilog to prevent the deletes
    from replicating.
    """
    objs = get_obj_names(zone, bucket_name, 10000)
    # Delete the objects
    for obj in objs:
        cmd = ['object', 'rm'] + zone.zone_args()
        cmd += ['--bucket', bucket_name]
        cmd += ['--object', obj]
        zone.cluster.admin(cmd)

    # Zap the bilog
    cmd = ['bilog', 'trim'] + zone.zone_args()
    cmd += ['--bucket', bucket_name]
    zone.cluster.admin(cmd)

@attr('bucket_reshard')
def test_zap_init_bucket_sync_run():
    """
    Create several generations of objects, trash them, then run bucket sync init
    and bucket sync run.
    """
    zonegroup = realm.master_zonegroup()
    zonegroup_conns = ZonegroupConns(zonegroup)
    primary = zonegroup_conns.rw_zones[0]

    bucket = primary.create_bucket(gen_bucket_name())
    log.debug('created bucket=%s', bucket.name)
    zonegroup_meta_checkpoint(zonegroup)

    # Write zeroth generation
    for obj in range(1, 6):
        k = new_key(primary, bucket.name, f'obj{obj * 11}')
        k.set_contents_from_string('foo')
    zonegroup_bucket_checkpoint(zonegroup_conns, bucket.name)

    # Write several more generations
    generations = [17, 19, 23, 29, 31, 37]
    for num_shards in generations:
        reshard_bucket(primary.zone, bucket.name, num_shards)
        for obj in range(1, 6):
            k = new_key(primary, bucket.name, f'obj{obj * num_shards}')
            k.set_contents_from_string('foo')
        zonegroup_bucket_checkpoint(zonegroup_conns, bucket.name)


    # Stop gateways, trash bucket, init, sync, and restart for every secondary
    for secondary in zonegroup_conns.rw_zones[1:]:
        try:
            secondary.zone.stop()

            trash_bucket(secondary.zone, bucket.name)

            cmd = ['bucket', 'sync', 'init'] + secondary.zone.zone_args()
            cmd += ['--bucket', bucket.name]
            cmd += ['--source-zone', primary.name]
            secondary.zone.cluster.admin(cmd)

            cmd = ['bucket', 'sync', 'run'] + secondary.zone.zone_args()
            cmd += ['--bucket', bucket.name, '--source-zone', primary.name]
            secondary.zone.cluster.admin(cmd)

            bucket_keys_eq(primary.zone, secondary.zone, bucket.name)

        finally:
            # Do this as a finally so we bring the zone back up even on error.
            secondary.zone.start()

    zonegroup_bucket_checkpoint(zonegroup_conns, bucket.name)

def test_role_sync():
    zonegroup = realm.master_zonegroup()
    zonegroup_conns = ZonegroupConns(zonegroup)
    roles, zone_role = create_role_per_zone(zonegroup_conns)

    zonegroup_meta_checkpoint(zonegroup)

    for source_conn, role in zone_role:
        for target_conn in zonegroup_conns.zones:
            if source_conn.zone == target_conn.zone:
                continue

            check_role_eq(source_conn, target_conn, role)

def test_role_delete_sync():
    zonegroup = realm.master_zonegroup()
    zonegroup_conns = ZonegroupConns(zonegroup)
    role_name = gen_role_name()
    log.info('create role zone=%s name=%s', zonegroup_conns.master_zone.name, role_name)
    zonegroup_conns.master_zone.create_role("", role_name, None, "")

    zonegroup_meta_checkpoint(zonegroup)

    for zone in zonegroup_conns.zones:
        log.info(f'checking if zone: {zone.name} has role: {role_name}')
        assert(zone.has_role(role_name))
        log.info(f'success, zone: {zone.name} has role: {role_name}')

    log.info(f"deleting role: {role_name}")
    zonegroup_conns.master_zone.delete_role(role_name)
    zonegroup_meta_checkpoint(zonegroup)

    for zone in zonegroup_conns.zones:
        log.info(f'checking if zone: {zone.name} does not have role: {role_name}')
        assert(not zone.has_role(role_name))
        log.info(f'success, zone: {zone.name} does not have role: {role_name}')


@attr('data_sync_init')
def test_bucket_full_sync_after_data_sync_init():
    zonegroup = realm.master_zonegroup()
    zonegroup_conns = ZonegroupConns(zonegroup)
    primary = zonegroup_conns.rw_zones[0]
    secondary = zonegroup_conns.rw_zones[1]

    bucket = primary.create_bucket(gen_bucket_name())
    log.debug('created bucket=%s', bucket.name)
    zonegroup_meta_checkpoint(zonegroup)

    try:
        # stop secondary zone before it starts a bucket full sync
        secondary.zone.stop()

        # write some objects that don't sync yet
        for obj in range(1, 6):
            k = new_key(primary, bucket.name, f'obj{obj * 11}')
            k.set_contents_from_string('foo')

        cmd = ['data', 'sync', 'init'] + secondary.zone.zone_args()
        cmd += ['--source-zone', primary.name]
        secondary.zone.cluster.admin(cmd)
    finally:
        # Do this as a finally so we bring the zone back up even on error.
        secondary.zone.start()

    # expect all objects to replicate via 'bucket full sync'
    zonegroup_bucket_checkpoint(zonegroup_conns, bucket.name)
    zonegroup_data_checkpoint(zonegroup_conns)

@attr('data_sync_init')
@attr('bucket_reshard')
def test_resharded_bucket_full_sync_after_data_sync_init():
    zonegroup = realm.master_zonegroup()
    zonegroup_conns = ZonegroupConns(zonegroup)
    primary = zonegroup_conns.rw_zones[0]
    secondary = zonegroup_conns.rw_zones[1]

    bucket = primary.create_bucket(gen_bucket_name())
    log.debug('created bucket=%s', bucket.name)
    zonegroup_meta_checkpoint(zonegroup)

    try:
        # stop secondary zone before it starts a bucket full sync
        secondary.zone.stop()

        # Write zeroth generation
        for obj in range(1, 6):
            k = new_key(primary, bucket.name, f'obj{obj * 11}')
            k.set_contents_from_string('foo')

        # Write several more generations
        generations = [17, 19, 23, 29, 31, 37]
        for num_shards in generations:
            reshard_bucket(primary.zone, bucket.name, num_shards)
            for obj in range(1, 6):
                k = new_key(primary, bucket.name, f'obj{obj * num_shards}')
                k.set_contents_from_string('foo')

        cmd = ['data', 'sync', 'init'] + secondary.zone.zone_args()
        cmd += ['--source-zone', primary.name]
        secondary.zone.cluster.admin(cmd)
    finally:
        # Do this as a finally so we bring the zone back up even on error.
        secondary.zone.start()

    # expect all objects to replicate via 'bucket full sync'
    zonegroup_bucket_checkpoint(zonegroup_conns, bucket.name)
    zonegroup_data_checkpoint(zonegroup_conns)

@attr('data_sync_init')
def test_bucket_incremental_sync_after_data_sync_init():
    zonegroup = realm.master_zonegroup()
    zonegroup_conns = ZonegroupConns(zonegroup)
    primary = zonegroup_conns.rw_zones[0]
    secondary = zonegroup_conns.rw_zones[1]

    bucket = primary.create_bucket(gen_bucket_name())
    log.debug('created bucket=%s', bucket.name)
    zonegroup_meta_checkpoint(zonegroup)

    # upload a dummy object and wait for sync. this forces each zone to finish
    # a full sync and switch to incremental
    k = new_key(primary, bucket, 'dummy')
    k.set_contents_from_string('foo')
    zonegroup_bucket_checkpoint(zonegroup_conns, bucket.name)

    try:
        # stop secondary zone before it syncs the rest
        secondary.zone.stop()

        # Write more objects to primary
        for obj in range(1, 6):
            k = new_key(primary, bucket.name, f'obj{obj * 11}')
            k.set_contents_from_string('foo')

        cmd = ['data', 'sync', 'init'] + secondary.zone.zone_args()
        cmd += ['--source-zone', primary.name]
        secondary.zone.cluster.admin(cmd)
    finally:
        # Do this as a finally so we bring the zone back up even on error.
        secondary.zone.start()

    # expect remaining objects to replicate via 'bucket incremental sync'
    zonegroup_bucket_checkpoint(zonegroup_conns, bucket.name)
    zonegroup_data_checkpoint(zonegroup_conns)

@attr('data_sync_init')
@attr('bucket_reshard')
def test_resharded_bucket_incremental_sync_latest_after_data_sync_init():
    zonegroup = realm.master_zonegroup()
    zonegroup_conns = ZonegroupConns(zonegroup)
    primary = zonegroup_conns.rw_zones[0]
    secondary = zonegroup_conns.rw_zones[1]

    bucket = primary.create_bucket(gen_bucket_name())
    log.debug('created bucket=%s', bucket.name)
    zonegroup_meta_checkpoint(zonegroup)

    # Write zeroth generation to primary
    for obj in range(1, 6):
        k = new_key(primary, bucket.name, f'obj{obj * 11}')
        k.set_contents_from_string('foo')

    # Write several more generations
    generations = [17, 19, 23, 29, 31, 37]
    for num_shards in generations:
        reshard_bucket(primary.zone, bucket.name, num_shards)
        for obj in range(1, 6):
            k = new_key(primary, bucket.name, f'obj{obj * num_shards}')
            k.set_contents_from_string('foo')

    # wait for the secondary to catch up to the latest gen
    zonegroup_bucket_checkpoint(zonegroup_conns, bucket.name)

    try:
        # stop secondary zone before it syncs the rest
        secondary.zone.stop()

        # write some more objects to the last gen
        for obj in range(1, 6):
            k = new_key(primary, bucket.name, f'obj{obj * generations[-1]}')
            k.set_contents_from_string('foo')

        cmd = ['data', 'sync', 'init'] + secondary.zone.zone_args()
        cmd += ['--source-zone', primary.name]
        secondary.zone.cluster.admin(cmd)
    finally:
        # Do this as a finally so we bring the zone back up even on error.
        secondary.zone.start()

    # expect remaining objects in last gen to replicate via 'bucket incremental sync'
    zonegroup_bucket_checkpoint(zonegroup_conns, bucket.name)
    zonegroup_data_checkpoint(zonegroup_conns)

@attr('data_sync_init')
@attr('bucket_reshard')
def test_resharded_bucket_incremental_sync_oldest_after_data_sync_init():
    zonegroup = realm.master_zonegroup()
    zonegroup_conns = ZonegroupConns(zonegroup)
    primary = zonegroup_conns.rw_zones[0]
    secondary = zonegroup_conns.rw_zones[1]

    bucket = primary.create_bucket(gen_bucket_name())
    log.debug('created bucket=%s', bucket.name)
    zonegroup_meta_checkpoint(zonegroup)

    # Write zeroth generation to primary
    for obj in range(1, 6):
        k = new_key(primary, bucket.name, f'obj{obj * 11}')
        k.set_contents_from_string('foo')

    # wait for the secondary to catch up
    zonegroup_bucket_checkpoint(zonegroup_conns, bucket.name)

    try:
        # stop secondary zone before it syncs later generations
        secondary.zone.stop()

        # Write several more generations
        generations = [17, 19, 23, 29, 31, 37]
        for num_shards in generations:
            reshard_bucket(primary.zone, bucket.name, num_shards)
            for obj in range(1, 6):
                k = new_key(primary, bucket.name, f'obj{obj * num_shards}')
                k.set_contents_from_string('foo')

        cmd = ['data', 'sync', 'init'] + secondary.zone.zone_args()
        cmd += ['--source-zone', primary.name]
        secondary.zone.cluster.admin(cmd)
    finally:
        # Do this as a finally so we bring the zone back up even on error.
        secondary.zone.start()

    # expect all generations to replicate via 'bucket incremental sync'
    zonegroup_bucket_checkpoint(zonegroup_conns, bucket.name)
    zonegroup_data_checkpoint(zonegroup_conns)

def sync_info(cluster, bucket = None):
    cmd = ['sync', 'info']
    if bucket:
        cmd += ['--bucket', bucket]
    (result_json, retcode) = cluster.admin(cmd)
    if retcode != 0:
        assert False, 'failed to get sync policy'

    return json.loads(result_json)

def get_sync_policy(cluster, bucket = None):
    cmd = ['sync', 'policy', 'get']
    if bucket:
        cmd += ['--bucket', bucket]
    (result_json, retcode) = cluster.admin(cmd)
    if retcode != 0:
        assert False, 'failed to get sync policy'

    return json.loads(result_json)

def create_sync_policy_group(cluster, group, status = "allowed", bucket = None):
    cmd = ['sync', 'group', 'create', '--group-id', group, '--status' , status]
    if bucket:
        cmd += ['--bucket', bucket]
    (result_json, retcode) = cluster.admin(cmd)
    if retcode != 0:
        assert False, 'failed to create sync policy group id=%s, bucket=%s' % (group, bucket)
    return json.loads(result_json)

def set_sync_policy_group_status(cluster, group, status, bucket = None):
    cmd = ['sync', 'group', 'modify', '--group-id', group, '--status' , status]
    if bucket:
        cmd += ['--bucket', bucket]
    (result_json, retcode) = cluster.admin(cmd)
    if retcode != 0:
        assert False, 'failed to set sync policy group id=%s, bucket=%s' % (group, bucket)
    return json.loads(result_json)

def get_sync_policy_group(cluster, group, bucket = None):
    cmd = ['sync', 'group', 'get', '--group-id', group]
    if bucket:
        cmd += ['--bucket', bucket]
    (result_json, retcode) = cluster.admin(cmd)
    if retcode != 0:
        assert False, 'failed to get sync policy group id=%s, bucket=%s' % (group, bucket)
    return json.loads(result_json)

def remove_sync_policy_group(cluster, group, bucket = None):
    cmd = ['sync', 'group', 'remove', '--group-id', group]
    if bucket:
        cmd += ['--bucket', bucket]
    (result_json, retcode) = cluster.admin(cmd)
    if retcode != 0:
        assert False, 'failed to remove sync policy group id=%s, bucket=%s' % (group, bucket)
    return json.loads(result_json)

def create_sync_group_flow_symmetrical(cluster, group, flow_id, zones, bucket = None):
    cmd = ['sync', 'group', 'flow', 'create', '--group-id', group, '--flow-id' , flow_id, '--flow-type', 'symmetrical', '--zones=%s' % zones]
    if bucket:
        cmd += ['--bucket', bucket]
    (result_json, retcode) = cluster.admin(cmd)
    if retcode != 0:
        assert False, 'failed to create sync group flow symmetrical groupid=%s, flow_id=%s, zones=%s, bucket=%s' % (group, flow_id, zones, bucket)
    return json.loads(result_json)

def create_sync_group_flow_directional(cluster, group, flow_id, src_zones, dest_zones, bucket = None):
    cmd = ['sync', 'group', 'flow', 'create', '--group-id', group, '--flow-id' , flow_id, '--flow-type', 'directional', '--source-zone=%s' % src_zones, '--dest-zone=%s' % dest_zones]
    if bucket:
        cmd += ['--bucket', bucket]
    (result_json, retcode) = cluster.admin(cmd)
    if retcode != 0:
        assert False, 'failed to create sync group flow directional groupid=%s, flow_id=%s, src_zones=%s, dest_zones=%s, bucket=%s' % (group, flow_id, src_zones, dest_zones, bucket)
    return json.loads(result_json)

def remove_sync_group_flow_symmetrical(cluster, group, flow_id, zones = None, bucket = None):
    cmd = ['sync', 'group', 'flow', 'remove', '--group-id', group, '--flow-id' , flow_id, '--flow-type', 'symmetrical']
    if zones:
        cmd += ['--zones=%s' % zones]
    if bucket:
        cmd += ['--bucket', bucket]
    (result_json, retcode) = cluster.admin(cmd)
    if retcode != 0:
        assert False, 'failed to remove sync group flow symmetrical groupid=%s, flow_id=%s, zones=%s, bucket=%s' % (group, flow_id, zones, bucket)
    return json.loads(result_json)

def remove_sync_group_flow_directional(cluster, group, flow_id, src_zones, dest_zones, bucket = None):
    cmd = ['sync', 'group', 'flow', 'remove', '--group-id', group, '--flow-id' , flow_id, '--flow-type', 'directional', '--source-zone=%s' % src_zones, '--dest-zone=%s' % dest_zones]
    if bucket:
        cmd += ['--bucket', bucket]
    (result_json, retcode) = cluster.admin(cmd)
    if retcode != 0:
        assert False, 'failed to remove sync group flow directional groupid=%s, flow_id=%s, src_zones=%s, dest_zones=%s, bucket=%s' % (group, flow_id, src_zones, dest_zones, bucket)
    return json.loads(result_json)

def create_sync_group_pipe(cluster, group, pipe_id, src_zones, dest_zones, bucket = None, args = []):
    cmd = ['sync', 'group', 'pipe', 'create', '--group-id', group, '--pipe-id' , pipe_id, '--source-zones=%s' % src_zones, '--dest-zones=%s' % dest_zones]
    if bucket:
        b_args = '--bucket=' + bucket
        cmd.append(b_args)
    if args:
        cmd += args
    (result_json, retcode) = cluster.admin(cmd)
    if retcode != 0:
        assert False, 'failed to create sync group pipe groupid=%s, pipe_id=%s, src_zones=%s, dest_zones=%s, bucket=%s' % (group, pipe_id, src_zones, dest_zones, bucket)
    return json.loads(result_json)

def remove_sync_group_pipe(cluster, group, pipe_id, bucket = None, args = None):
    cmd = ['sync', 'group', 'pipe', 'remove', '--group-id', group, '--pipe-id' , pipe_id]
    if bucket:
        b_args = '--bucket=' + bucket
        cmd.append(b_args)
    if args:
        cmd.append(args)
    (result_json, retcode) = cluster.admin(cmd)
    if retcode != 0:
        assert False, 'failed to remove sync group pipe groupid=%s, pipe_id=%s, src_zones=%s, dest_zones=%s, bucket=%s' % (group, pipe_id, src_zones, dest_zones, bucket)
    return json.loads(result_json)

def create_zone_bucket(zone):
    b_name = gen_bucket_name()
    log.info('create bucket zone=%s name=%s', zone.name, b_name)
    bucket = zone.create_bucket(b_name)
    return bucket

def create_object(zone_conn, bucket, objname, content):
    k = new_key(zone_conn, bucket.name, objname)
    k.set_contents_from_string(content)

def create_objects(zone_conn, bucket, obj_arr, content):
    for objname in obj_arr:
        create_object(zone_conn, bucket, objname, content)

def check_object_exists(bucket, objname, content = None):
    k = bucket.get_key(objname)
    assert_not_equal(k, None)
    if (content != None):
        assert_equal(k.get_contents_as_string(encoding='ascii'), content)

def check_objects_exist(bucket, obj_arr, content = None):
    for objname in obj_arr:
        check_object_exists(bucket, objname, content)

def check_object_not_exists(bucket, objname):
    k = bucket.get_key(objname)
    assert_equal(k, None)

def check_objects_not_exist(bucket, obj_arr):
    for objname in obj_arr:
        check_object_not_exists(bucket, objname)

@attr('sync_policy')
def test_sync_policy_config_zonegroup():
    """
    test_sync_policy_config_zonegroup:
        test configuration of all sync commands
    """
    zonegroup = realm.master_zonegroup()
    zonegroup_meta_checkpoint(zonegroup)

    zonegroup_conns = ZonegroupConns(zonegroup)
    z1, z2 = zonegroup.zones[0:2]
    c1, c2 = (z1.cluster, z2.cluster)

    zones = z1.name+","+z2.name

    c1.admin(['sync', 'policy', 'get'])

    # (a) zonegroup level
    create_sync_policy_group(c1, "sync-group")
    set_sync_policy_group_status(c1, "sync-group", "enabled")
    get_sync_policy_group(c1, "sync-group")

    get_sync_policy(c1)

    create_sync_group_flow_symmetrical(c1, "sync-group", "sync-flow1", zones)
    create_sync_group_flow_directional(c1, "sync-group", "sync-flow2", z1.name, z2.name)

    create_sync_group_pipe(c1, "sync-group", "sync-pipe", zones, zones)
    get_sync_policy_group(c1, "sync-group")

    zonegroup.period.update(z1, commit=True)

    # (b)  bucket level
    zc1, zc2 = zonegroup_conns.zones[0:2]
    bucket = create_zone_bucket(zc1)
    bucket_name = bucket.name

    create_sync_policy_group(c1, "sync-bucket", "allowed", bucket_name)
    set_sync_policy_group_status(c1, "sync-bucket", "enabled", bucket_name)
    get_sync_policy_group(c1, "sync-bucket", bucket_name)

    get_sync_policy(c1, bucket_name)

    create_sync_group_flow_symmetrical(c1, "sync-bucket", "sync-flow1", zones, bucket_name)
    create_sync_group_flow_directional(c1, "sync-bucket", "sync-flow2", z1.name, z2.name, bucket_name)

    create_sync_group_pipe(c1, "sync-bucket", "sync-pipe", zones, zones, bucket_name)
    get_sync_policy_group(c1, "sync-bucket", bucket_name)

    zonegroup_meta_checkpoint(zonegroup)

    remove_sync_group_pipe(c1, "sync-bucket", "sync-pipe", bucket_name)
    remove_sync_group_flow_directional(c1, "sync-bucket", "sync-flow2", z1.name, z2.name, bucket_name)
    remove_sync_group_flow_symmetrical(c1, "sync-bucket", "sync-flow1", zones, bucket_name)
    remove_sync_policy_group(c1, "sync-bucket", bucket_name)

    get_sync_policy(c1, bucket_name)

    zonegroup_meta_checkpoint(zonegroup)

    remove_sync_group_pipe(c1, "sync-group", "sync-pipe")
    remove_sync_group_flow_directional(c1, "sync-group", "sync-flow2", z1.name, z2.name)
    remove_sync_group_flow_symmetrical(c1, "sync-group", "sync-flow1")
    remove_sync_policy_group(c1, "sync-group")

    get_sync_policy(c1)

    zonegroup.period.update(z1, commit=True)

    return

@attr('sync_policy')
def test_sync_flow_symmetrical_zonegroup_all():
    """
    test_sync_flow_symmetrical_zonegroup_all:
        allows sync from all the zones to all other zones (default case)
    """

    zonegroup = realm.master_zonegroup()
    zonegroup_meta_checkpoint(zonegroup)

    zonegroup_conns = ZonegroupConns(zonegroup)

    (zoneA, zoneB) = zonegroup.zones[0:2]
    (zcA, zcB) = zonegroup_conns.zones[0:2]

    c1 = zoneA.cluster

    c1.admin(['sync', 'policy', 'get'])

    zones = zoneA.name + ',' + zoneB.name
    create_sync_policy_group(c1, "sync-group")
    create_sync_group_flow_symmetrical(c1, "sync-group", "sync-flow1", zones)
    create_sync_group_pipe(c1, "sync-group", "sync-pipe", zones, zones)
    set_sync_policy_group_status(c1, "sync-group", "enabled")

    zonegroup.period.update(zoneA, commit=True)
    get_sync_policy(c1)

    objnames = [ 'obj1', 'obj2' ]
    content = 'asdasd'
    buckets = []

    # create bucket & object in all zones
    bucketA = create_zone_bucket(zcA)
    buckets.append(bucketA)
    create_object(zcA, bucketA, objnames[0], content)

    bucketB = create_zone_bucket(zcB)
    buckets.append(bucketB)
    create_object(zcB, bucketB, objnames[1], content)

    zonegroup_meta_checkpoint(zonegroup)
    # 'zonegroup_data_checkpoint' currently fails for the zones not
    # allowed to sync. So as a workaround, data checkpoint is done
    # for only the ones configured.
    zone_data_checkpoint(zoneB, zoneA)

    # verify if objects are synced accross the zone
    bucket = get_bucket(zcB, bucketA.name)
    check_object_exists(bucket, objnames[0], content)

    bucket = get_bucket(zcA, bucketB.name)
    check_object_exists(bucket, objnames[1], content)

    remove_sync_policy_group(c1, "sync-group")
    return

@attr('sync_policy')
def test_sync_flow_symmetrical_zonegroup_select():
    """
    test_sync_flow_symmetrical_zonegroup_select:
        allow sync between zoneA & zoneB
        verify zoneC doesnt sync the data
    """

    zonegroup = realm.master_zonegroup()
    zonegroup_conns = ZonegroupConns(zonegroup)

    if len(zonegroup.zones) < 3:
        raise SkipTest("test_sync_flow_symmetrical_zonegroup_select skipped. Requires 3 or more zones in master zonegroup.")

    zonegroup_meta_checkpoint(zonegroup)

    (zoneA, zoneB, zoneC) = zonegroup.zones[0:3]
    (zcA, zcB, zcC) = zonegroup_conns.zones[0:3]

    c1 = zoneA.cluster

    # configure sync policy
    zones = zoneA.name + ',' + zoneB.name
    c1.admin(['sync', 'policy', 'get'])
    create_sync_policy_group(c1, "sync-group")
    create_sync_group_flow_symmetrical(c1, "sync-group", "sync-flow", zones)
    create_sync_group_pipe(c1, "sync-group", "sync-pipe", zones, zones)
    set_sync_policy_group_status(c1, "sync-group", "enabled")

    zonegroup.period.update(zoneA, commit=True)
    get_sync_policy(c1)

    buckets = []
    content = 'asdasd'

    # create bucketA & objects in zoneA
    objnamesA = [ 'obj1', 'obj2', 'obj3' ]
    bucketA = create_zone_bucket(zcA)
    buckets.append(bucketA)
    create_objects(zcA, bucketA, objnamesA, content)

    # create bucketB & objects in zoneB
    objnamesB = [ 'obj4', 'obj5', 'obj6' ]
    bucketB = create_zone_bucket(zcB)
    buckets.append(bucketB)
    create_objects(zcB, bucketB, objnamesB, content)

    zonegroup_meta_checkpoint(zonegroup)
    zone_data_checkpoint(zoneB, zoneA)
    zone_data_checkpoint(zoneA, zoneB)

    # verify if objnamesA synced to only zoneB but not zoneC
    bucket = get_bucket(zcB, bucketA.name)
    check_objects_exist(bucket, objnamesA, content)

    bucket = get_bucket(zcC, bucketA.name)
    check_objects_not_exist(bucket, objnamesA)

    # verify if objnamesB synced to only zoneA but not zoneC
    bucket = get_bucket(zcA, bucketB.name)
    check_objects_exist(bucket, objnamesB, content)

    bucket = get_bucket(zcC, bucketB.name)
    check_objects_not_exist(bucket, objnamesB)

    remove_sync_policy_group(c1, "sync-group")
    return

@attr('sync_policy')
def test_sync_flow_directional_zonegroup_select():
    """
    test_sync_flow_directional_zonegroup_select:
        allow sync from only zoneA to zoneB
        
        verify that data doesn't get synced to zoneC and
        zoneA shouldn't sync data from zoneB either
    """

    zonegroup = realm.master_zonegroup()
    zonegroup_conns = ZonegroupConns(zonegroup)

    if len(zonegroup.zones) < 3:
        raise SkipTest("test_sync_flow_symmetrical_zonegroup_select skipped. Requires 3 or more zones in master zonegroup.")

    zonegroup_meta_checkpoint(zonegroup)

    (zoneA, zoneB, zoneC) = zonegroup.zones[0:3]
    (zcA, zcB, zcC) = zonegroup_conns.zones[0:3]

    c1 = zoneA.cluster

    # configure sync policy
    zones = zoneA.name + ',' + zoneB.name
    c1.admin(['sync', 'policy', 'get'])
    create_sync_policy_group(c1, "sync-group")
    create_sync_group_flow_directional(c1, "sync-group", "sync-flow", zoneA.name, zoneB.name)
    create_sync_group_pipe(c1, "sync-group", "sync-pipe", zoneA.name, zoneB.name)
    set_sync_policy_group_status(c1, "sync-group", "enabled")

    zonegroup.period.update(zoneA, commit=True)
    get_sync_policy(c1)

    buckets = []
    content = 'asdasd'

    # create bucketA & objects in zoneA
    objnamesA = [ 'obj1', 'obj2', 'obj3' ]
    bucketA = create_zone_bucket(zcA)
    buckets.append(bucketA)
    create_objects(zcA, bucketA, objnamesA, content)

    # create bucketB & objects in zoneB
    objnamesB = [ 'obj4', 'obj5', 'obj6' ]
    bucketB = create_zone_bucket(zcB)
    buckets.append(bucketB)
    create_objects(zcB, bucketB, objnamesB, content)

    zonegroup_meta_checkpoint(zonegroup)
    zone_data_checkpoint(zoneB, zoneA)

    # verify if objnamesA synced to only zoneB but not zoneC
    bucket = get_bucket(zcB, bucketA.name)
    check_objects_exist(bucket, objnamesA, content)

    bucket = get_bucket(zcC, bucketA.name)
    check_objects_not_exist(bucket, objnamesA)

    # verify if objnamesB are not synced to either zoneA or zoneC
    bucket = get_bucket(zcA, bucketB.name)
    check_objects_not_exist(bucket, objnamesB)

    bucket = get_bucket(zcC, bucketB.name)
    check_objects_not_exist(bucket, objnamesB)

    """
        verify the same at bucketA level
        configure another policy at bucketA level with src and dest
        zones specified to zoneA and zoneB resp.

        verify zoneA bucketA syncs to zoneB BucketA but not viceversa.
    """
    # reconfigure zonegroup pipe & flow
    remove_sync_group_pipe(c1, "sync-group", "sync-pipe")
    remove_sync_group_flow_directional(c1, "sync-group", "sync-flow", zoneA.name, zoneB.name)
    create_sync_group_flow_symmetrical(c1, "sync-group", "sync-flow1", zones)
    create_sync_group_pipe(c1, "sync-group", "sync-pipe", zones, zones)

    # change state to allowed
    set_sync_policy_group_status(c1, "sync-group", "allowed")

    zonegroup.period.update(zoneA, commit=True)
    get_sync_policy(c1)

    # configure sync policy for only bucketA and enable it
    create_sync_policy_group(c1, "sync-bucket", "allowed", bucketA.name)
    create_sync_group_flow_symmetrical(c1, "sync-bucket", "sync-flowA", zones, bucketA.name)
    args = ['--source-bucket=*', '--dest-bucket=*']
    create_sync_group_pipe(c1, "sync-bucket", "sync-pipe", zoneA.name, zoneB.name, bucketA.name, args)
    set_sync_policy_group_status(c1, "sync-bucket", "enabled", bucketA.name)

    get_sync_policy(c1, bucketA.name)

    zonegroup_meta_checkpoint(zonegroup)

    # create objects in bucketA in zoneA and zoneB
    objnamesC = [ 'obj7', 'obj8', 'obj9' ]
    objnamesD = [ 'obj10', 'obj11', 'obj12' ]
    create_objects(zcA, bucketA, objnamesC, content)
    create_objects(zcB, bucketA, objnamesD, content)

    zonegroup_meta_checkpoint(zonegroup)
    zone_data_checkpoint(zoneB, zoneA)

    # verify that objnamesC are synced to bucketA in zoneB
    bucket = get_bucket(zcB, bucketA.name)
    check_objects_exist(bucket, objnamesC, content)

    # verify that objnamesD are not synced to bucketA in zoneA
    bucket = get_bucket(zcA, bucketA.name)
    check_objects_not_exist(bucket, objnamesD)

    remove_sync_policy_group(c1, "sync-bucket", bucketA.name)
    remove_sync_policy_group(c1, "sync-group")
    return

@attr('sync_policy')
def test_sync_single_bucket():
    """
    test_sync_single_bucket:
        Allow data sync for only bucketA but not for other buckets via
        below 2 methods

        (a) zonegroup: symmetrical flow but configure pipe for only bucketA.
        (b) bucket level: configure policy for bucketA
    """

    zonegroup = realm.master_zonegroup()
    zonegroup_meta_checkpoint(zonegroup)

    zonegroup_conns = ZonegroupConns(zonegroup)

    (zoneA, zoneB) = zonegroup.zones[0:2]
    (zcA, zcB) = zonegroup_conns.zones[0:2]

    c1 = zoneA.cluster

    c1.admin(['sync', 'policy', 'get'])

    zones = zoneA.name + ',' + zoneB.name
    get_sync_policy(c1)

    objnames = [ 'obj1', 'obj2', 'obj3' ]
    content = 'asdasd'
    buckets = []

    # create bucketA & bucketB in zoneA
    bucketA = create_zone_bucket(zcA)
    buckets.append(bucketA)
    bucketB = create_zone_bucket(zcA)
    buckets.append(bucketB)

    zonegroup_meta_checkpoint(zonegroup)

    """
        Method (a): configure pipe for only bucketA
    """
    # configure sync policy & pipe for only bucketA
    create_sync_policy_group(c1, "sync-group")
    create_sync_group_flow_symmetrical(c1, "sync-group", "sync-flow1", zones)
    args = ['--source-bucket=' +  bucketA.name, '--dest-bucket=' + bucketA.name]

    create_sync_group_pipe(c1, "sync-group", "sync-pipe", zones, zones, None, args)
    set_sync_policy_group_status(c1, "sync-group", "enabled")
    get_sync_policy(c1)
    zonegroup.period.update(zoneA, commit=True)
    
    sync_info(c1)

    # create objects in bucketA & bucketB
    create_objects(zcA, bucketA, objnames, content)
    create_object(zcA, bucketB, objnames, content)

    zonegroup_meta_checkpoint(zonegroup)
    zone_data_checkpoint(zoneB, zoneA)

    # verify if bucketA objects are synced
    bucket = get_bucket(zcB, bucketA.name)
    check_objects_exist(bucket, objnames, content)

    # bucketB objects should not be synced
    bucket = get_bucket(zcB, bucketB.name)
    check_objects_not_exist(bucket, objnames)


    """
        Method (b): configure policy at only bucketA level 
    """
    # reconfigure group pipe
    remove_sync_group_pipe(c1, "sync-group", "sync-pipe")
    create_sync_group_pipe(c1, "sync-group", "sync-pipe", zones, zones)

    # change state to allowed
    set_sync_policy_group_status(c1, "sync-group", "allowed")

    zonegroup.period.update(zoneA, commit=True)
    get_sync_policy(c1)


    # configure sync policy for only bucketA and enable it
    create_sync_policy_group(c1, "sync-bucket", "allowed", bucketA.name)
    create_sync_group_flow_symmetrical(c1, "sync-bucket", "sync-flowA", zones, bucketA.name)
    create_sync_group_pipe(c1, "sync-bucket", "sync-pipe", zones, zones, bucketA.name)
    set_sync_policy_group_status(c1, "sync-bucket", "enabled", bucketA.name)

    get_sync_policy(c1, bucketA.name)

    # create object in bucketA
    create_object(zcA, bucketA, objnames[2], content)

    # create object in bucketA too
    create_object(zcA, bucketB, objnames[2], content)

    zonegroup_meta_checkpoint(zonegroup)
    zone_data_checkpoint(zoneB, zoneA)

    # verify if bucketA objects are synced
    bucket = get_bucket(zcB, bucketA.name)
    check_object_exists(bucket, objnames[2], content)

    # bucketB objects should not be synced
    bucket = get_bucket(zcB, bucketB.name)
    check_object_not_exists(bucket, objnames[2])

    remove_sync_policy_group(c1, "sync-bucket", bucketA.name)
    remove_sync_policy_group(c1, "sync-group")
    return

@attr('sync_policy')
def test_sync_different_buckets():
    """
    test_sync_different_buckets:
        sync zoneA bucketA to zoneB bucketB via below methods

        (a) zonegroup: directional flow but configure pipe for zoneA bucketA to zoneB bucketB
        (b) bucket: configure another policy at bucketA level with pipe set to
        another bucket(bucketB) in target zone.

        sync zoneA bucketA from zoneB bucketB
        (c) configure another policy at bucketA level with pipe set from
        another bucket(bucketB) in source zone.

    """

    zonegroup = realm.master_zonegroup()
    zonegroup_meta_checkpoint(zonegroup)

    zonegroup_conns = ZonegroupConns(zonegroup)

    (zoneA, zoneB) = zonegroup.zones[0:2]
    (zcA, zcB) = zonegroup_conns.zones[0:2]
    zones = zoneA.name + ',' + zoneB.name

    c1 = zoneA.cluster

    c1.admin(['sync', 'policy', 'get'])

    objnames = [ 'obj1', 'obj2' ]
    objnamesB = [ 'obj3', 'obj4' ]
    content = 'asdasd'
    buckets = []

    # create bucketA & bucketB in zoneA
    bucketA = create_zone_bucket(zcA)
    buckets.append(bucketA)
    bucketB = create_zone_bucket(zcA)
    buckets.append(bucketB)

    zonegroup_meta_checkpoint(zonegroup)

    """
        Method (a): zonegroup - configure pipe for only bucketA
    """
    # configure pipe from zoneA bucketA to zoneB bucketB
    create_sync_policy_group(c1, "sync-group")
    create_sync_group_flow_symmetrical(c1, "sync-group", "sync-flow1", zones)
    args = ['--source-bucket=' +  bucketA.name, '--dest-bucket=' + bucketB.name]
    create_sync_group_pipe(c1, "sync-group", "sync-pipe", zoneA.name, zoneB.name, None, args)
    set_sync_policy_group_status(c1, "sync-group", "enabled")
    zonegroup.period.update(zoneA, commit=True)
    get_sync_policy(c1)

    # create objects in bucketA
    create_objects(zcA, bucketA, objnames, content)

    zonegroup_meta_checkpoint(zonegroup)
    zone_data_checkpoint(zoneB, zoneA)

    # verify that objects are synced to bucketB in zoneB
    # but not to bucketA
    bucket = get_bucket(zcB, bucketA.name)
    check_objects_not_exist(bucket, objnames)

    bucket = get_bucket(zcB, bucketB.name)
    check_objects_exist(bucket, objnames, content)
    """
        Method (b): configure policy at only bucketA level with pipe
        set to bucketB in target zone
    """

    remove_sync_group_pipe(c1, "sync-group", "sync-pipe")
    create_sync_group_pipe(c1, "sync-group", "sync-pipe", zones, zones)

    # change state to allowed
    set_sync_policy_group_status(c1, "sync-group", "allowed")

    zonegroup.period.update(zoneA, commit=True)
    get_sync_policy(c1)

    # configure sync policy for only bucketA and enable it
    create_sync_policy_group(c1, "sync-bucket", "allowed", bucketA.name)
    create_sync_group_flow_symmetrical(c1, "sync-bucket", "sync-flowA", zones, bucketA.name)
    args = ['--source-bucket=*', '--dest-bucket=' + bucketB.name]
    create_sync_group_pipe(c1, "sync-bucket", "sync-pipeA", zones, zones, bucketA.name, args)
    set_sync_policy_group_status(c1, "sync-bucket", "enabled", bucketA.name)

    get_sync_policy(c1, bucketA.name)

    objnamesC = [ 'obj5', 'obj6' ]

    zonegroup_meta_checkpoint(zonegroup)
    # create objects in bucketA
    create_objects(zcA, bucketA, objnamesC, content)

    zonegroup_meta_checkpoint(zonegroup)
    zone_data_checkpoint(zoneB, zoneA)

    """
    # verify that objects are synced to bucketB in zoneB
    # but not to bucketA
    """
    bucket = get_bucket(zcB, bucketA.name)
    check_objects_not_exist(bucket, objnamesC)

    bucket = get_bucket(zcB, bucketB.name)
    check_objects_exist(bucket, objnamesC, content)

    remove_sync_policy_group(c1, "sync-bucket", bucketA.name)
    zonegroup_meta_checkpoint(zonegroup)
    get_sync_policy(c1, bucketA.name)

    """
        Method (c): configure policy at only bucketA level with pipe
        set from bucketB in source zone
        verify zoneA bucketA syncs from zoneB BucketB but not bucketA
    """

    # configure sync policy for only bucketA and enable it
    create_sync_policy_group(c1, "sync-bucket", "allowed", bucketA.name)
    create_sync_group_flow_symmetrical(c1, "sync-bucket", "sync-flowA", zones, bucketA.name)
    args = ['--source-bucket=' +  bucketB.name, '--dest-bucket=' + '*']
    create_sync_group_pipe(c1, "sync-bucket", "sync-pipe", zones, zones, bucketA.name, args)
    set_sync_policy_group_status(c1, "sync-bucket", "enabled", bucketA.name)

    get_sync_policy(c1, bucketA.name)

    # create objects in bucketA & B in ZoneB
    objnamesD = [ 'obj7', 'obj8' ]
    objnamesE = [ 'obj9', 'obj10' ]

    create_objects(zcB, bucketA, objnamesD, content)
    create_objects(zcB, bucketB, objnamesE, content)

    zonegroup_meta_checkpoint(zonegroup)
    zone_data_checkpoint(zoneA, zoneB)
    """
    # verify that objects from only bucketB are synced to
    # bucketA in zoneA
    """
    bucket = get_bucket(zcA, bucketA.name)
    check_objects_not_exist(bucket, objnamesD)
    check_objects_exist(bucket, objnamesE, content)

    remove_sync_policy_group(c1, "sync-bucket", bucketA.name)
    remove_sync_policy_group(c1, "sync-group")
    return

@attr('sync_policy')
def test_sync_multiple_buckets_to_single():
    """
    test_sync_multiple_buckets_to_single:
        directional flow
        (a) pipe: sync zoneA bucketA,bucketB to zoneB bucketB

        (b) configure another policy at bucketA level with pipe configured
        to sync from multiple buckets (bucketA & bucketB)

        verify zoneA bucketA & bucketB syncs to zoneB BucketB
    """

    zonegroup = realm.master_zonegroup()
    zonegroup_meta_checkpoint(zonegroup)

    zonegroup_conns = ZonegroupConns(zonegroup)

    (zoneA, zoneB) = zonegroup.zones[0:2]
    (zcA, zcB) = zonegroup_conns.zones[0:2]
    zones = zoneA.name + ',' + zoneB.name

    c1 = zoneA.cluster

    c1.admin(['sync', 'policy', 'get'])

    objnamesA = [ 'obj1', 'obj2' ]
    objnamesB = [ 'obj3', 'obj4' ]
    content = 'asdasd'
    buckets = []

    # create bucketA & bucketB in zoneA
    bucketA = create_zone_bucket(zcA)
    buckets.append(bucketA)
    bucketB = create_zone_bucket(zcA)
    buckets.append(bucketB)

    zonegroup_meta_checkpoint(zonegroup)

    # configure pipe from zoneA bucketA,bucketB to zoneB bucketB
    create_sync_policy_group(c1, "sync-group")
    create_sync_group_flow_directional(c1, "sync-group", "sync-flow", zoneA.name, zoneB.name)
    source_buckets = [ bucketA.name, bucketB.name ]
    for source_bucket in source_buckets:
        args = ['--source-bucket=' +  source_bucket, '--dest-bucket=' + bucketB.name]
        create_sync_group_pipe(c1, "sync-group", "sync-pipe-%s" % source_bucket, zoneA.name, zoneB.name, None, args)

    set_sync_policy_group_status(c1, "sync-group", "enabled")
    zonegroup.period.update(zoneA, commit=True)
    get_sync_policy(c1)

    # create objects in bucketA & bucketB
    create_objects(zcA, bucketA, objnamesA, content)
    create_objects(zcA, bucketB, objnamesB, content)

    zonegroup_meta_checkpoint(zonegroup)
    zone_data_checkpoint(zoneB, zoneA)

    # verify that both zoneA bucketA & bucketB objects are synced to
    # bucketB in zoneB but not to bucketA
    bucket = get_bucket(zcB, bucketA.name)
    check_objects_not_exist(bucket, objnamesA)
    check_objects_not_exist(bucket, objnamesB)

    bucket = get_bucket(zcB, bucketB.name)
    check_objects_exist(bucket, objnamesA, content)
    check_objects_exist(bucket, objnamesB, content)

    """
        Method (b): configure at bucket level
    """
    # reconfigure pipe & flow
    for source_bucket in source_buckets:
        remove_sync_group_pipe(c1, "sync-group", "sync-pipe-%s" % source_bucket)
    remove_sync_group_flow_directional(c1, "sync-group", "sync-flow", zoneA.name, zoneB.name)
    create_sync_group_flow_symmetrical(c1, "sync-group", "sync-flow1", zones)
    create_sync_group_pipe(c1, "sync-group", "sync-pipe", zones, zones)

    # change state to allowed
    set_sync_policy_group_status(c1, "sync-group", "allowed")

    zonegroup.period.update(zoneA, commit=True)
    get_sync_policy(c1)

    objnamesC = [ 'obj5', 'obj6' ]
    objnamesD = [ 'obj7', 'obj8' ]

    # configure sync policy for only bucketA and enable it
    create_sync_policy_group(c1, "sync-bucket", "allowed", bucketA.name)
    create_sync_group_flow_symmetrical(c1, "sync-bucket", "sync-flowA", zones, bucketA.name)
    source_buckets = [ bucketA.name, bucketB.name ]
    for source_bucket in source_buckets:
        args = ['--source-bucket=' +  source_bucket, '--dest-bucket=' + '*']
        create_sync_group_pipe(c1, "sync-bucket", "sync-pipe-%s" % source_bucket, zoneA.name, zoneB.name, bucketA.name, args)

    set_sync_policy_group_status(c1, "sync-bucket", "enabled", bucketA.name)

    get_sync_policy(c1)

    zonegroup_meta_checkpoint(zonegroup)
    # create objects in bucketA
    create_objects(zcA, bucketA, objnamesC, content)
    create_objects(zcA, bucketB, objnamesD, content)

    zonegroup_meta_checkpoint(zonegroup)
    zone_data_checkpoint(zoneB, zoneA)

    # verify that both zoneA bucketA & bucketB objects are synced to
    # bucketA in zoneB but not to bucketB
    bucket = get_bucket(zcB, bucketB.name)
    check_objects_not_exist(bucket, objnamesC)
    check_objects_not_exist(bucket, objnamesD)

    bucket = get_bucket(zcB, bucketA.name)
    check_objects_exist(bucket, objnamesD, content)
    check_objects_exist(bucket, objnamesD, content)

    remove_sync_policy_group(c1, "sync-bucket", bucketA.name)
    remove_sync_policy_group(c1, "sync-group")
    return

@attr('sync_policy')
def test_sync_single_bucket_to_multiple():
    """
    test_sync_single_bucket_to_multiple:
        directional flow
        (a) pipe: sync zoneA bucketA to zoneB bucketA & bucketB

        (b) configure another policy at bucketA level with pipe configured
        to sync to multiple buckets (bucketA & bucketB)

        verify zoneA bucketA syncs to zoneB bucketA & bucketB
    """

    zonegroup = realm.master_zonegroup()
    zonegroup_meta_checkpoint(zonegroup)

    zonegroup_conns = ZonegroupConns(zonegroup)

    (zoneA, zoneB) = zonegroup.zones[0:2]
    (zcA, zcB) = zonegroup_conns.zones[0:2]
    zones = zoneA.name + ',' + zoneB.name

    c1 = zoneA.cluster

    c1.admin(['sync', 'policy', 'get'])

    objnamesA = [ 'obj1', 'obj2' ]
    content = 'asdasd'
    buckets = []

    # create bucketA & bucketB in zoneA
    bucketA = create_zone_bucket(zcA)
    buckets.append(bucketA)
    bucketB = create_zone_bucket(zcA)
    buckets.append(bucketB)

    zonegroup_meta_checkpoint(zonegroup)

    # configure pipe from zoneA bucketA to zoneB bucketA, bucketB
    create_sync_policy_group(c1, "sync-group")
    create_sync_group_flow_symmetrical(c1, "sync-group", "sync-flow1", zones)

    dest_buckets = [ bucketA.name, bucketB.name ]
    for dest_bucket in dest_buckets:
        args = ['--source-bucket=' +  bucketA.name, '--dest-bucket=' + dest_bucket]
        create_sync_group_pipe(c1, "sync-group", "sync-pipe-%s" % dest_bucket, zoneA.name, zoneB.name, None, args)

    create_sync_group_pipe(c1, "sync-group", "sync-pipe", zoneA.name, zoneB.name, None, args)
    set_sync_policy_group_status(c1, "sync-group", "enabled")
    zonegroup.period.update(zoneA, commit=True)
    get_sync_policy(c1)

    # create objects in bucketA
    create_objects(zcA, bucketA, objnamesA, content)

    zonegroup_meta_checkpoint(zonegroup)
    zone_data_checkpoint(zoneB, zoneA)

    # verify that objects from zoneA bucketA are synced to both
    # bucketA & bucketB in zoneB
    bucket = get_bucket(zcB, bucketA.name)
    check_objects_exist(bucket, objnamesA, content)

    bucket = get_bucket(zcB, bucketB.name)
    check_objects_exist(bucket, objnamesA, content)

    """
        Method (b): configure at bucket level
    """
    remove_sync_group_pipe(c1, "sync-group", "sync-pipe")
    create_sync_group_pipe(c1, "sync-group", "sync-pipe", '*', '*')

    # change state to allowed
    set_sync_policy_group_status(c1, "sync-group", "allowed")

    zonegroup.period.update(zoneA, commit=True)
    get_sync_policy(c1)

    objnamesB = [ 'obj3', 'obj4' ]

    # configure sync policy for only bucketA and enable it
    create_sync_policy_group(c1, "sync-bucket", "allowed", bucketA.name)
    create_sync_group_flow_symmetrical(c1, "sync-bucket", "sync-flowA", zones, bucketA.name)
    dest_buckets = [ bucketA.name, bucketB.name ]
    for dest_bucket in dest_buckets:
        args = ['--source-bucket=' + '*', '--dest-bucket=' + dest_bucket]
        create_sync_group_pipe(c1, "sync-bucket", "sync-pipe-%s" % dest_bucket, zoneA.name, zoneB.name, bucketA.name, args)

    set_sync_policy_group_status(c1, "sync-bucket", "enabled", bucketA.name)

    get_sync_policy(c1)

    zonegroup_meta_checkpoint(zonegroup)
    # create objects in bucketA
    create_objects(zcA, bucketA, objnamesB, content)

    zonegroup_meta_checkpoint(zonegroup)
    zone_data_checkpoint(zoneB, zoneA)

    # verify that objects from zoneA bucketA are synced to both
    # bucketA & bucketB in zoneB
    bucket = get_bucket(zcB, bucketA.name)
    check_objects_exist(bucket, objnamesB, content)

    bucket = get_bucket(zcB, bucketB.name)
    check_objects_exist(bucket, objnamesB, content)

    remove_sync_policy_group(c1, "sync-bucket", bucketA.name)
    remove_sync_policy_group(c1, "sync-group")
    return

def stop_2nd_rgw(zonegroup):
    rgw_down = False
    for z in zonegroup.zones:
        if len(z.gateways) <= 1:
            continue
        z.gateways[1].stop()
        log.info('gateway stopped zone=%s gateway=%s', z.name, z.gateways[1].endpoint())
        rgw_down = True
    return rgw_down

def start_2nd_rgw(zonegroup):
    for z in zonegroup.zones:
        if len(z.gateways) <= 1:
            continue
        z.gateways[1].start()
        log.info('gateway started zone=%s gateway=%s', z.name, z.gateways[1].endpoint())

@attr('rgw_down')
def test_bucket_create_rgw_down():
    zonegroup = realm.master_zonegroup()
    try:
        if not stop_2nd_rgw(zonegroup):
            raise SkipTest("test_bucket_create_rgw_down skipped. More than one rgw needed in any one or multiple zone(s).")

        zonegroup_conns = ZonegroupConns(zonegroup)
        buckets, _ = create_bucket_per_zone(zonegroup_conns, 2)
        zonegroup_meta_checkpoint(zonegroup)

        for zone in zonegroup_conns.zones:
            assert check_all_buckets_exist(zone, buckets)

    finally:
        start_2nd_rgw(zonegroup)

@attr('rgw_down')
def test_bucket_remove_rgw_down():
    zonegroup = realm.master_zonegroup()
    try:
        if not stop_2nd_rgw(zonegroup):
            raise SkipTest("test_bucket_remove_rgw_down skipped. More than one rgw needed in any one or multiple zone(s).")

        zonegroup_conns = ZonegroupConns(zonegroup)
        buckets, zone_bucket = create_bucket_per_zone(zonegroup_conns, 2)
        zonegroup_meta_checkpoint(zonegroup)

        for zone in zonegroup_conns.zones:
            assert check_all_buckets_exist(zone, buckets)

        for zone, bucket_name in zone_bucket:
            zone.conn.delete_bucket(bucket_name)

        zonegroup_meta_checkpoint(zonegroup)

        for zone in zonegroup_conns.zones:
            assert check_all_buckets_dont_exist(zone, buckets)

    finally:
        start_2nd_rgw(zonegroup)

@attr('rgw_down')
def test_object_sync_rgw_down():
    zonegroup = realm.master_zonegroup()
    try:
        if not stop_2nd_rgw(zonegroup):
            raise SkipTest("test_object_sync_rgw_down skipped. More than one rgw needed in any one or multiple zone(s).")

        test_object_sync()
    finally:
        start_2nd_rgw(zonegroup)

@attr('rgw_down')
def test_object_delete_rgw_down():
    zonegroup = realm.master_zonegroup()
    try:
        if not stop_2nd_rgw(zonegroup):
            raise SkipTest("test_object_delete_rgw_down skipped. More than one rgw needed in any one or multiple zone(s).")

        test_object_delete()
    finally:
        start_2nd_rgw(zonegroup)

@attr('rgw_down')
def test_concurrent_versioned_object_incremental_sync_rgw_down():
    zonegroup = realm.master_zonegroup()
    try:
        if not stop_2nd_rgw(zonegroup):
            raise SkipTest("test_concurrent_versioned_object_incremental_sync_rgw_down skipped. More than one rgw needed in any one or multiple zone(s).")

        test_concurrent_versioned_object_incremental_sync()
    finally:
        start_2nd_rgw(zonegroup)

@attr('rgw_down')
def test_suspended_delete_marker_full_sync_rgw_down():
    zonegroup = realm.master_zonegroup()
    try:
        if not stop_2nd_rgw(zonegroup):
            raise SkipTest("test_suspended_delete_marker_full_sync_rgw_down skipped. More than one rgw needed in any one or multiple zone(s).")

        test_suspended_delete_marker_full_sync()
    finally:
        start_2nd_rgw(zonegroup)

@attr('rgw_down')
def test_bucket_acl_rgw_down():
    zonegroup = realm.master_zonegroup()
    try:
        if not stop_2nd_rgw(zonegroup):
            raise SkipTest("test_bucket_acl_rgw_down skipped. More than one rgw needed in any one or multiple zone(s).")

        test_bucket_acl()
    finally:
        start_2nd_rgw(zonegroup)

@attr('rgw_down')
def test_bucket_sync_enable_right_after_disable_rgw_down():
    zonegroup = realm.master_zonegroup()
    try:
        if not stop_2nd_rgw(zonegroup):
            raise SkipTest("test_bucket_sync_enable_right_after_disable_rgw_down skipped. More than one rgw needed in any one or multiple zone(s).")

        test_bucket_sync_enable_right_after_disable()
    finally:
        start_2nd_rgw(zonegroup)

@attr('rgw_down')
def test_multipart_object_sync_rgw_down():
    zonegroup = realm.master_zonegroup()
    try:
        if not stop_2nd_rgw(zonegroup):
            raise SkipTest("test_multipart_object_sync_rgw_down skipped. More than one rgw needed in any one or multiple zone(s).")

        test_multipart_object_sync()
    finally:
        start_2nd_rgw(zonegroup)

@attr('rgw_down')
def test_bucket_sync_run_basic_incremental_rgw_down():
    zonegroup = realm.master_zonegroup()
    try:
        if not stop_2nd_rgw(zonegroup):
            raise SkipTest("test_bucket_sync_run_basic_incremental_rgw_down skipped. More than one rgw needed in any one or multiple zone(s).")

        test_bucket_sync_run_basic_incremental()
    finally:
        start_2nd_rgw(zonegroup)

@attr('rgw_down')
def test_role_sync_rgw_down():
    zonegroup = realm.master_zonegroup()
    try:
        if not stop_2nd_rgw(zonegroup):
            raise SkipTest("test_role_sync_rgw_down skipped. More than one rgw needed in any one or multiple zone(s).")

        test_role_sync()
    finally:
        start_2nd_rgw(zonegroup)

@attr('rgw_down')
def test_bucket_full_sync_after_data_sync_init_rgw_down():
    zonegroup = realm.master_zonegroup()
    try:
        if not stop_2nd_rgw(zonegroup):
            raise SkipTest("test_bucket_full_sync_after_data_sync_init_rgw_down skipped. More than one rgw needed in any one or multiple zone(s).")

        test_bucket_full_sync_after_data_sync_init()
    finally:
        start_2nd_rgw(zonegroup)

@attr('rgw_down')
def test_sync_policy_config_zonegroup_rgw_down():
    zonegroup = realm.master_zonegroup()
    try:
        if not stop_2nd_rgw(zonegroup):
            raise SkipTest("test_sync_policy_config_zonegroup_rgw_down skipped. More than one rgw needed in any one or multiple zone(s).")

        test_sync_policy_config_zonegroup()
    finally:
        start_2nd_rgw(zonegroup)

@attr('rgw_down')
def test_sync_flow_symmetrical_zonegroup_all_rgw_down():
    zonegroup = realm.master_zonegroup()
    try:
        if not stop_2nd_rgw(zonegroup):
            raise SkipTest("test_sync_flow_symmetrical_zonegroup_all_rgw_down skipped. More than one rgw needed in any one or multiple zone(s).")

        test_sync_flow_symmetrical_zonegroup_all()
    finally:
        start_2nd_rgw(zonegroup)

def test_topic_notification_sync():
    zonegroup = realm.master_zonegroup()
    zonegroup_meta_checkpoint(zonegroup)
    # let wait for users and other settings to sync across all zones.
    time.sleep(config.checkpoint_delay)
    # create topics in each zone.
    zonegroup_conns = ZonegroupConns(zonegroup)
    topic_arns, zone_topic = create_topic_per_zone(zonegroup_conns)
    log.debug("topic_arns: %s", topic_arns)

    zonegroup_meta_checkpoint(zonegroup)

    # verify topics exists in all zones
    for conn in zonegroup_conns.zones:
        topic_list = conn.list_topics()
        log.debug("topics for zone=%s = %s", conn.name, topic_list)
        assert_equal(len(topic_list), len(topic_arns))
        for topic_arn_map in topic_list:
            assert_true(topic_arn_map['TopicArn'] in topic_arns)

    # create a bucket
    bucket = zonegroup_conns.rw_zones[0].create_bucket(gen_bucket_name())
    log.debug('created bucket=%s', bucket.name)
    zonegroup_meta_checkpoint(zonegroup)

    # create bucket_notification in each zone.
    notification_ids = []
    num = 1
    for zone_conn, topic_arn in zone_topic:
        noti_id = "bn" + '-' + run_prefix + '-' + str(num)
        notification_ids.append(noti_id)
        topic_conf = {'Id': noti_id,
                      'TopicArn': topic_arn,
                      'Events': ['s3:ObjectCreated:*']
                     }
        num += 1
        log.info('creating bucket notification for zone=%s name=%s', zone_conn.name, noti_id)
        zone_conn.create_notification(bucket.name, [topic_conf])
    zonegroup_meta_checkpoint(zonegroup)

    # verify notifications exists in all zones
    for conn in zonegroup_conns.zones:
        notification_list = conn.list_notifications(bucket.name)
        log.debug("notifications for zone=%s = %s", conn.name, notification_list)
        assert_equal(len(notification_list), len(topic_arns))
        for notification in notification_list:
            assert_true(notification['Id'] in notification_ids)

    # verify bucket_topic mapping
    # create a new bucket and subcribe it to first topic.
    bucket_2 = zonegroup_conns.rw_zones[0].create_bucket(gen_bucket_name())
    notif_id = "bn-2" + '-' + run_prefix
    topic_conf = {'Id': notif_id,
                  'TopicArn': topic_arns[0],
                  'Events': ['s3:ObjectCreated:*']
                  }
    zonegroup_conns.rw_zones[0].create_notification(bucket_2.name, [topic_conf])
    zonegroup_meta_checkpoint(zonegroup)
    for conn in zonegroup_conns.zones:
        topics = get_topics(conn.zone)
        for topic in topics:
            if topic['arn'] == topic_arns[0]:
                assert_equal(len(topic['subscribed_buckets']), 2)
                assert_true(bucket_2.name in topic['subscribed_buckets'])
            else:
                assert_equal(len(topic['subscribed_buckets']), 1)
            assert_true(bucket.name in topic['subscribed_buckets'])

    # delete the 2nd bucket and verify the mapping is removed.
    zonegroup_conns.rw_zones[0].delete_bucket(bucket_2.name)
    zonegroup_meta_checkpoint(zonegroup)
    for conn in zonegroup_conns.zones:
        topics = get_topics(conn.zone)
        for topic in topics:
            assert_equal(len(topic['subscribed_buckets']), 1)
        '''TODO(Remove the break once the https://tracker.ceph.com/issues/20802
           is fixed, as the secondary site bucket instance info is currently not
           getting deleted coz of the bug hence the bucket-topic mapping
           deletion is not invoked on secondary sites.)'''
        break

    # delete notifications
    zonegroup_conns.rw_zones[0].delete_notifications(bucket.name)
    log.debug('Deleting all notifications for  bucket=%s', bucket.name)
    zonegroup_meta_checkpoint(zonegroup)

    # verify notification deleted in all zones
    for conn in zonegroup_conns.zones:
        notification_list = conn.list_notifications(bucket.name)
        assert_equal(len(notification_list), 0)

    # delete topics
    for zone_conn, topic_arn in zone_topic:
        log.debug('deleting topic zone=%s arn=%s', zone_conn.name, topic_arn)
        zone_conn.delete_topic(topic_arn)
    zonegroup_meta_checkpoint(zonegroup)

    # verify topics deleted in all zones
    for conn in zonegroup_conns.zones:
        topic_list = conn.list_topics()
        assert_equal(len(topic_list), 0)
