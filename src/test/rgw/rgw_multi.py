import subprocess
import os
import json
import random
import string
import argparse
import sys
import time
import itertools

import ConfigParser

import boto
import boto.s3.connection

import inspect

from nose.tools import eq_ as eq

log_level = 20

num_buckets = 0
run_prefix=''.join(random.SystemRandom().choice(string.ascii_lowercase) for _ in range(6))

mstart_path = os.getenv('MSTART_PATH')
if mstart_path is None:
    mstart_path = os.path.normpath(os.path.dirname(os.path.realpath(__file__)) + '/../..') + '/'

test_path = os.path.normpath(os.path.dirname(os.path.realpath(__file__))) + '/'

def lineno():
    return inspect.currentframe().f_back.f_lineno

def log(level, *params):
    if level > log_level:
        return

    s = '>>> '
    for p in params:
        if p:
            s += str(p)

    print s
    sys.stdout.flush()

def build_cmd(*params):
    s = ''
    for p in params:
        if len(s) != 0:
            s += ' '
        s += p

    return s

def mpath(bin, *params):
    s = mstart_path + bin
    for p in params:
        s += ' ' + str(p)

    return s

def tpath(bin, *params):
    s = test_path + bin
    for p in params:
        s += ' ' + str(p)

    return s

def bash(cmd, check_retcode = True):
    log(5, 'running cmd: ', cmd)
    process = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
    s = process.communicate()[0]
    log(20, 'command returned status=', process.returncode, ' stdout=', s)
    if check_retcode:
        assert(process.returncode == 0)
    return (s, process.returncode)

def mstart(cluster_id, is_new):
    cmd = mpath('mstart.sh', cluster_id)
    if is_new:
        cmd += ' -n'

    bash(cmd)

def mstop(cluster_id, entity = None):
    cmd  = mpath('mstop.sh', cluster_id)
    if entity is not None:
        cmd += ' ' + entity
    bash(cmd)

def mrgw(cluster_id, port, extra_cmd = None):
    cmd  = mpath('mrgw.sh', cluster_id, port)
    if extra_cmd is not None:
        cmd += ' ' + extra_cmd
    bash(cmd)

class RGWRealmCredentials:
    def __init__(self, access_key, secret):
        self.access_key = access_key
        self.secret = secret

class RGWCluster:
    def __init__(self, cluster_num, port):
        self.cluster_num = cluster_num
        self.cluster_id = 'c' + str(cluster_num)
        self.port = port
        self.needs_reset = True

    def start(self):
        mstart(self.cluster_id, self.needs_reset)
        self.needs_reset = False

    def stop(self):
        mstop(self.cluster_id)

    def start_rgw(self, zone):
        args = '--debug-rgw=20 --debug-ms=1 --rgw-zone=' + zone
        mrgw(self.cluster_id, self.port, args)

    def stop_rgw(self):
        mstop(self.cluster_id, 'radosgw')

    def rgw_admin(self, cmd, check_retcode = True):
        (s, retcode) = bash(tpath('test-rgw-call.sh', 'call_rgw_admin', self.cluster_num, cmd))
        return (s, retcode)

    def rgw_admin_ro(self, cmd, check_retcode = True):
        (s, retcode) = bash(tpath('test-rgw-call.sh', 'call_rgw_admin', self.cluster_num, '--rgw-cache-enabled=false ' + cmd), check_retcode)
        return (s, retcode)

class RGWZone:
    def __init__(self, realm, cluster, zg, zone_name):
        self.realm = realm
        self.cluster = cluster
        self.zg = zg
        self.zone_name = zone_name
        self.connection = None

    def get_connection(self, user):
        if self.connection is None:
            self.connection = boto.connect_s3(aws_access_key_id = user.access_key,
                                          aws_secret_access_key = user.secret,
                                          host = 'localhost',
                                          port = self.cluster.port,
                                          is_secure = False,
                                          calling_format = boto.s3.connection.OrdinaryCallingFormat())
        return self.connection

class RGWRealm:
    def __init__(self, realm, credentials, clusters):
        self.realm = realm
        self.credentials = credentials
        self.clusters = clusters
        self.zones = {}
        self.total_zones = 0

    def init_zone(self, cluster, zg, zone_name, first_zone_port):
        is_master = (first_zone_port == cluster.port)
        if is_master:
            bash(tpath('test-rgw-call.sh', 'init_first_zone', cluster.cluster_num,
                       self.realm, zg, zone_name, cluster.port,
                       self.credentials.access_key, self.credentials.secret))
        else:
            bash(tpath('test-rgw-call.sh', 'init_zone_in_existing_zg', cluster.cluster_num,
                       self.realm, zg, zone_name, first_zone_port, cluster.port,
                       self.credentials.access_key, self.credentials.secret))

        self.add_zone(cluster, zg, zone_name, is_master)

    def add_zone(self, cluster, zg, zone_name, is_master):
        zone = RGWZone(self.realm, cluster, zg, zone_name)
        self.zones[self.total_zones] = zone
        self.total_zones += 1

        if is_master:
            self.master_zone = zone


    def get_zone(self, num):
        if num >= self.total_zones:
            return None
        return self.zones[num]

    def get_zones(self):
        for (k, zone) in self.zones.iteritems():
            yield zone


    def num_zones(self):
        return self.total_zones

    def meta_sync_status(self, zone):
        if zone.zone_name == self.master_zone.zone_name:
            return None

        while True:
            (meta_sync_status_json, retcode) = zone.cluster.rgw_admin_ro('--rgw-realm=' + self.realm + ' metadata sync status', check_retcode = False)
            if retcode == 0:
                break

            assert(retcode == 2) # ENOENT

        log(20, 'current meta sync status=', meta_sync_status_json)
        sync_status = json.loads(meta_sync_status_json)
        
        global_sync_status=sync_status['sync_status']['info']['status']
        num_shards=sync_status['sync_status']['info']['num_shards']

        sync_markers=sync_status['sync_status']['markers']
        log(20, 'sync_markers=', sync_markers)
        assert(num_shards == len(sync_markers))

        markers={}
        for i in xrange(num_shards):
            markers[i] = sync_markers[i]['val']['marker']

        return (num_shards, markers)

    def meta_master_log_status(self, master_zone):
        (mdlog_status_json, retcode) = master_zone.cluster.rgw_admin_ro('--rgw-realm=' + self.realm + ' mdlog status')
        mdlog_status = json.loads(mdlog_status_json)

        markers={}
        i = 0
        for s in mdlog_status:
            markers[i] = s['marker']
            i += 1

        log(20, 'master meta markers=', markers)

        return markers

    def compare_meta_status(self, zone, log_status, sync_status):
        if len(log_status) != len(sync_status):
            log(10, 'len(log_status)=', len(log_status), ' len(sync_status=', len(sync_status))
            return False

        msg =  ''
        for i, l, s in zip(log_status, log_status.itervalues(), sync_status.itervalues()):
            if l > s:
                if len(s) != 0:
                    msg += ', '
                msg += 'shard=' + str(i) + ' master=' + l + ' target=' + s

        if len(msg) > 0:
            log(1, 'zone ', zone.zone_name, ' behind master: ', msg)
            return False

        return True

    def zone_meta_checkpoint(self, zone):
        if zone.zone_name == self.master_zone.zone_name:
            return

        log(10, 'starting meta checkpoint for zone=', zone.zone_name)

        while True:
            log_status = self.meta_master_log_status(self.master_zone)
            (num_shards, sync_status) = self.meta_sync_status(zone)

            log(20, 'log_status=', log_status)
            log(20, 'sync_status=', sync_status)

            if self.compare_meta_status(zone, log_status, sync_status):
                break

            time.sleep(5)


        log(10, 'finish meta checkpoint for zone=', zone.zone_name)

    def meta_checkpoint(self):
        log(5, 'meta checkpoint')
        for z in self.get_zones():
            self.zone_meta_checkpoint(z)

    def data_sync_status(self, target_zone, source_zone):
        if target_zone.zone_name == source_zone.zone_name:
            return None

        while True:
            (data_sync_status_json, retcode) = target_zone.cluster.rgw_admin_ro('--rgw-realm=' + self.realm + ' data sync status --source-zone=' + source_zone.zone_name, check_retcode = False)
            if retcode == 0:
                break

            assert(retcode == 2) # ENOENT

        log(20, 'current data sync status=', data_sync_status_json)
        sync_status = json.loads(data_sync_status_json)
        
        global_sync_status=sync_status['sync_status']['info']['status']
        num_shards=sync_status['sync_status']['info']['num_shards']

        sync_markers=sync_status['sync_status']['markers']
        log(20, 'sync_markers=', sync_markers)
        assert(num_shards == len(sync_markers))

        markers={}
        for i in xrange(num_shards):
            markers[i] = sync_markers[i]['val']['marker']

        return (num_shards, markers)

    def bucket_sync_status(self, target_zone, source_zone, bucket_name):
        if target_zone.zone_name == source_zone.zone_name:
            return None

        while True:
            (bucket_sync_status_json, retcode) = target_zone.cluster.rgw_admin_ro('--rgw-realm=' + self.realm +
                                                                                ' bucket sync status --source-zone=' + source_zone.zone_name +
                                                                                ' --bucket=' + bucket_name, check_retcode = False)
            if retcode == 0:
                break

            assert(retcode == 2) # ENOENT

        log(20, 'current bucket sync status=', bucket_sync_status_json)
        sync_status = json.loads(bucket_sync_status_json)

        markers={}
        for entry in sync_status:
            val = entry['val']
            pos = val['inc_marker']['position'].split('#')[-1] # get rid of shard id; e.g., 6#00000000002.132.3 -> 00000000002.132.3
            markers[entry['key']] = pos

        return markers

    def data_source_log_status(self, source_zone):
        source_cluster = source_zone.cluster
        (datalog_status_json, retcode) = source_cluster.rgw_admin_ro('--rgw-realm=' + self.realm + ' datalog status')
        datalog_status = json.loads(datalog_status_json)

        markers={}
        i = 0
        for s in datalog_status:
            markers[i] = s['marker']
            i += 1

        log(20, 'data markers for zone=', source_zone.zone_name, ' markers=', markers)

        return markers

    def bucket_source_log_status(self, source_zone, bucket_name):
        source_cluster = source_zone.cluster
        (bilog_status_json, retcode) = source_cluster.rgw_admin_ro('--rgw-realm=' + self.realm + ' bilog status --bucket=' + bucket_name)
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

        log(20, 'bilog markers for zone=', source_zone.zone_name, ' bucket=', bucket_name, ' markers=', markers)

        return markers

    def compare_data_status(self, target_zone, source_zone, log_status, sync_status):
        if len(log_status) != len(sync_status):
            log(10, 'len(log_status)=', len(log_status), ' len(sync_status)=', len(sync_status))
            return False

        msg =  ''
        for i, l, s in zip(log_status, log_status.itervalues(), sync_status.itervalues()):
            if l > s:
                if len(s) != 0:
                    msg += ', '
                msg += 'shard=' + str(i) + ' master=' + l + ' target=' + s

        if len(msg) > 0:
            log(1, 'data of zone ', target_zone.zone_name, ' behind zone ', source_zone.zone_name, ': ', msg)
            return False

        return True

    def compare_bucket_status(self, target_zone, source_zone, bucket_name, log_status, sync_status):
        if len(log_status) != len(sync_status):
            log(10, 'len(log_status)=', len(log_status), ' len(sync_status)=', len(sync_status))
            return False

        msg =  ''
        for i, l, s in zip(log_status, log_status.itervalues(), sync_status.itervalues()):
            if l > s:
                if len(s) != 0:
                    msg += ', '
                msg += 'shard=' + str(i) + ' master=' + l + ' target=' + s

        if len(msg) > 0:
            log(1, 'bucket ', bucket_name, ' zone ', target_zone.zone_name, ' behind zone ', source_zone.zone_name, ': ', msg)
            return False

        return True

    def zone_data_checkpoint(self, target_zone, source_zone):
        if target_zone.zone_name == source_zone.zone_name:
            return

        log(10, 'starting data checkpoint for target_zone=', target_zone.zone_name, ' source_zone=', source_zone.zone_name)

        while True:
            log_status = self.data_source_log_status(source_zone)
            (num_shards, sync_status) = self.data_sync_status(target_zone, source_zone)

            log(20, 'log_status=', log_status)
            log(20, 'sync_status=', sync_status)

            if self.compare_data_status(target_zone, source_zone, log_status, sync_status):
                break

            time.sleep(5)

        log(10, 'finished data checkpoint for target_zone=', target_zone.zone_name, ' source_zone=', source_zone.zone_name)

    def zone_bucket_checkpoint(self, target_zone, source_zone, bucket_name):
        if target_zone.zone_name == source_zone.zone_name:
            return

        log(10, 'starting bucket checkpoint for target_zone=', target_zone.zone_name, ' source_zone=', source_zone.zone_name, ' bucket_name=', bucket_name)

        while True:
            log_status = self.bucket_source_log_status(source_zone, bucket_name)
            sync_status = self.bucket_sync_status(target_zone, source_zone, bucket_name)

            log(20, 'log_status=', log_status)
            log(20, 'sync_status=', sync_status)

            if self.compare_bucket_status(target_zone, source_zone, bucket_name, log_status, sync_status):
                break

            time.sleep(5)

        log(10, 'finished bucket checkpoint for target_zone=', target_zone.zone_name, ' source_zone=', source_zone.zone_name, ' bucket_name=', bucket_name)


    def create_user(self, user, wait_meta = True):
        log(5, 'creating user uid=', user.uid)
        cmd = build_cmd('--uid', user.uid, '--display-name', user.display_name,
                        '--access-key', user.access_key, '--secret', user.secret)
        self.master_zone.cluster.rgw_admin('--rgw-realm=' + self.realm + ' user create ' + cmd)

        if wait_meta:
            self.meta_checkpoint()

    def set_master_zone(self, zone):
        (zg_json, retcode) = zone.cluster.rgw_admin('--rgw-realm=' + self.realm + ' --rgw-zonegroup=' + zone.zg + ' --rgw-zone=' + zone.zone_name + ' zone modify --master=1')
        (period_json, retcode) = zone.cluster.rgw_admin('--rgw-realm=' + self.realm + ' period update --commit')
	self.master_zone = zone


class RGWUser:
    def __init__(self, uid, display_name, access_key, secret):
        self.uid = uid
        self.display_name = display_name
        self.access_key = access_key
        self.secret = secret

def gen_access_key():
     return ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(16))

def gen_secret():
     return ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(32))

def gen_bucket_name():
    global num_buckets

    num_buckets += 1
    return run_prefix + '-' + str(num_buckets)

class RGWMulti:
    def __init__(self, num_clusters):
        self.num_clusters = num_clusters

        self.base_port = 8000

        self.clusters = {}
        for i in xrange(num_clusters):
            self.clusters[i] = RGWCluster(i + 1, self.base_port + i)

        self.realm = None
	self.user = None

    def setup(self, bootstrap):
        realm_credentials = RGWRealmCredentials(gen_access_key(), gen_secret())
        self.realm = RGWRealm('earth', realm_credentials, self.clusters)

        if bootstrap:
            log(1, 'bootstapping clusters')
            self.clusters[0].start()
            self.realm.init_zone(self.clusters[0], 'us', 'us-1', self.base_port)

            for i in xrange(1, self.num_clusters):
                self.clusters[i].start()
                self.realm.init_zone(self.clusters[i], 'us', 'us-' + str(i + 1), self.base_port)
        else:
            for i in xrange(0, self.num_clusters):
                self.realm.add_zone(self.clusters[i], 'us', 'us-' + str(i + 1), (i == 0))

        self.realm.meta_checkpoint()

        self.user = RGWUser('tester', '"Test User"', gen_access_key(), gen_secret())
        self.realm.create_user(self.user)

        self.realm.meta_checkpoint()

def check_all_buckets_exist(user, zone, buckets):
    conn = zone.get_connection(user)

    for b in buckets:
        try:
            conn.get_bucket(b)
        except:
            log(0, 'zone ', zone.zone_name, ' does not contain bucket ', b)
            return False

    return True

def check_all_buckets_dont_exist(user, zone, buckets):
    conn = zone.get_connection(user)

    for b in buckets:
        try:
            conn.get_bucket(b)
        except:
            continue

        log(0, 'zone ', zone.zone_name, ' contains bucket ', b)
        return False

    return True

def create_bucket_per_zone(user, realm):
    buckets = []
    zone_bucket = {}
    for zone in realm.get_zones():
        conn = zone.get_connection(user)
        bucket_name = gen_bucket_name()
        log(1, 'create bucket zone=', zone.zone_name, ' name=', bucket_name)
        bucket = conn.create_bucket(bucket_name)
        buckets.append(bucket_name)
        zone_bucket[zone] = bucket

    return buckets, zone_bucket

def get_bucket(user, zone, bucket_name):
    conn = zone.get_connection(user)
    return conn.get_bucket(bucket_name)

def get_key(user, zone, bucket_name, obj_name):
    b = get_bucket(user, zone, bucket_name)
    return b.get_key(obj_name)

def new_key(user, zone, bucket_name, obj_name):
    b = get_bucket(user, zone, bucket_name)
    return b.new_key(obj_name)

def check_object_eq(k1, k2, check_extra = True):
    assert k1
    assert k2
    log(10, 'comparing key name=', k1.name)
    eq(k1.name, k2.name)
    eq(k1.get_contents_as_string(), k2.get_contents_as_string())
    eq(k1.metadata, k2.metadata)
    eq(k1.cache_control, k2.cache_control)
    eq(k1.content_type, k2.content_type)
    eq(k1.content_encoding, k2.content_encoding)
    eq(k1.content_disposition, k2.content_disposition)
    eq(k1.content_language, k2.content_language)
    eq(k1.etag, k2.etag)
    # eq(k1.last_modified, k2.last_modified)
    if check_extra:
        eq(k1.owner.id, k2.owner.id)
        eq(k1.owner.display_name, k2.owner.display_name)
    eq(k1.storage_class, k2.storage_class)
    eq(k1.size, k2.size)
    eq(k1.version_id, k2.version_id)
    eq(k1.encrypted, k2.encrypted)

def check_bucket_eq(user, zone1, zone2, bucket_name):
    log(10, 'comparing bucket=', bucket_name, ' zones={', zone1.zone_name, ', ', zone2.zone_name, '}')
    b1 = get_bucket(user, zone1, bucket_name)
    b2 = get_bucket(user, zone2, bucket_name)

    log(20, 'bucket1 objects:')
    for o in b1.get_all_versions():
        log(20, 'o=', o.name)
    log(20, 'bucket2 objects:')
    for o in b2.get_all_versions():
        log(20, 'o=', o.name)

    for k1, k2 in itertools.izip_longest(b1.get_all_versions(), b2.get_all_versions()):
        if k1 is None:
            log(0, 'failure: key=', k2.name, ' is missing from zone=', zone1.zone_name)
            assert False
        if k2 is None:
            log(0, 'failure: key=', k1.name, ' is missing from zone=', zone2.zone_name)
            assert False

        check_object_eq(k1, k2)

        # now get the keys through a HEAD operation, verify that the available data is the same
        k1_head = b1.get_key(k1.name)
        k2_head = b2.get_key(k2.name)

        check_object_eq(k1_head, k2_head, False)

    log(5, 'success, bucket identical: bucket=', bucket_name, ' zones={', zone1.zone_name, ', ', zone2.zone_name, '}')
