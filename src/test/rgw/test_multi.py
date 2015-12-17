import subprocess
import os
import json
import random
import string
import argparse
import sys
import time

import boto
import boto.s3.connection

import inspect

def lineno():
    return inspect.currentframe().f_back.f_lineno

mstart_path = os.getenv('MSTART_PATH')
if mstart_path is None:
    mstart_path = os.path.normpath(os.path.dirname(os.path.realpath(__file__)) + '/../..') + '/'

test_path = os.path.normpath(os.path.dirname(os.path.realpath(__file__))) + '/'

def log(*params):
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
    log('running cmd: ', cmd)
    process = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
    s = process.communicate()[0]
    log('command returned status=', process.returncode, ' stdout=', s)
    if check_retcode:
        assert(process.returncode == 0)
    return (s, process.returncode)

def mstart(cluster_id, is_new):
    cmd = mpath('mstart.sh', cluster_id)
    if is_new:
        cmd += ' -n'

    bash(cmd)

def mstop(cluster_id, entity = None):
    cmd  = mpath('mstop.sh', cluseter_id)
    if entity is not None:
        cmd += ' ' + entity
    bash(cmd)

def mrgw(cluster_id, port, extra_cmd = None):
    cmd  = mpath('mrgw.sh', port)
    if extra_cmd is not None:
        cmd += ' ' + extra_cmd
    bash(cmd)

def init_multi_site(num_clusters):
    bash(tpath('test-rgw-multisite.sh', num_clusters))


class RGWRealmCredentials:
    def __init__(self, access_key, secret):
        self.access_key = access_key
        self.secret = secret

class RGWCluster:
    def __init__(self, cluster_num):
        self.cluster_num = cluster_num
        self.cluster_id = 'c' + str(cluster_num)
        self.needs_reset = True

    def start(self):
        mstart(self.cluster_id, self.needs_reset)
        self.needs_reset = False

    def stop(self):
        mstop(self.cluster_id)

    def start_rgw(port):
        mrgw(self.cluster_id, port)

    def stop_rgw(self):
        mstop(self.cluster_id, 'radosgw')

    def rgw_admin(self, cmd, check_retcode = True):
        (s, retcode) = bash(tpath('test-rgw-call.sh', 'call_rgw_admin', self.cluster_num, cmd))
        return (s, retcode)

    def rgw_admin_ro(self, cmd, check_retcode = True):
        (s, retcode) = bash(tpath('test-rgw-call.sh', 'call_rgw_admin', self.cluster_num, '--rgw-cache-enabled=false ' + cmd), check_retcode)
        return (s, retcode)

class RGWZone:
    def __init__(self, realm, zg, zone_name, port):
        self.realm = realm
        self.zg = zg
        self.zone_name = zone_name
        self.port = port
        self.connection = None

    def get_connection(self, user):
        if self.connection is None:
            self.connection = boto.connect_s3(aws_access_key_id = user.access_key,
                                          aws_secret_access_key = user.secret,
                                          host = 'localhost',
                                          port = self.port,
                                          is_secure = False,
                                          calling_format = boto.s3.connection.OrdinaryCallingFormat())
        return self.connection

class RGWRealm:
    def __init__(self, realm, credentials, clusters, master_index):
        self.realm = realm
        self.credentials = credentials
        self.clusters = clusters
        self.master_index = master_index
        self.master_cluster = clusters[master_index]
        self.zones = {}
        self.total_zones = 0

    def init_zone(self, cluster, zg, zone_name, port, first_zone_port=0):
        if first_zone_port == 0:
            bash(tpath('test-rgw-call.sh', 'init_first_zone', cluster.cluster_num,
                       self.realm, zg, zone_name, port,
                       self.credentials.access_key, self.credentials.secret))
        else:
            bash(tpath('test-rgw-call.sh', 'init_zone_in_existing_zg', cluster.cluster_num,
                       self.realm, zg, zone_name, first_zone_port, port,
                       self.credentials.access_key, self.credentials.secret))

        self.add_zone(zg, zone_name, port)

    def add_zone(self, zg, zone_name, port):
        zone = RGWZone(self.realm, zg, zone_name, port)
        self.zones[self.total_zones] = zone
        self.total_zones += 1


    def get_zone(self, num):
        if num >= self.total_zones:
            return None
        return self.zones[num]

    def num_zones(self):
        return self.total_zones

    def meta_sync_status(self, cluster):
        if cluster.cluster_num == self.master_cluster.cluster_num:
            return None

        while True:
            (meta_sync_status_json, retcode)=cluster.rgw_admin_ro('--rgw-realm=' + self.realm + ' metadata sync status', check_retcode = False)
            if retcode == 0:
                break

            assert(retcode == 2) # ENOENT

        log('current meta sync status=', meta_sync_status_json)
        sync_status = json.loads(meta_sync_status_json)
        
        global_sync_status=sync_status['sync_status']['info']['status']
        num_shards=sync_status['sync_status']['info']['num_shards']

        sync_markers=sync_status['sync_status']['markers']
        log('sync_markers=', sync_markers)
        assert(num_shards == len(sync_markers))

        markers={}
        for i in xrange(num_shards):
            markers[i] = sync_markers[i]['val']['marker']

        return (num_shards, markers)

    def meta_master_log_status(self, master_cluster):
        (mdlog_status_json, retcode)=master_cluster.rgw_admin_ro('--rgw-realm=' + self.realm + ' mdlog status')
        mdlog_status = json.loads(mdlog_status_json)

        markers={}
        i = 0
        for s in mdlog_status:
            markers[i] = s['marker']
            i += 1

        log('master meta markers=', markers)

        return markers

    def compare_meta_status(self, cluster, log_status, sync_status):
        if len(log_status) != len(sync_status):
            log('len(log_status)=', len(log_status), ' len(sync_status=', len(sync_status))
            return False

        i = 0
        msg =  ''
        for l, s in zip(log_status, sync_status):
            if l > s:
                if len(s) != 0:
                    msg += ', '
                msg += 'shard=' + str(i) + ' master=' + ' target=' + s
            i += 1

        if len(msg) > 0:
            log('cluster ', cluster.cluster_id, ' behind master: ', msg)
            return False

        return True

    def cluster_meta_checkpoint(self, cluster):
        if cluster.cluster_num == self.master_cluster.cluster_num:
            return

        log('starting meta checkpoint for cluster=', cluster.cluster_id)

        while True:
            log_status = self.meta_master_log_status(self.master_cluster)
            (num_shards, sync_status) = self.meta_sync_status(cluster)

            if self.compare_meta_status(cluster, log_status, sync_status):
                break

            time.sleep(5)


        log('finish meta checkpoint for cluster=', cluster.cluster_id)

    def meta_checkpoint(self):
        log('meta checkpoint')
        for (index, c) in self.clusters.iteritems():
            self.cluster_meta_checkpoint(c)


    def create_user(self, user, wait_meta = True):
        log('creating user uid=', user.uid)
        cmd = build_cmd('--uid', user.uid, '--display-name', user.display_name,
                        '--access-key', user.access_key, '--secret', user.secret)
        self.master_cluster.rgw_admin('--rgw-realm=' + self.realm + ' user create ' + cmd)

        if wait_meta:
            self.meta_checkpoint()




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

class RGWMulti:
    def __init__(self, num_clusters):
        self.num_clusters = num_clusters

        self.clusters = {}
        for i in xrange(num_clusters):
            self.clusters[i] = RGWCluster(i + 1)

        self.base_port = 8000

    def setup(self, bootstrap):
        credentials = RGWRealmCredentials(gen_access_key(), gen_secret())
        realm = RGWRealm('earth', credentials, self.clusters, 0)

        if bootstrap:
            log('bootstapping clusters')
            self.clusters[0].start()
            realm.init_zone(self.clusters[0], 'us', 'us-1', self.base_port)

            for i in xrange(1, self.num_clusters):
                self.clusters[i].start()
                realm.init_zone(self.clusters[i], 'us', 'us-' + str(i + 1), self.base_port + i, first_zone_port=self.base_port)
        else:
            for i in xrange(0, self.num_clusters):
                realm.add_zone('us', 'us-' + str(i + 1), self.base_port + i)

        realm.meta_checkpoint()

        user = RGWUser('tester', '"Test User"', gen_access_key(), gen_secret())
        realm.create_user(user)


        zone = realm.get_zone(0)
        conn = zone.get_connection(user)

        log('create bucket')
        bucket = conn.create_bucket('mybucket')

        realm.meta_checkpoint()



def main():
    parser = argparse.ArgumentParser(
            description='Add bucket lifecycle configuration',
            usage='obo bucket lifecycle add <bucket>')

    parser.add_argument('--num-zones', type=int, default=2)
    parser.add_argument('--no-bootstrap', action='store_true')

    args = parser.parse_args(sys.argv[1:])
    rgw_multi = RGWMulti(int(args.num_zones))

    rgw_multi.setup(not args.no_bootstrap)

if __name__ == "__main__":
    main()
