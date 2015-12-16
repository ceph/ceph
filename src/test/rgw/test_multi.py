import subprocess
import os
import json
import random
import string
import argparse
import sys
import time

mstart_path = os.getenv('MSTART_PATH')
if mstart_path is None:
    mstart_path = os.path.normpath(os.path.dirname(os.path.realpath(__file__)) + '/../..') + '/'

test_path = os.path.normpath(os.path.dirname(os.path.realpath(__file__))) + '/'

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
    print 'cmd:', cmd
    process = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
    s = process.communicate()[0]
    print s
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
        print s
        return (s, retcode)

    def rgw_admin_ro(self, cmd, check_retcode = True):
        (s, retcode) = bash(tpath('test-rgw-call.sh', 'call_rgw_admin', self.cluster_num, '--rgw-cache-enabled=false ' + cmd), check_retcode)
        print s
        return (s, retcode)

class RGWRealm:
    def __init__(self, realm, credentials, master_index):
        self.realm = realm
        self.credentials = credentials
        self.master_index = master_index

    def init_zone(self, cluster, zg, zone_name, port, first_zone_port=0):
        if first_zone_port == 0:
            bash(tpath('test-rgw-call.sh', 'init_first_zone', cluster.cluster_num,
                       self.realm, zg, zone_name, port,
                       self.credentials.access_key, self.credentials.secret))
        else:
            bash(tpath('test-rgw-call.sh', 'init_zone_in_existing_zg', cluster.cluster_num,
                       self.realm, zg, zone_name, first_zone_port, port,
                       self.credentials.access_key, self.credentials.secret))

    def meta_sync_status(self, cluster):
        if cluster.cluster_num == self.master_index:
            return None

        while True:
            (meta_sync_status_json, retcode)=cluster.rgw_admin_ro('--rgw-realm=' + self.realm + ' metadata sync status', check_retcode = False)
            if retcode == 0:
                break

            assert(retcode == 2) # ENOENT

        print 'm=', meta_sync_status_json
        meta_sync_status = json.loads(meta_sync_status_json)
        
        global_sync_status=meta_sync_status['sync_status']['info']['status']
        num_shards=meta_sync_status['sync_status']['info']['num_shards']

        sync_markers=meta_sync_status['sync_status']['markers']
        print 'sync_markers=', sync_markers
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

        return markers

    def meta_checkpoint(self, master_cluster, cluster):
        if cluster.cluster_num == self.master_index:
            return

        while True:
            log_status = self.meta_master_log_status(master_cluster)
            (num_shards, sync_status) = self.meta_sync_status(cluster)

            print 'log_status', log_status
            print 'sync_status', sync_status

            if (log_status == sync_status):
                break

            time.sleep(5)


        bash(tpath('test-rgw-call.sh', 'wait_for_meta_sync', self.master_index + 1, cluster.cluster_num, self.realm))

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
        realm = RGWRealm('earth', credentials, 0)

        if bootstrap:
            self.clusters[0].start()
            realm.init_zone(self.clusters[0], 'us', 'us-1', self.base_port)

            for i in xrange(1, self.num_clusters):
                self.clusters[i].start()
                realm.init_zone(self.clusters[i], 'us', 'us-' + str(i + 1), self.base_port + i, first_zone_port=self.base_port)

        for i in xrange(1, self.num_clusters):
            print 'meta checkpoint start on cluster #', i
            realm.meta_checkpoint(self.clusters[0], self.clusters[i])
            print 'meta checkpoint finish on cluster #', i


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
