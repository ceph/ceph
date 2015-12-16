import subprocess
import os


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
    return s

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

    def rgw_admin(self, cmd):
        print bash(tpath('test-rgw-call.sh', 'call_rgw_admin', self.cluster_num, cmd))

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

    def meta_checkpoint(self, cluster_index):
        if cluster_index == self.master_index:
            return

        bash(tpath('test-rgw-call.sh', 'wait_for_meta_sync', self.master_index + 1, cluster_index + 1, self.realm))



class RGWMulti:
    def __init__(self, num_clusters):
        self.num_clusters = num_clusters

        self.clusters = {}
        for i in xrange(num_clusters):
            self.clusters[i] = RGWCluster(i + 1)

        self.base_port = 8000

    def setup(self):
        credentials = RGWRealmCredentials('1234567890', 'pencil')
        realm = RGWRealm('earth', credentials, 0)

        self.clusters[0].start()
        realm.init_zone(self.clusters[0], 'us', 'us-1', self.base_port)

        for i in xrange(1, self.num_clusters):
            self.clusters[i].start()
            realm.init_zone(self.clusters[i], 'us', 'us-' + str(i + 1), self.base_port + i, first_zone_port=self.base_port)

        for i in xrange(1, self.num_clusters):
            realm.meta_checkpoint(i)


def main():
    rgw_multi = RGWMulti(2)

    rgw_multi.setup()

if __name__ == "__main__":
    main()
