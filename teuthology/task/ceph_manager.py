import random
import time
import re
import gevent
from orchestra import run

class Thrasher(gevent.Greenlet):
    def __init__(self, manager, config, logger=None):
        self.ceph_manager = manager
        self.ceph_manager.wait_till_clean()
        osd_status = self.ceph_manager.get_osd_status()
        self.in_osds = osd_status['in']
        self.out_osds = osd_status['out']
        self.stopping = False
        self.logger = logger
        self.config = config
        if self.logger is not None:
            self.log = lambda x: self.logger.info(x)
        else:
            def tmp(x):
                print x
            self.log = tmp
        if self.config is None:
            self.config = dict()
        gevent.Greenlet.__init__(self, self.do_thrash)
        self.start()

    def remove_osd(self):
        chance_down = self.config.get("chance_down", 0)
        if isinstance(chance_down, int):
            chance_down = float(chance_down) / 100
        osd = random.choice(self.in_osds)
        if random.uniform(0,1) < chance_down:
            self.log("Marking osd {id_} down".format(id_=osd))
            self.ceph_manager.mark_down_osd(osd)
        else:
            self.log("Removing osd %s"%(str(osd),))
            self.ceph_manager.mark_out_osd(osd)
            self.in_osds.remove(osd)
            self.out_osds.append(osd)


    def add_osd(self):
        osd = random.choice(self.out_osds)
        self.log("Adding osd %s"%(str(osd),))
        self.out_osds.remove(osd)
        self.in_osds.append(osd)
        self.ceph_manager.mark_in_osd(osd)

    def all_up(self):
        while len(self.out_osds) > 0:
            self.add_osd()

    def do_join(self):
        self.stopping = True
        self.get()

    def do_thrash(self):
        cleanint = self.config.get("clean_interval", 60)
        delay = self.config.get("op_delay", 5)
        minin = self.config.get("min_in", 2)
        minout = self.config.get("min_out", 0)
        self.log("starting do_thrash")
        while not self.stopping:
            self.log(" ".join([str(x) for x in ["in_osds: ", self.in_osds, " out_osds: ", self.out_osds]]))
            if random.uniform(0,1) < (float(delay) / cleanint):
                self.ceph_manager.wait_till_clean()
            if (len(self.out_osds) == minout):
                self.remove_osd()
            elif (len(self.in_osds) <= minin):
                self.add_osd()
            else:
                x = random.choice([self.remove_osd, self.add_osd])
                x()
            time.sleep(delay)
        self.all_up()

class CephManager:
    def __init__(self, controller, logger=None):
        self.controller = controller
        if (logger):
            self.log = lambda x: logger.info(x)
        else:
            def tmp(x):
                print x
            self.log = tmp

    def raw_cluster_cmd(self, suffix):
        proc = self.controller.run(
            args=[
                "/bin/sh", "-c",
                " ".join([
                        "LD_LIBRARY_PRELOAD=/tmp/cephtest/binary/usr/local/lib",
                        '/tmp/cephtest/enable-coredump',
                        '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
                        '/tmp/cephtest/archive/coverage',
                        "/tmp/cephtest/binary/usr/local/bin/ceph -k /tmp/cephtest/ceph.keyring -c "+\
                            "/tmp/cephtest/ceph.conf " + suffix
                        ])
                ],
            stdout=run.PIPE,
            wait=False
            )

        out = ""
        tmp = proc.stdout.read(1)
        while tmp:
            out += tmp
            tmp = proc.stdout.read(1)
        return out

    def raw_cluster_status(self):
        return self.raw_cluster_cmd("-s")

    def raw_osd_status(self):
        return self.raw_cluster_cmd("osd dump -o -")

    def raw_pg_status(self):
        return self.controller.do_ssh("pg dump -o -")

    def get_osd_status(self):
        osd_lines = filter(
            lambda x: x[:3] == 'osd' and (("up" in x) or ("down" in x)),
            self.raw_osd_status().split('\n'))
        self.log(osd_lines)
        in_osds = [int(i[3:].split()[0]) for i in filter(
                lambda x: " in " in x,
                osd_lines)]
        out_osds = [int(i[3:].split()[0]) for i in filter(
                lambda x: " out " in x,
                osd_lines)]
        up_osds = [int(i[3:].split()[0]) for i in filter(
                lambda x: " up " in x,
                osd_lines)]
        down_osds = [int(i[3:].split()[0]) for i in filter(
                lambda x: " down " in x,
                osd_lines)]
        return { 'in' : in_osds, 'out' : out_osds, 'up' : up_osds,
                 'down' : down_osds, 'raw' : osd_lines }

    def get_num_pgs(self):
        status = self.raw_cluster_status()
        return int(re.search(
                "\d* pgs:",
                status).group(0).split()[0])

    def get_num_active_clean(self):
        status = self.raw_cluster_status()
        self.log(status)
        match = re.search(
            "\d* active.clean",
            status)
        if match == None:
            return 0
        else:
            return int(match.group(0).split()[0])

    def is_clean(self):
        return self.get_num_active_clean() == self.get_num_pgs()

    def wait_till_clean(self, timeout=None):
        self.log("waiting till clean")
        start = time.time()
        while not self.is_clean():
            if timeout is not None:
                assert time.time() - start < timeout, \
                    'failed to become clean before timeout expired'
            time.sleep(3)
        self.log("clean!")

    def mark_out_osd(self, osd):
        self.raw_cluster_cmd("osd out %s"%(str(osd,)))

    def mark_down_osd(self, osd):
        self.raw_cluster_cmd("osd down %s"%(str(osd,)))

    def mark_in_osd(self, osd):
        self.raw_cluster_cmd("osd in %s"%(str(osd,)))
