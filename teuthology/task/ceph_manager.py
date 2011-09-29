import random
import time
import re
import gevent
from ..orchestra import run

class Thrasher(gevent.Greenlet):
    def __init__(self, manager, config, logger=None):
        self.ceph_manager = manager
        self.ceph_manager.wait_till_clean()
        osd_status = self.ceph_manager.get_osd_status()
        self.in_osds = osd_status['in']
        self.live_osds = osd_status['live']
        self.out_osds = osd_status['out']
        self.dead_osds = osd_status['dead']
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

    def kill_osd(self, osd=None):
        if osd is None:
            osd = random.choice(self.live_osds)
        self.log("Killing osd %s, live_osds are %s"%(str(osd),str(self.live_osds)))
        self.live_osds.remove(osd)
        self.dead_osds.append(osd)
        if osd in self.in_osds:
            self.in_osds.remove(osd)
            self.out_osds.append(osd)
        self.ceph_manager.kill_osd(osd)

    def revive_osd(self, osd=None):
        if osd is None:
            osd = random.choice(self.dead_osds)
        self.log("Reviving osd %s"%(str(osd),))
        self.live_osds.append(osd)
        self.dead_osds.remove(osd)
        if osd in self.out_osds:
            self.in_osds.append(osd)
            self.out_osds.remove(osd)
        self.ceph_manager.revive_osd(osd)

    def out_osd(self, osd=None):
        if osd is None:
            osd = random.choice(self.in_osds)
        self.log("Removing osd %s, in_osds are: %s"%(str(osd),str(self.in_osds)))
        self.ceph_manager.mark_out_osd(osd)
        self.in_osds.remove(osd)
        self.out_osds.append(osd)

    def in_osd(self, osd=None):
        if osd is None:
            osd = random.choice(self.out_osds)
        if osd in self.dead_osds:
            return self.revive_osd(osd)
        self.log("Adding osd %s"%(str(osd),))
        self.out_osds.remove(osd)
        self.in_osds.append(osd)
        self.ceph_manager.mark_in_osd(osd)

    def all_up(self):
        while len(self.dead_osds) > 0:
            self.revive_osd()
        while len(self.out_osds) > 0:
            self.in_osd()

    def do_join(self):
        self.stopping = True
        self.get()

    def choose_action(self):
        chance_down = self.config.get("chance_down", 0)
        if isinstance(chance_down, int):
            chance_down = float(chance_down) / 100
        minin = self.config.get("min_in", 2)
        minout = self.config.get("min_out", 0)
        minlive = self.config.get("min_live", 2)
        mindead = self.config.get("min_dead", 2)

        actions = []
        if len(self.in_osds) > minin:
            actions.append((self.out_osd, 1.0,))
            if len(self.live_osds) > minlive and chance_down > 0:
                actions.append((self.kill_osd, chance_down))
        if len(self.out_osds) > minout:
            actions.append((self.in_osd, 1.0,))
            if len(self.dead_osds) > mindead:
                actions.append((self.revive_osd, 1.0))

        total = sum([y for (x,y) in actions])
        rev_cum = reduce(lambda l,(y1,y2): l+[(y1, (l[-1][1]+y2)/total)], actions, [(0, 0)])[1:]
        rev_cum.reverse()
        final_rev_cum = [(x, y-rev_cum[-1][1]) for (x,y) in rev_cum]
        val = random.uniform(0, 1)
        for (action, prob) in final_rev_cum:
            if (prob < val):
                return action
        return None

    def do_thrash(self):
        cleanint = self.config.get("clean_interval", 60)
        delay = self.config.get("op_delay", 5)
        self.log("starting do_thrash")
        while not self.stopping:
            self.log(" ".join([str(x) for x in ["in_osds: ", self.in_osds, " out_osds: ", self.out_osds,
                                                "dead_osds: ", self.dead_osds, "live_osds: ",
                                                self.live_osds]]))
            if random.uniform(0,1) < (float(delay) / cleanint):
                self.ceph_manager.wait_till_clean()
            self.choose_action()()
            time.sleep(delay)
        self.all_up()

class CephManager:
    def __init__(self, controller, ctx=None, logger=None):
        self.ctx = ctx
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
            lambda x: x.startswith('osd.') and (("up" in x) or ("down" in x)),
            self.raw_osd_status().split('\n'))
        self.log(osd_lines)
        in_osds = [int(i[4:].split()[0]) for i in filter(
                lambda x: " in " in x,
                osd_lines)]
        out_osds = [int(i[4:].split()[0]) for i in filter(
                lambda x: " out " in x,
                osd_lines)]
        up_osds = [int(i[4:].split()[0]) for i in filter(
                lambda x: " up " in x,
                osd_lines)]
        down_osds = [int(i[4:].split()[0]) for i in filter(
                lambda x: " down " in x,
                osd_lines)]
        dead_osds = [int(x.id_) for x in
                     filter(lambda x: not x.running(), self.ctx.daemons.iter_daemons_of_role('osd'))]
        live_osds = [int(x.id_) for x in
                     filter(lambda x: x.running(), self.ctx.daemons.iter_daemons_of_role('osd'))]
        return { 'in' : in_osds, 'out' : out_osds, 'up' : up_osds,
                 'down' : down_osds, 'dead' : dead_osds, 'live' : live_osds, 'raw' : osd_lines }

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

    def kill_osd(self, osd):
        self.ctx.daemons.get_daemon('osd', osd).stop()

    def revive_osd(self, osd):
        self.ctx.daemons.get_daemon('osd', osd).restart()

    def mark_down_osd(self, osd):
        self.raw_cluster_cmd("osd down %s"%(str(osd,)))

    def mark_in_osd(self, osd):
        self.raw_cluster_cmd("osd in %s"%(str(osd,)))
