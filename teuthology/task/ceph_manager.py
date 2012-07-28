from cStringIO import StringIO
import random
import time
import re
import gevent
import json

class Thrasher:
    def __init__(self, manager, config, logger=None):
        self.ceph_manager = manager
        self.ceph_manager.wait_for_clean()
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
        # prevent monitor from auto-marking things out while thrasher runs
        manager.raw_cluster_cmd('mon', 'tell', '*', 'injectargs',
                                '--mon-osd-down-out-interval', '0')
        self.thread = gevent.spawn(self.do_thrash)

    def kill_osd(self, osd=None):
        if osd is None:
            osd = random.choice(self.live_osds)
        self.log("Killing osd %s, live_osds are %s"%(str(osd),str(self.live_osds)))
        self.live_osds.remove(osd)
        self.dead_osds.append(osd)
        self.ceph_manager.kill_osd(osd)

    def blackhole_kill_osd(self, osd=None):
        if osd is None:
            osd = random.choice(self.live_osds)
        self.log("Blackholing and then killing osd %s, live_osds are %s"%(str(osd),str(self.live_osds)))
        self.live_osds.remove(osd)
        self.dead_osds.append(osd)
        self.ceph_manager.blackhole_kill_osd(osd)

    def revive_osd(self, osd=None):
        if osd is None:
            osd = random.choice(self.dead_osds)
        self.log("Reviving osd %s"%(str(osd),))
        self.live_osds.append(osd)
        self.dead_osds.remove(osd)
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
        self.thread.get()

    def choose_action(self):
        chance_down = self.config.get("chance_down", 0)
        if isinstance(chance_down, int):
            chance_down = float(chance_down) / 100
        minin = self.config.get("min_in", 2)
        minout = self.config.get("min_out", 0)
        minlive = self.config.get("min_live", 2)
        mindead = self.config.get("min_dead", 0)

        self.log('choose_action: min_in %d min_out %d min_live %d min_dead %d' %
                 (minin,minout,minlive,mindead))
        actions = []
        if len(self.in_osds) > minin:
            actions.append((self.out_osd, 1.0,))
        if len(self.live_osds) > minlive and chance_down > 0:
            actions.append((self.kill_osd, chance_down,))
        if len(self.out_osds) > minout:
            actions.append((self.in_osd, 1.7,))
        if len(self.dead_osds) > mindead:
            actions.append((self.revive_osd, 1.0,))

        total = sum([y for (x,y) in actions])
        val = random.uniform(0, total)
        for (action, prob) in actions:
            if val < prob:
                return action
            val -= prob
        return None

    def do_thrash(self):
        cleanint = self.config.get("clean_interval", 60)
        maxdead = self.config.get("max_dead", 0);
        delay = self.config.get("op_delay", 5)
        self.log("starting do_thrash")
        while not self.stopping:
            self.log(" ".join([str(x) for x in ["in_osds: ", self.in_osds, " out_osds: ", self.out_osds,
                                                "dead_osds: ", self.dead_osds, "live_osds: ",
                                                self.live_osds]]))
            if random.uniform(0,1) < (float(delay) / cleanint):
                while len(self.dead_osds) > maxdead:
                    self.revive_osd()
                self.ceph_manager.wait_for_recovery(
                    timeout=self.config.get('timeout')
                    )
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

    def raw_cluster_cmd(self, *args):
        ceph_args = [
                'LD_LIBRARY_PRELOAD=/tmp/cephtest/binary/usr/local/lib',
                '/tmp/cephtest/enable-coredump',
                '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
                '/tmp/cephtest/archive/coverage',
                '/tmp/cephtest/binary/usr/local/bin/ceph',
                '-k', '/tmp/cephtest/ceph.keyring',
                '-c', '/tmp/cephtest/ceph.conf',
                '--concise',
                ]
        ceph_args.extend(args)
        proc = self.controller.run(
            args=ceph_args,
            stdout=StringIO(),
            )
        return proc.stdout.getvalue()

    def raw_cluster_status(self):
        return self.raw_cluster_cmd('-s')

    def raw_osd_status(self):
        return self.raw_cluster_cmd('osd', 'dump')

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
        self.log(status)
        return int(re.search(
                "\d* pgs:",
                status).group(0).split()[0])

    def list_pg_missing(self, pgid):
        r = None
        offset = {}
        while True:
            out = self.raw_cluster_cmd('--', 'pg',pgid,'list_missing',
                                       json.dumps(offset))
            j = json.loads('\n'.join(out.split('\n')[1:]))
            if r is None:
                r = j
            else:
                r['objects'].extend(j['objects'])
            if not 'more' in j:
                break
            if j['more'] == 0:
                break
            offset = j['objects'][-1]['oid']
        if 'more' in r:
            del r['more']
        return r

    def get_pg_stats(self):
        out = self.raw_cluster_cmd('--', 'pg','dump','--format=json')
        j = json.loads('\n'.join(out.split('\n')[1:]))
        return j['pg_stats']

    def get_osd_dump(self):
        out = self.raw_cluster_cmd('--', 'osd','dump','--format=json')
        j = json.loads('\n'.join(out.split('\n')[1:]))
        return j['osds']

    def get_stuck_pgs(self, type_, threshold):
        out = self.raw_cluster_cmd('--', 'pg','dump_stuck', type_,
                                   '--format=json', '-t', str(threshold))
        return json.loads('\n'.join(out.split('\n')[1:]))

    def get_num_unfound_objects(self):
        status = self.raw_cluster_status()
        self.log(status)
        match = re.search(
            "\d+/\d+ unfound",
            status)
        if match == None:
            return 0
        else:
            return int(match.group(0).split('/')[0])

    def get_num_active_clean(self):
        pgs = self.get_pg_stats()
        num = 0
        for pg in pgs:
            if pg['state'].count('active') and pg['state'].count('clean') and not pg['state'].count('stale'):
                num += 1
        return num

    def get_num_active_recovered(self):
        pgs = self.get_pg_stats()
        num = 0
        for pg in pgs:
            if pg['state'].count('active') and not pg['state'].count('recovering') and not pg['state'].count('stale'):
                num += 1
        return num

    def get_num_active(self):
        pgs = self.get_pg_stats()
        num = 0
        for pg in pgs:
            if pg['state'].count('active') and not pg['state'].count('stale'):
                num += 1
        return num

    def get_num_down(self):
        pgs = self.get_pg_stats()
        num = 0
        for pg in pgs:
            if (pg['state'].count('down') and not pg['state'].count('stale')) or \
                    (pg['state'].count('incomplete') and not pg['state'].count('stale')):
                num += 1
        return num

    def get_num_active_down(self):
        pgs = self.get_pg_stats()
        num = 0
        for pg in pgs:
            if (pg['state'].count('active') and not pg['state'].count('stale')) or \
                    (pg['state'].count('down') and not pg['state'].count('stale')) or \
                    (pg['state'].count('incomplete') and not pg['state'].count('stale')):
                num += 1
        return num

    def is_clean(self):
        return self.get_num_active_clean() == self.get_num_pgs()

    def is_recovered(self):
        return self.get_num_active_recovered() == self.get_num_pgs()

    def is_active_or_down(self):
        return self.get_num_active_down() == self.get_num_pgs()

    def wait_for_clean(self, timeout=None):
        self.log("waiting for clean")
        start = time.time()
        num_active_clean = self.get_num_active_clean()
        while not self.is_clean():
            if timeout is not None:
                assert time.time() - start < timeout, \
                    'failed to become clean before timeout expired'
            cur_active_clean = self.get_num_active_clean()
            if cur_active_clean != num_active_clean:
                start = time.time()
                num_active_clean = cur_active_clean
            time.sleep(3)
        self.log("clean!")

    def wait_for_recovery(self, timeout=None):
        self.log("waiting for recovery to complete")
        start = time.time()
        num_active_recovered = self.get_num_active_recovered()
        while not self.is_recovered():
            if timeout is not None:
                assert time.time() - start < timeout, \
                    'failed to recover before timeout expired'
            cur_active_recovered = self.get_num_active_recovered()
            if cur_active_recovered != num_active_recovered:
                start = time.time()
                num_active_recovered = cur_active_recovered
            time.sleep(3)
        self.log("recovered!")

    def wait_for_active(self, timeout=None):
        self.log("waiting for peering to complete")
        start = time.time()
        num_active = self.get_num_active()
        while not self.is_active():
            if timeout is not None:
                assert time.time() - start < timeout, \
                    'failed to recover before timeout expired'
            cur_active = self.get_num_active()
            if cur_active != num_active:
                start = time.time()
                num_active = cur_active
            time.sleep(3)
        self.log("active!")

    def wait_for_active_or_down(self, timeout=None):
        self.log("waiting for peering to complete or become blocked")
        start = time.time()
        num_active_down = self.get_num_active_down()
        while not self.is_active_or_down():
            if timeout is not None:
                assert time.time() - start < timeout, \
                    'failed to recover before timeout expired'
            cur_active_down = self.get_num_active_down()
            if cur_active_down != num_active_down:
                start = time.time()
                num_active_down = cur_active_down
            time.sleep(3)
        self.log("active or down!")

    def osd_is_up(self, osd):
        osds = self.get_osd_dump()
        return osds[osd]['up'] > 0

    def wait_till_osd_is_up(self, osd, timeout=None):
        self.log('waiting for osd.%d to be up' % osd);
        start = time.time()
        while not self.osd_is_up(osd):
            if timeout is not None:
                assert time.time() - start < timeout, \
                    'osd.%d failed to come up before timeout expired' % osd
            time.sleep(3)
        self.log('osd.%d is up' % osd)

    def is_active(self):
        return self.get_num_active() == self.get_num_pgs()

    def wait_till_active(self, timeout=None):
        self.log("waiting till active")
        start = time.time()
        while not self.is_active():
            if timeout is not None:
                assert time.time() - start < timeout, \
                    'failed to become active before timeout expired'
            time.sleep(3)
        self.log("active!")

    def mark_out_osd(self, osd):
        self.raw_cluster_cmd('osd', 'out', str(osd))

    def kill_osd(self, osd):
        self.ctx.daemons.get_daemon('osd', osd).stop()

    def blackhole_kill_osd(self, osd):
        self.raw_cluster_cmd('--', 'tell', 'osd.%d' % osd,
                             'injectargs', '--filestore-blackhole')
        time.sleep(2)
        self.ctx.daemons.get_daemon('osd', osd).stop()

    def revive_osd(self, osd):
        self.ctx.daemons.get_daemon('osd', osd).restart()

    def mark_down_osd(self, osd):
        self.raw_cluster_cmd('osd', 'down', str(osd))

    def mark_in_osd(self, osd):
        self.raw_cluster_cmd('osd', 'in', str(osd))


    ## monitors

    def kill_mon(self, mon):
        self.ctx.daemons.get_daemon('mon', mon).stop()

    def revive_mon(self, mon):
        self.ctx.daemons.get_daemon('mon', mon).restart()

    def get_mon_status(self, mon):
        addr = self.ctx.ceph.conf['mon.%s' % mon]['mon addr']
        out = self.raw_cluster_cmd('-m', addr, 'mon_status')
        return json.loads(out)

    def get_mon_quorum(self):
        out = self.raw_cluster_cmd('quorum_status')
        j = json.loads(out)
        self.log('quorum_status is %s' % out)
        return j['quorum']

    def wait_for_mon_quorum_size(self, size, timeout=300):
        self.log('waiting for quorum size %d' % size)
        start = time.time()
        while not len(self.get_mon_quorum()) == size:
            if timeout is not None:
                assert time.time() - start < timeout, \
                    'failed to reach quorum size %d before timeout expired' % size
            time.sleep(3)
        self.log("quorum is size %d" % size)
