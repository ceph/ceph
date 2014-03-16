"""
ceph manager -- Thrasher and CephManager objects
"""
from cStringIO import StringIO
import random
import time
import gevent
import json
import threading
from teuthology import misc as teuthology
from teuthology.task import ceph as ceph_task

class Thrasher:
    """
    Object used to thrash Ceph
    """
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
        self.revive_timeout = self.config.get("revive_timeout", 75)
        if self.config.get('powercycle'):
            self.revive_timeout += 120
        self.clean_wait = self.config.get('clean_wait', 0)
        self.minin = self.config.get("min_in", 3)

        num_osds = self.in_osds + self.out_osds
        self.max_pgs = self.config.get("max_pgs_per_pool_osd", 1200) * num_osds
        if self.logger is not None:
            self.log = lambda x: self.logger.info(x)
        else:
            def tmp(x):
                """
                Implement log behavior
                """
                print x
            self.log = tmp
        if self.config is None:
            self.config = dict()
        # prevent monitor from auto-marking things out while thrasher runs
        # try both old and new tell syntax, in case we are testing old code
        try:
            manager.raw_cluster_cmd('--', 'tell', 'mon.*', 'injectargs',
                                    '--mon-osd-down-out-interval 0')
        except Exception:
            manager.raw_cluster_cmd('--', 'mon', 'tell', '*', 'injectargs',
                                    '--mon-osd-down-out-interval 0')
        self.thread = gevent.spawn(self.do_thrash)

    def kill_osd(self, osd=None, mark_down=False, mark_out=False):
        """
        :param osd: Osd to be killed.
        :mark_down: Mark down if true.
        :mark_out: Mark out if true.
        """
        if osd is None:
            osd = random.choice(self.live_osds)
        self.log("Killing osd %s, live_osds are %s" % (str(osd), str(self.live_osds)))
        self.live_osds.remove(osd)
        self.dead_osds.append(osd)
        self.ceph_manager.kill_osd(osd)
        if mark_down:
            self.ceph_manager.mark_down_osd(osd)
        if mark_out and osd in self.in_osds:
            self.out_osd(osd)

    def blackhole_kill_osd(self, osd=None):
        """
        If all else fails, kill the osd.
        :param osd: Osd to be killed.
        """
        if osd is None:
            osd = random.choice(self.live_osds)
        self.log("Blackholing and then killing osd %s, live_osds are %s" % (str(osd), str(self.live_osds)))
        self.live_osds.remove(osd)
        self.dead_osds.append(osd)
        self.ceph_manager.blackhole_kill_osd(osd)

    def revive_osd(self, osd=None):
        """
        Revive the osd.
        :param osd: Osd to be revived.
        """
        if osd is None:
            osd = random.choice(self.dead_osds)
        self.log("Reviving osd %s" % (str(osd),))
        self.live_osds.append(osd)
        self.dead_osds.remove(osd)
        self.ceph_manager.revive_osd(osd, self.revive_timeout)

    def out_osd(self, osd=None):
        """
        Mark the osd out
        :param osd: Osd to be marked.
        """
        if osd is None:
            osd = random.choice(self.in_osds)
        self.log("Removing osd %s, in_osds are: %s" % (str(osd), str(self.in_osds)))
        self.ceph_manager.mark_out_osd(osd)
        self.in_osds.remove(osd)
        self.out_osds.append(osd)

    def in_osd(self, osd=None):
        """
        Mark the osd out
        :param osd: Osd to be marked.
        """
        if osd is None:
            osd = random.choice(self.out_osds)
        if osd in self.dead_osds:
            return self.revive_osd(osd)
        self.log("Adding osd %s" % (str(osd),))
        self.out_osds.remove(osd)
        self.in_osds.append(osd)
        self.ceph_manager.mark_in_osd(osd)
        self.log("Added osd %s"%(str(osd),))

    def primary_affinity(self, osd=None):
        if osd is None:
            osd = random.choice(self.in_osds)
        if random.random() >= .5:
            pa = random.random()
        elif random.random() >= .5:
            pa = 1
        else:
            pa = 0
        self.log('Setting osd %s primary_affinity to %f' % (str(osd), pa))
        self.ceph_manager.raw_cluster_cmd('osd', 'primary-affinity', str(osd), str(pa))

    def all_up(self):
        """
        Make sure all osds are up and not out.
        """
        while len(self.dead_osds) > 0:
            self.log("reviving osd")
            self.revive_osd()
        while len(self.out_osds) > 0:
            self.log("inning osd")
            self.in_osd()

    def do_join(self):
        """
        Break out of this Ceph loop
        """
        self.stopping = True
        self.thread.get()

    def grow_pool(self):
        """
        Increase the size of the pool
        """
        pool = self.ceph_manager.get_pool()
        self.log("Growing pool %s"%(pool,))
        self.ceph_manager.expand_pool(pool, self.config.get('pool_grow_by', 10), self.max_pgs)

    def fix_pgp_num(self):
        """
        Fix number of pgs in pool.
        """
        pool = self.ceph_manager.get_pool()
        self.log("fixing pg num pool %s"%(pool,))
        self.ceph_manager.set_pool_pgpnum(pool)

    def test_pool_min_size(self):
        """
        Kill and revive all osds except one.
        """
        self.log("test_pool_min_size")
        self.all_up()
        self.ceph_manager.wait_for_recovery(
            timeout=self.config.get('timeout')
            )
        the_one = random.choice(self.in_osds)
        self.log("Killing everyone but %s", the_one)
        to_kill = filter(lambda x: x != the_one, self.in_osds)
        [self.kill_osd(i) for i in to_kill]
        [self.out_osd(i) for i in to_kill]
        time.sleep(self.config.get("test_pool_min_size_time", 10))
        self.log("Killing %s" % (the_one,))
        self.kill_osd(the_one)
        self.out_osd(the_one)
        self.log("Reviving everyone but %s" % (the_one,))
        [self.revive_osd(i) for i in to_kill]
        [self.in_osd(i) for i in to_kill]
        self.log("Revived everyone but %s" % (the_one,))
        self.log("Waiting for clean")
        self.ceph_manager.wait_for_recovery(
            timeout=self.config.get('timeout')
            )

    def inject_pause(self, conf_key, duration, check_after, should_be_down):
        """
        Pause injection testing. Check for osd being down when finished.
        """
        the_one = random.choice(self.live_osds)
        self.log("inject_pause on {osd}".format(osd = the_one))
        self.log(
            "Testing {key} pause injection for duration {duration}".format(
                key = conf_key,
                duration = duration
                ))
        self.log(
            "Checking after {after}, should_be_down={shouldbedown}".format(
                after = check_after,
                shouldbedown = should_be_down
                ))
        self.ceph_manager.set_config(the_one, **{conf_key:duration})
        if not should_be_down:
            return
        time.sleep(check_after)
        status = self.ceph_manager.get_osd_status()
        assert the_one in status['down']
        time.sleep(duration - check_after + 20)
        status = self.ceph_manager.get_osd_status()
        assert not the_one in status['down']

    def test_backfill_full(self):
        """
        Test backfills stopping when the replica fills up.

        First, use osd_backfill_full_ratio to simulate a now full
        osd by setting it to 0 on all of the OSDs.

        Second, on a random subset, set
        osd_debug_skip_full_check_in_backfill_reservation to force
        the more complicated check in do_scan to be exercised.

        Then, verify that all backfills stop.
        """
        self.log("injecting osd_backfill_full_ratio = 0")
        for i in self.live_osds:
            self.ceph_manager.set_config(
                i,
                osd_debug_skip_full_check_in_backfill_reservation = random.choice(
                    ['false', 'true']),
                osd_backfill_full_ratio = 0)
        for i in range(30):
            status = self.ceph_manager.compile_pg_status()
            if 'backfill' not in status.keys():
                break
            self.log(
                "waiting for {still_going} backfills".format(
                    still_going=status.get('backfill')))
            time.sleep(1)
        assert('backfill' not in self.ceph_manager.compile_pg_status().keys())
        for i in self.live_osds:
            self.ceph_manager.set_config(
                i,
                osd_debug_skip_full_check_in_backfill_reservation = \
                    'false',
                osd_backfill_full_ratio = 0.85)

    def test_map_discontinuity(self):
        """
        1) Allows the osds to recover
        2) kills an osd
        3) allows the remaining osds to recover
        4) waits for some time
        5) revives the osd
        This sequence should cause the revived osd to have to handle
        a map gap since the mons would have trimmed
        """
        while len(self.in_osds) < (self.minin + 1):
            self.in_osd()
        self.log("Waiting for recovery")
        self.ceph_manager.wait_for_all_up(
            timeout=self.config.get('timeout')
            )
        # now we wait 20s for the pg status to change, if it takes longer,
        # the test *should* fail!
        time.sleep(20)
        self.ceph_manager.wait_for_clean(
            timeout=self.config.get('timeout')
            )

        # now we wait 20s for the backfill replicas to hear about the clean
        time.sleep(20)
        self.log("Recovered, killing an osd")
        self.kill_osd(mark_down=True, mark_out=True)
        self.log("Waiting for clean again")
        self.ceph_manager.wait_for_clean(
            timeout=self.config.get('timeout')
            )
        self.log("Waiting for trim")
        time.sleep(int(self.config.get("map_discontinuity_sleep_time", 40)))
        self.revive_osd()

    def choose_action(self):
        """
        Random action selector.
        """
        chance_down = self.config.get('chance_down', 0.4)
        chance_test_min_size = self.config.get('chance_test_min_size', 0)
        chance_test_backfill_full = self.config.get('chance_test_backfill_full', 0)
        if isinstance(chance_down, int):
            chance_down = float(chance_down) / 100
        minin = self.minin
        minout = self.config.get("min_out", 0)
        minlive = self.config.get("min_live", 2)
        mindead = self.config.get("min_dead", 0)

        self.log('choose_action: min_in %d min_out %d min_live %d min_dead %d' %
                 (minin, minout, minlive, mindead))
        actions = []
        if len(self.in_osds) > minin:
            actions.append((self.out_osd, 1.0,))
        if len(self.live_osds) > minlive and chance_down > 0:
            actions.append((self.kill_osd, chance_down,))
        if len(self.out_osds) > minout:
            actions.append((self.in_osd, 1.7,))
        if len(self.dead_osds) > mindead:
            actions.append((self.revive_osd, 1.0,))
        if self.config.get('thrash_primary_affinity', True):
            actions.append((self.primary_affinity, 1.0,))
        actions.append((self.grow_pool, self.config.get('chance_pgnum_grow', 0),))
        actions.append((self.fix_pgp_num, self.config.get('chance_pgpnum_fix', 0),))
        actions.append((self.test_pool_min_size, chance_test_min_size,))
        actions.append((self.test_backfill_full, chance_test_backfill_full,))
        for key in ['heartbeat_inject_failure', 'filestore_inject_stall']:
            for scenario in [
                (lambda: self.inject_pause(key,
                                           self.config.get('pause_short', 3),
                                           0,
                                           False),
                 self.config.get('chance_inject_pause_short', 1),),
                (lambda: self.inject_pause(key,
                                           self.config.get('pause_long', 80),
                                           self.config.get('pause_check_after', 70),
                                           True),
                 self.config.get('chance_inject_pause_long', 0),)]:
                actions.append(scenario)

        total = sum([y for (x, y) in actions])
        val = random.uniform(0, total)
        for (action, prob) in actions:
            if val < prob:
                return action
            val -= prob
        return None

    def do_thrash(self):
        """
        Loop to select random actions to thrash ceph manager with.
        """
        cleanint = self.config.get("clean_interval", 60)
        maxdead = self.config.get("max_dead", 0)
        delay = self.config.get("op_delay", 5)
        self.log("starting do_thrash")
        while not self.stopping:
            self.log(" ".join([str(x) for x in ["in_osds: ", self.in_osds, " out_osds: ", self.out_osds,
                                                "dead_osds: ", self.dead_osds, "live_osds: ",
                                                self.live_osds]]))
            if random.uniform(0, 1) < (float(delay) / cleanint):
                while len(self.dead_osds) > maxdead:
                    self.revive_osd()
                if random.uniform(0, 1) < float(
                    self.config.get('chance_test_map_discontinuity', 0)):
                    self.test_map_discontinuity()
                else:
                    self.ceph_manager.wait_for_recovery(
                        timeout=self.config.get('timeout')
                        )
                time.sleep(self.clean_wait)
            self.choose_action()()
            time.sleep(delay)
        self.all_up()

class CephManager:
    """
    Ceph manager object.
    Contains several local functions that form a bulk of this module.
    """
    def __init__(self, controller, ctx=None, config=None, logger=None):
        self.lock = threading.RLock()
        self.ctx = ctx
        self.config = config
        self.controller = controller
        self.next_pool_id = 0
        self.created_erasure_pool = False
        if (logger):
            self.log = lambda x: logger.info(x)
        else:
            def tmp(x):
                """
                implement log behavior.
                """
                print x
            self.log = tmp
        if self.config is None:
            self.config = dict()
        pools = self.list_pools()
        self.pools = {}
        for pool in pools:
            self.pools[pool] = self.get_pool_property(pool, 'pg_num')

    def raw_cluster_cmd(self, *args):
        """
        Start ceph on a raw cluster.  Return count
        """
        testdir = teuthology.get_testdir(self.ctx)
        ceph_args = [
                'adjust-ulimits',
                'ceph-coverage',
                '{tdir}/archive/coverage'.format(tdir=testdir),
                'ceph',
                ]
        ceph_args.extend(args)
        proc = self.controller.run(
            args=ceph_args,
            stdout=StringIO(),
            )
        return proc.stdout.getvalue()

    def raw_cluster_cmd_result(self, *args):
        """
        Start ceph on a cluster.  Return success or failure information.
        """
        testdir = teuthology.get_testdir(self.ctx)
        ceph_args = [
                'adjust-ulimits',
                'ceph-coverage',
                '{tdir}/archive/coverage'.format(tdir=testdir),
                'ceph',
                ]
        ceph_args.extend(args)
        proc = self.controller.run(
            args=ceph_args,
            check_status=False,
            )
        return proc.exitstatus

    def do_rados(self, remote, cmd):
        """
        Execute a remote rados command.
        """
        testdir = teuthology.get_testdir(self.ctx)
        pre = [
            'adjust-ulimits',
            'ceph-coverage',
            '{tdir}/archive/coverage'.format(tdir=testdir),
            'rados',
            ]
        pre.extend(cmd)
        proc = remote.run(
            args=pre,
            wait=True,
            )
        return proc

    def rados_write_objects(
        self, pool, num_objects, size, timelimit, threads, cleanup=False):
        """
        Write rados objects
        Threads not used yet.
        """
        args = [
            '-p', pool,
            '--num-objects', num_objects,
            '-b', size,
            'bench', timelimit,
            'write'
            ]
        if not cleanup: args.append('--no-cleanup')
        return self.do_rados(self.controller, map(str, args))

    def do_put(self, pool, obj, fname):
        """
        Implement rados put operation
        """
        return self.do_rados(
            self.controller,
            [
                '-p',
                pool,
                'put',
                obj,
                fname
                ]
            )

    def do_get(self, pool, obj, fname='/dev/null'):
        """
        Implement rados get operation
        """
        return self.do_rados(
            self.controller,
            [
                '-p',
                pool,
                'stat',
                obj,
                fname
                ]
            )

    def osd_admin_socket(self, osdnum, command, check_status=True):
        """
        Remotely start up ceph specifying the admin socket
        """
        testdir = teuthology.get_testdir(self.ctx)
        remote = None
        for _remote, roles_for_host in self.ctx.cluster.remotes.iteritems():
            for id_ in teuthology.roles_of_type(roles_for_host, 'osd'):
                if int(id_) == int(osdnum):
                    remote = _remote
        assert remote is not None
        args = [
            'sudo',
            'adjust-ulimits',
            'ceph-coverage',
            '{tdir}/archive/coverage'.format(tdir=testdir),
            'ceph',
            '--admin-daemon',
            '/var/run/ceph/ceph-osd.{id}.asok'.format(id=osdnum),
            ]
        args.extend(command)
        return remote.run(
            args=args,
            stdout=StringIO(),
            wait=True,
            check_status=check_status
            )

    def get_pgid(self, pool, pgnum):
        """
        :param pool: pool number
        :param pgnum: pg number
        :returns: a string representing this pg.
        """
        poolnum = self.get_pool_num(pool)
        pg_str = "{poolnum}.{pgnum}".format(
            poolnum=poolnum,
            pgnum=pgnum)
        return pg_str

    def get_pg_replica(self, pool, pgnum):
        """
        get replica for pool, pgnum (e.g. (data, 0)->0
        """
        output = self.raw_cluster_cmd("pg", "dump", '--format=json')
        j = json.loads('\n'.join(output.split('\n')[1:]))
        pg_str = self.get_pgid(pool, pgnum)
        for pg in j['pg_stats']:
            if pg['pgid'] == pg_str:
                return int(pg['acting'][-1])
        assert False

    def get_pg_primary(self, pool, pgnum):
        """
        get primary for pool, pgnum (e.g. (data, 0)->0
        """
        output = self.raw_cluster_cmd("pg", "dump", '--format=json')
        j = json.loads('\n'.join(output.split('\n')[1:]))
        pg_str = self.get_pgid(pool, pgnum)
        for pg in j['pg_stats']:
            if pg['pgid'] == pg_str:
                return int(pg['acting'][0])
        assert False

    def get_pool_num(self, pool):
        """
        get number for pool (e.g., data -> 2)
        """
        out = self.raw_cluster_cmd('osd', 'dump', '--format=json')
        j = json.loads('\n'.join(out.split('\n')[1:]))
        for i in j['pools']:
            if i['pool_name'] == pool:
                return int(i['pool'])
        assert False

    def list_pools(self):
        """
        list all pool names
        """
        out = self.raw_cluster_cmd('osd', 'dump', '--format=json')
        j = json.loads('\n'.join(out.split('\n')[1:]))
        self.log(j['pools'])
        return [str(i['pool_name']) for i in j['pools']]

    def clear_pools(self):
        """
        remove all pools
        """
        [self.remove_pool(i) for i in self.list_pools()]

    def kick_recovery_wq(self, osdnum):
        """
        Run kick_recovery_wq on cluster.
        """
        return self.raw_cluster_cmd(
            'tell', "osd.%d" % (int(osdnum),),
            'debug',
            'kick_recovery_wq',
            '0')

    def wait_run_admin_socket(self, osdnum, args=['version'], timeout=75):
        """
        If osd_admin_socket call suceeds, return.  Otherwise wait
        five seconds and try again.
        """
        tries = 0
        while True:
            proc = self.osd_admin_socket(
                osdnum, args,
                check_status=False)
            if proc.exitstatus is 0:
                break
            else:
                tries += 1
                if (tries * 5) > timeout:
                    raise Exception('timed out waiting for admin_socket to appear after osd.{o} restart'.format(o=osdnum))
                self.log(
                    "waiting on admin_socket for {osdnum}, {command}".format(
                        osdnum=osdnum,
                        command=args))
                time.sleep(5)

    def set_config(self, osdnum, **argdict):
        """
        :param osdnum: osd number
        :param argdict: dictionary containing values to set.
        """
        for k, v in argdict.iteritems():
            self.wait_run_admin_socket(
                osdnum,
                ['config', 'set', str(k), str(v)])

    def raw_cluster_status(self):
        """
        Get status from cluster
        """
        status = self.raw_cluster_cmd('status', '--format=json-pretty')
        return json.loads(status)

    def raw_osd_status(self):
        """
        Get osd status from cluster
        """
        return self.raw_cluster_cmd('osd', 'dump')

    def get_osd_status(self):
        """
        Get osd statuses sorted by states that the osds are in.
        """
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
                 'down' : down_osds, 'dead' : dead_osds, 'live' : live_osds,
                 'raw' : osd_lines}

    def get_num_pgs(self):
        """
        Check cluster status for the number of pgs
        """
        status = self.raw_cluster_status()
        self.log(status)
        return status['pgmap']['num_pgs']

    def create_pool_with_unique_name(self, pg_num=1, ec_pool=False):
        """
        Create a pool named unique_pool_X where X is unique.
        """
        name = ""
        with self.lock:
            name = "unique_pool_%s" % (str(self.next_pool_id),)
            self.next_pool_id += 1
            self.create_pool(name, pg_num, ec_pool=ec_pool)
        return name

    def create_pool(self, pool_name, pg_num=1, ec_pool=False):
        """
        Create a pool named from the pool_name parameter.
        :param pool_name: name of the pool being created.
        :param pg_num: initial number of pgs.
        """
        with self.lock:
            assert isinstance(pool_name, str)
            assert isinstance(pg_num, int)
            assert pool_name not in self.pools
            self.log("creating pool_name %s"%(pool_name,))
            if ec_pool and not self.created_erasure_pool:
                self.created_erasure_pool = True
                self.raw_cluster_cmd('osd', 'erasure-code-profile', 'set', 'teuthologyprofile', 'ruleset-failure-domain=osd', 'm=1', 'k=2')

            if ec_pool:
                self.raw_cluster_cmd('osd', 'pool', 'create', pool_name, str(pg_num), str(pg_num), 'erasure', 'teuthologyprofile')
            else:
                self.raw_cluster_cmd('osd', 'pool', 'create', pool_name, str(pg_num))
            self.pools[pool_name] = pg_num

    def remove_pool(self, pool_name):
        """
        Remove the indicated pool
        :param pool_name: Pool to be removed
        """
        with self.lock:
            assert isinstance(pool_name, str)
            assert pool_name in self.pools
            self.log("removing pool_name %s" % (pool_name,))
            del self.pools[pool_name]
            self.do_rados(
                self.controller,
                ['rmpool', pool_name, pool_name, "--yes-i-really-really-mean-it"]
                )

    def get_pool(self):
        """
        Pick a random pool
        """
        with self.lock:
            return random.choice(self.pools.keys())

    def get_pool_pg_num(self, pool_name):
        """
        Return the number of pgs in the pool specified.
        """
        with self.lock:
            assert isinstance(pool_name, str)
            if pool_name in self.pools:
                return self.pools[pool_name]
            return 0

    def get_pool_property(self, pool_name, prop):
        """
        :param pool_name: pool
        :param prop: property to be checked.
        :returns: property as an int value.
        """
        with self.lock:
            assert isinstance(pool_name, str)
            assert isinstance(prop, str)
            output = self.raw_cluster_cmd(
                'osd',
                'pool',
                'get',
                pool_name,
                prop)
            return int(output.split()[1])

    def set_pool_property(self, pool_name, prop, val):
        """
        :param pool_name: pool
        :param prop: property to be set.
        :param val: value to set.

        This routine retries if set operation fails.
        """
        with self.lock:
            assert isinstance(pool_name, str)
            assert isinstance(prop, str)
            assert isinstance(val, int)
            tries = 0
            while True:
                r = self.raw_cluster_cmd_result(
                    'osd',
                    'pool',
                    'set',
                    pool_name,
                    prop,
                    str(val))
                if r != 11: # EAGAIN
                    break
                tries += 1
                if tries > 50:
                    raise Exception('timed out getting EAGAIN when setting pool property %s %s = %s' % (pool_name, prop, val))
                self.log('got EAGAIN setting pool property, waiting a few seconds...')
                time.sleep(2)

    def expand_pool(self, pool_name, by, max_pgs):
        """
        Increase the number of pgs in a pool
        """
        with self.lock:
            assert isinstance(pool_name, str)
            assert isinstance(by, int)
            assert pool_name in self.pools
            if self.get_num_creating() > 0:
                return
            if (self.pools[pool_name] + by) > max_pgs:
                return
            self.log("increase pool size by %d"%(by,))
            new_pg_num = self.pools[pool_name] + by
            self.set_pool_property(pool_name, "pg_num", new_pg_num)
            self.pools[pool_name] = new_pg_num

    def set_pool_pgpnum(self, pool_name):
        """
        Set pgpnum property of pool_name pool.
        """
        with self.lock:
            assert isinstance(pool_name, str)
            assert pool_name in self.pools
            if self.get_num_creating() > 0:
                return
            self.set_pool_property(pool_name, 'pgp_num', self.pools[pool_name])

    def list_pg_missing(self, pgid):
        """
        return list of missing pgs with the id specified
        """
        r = None
        offset = {}
        while True:
            out = self.raw_cluster_cmd('--', 'pg', pgid, 'list_missing',
                                       json.dumps(offset))
            j = json.loads(out)
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
        """
        Dump the cluster and get pg stats
        """
        out = self.raw_cluster_cmd('pg', 'dump', '--format=json')
        j = json.loads('\n'.join(out.split('\n')[1:]))
        return j['pg_stats']

    def compile_pg_status(self):
        """
        Return a histogram of pg state values
        """
        ret = {}
        j = self.get_pg_stats()
        for pg in j:
            for status in pg['state'].split('+'):
                if status not in ret:
                    ret[status] = 0
                ret[status] += 1
        return ret

    def pg_scrubbing(self, pool, pgnum):
        """
        pg scrubbing wrapper
        """
        pgstr = self.get_pgid(pool, pgnum)
        stats = self.get_single_pg_stats(pgstr)
        return 'scrub' in stats['state']

    def pg_repairing(self, pool, pgnum):
        """
        pg repairing wrapper
        """
        pgstr = self.get_pgid(pool, pgnum)
        stats = self.get_single_pg_stats(pgstr)
        return 'repair' in stats['state']

    def pg_inconsistent(self, pool, pgnum):
        """
        pg inconsistent wrapper
        """
        pgstr = self.get_pgid(pool, pgnum)
        stats = self.get_single_pg_stats(pgstr)
        return 'inconsistent' in stats['state']

    def get_last_scrub_stamp(self, pool, pgnum):
        """
        Get the timestamp of the last scrub.
        """
        stats = self.get_single_pg_stats(self.get_pgid(pool, pgnum))
        return stats["last_scrub_stamp"]

    def do_pg_scrub(self, pool, pgnum, stype):
        """
        Scrub pg and wait for scrubbing to finish
        """
        init = self.get_last_scrub_stamp(pool, pgnum)
        self.raw_cluster_cmd('pg', stype, self.get_pgid(pool, pgnum))
        while init == self.get_last_scrub_stamp(pool, pgnum):
            self.log("waiting for scrub type %s"%(stype,))
            time.sleep(10)

    def get_single_pg_stats(self, pgid):
        """
        Return pg for the pgid specified.
        """
        all_stats = self.get_pg_stats()

        for pg in all_stats:
            if pg['pgid'] == pgid:
                return pg

        return None

    def get_osd_dump(self):
        """
        Dump osds
        :returns: all osds
        """
        out = self.raw_cluster_cmd('osd', 'dump', '--format=json')
        j = json.loads('\n'.join(out.split('\n')[1:]))
        return j['osds']

    def get_stuck_pgs(self, type_, threshold):
        """
        :returns: stuck pg information from the cluster
        """
        out = self.raw_cluster_cmd('pg', 'dump_stuck', type_, str(threshold),
                                   '--format=json')
        return json.loads(out)

    def get_num_unfound_objects(self):
        """
        Check cluster status to get the number of unfound objects
        """
        status = self.raw_cluster_status()
        self.log(status)
        return status['pgmap'].get('unfound_objects', 0)

    def get_num_creating(self):
        """
        Find the number of pgs in creating mode.
        """
        pgs = self.get_pg_stats()
        num = 0
        for pg in pgs:
            if 'creating' in pg['state']:
                num += 1
        return num

    def get_num_active_clean(self):
        """
        Find the number of active and clean pgs.
        """
        pgs = self.get_pg_stats()
        num = 0
        for pg in pgs:
            if pg['state'].count('active') and pg['state'].count('clean') and not pg['state'].count('stale'):
                num += 1
        return num

    def get_num_active_recovered(self):
        """
        Find the number of active and recovered pgs.
        """
        pgs = self.get_pg_stats()
        num = 0
        for pg in pgs:
            if pg['state'].count('active') and not pg['state'].count('recover') and not pg['state'].count('backfill') and not pg['state'].count('stale'):
                num += 1
        return num

    def get_num_active(self):
        """
        Find the number of active pgs.
        """
        pgs = self.get_pg_stats()
        num = 0
        for pg in pgs:
            if pg['state'].count('active') and not pg['state'].count('stale'):
                num += 1
        return num

    def get_num_down(self):
        """
        Find the number of pgs that are down.
        """
        pgs = self.get_pg_stats()
        num = 0
        for pg in pgs:
            if (pg['state'].count('down') and not pg['state'].count('stale')) or \
                    (pg['state'].count('incomplete') and not pg['state'].count('stale')):
                num += 1
        return num

    def get_num_active_down(self):
        """
        Find the number of pgs that are either active or down.
        """
        pgs = self.get_pg_stats()
        num = 0
        for pg in pgs:
            if (pg['state'].count('active') and not pg['state'].count('stale')) or \
                    (pg['state'].count('down') and not pg['state'].count('stale')) or \
                    (pg['state'].count('incomplete') and not pg['state'].count('stale')):
                num += 1
        return num

    def is_clean(self):
        """
        True if all pgs are clean
        """
        return self.get_num_active_clean() == self.get_num_pgs()

    def is_recovered(self):
        """
        True if all pgs have recovered
        """
        return self.get_num_active_recovered() == self.get_num_pgs()

    def is_active_or_down(self):
        """
        True if all pgs are active or down
        """
        return self.get_num_active_down() == self.get_num_pgs()

    def wait_for_clean(self, timeout=None):
        """
        Returns trues when all pgs are clean.
        """
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

    def are_all_osds_up(self):
        """
        Returns true if all osds are up.
        """
        x = self.get_osd_dump()
        return (len(x) == \
                    sum([(y['up'] > 0) for y in x]))

    def wait_for_all_up(self, timeout=None):
        """
        When this exits, either the timeout has expired, or all
        osds are up.
        """
        self.log("waiting for all up")
        start = time.time()
        while not self.are_all_osds_up():
            if timeout is not None:
                assert time.time() - start < timeout, \
                    'timeout expired in wait_for_all_up'
            time.sleep(3)
        self.log("all up!")

    def wait_for_recovery(self, timeout=None):
        """
        Check peering. When this exists, we have recovered.
        """
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
        """
        Check peering. When this exists, we are definitely active
        """
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
        """
        Check peering. When this exists, we are definitely either
        active or down
        """
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
        """
        Wrapper for osd check
        """
        osds = self.get_osd_dump()
        return osds[osd]['up'] > 0

    def wait_till_osd_is_up(self, osd, timeout=None):
        """
        Loop waiting for osd.
        """
        self.log('waiting for osd.%d to be up' % osd)
        start = time.time()
        while not self.osd_is_up(osd):
            if timeout is not None:
                assert time.time() - start < timeout, \
                    'osd.%d failed to come up before timeout expired' % osd
            time.sleep(3)
        self.log('osd.%d is up' % osd)

    def is_active(self):
        """
        Wrapper to check if active
        """
        return self.get_num_active() == self.get_num_pgs()

    def wait_till_active(self, timeout=None):
        """
        Wait until osds are active.
        """
        self.log("waiting till active")
        start = time.time()
        while not self.is_active():
            if timeout is not None:
                assert time.time() - start < timeout, \
                    'failed to become active before timeout expired'
            time.sleep(3)
        self.log("active!")

    def mark_out_osd(self, osd):
        """
        Wrapper to mark osd out.
        """
        self.raw_cluster_cmd('osd', 'out', str(osd))

    def kill_osd(self, osd):
        """
        Kill osds by either power cycling (if indicated by the config)
        or by stopping.
        """
        if self.config.get('powercycle'):
            (remote,) = self.ctx.cluster.only('osd.{o}'.format(o=osd)).remotes.iterkeys()
            self.log('kill_osd on osd.{o} doing powercycle of {s}'.format(o=osd, s=remote.name))
            assert remote.console is not None, "powercycling requested but RemoteConsole is not initialized.  Check ipmi config."
            remote.console.power_off()
        else:
            self.ctx.daemons.get_daemon('osd', osd).stop()

    def blackhole_kill_osd(self, osd):
        """
        Stop osd if nothing else works.
        """
        self.raw_cluster_cmd('--', 'tell', 'osd.%d' % osd,
                             'injectargs', '--filestore-blackhole')
        time.sleep(2)
        self.ctx.daemons.get_daemon('osd', osd).stop()

    def revive_osd(self, osd, timeout=75):
        """
        Revive osds by either power cycling (if indicated by the config)
        or by restarting.
        """
        if self.config.get('powercycle'):
            (remote,) = self.ctx.cluster.only('osd.{o}'.format(o=osd)).remotes.iterkeys()
            self.log('kill_osd on osd.{o} doing powercycle of {s}'.format(o=osd, s=remote.name))
            assert remote.console is not None, "powercycling requested but RemoteConsole is not initialized.  Check ipmi config."
            remote.console.power_on()
            if not remote.console.check_status(300):
                raise Exception('Failed to revive osd.{o} via ipmi'.format(o=osd))
            teuthology.reconnect(self.ctx, 60, [remote])
            ceph_task.mount_osd_data(self.ctx, remote, str(osd))
            ceph_task.make_admin_daemon_dir(self.ctx, remote)
            self.ctx.daemons.get_daemon('osd', osd).reset()
        self.ctx.daemons.get_daemon('osd', osd).restart()
        # wait for dump_ops_in_flight; this command doesn't appear
        # until after the signal handler is installed and it is safe
        # to stop the osd again without making valgrind leak checks
        # unhappy.  see #5924.
        self.wait_run_admin_socket(osd,
                                   args=['dump_ops_in_flight'],
                                   timeout=timeout)

    def mark_down_osd(self, osd):
        """
        Cluster command wrapper
        """
        self.raw_cluster_cmd('osd', 'down', str(osd))

    def mark_in_osd(self, osd):
        """
        Cluster command wrapper
        """
        self.raw_cluster_cmd('osd', 'in', str(osd))


    ## monitors

    def signal_mon(self, mon, sig):
        """
        Wrapper to local get_deamon call
        """
        self.ctx.daemons.get_daemon('mon', mon).signal(sig)

    def kill_mon(self, mon):
        """
        Kill the monitor by either power cycling (if the config says so),
        or by doing a stop.
        """
        if self.config.get('powercycle'):
            (remote,) = self.ctx.cluster.only('mon.{m}'.format(m=mon)).remotes.iterkeys()
            self.log('kill_mon on mon.{m} doing powercycle of {s}'.format(m=mon, s=remote.name))
            assert remote.console is not None, "powercycling requested but RemoteConsole is not initialized.  Check ipmi config."
            remote.console.power_off()
        else:
            self.ctx.daemons.get_daemon('mon', mon).stop()

    def revive_mon(self, mon):
        """
        Restart by either power cycling (if the config says so),
        or by doing a normal restart.
        """
        if self.config.get('powercycle'):
            (remote,) = self.ctx.cluster.only('mon.{m}'.format(m=mon)).remotes.iterkeys()
            self.log('revive_mon on mon.{m} doing powercycle of {s}'.format(m=mon, s=remote.name))
            assert remote.console is not None, "powercycling requested but RemoteConsole is not initialized.  Check ipmi config."
            remote.console.power_on()
            ceph_task.make_admin_daemon_dir(self.ctx, remote)
        self.ctx.daemons.get_daemon('mon', mon).restart()

    def get_mon_status(self, mon):
        """
        Extract all the monitor status information from the cluster
        """
        addr = self.ctx.ceph.conf['mon.%s' % mon]['mon addr']
        out = self.raw_cluster_cmd('-m', addr, 'mon_status')
        return json.loads(out)

    def get_mon_quorum(self):
        """
        Extract monitor quorum information from the cluster
        """
        out = self.raw_cluster_cmd('quorum_status')
        j = json.loads(out)
        self.log('quorum_status is %s' % out)
        return j['quorum']

    def wait_for_mon_quorum_size(self, size, timeout=300):
        """
        Loop until quorum size is reached.
        """
        self.log('waiting for quorum size %d' % size)
        start = time.time()
        while not len(self.get_mon_quorum()) == size:
            if timeout is not None:
                assert time.time() - start < timeout, \
                    'failed to reach quorum size %d before timeout expired' % size
            time.sleep(3)
        self.log("quorum is size %d" % size)

    def get_mon_health(self, debug=False):
        """
        Extract all the monitor health information.
        """
        out = self.raw_cluster_cmd('health', '--format=json')
        if debug:
            self.log('health:\n{h}'.format(h=out))
        return json.loads(out)

    ## metadata servers

    def kill_mds(self, mds):
        """
        Powercyle if set in config, otherwise just stop.
        """
        if self.config.get('powercycle'):
            (remote,) = self.ctx.cluster.only('mds.{m}'.format(m=mds)).remotes.iterkeys()
            self.log('kill_mds on mds.{m} doing powercycle of {s}'.format(m=mds, s=remote.name))
            assert remote.console is not None, "powercycling requested but RemoteConsole is not initialized.  Check ipmi config."
            remote.console.power_off()
        else:
            self.ctx.daemons.get_daemon('mds', mds).stop()

    def kill_mds_by_rank(self, rank):
        """
        kill_mds wrapper to kill based on rank passed.
        """
        status = self.get_mds_status_by_rank(rank)
        self.kill_mds(status['name'])

    def revive_mds(self, mds, standby_for_rank=None):
        """
        Revive mds -- do an ipmpi powercycle (if indicated by the config)
        and then restart (using --hot-standby if specified.
        """
        if self.config.get('powercycle'):
            (remote,) = self.ctx.cluster.only('mds.{m}'.format(m=mds)).remotes.iterkeys()
            self.log('revive_mds on mds.{m} doing powercycle of {s}'.format(m=mds, s=remote.name))
            assert remote.console is not None, "powercycling requested but RemoteConsole is not initialized.  Check ipmi config."
            remote.console.power_on()
            ceph_task.make_admin_daemon_dir(self.ctx, remote)
        args = []
        if standby_for_rank:
            args.extend(['--hot-standby', standby_for_rank])
        self.ctx.daemons.get_daemon('mds', mds).restart(*args)

    def revive_mds_by_rank(self, rank, standby_for_rank=None):
        """
        revive_mds wrapper to revive based on rank passed.
        """
        status = self.get_mds_status_by_rank(rank)
        self.revive_mds(status['name'], standby_for_rank)

    def get_mds_status(self, mds):
        """
        Run cluster commands for the mds in order to get mds information
        """
        out = self.raw_cluster_cmd('mds', 'dump', '--format=json')
        j = json.loads(' '.join(out.splitlines()[1:]))
        # collate; for dup ids, larger gid wins.
        for info in j['info'].itervalues():
            if info['name'] == mds:
                return info
        return None

    def get_mds_status_by_rank(self, rank):
        """
        Run cluster commands for the mds in order to get mds information
        check rank.
        """
        out = self.raw_cluster_cmd('mds', 'dump', '--format=json')
        j = json.loads(' '.join(out.splitlines()[1:]))
        # collate; for dup ids, larger gid wins.
        for info in j['info'].itervalues():
            if info['rank'] == rank:
                return info
        return None

    def get_mds_status_all(self):
        """
        Run cluster command to extract all the mds status.
        """
        out = self.raw_cluster_cmd('mds', 'dump', '--format=json')
        j = json.loads(' '.join(out.splitlines()[1:]))
        return j
