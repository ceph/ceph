"""
Thrash -- Simulate random osd failures.
"""
import contextlib
import logging
import random
import signal
import time
import gevent
import json
import traceback
import os
from teuthology import misc as teuthology
from cStringIO import StringIO
from functools import wraps
from tasks.scrub import Scrubber
from teuthology.contextutil import safe_while
from teuthology.orchestra.remote import Remote
from teuthology.exceptions import CommandFailedError
from tasks.thrasher import Thrasher

log = logging.getLogger(__name__)

class OSDThrasher(Thrasher):
    """
    Object used to thrash Ceph
    """
    def __init__(self, manager, config, logger):
        super(OSDThrasher, self).__init__()
        self.ceph_manager = manager
        self.cluster = manager.cluster
        self.ceph_manager.wait_for_clean()
        osd_status = self.ceph_manager.get_osd_status()
        self.in_osds = osd_status['in']
        self.live_osds = osd_status['live']
        self.out_osds = osd_status['out']
        self.dead_osds = osd_status['dead']
        self.stopping = False
        self.logger = logger
        self.config = config
        self.revive_timeout = self.config.get("revive_timeout", 360)
        self.pools_to_fix_pgp_num = set()
        if self.config.get('powercycle'):
            self.revive_timeout += 120
        self.clean_wait = self.config.get('clean_wait', 0)
        self.minin = self.config.get("min_in", 4)
        self.chance_move_pg = self.config.get('chance_move_pg', 1.0)
        self.sighup_delay = self.config.get('sighup_delay')
        self.optrack_toggle_delay = self.config.get('optrack_toggle_delay')
        self.dump_ops_enable = self.config.get('dump_ops_enable')
        self.noscrub_toggle_delay = self.config.get('noscrub_toggle_delay')
        self.chance_thrash_cluster_full = self.config.get('chance_thrash_cluster_full', .05)
        self.chance_thrash_pg_upmap = self.config.get('chance_thrash_pg_upmap', 1.0)
        self.chance_thrash_pg_upmap_items = self.config.get('chance_thrash_pg_upmap', 1.0)
        self.random_eio = self.config.get('random_eio')
        self.chance_force_recovery = self.config.get('chance_force_recovery', 0.3)

        num_osds = self.in_osds + self.out_osds
        self.max_pgs = self.config.get("max_pgs_per_pool_osd", 1200) * len(num_osds)
        self.min_pgs = self.config.get("min_pgs_per_pool_osd", 1) * len(num_osds)
        if self.config is None:
            self.config = dict()
        # prevent monitor from auto-marking things out while thrasher runs
        # try both old and new tell syntax, in case we are testing old code
        self.saved_options = []
        # assuming that the default settings do not vary from one daemon to
        # another
        first_mon = teuthology.get_first_mon(manager.ctx, self.config).split('.')
        opts = [('mon', 'mon_osd_down_out_interval', 0)]
        #why do we disable marking an OSD out automatically? :/
        for service, opt, new_value in opts:
            old_value = manager.get_config(first_mon[0],
                                           first_mon[1],
                                           opt)
            self.saved_options.append((service, opt, old_value))
            manager.inject_args(service, '*', opt, new_value)
        # initialize ceph_objectstore_tool property - must be done before
        # do_thrash is spawned - http://tracker.ceph.com/issues/18799
        if (self.config.get('powercycle') or
            not self.cmd_exists_on_osds("ceph-objectstore-tool") or
            self.config.get('disable_objectstore_tool_tests', False)):
            self.ceph_objectstore_tool = False
            if self.config.get('powercycle'):
                self.log("Unable to test ceph-objectstore-tool, "
                         "powercycle testing")
            else:
                self.log("Unable to test ceph-objectstore-tool, "
                         "not available on all OSD nodes")
        else:
            self.ceph_objectstore_tool = \
                self.config.get('ceph_objectstore_tool', True)
        # spawn do_thrash
        self.thread = gevent.spawn(self.do_thrash)
        if self.sighup_delay:
            self.sighup_thread = gevent.spawn(self.do_sighup)
        if self.optrack_toggle_delay:
            self.optrack_toggle_thread = gevent.spawn(self.do_optrack_toggle)
        if self.dump_ops_enable == "true":
            self.dump_ops_thread = gevent.spawn(self.do_dump_ops)
        if self.noscrub_toggle_delay:
            self.noscrub_toggle_thread = gevent.spawn(self.do_noscrub_toggle)

    def log(self, msg, *args, **kwargs):
        self.logger.info(msg, *args, **kwargs)

    def cmd_exists_on_osds(self, cmd):
        allremotes = self.ceph_manager.ctx.cluster.only(\
            teuthology.is_type('osd', self.cluster)).remotes.keys()
        allremotes = list(set(allremotes))
        for remote in allremotes:
            proc = remote.run(args=['type', cmd], wait=True,
                              check_status=False, stdout=StringIO(),
                              stderr=StringIO())
            if proc.exitstatus != 0:
                return False;
        return True;

    def kill_osd(self, osd=None, mark_down=False, mark_out=False):
        """
        :param osd: Osd to be killed.
        :mark_down: Mark down if true.
        :mark_out: Mark out if true.
        """
        if osd is None:
            osd = random.choice(self.live_osds)
        self.log("Killing osd %s, live_osds are %s" % (str(osd),
                                                       str(self.live_osds)))
        self.live_osds.remove(osd)
        self.dead_osds.append(osd)
        self.ceph_manager.kill_osd(osd)
        if mark_down:
            self.ceph_manager.mark_down_osd(osd)
        if mark_out and osd in self.in_osds:
            self.out_osd(osd)
        if self.ceph_objectstore_tool:
            self.log("Testing ceph-objectstore-tool on down osd")
            remote = self.ceph_manager.find_remote('osd', osd)
            FSPATH = self.ceph_manager.get_filepath()
            JPATH = os.path.join(FSPATH, "journal")
            exp_osd = imp_osd = osd
            exp_remote = imp_remote = remote
            # If an older osd is available we'll move a pg from there
            if (len(self.dead_osds) > 1 and
                    random.random() < self.chance_move_pg):
                exp_osd = random.choice(self.dead_osds[:-1])
                exp_remote = self.ceph_manager.find_remote('osd', exp_osd)
            if ('keyvaluestore_backend' in
                    self.ceph_manager.ctx.ceph[self.cluster].conf['osd']):
                prefix = ("sudo adjust-ulimits ceph-objectstore-tool "
                          "--data-path {fpath} --journal-path {jpath} "
                          "--type keyvaluestore "
                          "--log-file="
                          "/var/log/ceph/objectstore_tool.\\$pid.log ".
                          format(fpath=FSPATH, jpath=JPATH))
            else:
                prefix = ("sudo adjust-ulimits ceph-objectstore-tool "
                          "--data-path {fpath} --journal-path {jpath} "
                          "--log-file="
                          "/var/log/ceph/objectstore_tool.\\$pid.log ".
                          format(fpath=FSPATH, jpath=JPATH))
            cmd = (prefix + "--op list-pgs").format(id=exp_osd)

            # ceph-objectstore-tool might be temporarily absent during an
            # upgrade - see http://tracker.ceph.com/issues/18014
            with safe_while(sleep=15, tries=40, action="type ceph-objectstore-tool") as proceed:
                while proceed():
                    proc = exp_remote.run(args=['type', 'ceph-objectstore-tool'],
                               wait=True, check_status=False, stdout=StringIO(),
                               stderr=StringIO())
                    if proc.exitstatus == 0:
                        break
                    log.debug("ceph-objectstore-tool binary not present, trying again")

            # ceph-objectstore-tool might bogusly fail with "OSD has the store locked"
            # see http://tracker.ceph.com/issues/19556
            with safe_while(sleep=15, tries=40, action="ceph-objectstore-tool --op list-pgs") as proceed:
                while proceed():
                    proc = exp_remote.run(args=cmd, wait=True,
                                          check_status=False,
                                          stdout=StringIO(), stderr=StringIO())
                    if proc.exitstatus == 0:
                        break
                    elif proc.exitstatus == 1 and proc.stderr == "OSD has the store locked":
                        continue
                    else:
                        raise Exception("ceph-objectstore-tool: "
                                        "exp list-pgs failure with status {ret}".
                                        format(ret=proc.exitstatus))

            pgs = proc.stdout.getvalue().split('\n')[:-1]
            if len(pgs) == 0:
                self.log("No PGs found for osd.{osd}".format(osd=exp_osd))
                return
            pg = random.choice(pgs)
            exp_path = teuthology.get_testdir(self.ceph_manager.ctx)
            exp_path = os.path.join(exp_path, '{0}.data'.format(self.cluster))
            exp_path = os.path.join(exp_path,
                                    "exp.{pg}.{id}".format(
                                        pg=pg,
                                        id=exp_osd))
            # export
            # Can't use new export-remove op since this is part of upgrade testing
            cmd = prefix + "--op export --pgid {pg} --file {file}"
            cmd = cmd.format(id=exp_osd, pg=pg, file=exp_path)
            proc = exp_remote.run(args=cmd)
            if proc.exitstatus:
                raise Exception("ceph-objectstore-tool: "
                                "export failure with status {ret}".
                                format(ret=proc.exitstatus))
            # remove
            cmd = prefix + "--force --op remove --pgid {pg}"
            cmd = cmd.format(id=exp_osd, pg=pg)
            proc = exp_remote.run(args=cmd)
            if proc.exitstatus:
                raise Exception("ceph-objectstore-tool: "
                                "remove failure with status {ret}".
                                format(ret=proc.exitstatus))
            # If there are at least 2 dead osds we might move the pg
            if exp_osd != imp_osd:
                # If pg isn't already on this osd, then we will move it there
                cmd = (prefix + "--op list-pgs").format(id=imp_osd)
                proc = imp_remote.run(args=cmd, wait=True,
                                      check_status=False, stdout=StringIO())
                if proc.exitstatus:
                    raise Exception("ceph-objectstore-tool: "
                                    "imp list-pgs failure with status {ret}".
                                    format(ret=proc.exitstatus))
                pgs = proc.stdout.getvalue().split('\n')[:-1]
                if pg not in pgs:
                    self.log("Moving pg {pg} from osd.{fosd} to osd.{tosd}".
                             format(pg=pg, fosd=exp_osd, tosd=imp_osd))
                    if imp_remote != exp_remote:
                        # Copy export file to the other machine
                        self.log("Transfer export file from {srem} to {trem}".
                                 format(srem=exp_remote, trem=imp_remote))
                        tmpexport = Remote.get_file(exp_remote, exp_path)
                        Remote.put_file(imp_remote, tmpexport, exp_path)
                        os.remove(tmpexport)
                else:
                    # Can't move the pg after all
                    imp_osd = exp_osd
                    imp_remote = exp_remote
            # import
            cmd = (prefix + "--op import --file {file}")
            cmd = cmd.format(id=imp_osd, file=exp_path)
            proc = imp_remote.run(args=cmd, wait=True, check_status=False,
                                  stderr=StringIO())
            if proc.exitstatus == 1:
                bogosity = "The OSD you are using is older than the exported PG"
                if bogosity in proc.stderr.getvalue():
                    self.log("OSD older than exported PG"
                             "...ignored")
            elif proc.exitstatus == 10:
                self.log("Pool went away before processing an import"
                         "...ignored")
            elif proc.exitstatus == 11:
                self.log("Attempt to import an incompatible export"
                         "...ignored")
            elif proc.exitstatus == 12:
                # this should be safe to ignore because we only ever move 1
                # copy of the pg at a time, and merge is only initiated when
                # all replicas are peered and happy.  /me crosses fingers
                self.log("PG merged on target"
                         "...ignored")
            elif proc.exitstatus:
                raise Exception("ceph-objectstore-tool: "
                                "import failure with status {ret}".
                                format(ret=proc.exitstatus))
            cmd = "rm -f {file}".format(file=exp_path)
            exp_remote.run(args=cmd)
            if imp_remote != exp_remote:
                imp_remote.run(args=cmd)

            # apply low split settings to each pool
            for pool in self.ceph_manager.list_pools():
                no_sudo_prefix = prefix[5:]
                cmd = ("CEPH_ARGS='--filestore-merge-threshold 1 "
                       "--filestore-split-multiple 1' sudo -E "
                       + no_sudo_prefix + "--op apply-layout-settings --pool " + pool).format(id=osd)
                proc = remote.run(args=cmd, wait=True, check_status=False, stderr=StringIO())
                output = proc.stderr.getvalue()
                if 'Couldn\'t find pool' in output:
                    continue
                if proc.exitstatus:
                    raise Exception("ceph-objectstore-tool apply-layout-settings"
                                    " failed with {status}".format(status=proc.exitstatus))


    def blackhole_kill_osd(self, osd=None):
        """
        If all else fails, kill the osd.
        :param osd: Osd to be killed.
        """
        if osd is None:
            osd = random.choice(self.live_osds)
        self.log("Blackholing and then killing osd %s, live_osds are %s" %
                 (str(osd), str(self.live_osds)))
        self.live_osds.remove(osd)
        self.dead_osds.append(osd)
        self.ceph_manager.blackhole_kill_osd(osd)

    def revive_osd(self, osd=None, skip_admin_check=False):
        """
        Revive the osd.
        :param osd: Osd to be revived.
        """
        if osd is None:
            osd = random.choice(self.dead_osds)
        self.log("Reviving osd %s" % (str(osd),))
        self.ceph_manager.revive_osd(
            osd,
            self.revive_timeout,
            skip_admin_check=skip_admin_check)
        self.dead_osds.remove(osd)
        self.live_osds.append(osd)
        if self.random_eio > 0 and osd == self.rerrosd:
            self.ceph_manager.set_config(self.rerrosd,
                                         filestore_debug_random_read_err = self.random_eio)
            self.ceph_manager.set_config(self.rerrosd,
                                         bluestore_debug_random_read_err = self.random_eio)


    def out_osd(self, osd=None):
        """
        Mark the osd out
        :param osd: Osd to be marked.
        """
        if osd is None:
            osd = random.choice(self.in_osds)
        self.log("Removing osd %s, in_osds are: %s" %
                 (str(osd), str(self.in_osds)))
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
        self.log("Added osd %s" % (str(osd),))

    def reweight_osd_or_by_util(self, osd=None):
        """
        Reweight an osd that is in
        :param osd: Osd to be marked.
        """
        if osd is not None or random.choice([True, False]):
            if osd is None:
                osd = random.choice(self.in_osds)
            val = random.uniform(.1, 1.0)
            self.log("Reweighting osd %s to %s" % (str(osd), str(val)))
            self.ceph_manager.raw_cluster_cmd('osd', 'reweight',
                                              str(osd), str(val))
        else:
            # do it several times, the option space is large
            for i in range(5):
                options = {
                    'max_change': random.choice(['0.05', '1.0', '3.0']),
                    'overage': random.choice(['110', '1000']),
                    'type': random.choice([
                        'reweight-by-utilization',
                        'test-reweight-by-utilization']),
                }
                self.log("Reweighting by: %s"%(str(options),))
                self.ceph_manager.raw_cluster_cmd(
                    'osd',
                    options['type'],
                    options['overage'],
                    options['max_change'])

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
        self.ceph_manager.raw_cluster_cmd('osd', 'primary-affinity',
                                          str(osd), str(pa))

    def thrash_cluster_full(self):
        """
        Set and unset cluster full condition
        """
        self.log('Setting full ratio to .001')
        self.ceph_manager.raw_cluster_cmd('osd', 'set-full-ratio', '.001')
        time.sleep(1)
        self.log('Setting full ratio back to .95')
        self.ceph_manager.raw_cluster_cmd('osd', 'set-full-ratio', '.95')

    def thrash_pg_upmap(self):
        """
        Install or remove random pg_upmap entries in OSDMap
        """
        from random import shuffle
        out = self.ceph_manager.raw_cluster_cmd('osd', 'dump', '-f', 'json-pretty')
        j = json.loads(out)
        self.log('j is %s' % j)
        try:
            if random.random() >= .3:
                pgs = self.ceph_manager.get_pg_stats()
                if not pgs:
                    return
                pg = random.choice(pgs)
                pgid = str(pg['pgid'])
                poolid = int(pgid.split('.')[0])
                sizes = [x['size'] for x in j['pools'] if x['pool'] == poolid]
                if len(sizes) == 0:
                    return
                n = sizes[0]
                osds = self.in_osds + self.out_osds
                shuffle(osds)
                osds = osds[0:n]
                self.log('Setting %s to %s' % (pgid, osds))
                cmd = ['osd', 'pg-upmap', pgid] + [str(x) for x in osds]
                self.log('cmd %s' % cmd)
                self.ceph_manager.raw_cluster_cmd(*cmd)
            else:
                m = j['pg_upmap']
                if len(m) > 0:
                    shuffle(m)
                    pg = m[0]['pgid']
                    self.log('Clearing pg_upmap on %s' % pg)
                    self.ceph_manager.raw_cluster_cmd(
                        'osd',
                        'rm-pg-upmap',
                        pg)
                else:
                    self.log('No pg_upmap entries; doing nothing')
        except CommandFailedError:
            self.log('Failed to rm-pg-upmap, ignoring')

    def thrash_pg_upmap_items(self):
        """
        Install or remove random pg_upmap_items entries in OSDMap
        """
        from random import shuffle
        out = self.ceph_manager.raw_cluster_cmd('osd', 'dump', '-f', 'json-pretty')
        j = json.loads(out)
        self.log('j is %s' % j)
        try:
            if random.random() >= .3:
                pgs = self.ceph_manager.get_pg_stats()
                if not pgs:
                    return
                pg = random.choice(pgs)
                pgid = str(pg['pgid'])
                poolid = int(pgid.split('.')[0])
                sizes = [x['size'] for x in j['pools'] if x['pool'] == poolid]
                if len(sizes) == 0:
                    return
                n = sizes[0]
                osds = self.in_osds + self.out_osds
                shuffle(osds)
                osds = osds[0:n*2]
                self.log('Setting %s to %s' % (pgid, osds))
                cmd = ['osd', 'pg-upmap-items', pgid] + [str(x) for x in osds]
                self.log('cmd %s' % cmd)
                self.ceph_manager.raw_cluster_cmd(*cmd)
            else:
                m = j['pg_upmap_items']
                if len(m) > 0:
                    shuffle(m)
                    pg = m[0]['pgid']
                    self.log('Clearing pg_upmap on %s' % pg)
                    self.ceph_manager.raw_cluster_cmd(
                        'osd',
                        'rm-pg-upmap-items',
                        pg)
                else:
                    self.log('No pg_upmap entries; doing nothing')
        except CommandFailedError:
            self.log('Failed to rm-pg-upmap-items, ignoring')

    def force_recovery(self):
        """
        Force recovery on some of PGs
        """
        backfill = random.random() >= 0.5
        j = self.ceph_manager.get_pgids_to_force(backfill)
        if j:
            try:
                if backfill:
                    self.ceph_manager.raw_cluster_cmd('pg', 'force-backfill', *j)
                else:
                    self.ceph_manager.raw_cluster_cmd('pg', 'force-recovery', *j)
            except CommandFailedError:
                self.log('Failed to force backfill|recovery, ignoring')


    def cancel_force_recovery(self):
        """
        Force recovery on some of PGs
        """
        backfill = random.random() >= 0.5
        j = self.ceph_manager.get_pgids_to_cancel_force(backfill)
        if j:
            try:
                if backfill:
                    self.ceph_manager.raw_cluster_cmd('pg', 'cancel-force-backfill', *j)
                else:
                    self.ceph_manager.raw_cluster_cmd('pg', 'cancel-force-recovery', *j)
            except CommandFailedError:
                self.log('Failed to force backfill|recovery, ignoring')

    def force_cancel_recovery(self):
        """
        Force or cancel forcing recovery
        """
        if random.random() >= 0.4:
           self.force_recovery()
        else:
           self.cancel_force_recovery()

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

    def all_up_in(self):
        """
        Make sure all osds are up and fully in.
        """
        self.all_up();
        for osd in self.live_osds:
            self.ceph_manager.raw_cluster_cmd('osd', 'reweight',
                                              str(osd), str(1))
            self.ceph_manager.raw_cluster_cmd('osd', 'primary-affinity',
                                              str(osd), str(1))

    def do_join(self):
        """
        Break out of this Ceph loop
        """
        self.stopping = True
        self.thread.get()
        if self.sighup_delay:
            self.log("joining the do_sighup greenlet")
            self.sighup_thread.get()
        if self.optrack_toggle_delay:
            self.log("joining the do_optrack_toggle greenlet")
            self.optrack_toggle_thread.join()
        if self.dump_ops_enable == "true":
            self.log("joining the do_dump_ops greenlet")
            self.dump_ops_thread.join()
        if self.noscrub_toggle_delay:
            self.log("joining the do_noscrub_toggle greenlet")
            self.noscrub_toggle_thread.join()

    def grow_pool(self):
        """
        Increase the size of the pool
        """
        pool = self.ceph_manager.get_pool()
        self.log("Growing pool %s" % (pool,))
        if self.ceph_manager.expand_pool(pool,
                                         self.config.get('pool_grow_by', 10),
                                         self.max_pgs):
            self.pools_to_fix_pgp_num.add(pool)

    def shrink_pool(self):
        """
        Decrease the size of the pool
        """
        pool = self.ceph_manager.get_pool()
        orig_pg_num = self.ceph_manager.get_pool_pg_num(pool)
        self.log("Shrinking pool %s" % (pool,))
        if self.ceph_manager.contract_pool(
                pool,
                self.config.get('pool_shrink_by', 10),
                self.min_pgs):
            self.pools_to_fix_pgp_num.add(pool)

    def fix_pgp_num(self, pool=None):
        """
        Fix number of pgs in pool.
        """
        if pool is None:
            pool = self.ceph_manager.get_pool()
            force = False
        else:
            force = True
        self.log("fixing pg num pool %s" % (pool,))
        if self.ceph_manager.set_pool_pgpnum(pool, force):
            self.pools_to_fix_pgp_num.discard(pool)

    def test_pool_min_size(self):
        """
        Loop to selectively push PGs below their min_size and test that recovery
        still occurs.
        """
        self.log("test_pool_min_size")
        self.all_up()
        self.ceph_manager.wait_for_recovery(
            timeout=self.config.get('timeout')
            )

        minout = int(self.config.get("min_out", 1))
        minlive = int(self.config.get("min_live", 2))
        mindead = int(self.config.get("min_dead", 1))
        self.log("doing min_size thrashing")
        self.ceph_manager.wait_for_clean(timeout=60)
        assert self.ceph_manager.is_clean(), \
            'not clean before minsize thrashing starts'
        while not self.stopping:
            # look up k and m from all the pools on each loop, in case it
            # changes as the cluster runs
            k = 0
            m = 99
            has_pools = False
            pools_json = self.ceph_manager.get_osd_dump_json()['pools']

            for pool_json in pools_json:
                pool = pool_json['pool_name']
                has_pools = True
                pool_type = pool_json['type']  # 1 for rep, 3 for ec
                min_size = pool_json['min_size']
                self.log("pool {pool} min_size is {min_size}".format(pool=pool,min_size=min_size))
                try:
                    ec_profile = self.ceph_manager.get_pool_property(pool, 'erasure_code_profile')
                    if pool_type != PoolType.ERASURE_CODED:
                        continue
                    ec_profile = pool_json['erasure_code_profile']
                    ec_profile_json = self.ceph_manager.raw_cluster_cmd(
                        'osd',
                        'erasure-code-profile',
                        'get',
                        ec_profile,
                        '--format=json')
                    ec_json = json.loads(ec_profile_json)
                    local_k = int(ec_json['k'])
                    local_m = int(ec_json['m'])
                    self.log("pool {pool} local_k={k} local_m={m}".format(pool=pool,
                                                                          k=local_k, m=local_m))
                    if local_k > k:
                        self.log("setting k={local_k} from previous {k}".format(local_k=local_k, k=k))
                        k = local_k
                    if local_m < m:
                        self.log("setting m={local_m} from previous {m}".format(local_m=local_m, m=m))
                        m = local_m
                except CommandFailedError:
                    self.log("failed to read erasure_code_profile. %s was likely removed", pool)
                    continue

            if has_pools :
                self.log("using k={k}, m={m}".format(k=k,m=m))
            else:
                self.log("No pools yet, waiting")
                time.sleep(5)
                continue

            if minout > len(self.out_osds): # kill OSDs and mark out
                self.log("forced to out an osd")
                self.kill_osd(mark_out=True)
                continue
            elif mindead > len(self.dead_osds): # kill OSDs but force timeout
                self.log("forced to kill an osd")
                self.kill_osd()
                continue
            else: # make mostly-random choice to kill or revive OSDs
                minup = max(minlive, k)
                rand_val = random.uniform(0, 1)
                self.log("choosing based on number of live OSDs and rand val {rand}".\
                         format(rand=rand_val))
                if len(self.live_osds) > minup+1 and rand_val < 0.5:
                    # chose to knock out as many OSDs as we can w/out downing PGs

                    most_killable = min(len(self.live_osds) - minup, m)
                    self.log("chose to kill {n} OSDs".format(n=most_killable))
                    for i in range(1, most_killable):
                        self.kill_osd(mark_out=True)
                    time.sleep(15)
                    assert self.ceph_manager.all_active_or_peered(), \
                            'not all PGs are active or peered 15 seconds after marking out OSDs'
                else: # chose to revive OSDs, bring up a random fraction of the dead ones
                    self.log("chose to revive osds")
                    for i in range(1, int(rand_val * len(self.dead_osds))):
                        revive_osd()

            # let PGs repair themselves or our next knockout might kill one
            self.ceph_manager.wait_for_clean(timeout=self.config.get('timeout'))

        # / while not self.stopping
        self.all_up_in()

        self.ceph_manager.wait_for_recovery(
            timeout=self.config.get('timeout')
            )

    def inject_pause(self, conf_key, duration, check_after, should_be_down):
        """
        Pause injection testing. Check for osd being down when finished.
        """
        the_one = random.choice(self.live_osds)
        self.log("inject_pause on {osd}".format(osd=the_one))
        self.log(
            "Testing {key} pause injection for duration {duration}".format(
                key=conf_key,
                duration=duration
                ))
        self.log(
            "Checking after {after}, should_be_down={shouldbedown}".format(
                after=check_after,
                shouldbedown=should_be_down
                ))
        self.ceph_manager.set_config(the_one, **{conf_key: duration})
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

        First, use injectfull admin command to simulate a now full
        osd by setting it to 0 on all of the OSDs.

        Second, on a random subset, set
        osd_debug_skip_full_check_in_backfill_reservation to force
        the more complicated check in do_scan to be exercised.

        Then, verify that all backfillings stop.
        """
        self.log("injecting backfill full")
        for i in self.live_osds:
            self.ceph_manager.set_config(
                i,
                osd_debug_skip_full_check_in_backfill_reservation=
                random.choice(['false', 'true']))
            self.ceph_manager.osd_admin_socket(i, command=['injectfull', 'backfillfull'],
                                     check_status=True, timeout=30, stdout=DEVNULL)
        for i in range(30):
            status = self.ceph_manager.compile_pg_status()
            if 'backfilling' not in status.keys():
                break
            self.log(
                "waiting for {still_going} backfillings".format(
                    still_going=status.get('backfilling')))
            time.sleep(1)
        assert('backfilling' not in self.ceph_manager.compile_pg_status().keys())
        for i in self.live_osds:
            self.ceph_manager.set_config(
                i,
                osd_debug_skip_full_check_in_backfill_reservation='false')
            self.ceph_manager.osd_admin_socket(i, command=['injectfull', 'none'],
                                     check_status=True, timeout=30, stdout=DEVNULL)

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
        self.ceph_manager.wait_for_all_osds_up(
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
        chance_test_backfill_full = \
            self.config.get('chance_test_backfill_full', 0)
        if isinstance(chance_down, int):
            chance_down = float(chance_down) / 100
        minin = self.minin
        minout = int(self.config.get("min_out", 0))
        minlive = int(self.config.get("min_live", 2))
        mindead = int(self.config.get("min_dead", 0))

        self.log('choose_action: min_in %d min_out '
                 '%d min_live %d min_dead %d' %
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
        actions.append((self.reweight_osd_or_by_util,
                        self.config.get('reweight_osd', .5),))
        actions.append((self.grow_pool,
                        self.config.get('chance_pgnum_grow', 0),))
        actions.append((self.shrink_pool,
                        self.config.get('chance_pgnum_shrink', 0),))
        actions.append((self.fix_pgp_num,
                        self.config.get('chance_pgpnum_fix', 0),))
        actions.append((self.test_pool_min_size,
                        self.config.get('chance_test_min_size', 0),))
        actions.append((self.test_backfill_full,
                        chance_test_backfill_full,))
        if self.chance_thrash_cluster_full > 0:
            actions.append((self.thrash_cluster_full, self.chance_thrash_cluster_full,))
        if self.chance_thrash_pg_upmap > 0:
            actions.append((self.thrash_pg_upmap, self.chance_thrash_pg_upmap,))
        if self.chance_thrash_pg_upmap_items > 0:
            actions.append((self.thrash_pg_upmap_items, self.chance_thrash_pg_upmap_items,))
        if self.chance_force_recovery > 0:
            actions.append((self.force_cancel_recovery, self.chance_force_recovery))

        for key in ['heartbeat_inject_failure', 'filestore_inject_stall']:
            for scenario in [
                (lambda:
                 self.inject_pause(key,
                                   self.config.get('pause_short', 3),
                                   0,
                                   False),
                 self.config.get('chance_inject_pause_short', 1),),
                (lambda:
                 self.inject_pause(key,
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
        _do_thrash() wrapper.
        """
        try:
            self._do_thrash()
        except Exception as e:
            # See _run exception comment for MDSThrasher
            self.exception = e
            self.logger.exception("exception:")
            # Allow successful completion so gevent doesn't see an exception.
            # The DaemonWatchdog will observe the error and tear down the test.

    def log_exc(func):
        @wraps(func)
        def wrapper(self):
            try:
                return func(self)
            except:
                self.log(traceback.format_exc())
                raise
        return wrapper

    @log_exc
    def do_sighup(self):
        """
        Loops and sends signal.SIGHUP to a random live osd.

        Loop delay is controlled by the config value sighup_delay.
        """
        delay = float(self.sighup_delay)
        self.log("starting do_sighup with a delay of {0}".format(delay))
        while not self.stopping:
            osd = random.choice(self.live_osds)
            self.ceph_manager.signal_osd(osd, signal.SIGHUP, silent=True)
            time.sleep(delay)

    @log_exc
    def do_optrack_toggle(self):
        """
        Loops and toggle op tracking to all osds.

        Loop delay is controlled by the config value optrack_toggle_delay.
        """
        delay = float(self.optrack_toggle_delay)
        osd_state = "true"
        self.log("starting do_optrack_toggle with a delay of {0}".format(delay))
        while not self.stopping:
            if osd_state == "true":
                osd_state = "false"
            else:
                osd_state = "true"
            try:
                self.ceph_manager.inject_args('osd', '*',
                                              'osd_enable_op_tracker',
                                              osd_state)
            except CommandFailedError:
                self.log('Failed to tell all osds, ignoring')
            gevent.sleep(delay)

    @log_exc
    def do_dump_ops(self):
        """
        Loops and does op dumps on all osds
        """
        self.log("starting do_dump_ops")
        while not self.stopping:
            for osd in self.live_osds:
                # Ignore errors because live_osds is in flux
                self.ceph_manager.osd_admin_socket(osd, command=['dump_ops_in_flight'],
                                     check_status=False, timeout=30, stdout=DEVNULL)
                self.ceph_manager.osd_admin_socket(osd, command=['dump_blocked_ops'],
                                     check_status=False, timeout=30, stdout=DEVNULL)
                self.ceph_manager.osd_admin_socket(osd, command=['dump_historic_ops'],
                                     check_status=False, timeout=30, stdout=DEVNULL)
            gevent.sleep(0)

    @log_exc
    def do_noscrub_toggle(self):
        """
        Loops and toggle noscrub flags

        Loop delay is controlled by the config value noscrub_toggle_delay.
        """
        delay = float(self.noscrub_toggle_delay)
        scrub_state = "none"
        self.log("starting do_noscrub_toggle with a delay of {0}".format(delay))
        while not self.stopping:
            if scrub_state == "none":
                self.ceph_manager.raw_cluster_cmd('osd', 'set', 'noscrub')
                scrub_state = "noscrub"
            elif scrub_state == "noscrub":
                self.ceph_manager.raw_cluster_cmd('osd', 'set', 'nodeep-scrub')
                scrub_state = "both"
            elif scrub_state == "both":
                self.ceph_manager.raw_cluster_cmd('osd', 'unset', 'noscrub')
                scrub_state = "nodeep-scrub"
            else:
                self.ceph_manager.raw_cluster_cmd('osd', 'unset', 'nodeep-scrub')
                scrub_state = "none"
            gevent.sleep(delay)
        self.ceph_manager.raw_cluster_cmd('osd', 'unset', 'noscrub')
        self.ceph_manager.raw_cluster_cmd('osd', 'unset', 'nodeep-scrub')

    @log_exc
    def _do_thrash(self):
        """
        Loop to select random actions to thrash ceph manager with.
        """
        cleanint = self.config.get("clean_interval", 60)
        scrubint = self.config.get("scrub_interval", -1)
        maxdead = self.config.get("max_dead", 0)
        delay = self.config.get("op_delay", 5)
        self.rerrosd = self.live_osds[0]
        if self.random_eio > 0:
            self.ceph_manager.inject_args('osd', self.rerrosd,
                                          'filestore_debug_random_read_err',
                                          self.random_eio)
            self.ceph_manager.inject_args('osd', self.rerrosd,
                                          'bluestore_debug_random_read_err',
                                          self.random_eio)
        self.log("starting do_thrash")
        while not self.stopping:
            to_log = [str(x) for x in ["in_osds: ", self.in_osds,
                                       "out_osds: ", self.out_osds,
                                       "dead_osds: ", self.dead_osds,
                                       "live_osds: ", self.live_osds]]
            self.log(" ".join(to_log))
            if random.uniform(0, 1) < (float(delay) / cleanint):
                while len(self.dead_osds) > maxdead:
                    self.revive_osd()
                for osd in self.in_osds:
                    self.ceph_manager.raw_cluster_cmd('osd', 'reweight',
                                                      str(osd), str(1))
                if random.uniform(0, 1) < float(
                        self.config.get('chance_test_map_discontinuity', 0)) \
                        and len(self.live_osds) > 5: # avoid m=2,k=2 stall, w/ some buffer for crush being picky
                    self.test_map_discontinuity()
                else:
                    self.ceph_manager.wait_for_recovery(
                        timeout=self.config.get('timeout')
                        )
                time.sleep(self.clean_wait)
                if scrubint > 0:
                    if random.uniform(0, 1) < (float(delay) / scrubint):
                        self.log('Scrubbing while thrashing being performed')
                        Scrubber(self.ceph_manager, self.config)
            self.choose_action()()
            time.sleep(delay)
        self.all_up()
        if self.random_eio > 0:
            self.ceph_manager.inject_args('osd', self.rerrosd,
                                          'filestore_debug_random_read_err', '0.0')
            self.ceph_manager.inject_args('osd', self.rerrosd,
                                          'bluestore_debug_random_read_err', '0.0')
        for pool in list(self.pools_to_fix_pgp_num):
            if self.ceph_manager.get_pool_pg_num(pool) > 0:
                self.fix_pgp_num(pool)
        self.pools_to_fix_pgp_num.clear()
        for service, opt, saved_value in self.saved_options:
            self.ceph_manager.inject_args(service, '*', opt, saved_value)
        self.saved_options = []
        self.all_up_in()


@contextlib.contextmanager
def task(ctx, config):
    """
    "Thrash" the OSDs by randomly marking them out/down (and then back
    in) until the task is ended. This loops, and every op_delay
    seconds it randomly chooses to add or remove an OSD (even odds)
    unless there are fewer than min_out OSDs out of the cluster, or
    more than min_in OSDs in the cluster.

    All commands are run on mon0 and it stops when __exit__ is called.

    The config is optional, and is a dict containing some or all of:

    cluster: (default 'ceph') the name of the cluster to thrash

    min_in: (default 4) the minimum number of OSDs to keep in the
       cluster

    min_out: (default 0) the minimum number of OSDs to keep out of the
       cluster

    op_delay: (5) the length of time to sleep between changing an
       OSD's status

    min_dead: (0) minimum number of osds to leave down/dead.

    max_dead: (0) maximum number of osds to leave down/dead before waiting
       for clean.  This should probably be num_replicas - 1.

    clean_interval: (60) the approximate length of time to loop before
       waiting until the cluster goes clean. (In reality this is used
       to probabilistically choose when to wait, and the method used
       makes it closer to -- but not identical to -- the half-life.)

    scrub_interval: (-1) the approximate length of time to loop before
       waiting until a scrub is performed while cleaning. (In reality
       this is used to probabilistically choose when to wait, and it
       only applies to the cases where cleaning is being performed).
       -1 is used to indicate that no scrubbing will be done.

    chance_down: (0.4) the probability that the thrasher will mark an
       OSD down rather than marking it out. (The thrasher will not
       consider that OSD out of the cluster, since presently an OSD
       wrongly marked down will mark itself back up again.) This value
       can be either an integer (eg, 75) or a float probability (eg
       0.75).

    chance_test_min_size: (0) chance to run test_pool_min_size,
       which:
       - kills all but one osd
       - waits
       - kills that osd
       - revives all other osds
       - verifies that the osds fully recover

    timeout: (360) the number of seconds to wait for the cluster
       to become clean after each cluster change. If this doesn't
       happen within the timeout, an exception will be raised.

    revive_timeout: (150) number of seconds to wait for an osd asok to
       appear after attempting to revive the osd

    thrash_primary_affinity: (true) randomly adjust primary-affinity

    chance_pgnum_grow: (0) chance to increase a pool's size
    chance_pgpnum_fix: (0) chance to adjust pgpnum to pg for a pool
    pool_grow_by: (10) amount to increase pgnum by
    chance_pgnum_shrink: (0) chance to decrease a pool's size
    pool_shrink_by: (10) amount to decrease pgnum by
    max_pgs_per_pool_osd: (1200) don't expand pools past this size per osd

    pause_short: (3) duration of short pause
    pause_long: (80) duration of long pause
    pause_check_after: (50) assert osd down after this long
    chance_inject_pause_short: (1) chance of injecting short stall
    chance_inject_pause_long: (0) chance of injecting long stall

    clean_wait: (0) duration to wait before resuming thrashing once clean

    sighup_delay: (0.1) duration to delay between sending signal.SIGHUP to a
                  random live osd

    powercycle: (false) whether to power cycle the node instead
        of just the osd process. Note that this assumes that a single
        osd is the only important process on the node.

    bdev_inject_crash: (0) seconds to delay while inducing a synthetic crash.
        the delay lets the BlockDevice "accept" more aio operations but blocks
        any flush, and then eventually crashes (losing some or all ios).  If 0,
        no bdev failure injection is enabled.

    bdev_inject_crash_probability: (.5) probability of doing a bdev failure
        injection crash vs a normal OSD kill.

    chance_test_backfill_full: (0) chance to simulate full disks stopping
        backfill

    chance_test_map_discontinuity: (0) chance to test map discontinuity
    map_discontinuity_sleep_time: (40) time to wait for map trims

    ceph_objectstore_tool: (true) whether to export/import a pg while an osd is down
    chance_move_pg: (1.0) chance of moving a pg if more than 1 osd is down (default 100%)

    optrack_toggle_delay: (2.0) duration to delay between toggling op tracker
                  enablement to all osds

    dump_ops_enable: (true) continuously dump ops on all live osds

    noscrub_toggle_delay: (2.0) duration to delay between toggling noscrub

    disable_objectstore_tool_tests: (false) disable ceph_objectstore_tool based
                                    tests

    chance_thrash_cluster_full: .05

    chance_thrash_pg_upmap: 1.0
    chance_thrash_pg_upmap_items: 1.0

    aggressive_pg_num_changes: (true)  whether we should bypass the careful throttling of pg_num and pgp_num changes in mgr's adjust_pgs() controller

    example:

    tasks:
    - ceph:
    - thrashosds:
        cluster: ceph
        chance_down: 10
        op_delay: 3
        min_in: 1
        timeout: 600
    - interactive:
    """
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'thrashosds task only accepts a dict for configuration'
    # add default value for sighup_delay
    config['sighup_delay'] = config.get('sighup_delay', 0.1)
    # add default value for optrack_toggle_delay
    config['optrack_toggle_delay'] = config.get('optrack_toggle_delay', 2.0)
    # add default value for dump_ops_enable
    config['dump_ops_enable'] = config.get('dump_ops_enable', "true")
    # add default value for noscrub_toggle_delay
    config['noscrub_toggle_delay'] = config.get('noscrub_toggle_delay', 2.0)
    # add default value for random_eio
    config['random_eio'] = config.get('random_eio', 0.0)
    aggro = config.get('aggressive_pg_num_changes', True)

    log.info("config is {config}".format(config=str(config)))

    overrides = ctx.config.get('overrides', {})
    log.info("overrides is {overrides}".format(overrides=str(overrides)))
    teuthology.deep_merge(config, overrides.get('thrashosds', {}))
    cluster = config.get('cluster', 'ceph')

    log.info("config is {config}".format(config=str(config)))

    if 'powercycle' in config:

        # sync everyone first to avoid collateral damage to / etc.
        log.info('Doing preliminary sync to avoid collateral damage...')
        ctx.cluster.run(args=['sync'])

        if 'ipmi_user' in ctx.teuthology_config:
            for remote in ctx.cluster.remotes.keys():
                log.debug('checking console status of %s' % remote.shortname)
                if not remote.console.check_status():
                    log.warn('Failed to get console status for %s',
                             remote.shortname)

            # check that all osd remotes have a valid console
            osds = ctx.cluster.only(teuthology.is_type('osd', cluster))
            for remote in osds.remotes.keys():
                if not remote.console.has_ipmi_credentials:
                    raise Exception(
                        'IPMI console required for powercycling, '
                        'but not available on osd role: {r}'.format(
                            r=remote.name))

    cluster_manager = ctx.managers[cluster]
    for f in ['powercycle', 'bdev_inject_crash']:
        if config.get(f):
            cluster_manager.config[f] = config.get(f)

    if aggro:
        cluster_manager.raw_cluster_cmd(
            'config', 'set', 'mgr',
            'mgr_debug_aggressive_pg_num_changes',
            'true')

    log.info('Beginning thrashosds...')
    thrash_proc = OSDThrasher(
        cluster_manager,
        config,
        logger=log.getChild('thrasher')
        )
    ctx.ceph[cluster].thrashers.append(thrash_proc)
    try:
        yield
    finally:
        log.info('joining thrashosds')
        thrash_proc.do_join()
        cluster_manager.wait_for_all_osds_up()
        cluster_manager.flush_all_pg_stats()
        cluster_manager.wait_for_recovery(config.get('timeout', 360))
        if aggro:
            cluster_manager.raw_cluster_cmd(
                'config', 'rm', 'mgr',
                'mgr_debug_aggressive_pg_num_changes')
