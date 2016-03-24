"""
ceph manager -- Thrasher and CephManager objects
"""
from cStringIO import StringIO
from functools import wraps
import contextlib
import random
import signal
import time
import gevent
import base64
import json
import logging
import threading
import traceback
import os
from teuthology import misc as teuthology
from tasks.scrub import Scrubber
from util.rados import cmd_erasure_code_profile
from teuthology.orchestra.remote import Remote
from teuthology.orchestra import run
from teuthology.exceptions import CommandFailedError


DEFAULT_CONF_PATH = '/etc/ceph/ceph.conf'

log = logging.getLogger(__name__)


def write_conf(ctx, conf_path=DEFAULT_CONF_PATH, cluster='ceph'):
    conf_fp = StringIO()
    ctx.ceph[cluster].conf.write(conf_fp)
    conf_fp.seek(0)
    writes = ctx.cluster.run(
        args=[
            'sudo', 'mkdir', '-p', '/etc/ceph', run.Raw('&&'),
            'sudo', 'chmod', '0755', '/etc/ceph', run.Raw('&&'),
            'sudo', 'python',
            '-c',
            ('import shutil, sys; '
             'shutil.copyfileobj(sys.stdin, file(sys.argv[1], "wb"))'),
            conf_path,
            run.Raw('&&'),
            'sudo', 'chmod', '0644', conf_path,
        ],
        stdin=run.PIPE,
        wait=False)
    teuthology.feed_many_stdins_and_close(conf_fp, writes)
    run.wait(writes)


def mount_osd_data(ctx, remote, cluster, osd):
    """
    Mount a remote OSD

    :param ctx: Context
    :param remote: Remote site
    :param cluster: name of ceph cluster
    :param osd: Osd name
    """
    log.debug('Mounting data for osd.{o} on {r}'.format(o=osd, r=remote))
    role = "{0}.osd.{1}".format(cluster, osd)
    alt_role = role if cluster != 'ceph' else "osd.{0}".format(osd)
    if remote in ctx.disk_config.remote_to_roles_to_dev:
        if alt_role in ctx.disk_config.remote_to_roles_to_dev[remote]:
            role = alt_role
        if role not in ctx.disk_config.remote_to_roles_to_dev[remote]:
            return
        dev = ctx.disk_config.remote_to_roles_to_dev[remote][role]
        mount_options = ctx.disk_config.\
            remote_to_roles_to_dev_mount_options[remote][role]
        fstype = ctx.disk_config.remote_to_roles_to_dev_fstype[remote][role]
        mnt = os.path.join('/var/lib/ceph/osd', '{0}-{1}'.format(cluster, osd))

        log.info('Mounting osd.{o}: dev: {n}, cluster: {c}'
                 'mountpoint: {p}, type: {t}, options: {v}'.format(
                     o=osd, n=remote.name, p=mnt, t=fstype, v=mount_options,
                     c=cluster))

        remote.run(
            args=[
                'sudo',
                'mount',
                '-t', fstype,
                '-o', ','.join(mount_options),
                dev,
                mnt,
            ]
            )


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
        self.revive_timeout = self.config.get("revive_timeout", 150)
        if self.config.get('powercycle'):
            self.revive_timeout += 120
        self.clean_wait = self.config.get('clean_wait', 0)
        self.minin = self.config.get("min_in", 3)
        self.chance_move_pg = self.config.get('chance_move_pg', 1.0)
        self.sighup_delay = self.config.get('sighup_delay')
        self.optrack_toggle_delay = self.config.get('optrack_toggle_delay')
        self.dump_ops_enable = self.config.get('dump_ops_enable')
        self.noscrub_toggle_delay = self.config.get('noscrub_toggle_delay')

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
        if self.sighup_delay:
            self.sighup_thread = gevent.spawn(self.do_sighup)
        if self.optrack_toggle_delay:
            self.optrack_toggle_thread = gevent.spawn(self.do_optrack_toggle)
        if self.dump_ops_enable == "true":
            self.dump_ops_thread = gevent.spawn(self.do_dump_ops)
        if self.noscrub_toggle_delay:
            self.noscrub_toggle_thread = gevent.spawn(self.do_noscrub_toggle)
        if self.config.get('powercycle') or not self.cmd_exists_on_osds("ceph-objectstore-tool"):
            self.ceph_objectstore_tool = False
            self.test_rm_past_intervals = False
            if self.config.get('powercycle'):
                self.log("Unable to test ceph-objectstore-tool, "
                         "powercycle testing")
            else:
                self.log("Unable to test ceph-objectstore-tool, "
                         "not available on all OSD nodes")
        else:
            self.ceph_objectstore_tool = \
                self.config.get('ceph_objectstore_tool', True)
            self.test_rm_past_intervals = \
                self.config.get('test_rm_past_intervals', True)

    def cmd_exists_on_osds(self, cmd):
        allremotes = self.ceph_manager.ctx.cluster.only(\
            teuthology.is_type('osd')).remotes.keys()
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
            (remote,) = self.ceph_manager.ctx.\
                cluster.only('osd.{o}'.format(o=osd)).remotes.iterkeys()
            FSPATH = self.ceph_manager.get_filepath()
            JPATH = os.path.join(FSPATH, "journal")
            exp_osd = imp_osd = osd
            exp_remote = imp_remote = remote
            # If an older osd is available we'll move a pg from there
            if (len(self.dead_osds) > 1 and
                    random.random() < self.chance_move_pg):
                exp_osd = random.choice(self.dead_osds[:-1])
                (exp_remote,) = self.ceph_manager.ctx.\
                    cluster.only('osd.{o}'.format(o=exp_osd)).\
                    remotes.iterkeys()
            if ('keyvaluestore_backend' in
                    self.ceph_manager.ctx.ceph.conf['osd']):
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
            proc = exp_remote.run(args=cmd, wait=True,
                                  check_status=False, stdout=StringIO())
            if proc.exitstatus:
                raise Exception("ceph-objectstore-tool: "
                                "exp list-pgs failure with status {ret}".
                                format(ret=proc.exitstatus))
            pgs = proc.stdout.getvalue().split('\n')[:-1]
            if len(pgs) == 0:
                self.log("No PGs found for osd.{osd}".format(osd=exp_osd))
                return
            pg = random.choice(pgs)
            exp_path = teuthology.get_testdir(self.ceph_manager.ctx)
            exp_path = os.path.join(exp_path, "data")
            exp_path = os.path.join(exp_path,
                                    "exp.{pg}.{id}".format(pg=pg, id=exp_osd))
            # export
            cmd = prefix + "--op export --pgid {pg} --file {file}"
            cmd = cmd.format(id=exp_osd, pg=pg, file=exp_path)
            proc = exp_remote.run(args=cmd)
            if proc.exitstatus:
                raise Exception("ceph-objectstore-tool: "
                                "export failure with status {ret}".
                                format(ret=proc.exitstatus))
            # remove
            cmd = prefix + "--op remove --pgid {pg}"
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
            proc = imp_remote.run(args=cmd, wait=True, check_status=False)
            if proc.exitstatus == 10:
                self.log("Pool went away before processing an import"
                         "...ignored")
            elif proc.exitstatus == 11:
                self.log("Attempt to import an incompatible export"
                         "...ignored")
            elif proc.exitstatus:
                raise Exception("ceph-objectstore-tool: "
                                "import failure with status {ret}".
                                format(ret=proc.exitstatus))
            cmd = "rm -f {file}".format(file=exp_path)
            exp_remote.run(args=cmd)
            if imp_remote != exp_remote:
                imp_remote.run(args=cmd)

    def rm_past_intervals(self, osd=None):
        """
        :param osd: Osd to find pg to remove past intervals
        """
        if self.test_rm_past_intervals:
            if osd is None:
                osd = random.choice(self.dead_osds)
            self.log("Use ceph_objectstore_tool to remove past intervals")
            (remote,) = self.ceph_manager.ctx.\
                cluster.only('osd.{o}'.format(o=osd)).remotes.iterkeys()
            FSPATH = self.ceph_manager.get_filepath()
            JPATH = os.path.join(FSPATH, "journal")
            if ('keyvaluestore_backend' in
                    self.ceph_manager.ctx.ceph.conf['osd']):
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
            cmd = (prefix + "--op list-pgs").format(id=osd)
            proc = remote.run(args=cmd, wait=True,
                              check_status=False, stdout=StringIO())
            if proc.exitstatus:
                raise Exception("ceph_objectstore_tool: "
                                "exp list-pgs failure with status {ret}".
                                format(ret=proc.exitstatus))
            pgs = proc.stdout.getvalue().split('\n')[:-1]
            if len(pgs) == 0:
                self.log("No PGs found for osd.{osd}".format(osd=osd))
                return
            pg = random.choice(pgs)
            cmd = (prefix + "--op rm-past-intervals --pgid {pg}").\
                format(id=osd, pg=pg)
            proc = remote.run(args=cmd)
            if proc.exitstatus:
                raise Exception("ceph_objectstore_tool: "
                                "rm-past-intervals failure with status {ret}".
                                format(ret=proc.exitstatus))

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

    def reweight_osd(self, osd=None):
        """
        Reweight an osd that is in
        :param osd: Osd to be marked.
        """
        if osd is None:
            osd = random.choice(self.in_osds)
        val = random.uniform(.1, 1.0)
        self.log("Reweighting osd %s to %s" % (str(osd), str(val)))
        self.ceph_manager.raw_cluster_cmd('osd', 'reweight',
                                          str(osd), str(val))

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
        self.ceph_manager.expand_pool(pool,
                                      self.config.get('pool_grow_by', 10),
                                      self.max_pgs)

    def fix_pgp_num(self):
        """
        Fix number of pgs in pool.
        """
        pool = self.ceph_manager.get_pool()
        self.log("fixing pg num pool %s" % (pool,))
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
                osd_debug_skip_full_check_in_backfill_reservation=
                random.choice(['false', 'true']),
                osd_backfill_full_ratio=0)
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
                osd_debug_skip_full_check_in_backfill_reservation='false',
                osd_backfill_full_ratio=0.85)

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
        chance_test_backfill_full = \
            self.config.get('chance_test_backfill_full', 0)
        if isinstance(chance_down, int):
            chance_down = float(chance_down) / 100
        minin = self.minin
        minout = self.config.get("min_out", 0)
        minlive = self.config.get("min_live", 2)
        mindead = self.config.get("min_dead", 0)

        self.log('choose_action: min_in %d min_out '
                 '%d min_live %d min_dead %d' %
                 (minin, minout, minlive, mindead))
        actions = []
        if len(self.in_osds) > minin:
            actions.append((self.out_osd, 1.0,))
        if len(self.live_osds) > minlive and chance_down > 0:
            actions.append((self.kill_osd, chance_down,))
        if len(self.dead_osds) > 1:
            actions.append((self.rm_past_intervals, 1.0,))
        if len(self.out_osds) > minout:
            actions.append((self.in_osd, 1.7,))
        if len(self.dead_osds) > mindead:
            actions.append((self.revive_osd, 1.0,))
        if self.config.get('thrash_primary_affinity', True):
            actions.append((self.primary_affinity, 1.0,))
        actions.append((self.reweight_osd,
                        self.config.get('reweight_osd', .5),))
        actions.append((self.grow_pool,
                        self.config.get('chance_pgnum_grow', 0),))
        actions.append((self.fix_pgp_num,
                        self.config.get('chance_pgpnum_fix', 0),))
        actions.append((self.test_pool_min_size,
                        chance_test_min_size,))
        actions.append((self.test_backfill_full,
                        chance_test_backfill_full,))
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
            self.ceph_manager.raw_cluster_cmd_result('tell', 'osd.*',
                             'injectargs', '--osd_enable_op_tracker=%s' % osd_state)
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
                                     check_status=False, timeout=30)
                self.ceph_manager.osd_admin_socket(osd, command=['dump_blocked_ops'],
                                     check_status=False, timeout=30)
                self.ceph_manager.osd_admin_socket(osd, command=['dump_historic_ops'],
                                     check_status=False, timeout=30)
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
    def do_thrash(self):
        """
        Loop to select random actions to thrash ceph manager with.
        """
        cleanint = self.config.get("clean_interval", 60)
        scrubint = self.config.get("scrub_interval", -1)
        maxdead = self.config.get("max_dead", 0)
        delay = self.config.get("op_delay", 5)
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
                        self.config.get('chance_test_map_discontinuity', 0)):
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


class ObjectStoreTool:

    def __init__(self, manager, pool, **kwargs):
        self.manager = manager
        self.pool = pool
        self.osd = kwargs.get('osd', None)
        self.object_name = kwargs.get('object_name', None)
        if self.osd and self.pool and self.object_name:
            if self.osd == "primary":
                self.osd = self.manager.get_object_primary(self.pool,
                                                           self.object_name)
        assert self.osd
        if self.object_name:
            self.pgid = self.manager.get_object_pg_with_shard(self.pool,
                                                              self.object_name,
                                                              self.osd)
        self.remote = self.manager.ctx.\
            cluster.only('osd.{o}'.format(o=self.osd)).remotes.keys()[0]
        path = self.manager.get_filepath().format(id=self.osd)
        self.paths = ("--data-path {path} --journal-path {path}/journal".
                      format(path=path))

    def build_cmd(self, options, args, stdin):
        lines = []
        if self.object_name:
            lines.append("object=$(sudo adjust-ulimits ceph-objectstore-tool "
                         "{paths} --pgid {pgid} --op list |"
                         "grep '\"oid\":\"{name}\"')".
                         format(paths=self.paths,
                                pgid=self.pgid,
                                name=self.object_name))
            args = '"$object" ' + args
            options += " --pgid {pgid}".format(pgid=self.pgid)
        cmd = ("sudo adjust-ulimits ceph-objectstore-tool {paths} {options} {args}".
               format(paths=self.paths,
                      args=args,
                      options=options))
        if stdin:
            cmd = ("echo {payload} | base64 --decode | {cmd}".
                   format(payload=base64.encode(stdin),
                          cmd=cmd))
        lines.append(cmd)
        return "\n".join(lines)

    def run(self, options, args, stdin=None):
        self.manager.kill_osd(self.osd)
        cmd = self.build_cmd(options, args, stdin)
        self.manager.log(cmd)
        try:
            proc = self.remote.run(args=['bash', '-e', '-x', '-c', cmd],
                                   check_status=False,
                                   stdout=StringIO(),
                                   stderr=StringIO())
            proc.wait()
            if proc.exitstatus != 0:
                self.manager.log("failed with " + str(proc.exitstatus))
                error = proc.stdout.getvalue() + " " + proc.stderr.getvalue()
                raise Exception(error)
        finally:
            self.manager.revive_osd(self.osd)


class CephManager:
    """
    Ceph manager object.
    Contains several local functions that form a bulk of this module.
    """

    REPLICATED_POOL = 1
    ERASURE_CODED_POOL = 3

    def __init__(self, controller, ctx=None, config=None, logger=None,
                 cluster='ceph'):
        self.lock = threading.RLock()
        self.ctx = ctx
        self.config = config
        self.controller = controller
        self.next_pool_id = 0
        self.cluster = cluster
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
            # we may race with a pool deletion; ignore failures here
            try:
                self.pools[pool] = self.get_pool_property(pool, 'pg_num')
            except CommandFailedError:
                self.log('Failed to get pg_num from pool %s, ignoring' % pool)

    def raw_cluster_cmd(self, *args):
        """
        Start ceph on a raw cluster.  Return count
        """
        testdir = teuthology.get_testdir(self.ctx)
        ceph_args = [
            'sudo',
            'adjust-ulimits',
            'ceph-coverage',
            '{tdir}/archive/coverage'.format(tdir=testdir),
            'timeout',
            '120',
            'ceph',
            '--cluster',
            self.cluster,
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
            'sudo',
            'adjust-ulimits',
            'ceph-coverage',
            '{tdir}/archive/coverage'.format(tdir=testdir),
            'timeout',
            '120',
            'ceph',
            '--cluster',
            self.cluster,
        ]
        ceph_args.extend(args)
        proc = self.controller.run(
            args=ceph_args,
            check_status=False,
            )
        return proc.exitstatus

    def run_ceph_w(self):
        """
        Execute "ceph -w" in the background with stdout connected to a StringIO,
        and return the RemoteProcess.
        """
        return self.controller.run(
            args=["sudo",
                  "daemon-helper",
                  "kill",
                  "ceph",
                  '--cluster',
                  self.cluster,
                  "-w"],
            wait=False, stdout=StringIO(), stdin=run.PIPE)

    def do_rados(self, remote, cmd, check_status=True):
        """
        Execute a remote rados command.
        """
        testdir = teuthology.get_testdir(self.ctx)
        pre = [
            'adjust-ulimits',
            'ceph-coverage',
            '{tdir}/archive/coverage'.format(tdir=testdir),
            'rados',
            '--cluster',
            self.cluster,
            ]
        pre.extend(cmd)
        proc = remote.run(
            args=pre,
            wait=True,
            check_status=check_status
            )
        return proc

    def rados_write_objects(self, pool, num_objects, size,
                            timelimit, threads, cleanup=False):
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
        if not cleanup:
            args.append('--no-cleanup')
        return self.do_rados(self.controller, map(str, args))

    def do_put(self, pool, obj, fname, namespace=None):
        """
        Implement rados put operation
        """
        args = ['-p', pool]
        if namespace is not None:
            args += ['-N', namespace]
        args += [
            'put',
            obj,
            fname
        ]
        return self.do_rados(
            self.controller,
            args,
            check_status=False
        ).exitstatus

    def do_get(self, pool, obj, fname='/dev/null', namespace=None):
        """
        Implement rados get operation
        """
        args = ['-p', pool]
        if namespace is not None:
            args += ['-N', namespace]
        args += [
            'get',
            obj,
            fname
        ]
        return self.do_rados(
            self.controller,
            args,
            check_status=False
        ).exitstatus

    def do_rm(self, pool, obj, namespace=None):
        """
        Implement rados rm operation
        """
        args = ['-p', pool]
        if namespace is not None:
            args += ['-N', namespace]
        args += [
            'rm',
            obj
        ]
        return self.do_rados(
            self.controller,
            args,
            check_status=False
        ).exitstatus

    def osd_admin_socket(self, osd_id, command, check_status=True, timeout=0):
        return self.admin_socket('osd', osd_id, command, check_status, timeout)

    def find_remote(self, service_type, service_id):
        """
        Get the Remote for the host where a particular service runs.

        :param service_type: 'mds', 'osd', 'client'
        :param service_id: The second part of a role, e.g. '0' for
                           the role 'client.0'
        :return: a Remote instance for the host where the
                 requested role is placed
        """
        def _is_instance(role):
            role_tuple = teuthology.split_role(role)
            return role_tuple == (self.cluster, service_type, str(service_id))
        try:
            (remote,) = self.ctx.cluster.only(_is_instance).remotes.keys()
        except ValueError:
            raise KeyError("Service {0}.{1} not found".format(service_type,
                                                              service_id))
        return remote

    def admin_socket(self, service_type, service_id,
                     command, check_status=True, timeout=0):
        """
        Remotely start up ceph specifying the admin socket
        :param command: a list of words to use as the command
                        to the admin socket
        """
        testdir = teuthology.get_testdir(self.ctx)
        remote = self.find_remote(service_type, service_id)
        args = [
            'sudo',
            'adjust-ulimits',
            'ceph-coverage',
            '{tdir}/archive/coverage'.format(tdir=testdir),
            'timeout',
            str(timeout),
            'ceph',
            '--cluster',
            self.cluster,
            '--admin-daemon',
            '/var/run/ceph/{cluster}-{type}.{id}.asok'.format(
                cluster=self.cluster,
                type=service_type,
                id=service_id),
            ]
        args.extend(command)
        return remote.run(
            args=args,
            stdout=StringIO(),
            wait=True,
            check_status=check_status
            )

    def objectstore_tool(self, pool, options, args, **kwargs):
        return ObjectStoreTool(self, pool, **kwargs).run(options, args)

    def get_pgid(self, pool, pgnum):
        """
        :param pool: pool name
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
        return int(self.get_pool_dump(pool)['pool'])

    def list_pools(self):
        """
        list all pool names
        """
        osd_dump = self.get_osd_dump_json()
        self.log(osd_dump['pools'])
        return [str(i['pool_name']) for i in osd_dump['pools']]

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

    def wait_run_admin_socket(self, service_type,
                              service_id, args=['version'], timeout=75):
        """
        If osd_admin_socket call suceeds, return.  Otherwise wait
        five seconds and try again.
        """
        tries = 0
        while True:
            proc = self.admin_socket(service_type, service_id,
                                     args, check_status=False)
            if proc.exitstatus is 0:
                break
            else:
                tries += 1
                if (tries * 5) > timeout:
                    raise Exception('timed out waiting for admin_socket '
                                    'to appear after {type}.{id} restart'.
                                    format(type=service_type,
                                           id=service_id))
                self.log("waiting on admin_socket for {type}-{id}, "
                         "{command}".format(type=service_type,
                                            id=service_id,
                                            command=args))
                time.sleep(5)

    def get_pool_dump(self, pool):
        """
        get the osd dump part of a pool
        """
        osd_dump = self.get_osd_dump_json()
        for i in osd_dump['pools']:
            if i['pool_name'] == pool:
                return i
        assert False

    def set_config(self, osdnum, **argdict):
        """
        :param osdnum: osd number
        :param argdict: dictionary containing values to set.
        """
        for k, v in argdict.iteritems():
            self.wait_run_admin_socket(
                'osd', osdnum,
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
        in_osds = [int(i[4:].split()[0])
                   for i in filter(lambda x: " in " in x, osd_lines)]
        out_osds = [int(i[4:].split()[0])
                    for i in filter(lambda x: " out " in x, osd_lines)]
        up_osds = [int(i[4:].split()[0])
                   for i in filter(lambda x: " up " in x, osd_lines)]
        down_osds = [int(i[4:].split()[0])
                     for i in filter(lambda x: " down " in x, osd_lines)]
        dead_osds = [int(x.id_)
                     for x in filter(lambda x:
                                     not x.running(),
                                     self.ctx.daemons.
                                     iter_daemons_of_role('osd', self.cluster))]
        live_osds = [int(x.id_) for x in
                     filter(lambda x:
                            x.running(),
                            self.ctx.daemons.iter_daemons_of_role('osd',
                                                                  self.cluster))]
        return {'in': in_osds, 'out': out_osds, 'up': up_osds,
                'down': down_osds, 'dead': dead_osds, 'live': live_osds,
                'raw': osd_lines}

    def get_num_pgs(self):
        """
        Check cluster status for the number of pgs
        """
        status = self.raw_cluster_status()
        self.log(status)
        return status['pgmap']['num_pgs']

    def create_erasure_code_profile(self, profile_name, profile):
        """
        Create an erasure code profile name that can be used as a parameter
        when creating an erasure coded pool.
        """
        with self.lock:
            args = cmd_erasure_code_profile(profile_name, profile)
            self.raw_cluster_cmd(*args)

    def create_pool_with_unique_name(self, pg_num=16,
                                     erasure_code_profile_name=None):
        """
        Create a pool named unique_pool_X where X is unique.
        """
        name = ""
        with self.lock:
            name = "unique_pool_%s" % (str(self.next_pool_id),)
            self.next_pool_id += 1
            self.create_pool(
                name,
                pg_num,
                erasure_code_profile_name=erasure_code_profile_name)
        return name

    @contextlib.contextmanager
    def pool(self, pool_name, pg_num=16, erasure_code_profile_name=None):
        self.create_pool(pool_name, pg_num, erasure_code_profile_name)
        yield
        self.remove_pool(pool_name)

    def create_pool(self, pool_name, pg_num=16,
                    erasure_code_profile_name=None):
        """
        Create a pool named from the pool_name parameter.
        :param pool_name: name of the pool being created.
        :param pg_num: initial number of pgs.
        :param erasure_code_profile_name: if set and !None create an
                                          erasure coded pool using the profile
        """
        with self.lock:
            assert isinstance(pool_name, basestring)
            assert isinstance(pg_num, int)
            assert pool_name not in self.pools
            self.log("creating pool_name %s" % (pool_name,))
            if erasure_code_profile_name:
                self.raw_cluster_cmd('osd', 'pool', 'create',
                                     pool_name, str(pg_num), str(pg_num),
                                     'erasure', erasure_code_profile_name)
            else:
                self.raw_cluster_cmd('osd', 'pool', 'create',
                                     pool_name, str(pg_num))
            self.pools[pool_name] = pg_num
        time.sleep(1)

    def add_pool_snap(self, pool_name, snap_name):
        """
        Add pool snapshot
        :param pool_name: name of pool to snapshot
        :param snap_name: name of snapshot to take
        """
        self.raw_cluster_cmd('osd', 'pool', 'mksnap',
                             str(pool_name), str(snap_name))

    def remove_pool_snap(self, pool_name, snap_name):
        """
        Remove pool snapshot
        :param pool_name: name of pool to snapshot
        :param snap_name: name of snapshot to remove
        """
        self.raw_cluster_cmd('osd', 'pool', 'rmsnap',
                             str(pool_name), str(snap_name))

    def remove_pool(self, pool_name):
        """
        Remove the indicated pool
        :param pool_name: Pool to be removed
        """
        with self.lock:
            assert isinstance(pool_name, basestring)
            assert pool_name in self.pools
            self.log("removing pool_name %s" % (pool_name,))
            del self.pools[pool_name]
            self.do_rados(self.controller,
                          ['rmpool', pool_name, pool_name,
                           "--yes-i-really-really-mean-it"])

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
            assert isinstance(pool_name, basestring)
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
            assert isinstance(pool_name, basestring)
            assert isinstance(prop, basestring)
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
            assert isinstance(pool_name, basestring)
            assert isinstance(prop, basestring)
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
                if r != 11:  # EAGAIN
                    break
                tries += 1
                if tries > 50:
                    raise Exception('timed out getting EAGAIN '
                                    'when setting pool property %s %s = %s' %
                                    (pool_name, prop, val))
                self.log('got EAGAIN setting pool property, '
                         'waiting a few seconds...')
                time.sleep(2)

    def expand_pool(self, pool_name, by, max_pgs):
        """
        Increase the number of pgs in a pool
        """
        with self.lock:
            assert isinstance(pool_name, basestring)
            assert isinstance(by, int)
            assert pool_name in self.pools
            if self.get_num_creating() > 0:
                return
            if (self.pools[pool_name] + by) > max_pgs:
                return
            self.log("increase pool size by %d" % (by,))
            new_pg_num = self.pools[pool_name] + by
            self.set_pool_property(pool_name, "pg_num", new_pg_num)
            self.pools[pool_name] = new_pg_num

    def set_pool_pgpnum(self, pool_name):
        """
        Set pgpnum property of pool_name pool.
        """
        with self.lock:
            assert isinstance(pool_name, basestring)
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
        while init == self.get_last_scrub_stamp(pool, pgnum):
            self.log("waiting for scrub type %s" % (stype,))
            self.raw_cluster_cmd('pg', stype, self.get_pgid(pool, pgnum))
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

    def get_object_pg_with_shard(self, pool, name, osdid):
        """
        """
        pool_dump = self.get_pool_dump(pool)
        object_map = self.get_object_map(pool, name)
        if pool_dump["type"] == CephManager.ERASURE_CODED_POOL:
            shard = object_map['acting'].index(osdid)
            return "{pgid}s{shard}".format(pgid=object_map['pgid'],
                                           shard=shard)
        else:
            return object_map['pgid']

    def get_object_primary(self, pool, name):
        """
        """
        object_map = self.get_object_map(pool, name)
        return object_map['acting_primary']

    def get_object_map(self, pool, name):
        """
        osd map --format=json converted to a python object
        :returns: the python object
        """
        out = self.raw_cluster_cmd('--format=json', 'osd', 'map', pool, name)
        return json.loads('\n'.join(out.split('\n')[1:]))

    def get_osd_dump_json(self):
        """
        osd dump --format=json converted to a python object
        :returns: the python object
        """
        out = self.raw_cluster_cmd('osd', 'dump', '--format=json')
        return json.loads('\n'.join(out.split('\n')[1:]))

    def get_osd_dump(self):
        """
        Dump osds
        :returns: all osds
        """
        return self.get_osd_dump_json()['osds']

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
            if (pg['state'].count('active') and
                    pg['state'].count('clean') and
                    not pg['state'].count('stale')):
                num += 1
        return num

    def get_num_active_recovered(self):
        """
        Find the number of active and recovered pgs.
        """
        pgs = self.get_pg_stats()
        num = 0
        for pg in pgs:
            if (pg['state'].count('active') and
                    not pg['state'].count('recover') and
                    not pg['state'].count('backfill') and
                    not pg['state'].count('stale')):
                num += 1
        return num

    def get_is_making_recovery_progress(self):
        """
        Return whether there is recovery progress discernable in the
        raw cluster status
        """
        status = self.raw_cluster_status()
        kps = status['pgmap'].get('recovering_keys_per_sec', 0)
        bps = status['pgmap'].get('recovering_bytes_per_sec', 0)
        ops = status['pgmap'].get('recovering_objects_per_sec', 0)
        return kps > 0 or bps > 0 or ops > 0

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
            if ((pg['state'].count('down') and not
                    pg['state'].count('stale')) or
                (pg['state'].count('incomplete') and not
                    pg['state'].count('stale'))):
                num += 1
        return num

    def get_num_active_down(self):
        """
        Find the number of pgs that are either active or down.
        """
        pgs = self.get_pg_stats()
        num = 0
        for pg in pgs:
            if ((pg['state'].count('active') and not
                    pg['state'].count('stale')) or
                (pg['state'].count('down') and not
                    pg['state'].count('stale')) or
                (pg['state'].count('incomplete') and not
                    pg['state'].count('stale'))):
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
        Returns true when all pgs are clean.
        """
        self.log("waiting for clean")
        start = time.time()
        num_active_clean = self.get_num_active_clean()
        while not self.is_clean():
            if timeout is not None:
                if self.get_is_making_recovery_progress():
                    self.log("making progress, resetting timeout")
                    start = time.time()
                else:
                    self.log("no progress seen, keeping timeout for now")
                    if time.time() - start >= timeout:
                        self.log('dumping pgs')
                        out = self.raw_cluster_cmd('pg', 'dump')
                        self.log(out)
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
        return (len(x) == sum([(y['up'] > 0) for y in x]))

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
            now = time.time()
            if timeout is not None:
                if self.get_is_making_recovery_progress():
                    self.log("making progress, resetting timeout")
                    start = time.time()
                else:
                    self.log("no progress seen, keeping timeout for now")
                    if now - start >= timeout:
                        self.log('dumping pgs')
                        out = self.raw_cluster_cmd('pg', 'dump')
                        self.log(out)
                        assert now - start < timeout, \
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
                if time.time() - start >= timeout:
                    self.log('dumping pgs')
                    out = self.raw_cluster_cmd('pg', 'dump')
                    self.log(out)
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
                if time.time() - start >= timeout:
                    self.log('dumping pgs')
                    out = self.raw_cluster_cmd('pg', 'dump')
                    self.log(out)
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
        Wrapper to check if all pgs are active
        """
        return self.get_num_active() == self.get_num_pgs()

    def wait_till_active(self, timeout=None):
        """
        Wait until all pgs are active.
        """
        self.log("waiting till active")
        start = time.time()
        while not self.is_active():
            if timeout is not None:
                if time.time() - start >= timeout:
                    self.log('dumping pgs')
                    out = self.raw_cluster_cmd('pg', 'dump')
                    self.log(out)
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
            remote = self.find_remote('osd', osd)
            self.log('kill_osd on osd.{o} '
                     'doing powercycle of {s}'.format(o=osd, s=remote.name))
            assert remote.console is not None, ("powercycling requested "
                                                "but RemoteConsole is not "
                                                "initialized.  "
                                                "Check ipmi config.")
            remote.console.power_off()
        else:
            self.ctx.daemons.get_daemon('osd', osd, self.cluster).stop()

    def blackhole_kill_osd(self, osd):
        """
        Stop osd if nothing else works.
        """
        self.raw_cluster_cmd('--', 'tell', 'osd.%d' % osd,
                             'injectargs', '--filestore-blackhole')
        time.sleep(2)
        self.ctx.daemons.get_daemon('osd', osd, self.cluster).stop()

    def revive_osd(self, osd, timeout=150, skip_admin_check=False):
        """
        Revive osds by either power cycling (if indicated by the config)
        or by restarting.
        """
        if self.config.get('powercycle'):
            remote = self.find_remote('osd', osd)
            self.log('kill_osd on osd.{o} doing powercycle of {s}'.
                     format(o=osd, s=remote.name))
            assert remote.console is not None, ("powercycling requested "
                                                "but RemoteConsole is not "
                                                "initialized.  "
                                                "Check ipmi config.")
            remote.console.power_on()
            if not remote.console.check_status(300):
                raise Exception('Failed to revive osd.{o} via ipmi'.
                                format(o=osd))
            teuthology.reconnect(self.ctx, 60, [remote])
            mount_osd_data(self.ctx, remote, self.cluster, str(osd))
            self.make_admin_daemon_dir(remote)
            self.ctx.daemons.get_daemon('osd', osd, self.cluster).reset()
        self.ctx.daemons.get_daemon('osd', osd, self.cluster).restart()

        if not skip_admin_check:
            # wait for dump_ops_in_flight; this command doesn't appear
            # until after the signal handler is installed and it is safe
            # to stop the osd again without making valgrind leak checks
            # unhappy.  see #5924.
            self.wait_run_admin_socket('osd', osd,
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

    def signal_osd(self, osd, sig, silent=False):
        """
        Wrapper to local get_daemon call which sends the given
        signal to the given osd.
        """
        self.ctx.daemons.get_daemon('osd', osd,
                                    self.cluster).signal(sig, silent=silent)

    ## monitors
    def signal_mon(self, mon, sig, silent=False):
        """
        Wrapper to local get_deamon call
        """
        self.ctx.daemons.get_daemon('mon', mon,
                                    self.cluster).signal(sig, silent=silent)

    def kill_mon(self, mon):
        """
        Kill the monitor by either power cycling (if the config says so),
        or by doing a stop.
        """
        if self.config.get('powercycle'):
            remote = self.find_remote('mon', mon)
            self.log('kill_mon on mon.{m} doing powercycle of {s}'.
                     format(m=mon, s=remote.name))
            assert remote.console is not None, ("powercycling requested "
                                                "but RemoteConsole is not "
                                                "initialized.  "
                                                "Check ipmi config.")

            remote.console.power_off()
        else:
            self.ctx.daemons.get_daemon('mon', mon, self.cluster).stop()

    def revive_mon(self, mon):
        """
        Restart by either power cycling (if the config says so),
        or by doing a normal restart.
        """
        if self.config.get('powercycle'):
            remote = self.find_remote('mon', mon)
            self.log('revive_mon on mon.{m} doing powercycle of {s}'.
                     format(m=mon, s=remote.name))
            assert remote.console is not None, ("powercycling requested "
                                                "but RemoteConsole is not "
                                                "initialized.  "
                                                "Check ipmi config.")

            remote.console.power_on()
            self.make_admin_daemon_dir(remote)
        self.ctx.daemons.get_daemon('mon', mon, self.cluster).restart()

    def get_mon_status(self, mon):
        """
        Extract all the monitor status information from the cluster
        """
        addr = self.ctx.ceph[self.cluster].conf['mon.%s' % mon]['mon addr']
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
                    ('failed to reach quorum size %d '
                     'before timeout expired' % size)
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

    def get_filepath(self):
        """
        Return path to osd data with {id} needing to be replaced
        """
        return '/var/lib/ceph/osd/' + self.cluster + '-{id}'

    def make_admin_daemon_dir(self, remote):
        """
        Create /var/run/ceph directory on remote site.

        :param ctx: Context
        :param remote: Remote site
        """
        remote.run(args=['sudo',
                         'install', '-d', '-m0777', '--', '/var/run/ceph', ], )


def utility_task(name):
    """
    Generate ceph_manager subtask corresponding to ceph_manager
    method name
    """
    def task(ctx, config):
        if config is None:
            config = {}
        args = config.get('args', [])
        kwargs = config.get('kwargs', {})
        fn = getattr(ctx.manager, name)
        fn(*args, **kwargs)
    return task

revive_osd = utility_task("revive_osd")
revive_mon = utility_task("revive_mon")
kill_osd = utility_task("kill_osd")
kill_mon = utility_task("kill_mon")
create_pool = utility_task("create_pool")
remove_pool = utility_task("remove_pool")
wait_for_clean = utility_task("wait_for_clean")
set_pool_property = utility_task("set_pool_property")
do_pg_scrub = utility_task("do_pg_scrub")
