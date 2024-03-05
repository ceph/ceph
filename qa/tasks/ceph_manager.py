"""
ceph manager -- Thrasher and CephManager objects
"""
from functools import wraps
import contextlib
import errno
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
import shlex

from io import BytesIO, StringIO
from subprocess import DEVNULL
from teuthology import misc as teuthology
from tasks.scrub import Scrubber
from tasks.util.rados import cmd_erasure_code_profile
from tasks.util import get_remote

from teuthology.contextutil import safe_while
from teuthology.orchestra.remote import Remote
from teuthology.orchestra import run
from teuthology.parallel import parallel
from teuthology.exceptions import CommandFailedError
from tasks.thrasher import Thrasher


DEFAULT_CONF_PATH = '/etc/ceph/ceph.conf'

log = logging.getLogger(__name__)

# this is for cephadm clusters
def shell(ctx, cluster_name, remote, args, name=None, **kwargs):
    extra_args = []
    if name:
        extra_args = ['-n', name]
    return remote.run(
        args=[
            'sudo',
            ctx.cephadm,
            '--image', ctx.ceph[cluster_name].image,
            'shell',
        ] + extra_args + [
            '--fsid', ctx.ceph[cluster_name].fsid,
            '--',
        ] + args,
        **kwargs
    )

# this is for rook clusters
def toolbox(ctx, cluster_name, args, **kwargs):
    return ctx.rook[cluster_name].remote.run(
        args=[
            'kubectl',
            '-n', 'rook-ceph',
            'exec',
            ctx.rook[cluster_name].toolbox,
            '--',
        ] + args,
        **kwargs
    )


def write_conf(ctx, conf_path=DEFAULT_CONF_PATH, cluster='ceph'):
    conf_fp = BytesIO()
    ctx.ceph[cluster].conf.write(conf_fp)
    conf_fp.seek(0)
    writes = ctx.cluster.run(
        args=[
            'sudo', 'mkdir', '-p', '/etc/ceph', run.Raw('&&'),
            'sudo', 'chmod', '0755', '/etc/ceph', run.Raw('&&'),
            'sudo', 'tee', conf_path, run.Raw('&&'),
            'sudo', 'chmod', '0644', conf_path,
            run.Raw('>'), '/dev/null',

        ],
        stdin=run.PIPE,
        wait=False)
    teuthology.feed_many_stdins_and_close(conf_fp, writes)
    run.wait(writes)

def get_valgrind_args(testdir, name, preamble, v, exit_on_first_error=True, cd=True):
    """
    Build a command line for running valgrind.

    testdir - test results directory
    name - name of daemon (for naming hte log file)
    preamble - stuff we should run before valgrind
    v - valgrind arguments
    """
    if v is None:
        return preamble
    if not isinstance(v, list):
        v = [v]

    # https://tracker.ceph.com/issues/44362
    preamble.extend([
        'env', 'OPENSSL_ia32cap=~0x1000000000000000',
    ])

    val_path = '/var/log/ceph/valgrind'
    if '--tool=memcheck' in v or '--tool=helgrind' in v:
        extra_args = [
            'valgrind',
            '--trace-children=no',
            '--child-silent-after-fork=yes',
            '--soname-synonyms=somalloc=*tcmalloc*',
            '--num-callers=50',
            '--suppressions={tdir}/valgrind.supp'.format(tdir=testdir),
            '--xml=yes',
            '--xml-file={vdir}/{n}.log'.format(vdir=val_path, n=name),
            '--time-stamp=yes',
            '--vgdb=yes',
        ]
    else:
        extra_args = [
            'valgrind',
            '--trace-children=no',
            '--child-silent-after-fork=yes',
            '--soname-synonyms=somalloc=*tcmalloc*',
            '--suppressions={tdir}/valgrind.supp'.format(tdir=testdir),
            '--log-file={vdir}/{n}.log'.format(vdir=val_path, n=name),
            '--time-stamp=yes',
            '--vgdb=yes',
        ]
    if exit_on_first_error:
        extra_args.extend([
            # at least Valgrind 3.14 is required
            '--exit-on-first-error=yes',
            '--error-exitcode=42',
        ])
    args = []
    if cd:
        args += ['cd', testdir, run.Raw('&&')]
    args += preamble + extra_args + v
    log.debug('running %s under valgrind with args %s', name, args)
    return args


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


def log_exc(func):
    @wraps(func)
    def wrapper(self):
        try:
            return func(self)
        except:
            self.log(traceback.format_exc())
            raise
    return wrapper


class PoolType:
    REPLICATED = 1
    ERASURE_CODED = 3


class OSDThrasher(Thrasher):
    """
    Object used to thrash Ceph
    """
    def __init__(self, manager, config, name, logger):
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
        self.name = name
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
        self.chance_reset_purged_snaps_last = self.config.get('chance_reset_purged_snaps_last', 0.3)
        self.chance_trim_stale_osdmaps = self.config.get('chance_trim_stale_osdmaps', 0.3)
        self.chance_force_remove_snap = self.config.get('chance_force_remove_snap', 0)

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
        if self.ceph_manager.cephadm or self.ceph_manager.rook:
            return True
        allremotes = self.ceph_manager.ctx.cluster.only(\
            teuthology.is_type('osd', self.cluster)).remotes.keys()
        allremotes = list(set(allremotes))
        for remote in allremotes:
            proc = remote.run(args=['type', cmd], wait=True,
                              check_status=False, stdout=BytesIO(),
                              stderr=BytesIO())
            if proc.exitstatus != 0:
                return False;
        return True;

    def run_ceph_objectstore_tool(self, remote, osd, cmd):
        if self.ceph_manager.cephadm:
            return shell(
                self.ceph_manager.ctx, self.ceph_manager.cluster, remote,
                args=['ceph-objectstore-tool', '--err-to-stderr'] + cmd,
                name=osd,
                wait=True, check_status=False,
                stdout=StringIO(),
                stderr=StringIO())
        elif self.ceph_manager.rook:
            assert False, 'not implemented'
        else:
            return remote.run(
                args=['sudo', 'adjust-ulimits', 'ceph-objectstore-tool', '--err-to-stderr'] + cmd,
                wait=True, check_status=False,
                stdout=StringIO(),
                stderr=StringIO())

    def run_ceph_bluestore_tool(self, remote, osd, cmd):
        if self.ceph_manager.cephadm:
            return shell(
                self.ceph_manager.ctx, self.ceph_manager.cluster, remote,
                args=['ceph-bluestore-tool', '--err-to-stderr'] + cmd,
                name=osd,
                wait=True, check_status=False,
                stdout=StringIO(),
                stderr=StringIO())
        elif self.ceph_manager.rook:
            assert False, 'not implemented'
        else:
            return remote.run(
                args=['sudo', 'ceph-bluestore-tool', '--err-to-stderr'] + cmd,
                wait=True, check_status=False,
                stdout=StringIO(),
                stderr=StringIO())

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
            self.log("Testing ceph-objectstore-tool on down osd.%s" % osd)
            remote = self.ceph_manager.find_remote('osd', osd)
            FSPATH = self.ceph_manager.get_filepath()
            JPATH = os.path.join(FSPATH, "journal")
            exp_osd = imp_osd = osd
            self.log('remote for osd %s is %s' % (osd, remote))
            exp_remote = imp_remote = remote
            # If an older osd is available we'll move a pg from there
            if (len(self.dead_osds) > 1 and
                    random.random() < self.chance_move_pg):
                exp_osd = random.choice(self.dead_osds[:-1])
                exp_remote = self.ceph_manager.find_remote('osd', exp_osd)
                self.log('remote for exp osd %s is %s' % (exp_osd, exp_remote))
            prefix = [
                '--no-mon-config',
                '--log-file=/var/log/ceph/objectstore_tool.$pid.log',
            ]

            if self.ceph_manager.rook:
                assert False, 'not implemented'

            if not self.ceph_manager.cephadm:
                # ceph-objectstore-tool might be temporarily absent during an
                # upgrade - see http://tracker.ceph.com/issues/18014
                with safe_while(sleep=15, tries=40, action="type ceph-objectstore-tool") as proceed:
                    while proceed():
                        proc = exp_remote.run(args=['type', 'ceph-objectstore-tool'],
                                              wait=True, check_status=False, stdout=BytesIO(),
                                              stderr=BytesIO())
                        if proc.exitstatus == 0:
                            break
                        log.debug("ceph-objectstore-tool binary not present, trying again")

            # ceph-objectstore-tool might bogusly fail with "OSD has the store locked"
            # see http://tracker.ceph.com/issues/19556
            with safe_while(sleep=15, tries=40, action="ceph-objectstore-tool --op list-pgs") as proceed:
                while proceed():
                    proc = self.run_ceph_objectstore_tool(
                        exp_remote, 'osd.%s' % exp_osd,
                        prefix + [
                            '--data-path', FSPATH.format(id=exp_osd),
                            '--journal-path', JPATH.format(id=exp_osd),
                            '--op', 'list-pgs',
                        ])
                    if proc.exitstatus == 0:
                        break
                    elif (proc.exitstatus == 1 and
                          proc.stderr.getvalue() == "OSD has the store locked"):
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
            #exp_path = teuthology.get_testdir(self.ceph_manager.ctx)
            #exp_path = os.path.join(exp_path, '{0}.data'.format(self.cluster))
            exp_path = os.path.join('/var/log/ceph', # available inside 'shell' container
                                    "exp.{pg}.{id}".format(
                                        pg=pg,
                                        id=exp_osd))
            if self.ceph_manager.cephadm:
                exp_host_path = os.path.join(
                    '/var/log/ceph',
                    self.ceph_manager.ctx.ceph[self.ceph_manager.cluster].fsid,
                    "exp.{pg}.{id}".format(
                        pg=pg,
                        id=exp_osd))
            else:
                exp_host_path = exp_path

            # export
            # Can't use new export-remove op since this is part of upgrade testing
            proc = self.run_ceph_objectstore_tool(
                exp_remote, 'osd.%s' % exp_osd,
                prefix + [
                    '--data-path', FSPATH.format(id=exp_osd),
                    '--journal-path', JPATH.format(id=exp_osd),
                    '--op', 'export',
                    '--pgid', pg,
                    '--file', exp_path,
                ])
            if proc.exitstatus:
                raise Exception("ceph-objectstore-tool: "
                                "export failure with status {ret}".
                                format(ret=proc.exitstatus))
            # remove
            proc = self.run_ceph_objectstore_tool(
                exp_remote, 'osd.%s' % exp_osd,
                prefix + [
                    '--data-path', FSPATH.format(id=exp_osd),
                    '--journal-path', JPATH.format(id=exp_osd),
                    '--force',
                    '--op', 'remove',
                    '--pgid', pg,
                ])
            if proc.exitstatus:
                raise Exception("ceph-objectstore-tool: "
                                "remove failure with status {ret}".
                                format(ret=proc.exitstatus))
            # If there are at least 2 dead osds we might move the pg
            if exp_osd != imp_osd:
                # If pg isn't already on this osd, then we will move it there
                proc = self.run_ceph_objectstore_tool(
                    imp_remote,
                    'osd.%s' % imp_osd,
                    prefix + [
                        '--data-path', FSPATH.format(id=imp_osd),
                        '--journal-path', JPATH.format(id=imp_osd),
                        '--op', 'list-pgs',
                    ])
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
                        # just in case an upgrade make /var/log/ceph unreadable by non-root,
                        exp_remote.run(args=['sudo', 'chmod', '777',
                                             '/var/log/ceph'])
                        imp_remote.run(args=['sudo', 'chmod', '777',
                                             '/var/log/ceph'])
                        tmpexport = Remote.get_file(exp_remote, exp_host_path,
                                                    sudo=True)
                        if exp_host_path != exp_path:
                            # push to /var/log/ceph, then rename (we can't
                            # chmod 777 the /var/log/ceph/$fsid mountpoint)
                            Remote.put_file(imp_remote, tmpexport, exp_path)
                            imp_remote.run(args=[
                                'sudo', 'mv', exp_path, exp_host_path])
                        else:
                            Remote.put_file(imp_remote, tmpexport, exp_host_path)
                        os.remove(tmpexport)
                else:
                    # Can't move the pg after all
                    imp_osd = exp_osd
                    imp_remote = exp_remote
            # import
            proc = self.run_ceph_objectstore_tool(
                imp_remote, 'osd.%s' % imp_osd,
                [
                    '--data-path', FSPATH.format(id=imp_osd),
                    '--journal-path', JPATH.format(id=imp_osd),
                    '--log-file=/var/log/ceph/objectstore_tool.$pid.log',
                    '--op', 'import',
                    '--file', exp_path,
                ])
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
            cmd = "sudo rm -f {file}".format(file=exp_host_path)
            exp_remote.run(args=cmd)
            if imp_remote != exp_remote:
                imp_remote.run(args=cmd)

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
        self.log("primary_affinity")
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
        self.log("thrash_pg_upmap")
        from random import shuffle
        out = self.ceph_manager.raw_cluster_cmd('osd', 'dump', '-f', 'json-pretty')
        j = json.loads(out)
        self.log('j is %s' % j)
        try:
            if random.random() >= .3:
                pgs = self.ceph_manager.get_pg_stats()
                if not pgs:
                    self.log('No pgs; doing nothing')
                    return
                pg = random.choice(pgs)
                pgid = str(pg['pgid'])
                poolid = int(pgid.split('.')[0])
                sizes = [x['size'] for x in j['pools'] if x['pool'] == poolid]
                if len(sizes) == 0:
                    self.log('No pools; doing nothing')
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
        self.log("thrash_pg_upmap_items")
        from random import shuffle
        out = self.ceph_manager.raw_cluster_cmd('osd', 'dump', '-f', 'json-pretty')
        j = json.loads(out)
        self.log('j is %s' % j)
        try:
            if random.random() >= .3:
                pgs = self.ceph_manager.get_pg_stats()
                if not pgs:
                    self.log('No pgs; doing nothing')
                    return
                pg = random.choice(pgs)
                pgid = str(pg['pgid'])
                poolid = int(pgid.split('.')[0])
                sizes = [x['size'] for x in j['pools'] if x['pool'] == poolid]
                if len(sizes) == 0:
                    self.log('No pools; doing nothing')
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

    def reset_purged_snaps_last(self):
        """
        Run reset_purged_snaps_last
        """
        self.log('reset_purged_snaps_last')
        for osd in self.in_osds:
            try:
               self.ceph_manager.raw_cluster_cmd(
               'tell', "osd.%s" % (str(osd)),
               'reset_purged_snaps_last')
            except CommandFailedError:
                self.log('Failed to reset_purged_snaps_last, ignoring')

    def trim_stale_osdmaps(self):
       """
       Trim stale osdmaps
       """
       self.log('trim_stale_osdmaps')
       for osd in self.in_osds:
           try:
               self.ceph_manager.raw_cluster_cmd(
               'tell', "osd.%s" % (str(osd)),
               'trim stale osdmaps')
           except CommandFailedError:
               self.log('Failed to trim stale osdmaps, ignoring')

    def force_remove_snap(self):
        """
        Force reremove snapshots already makred as purged.
        Please note that internal snap ids are the ones being
        used to detect purges snaps. The internal snap ids are
        *not* the same snap ids used by thrasher.
        """
        self.log('force_remove_snap')
        pool = self.ceph_manager.get_pool()
        if pool is None:
            self.log('Failed to get pool')
            return
        try:
            self.ceph_manager.raw_cluster_cmd('osd', 'pool',
                                              'force-remove-snap',
                                              pool, '--purged-snaps-only')
        except CommandFailedError:
            self.log('Failed to force reremove snap, ignoring')

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
        if pool is None:
            return
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
        if pool is None:
            return
        _ = self.ceph_manager.get_pool_pg_num(pool)
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
            if not pool:
                return
            force = False
        else:
            force = True
        self.log("fixing pg num pool %s" % (pool,))
        if self.ceph_manager.set_pool_pgpnum(pool, force):
            self.pools_to_fix_pgp_num.discard(pool)

    def get_rand_pg_acting_set(self, pool_id=None):
        """
        Return an acting set of a random PG, you
        have the option to specify which pool you
        want the PG from.
        """
        pgs = self.ceph_manager.get_pg_stats()
        if not pgs:
            self.log('No pgs; doing nothing')
            return
        if pool_id:
           pgs_in_pool = [pg for pg in pgs if int(pg['pgid'].split('.')[0]) == pool_id]
           pg = random.choice(pgs_in_pool)
        else:
            pg = random.choice(pgs)
        self.log('Choosing PG {id} with acting set {act}'.format(id=pg['pgid'],act=pg['acting']))
        return pg['acting']

    def get_k_m_ec_pool(self, pool, pool_json):
        """
        Returns k and m
        """
        k = 0
        m = 99
        try:
            ec_profile = self.ceph_manager.get_pool_property(pool, 'erasure_code_profile')
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
            return None, None

        return k, m

    def test_pool_min_size(self):
        """
        Loop to selectively push PGs to their min_size and test that recovery
        still occurs. We achieve this by randomly picking a PG and fail the OSDs
        according to the PG's acting set.
        """
        self.log("test_pool_min_size")
        self.all_up()
        time.sleep(60) # buffer time for recovery to start.
        self.ceph_manager.wait_for_recovery(
            timeout=self.config.get('timeout')
            )
        self.log("doing min_size thrashing")
        self.ceph_manager.wait_for_clean(timeout=180)
        assert self.ceph_manager.is_clean(), \
            'not clean before minsize thrashing starts'
        start = time.time()
        while time.time() - start < self.config.get("test_min_size_duration", 1800):
            # look up k and m from all the pools on each loop, in case it
            # changes as the cluster runs
            pools_json = self.ceph_manager.get_osd_dump_json()['pools']
            if len(pools_json) == 0:
                self.log("No pools yet, waiting")
                time.sleep(5)
                continue
            for pool_json in pools_json:
                pool = pool_json['pool_name']
                pool_id = pool_json['pool']
                pool_type = pool_json['type']  # 1 for rep, 3 for ec
                min_size = pool_json['min_size']
                self.log("pool {pool} min_size is {min_size}".format(pool=pool,min_size=min_size))
                if pool_type != PoolType.ERASURE_CODED:
                    continue
                else:
                    k, m = self.get_k_m_ec_pool(pool, pool_json)
                    if k == None and m == None:
                        continue
                    self.log("using k={k}, m={m}".format(k=k,m=m))

                self.log("dead_osds={d}, live_osds={ld}".format(d=self.dead_osds, ld=self.live_osds))
                minup = max(min_size, k)
                # Choose a random PG and kill OSDs until only min_size remain
                most_killable = min(len(self.live_osds) - minup, m)
                self.log("chose to kill {n} OSDs".format(n=most_killable))
                acting_set = self.get_rand_pg_acting_set(pool_id)
                assert most_killable < len(acting_set)
                for i in range(0, most_killable):
                    self.kill_osd(osd=acting_set[i], mark_out=True)
                self.log("dead_osds={d}, live_osds={ld}".format(d=self.dead_osds, ld=self.live_osds))
                with safe_while(
                    sleep=25, tries=5,
                    action='check for active or peered') as proceed:
                    while proceed():
                        if self.ceph_manager.all_active_or_peered():
                            break
                        self.log('not all PGs are active or peered')
                self.all_up_in() # revive all OSDs
                # let PGs repair themselves or our next knockout might kill one
                # wait_for_recovery since some workloads won't be able to go clean
                self.ceph_manager.wait_for_recovery(
                    timeout=self.config.get('timeout')
                )
        # while not self.stopping
        self.all_up_in() # revive all OSDs

        # Wait until all PGs are active+clean after we have revived all the OSDs
        self.ceph_manager.wait_for_clean(timeout=self.config.get('timeout'))

    def inject_pause(self, conf_key, duration, check_after, should_be_down):
        """
        Pause injection testing. Check for osd being down when finished.
        """
        the_one = random.choice(self.live_osds)
        self.log("inject_pause on osd.{osd}".format(osd=the_one))
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


    def generate_random_sharding(self):
        prefixes = [
            'm','O','P','L'
        ]
        new_sharding = ''
        for prefix in prefixes:
            choose = random.choice([False, True])
            if not choose:
                continue
            if new_sharding != '':
                new_sharding = new_sharding + ' '
            columns = random.randint(1, 5)
            do_hash = random.choice([False, True])
            if do_hash:
                low_hash = random.choice([0, 5, 8])
                do_high_hash = random.choice([False, True])
                if do_high_hash:
                    high_hash = random.choice([8, 16, 30]) + low_hash
                    new_sharding = new_sharding + prefix + '(' + str(columns) + ',' + str(low_hash) + '-' + str(high_hash) + ')'
                else:
                    new_sharding = new_sharding + prefix + '(' + str(columns) + ',' + str(low_hash) + '-)'
            else:
                if columns == 1:
                    new_sharding = new_sharding + prefix
                else:
                    new_sharding = new_sharding + prefix + '(' + str(columns) + ')'
        return new_sharding

    def test_bluestore_reshard_action(self):
        """
        Test if resharding of bluestore works properly.
        If bluestore is not used, or bluestore is in version that
        does not support sharding, skip.
        """

        osd = random.choice(self.dead_osds)
        remote = self.ceph_manager.find_remote('osd', osd)
        FSPATH = self.ceph_manager.get_filepath()

        prefix = [
                '--no-mon-config',
                '--log-file=/var/log/ceph/bluestore_tool.$pid.log',
                '--log-level=10',
                '--path', FSPATH.format(id=osd)
            ]

        # sanity check if bluestore-tool accessible
        self.log('checking if target objectstore is bluestore on osd.%s' % osd)
        cmd = prefix + [
            'show-label'
            ]
        proc = self.run_ceph_bluestore_tool(remote, 'osd.%s' % osd, cmd)
        if proc.exitstatus != 0:
            raise Exception("ceph-bluestore-tool access failed.")

        # check if sharding is possible
        self.log('checking if target bluestore supports sharding on osd.%s' % osd)
        cmd = prefix + [
            'show-sharding'
            ]
        proc = self.run_ceph_bluestore_tool(remote, 'osd.%s' % osd, cmd)
        if proc.exitstatus != 0:
            self.log("Unable to test resharding, "
                     "ceph-bluestore-tool does not support it.")
            return

        # now go for reshard to something else
        self.log('applying new sharding to bluestore on osd.%s' % osd)
        new_sharding = self.config.get('bluestore_new_sharding','random')

        if new_sharding == 'random':
            self.log('generate random sharding')
            new_sharding = self.generate_random_sharding()

        self.log("applying new sharding: " + new_sharding)
        cmd = prefix + [
            '--sharding', new_sharding,
            'reshard'
            ]
        proc = self.run_ceph_bluestore_tool(remote, 'osd.%s' % osd, cmd)
        if proc.exitstatus != 0:
            raise Exception("ceph-bluestore-tool resharding failed.")

        # now do fsck to
        self.log('running fsck to verify new sharding on osd.%s' % osd)
        cmd = prefix + [
            'fsck'
            ]
        proc = self.run_ceph_bluestore_tool(remote, 'osd.%s' % osd, cmd)
        if proc.exitstatus != 0:
            raise Exception("ceph-bluestore-tool fsck failed.")
        self.log('resharding successfully completed')

    def test_bluestore_reshard(self):
        """
        1) kills an osd
        2) reshards bluestore on killed osd
        3) revives the osd
        """
        self.log('test_bluestore_reshard started')
        self.kill_osd(mark_down=True, mark_out=True)
        self.test_bluestore_reshard_action()
        self.revive_osd()
        self.log('test_bluestore_reshard completed')


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
        self.log("test_map_discontinuity")
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
        _ = self.config.get('chance_test_min_size', 0)
        chance_test_backfill_full = \
            self.config.get('chance_test_backfill_full', 0)
        if isinstance(chance_down, int):
            chance_down = float(chance_down) / 100
        minin = self.minin
        minout = int(self.config.get("min_out", 0))
        minlive = int(self.config.get("min_live", 2))
        mindead = int(self.config.get("min_dead", 0))

        self.log('choose_action: min_in %d min_out '
                 '%d min_live %d min_dead %d '
                 'chance_down %.2f' %
                 (minin, minout, minlive, mindead, chance_down))
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
        if self.chance_reset_purged_snaps_last > 0:
            actions.append((self.reset_purged_snaps_last, self.chance_reset_purged_snaps_last))
        if self.chance_trim_stale_osdmaps > 0:
            actions.append((self.trim_stale_osdmaps, self.chance_trim_stale_osdmaps))
        if self.chance_force_remove_snap > 0:
            actions.append((self.force_remove_snap, self.chance_force_remove_snap))

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

        # only consider resharding if objectstore is bluestore
        cluster_name = self.ceph_manager.cluster
        cluster = self.ceph_manager.ctx.ceph[cluster_name]
        if cluster.conf.get('osd', {}).get('osd objectstore', 'bluestore') == 'bluestore':
            actions.append((self.test_bluestore_reshard,
                            self.config.get('chance_bluestore_reshard', 0),))

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
            self.set_thrasher_exception(e)
            self.logger.exception("exception:")
            # Allow successful completion so gevent doesn't see an exception.
            # The DaemonWatchdog will observe the error and tear down the test.

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


class ObjectStoreTool:

    def __init__(self, manager, pool, **kwargs):
        self.manager = manager
        self.pool = pool
        self.osd = kwargs.get('osd', None)
        self.object_name = kwargs.get('object_name', None)
        self.do_revive = kwargs.get('do_revive', True)
        if self.osd and self.pool and self.object_name:
            if self.osd == "primary":
                self.osd = self.manager.get_object_primary(self.pool,
                                                           self.object_name)
        assert self.osd is not None
        if self.object_name:
            self.pgid = self.manager.get_object_pg_with_shard(self.pool,
                                                              self.object_name,
                                                              self.osd)
        self.remote = next(iter(self.manager.ctx.\
            cluster.only('osd.{o}'.format(o=self.osd)).remotes.keys()))
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

    def run(self, options, args):
        self.manager.kill_osd(self.osd)
        cmd = self.build_cmd(options, args, None)
        self.manager.log(cmd)
        try:
            proc = self.remote.run(args=['bash', '-e', '-x', '-c', cmd],
                                   check_status=False,
                                   stdout=BytesIO(),
                                   stderr=BytesIO())
            proc.wait()
            if proc.exitstatus != 0:
                self.manager.log("failed with " + str(proc.exitstatus))
                error = proc.stdout.getvalue().decode()  + " " + \
                        proc.stderr.getvalue().decode()
                raise Exception(error)
        finally:
            if self.do_revive:
                self.manager.revive_osd(self.osd)
                self.manager.wait_till_osd_is_up(self.osd, 300)


# XXX: this class has nothing to do with the Ceph daemon (ceph-mgr) of
# the same name.
class CephManager:
    """
    Ceph manager object.
    Contains several local functions that form a bulk of this module.

    :param controller: the remote machine where the Ceph commands should be
                       executed
    :param ctx: the cluster context
    :param config: path to Ceph config file
    :param logger: for logging messages
    :param cluster: name of the Ceph cluster
    """

    def __init__(self, controller, ctx=None, config=None, logger=None,
                 cluster='ceph', cephadm=False, rook=False) -> None:
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
                print(x)
            self.log = tmp

        if self.config is None:
            self.config = dict()

        # NOTE: These variables are meant to be overriden by vstart_runner.py.
        self.rook = rook
        self.cephadm = cephadm
        self.testdir = teuthology.get_testdir(self.ctx)
        # prefix args for ceph cmds to be executed
        self.pre = ['adjust-ulimits', 'ceph-coverage',
                    f'{self.testdir}/archive/coverage']
        self.RADOS_CMD = self.pre + ['rados', '--cluster', self.cluster]

        pools = self.list_pools()
        self.pools = {}
        for pool in pools:
            # we may race with a pool deletion; ignore failures here
            try:
                self.pools[pool] = self.get_pool_int_property(pool, 'pg_num')
            except CommandFailedError:
                self.log('Failed to get pg_num from pool %s, ignoring' % pool)

    def get_ceph_cmd(self, **kwargs):
        timeout = kwargs.pop('timeout', 120)
        return ['sudo'] + self.pre + ['timeout', f'{timeout}', 'ceph',
                                      '--cluster', self.cluster]

    def ceph(self, cmd, **kwargs):
        """
        Simple Ceph admin command wrapper around run_cluster_cmd.
        """

        kwargs.pop('args', None)
        args = shlex.split(cmd)
        stdout = kwargs.pop('stdout', StringIO())
        stderr = kwargs.pop('stderr', StringIO())
        return self.run_cluster_cmd(args=args, stdout=stdout, stderr=stderr, **kwargs)

    def run_cluster_cmd(self, **kwargs):
        """
        Run a Ceph command and return the object representing the process
        for the command.

        Accepts arguments same as that of teuthology.orchestra.run.run()
        """
        if isinstance(kwargs['args'], str):
            kwargs['args'] = shlex.split(kwargs['args'])
        elif isinstance(kwargs['args'], tuple):
            kwargs['args'] = list(kwargs['args'])

        prefixcmd = []
        timeoutcmd = kwargs.pop('timeoutcmd', None)
        if timeoutcmd is not None:
            prefixcmd += ['timeout', str(timeoutcmd)]

        if self.cephadm:
            prefixcmd += ['ceph']
            cmd = prefixcmd + list(kwargs['args'])
            return shell(self.ctx, self.cluster, self.controller,
                         args=cmd,
                         stdout=StringIO(),
                         check_status=kwargs.get('check_status', True))
        elif self.rook:
            prefixcmd += ['ceph']
            cmd = prefixcmd + list(kwargs['args'])
            return toolbox(self.ctx, self.cluster,
                           args=cmd,
                           stdout=StringIO(),
                           check_status=kwargs.get('check_status', True))
        else:
            kwargs['args'] = prefixcmd + self.get_ceph_cmd(**kwargs) + kwargs['args']
            return self.controller.run(**kwargs)

    def raw_cluster_cmd(self, *args, **kwargs) -> str:
        """
        Start ceph on a raw cluster.  Return count
        """
        if kwargs.get('args') is None and args:
            kwargs['args'] = args
        kwargs['stdout'] = kwargs.pop('stdout', StringIO())
        return self.run_cluster_cmd(**kwargs).stdout.getvalue()

    def raw_cluster_cmd_result(self, *args, **kwargs):
        """
        Start ceph on a cluster.  Return success or failure information.
        """
        if kwargs.get('args') is None and args:
           kwargs['args'] = args
        kwargs['check_status'] = False
        return self.run_cluster_cmd(**kwargs).exitstatus

    def get_keyring(self, client_id):
        """
        Return keyring for the given client.

        :param client_id: str
        :return keyring: str
        """
        if client_id.find('client.') != -1:
            client_id = client_id.replace('client.', '')

        keyring = self.run_cluster_cmd(args=f'auth get client.{client_id}',
                                       stdout=StringIO()).stdout.getvalue()

        assert isinstance(keyring, str) and keyring != ''
        return keyring

    def run_ceph_w(self, watch_channel=None):
        """
        Execute "ceph -w" in the background with stdout connected to a BytesIO,
        and return the RemoteProcess.

        :param watch_channel: Specifies the channel to be watched. This can be
                              'cluster', 'audit', ...
        :type watch_channel: str
        """
        args = ['sudo', 'daemon-helper', 'kill', 'ceph', '--cluster', self.cluster, '-w']
        if watch_channel is not None:
            args.append("--watch-channel")
            args.append(watch_channel)
        return self.controller.run(args=args, wait=False, stdout=StringIO(), stdin=run.PIPE)

    def get_mon_socks(self):
        """
        Get monitor sockets.

        :return socks: tuple of strings; strings are individual sockets.
        """
        from json import loads

        output = loads(self.raw_cluster_cmd(['--format=json', 'mon', 'dump']))
        socks = []
        for mon in output['mons']:
            for addrvec_mem in mon['public_addrs']['addrvec']:
                socks.append(addrvec_mem['addr'])
        return tuple(socks)

    def get_msgrv1_mon_socks(self):
        """
        Get monitor sockets that use msgrv1 to operate.

        :return socks: tuple of strings; strings are individual sockets.
        """
        from json import loads

        output = loads(self.raw_cluster_cmd('--format=json', 'mon', 'dump'))
        socks = []
        for mon in output['mons']:
            for addrvec_mem in mon['public_addrs']['addrvec']:
                if addrvec_mem['type'] == 'v1':
                    socks.append(addrvec_mem['addr'])
        return tuple(socks)

    def get_msgrv2_mon_socks(self):
        """
        Get monitor sockets that use msgrv2 to operate.

        :return socks: tuple of strings; strings are individual sockets.
        """
        from json import loads

        output = loads(self.raw_cluster_cmd('--format=json', 'mon', 'dump'))
        socks = []
        for mon in output['mons']:
            for addrvec_mem in mon['public_addrs']['addrvec']:
                if addrvec_mem['type'] == 'v2':
                    socks.append(addrvec_mem['addr'])
        return tuple(socks)

    def flush_pg_stats(self, osds, no_wait=None, wait_for_mon=300):
        """
        Flush pg stats from a list of OSD ids, ensuring they are reflected
        all the way to the monitor.  Luminous and later only.

        :param osds: list of OSDs to flush
        :param no_wait: list of OSDs not to wait for seq id. by default, we
                        wait for all specified osds, but some of them could be
                        moved out of osdmap, so we cannot get their updated
                        stat seq from monitor anymore. in that case, you need
                        to pass a blocklist.
        :param wait_for_mon: wait for mon to be synced with mgr. 0 to disable
                             it. (5 min by default)
        """
        if no_wait is None:
            no_wait = []

        def flush_one_osd(osd: int, wait_for_mon: int):
            need = int(self.raw_cluster_cmd('tell', 'osd.%d' % osd, 'flush_pg_stats'))
            if not wait_for_mon:
                return
            if osd in no_wait:
                return
            got = 0
            while wait_for_mon > 0:
                got = int(self.raw_cluster_cmd('osd', 'last-stat-seq', 'osd.%d' % osd))
                self.log('need seq {need} got {got} for osd.{osd}'.format(
                    need=need, got=got, osd=osd))
                if got >= need:
                    break
                A_WHILE = 1
                time.sleep(A_WHILE)
                wait_for_mon -= A_WHILE
            else:
                raise Exception('timed out waiting for mon to be updated with '
                                'osd.{osd}: {got} < {need}'.
                                format(osd=osd, got=got, need=need))

        with parallel() as p:
            for osd in osds:
                p.spawn(flush_one_osd, osd, wait_for_mon)

    def flush_all_pg_stats(self):
        self.flush_pg_stats(range(len(self.get_osd_dump())))

    def do_rados(self, cmd, pool=None, namespace=None, remote=None, **kwargs):
        """
        Execute a remote rados command.
        """
        if remote is None:
            remote = self.controller

        pre = self.RADOS_CMD + [] # deep-copying!
        if pool is not None:
            pre += ['--pool', pool]
        if namespace is not None:
            pre += ['--namespace', namespace]
        pre.extend(cmd)
        proc = remote.run(
            args=pre,
            wait=True,
            **kwargs
            )
        return proc

    def rados_write_objects(self, pool, num_objects, size,
                            timelimit, threads, cleanup=False):
        """
        Write rados objects
        Threads not used yet.
        """
        args = [
            '--num-objects', num_objects,
            '-b', size,
            'bench', timelimit,
            'write'
            ]
        if not cleanup:
            args.append('--no-cleanup')
        return self.do_rados(map(str, args), pool=pool)

    def do_put(self, pool, obj, fname, namespace=None):
        """
        Implement rados put operation
        """
        args = ['put', obj, fname]
        return self.do_rados(
            args,
            check_status=False,
            pool=pool,
            namespace=namespace
        ).exitstatus

    def do_get(self, pool, obj, fname='/dev/null', namespace=None):
        """
        Implement rados get operation
        """
        args = ['get', obj, fname]
        return self.do_rados(
            args,
            check_status=False,
            pool=pool,
            namespace=namespace,
        ).exitstatus

    def do_rm(self, pool, obj, namespace=None):
        """
        Implement rados rm operation
        """
        args = ['rm', obj]
        return self.do_rados(
            args,
            check_status=False,
            pool=pool,
            namespace=namespace
        ).exitstatus

    def osd_admin_socket(self, osd_id, command, check_status=True, timeout=0, stdout=None):
        if stdout is None:
            stdout = StringIO()
        return self.admin_socket('osd', osd_id, command, check_status, timeout, stdout)

    def find_remote(self, service_type, service_id):
        """
        Get the Remote for the host where a particular service runs.

        :param service_type: 'mds', 'osd', 'client'
        :param service_id: The second part of a role, e.g. '0' for
                           the role 'client.0'
        :return: a Remote instance for the host where the
                 requested role is placed
        """
        return get_remote(self.ctx, self.cluster,
                          service_type, service_id)

    def admin_socket(self, service_type, service_id,
                     command, check_status=True, timeout=0, stdout=None):
        """
        Remotely start up ceph specifying the admin socket
        :param command: a list of words to use as the command
                        to the admin socket
        """
        if stdout is None:
            stdout = StringIO()

        remote = self.find_remote(service_type, service_id)

        if self.cephadm:
            return shell(
                self.ctx, self.cluster, remote,
                args=[
                    'ceph', 'daemon', '%s.%s' % (service_type, service_id),
                ] + command,
                stdout=stdout,
                wait=True,
                check_status=check_status,
            )
        if self.rook:
            assert False, 'not implemented'

        args = [
            'sudo',
            'adjust-ulimits',
            'ceph-coverage',
           f'{self.testdir}/archive/coverage',
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
            stdout=stdout,
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
        pg_str = self.get_pgid(pool, pgnum)
        output = self.raw_cluster_cmd("pg", "map", pg_str, '--format=json')
        j = json.loads('\n'.join(output.split('\n')[1:]))
        return int(j['acting'][-1])
        assert False

    def wait_for_pg_stats(func):
        # both osd_mon_report_interval and mgr_stats_period are 5 seconds
        # by default, and take the faulty injection in ms into consideration,
        # 12 seconds are more than enough
        delays = [1, 1, 2, 3, 5, 8, 13, 0]
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            exc = None
            for delay in delays:
                try:
                    return func(self, *args, **kwargs)
                except AssertionError as e:
                    time.sleep(delay)
                    exc = e
            raise exc
        return wrapper

    def get_pg_primary(self, pool, pgnum):
        """
        get primary for pool, pgnum (e.g. (data, 0)->0
        """
        pg_str = self.get_pgid(pool, pgnum)
        output = self.raw_cluster_cmd("pg", "map", pg_str, '--format=json')
        j = json.loads('\n'.join(output.split('\n')[1:]))
        return int(j['acting'][0])
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
                              service_id, args=['version'], timeout=75, stdout=None):
        """
        If osd_admin_socket call succeeds, return.  Otherwise wait
        five seconds and try again.
        """
        if stdout is None:
            stdout = StringIO()
        tries = 0
        while True:
            proc = self.admin_socket(service_type, service_id,
                                     args, check_status=False, stdout=stdout)
            if proc.exitstatus == 0:
                return proc
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

    def get_config(self, service_type, service_id, name):
        """
        :param node: like 'mon.a'
        :param name: the option name
        """
        proc = self.wait_run_admin_socket(service_type, service_id,
                                          ['config', 'show'])
        j = json.loads(proc.stdout.getvalue())
        return j[name]

    def inject_args(self, service_type, service_id, name, value):
        whom = '{0}.{1}'.format(service_type, service_id)
        if isinstance(value, bool):
            value = 'true' if value else 'false'
        opt_arg = '--{name}={value}'.format(name=name, value=value)
        self.raw_cluster_cmd('--', 'tell', whom, 'injectargs', opt_arg)

    def set_config(self, osdnum, **argdict):
        """
        :param osdnum: osd number
        :param argdict: dictionary containing values to set.
        """
        for k, v in argdict.items():
            self.wait_run_admin_socket(
                'osd', osdnum,
                ['config', 'set', str(k), str(v)])

    def raw_cluster_status(self):
        """
        Get status from cluster
        """
        status = self.raw_cluster_cmd('status', '--format=json')
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
        osd_lines = list(filter(
            lambda x: x.startswith('osd.') and (("up" in x) or ("down" in x)),
            self.raw_osd_status().split('\n')))
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
                                     erasure_code_profile_name=None,
                                     min_size=None,
                                     erasure_code_use_overwrites=False):
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
                erasure_code_profile_name=erasure_code_profile_name,
                min_size=min_size,
                erasure_code_use_overwrites=erasure_code_use_overwrites)
        return name

    @contextlib.contextmanager
    def pool(self, pool_name, pg_num=16, erasure_code_profile_name=None):
        self.create_pool(pool_name, pg_num, erasure_code_profile_name)
        yield
        self.remove_pool(pool_name)

    def create_pool(self, pool_name, pg_num=16,
                    erasure_code_profile_name=None,
                    min_size=None,
                    erasure_code_use_overwrites=False):
        """
        Create a pool named from the pool_name parameter.
        :param pool_name: name of the pool being created.
        :param pg_num: initial number of pgs.
        :param erasure_code_profile_name: if set and !None create an
                                          erasure coded pool using the profile
        :param erasure_code_use_overwrites: if true, allow overwrites
        """
        with self.lock:
            assert isinstance(pool_name, str)
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
            if min_size is not None:
                self.raw_cluster_cmd(
                    'osd', 'pool', 'set', pool_name,
                    'min_size',
                    str(min_size))
            if erasure_code_use_overwrites:
                self.raw_cluster_cmd(
                    'osd', 'pool', 'set', pool_name,
                    'allow_ec_overwrites',
                    'true')
            self.raw_cluster_cmd(
                'osd', 'pool', 'application', 'enable',
                pool_name, 'rados', '--yes-i-really-mean-it',
                run.Raw('||'), 'true')
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
            assert isinstance(pool_name, str)
            assert pool_name in self.pools
            self.log("removing pool_name %s" % (pool_name,))
            del self.pools[pool_name]
            self.raw_cluster_cmd('osd', 'pool', 'rm', pool_name, pool_name,
                                 "--yes-i-really-really-mean-it")

    def get_pool(self):
        """
        Pick a random pool
        """
        with self.lock:
            if self.pools:
                return random.sample(self.pools.keys(), 1)[0]

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
        :returns: property as string
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
            return output.split()[1]

    def get_pool_int_property(self, pool_name, prop):
        return int(self.get_pool_property(pool_name, prop))

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
            assert isinstance(pool_name, str)
            assert isinstance(by, int)
            assert pool_name in self.pools
            if self.get_num_creating() > 0:
                return False
            if (self.pools[pool_name] + by) > max_pgs:
                return False
            self.log("increase pool size by %d" % (by,))
            new_pg_num = self.pools[pool_name] + by
            self.set_pool_property(pool_name, "pg_num", new_pg_num)
            self.pools[pool_name] = new_pg_num
            return True

    def contract_pool(self, pool_name, by, min_pgs):
        """
        Decrease the number of pgs in a pool
        """
        with self.lock:
            self.log('contract_pool %s by %s min %s' % (
                     pool_name, str(by), str(min_pgs)))
            assert isinstance(pool_name, str)
            assert isinstance(by, int)
            assert pool_name in self.pools
            if self.get_num_creating() > 0:
                self.log('too many creating')
                return False
            proj = self.pools[pool_name] - by
            if proj < min_pgs:
                self.log('would drop below min_pgs, proj %d, currently %d' % (proj,self.pools[pool_name],))
                return False
            self.log("decrease pool size by %d" % (by,))
            new_pg_num = self.pools[pool_name] - by
            self.set_pool_property(pool_name, "pg_num", new_pg_num)
            self.pools[pool_name] = new_pg_num
            return True

    def stop_pg_num_changes(self):
        """
        Reset all pg_num_targets back to pg_num, canceling splits and merges
        """
        self.log('Canceling any pending splits or merges...')
        osd_dump = self.get_osd_dump_json()
        try:
            for pool in osd_dump['pools']:
                if pool['pg_num'] != pool['pg_num_target']:
                    self.log('Setting pool %s (%d) pg_num %d -> %d' %
                             (pool['pool_name'], pool['pool'],
                              pool['pg_num_target'],
                              pool['pg_num']))
                    self.raw_cluster_cmd('osd', 'pool', 'set', pool['pool_name'],
                                         'pg_num', str(pool['pg_num']))
        except KeyError:
            # we don't support pg_num_target before nautilus
            pass

    def set_pool_pgpnum(self, pool_name, force):
        """
        Set pgpnum property of pool_name pool.
        """
        with self.lock:
            assert isinstance(pool_name, str)
            assert pool_name in self.pools
            if not force and self.get_num_creating() > 0:
                return False
            self.set_pool_property(pool_name, 'pgp_num', self.pools[pool_name])
            return True

    def list_pg_unfound(self, pgid):
        """
        return list of unfound pgs with the id specified
        """
        r = None
        offset = {}
        while True:
            out = self.raw_cluster_cmd('--', 'pg', pgid, 'list_unfound',
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
        try:
            return j['pg_map']['pg_stats']
        except KeyError:
            return j['pg_stats']

    def get_osd_df(self, osdid):
        """
        Get the osd df stats
        """
        out = self.raw_cluster_cmd('osd', 'df', 'name', 'osd.{}'.format(osdid),
                                   '--format=json')
        j = json.loads('\n'.join(out.split('\n')[1:]))
        return j['nodes'][0]

    def get_pool_df(self, name):
        """
        Get the pool df stats
        """
        out = self.raw_cluster_cmd('df', 'detail', '--format=json')
        j = json.loads('\n'.join(out.split('\n')[1:]))
        return next((p['stats'] for p in j['pools'] if p['name'] == name),
                    None)

    def get_pgids_to_force(self, backfill):
        """
        Return the randomized list of PGs that can have their recovery/backfill forced
        """
        j = self.get_pg_stats();
        pgids = []
        if backfill:
            wanted = ['degraded', 'backfilling', 'backfill_wait']
        else:
            wanted = ['recovering', 'degraded', 'recovery_wait']
        for pg in j:
            status = pg['state'].split('+')
            for t in wanted:
                if random.random() > 0.5 and not ('forced_backfill' in status or 'forced_recovery' in status) and t in status:
                    pgids.append(pg['pgid'])
                    break
        return pgids

    def get_pgids_to_cancel_force(self, backfill):
       """
       Return the randomized list of PGs whose recovery/backfill priority is forced
       """
       j = self.get_pg_stats();
       pgids = []
       if backfill:
           wanted = 'forced_backfill'
       else:
           wanted = 'forced_recovery'
       for pg in j:
           status = pg['state'].split('+')
           if wanted in status and random.random() > 0.5:
               pgids.append(pg['pgid'])
       return pgids

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

    @wait_for_pg_stats # type: ignore
    def with_pg_state(self, pool, pgnum, check):
        pgstr = self.get_pgid(pool, pgnum)
        stats = self.get_single_pg_stats(pgstr)
        assert(check(stats['state']))

    @wait_for_pg_stats # type: ignore
    def with_pg(self, pool, pgnum, check):
        pgstr = self.get_pgid(pool, pgnum)
        stats = self.get_single_pg_stats(pgstr)
        return check(stats)

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
        RESEND_TIMEOUT = 120    # Must be a multiple of SLEEP_TIME
        FATAL_TIMEOUT = RESEND_TIMEOUT * 3
        SLEEP_TIME = 10
        timer = 0
        while init == self.get_last_scrub_stamp(pool, pgnum):
            assert timer < FATAL_TIMEOUT, "fatal timeout trying to " + stype
            self.log("waiting for scrub type %s" % (stype,))
            if (timer % RESEND_TIMEOUT) == 0:
                self.raw_cluster_cmd('pg', stype, self.get_pgid(pool, pgnum))
                # The first time in this loop is the actual request
                if timer != 0 and stype == "repair":
                    self.log("WARNING: Resubmitted a non-idempotent repair")
            time.sleep(SLEEP_TIME)
            timer += SLEEP_TIME

    def wait_snap_trimming_complete(self, pool):
        """
        Wait for snap trimming on pool to end
        """
        POLL_PERIOD = 10
        FATAL_TIMEOUT = 600
        start = time.time()
        poolnum = self.get_pool_num(pool)
        poolnumstr = "%s." % (poolnum,)
        while (True):
            now = time.time()
            if (now - start) > FATAL_TIMEOUT:
                assert (now - start) < FATAL_TIMEOUT, \
                    'failed to complete snap trimming before timeout'
            all_stats = self.get_pg_stats()
            trimming = False
            for pg in all_stats:
                if (poolnumstr in pg['pgid']) and ('snaptrim' in pg['state']):
                    self.log("pg {pg} in trimming, state: {state}".format(
                        pg=pg['pgid'],
                        state=pg['state']))
                    trimming = True
            if not trimming:
                break
            self.log("{pool} still trimming, waiting".format(pool=pool))
            time.sleep(POLL_PERIOD)

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
        if pool_dump["type"] == PoolType.ERASURE_CODED:
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

    def get_osd_metadata(self):
        """
        osd metadata --format=json converted to a python object
        :returns: the python object containing osd metadata information
        """
        out = self.raw_cluster_cmd('osd', 'metadata', '--format=json')
        return json.loads('\n'.join(out.split('\n')[1:]))

    def get_mgr_dump(self):
        out = self.raw_cluster_cmd('mgr', 'dump', '--format=json')
        return json.loads(out)

    def get_stuck_pgs(self, type_, threshold):
        """
        :returns: stuck pg information from the cluster
        """
        out = self.raw_cluster_cmd('pg', 'dump_stuck', type_, str(threshold),
                                   '--format=json')
        return json.loads(out).get('stuck_pg_stats',[])

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
        return self._get_num_active_clean(pgs)

    def _get_num_active_clean(self, pgs):
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
        return self._get_num_active_recovered(pgs)

    def _get_num_active_recovered(self, pgs):
        num = 0
        for pg in pgs:
            if (pg['state'].count('active') and
                    not pg['state'].count('recover') and
                    not pg['state'].count('backfilling') and
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
        return self._get_num_active(pgs)

    def _get_num_active(self, pgs):
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
        return self._get_num_active_down(pgs)

    def _get_num_active_down(self, pgs):
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

    def get_num_peered(self):
        """
        Find the number of PGs that are peered
        """
        pgs = self.get_pg_stats()
        return self._get_num_peered(pgs)

    def _get_num_peered(self, pgs):
        num = 0
        for pg in pgs:
            if pg['state'].count('peered') and not pg['state'].count('stale'):
                 num += 1
        return num

    def is_clean(self):
        """
        True if all pgs are clean
        """
        pgs = self.get_pg_stats()
        if self._get_num_active_clean(pgs) == len(pgs):
            return True
        else:
            self.dump_pgs_not_active_clean()
            return False

    def is_recovered(self):
        """
        True if all pgs have recovered
        """
        pgs = self.get_pg_stats()
        return self._get_num_active_recovered(pgs) == len(pgs)

    def is_active_or_down(self):
        """
        True if all pgs are active or down
        """
        pgs = self.get_pg_stats()
        return self._get_num_active_down(pgs) == len(pgs)

    def dump_pgs_not_active_clean(self):
        """
        Dumps all pgs that are not active+clean
        """
        pgs = self.get_pg_stats()
        for pg in pgs:
           if pg['state'] != 'active+clean':
             self.log('PG %s is not active+clean' % pg['pgid'])
             self.log(pg)

    def dump_pgs_not_active_down(self):
        """
        Dumps all pgs that are not active or down
        """
        pgs = self.get_pg_stats()
        for pg in pgs:
           if 'active' not in pg['state'] and 'down' not in pg['state']:
             self.log('PG %s is not active or down' % pg['pgid'])
             self.log(pg)

    def dump_pgs_not_active(self):
        """
        Dumps all pgs that are not active
        """
        pgs = self.get_pg_stats()
        for pg in pgs:
           if 'active' not in pg['state']:
             self.log('PG %s is not active' % pg['pgid'])
             self.log(pg)

    def dump_pgs_not_active_peered(self, pgs):
        for pg in pgs:
            if (not pg['state'].count('active')) and (not pg['state'].count('peered')):
                self.log('PG %s is not active or peered' % pg['pgid'])
                self.log(pg)

    def wait_for_clean(self, timeout=1200):
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
                        self.log('dumping pgs not clean')
                        self.dump_pgs_not_active_clean()
                        assert time.time() - start < timeout, \
                            'wait_for_clean: failed before timeout expired'
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

    def wait_for_all_osds_up(self, timeout=None):
        """
        When this exits, either the timeout has expired, or all
        osds are up.
        """
        self.log("waiting for all up")
        start = time.time()
        while not self.are_all_osds_up():
            if timeout is not None:
                assert time.time() - start < timeout, \
                    'timeout expired in wait_for_all_osds_up'
            time.sleep(3)
        self.log("all up!")

    def pool_exists(self, pool):
        if pool in self.list_pools():
            return True
        return False

    def wait_for_pool(self, pool, timeout=300):
        """
        Wait for a pool to exist
        """
        self.log('waiting for pool %s to exist' % pool)
        start = time.time()
        while not self.pool_exists(pool):
            if timeout is not None:
                assert time.time() - start < timeout, \
                    'timeout expired in wait_for_pool'
            time.sleep(3)

    def wait_for_pools(self, pools):
        for pool in pools:
            self.wait_for_pool(pool)

    def is_mgr_available(self):
        x = self.get_mgr_dump()
        return x.get('available', False)

    def wait_for_mgr_available(self, timeout=None):
        self.log("waiting for mgr available")
        start = time.time()
        while not self.is_mgr_available():
            if timeout is not None:
                assert time.time() - start < timeout, \
                    'timeout expired in wait_for_mgr_available'
            time.sleep(3)
        self.log("mgr available!")

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
                        if self.is_recovered():
                            break
                        self.log('dumping pgs not recovered yet')
                        self.dump_pgs_not_active_clean()
                        assert now - start < timeout, \
                            'wait_for_recovery: failed before timeout expired'
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
                    self.log('dumping pgs not active')
                    self.dump_pgs_not_active()
                    assert time.time() - start < timeout, \
                        'wait_for_active: failed before timeout expired'
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
                    self.log('dumping pgs not active or down')
                    self.dump_pgs_not_active_down()
                    assert time.time() - start < timeout, \
                        'wait_for_active_or_down: failed before timeout expired'
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

    def all_active_or_peered(self):
        """
        Wrapper to check if all PGs are active or peered
        """
        self.log("checking for active or peered")
        pgs = self.get_pg_stats()
        if self._get_num_active(pgs) + self._get_num_peered(pgs) == len(pgs):
            self.log("all pgs are active or peered!")
            return True
        else:
            self.dump_pgs_not_active_peered(pgs)
            return False

    def wait_till_active(self, timeout=None):
        """
        Wait until all pgs are active.
        """
        self.log("waiting till active")
        start = time.time()
        while not self.is_active():
            if timeout is not None:
                if time.time() - start >= timeout:
                    self.log('dumping pgs not active')
                    self.dump_pgs_not_active()
                    assert time.time() - start < timeout, \
                        'wait_till_active: failed before timeout expired'
            time.sleep(3)
        self.log("active!")

    def wait_till_pg_convergence(self, timeout=None):
        start = time.time()
        old_stats = None
        active_osds = [osd['osd'] for osd in self.get_osd_dump()
                       if osd['in'] and osd['up']]
        while True:
            # strictly speaking, no need to wait for mon. but due to the
            # "ms inject socket failures" setting, the osdmap could be delayed,
            # so mgr is likely to ignore the pg-stat messages with pgs serving
            # newly created pools which is not yet known by mgr. so, to make sure
            # the mgr is updated with the latest pg-stats, waiting for mon/mgr is
            # necessary.
            self.flush_pg_stats(active_osds)
            new_stats = dict((stat['pgid'], stat['state'])
                             for stat in self.get_pg_stats())
            if old_stats == new_stats:
                return old_stats
            if timeout is not None:
                assert time.time() - start < timeout, \
                    'failed to reach convergence before %d secs' % timeout
            old_stats = new_stats
            # longer than mgr_stats_period
            time.sleep(5 + 1)

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
            self._assert_ipmi(remote)
            remote.console.power_off()
        elif self.config.get('bdev_inject_crash') and self.config.get('bdev_inject_crash_probability'):
            if random.uniform(0, 1) < self.config.get('bdev_inject_crash_probability', .5):
                self.inject_args(
                    'osd', osd,
                    'bdev-inject-crash', self.config.get('bdev_inject_crash'))
                try:
                    self.ctx.daemons.get_daemon('osd', osd, self.cluster).wait()
                except:
                    pass
                else:
                    raise RuntimeError('osd.%s did not fail' % osd)
            else:
                self.ctx.daemons.get_daemon('osd', osd, self.cluster).stop()
        else:
            self.ctx.daemons.get_daemon('osd', osd, self.cluster).stop()

    @staticmethod
    def _assert_ipmi(remote):
        assert remote.console.has_ipmi_credentials, (
            "powercycling requested but RemoteConsole is not "
            "initialized.  Check ipmi config.")

    def blackhole_kill_osd(self, osd):
        """
        Stop osd if nothing else works.
        """
        self.inject_args('osd', osd,
                         'objectstore-blackhole', True)
        time.sleep(2)
        self.ctx.daemons.get_daemon('osd', osd, self.cluster).stop()

    def revive_osd(self, osd, timeout=360, skip_admin_check=False):
        """
        Revive osds by either power cycling (if indicated by the config)
        or by restarting.
        """
        if self.config.get('powercycle'):
            remote = self.find_remote('osd', osd)
            self.log('kill_osd on osd.{o} doing powercycle of {s}'.
                     format(o=osd, s=remote.name))
            self._assert_ipmi(remote)
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
                                       timeout=timeout, stdout=DEVNULL)

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
        Wrapper to local get_daemon call
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
            self._assert_ipmi(remote)
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
            self._assert_ipmi(remote)
            remote.console.power_on()
            self.make_admin_daemon_dir(remote)
        self.ctx.daemons.get_daemon('mon', mon, self.cluster).restart()

    def revive_mgr(self, mgr):
        """
        Restart by either power cycling (if the config says so),
        or by doing a normal restart.
        """
        if self.config.get('powercycle'):
            remote = self.find_remote('mgr', mgr)
            self.log('revive_mgr on mgr.{m} doing powercycle of {s}'.
                     format(m=mgr, s=remote.name))
            self._assert_ipmi(remote)
            remote.console.power_on()
            self.make_admin_daemon_dir(remote)
        self.ctx.daemons.get_daemon('mgr', mgr, self.cluster).restart()

    def get_mon_status(self, mon):
        """
        Extract all the monitor status information from the cluster
        """
        out = self.raw_cluster_cmd('tell', 'mon.%s' % mon, 'mon_status')
        return json.loads(out)

    def get_mon_quorum(self):
        """
        Extract monitor quorum information from the cluster
        """
        out = self.raw_cluster_cmd('quorum_status')
        j = json.loads(out)
        return j['quorum']

    def wait_for_mon_quorum_size(self, size, timeout=300):
        """
        Loop until quorum size is reached.
        """
        self.log('waiting for quorum size %d' % size)
        sleep = 3
        with safe_while(sleep=sleep,
                        tries=timeout // sleep,
                        action=f'wait for quorum size {size}') as proceed:
            while proceed():
                try:
                    if len(self.get_mon_quorum()) == size:
                        break
                except CommandFailedError as e:
                    # could fail instea4d of blocked if the rotating key of the
                    # connected monitor is not updated yet after they form the
                    # quorum
                    if e.exitstatus == errno.EACCES:
                        pass
                    else:
                        raise
        self.log("quorum is size %d" % size)

    def get_mon_health(self, debug=False, detail=False):
        """
        Extract all the monitor health information.
        """
        if detail:
            out = self.raw_cluster_cmd('health', 'detail', '--format=json')
        else:
            out = self.raw_cluster_cmd('health', '--format=json')
        if debug:
            self.log('health:\n{h}'.format(h=out))
        return json.loads(out)

    def wait_until_healthy(self, timeout=None):
        self.log("wait_until_healthy")
        start = time.time()
        while self.get_mon_health()['status'] != 'HEALTH_OK':
            if timeout is not None:
                assert time.time() - start < timeout, \
                    'timeout expired in wait_until_healthy'
            time.sleep(3)
        self.log("wait_until_healthy done")

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

    def get_service_task_status(self, service, status_key):
        """
        Return daemon task status for a given ceph service.

        :param service: ceph service (mds, osd, etc...)
        :param status_key: matching task status key
        """
        task_status = {}
        status = self.raw_cluster_status()
        try:
            for k,v in status['servicemap']['services'][service]['daemons'].items():
                ts = dict(v).get('task_status', None)
                if ts:
                    task_status[k] = ts[status_key]
        except KeyError: # catches missing service and status key
            return {}
        self.log(task_status)
        return task_status

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
        cluster = config.get('cluster', 'ceph')
        fn = getattr(ctx.managers[cluster], name)
        fn(*args, **kwargs)
    return task

revive_osd = utility_task("revive_osd")
revive_mon = utility_task("revive_mon")
kill_osd = utility_task("kill_osd")
kill_mon = utility_task("kill_mon")
create_pool = utility_task("create_pool")
remove_pool = utility_task("remove_pool")
wait_for_clean = utility_task("wait_for_clean")
flush_all_pg_stats = utility_task("flush_all_pg_stats")
set_pool_property = utility_task("set_pool_property")
do_pg_scrub = utility_task("do_pg_scrub")
wait_for_pool = utility_task("wait_for_pool")
wait_for_pools = utility_task("wait_for_pools")
