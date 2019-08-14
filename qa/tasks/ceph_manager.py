"""
ceph manager -- CephManager objects
"""
from cStringIO import StringIO
from functools import wraps
import contextlib
import random
import time
import base64
import json
import logging
import threading
import os
from teuthology import misc as teuthology
from util.rados import cmd_erasure_code_profile
from util import get_remote
from teuthology.orchestra import run
from teuthology.exceptions import CommandFailedError

try:
    from subprocess import DEVNULL # py3k
except ImportError:
    DEVNULL = open(os.devnull, 'r+')

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


class PoolType:
    REPLICATED = 1
    ERASURE_CODED = 3


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

    def run(self, options, args, stdin=None, stdout=None):
        if stdout is None:
            stdout = StringIO()
        self.manager.kill_osd(self.osd)
        cmd = self.build_cmd(options, args, stdin)
        self.manager.log(cmd)
        try:
            proc = self.remote.run(args=['bash', '-e', '-x', '-c', cmd],
                                   check_status=False,
                                   stdout=stdout,
                                   stderr=StringIO())
            proc.wait()
            if proc.exitstatus != 0:
                self.manager.log("failed with " + str(proc.exitstatus))
                error = proc.stdout.getvalue() + " " + proc.stderr.getvalue()
                raise Exception(error)
        finally:
            if self.do_revive:
                self.manager.revive_osd(self.osd)
                self.manager.wait_till_osd_is_up(self.osd, 300)


class CephManager:
    """
    Ceph manager object.
    Contains several local functions that form a bulk of this module.

    Note: this class has nothing to do with the Ceph daemon (ceph-mgr) of
    the same name.
    """

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
                self.pools[pool] = self.get_pool_int_property(pool, 'pg_num')
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

    def raw_cluster_cmd_result(self, *args, **kwargs):
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
        kwargs['args'] = ceph_args
        kwargs['check_status'] = False
        proc = self.controller.run(**kwargs)
        return proc.exitstatus

    def run_ceph_w(self, watch_channel=None):
        """
        Execute "ceph -w" in the background with stdout connected to a StringIO,
        and return the RemoteProcess.

        :param watch_channel: Specifies the channel to be watched. This can be
                              'cluster', 'audit', ...
        :type watch_channel: str
        """
        args = ["sudo",
                "daemon-helper",
                "kill",
                "ceph",
                '--cluster',
                self.cluster,
                "-w"]
        if watch_channel is not None:
            args.append("--watch-channel")
            args.append(watch_channel)
        return self.controller.run(args=args, wait=False, stdout=StringIO(), stdin=run.PIPE)

    def flush_pg_stats(self, osds, no_wait=None, wait_for_mon=300):
        """
        Flush pg stats from a list of OSD ids, ensuring they are reflected
        all the way to the monitor.  Luminous and later only.

        :param osds: list of OSDs to flush
        :param no_wait: list of OSDs not to wait for seq id. by default, we
                        wait for all specified osds, but some of them could be
                        moved out of osdmap, so we cannot get their updated
                        stat seq from monitor anymore. in that case, you need
                        to pass a blacklist.
        :param wait_for_mon: wait for mon to be synced with mgr. 0 to disable
                             it. (5 min by default)
        """
        seq = {osd: int(self.raw_cluster_cmd('tell', 'osd.%d' % osd, 'flush_pg_stats'))
               for osd in osds}
        if not wait_for_mon:
            return
        if no_wait is None:
            no_wait = []
        for osd, need in seq.iteritems():
            if osd in no_wait:
                continue
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

    def flush_all_pg_stats(self):
        self.flush_pg_stats(range(len(self.get_osd_dump())))

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
            if proc.exitstatus is 0:
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
        for k, v in argdict.iteritems():
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
            assert isinstance(pool_name, basestring)
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
        :returns: property as string
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
            assert isinstance(pool_name, basestring)
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
        for pool in osd_dump['pools']:
            if pool['pg_num'] != pool['pg_num_target']:
                self.log('Setting pool %s (%d) pg_num %d -> %d' %
                         (pool['pool_name'], pool['pool'],
                          pool['pg_num_target'],
                          pool['pg_num']))
                self.raw_cluster_cmd('osd', 'pool', 'set', pool['pool_name'],
                                     'pg_num', str(pool['pg_num']))

    def set_pool_pgpnum(self, pool_name, force):
        """
        Set pgpnum property of pool_name pool.
        """
        with self.lock:
            assert isinstance(pool_name, basestring)
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

    @wait_for_pg_stats
    def with_pg_state(self, pool, pgnum, check):
        pgstr = self.get_pgid(pool, pgnum)
        stats = self.get_single_pg_stats(pgstr)
        assert(check(stats['state']))

    @wait_for_pg_stats
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
        return self._get_num_active_clean(pgs) == len(pgs)

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

    def all_active_or_peered(self):
        """
        Wrapper to check if all PGs are active or peered
        """
        pgs = self.get_pg_stats()
        return self._get_num_active(pgs) + self._get_num_peered(pgs) == len(pgs)

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
        addr = self.ctx.ceph[self.cluster].mons['mon.%s' % mon]
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
