
import json
import logging
from gevent import Greenlet
import os
import time
import datetime
import re
import errno
import random
import traceback

from io import BytesIO
from io import StringIO

from teuthology.exceptions import CommandFailedError
from teuthology import misc
from teuthology.nuke import clear_firewall
from teuthology.parallel import parallel
from tasks.ceph_manager import write_conf
from tasks import ceph_manager


log = logging.getLogger(__name__)


DAEMON_WAIT_TIMEOUT = 120
ROOT_INO = 1

class FileLayout(object):
    def __init__(self, pool=None, pool_namespace=None, stripe_unit=None, stripe_count=None, object_size=None):
        self.pool = pool
        self.pool_namespace = pool_namespace
        self.stripe_unit = stripe_unit
        self.stripe_count = stripe_count
        self.object_size = object_size

    @classmethod
    def load_from_ceph(layout_str):
        # TODO
        pass

    def items(self):
        if self.pool is not None:
            yield ("pool", self.pool)
        if self.pool_namespace:
            yield ("pool_namespace", self.pool_namespace)
        if self.stripe_unit is not None:
            yield ("stripe_unit", self.stripe_unit)
        if self.stripe_count is not None:
            yield ("stripe_count", self.stripe_count)
        if self.object_size is not None:
            yield ("object_size", self.stripe_size)

class ObjectNotFound(Exception):
    def __init__(self, object_name):
        self._object_name = object_name

    def __str__(self):
        return "Object not found: '{0}'".format(self._object_name)

class FSStatus(object):
    """
    Operations on a snapshot of the FSMap.
    """
    def __init__(self, mon_manager):
        self.mon = mon_manager
        self.map = json.loads(self.mon.raw_cluster_cmd("fs", "dump", "--format=json"))

    def __str__(self):
        return json.dumps(self.map, indent = 2, sort_keys = True)

    # Expose the fsmap for manual inspection.
    def __getitem__(self, key):
        """
        Get a field from the fsmap.
        """
        return self.map[key]

    def get_filesystems(self):
        """
        Iterator for all filesystems.
        """
        for fs in self.map['filesystems']:
            yield fs

    def get_all(self):
        """
        Iterator for all the mds_info components in the FSMap.
        """
        for info in self.map['standbys']:
            yield info
        for fs in self.map['filesystems']:
            for info in fs['mdsmap']['info'].values():
                yield info

    def get_standbys(self):
        """
        Iterator for all standbys.
        """
        for info in self.map['standbys']:
            yield info

    def get_fsmap(self, fscid):
        """
        Get the fsmap for the given FSCID.
        """
        for fs in self.map['filesystems']:
            if fscid is None or fs['id'] == fscid:
                return fs
        raise RuntimeError("FSCID {0} not in map".format(fscid))

    def get_fsmap_byname(self, name):
        """
        Get the fsmap for the given file system name.
        """
        for fs in self.map['filesystems']:
            if name is None or fs['mdsmap']['fs_name'] == name:
                return fs
        raise RuntimeError("FS {0} not in map".format(name))

    def get_replays(self, fscid):
        """
        Get the standby:replay MDS for the given FSCID.
        """
        fs = self.get_fsmap(fscid)
        for info in fs['mdsmap']['info'].values():
            if info['state'] == 'up:standby-replay':
                yield info

    def get_ranks(self, fscid):
        """
        Get the ranks for the given FSCID.
        """
        fs = self.get_fsmap(fscid)
        for info in fs['mdsmap']['info'].values():
            if info['rank'] >= 0 and info['state'] != 'up:standby-replay':
                yield info

    def get_rank(self, fscid, rank):
        """
        Get the rank for the given FSCID.
        """
        for info in self.get_ranks(fscid):
            if info['rank'] == rank:
                return info
        raise RuntimeError("FSCID {0} has no rank {1}".format(fscid, rank))

    def get_mds(self, name):
        """
        Get the info for the given MDS name.
        """
        for info in self.get_all():
            if info['name'] == name:
                return info
        return None

    def get_mds_addr(self, name):
        """
        Return the instance addr as a string, like "10.214.133.138:6807\/10825"
        """
        info = self.get_mds(name)
        if info:
            return info['addr']
        else:
            log.warning(json.dumps(list(self.get_all()), indent=2))  # dump for debugging
            raise RuntimeError("MDS id '{0}' not found in map".format(name))

    def get_mds_gid(self, gid):
        """
        Get the info for the given MDS gid.
        """
        for info in self.get_all():
            if info['gid'] == gid:
                return info
        return None

    def hadfailover(self, status):
        """
        Compares two statuses for mds failovers.
        Returns True if there is a failover.
        """
        for fs in status.map['filesystems']:
            for info in fs['mdsmap']['info'].values():
                oldinfo = self.get_mds_gid(info['gid'])
                if oldinfo is None or oldinfo['incarnation'] != info['incarnation']:
                    return True
        #all matching
        return False

class CephCluster(object):
    @property
    def admin_remote(self):
        first_mon = misc.get_first_mon(self._ctx, None)
        (result,) = self._ctx.cluster.only(first_mon).remotes.keys()
        return result

    def __init__(self, ctx):
        self._ctx = ctx
        self.mon_manager = ceph_manager.CephManager(self.admin_remote, ctx=ctx, logger=log.getChild('ceph_manager'))

    def get_config(self, key, service_type=None):
        """
        Get config from mon by default, or a specific service if caller asks for it
        """
        if service_type is None:
            service_type = 'mon'

        service_id = sorted(misc.all_roles_of_type(self._ctx.cluster, service_type))[0]
        return self.json_asok(['config', 'get', key], service_type, service_id)[key]

    def set_ceph_conf(self, subsys, key, value):
        if subsys not in self._ctx.ceph['ceph'].conf:
            self._ctx.ceph['ceph'].conf[subsys] = {}
        self._ctx.ceph['ceph'].conf[subsys][key] = value
        write_conf(self._ctx)  # XXX because we don't have the ceph task's config object, if they
                               # used a different config path this won't work.

    def clear_ceph_conf(self, subsys, key):
        del self._ctx.ceph['ceph'].conf[subsys][key]
        write_conf(self._ctx)

    def json_asok(self, command, service_type, service_id, timeout=None):
        if timeout is None:
            timeout = 15*60
        command.insert(0, '--format=json')
        proc = self.mon_manager.admin_socket(service_type, service_id, command, timeout=timeout)
        response_data = proc.stdout.getvalue().strip()
        if len(response_data) > 0:
            j = json.loads(response_data)
            pretty = json.dumps(j, sort_keys=True, indent=2)
            log.debug(f"_json_asok output\n{pretty}")
            return j
        else:
            log.debug("_json_asok output empty")
            return None


class MDSCluster(CephCluster):
    """
    Collective operations on all the MDS daemons in the Ceph cluster.  These
    daemons may be in use by various Filesystems.

    For the benefit of pre-multi-filesystem tests, this class is also
    a parent of Filesystem.  The correct way to use MDSCluster going forward is
    as a separate instance outside of your (multiple) Filesystem instances.
    """
    def __init__(self, ctx):
        super(MDSCluster, self).__init__(ctx)

        self.mds_ids = list(misc.all_roles_of_type(ctx.cluster, 'mds'))

        if len(self.mds_ids) == 0:
            raise RuntimeError("This task requires at least one MDS")

        if hasattr(self._ctx, "daemons"):
            # Presence of 'daemons' attribute implies ceph task rather than ceph_deploy task
            self.mds_daemons = dict([(mds_id, self._ctx.daemons.get_daemon('mds', mds_id)) for mds_id in self.mds_ids])

    def _one_or_all(self, mds_id, cb, in_parallel=True):
        """
        Call a callback for a single named MDS, or for all.

        Note that the parallelism here isn't for performance, it's to avoid being overly kind
        to the cluster by waiting a graceful ssh-latency of time between doing things, and to
        avoid being overly kind by executing them in a particular order.  However, some actions
        don't cope with being done in parallel, so it's optional (`in_parallel`)

        :param mds_id: MDS daemon name, or None
        :param cb: Callback taking single argument of MDS daemon name
        :param in_parallel: whether to invoke callbacks concurrently (else one after the other)
        """
        if mds_id is None:
            if in_parallel:
                with parallel() as p:
                    for mds_id in self.mds_ids:
                        p.spawn(cb, mds_id)
            else:
                for mds_id in self.mds_ids:
                    cb(mds_id)
        else:
            cb(mds_id)

    def get_config(self, key, service_type=None):
        """
        get_config specialization of service_type="mds"
        """
        if service_type != "mds":
            return super(MDSCluster, self).get_config(key, service_type)

        # Some tests stop MDS daemons, don't send commands to a dead one:
        running_daemons = [i for i, mds in self.mds_daemons.items() if mds.running()]
        service_id = random.sample(running_daemons, 1)[0]
        return self.json_asok(['config', 'get', key], service_type, service_id)[key]

    def mds_stop(self, mds_id=None):
        """
        Stop the MDS daemon process(se).  If it held a rank, that rank
        will eventually go laggy.
        """
        self._one_or_all(mds_id, lambda id_: self.mds_daemons[id_].stop())

    def mds_fail(self, mds_id=None):
        """
        Inform MDSMonitor of the death of the daemon process(es).  If it held
        a rank, that rank will be relinquished.
        """
        self._one_or_all(mds_id, lambda id_: self.mon_manager.raw_cluster_cmd("mds", "fail", id_))

    def mds_restart(self, mds_id=None):
        self._one_or_all(mds_id, lambda id_: self.mds_daemons[id_].restart())

    def mds_fail_restart(self, mds_id=None):
        """
        Variation on restart that includes marking MDSs as failed, so that doing this
        operation followed by waiting for healthy daemon states guarantees that they
        have gone down and come up, rather than potentially seeing the healthy states
        that existed before the restart.
        """
        def _fail_restart(id_):
            self.mds_daemons[id_].stop()
            self.mon_manager.raw_cluster_cmd("mds", "fail", id_)
            self.mds_daemons[id_].restart()

        self._one_or_all(mds_id, _fail_restart)

    def mds_signal(self, mds_id, sig, silent=False):
        """
        signal a MDS daemon
        """
        self.mds_daemons[mds_id].signal(sig, silent);

    def newfs(self, name='cephfs', create=True):
        return Filesystem(self._ctx, name=name, create=create)

    def status(self):
        return FSStatus(self.mon_manager)

    def delete_all_filesystems(self):
        """
        Remove all filesystems that exist, and any pools in use by them.
        """
        pools = json.loads(self.mon_manager.raw_cluster_cmd("osd", "dump", "--format=json-pretty"))['pools']
        pool_id_name = {}
        for pool in pools:
            pool_id_name[pool['pool']] = pool['pool_name']

        # mark cluster down for each fs to prevent churn during deletion
        status = self.status()
        for fs in status.get_filesystems():
            self.mon_manager.raw_cluster_cmd("fs", "fail", str(fs['mdsmap']['fs_name']))

        # get a new copy as actives may have since changed
        status = self.status()
        for fs in status.get_filesystems():
            mdsmap = fs['mdsmap']
            metadata_pool = pool_id_name[mdsmap['metadata_pool']]

            self.mon_manager.raw_cluster_cmd('fs', 'rm', mdsmap['fs_name'], '--yes-i-really-mean-it')
            self.mon_manager.raw_cluster_cmd('osd', 'pool', 'delete',
                                             metadata_pool, metadata_pool,
                                             '--yes-i-really-really-mean-it')
            for data_pool in mdsmap['data_pools']:
                data_pool = pool_id_name[data_pool]
                try:
                    self.mon_manager.raw_cluster_cmd('osd', 'pool', 'delete',
                                                     data_pool, data_pool,
                                                     '--yes-i-really-really-mean-it')
                except CommandFailedError as e:
                    if e.exitstatus == 16: # EBUSY, this data pool is used
                        pass               # by two metadata pools, let the 2nd
                    else:                  # pass delete it
                        raise

    def get_standby_daemons(self):
        return set([s['name'] for s in self.status().get_standbys()])

    def get_mds_hostnames(self):
        result = set()
        for mds_id in self.mds_ids:
            mds_remote = self.mon_manager.find_remote('mds', mds_id)
            result.add(mds_remote.hostname)

        return list(result)

    def set_clients_block(self, blocked, mds_id=None):
        """
        Block (using iptables) client communications to this MDS.  Be careful: if
        other services are running on this MDS, or other MDSs try to talk to this
        MDS, their communications may also be blocked as collatoral damage.

        :param mds_id: Optional ID of MDS to block, default to all
        :return:
        """
        da_flag = "-A" if blocked else "-D"

        def set_block(_mds_id):
            remote = self.mon_manager.find_remote('mds', _mds_id)
            status = self.status()

            addr = status.get_mds_addr(_mds_id)
            ip_str, port_str, inst_str = re.match("(.+):(.+)/(.+)", addr).groups()

            remote.run(
                args=["sudo", "iptables", da_flag, "OUTPUT", "-p", "tcp", "--sport", port_str, "-j", "REJECT", "-m",
                      "comment", "--comment", "teuthology"])
            remote.run(
                args=["sudo", "iptables", da_flag, "INPUT", "-p", "tcp", "--dport", port_str, "-j", "REJECT", "-m",
                      "comment", "--comment", "teuthology"])

        self._one_or_all(mds_id, set_block, in_parallel=False)

    def clear_firewall(self):
        clear_firewall(self._ctx)

    def get_mds_info(self, mds_id):
        return FSStatus(self.mon_manager).get_mds(mds_id)

    def is_pool_full(self, pool_name):
        pools = json.loads(self.mon_manager.raw_cluster_cmd("osd", "dump", "--format=json-pretty"))['pools']
        for pool in pools:
            if pool['pool_name'] == pool_name:
                return 'full' in pool['flags_names'].split(",")

        raise RuntimeError("Pool not found '{0}'".format(pool_name))

class Filesystem(MDSCluster):
    """
    This object is for driving a CephFS filesystem.  The MDS daemons driven by
    MDSCluster may be shared with other Filesystems.
    """
    def __init__(self, ctx, fscid=None, name=None, create=False,
                 ec_profile=None):
        super(Filesystem, self).__init__(ctx)

        self.name = name
        self.ec_profile = ec_profile
        self.id = None
        self.metadata_pool_name = None
        self.metadata_overlay = False
        self.data_pool_name = None
        self.data_pools = None

        client_list = list(misc.all_roles_of_type(self._ctx.cluster, 'client'))
        self.client_id = client_list[0]
        self.client_remote = list(misc.get_clients(ctx=ctx, roles=["client.{0}".format(self.client_id)]))[0][1]

        if name is not None:
            if fscid is not None:
                raise RuntimeError("cannot specify fscid when creating fs")
            if create and not self.legacy_configured():
                self.create()
        else:
            if fscid is not None:
                self.id = fscid
                self.getinfo(refresh = True)

        # Stash a reference to the first created filesystem on ctx, so
        # that if someone drops to the interactive shell they can easily
        # poke our methods.
        if not hasattr(self._ctx, "filesystem"):
            self._ctx.filesystem = self

    def get_task_status(self, status_key):
        return self.mon_manager.get_service_task_status("mds", status_key)

    def getinfo(self, refresh = False):
        status = self.status()
        if self.id is not None:
            fsmap = status.get_fsmap(self.id)
        elif self.name is not None:
            fsmap = status.get_fsmap_byname(self.name)
        else:
            fss = [fs for fs in status.get_filesystems()]
            if len(fss) == 1:
                fsmap = fss[0]
            elif len(fss) == 0:
                raise RuntimeError("no file system available")
            else:
                raise RuntimeError("more than one file system available")
        self.id = fsmap['id']
        self.name = fsmap['mdsmap']['fs_name']
        self.get_pool_names(status = status, refresh = refresh)
        return status

    def set_metadata_overlay(self, overlay):
        if self.id is not None:
            raise RuntimeError("cannot specify fscid when configuring overlay")
        self.metadata_overlay = overlay

    def deactivate(self, rank):
        if rank < 0:
            raise RuntimeError("invalid rank")
        elif rank == 0:
            raise RuntimeError("cannot deactivate rank 0")
        self.mon_manager.raw_cluster_cmd("mds", "deactivate", "%d:%d" % (self.id, rank))

    def reach_max_mds(self):
        # Try to reach rank count == max_mds, up or down (UPGRADE SENSITIVE!)
        status = self.getinfo()
        mds_map = self.get_mds_map(status=status)
        max_mds = mds_map['max_mds']

        count = len(list(self.get_ranks(status=status)))
        if count > max_mds:
            try:
                # deactivate mds in decending order
                status = self.wait_for_daemons(status=status, skip_max_mds_check=True)
                while count > max_mds:
                    targets = sorted(self.get_ranks(status=status), key=lambda r: r['rank'], reverse=True)
                    target = targets[0]
                    log.info("deactivating rank %d" % target['rank'])
                    self.deactivate(target['rank'])
                    status = self.wait_for_daemons(skip_max_mds_check=True)
                    count = len(list(self.get_ranks(status=status)))
            except:
                # In Mimic, deactivation is done automatically:
                log.info("Error:\n{}".format(traceback.format_exc()))
                status = self.wait_for_daemons()
        else:
            status = self.wait_for_daemons()

        mds_map = self.get_mds_map(status=status)
        assert(mds_map['max_mds'] == max_mds)
        assert(mds_map['in'] == list(range(0, max_mds)))

    def fail(self):
        self.mon_manager.raw_cluster_cmd("fs", "fail", str(self.name))

    def set_flag(self, var, *args):
        a = map(lambda x: str(x).lower(), args)
        self.mon_manager.raw_cluster_cmd("fs", "flag", "set", var, *a)

    def set_allow_multifs(self, yes=True):
        self.set_flag("enable_multiple", yes)

    def set_var(self, var, *args):
        a = map(lambda x: str(x).lower(), args)
        self.mon_manager.raw_cluster_cmd("fs", "set", self.name, var, *a)

    def set_down(self, down=True):
        self.set_var("down", str(down).lower())

    def set_joinable(self, joinable=True):
        self.set_var("joinable", joinable)

    def set_max_mds(self, max_mds):
        self.set_var("max_mds", "%d" % max_mds)

    def set_allow_standby_replay(self, yes):
        self.set_var("allow_standby_replay", yes)

    def set_allow_new_snaps(self, yes):
        self.set_var("allow_new_snaps", yes, '--yes-i-really-mean-it')

    # In Octopus+, the PG count can be omitted to use the default. We keep the
    # hard-coded value for deployments of Mimic/Nautilus.
    pgs_per_fs_pool = 8

    def create(self):
        if self.name is None:
            self.name = "cephfs"
        if self.metadata_pool_name is None:
            self.metadata_pool_name = "{0}_metadata".format(self.name)
        if self.data_pool_name is None:
            data_pool_name = "{0}_data".format(self.name)
        else:
            data_pool_name = self.data_pool_name

        log.info("Creating filesystem '{0}'".format(self.name))

        self.mon_manager.raw_cluster_cmd('osd', 'pool', 'create',
                                         self.metadata_pool_name, self.pgs_per_fs_pool.__str__())
        if self.metadata_overlay:
            self.mon_manager.raw_cluster_cmd('fs', 'new',
                                             self.name, self.metadata_pool_name, data_pool_name,
                                             '--allow-dangerous-metadata-overlay')
        else:
            if self.ec_profile and 'disabled' not in self.ec_profile:
                log.info("EC profile is %s", self.ec_profile)
                cmd = ['osd', 'erasure-code-profile', 'set', data_pool_name]
                cmd.extend(self.ec_profile)
                self.mon_manager.raw_cluster_cmd(*cmd)
                self.mon_manager.raw_cluster_cmd(
                    'osd', 'pool', 'create',
                    data_pool_name, self.pgs_per_fs_pool.__str__(), 'erasure',
                    data_pool_name)
                self.mon_manager.raw_cluster_cmd(
                    'osd', 'pool', 'set',
                    data_pool_name, 'allow_ec_overwrites', 'true')
            else:
                self.mon_manager.raw_cluster_cmd(
                    'osd', 'pool', 'create',
                    data_pool_name, self.pgs_per_fs_pool.__str__())
            self.mon_manager.raw_cluster_cmd('fs', 'new',
                                             self.name,
                                             self.metadata_pool_name,
                                             data_pool_name,
                                             "--force")
        self.check_pool_application(self.metadata_pool_name)
        self.check_pool_application(data_pool_name)
        # Turn off spurious standby count warnings from modifying max_mds in tests.
        try:
            self.mon_manager.raw_cluster_cmd('fs', 'set', self.name, 'standby_count_wanted', '0')
        except CommandFailedError as e:
            if e.exitstatus == 22:
                # standby_count_wanted not available prior to luminous (upgrade tests would fail otherwise)
                pass
            else:
                raise

        self.getinfo(refresh = True)

        
    def check_pool_application(self, pool_name):
        osd_map = self.mon_manager.get_osd_dump_json()
        for pool in osd_map['pools']:
            if pool['pool_name'] == pool_name:
                if "application_metadata" in pool:
                    if not "cephfs" in pool['application_metadata']:
                        raise RuntimeError("Pool {pool_name} does not name cephfs as application!".\
                                           format(pool_name=pool_name))
        

    def __del__(self):
        if getattr(self._ctx, "filesystem", None) == self:
            delattr(self._ctx, "filesystem")

    def exists(self):
        """
        Whether a filesystem exists in the mon's filesystem list
        """
        fs_list = json.loads(self.mon_manager.raw_cluster_cmd('fs', 'ls', '--format=json-pretty'))
        return self.name in [fs['name'] for fs in fs_list]

    def legacy_configured(self):
        """
        Check if a legacy (i.e. pre "fs new") filesystem configuration is present.  If this is
        the case, the caller should avoid using Filesystem.create
        """
        try:
            out_text = self.mon_manager.raw_cluster_cmd('--format=json-pretty', 'osd', 'lspools')
            pools = json.loads(out_text)
            metadata_pool_exists = 'metadata' in [p['poolname'] for p in pools]
            if metadata_pool_exists:
                self.metadata_pool_name = 'metadata'
        except CommandFailedError as e:
            # For use in upgrade tests, Ceph cuttlefish and earlier don't support
            # structured output (--format) from the CLI.
            if e.exitstatus == 22:
                metadata_pool_exists = True
            else:
                raise

        return metadata_pool_exists

    def _df(self):
        return json.loads(self.mon_manager.raw_cluster_cmd("df", "--format=json-pretty"))

    def get_mds_map(self, status=None):
        if status is None:
            status = self.status()
        return status.get_fsmap(self.id)['mdsmap']

    def get_var(self, var, status=None):
        return self.get_mds_map(status=status)[var]

    def set_dir_layout(self, mount, path, layout):
        for name, value in layout.items():
            mount.run_shell(args=["setfattr", "-n", "ceph.dir.layout."+name, "-v", str(value), path])

    def add_data_pool(self, name, create=True):
        if create:
            self.mon_manager.raw_cluster_cmd('osd', 'pool', 'create', name, self.pgs_per_fs_pool.__str__())
        self.mon_manager.raw_cluster_cmd('fs', 'add_data_pool', self.name, name)
        self.get_pool_names(refresh = True)
        for poolid, fs_name in self.data_pools.items():
            if name == fs_name:
                return poolid
        raise RuntimeError("could not get just created pool '{0}'".format(name))

    def get_pool_names(self, refresh = False, status = None):
        if refresh or self.metadata_pool_name is None or self.data_pools is None:
            if status is None:
                status = self.status()
            fsmap = status.get_fsmap(self.id)

            osd_map = self.mon_manager.get_osd_dump_json()
            id_to_name = {}
            for p in osd_map['pools']:
                id_to_name[p['pool']] = p['pool_name']

            self.metadata_pool_name = id_to_name[fsmap['mdsmap']['metadata_pool']]
            self.data_pools = {}
            for data_pool in fsmap['mdsmap']['data_pools']:
                self.data_pools[data_pool] = id_to_name[data_pool]

    def get_data_pool_name(self, refresh = False):
        if refresh or self.data_pools is None:
            self.get_pool_names(refresh = True)
        assert(len(self.data_pools) == 1)
        return next(iter(self.data_pools.values()))

    def get_data_pool_id(self, refresh = False):
        """
        Don't call this if you have multiple data pools
        :return: integer
        """
        if refresh or self.data_pools is None:
            self.get_pool_names(refresh = True)
        assert(len(self.data_pools) == 1)
        return next(iter(self.data_pools.keys()))

    def get_data_pool_names(self, refresh = False):
        if refresh or self.data_pools is None:
            self.get_pool_names(refresh = True)
        return list(self.data_pools.values())

    def get_metadata_pool_name(self):
        return self.metadata_pool_name

    def set_data_pool_name(self, name):
        if self.id is not None:
            raise RuntimeError("can't set filesystem name if its fscid is set")
        self.data_pool_name = name

    def get_namespace_id(self):
        return self.id

    def get_pool_df(self, pool_name):
        """
        Return a dict like:
        {u'bytes_used': 0, u'max_avail': 83848701, u'objects': 0, u'kb_used': 0}
        """
        for pool_df in self._df()['pools']:
            if pool_df['name'] == pool_name:
                return pool_df['stats']

        raise RuntimeError("Pool name '{0}' not found".format(pool_name))

    def get_usage(self):
        return self._df()['stats']['total_used_bytes']

    def are_daemons_healthy(self, status=None, skip_max_mds_check=False):
        """
        Return true if all daemons are in one of active, standby, standby-replay, and
        at least max_mds daemons are in 'active'.

        Unlike most of Filesystem, this function is tolerant of new-style `fs`
        commands being missing, because we are part of the ceph installation
        process during upgrade suites, so must fall back to old style commands
        when we get an EINVAL on a new style command.

        :return:
        """
        # First, check to see that processes haven't exited with an error code
        for mds in self._ctx.daemons.iter_daemons_of_role('mds'):
            mds.check_status()

        active_count = 0
        try:
            mds_map = self.get_mds_map(status=status)
        except CommandFailedError as cfe:
            # Old version, fall back to non-multi-fs commands
            if cfe.exitstatus == errno.EINVAL:
                mds_map = json.loads(
                        self.mon_manager.raw_cluster_cmd('mds', 'dump', '--format=json'))
            else:
                raise

        log.info("are_daemons_healthy: mds map: {0}".format(mds_map))

        for mds_id, mds_status in mds_map['info'].items():
            if mds_status['state'] not in ["up:active", "up:standby", "up:standby-replay"]:
                log.warning("Unhealthy mds state {0}:{1}".format(mds_id, mds_status['state']))
                return False
            elif mds_status['state'] == 'up:active':
                active_count += 1

        log.info("are_daemons_healthy: {0}/{1}".format(
            active_count, mds_map['max_mds']
        ))

        if not skip_max_mds_check:
            if active_count > mds_map['max_mds']:
                log.info("are_daemons_healthy: number of actives is greater than max_mds: {0}".format(mds_map))
                return False
            elif active_count == mds_map['max_mds']:
                # The MDSMap says these guys are active, but let's check they really are
                for mds_id, mds_status in mds_map['info'].items():
                    if mds_status['state'] == 'up:active':
                        try:
                            daemon_status = self.mds_asok(["status"], mds_id=mds_status['name'])
                        except CommandFailedError as cfe:
                            if cfe.exitstatus == errno.EINVAL:
                                # Old version, can't do this check
                                continue
                            else:
                                # MDS not even running
                                return False

                        if daemon_status['state'] != 'up:active':
                            # MDS hasn't taken the latest map yet
                            return False

                return True
            else:
                return False
        else:
            log.info("are_daemons_healthy: skipping max_mds check")
            return True

    def get_daemon_names(self, state=None, status=None):
        """
        Return MDS daemon names of those daemons in the given state
        :param state:
        :return:
        """
        mdsmap = self.get_mds_map(status)
        result = []
        for mds_status in sorted(mdsmap['info'].values(),
                                 key=lambda _: _['rank']):
            if mds_status['state'] == state or state is None:
                result.append(mds_status['name'])

        return result

    def get_active_names(self, status=None):
        """
        Return MDS daemon names of those daemons holding ranks
        in state up:active

        :return: list of strings like ['a', 'b'], sorted by rank
        """
        return self.get_daemon_names("up:active", status=status)

    def get_all_mds_rank(self, status=None):
        mdsmap = self.get_mds_map(status)
        result = []
        for mds_status in sorted(mdsmap['info'].values(),
                                 key=lambda _: _['rank']):
            if mds_status['rank'] != -1 and mds_status['state'] != 'up:standby-replay':
                result.append(mds_status['rank'])

        return result

    def get_rank(self, rank=None, status=None):
        if status is None:
            status = self.getinfo()
        if rank is None:
            rank = 0
        return status.get_rank(self.id, rank)

    def rank_restart(self, rank=0, status=None):
        name = self.get_rank(rank=rank, status=status)['name']
        self.mds_restart(mds_id=name)

    def rank_signal(self, signal, rank=0, status=None):
        name = self.get_rank(rank=rank, status=status)['name']
        self.mds_signal(name, signal)

    def rank_freeze(self, yes, rank=0):
        self.mon_manager.raw_cluster_cmd("mds", "freeze", "{}:{}".format(self.id, rank), str(yes).lower())

    def rank_fail(self, rank=0):
        self.mon_manager.raw_cluster_cmd("mds", "fail", "{}:{}".format(self.id, rank))

    def get_ranks(self, status=None):
        if status is None:
            status = self.getinfo()
        return status.get_ranks(self.id)

    def get_replays(self, status=None):
        if status is None:
            status = self.getinfo()
        return status.get_replays(self.id)

    def get_replay(self, rank=0, status=None):
        for replay in self.get_replays(status=status):
            if replay['rank'] == rank:
                return replay
        return None

    def get_rank_names(self, status=None):
        """
        Return MDS daemon names of those daemons holding a rank,
        sorted by rank.  This includes e.g. up:replay/reconnect
        as well as active, but does not include standby or
        standby-replay.
        """
        mdsmap = self.get_mds_map(status)
        result = []
        for mds_status in sorted(mdsmap['info'].values(),
                                 key=lambda _: _['rank']):
            if mds_status['rank'] != -1 and mds_status['state'] != 'up:standby-replay':
                result.append(mds_status['name'])

        return result

    def wait_for_daemons(self, timeout=None, skip_max_mds_check=False, status=None):
        """
        Wait until all daemons are healthy
        :return:
        """

        if timeout is None:
            timeout = DAEMON_WAIT_TIMEOUT

        if status is None:
            status = self.status()

        elapsed = 0
        while True:
            if self.are_daemons_healthy(status=status, skip_max_mds_check=skip_max_mds_check):
                return status
            else:
                time.sleep(1)
                elapsed += 1

            if elapsed > timeout:
                log.info("status = {0}".format(status))
                raise RuntimeError("Timed out waiting for MDS daemons to become healthy")

            status = self.status()

    def get_lone_mds_id(self):
        """
        Get a single MDS ID: the only one if there is only one
        configured, else the only one currently holding a rank,
        else raise an error.
        """
        if len(self.mds_ids) != 1:
            alive = self.get_rank_names()
            if len(alive) == 1:
                return alive[0]
            else:
                raise ValueError("Explicit MDS argument required when multiple MDSs in use")
        else:
            return self.mds_ids[0]

    def recreate(self):
        log.info("Creating new filesystem")
        self.delete_all_filesystems()
        self.id = None
        self.create()

    def put_metadata_object_raw(self, object_id, infile):
        """
        Save an object to the metadata pool
        """
        temp_bin_path = infile
        self.client_remote.run(args=[
            'sudo', os.path.join(self._prefix, 'rados'), '-p', self.metadata_pool_name, 'put', object_id, temp_bin_path
        ])

    def get_metadata_object_raw(self, object_id):
        """
        Retrieve an object from the metadata pool and store it in a file.
        """
        temp_bin_path = '/tmp/' + object_id + '.bin'

        self.client_remote.run(args=[
            'sudo', os.path.join(self._prefix, 'rados'), '-p', self.metadata_pool_name, 'get', object_id, temp_bin_path
        ])

        return temp_bin_path

    def get_metadata_object(self, object_type, object_id):
        """
        Retrieve an object from the metadata pool, pass it through
        ceph-dencoder to dump it to JSON, and return the decoded object.
        """
        temp_bin_path = '/tmp/out.bin'

        self.client_remote.run(args=[
            'sudo', os.path.join(self._prefix, 'rados'), '-p', self.metadata_pool_name, 'get', object_id, temp_bin_path
        ])

        dump_json = self.client_remote.sh([
            'sudo', os.path.join(self._prefix, 'ceph-dencoder'), 'type', object_type, 'import', temp_bin_path, 'decode', 'dump_json'
        ]).strip()
        try:
            dump = json.loads(dump_json)
        except (TypeError, ValueError):
            log.error("Failed to decode JSON: '{0}'".format(dump_json))
            raise

        return dump

    def get_journal_version(self):
        """
        Read the JournalPointer and Journal::Header objects to learn the version of
        encoding in use.
        """
        journal_pointer_object = '400.00000000'
        journal_pointer_dump = self.get_metadata_object("JournalPointer", journal_pointer_object)
        journal_ino = journal_pointer_dump['journal_pointer']['front']

        journal_header_object = "{0:x}.00000000".format(journal_ino)
        journal_header_dump = self.get_metadata_object('Journaler::Header', journal_header_object)

        version = journal_header_dump['journal_header']['stream_format']
        log.info("Read journal version {0}".format(version))

        return version

    def mds_asok(self, command, mds_id=None, timeout=None):
        if mds_id is None:
            mds_id = self.get_lone_mds_id()

        return self.json_asok(command, 'mds', mds_id, timeout=timeout)

    def rank_asok(self, command, rank=0, status=None, timeout=None):
        info = self.get_rank(rank=rank, status=status)
        return self.json_asok(command, 'mds', info['name'], timeout=timeout)

    def rank_tell(self, command, rank=0, status=None):
        info = self.get_rank(rank=rank, status=status)
        return json.loads(self.mon_manager.raw_cluster_cmd("tell", 'mds.{0}'.format(info['name']), *command))

    def ranks_tell(self, command, status=None):
        if status is None:
            status = self.status()
        out = []
        for r in status.get_ranks(self.id):
            result = self.rank_tell(command, rank=r['rank'], status=status)
            out.append((r['rank'], result))
        return sorted(out)

    def ranks_perf(self, f, status=None):
        perf = self.ranks_tell(["perf", "dump"], status=status)
        out = []
        for rank, perf in perf:
            out.append((rank, f(perf)))
        return out

    def read_cache(self, path, depth=None):
        cmd = ["dump", "tree", path]
        if depth is not None:
            cmd.append(depth.__str__())
        result = self.mds_asok(cmd)
        if len(result) == 0:
            raise RuntimeError("Path not found in cache: {0}".format(path))

        return result

    def wait_for_state(self, goal_state, reject=None, timeout=None, mds_id=None, rank=None):
        """
        Block until the MDS reaches a particular state, or a failure condition
        is met.

        When there are multiple MDSs, succeed when exaclty one MDS is in the
        goal state, or fail when any MDS is in the reject state.

        :param goal_state: Return once the MDS is in this state
        :param reject: Fail if the MDS enters this state before the goal state
        :param timeout: Fail if this many seconds pass before reaching goal
        :return: number of seconds waited, rounded down to integer
        """

        started_at = time.time()
        while True:
            status = self.status()
            if rank is not None:
                try:
                    mds_info = status.get_rank(self.id, rank)
                    current_state = mds_info['state'] if mds_info else None
                    log.info("Looked up MDS state for mds.{0}: {1}".format(rank, current_state))
                except:
                    mdsmap = self.get_mds_map(status=status)
                    if rank in mdsmap['failed']:
                        log.info("Waiting for rank {0} to come back.".format(rank))
                        current_state = None
                    else:
                        raise
            elif mds_id is not None:
                # mds_info is None if no daemon with this ID exists in the map
                mds_info = status.get_mds(mds_id)
                current_state = mds_info['state'] if mds_info else None
                log.info("Looked up MDS state for {0}: {1}".format(mds_id, current_state))
            else:
                # In general, look for a single MDS
                states = [m['state'] for m in status.get_ranks(self.id)]
                if [s for s in states if s == goal_state] == [goal_state]:
                    current_state = goal_state
                elif reject in states:
                    current_state = reject
                else:
                    current_state = None
                log.info("mapped states {0} to {1}".format(states, current_state))

            elapsed = time.time() - started_at
            if current_state == goal_state:
                log.info("reached state '{0}' in {1}s".format(current_state, elapsed))
                return elapsed
            elif reject is not None and current_state == reject:
                raise RuntimeError("MDS in reject state {0}".format(current_state))
            elif timeout is not None and elapsed > timeout:
                log.error("MDS status at timeout: {0}".format(status.get_fsmap(self.id)))
                raise RuntimeError(
                    "Reached timeout after {0} seconds waiting for state {1}, while in state {2}".format(
                        elapsed, goal_state, current_state
                    ))
            else:
                time.sleep(1)

    def _read_data_xattr(self, ino_no, xattr_name, type, pool):
        mds_id = self.mds_ids[0]
        remote = self.mds_daemons[mds_id].remote
        if pool is None:
            pool = self.get_data_pool_name()

        obj_name = "{0:x}.00000000".format(ino_no)

        args = [
            os.path.join(self._prefix, "rados"), "-p", pool, "getxattr", obj_name, xattr_name
        ]
        try:
            proc = remote.run(args=args, stdout=BytesIO())
        except CommandFailedError as e:
            log.error(e.__str__())
            raise ObjectNotFound(obj_name)

        data = proc.stdout.getvalue()
        dump = remote.sh(
            [os.path.join(self._prefix, "ceph-dencoder"),
                                            "type", type,
                                            "import", "-",
                                            "decode", "dump_json"],
            stdin=data,
            stdout=StringIO()
        )

        return json.loads(dump.strip())

    def _write_data_xattr(self, ino_no, xattr_name, data, pool=None):
        """
        Write to an xattr of the 0th data object of an inode.  Will
        succeed whether the object and/or xattr already exist or not.

        :param ino_no: integer inode number
        :param xattr_name: string name of the xattr
        :param data: byte array data to write to the xattr
        :param pool: name of data pool or None to use primary data pool
        :return: None
        """
        remote = self.mds_daemons[self.mds_ids[0]].remote
        if pool is None:
            pool = self.get_data_pool_name()

        obj_name = "{0:x}.00000000".format(ino_no)
        args = [
            os.path.join(self._prefix, "rados"), "-p", pool, "setxattr",
            obj_name, xattr_name, data
        ]
        remote.sh(args)

    def read_backtrace(self, ino_no, pool=None):
        """
        Read the backtrace from the data pool, return a dict in the format
        given by inode_backtrace_t::dump, which is something like:

        ::

            rados -p cephfs_data getxattr 10000000002.00000000 parent > out.bin
            ceph-dencoder type inode_backtrace_t import out.bin decode dump_json

            { "ino": 1099511627778,
              "ancestors": [
                    { "dirino": 1,
                      "dname": "blah",
                      "version": 11}],
              "pool": 1,
              "old_pools": []}

        :param pool: name of pool to read backtrace from.  If omitted, FS must have only
                     one data pool and that will be used.
        """
        return self._read_data_xattr(ino_no, "parent", "inode_backtrace_t", pool)

    def read_layout(self, ino_no, pool=None):
        """
        Read 'layout' xattr of an inode and parse the result, returning a dict like:
        ::
            {
                "stripe_unit": 4194304,
                "stripe_count": 1,
                "object_size": 4194304,
                "pool_id": 1,
                "pool_ns": "",
            }

        :param pool: name of pool to read backtrace from.  If omitted, FS must have only
                     one data pool and that will be used.
        """
        return self._read_data_xattr(ino_no, "layout", "file_layout_t", pool)

    def _enumerate_data_objects(self, ino, size):
        """
        Get the list of expected data objects for a range, and the list of objects
        that really exist.

        :return a tuple of two lists of strings (expected, actual)
        """
        stripe_size = 1024 * 1024 * 4

        size = max(stripe_size, size)

        want_objects = [
            "{0:x}.{1:08x}".format(ino, n)
            for n in range(0, ((size - 1) // stripe_size) + 1)
        ]

        exist_objects = self.rados(["ls"], pool=self.get_data_pool_name()).split("\n")

        return want_objects, exist_objects

    def data_objects_present(self, ino, size):
        """
        Check that *all* the expected data objects for an inode are present in the data pool
        """

        want_objects, exist_objects = self._enumerate_data_objects(ino, size)
        missing = set(want_objects) - set(exist_objects)

        if missing:
            log.info("Objects missing (ino {0}, size {1}): {2}".format(
                ino, size, missing
            ))
            return False
        else:
            log.info("All objects for ino {0} size {1} found".format(ino, size))
            return True

    def data_objects_absent(self, ino, size):
        want_objects, exist_objects = self._enumerate_data_objects(ino, size)
        present = set(want_objects) & set(exist_objects)

        if present:
            log.info("Objects not absent (ino {0}, size {1}): {2}".format(
                ino, size, present
            ))
            return False
        else:
            log.info("All objects for ino {0} size {1} are absent".format(ino, size))
            return True

    def dirfrag_exists(self, ino, frag):
        try:
            self.rados(["stat", "{0:x}.{1:08x}".format(ino, frag)])
        except CommandFailedError:
            return False
        else:
            return True

    def rados(self, args, pool=None, namespace=None, stdin_data=None,
              stdin_file=None,
              stdout_data=None):
        """
        Call into the `rados` CLI from an MDS
        """

        if pool is None:
            pool = self.get_metadata_pool_name()

        # Doesn't matter which MDS we use to run rados commands, they all
        # have access to the pools
        mds_id = self.mds_ids[0]
        remote = self.mds_daemons[mds_id].remote

        # NB we could alternatively use librados pybindings for this, but it's a one-liner
        # using the `rados` CLI
        args = ([os.path.join(self._prefix, "rados"), "-p", pool] +
                (["--namespace", namespace] if namespace else []) +
                args)

        if stdin_file is not None:
            args = ["bash", "-c", "cat " + stdin_file + " | " + " ".join(args)]
        if stdout_data is None:
            stdout_data = StringIO()

        p = remote.run(args=args,
                       stdin=stdin_data,
                       stdout=stdout_data)
        return p.stdout.getvalue().strip()

    def list_dirfrag(self, dir_ino):
        """
        Read the named object and return the list of omap keys

        :return a list of 0 or more strings
        """

        dirfrag_obj_name = "{0:x}.00000000".format(dir_ino)

        try:
            key_list_str = self.rados(["listomapkeys", dirfrag_obj_name])
        except CommandFailedError as e:
            log.error(e.__str__())
            raise ObjectNotFound(dirfrag_obj_name)

        return key_list_str.split("\n") if key_list_str else []

    def get_meta_of_fs_file(self, dir_ino, obj_name, out):
        """
        get metadata from parent to verify the correctness of the data format encoded by the tool, cephfs-meta-injection.
        warning : The splitting of directory is not considered here.
        """
        dirfrag_obj_name = "{0:x}.00000000".format(dir_ino)
        try:
            self.rados(["getomapval", dirfrag_obj_name, obj_name+"_head", out])
        except CommandFailedError as e:
            log.error(e.__str__())
            raise ObjectNotFound(dir_ino)

    def erase_metadata_objects(self, prefix):
        """
        For all objects in the metadata pool matching the prefix,
        erase them.

        This O(N) with the number of objects in the pool, so only suitable
        for use on toy test filesystems.
        """
        all_objects = self.rados(["ls"]).split("\n")
        matching_objects = [o for o in all_objects if o.startswith(prefix)]
        for o in matching_objects:
            self.rados(["rm", o])

    def erase_mds_objects(self, rank):
        """
        Erase all the per-MDS objects for a particular rank.  This includes
        inotable, sessiontable, journal
        """

        def obj_prefix(multiplier):
            """
            MDS object naming conventions like rank 1's
            journal is at 201.***
            """
            return "%x." % (multiplier * 0x100 + rank)

        # MDS_INO_LOG_OFFSET
        self.erase_metadata_objects(obj_prefix(2))
        # MDS_INO_LOG_BACKUP_OFFSET
        self.erase_metadata_objects(obj_prefix(3))
        # MDS_INO_LOG_POINTER_OFFSET
        self.erase_metadata_objects(obj_prefix(4))
        # MDSTables & SessionMap
        self.erase_metadata_objects("mds{rank:d}_".format(rank=rank))

    @property
    def _prefix(self):
        """
        Override this to set a different
        """
        return ""

    def _make_rank(self, rank):
        return "{}:{}".format(self.name, rank)

    def _run_tool(self, tool, args, rank=None, quiet=False):
        # Tests frequently have [client] configuration that jacks up
        # the objecter log level (unlikely to be interesting here)
        # and does not set the mds log level (very interesting here)
        if quiet:
            base_args = [os.path.join(self._prefix, tool), '--debug-mds=1', '--debug-objecter=1']
        else:
            base_args = [os.path.join(self._prefix, tool), '--debug-mds=4', '--debug-objecter=1']

        if rank is not None:
            base_args.extend(["--rank", "%s" % str(rank)])

        t1 = datetime.datetime.now()
        r = self.tool_remote.sh(script=base_args + args, stdout=StringIO()).strip()
        duration = datetime.datetime.now() - t1
        log.info("Ran {0} in time {1}, result:\n{2}".format(
            base_args + args, duration, r
        ))
        return r

    @property
    def tool_remote(self):
        """
        An arbitrary remote to use when invoking recovery tools.  Use an MDS host because
        it'll definitely have keys with perms to access cephfs metadata pool.  This is public
        so that tests can use this remote to go get locally written output files from the tools.
        """
        mds_id = self.mds_ids[0]
        return self.mds_daemons[mds_id].remote

    def journal_tool(self, args, rank, quiet=False):
        """
        Invoke cephfs-journal-tool with the passed arguments for a rank, and return its stdout
        """
        fs_rank = self._make_rank(rank)
        return self._run_tool("cephfs-journal-tool", args, fs_rank, quiet)

    def meta_tool(self, args, rank, quiet=False):
        """
        Invoke cephfs-meta-injection with the passed arguments for a rank, and return its stdout
        """
        fs_rank = self._make_rank(rank)
        return self._run_tool("cephfs-meta-injection", args, fs_rank, quiet)

    def table_tool(self, args, quiet=False):
        """
        Invoke cephfs-table-tool with the passed arguments, and return its stdout
        """
        return self._run_tool("cephfs-table-tool", args, None, quiet)

    def data_scan(self, args, quiet=False, worker_count=1):
        """
        Invoke cephfs-data-scan with the passed arguments, and return its stdout

        :param worker_count: if greater than 1, multiple workers will be run
                             in parallel and the return value will be None
        """

        workers = []

        for n in range(0, worker_count):
            if worker_count > 1:
                # data-scan args first token is a command, followed by args to it.
                # insert worker arguments after the command.
                cmd = args[0]
                worker_args = [cmd] + ["--worker_n", n.__str__(), "--worker_m", worker_count.__str__()] + args[1:]
            else:
                worker_args = args

            workers.append(Greenlet.spawn(lambda wargs=worker_args:
                                          self._run_tool("cephfs-data-scan", wargs, None, quiet)))

        for w in workers:
            w.get()

        if worker_count == 1:
            return workers[0].value
        else:
            return None

    def is_full(self):
        return self.is_pool_full(self.get_data_pool_name())
