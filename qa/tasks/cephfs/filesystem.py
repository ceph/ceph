
import json
import logging
from gevent import Greenlet
import os
import time
import datetime
import re
import errno
import random

from io import BytesIO, StringIO
from errno import EBUSY

from teuthology.exceptions import CommandFailedError
from teuthology import misc
from teuthology.nuke import clear_firewall
from teuthology.parallel import parallel
from teuthology import contextutil
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

class FSMissing(Exception):
    def __init__(self, ident):
        self.ident = ident

    def __str__(self):
        return f"File system {self.ident} does not exist in the map"

class FSStatus(object):
    """
    Operations on a snapshot of the FSMap.
    """
    def __init__(self, mon_manager, epoch=None):
        self.mon = mon_manager
        cmd = ["fs", "dump", "--format=json"]
        if epoch is not None:
            cmd.append(str(epoch))
        self.map = json.loads(self.mon.raw_cluster_cmd(*cmd))

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
        raise FSMissing(fscid)

    def get_fsmap_byname(self, name):
        """
        Get the fsmap for the given file system name.
        """
        for fs in self.map['filesystems']:
            if name is None or fs['mdsmap']['fs_name'] == name:
                return fs
        raise FSMissing(name)

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

    def get_damaged(self, fscid):
        """
        Get the damaged ranks for the given FSCID.
        """
        fs = self.get_fsmap(fscid)
        return fs['mdsmap']['damaged']

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

    def get_mds_addrs(self, name):
        """
        Return the instance addr as a string, like "[10.214.133.138:6807 10.214.133.138:6808]"
        """
        info = self.get_mds(name)
        if info:
            return [e['addr'] for e in info['addrs']['addrvec']]
        else:
            log.warn(json.dumps(list(self.get_all()), indent=2))  # dump for debugging
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

    def __init__(self, ctx) -> None:
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
            timeout = 300
        command.insert(0, '--format=json')
        proc = self.mon_manager.admin_socket(service_type, service_id, command, timeout=timeout)
        response_data = proc.stdout.getvalue().strip()
        if len(response_data) > 0:

            def get_nonnumeric_values(value):
                c = {"NaN": float("nan"), "Infinity": float("inf"),
                     "-Infinity": -float("inf")}
                return c[value]

            j = json.loads(response_data.replace('inf', 'Infinity'),
                           parse_constant=get_nonnumeric_values)
            pretty = json.dumps(j, sort_keys=True, indent=2)
            log.debug(f"_json_asok output\n{pretty}")
            return j
        else:
            log.debug("_json_asok output empty")
            return None

    def is_addr_blocklisted(self, addr):
        blocklist = json.loads(self.mon_manager.raw_cluster_cmd(
            "osd", "dump", "--format=json"))['blocklist']
        if addr in blocklist:
            return True
        log.warn(f'The address {addr} is not blocklisted')
        return False


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

    @property
    def mds_ids(self):
        # do this dynamically because the list of ids may change periodically with cephadm
        return list(misc.all_roles_of_type(self._ctx.cluster, 'mds'))

    @property
    def mds_daemons(self):
        return dict([(mds_id, self._ctx.daemons.get_daemon('mds', mds_id)) for mds_id in self.mds_ids])

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

    def mds_is_running(self, mds_id):
        return self.mds_daemons[mds_id].running()

    def newfs(self, name='cephfs', create=True):
        return Filesystem(self._ctx, name=name, create=create)

    def status(self, epoch=None):
        return FSStatus(self.mon_manager, epoch)

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

    def set_inter_mds_block(self, blocked, mds_rank_1, mds_rank_2):
        """
        Block (using iptables) communications from a provided MDS to other MDSs.
        Block all ports that an MDS uses for communication.

        :param blocked: True to block the MDS, False otherwise
        :param mds_rank_1: MDS rank
        :param mds_rank_2: MDS rank
        :return:
        """
        da_flag = "-A" if blocked else "-D"

        def set_block(mds_ids):
            status = self.status()

            mds = mds_ids[0]
            remote = self.mon_manager.find_remote('mds', mds)
            addrs = status.get_mds_addrs(mds)
            for addr in addrs:
                ip_str, port_str = re.match("(.+):(.+)", addr).groups()
                remote.run(
                    args=["sudo", "iptables", da_flag, "INPUT", "-p", "tcp", "--dport", port_str, "-j", "REJECT", "-m",
                          "comment", "--comment", "teuthology"], omit_sudo=False)


            mds = mds_ids[1]
            remote = self.mon_manager.find_remote('mds', mds)
            addrs = status.get_mds_addrs(mds)
            for addr in addrs:
                ip_str, port_str = re.match("(.+):(.+)", addr).groups()
                remote.run(
                    args=["sudo", "iptables", da_flag, "OUTPUT", "-p", "tcp", "--sport", port_str, "-j", "REJECT", "-m",
                          "comment", "--comment", "teuthology"], omit_sudo=False)
                remote.run(
                    args=["sudo", "iptables", da_flag, "INPUT", "-p", "tcp", "--dport", port_str, "-j", "REJECT", "-m",
                          "comment", "--comment", "teuthology"], omit_sudo=False)

        self._one_or_all((mds_rank_1, mds_rank_2), set_block, in_parallel=False)

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

    def delete_all_filesystems(self):
        """
        Remove all filesystems that exist, and any pools in use by them.
        """
        for fs in self.status().get_filesystems():
            Filesystem(ctx=self._ctx, fscid=fs['id']).destroy()

    @property
    def beacon_timeout(self):
        """
        Generate an acceptable timeout for the mons to drive some MDSMap change
        because of missed beacons from some MDS. This involves looking up the
        grace period in use by the mons and adding an acceptable buffer.
        """

        grace = float(self.get_config("mds_beacon_grace", service_type="mon"))
        return grace*2+15


class Filesystem(MDSCluster):

    """
    Generator for all Filesystems in the cluster.
    """
    @classmethod
    def get_all_fs(cls, ctx):
        mdsc = MDSCluster(ctx)
        status = mdsc.status()
        for fs in status.get_filesystems():
            yield cls(ctx, fscid=fs['id'])

    """
    This object is for driving a CephFS filesystem.  The MDS daemons driven by
    MDSCluster may be shared with other Filesystems.
    """
    def __init__(self, ctx, fs_config={}, fscid=None, name=None, create=False):
        super(Filesystem, self).__init__(ctx)

        self.name = name
        self.id = None
        self.metadata_pool_name = None
        self.data_pool_name = None
        self.data_pools = None
        self.fs_config = fs_config
        self.ec_profile = fs_config.get('ec_profile')

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

    def dead(self):
        try:
            return not bool(self.get_mds_map())
        except FSMissing:
            return True

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

    def reach_max_mds(self):
        status = self.wait_for_daemons()
        mds_map = self.get_mds_map(status=status)
        assert(mds_map['in'] == list(range(0, mds_map['max_mds'])))

    def reset(self):
        self.mon_manager.raw_cluster_cmd("fs", "reset", str(self.name), '--yes-i-really-mean-it')

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

    def set_session_timeout(self, timeout):
        self.set_var("session_timeout", "%d" % timeout)

    def set_allow_standby_replay(self, yes):
        self.set_var("allow_standby_replay", yes)

    def set_allow_new_snaps(self, yes):
        self.set_var("allow_new_snaps", yes, '--yes-i-really-mean-it')

    def set_bal_rank_mask(self, bal_rank_mask):
        self.set_var("bal_rank_mask", bal_rank_mask)

    def set_refuse_client_session(self, yes):
        self.set_var("refuse_client_session", yes)

    def set_refuse_standby_for_another_fs(self, yes):
        self.set_var("refuse_standby_for_another_fs", yes)

    def compat(self, *args):
        a = map(lambda x: str(x).lower(), args)
        self.mon_manager.raw_cluster_cmd("fs", "compat", self.name, *a)

    def add_compat(self, *args):
        self.compat("add_compat", *args)

    def add_incompat(self, *args):
        self.compat("add_incompat", *args)

    def rm_compat(self, *args):
        self.compat("rm_compat", *args)

    def rm_incompat(self, *args):
        self.compat("rm_incompat", *args)

    def required_client_features(self, *args, **kwargs):
        c = ["fs", "required_client_features", self.name, *args]
        return self.mon_manager.run_cluster_cmd(args=c, **kwargs)

    # Since v15.1.0 the pg autoscale mode has been enabled as default,
    # will let the pg autoscale mode to calculate the pg_num as needed.
    # We set the pg_num_min to 64 to make sure that pg autoscale mode
    # won't set the pg_num to low to fix Tracker#45434.
    pg_num = 64
    pg_num_min = 64
    target_size_ratio = 0.9
    target_size_ratio_ec = 0.9

    def create(self, recover=False, metadata_overlay=False):
        if self.name is None:
            self.name = "cephfs"
        if self.metadata_pool_name is None:
            self.metadata_pool_name = "{0}_metadata".format(self.name)
        if self.data_pool_name is None:
            data_pool_name = "{0}_data".format(self.name)
        else:
            data_pool_name = self.data_pool_name

        # will use the ec pool to store the data and a small amount of
        # metadata still goes to the primary data pool for all files.
        if not metadata_overlay and self.ec_profile and 'disabled' not in self.ec_profile:
            self.target_size_ratio = 0.05

        log.debug("Creating filesystem '{0}'".format(self.name))

        try:
            self.mon_manager.raw_cluster_cmd('osd', 'pool', 'create',
                                             self.metadata_pool_name,
                                             '--pg_num_min', str(self.pg_num_min))

            self.mon_manager.raw_cluster_cmd('osd', 'pool', 'create',
                                             data_pool_name, str(self.pg_num),
                                             '--pg_num_min', str(self.pg_num_min),
                                             '--target_size_ratio',
                                             str(self.target_size_ratio))
        except CommandFailedError as e:
            if e.exitstatus == 22: # nautilus couldn't specify --pg_num_min option
                self.mon_manager.raw_cluster_cmd('osd', 'pool', 'create',
                                                 self.metadata_pool_name,
                                                 str(self.pg_num_min))

                self.mon_manager.raw_cluster_cmd('osd', 'pool', 'create',
                                                 data_pool_name, str(self.pg_num),
                                                 str(self.pg_num_min))
            else:
                raise

        args = ["fs", "new", self.name, self.metadata_pool_name, data_pool_name]
        if recover:
            args.append('--recover')
        if metadata_overlay:
            args.append('--allow-dangerous-metadata-overlay')
        self.mon_manager.raw_cluster_cmd(*args)

        if not recover:
            if self.ec_profile and 'disabled' not in self.ec_profile:
                ec_data_pool_name = data_pool_name + "_ec"
                log.debug("EC profile is %s", self.ec_profile)
                cmd = ['osd', 'erasure-code-profile', 'set', ec_data_pool_name]
                cmd.extend(self.ec_profile)
                self.mon_manager.raw_cluster_cmd(*cmd)
                try:
                    self.mon_manager.raw_cluster_cmd(
                        'osd', 'pool', 'create', ec_data_pool_name,
                        'erasure', ec_data_pool_name,
                        '--pg_num_min', str(self.pg_num_min),
                        '--target_size_ratio', str(self.target_size_ratio_ec))
                except CommandFailedError as e:
                    if e.exitstatus == 22: # nautilus couldn't specify --pg_num_min option
                        self.mon_manager.raw_cluster_cmd(
                            'osd', 'pool', 'create', ec_data_pool_name,
                            str(self.pg_num_min), 'erasure', ec_data_pool_name)
                    else:
                        raise
                self.mon_manager.raw_cluster_cmd(
                    'osd', 'pool', 'set',
                    ec_data_pool_name, 'allow_ec_overwrites', 'true')
                self.add_data_pool(ec_data_pool_name, create=False)
                self.check_pool_application(ec_data_pool_name)

                self.run_client_payload(f"setfattr -n ceph.dir.layout.pool -v {ec_data_pool_name} . && getfattr -n ceph.dir.layout .")

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

        if self.fs_config is not None:
            log.debug(f"fs_config: {self.fs_config}")
            max_mds = self.fs_config.get('max_mds', 1)
            if max_mds > 1:
                self.set_max_mds(max_mds)

            standby_replay = self.fs_config.get('standby_replay', False)
            self.set_allow_standby_replay(standby_replay)

            # If absent will use the default value (60 seconds)
            session_timeout = self.fs_config.get('session_timeout', 60)
            if session_timeout != 60:
                self.set_session_timeout(session_timeout)

            if self.fs_config.get('subvols', None) is not None:
                log.debug(f"Creating {self.fs_config.get('subvols')} subvols "
                          f"for filesystem '{self.name}'")
                if not hasattr(self._ctx, "created_subvols"):
                    self._ctx.created_subvols = dict()

                subvols = self.fs_config.get('subvols')
                assert(isinstance(subvols, dict))
                assert(isinstance(subvols['create'], int))
                assert(subvols['create'] > 0)

                self.run_ceph_cmd('fs', 'subvolumegroup', 'create', self.name, 'qa')
                subvol_options = self.fs_config.get('subvol_options', '')

                for sv in range(0, subvols['create']):
                    sv_name = f'sv_{sv}'
                    cmd = [
                      'fs',
                      'subvolume',
                      'create',
                      self.name,
                      sv_name,
                      '--group_name', 'qa',
                    ]
                    if subvol_options:
                        cmd.append(subvol_options)
                    self.mon_manager.raw_cluster_cmd(*cmd)

                    if self.name not in self._ctx.created_subvols:
                        self._ctx.created_subvols[self.name] = []
                    
                    subvol_path = self.mon_manager.raw_cluster_cmd(
                        'fs', 'subvolume', 'getpath', self.name,
                        '--group_name', 'qa',
                        sv_name)
                    subvol_path = subvol_path.strip()
                    self._ctx.created_subvols[self.name].append(subvol_path)
            else:
                log.debug(f"Not Creating any subvols for filesystem '{self.name}'")


        self.getinfo(refresh = True)

        # wait pgs to be clean
        self.mon_manager.wait_for_clean()

    def run_client_payload(self, cmd):
        # avoid circular dep by importing here:
        from tasks.cephfs.fuse_mount import FuseMount

        # Wait for at MDS daemons to be ready before mounting the
        # ceph-fuse client in run_client_payload()
        self.wait_for_daemons()

        d = misc.get_testdir(self._ctx)
        m = FuseMount(self._ctx, d, "admin", self.client_remote, cephfs_name=self.name)
        m.mount_wait()
        m.run_shell_payload(cmd)
        m.umount_wait(require_clean=True)

    def _remove_pool(self, name, **kwargs):
        c = f'osd pool rm {name} {name} --yes-i-really-really-mean-it'
        return self.mon_manager.ceph(c, **kwargs)

    def rm(self, **kwargs):
        c = f'fs rm {self.name} --yes-i-really-mean-it'
        return self.mon_manager.ceph(c, **kwargs)

    def remove_pools(self, data_pools):
        self._remove_pool(self.get_metadata_pool_name())
        for poolname in data_pools:
            try:
                self._remove_pool(poolname)
            except CommandFailedError as e:
                # EBUSY, this data pool is used by two metadata pools, let the
                # 2nd pass delete it
                if e.exitstatus == EBUSY:
                    pass
                else:
                    raise

    def destroy(self, reset_obj_attrs=True):
        log.info(f'Destroying file system {self.name} and related pools')

        if self.dead():
            log.debug('already dead...')
            return

        data_pools = self.get_data_pool_names(refresh=True)

        # make sure no MDSs are attached to given FS.
        self.fail()
        self.rm()

        self.remove_pools(data_pools)

        if reset_obj_attrs:
            self.id = None
            self.name = None
            self.metadata_pool_name = None
            self.data_pool_name = None
            self.data_pools = None

    def recreate(self):
        self.destroy()

        self.create()
        self.getinfo(refresh=True)

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

    # may raise FSMissing
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
            try:
                self.mon_manager.raw_cluster_cmd('osd', 'pool', 'create', name,
                                                 '--pg_num_min', str(self.pg_num_min))
            except CommandFailedError as e:
                if e.exitstatus == 22: # nautilus couldn't specify --pg_num_min option
                  self.mon_manager.raw_cluster_cmd('osd', 'pool', 'create', name,
                                                   str(self.pg_num_min))
                else:
                    raise
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

    def get_pool_pg_num(self, pool_name):
        pgs = json.loads(self.mon_manager.raw_cluster_cmd('osd', 'pool', 'get',
                                                          pool_name, 'pg_num',
                                                          '--format=json-pretty'))
        return int(pgs['pg_num'])

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
        mds_map = self.get_mds_map(status=status)

        log.debug("are_daemons_healthy: mds map: {0}".format(mds_map))

        for mds_id, mds_status in mds_map['info'].items():
            if mds_status['state'] not in ["up:active", "up:standby", "up:standby-replay"]:
                log.warning("Unhealthy mds state {0}:{1}".format(mds_id, mds_status['state']))
                return False
            elif mds_status['state'] == 'up:active':
                active_count += 1

        log.debug("are_daemons_healthy: {0}/{1}".format(
            active_count, mds_map['max_mds']
        ))

        if not skip_max_mds_check:
            if active_count > mds_map['max_mds']:
                log.debug("are_daemons_healthy: number of actives is greater than max_mds: {0}".format(mds_map))
                return False
            elif active_count == mds_map['max_mds']:
                # The MDSMap says these guys are active, but let's check they really are
                for mds_id, mds_status in mds_map['info'].items():
                    if mds_status['state'] == 'up:active':
                        try:
                            daemon_status = self.mds_tell(["status"], mds_id=mds_status['name'])
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
            log.debug("are_daemons_healthy: skipping max_mds check")
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

    def rank_repaired(self, rank):
        self.mon_manager.raw_cluster_cmd("mds", "repaired", "{}:{}".format(self.id, rank))

    def rank_fail(self, rank=0):
        self.mon_manager.raw_cluster_cmd("mds", "fail", "{}:{}".format(self.id, rank))

    def rank_is_running(self, rank=0, status=None):
        name = self.get_rank(rank=rank, status=status)['name']
        return self.mds_is_running(name)

    def get_ranks(self, status=None):
        if status is None:
            status = self.getinfo()
        return status.get_ranks(self.id)

    def get_damaged(self, status=None):
        if status is None:
            status = self.getinfo()
        return status.get_damaged(self.id)

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

        if self.id is None:
            status = self.getinfo(refresh=True)

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
                log.debug("status = {0}".format(status))
                raise RuntimeError("Timed out waiting for MDS daemons to become healthy")

            status = self.status()

    def dencoder(self, obj_type, obj_blob):
        args = [os.path.join(self._prefix, "ceph-dencoder"), 'type', obj_type, 'import', '-', 'decode', 'dump_json']
        p = self.mon_manager.controller.run(args=args, stdin=BytesIO(obj_blob), stdout=BytesIO())
        return p.stdout.getvalue()

    def rados(self, *args, **kwargs):
        """
        Callout to rados CLI.
        """

        return self.mon_manager.do_rados(*args, **kwargs)

    def radosm(self, *args, **kwargs):
        """
        Interact with the metadata pool via rados CLI.
        """

        return self.rados(*args, **kwargs, pool=self.get_metadata_pool_name())

    def radosmo(self, *args, stdout=BytesIO(), **kwargs):
        """
        Interact with the metadata pool via rados CLI. Get the stdout.
        """

        return self.radosm(*args, **kwargs, stdout=stdout).stdout.getvalue()

    def get_metadata_object(self, object_type, object_id):
        """
        Retrieve an object from the metadata pool, pass it through
        ceph-dencoder to dump it to JSON, and return the decoded object.
        """

        o = self.radosmo(['get', object_id, '-'])
        j = self.dencoder(object_type, o)
        try:
            return json.loads(j)
        except (TypeError, ValueError):
            log.error("Failed to decode JSON: '{0}'".format(j))
            raise

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
        log.debug("Read journal version {0}".format(version))

        return version

    def mds_asok(self, command, mds_id=None, timeout=None):
        if mds_id is None:
            return self.rank_asok(command, timeout=timeout)

        return self.json_asok(command, 'mds', mds_id, timeout=timeout)

    def mds_tell(self, command, mds_id=None):
        if mds_id is None:
            return self.rank_tell(command)

        return json.loads(self.mon_manager.raw_cluster_cmd("tell", f"mds.{mds_id}", *command))

    def rank_asok(self, command, rank=0, status=None, timeout=None):
        info = self.get_rank(rank=rank, status=status)
        return self.json_asok(command, 'mds', info['name'], timeout=timeout)

    def rank_tell(self, command, rank=0, status=None):
        try:
            out = self.mon_manager.raw_cluster_cmd("tell", f"mds.{self.id}:{rank}", *command)
            return json.loads(out)
        except json.decoder.JSONDecodeError:
            log.error("could not decode: {}".format(out))
            raise

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

    def read_cache(self, path, depth=None, rank=None):
        cmd = ["dump", "tree", path]
        if depth is not None:
            cmd.append(depth.__str__())
        result = self.rank_asok(cmd, rank=rank)
        if result is None or len(result) == 0:
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
                    log.debug("Looked up MDS state for mds.{0}: {1}".format(rank, current_state))
                except:
                    mdsmap = self.get_mds_map(status=status)
                    if rank in mdsmap['failed']:
                        log.debug("Waiting for rank {0} to come back.".format(rank))
                        current_state = None
                    else:
                        raise
            elif mds_id is not None:
                # mds_info is None if no daemon with this ID exists in the map
                mds_info = status.get_mds(mds_id)
                current_state = mds_info['state'] if mds_info else None
                log.debug("Looked up MDS state for {0}: {1}".format(mds_id, current_state))
            else:
                # In general, look for a single MDS
                states = [m['state'] for m in status.get_ranks(self.id)]
                if [s for s in states if s == goal_state] == [goal_state]:
                    current_state = goal_state
                elif reject in states:
                    current_state = reject
                else:
                    current_state = None
                log.debug("mapped states {0} to {1}".format(states, current_state))

            elapsed = time.time() - started_at
            if current_state == goal_state:
                log.debug("reached state '{0}' in {1}s".format(current_state, elapsed))
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

    def _read_data_xattr(self, ino_no, xattr_name, obj_type, pool):
        if pool is None:
            pool = self.get_data_pool_name()

        obj_name = "{0:x}.00000000".format(ino_no)

        args = ["getxattr", obj_name, xattr_name]
        try:
            proc = self.rados(args, pool=pool, stdout=BytesIO())
        except CommandFailedError as e:
            log.error(e.__str__())
            raise ObjectNotFound(obj_name)

        obj_blob = proc.stdout.getvalue()
        return json.loads(self.dencoder(obj_type, obj_blob).strip())

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
        if pool is None:
            pool = self.get_data_pool_name()

        obj_name = "{0:x}.00000000".format(ino_no)
        args = ["setxattr", obj_name, xattr_name, data]
        self.rados(args, pool=pool)

    def read_symlink(self, ino_no, pool=None):
        return self._read_data_xattr(ino_no, "symlink", "string_wrapper", pool)

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

        exist_objects = self.rados(["ls"], pool=self.get_data_pool_name(), stdout=StringIO()).stdout.getvalue().split("\n")

        return want_objects, exist_objects

    def data_objects_present(self, ino, size):
        """
        Check that *all* the expected data objects for an inode are present in the data pool
        """

        want_objects, exist_objects = self._enumerate_data_objects(ino, size)
        missing = set(want_objects) - set(exist_objects)

        if missing:
            log.debug("Objects missing (ino {0}, size {1}): {2}".format(
                ino, size, missing
            ))
            return False
        else:
            log.debug("All objects for ino {0} size {1} found".format(ino, size))
            return True

    def data_objects_absent(self, ino, size):
        want_objects, exist_objects = self._enumerate_data_objects(ino, size)
        present = set(want_objects) & set(exist_objects)

        if present:
            log.debug("Objects not absent (ino {0}, size {1}): {2}".format(
                ino, size, present
            ))
            return False
        else:
            log.debug("All objects for ino {0} size {1} are absent".format(ino, size))
            return True

    def dirfrag_exists(self, ino, frag):
        try:
            self.radosm(["stat", "{0:x}.{1:08x}".format(ino, frag)])
        except CommandFailedError:
            return False
        else:
            return True

    def list_dirfrag(self, dir_ino):
        """
        Read the named object and return the list of omap keys

        :return a list of 0 or more strings
        """

        dirfrag_obj_name = "{0:x}.00000000".format(dir_ino)

        try:
            key_list_str = self.radosmo(["listomapkeys", dirfrag_obj_name], stdout=StringIO())
        except CommandFailedError as e:
            log.error(e.__str__())
            raise ObjectNotFound(dirfrag_obj_name)

        return key_list_str.strip().split("\n") if key_list_str else []

    def get_meta_of_fs_file(self, dir_ino, obj_name, out):
        """
        get metadata from parent to verify the correctness of the data format encoded by the tool, cephfs-meta-injection.
        warning : The splitting of directory is not considered here.
        """

        dirfrag_obj_name = "{0:x}.00000000".format(dir_ino)
        try:
            self.radosm(["getomapval", dirfrag_obj_name, obj_name+"_head", out])
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
        all_objects = self.radosmo(["ls"], stdout=StringIO()).strip().split("\n")
        matching_objects = [o for o in all_objects if o.startswith(prefix)]
        for o in matching_objects:
            self.radosm(["rm", o])

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
            base_args = [os.path.join(self._prefix, tool), '--debug-mds=20', '--debug-ms=1', '--debug-objecter=1']

        if rank is not None:
            base_args.extend(["--rank", "%s" % str(rank)])

        t1 = datetime.datetime.now()
        r = self.tool_remote.sh(script=base_args + args, stdout=StringIO()).strip()
        duration = datetime.datetime.now() - t1
        log.debug("Ran {0} in time {1}, result:\n{2}".format(
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
        return self.mon_manager.controller

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

    def authorize(self, client_id, caps=('/', 'rw')):
        """
        Run "ceph fs authorize" and run "ceph auth get" to get and returnt the
        keyring.

        client_id: client id that will be authorized
        caps: tuple containing the path and permission (can be r or rw)
              respectively.
        """
        if isinstance(caps[0], (tuple, list)):
            x = []
            for c in caps:
                x.extend(c)
            caps = tuple(x)

        client_name = 'client.' + client_id
        return self.mon_manager.raw_cluster_cmd('fs', 'authorize', self.name,
                                                client_name, *caps)

    def grow(self, new_max_mds, status=None):
        oldmax = self.get_var('max_mds', status=status)
        assert(new_max_mds > oldmax)
        self.set_max_mds(new_max_mds)
        return self.wait_for_daemons()

    def shrink(self, new_max_mds, status=None):
        oldmax = self.get_var('max_mds', status=status)
        assert(new_max_mds < oldmax)
        self.set_max_mds(new_max_mds)
        return self.wait_for_daemons()

    def run_scrub(self, cmd, rank=0):
        return self.rank_tell(["scrub"] + cmd, rank)

    def get_scrub_status(self, rank=0):
        return self.run_scrub(["status"], rank)

    def flush(self, rank=0):
        return self.rank_tell(["flush", "journal"], rank=rank)

    def wait_until_scrub_complete(self, result=None, tag=None, rank=0, sleep=30,
                                  timeout=300, reverse=False):
        # time out after "timeout" seconds and assume as done
        if result is None:
            result = "no active scrubs running"
        with contextutil.safe_while(sleep=sleep, tries=timeout//sleep) as proceed:
            while proceed():
                out_json = self.rank_tell(["scrub", "status"], rank=rank)
                assert out_json is not None
                if not reverse:
                    if result in out_json['status']:
                        log.info("all active scrubs completed")
                        return True
                else:
                    if result not in out_json['status']:
                        log.info("all active scrubs completed")
                        return True

                if tag is not None:
                    status = out_json['scrubs'][tag]
                    if status is not None:
                        log.info(f"scrub status for tag:{tag} - {status}")
                    else:
                        log.info(f"scrub has completed for tag:{tag}")
                        return True

        # timed out waiting for scrub to complete
        return False

    def get_damage(self, rank=None):
        if rank is None:
            result = {}
            for info in self.get_ranks():
                rank = info['rank']
                result[rank] = self.get_damage(rank=rank)
            return result
        else:
            return self.rank_tell(['damage', 'ls'], rank=rank)
