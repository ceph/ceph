
from StringIO import StringIO
import json
import logging
from gevent import Greenlet
import os
import time
import datetime
import re
import errno

from teuthology.exceptions import CommandFailedError
from teuthology import misc
from teuthology.nuke import clear_firewall
from teuthology.parallel import parallel
from tasks.ceph_manager import write_conf
from tasks import ceph_manager


log = logging.getLogger(__name__)


DAEMON_WAIT_TIMEOUT = 120
ROOT_INO = 1


class ObjectNotFound(Exception):
    def __init__(self, object_name):
        self._object_name = object_name

    def __str__(self):
        return "Object not found: '{0}'".format(self._object_name)


class MDSCluster(object):
    """
    Collective operations on all the MDS daemons in the Ceph cluster.  These
    daemons may be in use by various Filesystems.

    For the benefit of pre-multi-filesystem tests, this class is also
    a parent of Filesystem.  The correct way to use MDSCluster going forward is
    as a separate instance outside of your (multiple) Filesystem instances.
    """

    @property
    def admin_remote(self):
        first_mon = misc.get_first_mon(self._ctx, None)
        (result,) = self._ctx.cluster.only(first_mon).remotes.iterkeys()
        return result

    def __init__(self, ctx):
        self.mds_ids = list(misc.all_roles_of_type(ctx.cluster, 'mds'))
        self._ctx = ctx

        if len(self.mds_ids) == 0:
            raise RuntimeError("This task requires at least one MDS")

        self.mon_manager = ceph_manager.CephManager(self.admin_remote, ctx=ctx, logger=log.getChild('ceph_manager'))
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

    def get_filesystem(self, name):
        return Filesystem(self._ctx, name)

    def get_fs_map(self):
        fs_map = json.loads(self.mon_manager.raw_cluster_cmd("fs", "dump", "--format=json-pretty"))
        return fs_map

    def delete_all_filesystems(self):
        """
        Remove all filesystems that exist, and any pools in use by them.
        """
        fs_ls = json.loads(self.mon_manager.raw_cluster_cmd("fs", "ls", "--format=json-pretty"))
        for fs in fs_ls:
            self.mon_manager.raw_cluster_cmd("fs", "set", fs['name'], "cluster_down", "true")
            mds_map = json.loads(
                self.mon_manager.raw_cluster_cmd(
                    "fs", "get", fs['name'], "--format=json-pretty"))['mdsmap']

            for gid in mds_map['up'].values():
                self.mon_manager.raw_cluster_cmd('mds', 'fail', gid.__str__())

            self.mon_manager.raw_cluster_cmd('fs', 'rm', fs['name'], '--yes-i-really-mean-it')
            self.mon_manager.raw_cluster_cmd('osd', 'pool', 'delete',
                                             fs['metadata_pool'],
                                             fs['metadata_pool'],
                                             '--yes-i-really-really-mean-it')
            for data_pool in fs['data_pools']:
                self.mon_manager.raw_cluster_cmd('osd', 'pool', 'delete',
                                                 data_pool, data_pool,
                                                 '--yes-i-really-really-mean-it')

    def get_standby_daemons(self):
        return set([s['name'] for s in self.get_fs_map()['standbys']])

    def get_mds_hostnames(self):
        result = set()
        for mds_id in self.mds_ids:
            mds_remote = self.mon_manager.find_remote('mds', mds_id)
            result.add(mds_remote.hostname)

        return list(result)

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

    def json_asok(self, command, service_type, service_id):
        proc = self.mon_manager.admin_socket(service_type, service_id, command)
        response_data = proc.stdout.getvalue()
        log.info("_json_asok output: {0}".format(response_data))
        if response_data.strip():
            return json.loads(response_data)
        else:
            return None

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

            addr = self.get_mds_addr(_mds_id)
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

    def _all_info(self):
        """
        Iterator for all the mds_info components in the FSMap
        """
        fs_map = self.get_fs_map()
        for i in fs_map['standbys']:
            yield i
        for fs in fs_map['filesystems']:
            for i in fs['mdsmap']['info'].values():
                yield i

    def get_mds_addr(self, mds_id):
        """
        Return the instance addr as a string, like "10.214.133.138:6807\/10825"
        """
        for mds_info in self._all_info():
            if mds_info['name'] == mds_id:
                return mds_info['addr']

        log.warn(json.dumps(list(self._all_info()), indent=2))  # dump for debugging
        raise RuntimeError("MDS id '{0}' not found in map".format(mds_id))

    def get_mds_info(self, mds_id):
        for mds_info in self._all_info():
            if mds_info['name'] == mds_id:
                return mds_info

        return None

    def get_mds_info_by_rank(self, mds_rank):
        for mds_info in self._all_info():
            if mds_info['rank'] == mds_rank:
                return mds_info

        return None


class Filesystem(MDSCluster):
    """
    This object is for driving a CephFS filesystem.  The MDS daemons driven by
    MDSCluster may be shared with other Filesystems.
    """
    def __init__(self, ctx, name=None):
        super(Filesystem, self).__init__(ctx)

        if name is None:
            name = "cephfs"

        self.name = name
        self.metadata_pool_name = "{0}_metadata".format(name)
        self.data_pool_name = "{0}_data".format(name)

        client_list = list(misc.all_roles_of_type(self._ctx.cluster, 'client'))
        self.client_id = client_list[0]
        self.client_remote = list(misc.get_clients(ctx=ctx, roles=["client.{0}".format(self.client_id)]))[0][1]

    def get_pgs_per_fs_pool(self):
        """
        Calculate how many PGs to use when creating a pool, in order to avoid raising any
        health warnings about mon_pg_warn_min_per_osd

        :return: an integer number of PGs
        """
        pg_warn_min_per_osd = int(self.get_config('mon_pg_warn_min_per_osd'))
        osd_count = len(list(misc.all_roles_of_type(self._ctx.cluster, 'osd')))
        return pg_warn_min_per_osd * osd_count

    def create(self):
        log.info("Creating filesystem '{0}'".format(self.name))

        pgs_per_fs_pool = self.get_pgs_per_fs_pool()

        self.mon_manager.raw_cluster_cmd('osd', 'pool', 'create',
                                         self.metadata_pool_name, pgs_per_fs_pool.__str__())
        self.mon_manager.raw_cluster_cmd('osd', 'pool', 'create',
                                         self.data_pool_name, pgs_per_fs_pool.__str__())
        self.mon_manager.raw_cluster_cmd('fs', 'new',
                                         self.name, self.metadata_pool_name, self.data_pool_name)

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

    def get_mds_map(self):
        fs = json.loads(self.mon_manager.raw_cluster_cmd("fs", "get", self.name, "--format=json-pretty"))
        return fs['mdsmap']

    def get_data_pool_name(self):
        return self.data_pool_name

    def get_data_pool_names(self):
        osd_map = self.mon_manager.get_osd_dump_json()
        id_to_name = {}
        for p in osd_map['pools']:
            id_to_name[p['pool']] = p['pool_name']

        return [id_to_name[pool_id] for pool_id in self.get_mds_map()['data_pools']]

    def get_metadata_pool_name(self):
        return self.metadata_pool_name

    def get_namespace_id(self):
        fs = json.loads(self.mon_manager.raw_cluster_cmd("fs", "get", self.name, "--format=json-pretty"))
        return fs['id']

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

    def are_daemons_healthy(self):
        """
        Return true if all daemons are in one of active, standby, standby-replay, and
        at least max_mds daemons are in 'active'.

        Unlike most of Filesystem, this function is tolerant of new-style `fs`
        commands being missing, because we are part of the ceph installation
        process during upgrade suites, so must fall back to old style commands
        when we get an EINVAL on a new style command.

        :return:
        """

        active_count = 0
        try:
            mds_map = self.get_mds_map()
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

        if active_count >= mds_map['max_mds']:
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

    def get_daemon_names(self, state=None):
        """
        Return MDS daemon names of those daemons in the given state
        :param state:
        :return:
        """
        status = self.get_mds_map()
        result = []
        for mds_status in sorted(status['info'].values(), lambda a, b: cmp(a['rank'], b['rank'])):
            if mds_status['state'] == state or state is None:
                result.append(mds_status['name'])

        return result

    def get_active_names(self):
        """
        Return MDS daemon names of those daemons holding ranks
        in state up:active

        :return: list of strings like ['a', 'b'], sorted by rank
        """
        return self.get_daemon_names("up:active")

    def get_rank_names(self):
        """
        Return MDS daemon names of those daemons holding a rank,
        sorted by rank.  This includes e.g. up:replay/reconnect
        as well as active, but does not include standby or
        standby-replay.
        """
        status = self.get_mds_map()
        result = []
        for mds_status in sorted(status['info'].values(), lambda a, b: cmp(a['rank'], b['rank'])):
            if mds_status['rank'] != -1 and mds_status['state'] != 'up:standby-replay':
                result.append(mds_status['name'])

        return result

    def wait_for_daemons(self, timeout=None):
        """
        Wait until all daemons are healthy
        :return:
        """

        if timeout is None:
            timeout = DAEMON_WAIT_TIMEOUT

        elapsed = 0
        while True:
            if self.are_daemons_healthy():
                return
            else:
                time.sleep(1)
                elapsed += 1

            if elapsed > timeout:
                raise RuntimeError("Timed out waiting for MDS daemons to become healthy")

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
        self.create()

    def get_metadata_object(self, object_type, object_id):
        """
        Retrieve an object from the metadata pool, pass it through
        ceph-dencoder to dump it to JSON, and return the decoded object.
        """
        temp_bin_path = '/tmp/out.bin'

        self.client_remote.run(args=[
            'sudo', os.path.join(self._prefix, 'rados'), '-p', self.metadata_pool_name, 'get', object_id, temp_bin_path
        ])

        stdout = StringIO()
        self.client_remote.run(args=[
            'sudo', os.path.join(self._prefix, 'ceph-dencoder'), 'type', object_type, 'import', temp_bin_path, 'decode', 'dump_json'
        ], stdout=stdout)
        dump_json = stdout.getvalue().strip()
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

    def mds_asok(self, command, mds_id=None):
        if mds_id is None:
            mds_id = self.get_lone_mds_id()

        return self.json_asok(command, 'mds', mds_id)

    def is_full(self):
        flags = json.loads(self.mon_manager.raw_cluster_cmd("osd", "dump", "--format=json-pretty"))['flags']
        return 'full' in flags

    def is_pool_full(self, pool_name):
        pools = json.loads(self.mon_manager.raw_cluster_cmd("osd", "dump", "--format=json-pretty"))['pools']
        for pool in pools:
            if pool['pool_name'] == pool_name:
                return 'full' in pool['flags_names'].split(",")

        raise RuntimeError("Pool not found '{0}'".format(pool_name))

    def wait_for_state(self, goal_state, reject=None, timeout=None, mds_id=None):
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
            if mds_id is not None:
                # mds_info is None if no daemon with this ID exists in the map
                mds_info = self.mon_manager.get_mds_status(mds_id)
                current_state = mds_info['state'] if mds_info else None
                log.info("Looked up MDS state for {0}: {1}".format(mds_id, current_state))
            else:
                # In general, look for a single MDS
                mds_status = self.get_mds_map()
                states = [m['state'] for m in mds_status['info'].values()]
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
                log.error("MDS status at timeout: {0}".format(self.get_mds_map()))
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
            proc = remote.run(
                args=args,
                stdout=StringIO())
        except CommandFailedError as e:
            log.error(e.__str__())
            raise ObjectNotFound(obj_name)

        data = proc.stdout.getvalue()

        p = remote.run(
            args=[os.path.join(self._prefix, "ceph-dencoder"), "type", type, "import", "-", "decode", "dump_json"],
            stdout=StringIO(),
            stdin=data
        )

        return json.loads(p.stdout.getvalue().strip())

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
            for n in range(0, ((size - 1) / stripe_size) + 1)
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

    def rados(self, args, pool=None, namespace=None, stdin_data=None):
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
        p = remote.run(
            args=args,
            stdin=stdin_data,
            stdout=StringIO())
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

    def _run_tool(self, tool, args, rank=None, quiet=False):
        # Tests frequently have [client] configuration that jacks up
        # the objecter log level (unlikely to be interesting here)
        # and does not set the mds log level (very interesting here)
        if quiet:
            base_args = [os.path.join(self._prefix, tool), '--debug-mds=1', '--debug-objecter=1']
        else:
            base_args = [os.path.join(self._prefix, tool), '--debug-mds=4', '--debug-objecter=1']

        if rank is not None:
            base_args.extend(["--rank", "%d" % rank])

        t1 = datetime.datetime.now()
        r = self.tool_remote.run(
            args=base_args + args,
            stdout=StringIO()).stdout.getvalue().strip()
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

    def journal_tool(self, args, rank=None, quiet=False):
        """
        Invoke cephfs-journal-tool with the passed arguments, and return its stdout
        """
        return self._run_tool("cephfs-journal-tool", args, rank, quiet)

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
