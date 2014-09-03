
from StringIO import StringIO
import json
import logging
import time
from tasks.ceph import write_conf

from teuthology import misc
from teuthology.nuke import clear_firewall
from teuthology.parallel import parallel
from tasks import ceph_manager


log = logging.getLogger(__name__)


DAEMON_WAIT_TIMEOUT = 120


class Filesystem(object):
    """
    This object is for driving a CephFS filesystem.

    Limitations:
     * Assume a single filesystem+cluster
     * Assume a single MDS
    """
    def __init__(self, ctx, config):
        self._ctx = ctx
        self._config = config

        self.mds_ids = list(misc.all_roles_of_type(ctx.cluster, 'mds'))
        if len(self.mds_ids) == 0:
            raise RuntimeError("This task requires at least one MDS")

        first_mon = misc.get_first_mon(ctx, config)
        (mon_remote,) = ctx.cluster.only(first_mon).remotes.iterkeys()
        self.mon_manager = ceph_manager.CephManager(mon_remote, ctx=ctx, logger=log.getChild('ceph_manager'))
        self.mds_daemons = dict([(mds_id, self._ctx.daemons.get_daemon('mds', mds_id)) for mds_id in self.mds_ids])

        client_list = list(misc.all_roles_of_type(self._ctx.cluster, 'client'))
        self.client_id = client_list[0]
        self.client_remote = list(misc.get_clients(ctx=ctx, roles=["client.{0}".format(self.client_id)]))[0][1]

    def get_mds_hostnames(self):
        result = set()
        for mds_id in self.mds_ids:
            mds_remote = self.mon_manager.find_remote('mds', mds_id)
            result.add(mds_remote.hostname)

        return list(result)

    def set_ceph_conf(self, subsys, key, value):
        # Set config so that journal will be created in older format
        if 'mds' not in self._ctx.ceph.conf:
            self._ctx.ceph.conf['mds'] = {}
        self._ctx.ceph.conf['mds'][key] = value
        write_conf(self._ctx)  # XXX because we don't have the ceph task's config object, if they
                         # used a different config path this won't work.

    def are_daemons_healthy(self):
        """
        Return true if all daemons are in one of active, standby, standby-replay
        :return:
        """
        status = self.mon_manager.get_mds_status_all()
        for mds_id, mds_status in status['info'].items():
            if mds_status['state'] not in ["up:active", "up:standby", "up:standby-replay"]:
                log.warning("Unhealthy mds state {0}:{1}".format(mds_id, mds_status['state']))
                return False

        return True

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
        if len(self.mds_ids) != 1:
            raise ValueError("Explicit MDS argument required when multiple MDSs in use")
        else:
            return self.mds_ids[0]

    def _one_or_all(self, mds_id, cb):
        """
        Call a callback for a single named MDS, or for all

        :param mds_id: MDS daemon name, or None
        :param cb: Callback taking single argument of MDS daemon name
        """
        if mds_id is None:
            with parallel() as p:
                for mds_id in self.mds_ids:
                    p.spawn(cb, mds_id)
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

    def reset(self):
        log.info("Creating new filesystem")

        self.mon_manager.raw_cluster_cmd_result('mds', 'set', "max_mds", "0")
        for mds_id in self.mds_ids:
            assert not self._ctx.daemons.get_daemon('mds', mds_id).running()
            self.mon_manager.raw_cluster_cmd_result('mds', 'fail', mds_id)
        self.mon_manager.raw_cluster_cmd_result('fs', 'rm', "default", "--yes-i-really-mean-it")
        self.mon_manager.raw_cluster_cmd_result('fs', 'new', "default", "metadata", "data")

    def get_metadata_object(self, object_type, object_id):
        """
        Retrieve an object from the metadata pool, pass it through
        ceph-dencoder to dump it to JSON, and return the decoded object.
        """
        temp_bin_path = '/tmp/out.bin'

        self.client_remote.run(args=[
            'sudo', 'rados', '-p', 'metadata', 'get', object_id, temp_bin_path
        ])

        stdout = StringIO()
        self.client_remote.run(args=[
            'sudo', 'ceph-dencoder', 'type', object_type, 'import', temp_bin_path, 'decode', 'dump_json'
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
        proc = self.mon_manager.admin_socket('mds', mds_id, command)
        response_data = proc.stdout.getvalue()
        log.info("mds_asok output: {0}".format(response_data))
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
            remote.run(args=["sudo", "iptables", da_flag, "OUTPUT", "-p", "tcp", "--sport", "6800:6900", "-j", "REJECT", "-m", "comment", "--comment", "teuthology"])
            remote.run(args=["sudo", "iptables", da_flag, "INPUT", "-p", "tcp", "--dport", "6800:6900", "-j", "REJECT", "-m", "comment", "--comment", "teuthology"])

        self._one_or_all(mds_id, set_block)

    def clear_firewall(self):
        clear_firewall(self._ctx)

    def wait_for_state(self, goal_state, reject=None, timeout=None, mds_id=None):
        """
        Block until the MDS reaches a particular state, or a failure condition
        is met.

        :param goal_state: Return once the MDS is in this state
        :param reject: Fail if the MDS enters this state before the goal state
        :param timeout: Fail if this many seconds pass before reaching goal
        :return: number of seconds waited, rounded down to integer
        """

        if mds_id is None:
            mds_id = self.get_lone_mds_id()

        elapsed = 0
        while True:
            # mds_info is None if no daemon currently claims this rank
            mds_info = self.mon_manager.get_mds_status(mds_id)
            current_state = mds_info['state'] if mds_info else None

            if current_state == goal_state:
                log.info("reached state '{0}' in {1}s".format(current_state, elapsed))
                return elapsed
            elif reject is not None and current_state == reject:
                raise RuntimeError("MDS in reject state {0}".format(current_state))
            elif timeout is not None and elapsed > timeout:
                raise RuntimeError(
                    "Reached timeout after {0} seconds waiting for state {1}, while in state {2}".format(
                    elapsed, goal_state, current_state
                ))
            else:
                time.sleep(1)
                elapsed += 1
