
from StringIO import StringIO
import json
import logging
import time

from teuthology import misc
from teuthology.task import ceph_manager


log = logging.getLogger(__name__)


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

        mds_list = list(misc.all_roles_of_type(ctx.cluster, 'mds'))
        if len(mds_list) != 1:
            # Require exactly one MDS, the code path for creation failure when
            # a standby is available is different
            raise RuntimeError("This task requires exactly one MDS")

        self.mds_id = mds_list[0]

        (mds_remote,) = ctx.cluster.only('mds.{_id}'.format(_id=self.mds_id)).remotes.iterkeys()
        manager = ceph_manager.CephManager(
            mds_remote, ctx=ctx, logger=log.getChild('ceph_manager'),
        )
        self.mds_manager = manager

        client_list = list(misc.all_roles_of_type(self._ctx.cluster, 'client'))
        self.client_id = client_list[0]
        self.client_remote = list(misc.get_clients(ctx=ctx, roles=["client.{0}".format(self.client_id)]))[0][1]

    def mds_stop(self):
        """
        Stop the MDS daemon process.  If it held a rank, that rank
        will eventually go laggy.
        """
        mds = self._ctx.daemons.get_daemon('mds', self.mds_id)
        mds.stop()

    def mds_fail(self):
        """
        Inform MDSMonitor that the daemon process is dead.  If it held
        a rank, that rank will be relinquished.
        """
        self.mds_manager.raw_cluster_cmd("mds", "fail", "0")

    def mds_restart(self):
        mds = self._ctx.daemons.get_daemon('mds', self.mds_id)
        mds.restart()

    def reset(self):
        log.info("Creating new filesystem")

        assert not self._ctx.daemons.get_daemon('mds', self.mds_id).running()
        self.mds_manager.raw_cluster_cmd_result('mds', 'set', "max_mds", "0")
        self.mds_manager.raw_cluster_cmd_result('mds', 'fail', self.mds_id)
        self.mds_manager.raw_cluster_cmd_result('fs', 'rm', "default", "--yes-i-really-mean-it")
        self.mds_manager.raw_cluster_cmd_result('fs', 'new', "default", "metadata", "data")

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

    def mds_asok(self, command):
        proc = self.mds_manager.admin_socket('mds', self.mds_id, command)
        response_data = proc.stdout.getvalue()
        log.info("mds_asok output: {0}".format(response_data))
        if response_data.strip():
            return json.loads(response_data)
        else:
            return None

    def wait_for_state(self, goal_state, reject=None, timeout=None):
        """
        Block until the MDS reaches a particular state, or a failure condition
        is met.

        :param goal_state: Return once the MDS is in this state
        :param reject: Fail if the MDS enters this state before the goal state
        :param timeout: Fail if this many seconds pass before reaching goal
        :return: number of seconds waited, rounded down to integer
        """

        elapsed = 0
        while True:
            # mds_info is None if no daemon currently claims this rank
            mds_info = self.mds_manager.get_mds_status(self.mds_id)
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