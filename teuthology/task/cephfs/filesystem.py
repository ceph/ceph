
from StringIO import StringIO
import json
import logging
import os

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

        self.test_files = ['a', 'b', 'c']

    def mds_stop(self):
        mds = self._ctx.daemons.get_daemon('mds', self.mds_id)
        mds.stop()

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

    @property
    def _mount_path(self):
        return os.path.join(misc.get_testdir(self._ctx), 'mnt.{0}'.format(self.client_id))

    def create_files(self):
        for suffix in self.test_files:
            log.info("Creating file {0}".format(suffix))
            self.client_remote.run(args=[
                'sudo', 'touch', os.path.join(self._mount_path, suffix)
            ])

    def check_files(self):
        """
        This will raise a CommandFailedException if expected files are not present
        """
        for suffix in self.test_files:
            log.info("Checking file {0}".format(suffix))
            r = self.client_remote.run(args=[
                'sudo', 'ls', os.path.join(self._mount_path, suffix)
            ], check_status=False)
            if r.exitstatus != 0:
                raise RuntimeError("Expected file {0} not found".format(suffix))

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