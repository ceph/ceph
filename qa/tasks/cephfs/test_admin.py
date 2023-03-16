import errno
import json
import logging
import time
import uuid
from io import StringIO
from os.path import join as os_path_join

from teuthology.exceptions import CommandFailedError
from teuthology.contextutil import safe_while

from tasks.cephfs.cephfs_test_case import CephFSTestCase, classhook
from tasks.cephfs.filesystem import FileLayout, FSMissing
from tasks.cephfs.fuse_mount import FuseMount
from tasks.cephfs.caps_helper import CapTester

log = logging.getLogger(__name__)

class TestLabeledPerfCounters(CephFSTestCase):
    CLIENTS_REQUIRED = 2
    MDSS_REQUIRED = 1

    def test_per_client_labeled_perf_counters(self):
        """
        That the per-client labelled perf counters depict the clients
        performaing IO.
        """
        def get_counters_for(filesystem, client_id):
            dump = self.fs.rank_tell(["counter", "dump"])
            per_client_metrics_key = f'mds_client_metrics-{filesystem}'
            counters = [c["counters"] for \
                        c in dump[per_client_metrics_key] if c["labels"]["client"] == client_id]
            return counters[0]

        # sleep a bit so that we get updated clients...
        time.sleep(10)

        # lookout for clients...
        dump = self.fs.rank_tell(["counter", "dump"])

        fs_suffix = dump["mds_client_metrics"][0]["labels"]["fs_name"]
        self.assertGreaterEqual(dump["mds_client_metrics"][0]["counters"]["num_clients"], 2)

        per_client_metrics_key = f'mds_client_metrics-{fs_suffix}'
        mount_a_id = f'client.{self.mount_a.get_global_id()}'
        mount_b_id = f'client.{self.mount_b.get_global_id()}'

        clients = [c["labels"]["client"] for c in dump[per_client_metrics_key]]
        self.assertIn(mount_a_id, clients)
        self.assertIn(mount_b_id, clients)

        # write workload
        self.mount_a.create_n_files("test_dir/test_file", 1000, sync=True)
        with safe_while(sleep=1, tries=30, action=f'wait for counters - {mount_a_id}') as proceed:
            counters_dump_a = get_counters_for(fs_suffix, mount_a_id)
            while proceed():
                if counters_dump_a["total_write_ops"] > 0 and counters_dump_a["total_write_size"] > 0:
                    return True

        # read from the other client
        for i in range(100):
            self.mount_b.open_background(basename=f'test_dir/test_file_{i}', write=False)
        with safe_while(sleep=1, tries=30, action=f'wait for counters - {mount_b_id}') as proceed:
            counters_dump_b = get_counters_for(fs_suffix, mount_b_id)
            while proceed():
                if counters_dump_b["total_read_ops"] > 0 and counters_dump_b["total_read_size"] > 0:
                    return True

        self.fs.teardown()

class TestAdminCommands(CephFSTestCase):
    """
    Tests for administration command.
    """

    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 1

    def check_pool_application_metadata_key_value(self, pool, app, key, value):
        output = self.get_ceph_cmd_stdout(
            'osd', 'pool', 'application', 'get', pool, app, key)
        self.assertEqual(str(output.strip()), value)

    def setup_ec_pools(self, n, metadata=True, overwrites=True):
        if metadata:
            self.run_ceph_cmd('osd', 'pool', 'create', n+"-meta", "8")
        cmd = ['osd', 'erasure-code-profile', 'set', n+"-profile", "m=2", "k=2", "crush-failure-domain=osd"]
        self.run_ceph_cmd(cmd)
        self.run_ceph_cmd('osd', 'pool', 'create', n+"-data", "8", "erasure", n+"-profile")
        if overwrites:
            self.run_ceph_cmd('osd', 'pool', 'set', n+"-data", 'allow_ec_overwrites', 'true')

@classhook('_add_valid_tell')
class TestValidTell(TestAdminCommands):
    @classmethod
    def _add_valid_tell(cls):
        tells = [
          ['cache', 'status'],
          ['damage', 'ls'],
          ['dump_blocked_ops'],
          ['dump_blocked_ops_count'],
          ['dump_historic_ops'],
          ['dump_historic_ops_by_duration'],
          ['dump_mempools'],
          ['dump_ops_in_flight'],
          ['flush', 'journal'],
          ['get', 'subtrees'],
          ['ops', 'locks'],
          ['ops'],
          ['status'],
          ['version'],
        ]
        def test(c):
            def f(self):
                J = self.fs.rank_tell(c)
                json.dumps(J)
                log.debug("dumped:\n%s", str(J))
            return f
        for c in tells:
            setattr(cls, 'test_valid_' + '_'.join(c), test(c))

class TestFsStatus(TestAdminCommands):
    """
    Test "ceph fs status subcommand.
    """

    def test_fs_status(self):
        """
        That `ceph fs status` command functions.
        """

        s = self.get_ceph_cmd_stdout("fs", "status")
        self.assertTrue("active" in s)

        mdsmap = json.loads(self.get_ceph_cmd_stdout("fs", "status", "--format=json-pretty"))["mdsmap"]
        self.assertEqual(mdsmap[0]["state"], "active")

        mdsmap = json.loads(self.get_ceph_cmd_stdout("fs", "status", "--format=json"))["mdsmap"]
        self.assertEqual(mdsmap[0]["state"], "active")


class TestAddDataPool(TestAdminCommands):
    """
    Test "ceph fs add_data_pool" subcommand.
    """

    def test_add_data_pool_root(self):
        """
        That a new data pool can be added and used for the root directory.
        """

        p = self.fs.add_data_pool("foo")
        self.fs.set_dir_layout(self.mount_a, ".", FileLayout(pool=p))

    def test_add_data_pool_application_metadata(self):
        """
        That the application metadata set on a newly added data pool is as expected.
        """
        pool_name = "foo"
        mon_cmd = self.get_ceph_cmd_stdout
        mon_cmd('osd', 'pool', 'create', pool_name, '--pg_num_min',
                str(self.fs.pg_num_min))
        # Check whether https://tracker.ceph.com/issues/43061 is fixed
        mon_cmd('osd', 'pool', 'application', 'enable', pool_name, 'cephfs')
        self.fs.add_data_pool(pool_name, create=False)
        self.check_pool_application_metadata_key_value(
            pool_name, 'cephfs', 'data', self.fs.name)

    def test_add_data_pool_subdir(self):
        """
        That a new data pool can be added and used for a sub-directory.
        """

        p = self.fs.add_data_pool("foo")
        self.mount_a.run_shell("mkdir subdir")
        self.fs.set_dir_layout(self.mount_a, "subdir", FileLayout(pool=p))

    def test_add_data_pool_non_alphamueric_name_as_subdir(self):
        """
        That a new data pool with non-alphanumeric name can be added and used for a sub-directory.
        """
        p = self.fs.add_data_pool("I-am-data_pool00.")
        self.mount_a.run_shell("mkdir subdir")
        self.fs.set_dir_layout(self.mount_a, "subdir", FileLayout(pool=p))

    def test_add_data_pool_ec(self):
        """
        That a new EC data pool can be added.
        """

        n = "test_add_data_pool_ec"
        self.setup_ec_pools(n, metadata=False)
        self.fs.add_data_pool(n+"-data", create=False)

    def test_add_already_in_use_data_pool(self):
        """
        That command try to add data pool which is already in use with another fs.
        """

        # create first data pool, metadata pool and add with filesystem
        first_fs = "first_fs"
        first_metadata_pool = "first_metadata_pool"
        first_data_pool = "first_data_pool"
        self.run_ceph_cmd('osd', 'pool', 'create', first_metadata_pool)
        self.run_ceph_cmd('osd', 'pool', 'create', first_data_pool)
        self.run_ceph_cmd('fs', 'new', first_fs, first_metadata_pool, first_data_pool)

        # create second data pool, metadata pool and add with filesystem
        second_fs = "second_fs"
        second_metadata_pool = "second_metadata_pool"
        second_data_pool = "second_data_pool"
        self.run_ceph_cmd('osd', 'pool', 'create', second_metadata_pool)
        self.run_ceph_cmd('osd', 'pool', 'create', second_data_pool)
        self.run_ceph_cmd('fs', 'new', second_fs, second_metadata_pool, second_data_pool)

        # try to add 'first_data_pool' with 'second_fs'
        # Expecting EINVAL exit status because 'first_data_pool' is already in use with 'first_fs'
        try:
            self.run_ceph_cmd('fs', 'add_data_pool', second_fs, first_data_pool)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.EINVAL)
        else:
            self.fail("Expected EINVAL because data pool is already in use as data pool for first_fs")

    def test_add_already_in_use_metadata_pool(self):
        """
        That command try to add metadata pool which is already in use with another fs.
        """

        # create first data pool, metadata pool and add with filesystem
        first_fs = "first_fs"
        first_metadata_pool = "first_metadata_pool"
        first_data_pool = "first_data_pool"
        self.run_ceph_cmd('osd', 'pool', 'create', first_metadata_pool)
        self.run_ceph_cmd('osd', 'pool', 'create', first_data_pool)
        self.run_ceph_cmd('fs', 'new', first_fs, first_metadata_pool, first_data_pool)

        # create second data pool, metadata pool and add with filesystem
        second_fs = "second_fs"
        second_metadata_pool = "second_metadata_pool"
        second_data_pool = "second_data_pool"
        self.run_ceph_cmd('osd', 'pool', 'create', second_metadata_pool)
        self.run_ceph_cmd('osd', 'pool', 'create', second_data_pool)
        self.run_ceph_cmd('fs', 'new', second_fs, second_metadata_pool, second_data_pool)

        # try to add 'second_metadata_pool' with 'first_fs' as a data pool
        # Expecting EINVAL exit status because 'second_metadata_pool'
        # is already in use with 'second_fs' as a metadata pool
        try:
            self.run_ceph_cmd('fs', 'add_data_pool', first_fs, second_metadata_pool)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.EINVAL)
        else:
            self.fail("Expected EINVAL because data pool is already in use as metadata pool for 'second_fs'")

class TestFsNew(TestAdminCommands):
    """
    Test "ceph fs new" subcommand.
    """
    MDSS_REQUIRED = 3

    def test_fsnames_can_only_by_goodchars(self):
        n = 'test_fsnames_can_only_by_goodchars'
        metapoolname, datapoolname = n+'-testmetapool', n+'-testdatapool'
        badname = n+'badname@#'

        self.run_ceph_cmd('osd', 'pool', 'create', n+metapoolname)
        self.run_ceph_cmd('osd', 'pool', 'create', n+datapoolname)

        # test that fsname not with "goodchars" fails
        args = ['fs', 'new', badname, metapoolname, datapoolname]
        proc = self.run_ceph_cmd(args=args, stderr=StringIO(),
                                 check_status=False)
        self.assertIn('invalid chars', proc.stderr.getvalue().lower())

        self.run_ceph_cmd('osd', 'pool', 'rm', metapoolname,
                          metapoolname,
                          '--yes-i-really-really-mean-it-not-faking')
        self.run_ceph_cmd('osd', 'pool', 'rm', datapoolname,
                          datapoolname,
                          '--yes-i-really-really-mean-it-not-faking')

    def test_new_default_ec(self):
        """
        That a new file system warns/fails with an EC default data pool.
        """

        self.mount_a.umount_wait(require_clean=True)
        self.mds_cluster.delete_all_filesystems()
        n = "test_new_default_ec"
        self.setup_ec_pools(n)
        try:
            self.run_ceph_cmd('fs', 'new', n, n+"-meta", n+"-data")
        except CommandFailedError as e:
            if e.exitstatus == 22:
                pass
            else:
                raise
        else:
            raise RuntimeError("expected failure")

    def test_new_default_ec_force(self):
        """
        That a new file system succeeds with an EC default data pool with --force.
        """

        self.mount_a.umount_wait(require_clean=True)
        self.mds_cluster.delete_all_filesystems()
        n = "test_new_default_ec_force"
        self.setup_ec_pools(n)
        self.run_ceph_cmd('fs', 'new', n, n+"-meta", n+"-data", "--force")

    def test_new_default_ec_no_overwrite(self):
        """
        That a new file system fails with an EC default data pool without overwrite.
        """

        self.mount_a.umount_wait(require_clean=True)
        self.mds_cluster.delete_all_filesystems()
        n = "test_new_default_ec_no_overwrite"
        self.setup_ec_pools(n, overwrites=False)
        try:
            self.run_ceph_cmd('fs', 'new', n, n+"-meta", n+"-data")
        except CommandFailedError as e:
            if e.exitstatus == 22:
                pass
            else:
                raise
        else:
            raise RuntimeError("expected failure")
        # and even with --force !
        try:
            self.run_ceph_cmd('fs', 'new', n, n+"-meta", n+"-data", "--force")
        except CommandFailedError as e:
            if e.exitstatus == 22:
                pass
            else:
                raise
        else:
            raise RuntimeError("expected failure")

    def test_fs_new_pool_application_metadata(self):
        """
        That the application metadata set on the pools of a newly created filesystem are as expected.
        """
        self.mount_a.umount_wait(require_clean=True)
        self.mds_cluster.delete_all_filesystems()
        fs_name = "test_fs_new_pool_application"
        keys = ['metadata', 'data']
        pool_names = [fs_name+'-'+key for key in keys]
        mon_cmd = self.get_ceph_cmd_stdout
        for p in pool_names:
            mon_cmd('osd', 'pool', 'create', p, '--pg_num_min', str(self.fs.pg_num_min))
            mon_cmd('osd', 'pool', 'application', 'enable', p, 'cephfs')
        mon_cmd('fs', 'new', fs_name, pool_names[0], pool_names[1])
        for i in range(2):
            self.check_pool_application_metadata_key_value(
                pool_names[i], 'cephfs', keys[i], fs_name)

    def test_fs_new_with_specific_id(self):
        """
        That a file system can be created with a specific ID.
        """
        fs_name = "test_fs_specific_id"
        fscid = 100
        keys = ['metadata', 'data']
        pool_names = [fs_name+'-'+key for key in keys]
        for p in pool_names:
            self.run_ceph_cmd(f'osd pool create {p}')
        self.run_ceph_cmd(f'fs new {fs_name} {pool_names[0]} {pool_names[1]} --fscid  {fscid} --force')
        self.fs.status().get_fsmap(fscid)
        for i in range(2):
            self.check_pool_application_metadata_key_value(pool_names[i], 'cephfs', keys[i], fs_name)

    def test_fs_new_with_specific_id_idempotency(self):
        """
        That command to create file system with specific ID is idempotent.
        """
        fs_name = "test_fs_specific_id"
        fscid = 100
        keys = ['metadata', 'data']
        pool_names = [fs_name+'-'+key for key in keys]
        for p in pool_names:
            self.run_ceph_cmd(f'osd pool create {p}')
        self.run_ceph_cmd(f'fs new {fs_name} {pool_names[0]} {pool_names[1]} --fscid  {fscid} --force')
        self.run_ceph_cmd(f'fs new {fs_name} {pool_names[0]} {pool_names[1]} --fscid  {fscid} --force')
        self.fs.status().get_fsmap(fscid)

    def test_fs_new_with_specific_id_fails_without_force_flag(self):
        """
        That command to create file system with specific ID fails without '--force' flag.
        """
        fs_name = "test_fs_specific_id"
        fscid = 100
        keys = ['metadata', 'data']
        pool_names = [fs_name+'-'+key for key in keys]
        for p in pool_names:
            self.run_ceph_cmd(f'osd pool create {p}')
        try:
            self.run_ceph_cmd(f'fs new {fs_name} {pool_names[0]} {pool_names[1]} --fscid  {fscid}')
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EINVAL,
                "invalid error code on creating a file system with specifc ID without --force flag")
        else:
            self.fail("expected creating file system with specific ID without '--force' flag to fail")

    def test_fs_new_with_specific_id_fails_already_in_use(self):
        """
        That creating file system with ID already in use fails.
        """
        fs_name = "test_fs_specific_id"
        # file system ID already in use
        fscid =  self.fs.status().map['filesystems'][0]['id']
        keys = ['metadata', 'data']
        pool_names = [fs_name+'-'+key for key in keys]
        for p in pool_names:
            self.run_ceph_cmd(f'osd pool create {p}')
        try:
            self.run_ceph_cmd(f'fs new {fs_name} {pool_names[0]} {pool_names[1]} --fscid  {fscid} --force')
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EINVAL,
                "invalid error code on creating a file system with specifc ID that is already in use")
        else:
            self.fail("expected creating file system with ID already in use to fail")

    def test_fs_new_metadata_pool_already_in_use(self):
        """
        That creating file system with metadata pool already in use.
        """

        # create first data pool, metadata pool and add with filesystem
        first_fs = "first_fs"
        first_metadata_pool = "first_metadata_pool"
        first_data_pool = "first_data_pool"
        self.run_ceph_cmd('osd', 'pool', 'create', first_metadata_pool)
        self.run_ceph_cmd('osd', 'pool', 'create', first_data_pool)
        self.run_ceph_cmd('fs', 'new', first_fs, first_metadata_pool, first_data_pool)

        second_fs = "second_fs"
        second_data_pool = "second_data_pool"
        self.run_ceph_cmd('osd', 'pool', 'create', second_data_pool)

        # try to create new fs 'second_fs' with following configuration
        # metadata pool -> 'first_metadata_pool'
        # data pool -> 'second_data_pool'
        # Expecting EINVAL exit status because 'first_metadata_pool'
        # is already in use with 'first_fs'
        try:
            self.run_ceph_cmd('fs', 'new', second_fs, first_metadata_pool, second_data_pool)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.EINVAL)
        else:
            self.fail("Expected EINVAL because metadata  pool is already in use for 'first_fs'")

    def test_fs_new_data_pool_already_in_use(self):
        """
        That creating file system with data pool already in use.
        """

        # create first data pool, metadata pool and add with filesystem
        first_fs = "first_fs"
        first_metadata_pool = "first_metadata_pool"
        first_data_pool = "first_data_pool"
        self.run_ceph_cmd('osd', 'pool', 'create', first_metadata_pool)
        self.run_ceph_cmd('osd', 'pool', 'create', first_data_pool)
        self.run_ceph_cmd('fs', 'new', first_fs, first_metadata_pool, first_data_pool)

        second_fs = "second_fs"
        second_metadata_pool = "second_metadata_pool"
        self.run_ceph_cmd('osd', 'pool', 'create', second_metadata_pool)

        # try to create new fs 'second_fs' with following configuration
        # metadata pool -> 'second_metadata_pool'
        # data pool -> 'first_data_pool'
        # Expecting EINVAL exit status because 'first_data_pool'
        # is already in use with 'first_fs'
        try:
            self.run_ceph_cmd('fs', 'new', second_fs, second_metadata_pool, first_data_pool)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.EINVAL)
        else:
            self.fail("Expected EINVAL because data pool is already in use for 'first_fs'")

    def test_fs_new_metadata_and_data_pool_in_use_by_another_same_fs(self):
        """
        That creating file system with metadata and data pool which is already in use by another same fs.
        """

        # create first data pool, metadata pool and add with filesystem
        first_fs = "first_fs"
        first_metadata_pool = "first_metadata_pool"
        first_data_pool = "first_data_pool"
        self.run_ceph_cmd('osd', 'pool', 'create', first_metadata_pool)
        self.run_ceph_cmd('osd', 'pool', 'create', first_data_pool)
        self.run_ceph_cmd('fs', 'new', first_fs, first_metadata_pool, first_data_pool)

        second_fs = "second_fs"

        # try to create new fs 'second_fs' with following configuration
        # metadata pool -> 'first_metadata_pool'
        # data pool -> 'first_data_pool'
        # Expecting EINVAL exit status because 'first_metadata_pool' and 'first_data_pool'
        # is already in use with 'first_fs'
        try:
            self.run_ceph_cmd('fs', 'new', second_fs, first_metadata_pool, first_data_pool)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.EINVAL)
        else:
            self.fail("Expected EINVAL because metadata and data pool is already in use for 'first_fs'")

    def test_fs_new_metadata_and_data_pool_in_use_by_different_fs(self):
        """
        That creating file system with metadata and data pool which is already in use by different fs.
        """

        # create first data pool, metadata pool and add with filesystem
        first_fs = "first_fs"
        first_metadata_pool = "first_metadata_pool"
        first_data_pool = "first_data_pool"
        self.run_ceph_cmd('osd', 'pool', 'create', first_metadata_pool)
        self.run_ceph_cmd('osd', 'pool', 'create', first_data_pool)
        self.run_ceph_cmd('fs', 'new', first_fs, first_metadata_pool, first_data_pool)

        # create second data pool, metadata pool and add with filesystem
        second_fs = "second_fs"
        second_metadata_pool = "second_metadata_pool"
        second_data_pool = "second_data_pool"
        self.run_ceph_cmd('osd', 'pool', 'create', second_metadata_pool)
        self.run_ceph_cmd('osd', 'pool', 'create', second_data_pool)
        self.run_ceph_cmd('fs', 'new', second_fs, second_metadata_pool, second_data_pool)

        third_fs = "third_fs"

        # try to create new fs 'third_fs' with following configuration
        # metadata pool -> 'first_metadata_pool'
        # data pool -> 'second_data_pool'
        # Expecting EINVAL exit status because 'first_metadata_pool' and 'second_data_pool'
        # is already in use with 'first_fs' and 'second_fs'
        try:
            self.run_ceph_cmd('fs', 'new', third_fs, first_metadata_pool, second_data_pool)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.EINVAL)
        else:
            self.fail("Expected EINVAL because metadata and data pool is already in use for 'first_fs' and 'second_fs'")

    def test_fs_new_interchange_already_in_use_metadata_and_data_pool_of_same_fs(self):
        """
        That creating file system with interchanging metadata and data pool which is already in use by same fs.
        """

        # create first data pool, metadata pool and add with filesystem
        first_fs = "first_fs"
        first_metadata_pool = "first_metadata_pool"
        first_data_pool = "first_data_pool"
        self.run_ceph_cmd('osd', 'pool', 'create', first_metadata_pool)
        self.run_ceph_cmd('osd', 'pool', 'create', first_data_pool)
        self.run_ceph_cmd('fs', 'new', first_fs, first_metadata_pool, first_data_pool)

        second_fs = "second_fs"

        # try to create new fs 'second_fs' with following configuration
        # metadata pool -> 'first_data_pool' (already used as data pool for 'first_fs')
        # data pool -> 'first_metadata_pool' (already used as metadata pool for 'first_fs')
        # Expecting EINVAL exit status because 'first_data_pool' and 'first_metadata_pool'
        # is already in use with 'first_fs'
        try:
            self.run_ceph_cmd('fs', 'new', second_fs, first_data_pool, first_metadata_pool)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.EINVAL)
        else:
            self.fail("Expected EINVAL because metadata and data pool is already in use for 'first_fs'")

    def test_fs_new_interchange_already_in_use_metadata_and_data_pool_of_different_fs(self):
        """
        That creating file system with interchanging metadata and data pool which is already in use by defferent fs.
        """

        # create first data pool, metadata pool and add with filesystem
        first_fs = "first_fs"
        first_metadata_pool = "first_metadata_pool"
        first_data_pool = "first_data_pool"
        self.run_ceph_cmd('osd', 'pool', 'create', first_metadata_pool)
        self.run_ceph_cmd('osd', 'pool', 'create', first_data_pool)
        self.run_ceph_cmd('fs', 'new', first_fs, first_metadata_pool, first_data_pool)

        # create second data pool, metadata pool and add with filesystem
        second_fs = "second_fs"
        second_metadata_pool = "second_metadata_pool"
        second_data_pool = "second_data_pool"
        self.run_ceph_cmd('osd', 'pool', 'create', second_metadata_pool)
        self.run_ceph_cmd('osd', 'pool', 'create', second_data_pool)
        self.run_ceph_cmd('fs', 'new', second_fs, second_metadata_pool, second_data_pool)

        third_fs = "third_fs"

        # try to create new fs 'third_fs' with following configuration
        # metadata pool -> 'first_data_pool' (already used as data pool for 'first_fs')
        # data pool -> 'second_metadata_pool' (already used as metadata pool for 'second_fs')
        # Expecting EINVAL exit status because 'first_data_pool' and 'second_metadata_pool'
        # is already in use with 'first_fs' and 'second_fs'
        try:
            self.run_ceph_cmd('fs', 'new', third_fs, first_data_pool, second_metadata_pool)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.EINVAL)
        else:
            self.fail("Expected EINVAL because metadata and data pool is already in use for 'first_fs' and 'second_fs'")

    def test_fs_new_metadata_pool_already_in_use_with_rbd(self):
        """
        That creating new file system with metadata pool already used by rbd.
        """

        # create pool and initialise with rbd
        new_pool = "new_pool"
        self.run_ceph_cmd('osd', 'pool', 'create', new_pool)
        self.ctx.cluster.run(args=['rbd', 'pool', 'init', new_pool])

        new_fs = "new_fs"
        new_data_pool = "new_data_pool"

        self.run_ceph_cmd('osd', 'pool', 'create', new_data_pool)

        # try to create new fs 'new_fs' with following configuration
        # metadata pool -> 'new_pool' (already used by rbd app)
        # data pool -> 'new_data_pool'
        # Expecting EINVAL exit status because 'new_pool' is already in use with 'rbd' app
        try:
            self.run_ceph_cmd('fs', 'new', new_fs, new_pool, new_data_pool)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.EINVAL)
        else:
            self.fail("Expected EINVAL because metadata pool is already in use for rbd")

    def test_fs_new_data_pool_already_in_use_with_rbd(self):
        """
        That creating new file system with data pool already used by rbd.
        """

        # create pool and initialise with rbd
        new_pool = "new_pool"
        self.run_ceph_cmd('osd', 'pool', 'create', new_pool)
        self.ctx.cluster.run(args=['rbd', 'pool', 'init', new_pool])

        new_fs = "new_fs"
        new_metadata_pool = "new_metadata_pool"

        self.run_ceph_cmd('osd', 'pool', 'create', new_metadata_pool)

        # try to create new fs 'new_fs' with following configuration
        # metadata pool -> 'new_metadata_pool'
        # data pool -> 'new_pool' (already used by rbd app)
        # Expecting EINVAL exit status because 'new_pool' is already in use with 'rbd' app
        try:
            self.run_ceph_cmd('fs', 'new', new_fs, new_metadata_pool, new_pool)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.EINVAL)
        else:
            self.fail("Expected EINVAL because data pool is already in use for rbd")

class TestRenameCommand(TestAdminCommands):
    """
    Tests for rename command.
    """

    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 2

    def test_fs_rename(self):
        """
        That the file system can be renamed, and the application metadata set on its pools are as expected.
        """
        # Renaming the file system breaks this mount as the client uses
        # file system specific authorization. The client cannot read
        # or write even if the client's cephx ID caps are updated to access
        # the new file system name without the client being unmounted and
        # re-mounted.
        self.mount_a.umount_wait(require_clean=True)
        orig_fs_name = self.fs.name
        new_fs_name = 'new_cephfs'
        client_id = 'test_new_cephfs'

        self.run_ceph_cmd(f'fs rename {orig_fs_name} {new_fs_name} --yes-i-really-mean-it')

        # authorize a cephx ID access to the renamed file system.
        # use the ID to write to the file system.
        self.fs.name = new_fs_name
        keyring = self.fs.authorize(client_id, ('/', 'rw'))
        keyring_path = self.mount_a.client_remote.mktemp(data=keyring)
        self.mount_a.remount(client_id=client_id,
                             client_keyring_path=keyring_path,
                             cephfs_mntpt='/',
                             cephfs_name=self.fs.name)
        filedata, filename = 'some data on fs', 'file_on_fs'
        filepath = os_path_join(self.mount_a.hostfs_mntpt, filename)
        self.mount_a.write_file(filepath, filedata)
        self.check_pool_application_metadata_key_value(
            self.fs.get_data_pool_name(), 'cephfs', 'data', new_fs_name)
        self.check_pool_application_metadata_key_value(
            self.fs.get_metadata_pool_name(), 'cephfs', 'metadata', new_fs_name)

        # cleanup
        self.mount_a.umount_wait()
        self.run_ceph_cmd(f'auth rm client.{client_id}')

    def test_fs_rename_idempotency(self):
        """
        That the file system rename operation is idempotent.
        """
        # Renaming the file system breaks this mount as the client uses
        # file system specific authorization.
        self.mount_a.umount_wait(require_clean=True)
        orig_fs_name = self.fs.name
        new_fs_name = 'new_cephfs'

        self.run_ceph_cmd(f'fs rename {orig_fs_name} {new_fs_name} --yes-i-really-mean-it')
        self.run_ceph_cmd(f'fs rename {orig_fs_name} {new_fs_name} --yes-i-really-mean-it')

        # original file system name does not appear in `fs ls` command
        self.assertFalse(self.fs.exists())
        self.fs.name = new_fs_name
        self.assertTrue(self.fs.exists())

    def test_fs_rename_fs_new_fails_with_old_fsname_existing_pools(self):
        """
        That after renaming a file system, creating a file system with
        old name and existing FS pools fails.
        """
        # Renaming the file system breaks this mount as the client uses
        # file system specific authorization.
        self.mount_a.umount_wait(require_clean=True)
        orig_fs_name = self.fs.name
        new_fs_name = 'new_cephfs'
        data_pool = self.fs.get_data_pool_name()
        metadata_pool = self.fs.get_metadata_pool_name()
        self.run_ceph_cmd(f'fs rename {orig_fs_name} {new_fs_name} --yes-i-really-mean-it')

        try:
            self.run_ceph_cmd(f"fs new {orig_fs_name} {metadata_pool} {data_pool}")
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EINVAL,
                "invalid error code on creating a new file system with old "
                "name and existing pools.")
        else:
            self.fail("expected creating new file system with old name and "
                      "existing pools to fail.")

        try:
            self.run_ceph_cmd(f"fs new {orig_fs_name} {metadata_pool} {data_pool} --force")
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EINVAL,
                "invalid error code on creating a new file system with old "
                "name, existing pools and --force flag.")
        else:
            self.fail("expected creating new file system with old name, "
                      "existing pools, and --force flag to fail.")

        try:
            self.run_ceph_cmd(f"fs new {orig_fs_name} {metadata_pool} {data_pool} "
                                 "--allow-dangerous-metadata-overlay")
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EINVAL,
                "invalid error code on creating a new file system with old name, "
                "existing pools and --allow-dangerous-metadata-overlay flag.")
        else:
            self.fail("expected creating new file system with old name, "
                      "existing pools, and --allow-dangerous-metadata-overlay flag to fail.")

    def test_fs_rename_fails_without_yes_i_really_mean_it_flag(self):
        """
        That renaming a file system without '--yes-i-really-mean-it' flag fails.
        """
        try:
            self.run_ceph_cmd(f"fs rename {self.fs.name} new_fs")
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EPERM,
                "invalid error code on renaming a file system without the  "
                "'--yes-i-really-mean-it' flag")
        else:
            self.fail("expected renaming of file system without the "
                      "'--yes-i-really-mean-it' flag to fail ")

    def test_fs_rename_fails_for_non_existent_fs(self):
        """
        That renaming a non-existent file system fails.
        """
        try:
            self.run_ceph_cmd("fs rename non_existent_fs new_fs --yes-i-really-mean-it")
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.ENOENT, "invalid error code on renaming a non-existent fs")
        else:
            self.fail("expected renaming of a non-existent file system to fail")

    def test_fs_rename_fails_new_name_already_in_use(self):
        """
        That renaming a file system fails if the new name refers to an existing file system.
        """
        self.fs2 = self.mds_cluster.newfs(name='cephfs2', create=True)

        try:
            self.run_ceph_cmd(f"fs rename {self.fs.name} {self.fs2.name} --yes-i-really-mean-it")
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EINVAL,
                             "invalid error code on renaming to a fs name that is already in use")
        else:
            self.fail("expected renaming to a new file system name that is already in use to fail.")

    def test_fs_rename_fails_with_mirroring_enabled(self):
        """
        That renaming a file system fails if mirroring is enabled on it.
        """
        orig_fs_name = self.fs.name
        new_fs_name = 'new_cephfs'

        self.run_ceph_cmd(f'fs mirror enable {orig_fs_name}')
        try:
            self.run_ceph_cmd(f'fs rename {orig_fs_name} {new_fs_name} --yes-i-really-mean-it')
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EPERM, "invalid error code on renaming a mirrored file system")
        else:
            self.fail("expected renaming of a mirrored file system to fail")
        self.run_ceph_cmd(f'fs mirror disable {orig_fs_name}')


class TestDump(CephFSTestCase):
    CLIENTS_REQUIRED = 0
    MDSS_REQUIRED = 1

    def test_fs_dump_epoch(self):
        """
        That dumping a specific epoch works.
        """

        status1 = self.fs.status()
        status2 = self.fs.status(epoch=status1["epoch"]-1)
        self.assertEqual(status1["epoch"], status2["epoch"]+1)

    def test_fsmap_trim(self):
        """
        That the fsmap is trimmed normally.
        """

        paxos_service_trim_min = 25
        self.config_set('mon', 'paxos_service_trim_min', paxos_service_trim_min)
        mon_max_mdsmap_epochs = 20
        self.config_set('mon', 'mon_max_mdsmap_epochs', mon_max_mdsmap_epochs)

        status = self.fs.status()
        epoch = status["epoch"]

        # for N mutations
        mutations = paxos_service_trim_min + mon_max_mdsmap_epochs
        b = False
        for i in range(mutations):
            self.fs.set_joinable(b)
            b = not b

        time.sleep(10) # for tick/compaction

        try:
            self.fs.status(epoch=epoch)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.ENOENT, "invalid error code when trying to fetch FSMap that was trimmed")
        else:
            self.fail("trimming did not occur as expected")

    def test_fsmap_force_trim(self):
        """
        That the fsmap is trimmed forcefully.
        """

        status = self.fs.status()
        epoch = status["epoch"]

        paxos_service_trim_min = 1
        self.config_set('mon', 'paxos_service_trim_min', paxos_service_trim_min)
        mon_mds_force_trim_to = epoch+1
        self.config_set('mon', 'mon_mds_force_trim_to', mon_mds_force_trim_to)

        # force a new fsmap
        self.fs.set_joinable(False)
        time.sleep(10) # for tick/compaction

        status = self.fs.status()
        log.debug(f"new epoch is {status['epoch']}")
        self.fs.status(epoch=epoch+1) # epoch+1 is not trimmed, may not == status["epoch"]

        try:
            self.fs.status(epoch=epoch)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.ENOENT, "invalid error code when trying to fetch FSMap that was trimmed")
        else:
            self.fail("trimming did not occur as expected")


class TestRequiredClientFeatures(CephFSTestCase):
    CLIENTS_REQUIRED = 0
    MDSS_REQUIRED = 1

    def test_required_client_features(self):
        """
        That `ceph fs required_client_features` command functions.
        """

        def is_required(index):
            out = self.get_ceph_cmd_stdout('fs', 'get', self.fs.name, '--format=json-pretty')
            features = json.loads(out)['mdsmap']['required_client_features']
            if "feature_{0}".format(index) in features:
                return True;
            return False;

        features = json.loads(self.get_ceph_cmd_stdout('fs', 'feature', 'ls', '--format=json-pretty'))
        self.assertGreater(len(features), 0);

        for f in features:
            self.fs.required_client_features('rm', str(f['index']))

        for f in features:
            index = f['index']
            feature = f['name']
            if feature == 'reserved':
                feature = str(index)

            if index % 3 == 0:
                continue;
            self.fs.required_client_features('add', feature)
            self.assertTrue(is_required(index))

            if index % 2 == 0:
                continue;
            self.fs.required_client_features('rm', feature)
            self.assertFalse(is_required(index))

    def test_required_client_feature_add_reserved(self):
        """
        That `ceph fs required_client_features X add reserved` fails.
        """

        p = self.fs.required_client_features('add', 'reserved', check_status=False, stderr=StringIO())
        self.assertIn('Invalid feature name', p.stderr.getvalue())

    def test_required_client_feature_rm_reserved(self):
        """
        That `ceph fs required_client_features X rm reserved` fails.
        """

        p = self.fs.required_client_features('rm', 'reserved', check_status=False, stderr=StringIO())
        self.assertIn('Invalid feature name', p.stderr.getvalue())

    def test_required_client_feature_add_reserved_bit(self):
        """
        That `ceph fs required_client_features X add <reserved_bit>` passes.
        """

        p = self.fs.required_client_features('add', '1', stderr=StringIO())
        self.assertIn("added feature 'reserved' to required_client_features", p.stderr.getvalue())

    def test_required_client_feature_rm_reserved_bit(self):
        """
        That `ceph fs required_client_features X rm <reserved_bit>` passes.
        """

        self.fs.required_client_features('add', '1')
        p = self.fs.required_client_features('rm', '1', stderr=StringIO())
        self.assertIn("removed feature 'reserved' from required_client_features", p.stderr.getvalue())

class TestCompatCommands(CephFSTestCase):
    """
    """

    CLIENTS_REQUIRED = 0
    MDSS_REQUIRED = 3

    def test_add_compat(self):
        """
        Test adding a compat.
        """

        self.fs.fail()
        self.fs.add_compat(63, 'placeholder')
        mdsmap = self.fs.get_mds_map()
        self.assertIn("feature_63", mdsmap['compat']['compat'])

    def test_add_incompat(self):
        """
        Test adding an incompat.
        """

        self.fs.fail()
        self.fs.add_incompat(63, 'placeholder')
        mdsmap = self.fs.get_mds_map()
        log.info(f"{mdsmap}")
        self.assertIn("feature_63", mdsmap['compat']['incompat'])

    def test_rm_compat(self):
        """
        Test removing a compat.
        """

        self.fs.fail()
        self.fs.add_compat(63, 'placeholder')
        self.fs.rm_compat(63)
        mdsmap = self.fs.get_mds_map()
        self.assertNotIn("feature_63", mdsmap['compat']['compat'])

    def test_rm_incompat(self):
        """
        Test removing an incompat.
        """

        self.fs.fail()
        self.fs.add_incompat(63, 'placeholder')
        self.fs.rm_incompat(63)
        mdsmap = self.fs.get_mds_map()
        self.assertNotIn("feature_63", mdsmap['compat']['incompat'])

    def test_standby_compat(self):
        """
        That adding a compat does not prevent standbys from joining.
        """

        self.fs.fail()
        self.fs.add_compat(63, "placeholder")
        self.fs.set_joinable()
        self.fs.wait_for_daemons()
        mdsmap = self.fs.get_mds_map()
        self.assertIn("feature_63", mdsmap['compat']['compat'])

    def test_standby_incompat_reject(self):
        """
        That adding an incompat feature prevents incompatible daemons from joining.
        """

        self.fs.fail()
        self.fs.add_incompat(63, "placeholder")
        self.fs.set_joinable()
        try:
            self.fs.wait_for_daemons(timeout=60)
        except RuntimeError as e:
            if "Timed out waiting for MDS daemons to become healthy" in str(e):
                pass
            else:
                raise
        else:
            self.fail()

    def test_standby_incompat_upgrade(self):
        """
        That an MDS can upgrade the compat of a fs.
        """

        self.fs.fail()
        self.fs.rm_incompat(1)
        self.fs.set_joinable()
        self.fs.wait_for_daemons()
        mdsmap = self.fs.get_mds_map()
        self.assertIn("feature_1", mdsmap['compat']['incompat'])

    def test_standby_replay_not_upgradeable(self):
        """
        That the mons will not upgrade the MDSMap compat if standby-replay is
        enabled.
        """

        self.fs.fail()
        self.fs.rm_incompat(1)
        self.fs.set_allow_standby_replay(True)
        self.fs.set_joinable()
        try:
            self.fs.wait_for_daemons(timeout=60)
        except RuntimeError as e:
            if "Timed out waiting for MDS daemons to become healthy" in str(e):
                pass
            else:
                raise
        else:
            self.fail()

    def test_standby_incompat_reject_multifs(self):
        """
        Like test_standby_incompat_reject but with a second fs.
        """

        fs2 = self.mds_cluster.newfs(name="cephfs2", create=True)
        fs2.fail()
        fs2.add_incompat(63, 'placeholder')
        fs2.set_joinable()
        try:
            fs2.wait_for_daemons(timeout=60)
        except RuntimeError as e:
            if "Timed out waiting for MDS daemons to become healthy" in str(e):
                pass
            else:
                raise
        else:
            self.fail()
        # did self.fs lose MDS or standbys suicide?
        self.fs.wait_for_daemons()
        mdsmap = fs2.get_mds_map()
        self.assertIn("feature_63", mdsmap['compat']['incompat'])

class TestConfigCommands(CephFSTestCase):
    """
    Test that daemons and clients respond to the otherwise rarely-used
    runtime config modification operations.
    """

    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 1

    def test_ceph_config_show(self):
        """
        That I can successfully show MDS configuration.
        """

        names = self.fs.get_rank_names()
        for n in names:
            s = self.get_ceph_cmd_stdout("config", "show", "mds."+n)
            self.assertTrue("NAME" in s)
            self.assertTrue("mon_host" in s)


    def test_client_config(self):
        """
        That I can successfully issue asok "config set" commands

        :return:
        """

        if not isinstance(self.mount_a, FuseMount):
            self.skipTest("Test only applies to FUSE clients")

        test_key = "client_cache_size"
        test_val = "123"
        self.mount_a.admin_socket(['config', 'set', test_key, test_val])
        out = self.mount_a.admin_socket(['config', 'get', test_key])
        self.assertEqual(out[test_key], test_val)


    def test_mds_config_asok(self):
        test_key = "mds_max_purge_ops"
        test_val = "123"
        self.fs.mds_asok(['config', 'set', test_key, test_val])
        out = self.fs.mds_asok(['config', 'get', test_key])
        self.assertEqual(out[test_key], test_val)

    def test_mds_dump_cache_asok(self):
        cache_file = "cache_file"
        timeout = "1"
        self.fs.rank_asok(['dump', 'cache', cache_file, timeout])

    def test_mds_config_tell(self):
        test_key = "mds_max_purge_ops"
        test_val = "123"

        self.fs.rank_tell(['injectargs', "--{0}={1}".format(test_key, test_val)])

        # Read it back with asok because there is no `tell` equivalent
        out = self.fs.rank_tell(['config', 'get', test_key])
        self.assertEqual(out[test_key], test_val)


class TestMirroringCommands(CephFSTestCase):
    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 1

    def _enable_mirroring(self, fs_name):
        self.run_ceph_cmd("fs", "mirror", "enable", fs_name)

    def _disable_mirroring(self, fs_name):
        self.run_ceph_cmd("fs", "mirror", "disable", fs_name)

    def _add_peer(self, fs_name, peer_spec, remote_fs_name):
        peer_uuid = str(uuid.uuid4())
        self.run_ceph_cmd("fs", "mirror", "peer_add", fs_name, peer_uuid, peer_spec, remote_fs_name)

    def _remove_peer(self, fs_name, peer_uuid):
        self.run_ceph_cmd("fs", "mirror", "peer_remove", fs_name, peer_uuid)

    def _verify_mirroring(self, fs_name, flag_str):
        status = self.fs.status()
        fs_map = status.get_fsmap_byname(fs_name)
        if flag_str == 'enabled':
            self.assertTrue('mirror_info' in fs_map)
        elif flag_str == 'disabled':
            self.assertTrue('mirror_info' not in fs_map)
        else:
            raise RuntimeError(f'invalid flag_str {flag_str}')

    def _get_peer_uuid(self, fs_name, peer_spec):
        status = self.fs.status()
        fs_map = status.get_fsmap_byname(fs_name)
        mirror_info = fs_map.get('mirror_info', None)
        self.assertTrue(mirror_info is not None)
        for peer_uuid, remote in mirror_info['peers'].items():
            client_name = remote['remote']['client_name']
            cluster_name = remote['remote']['cluster_name']
            spec = f'{client_name}@{cluster_name}'
            if spec == peer_spec:
                return peer_uuid
        return None

    def test_mirroring_command(self):
        """basic mirroring command test -- enable, disable mirroring on a
        filesystem"""
        self._enable_mirroring(self.fs.name)
        self._verify_mirroring(self.fs.name, "enabled")
        self._disable_mirroring(self.fs.name)
        self._verify_mirroring(self.fs.name, "disabled")

    def test_mirroring_peer_commands(self):
        """test adding and removing peers to a mirror enabled filesystem"""
        self._enable_mirroring(self.fs.name)
        self._add_peer(self.fs.name, "client.site-b@site-b", "fs_b")
        self._add_peer(self.fs.name, "client.site-c@site-c", "fs_c")
        self._verify_mirroring(self.fs.name, "enabled")
        uuid_peer_b = self._get_peer_uuid(self.fs.name, "client.site-b@site-b")
        uuid_peer_c = self._get_peer_uuid(self.fs.name, "client.site-c@site-c")
        self.assertTrue(uuid_peer_b is not None)
        self.assertTrue(uuid_peer_c is not None)
        self._remove_peer(self.fs.name, uuid_peer_b)
        self._remove_peer(self.fs.name, uuid_peer_c)
        self._disable_mirroring(self.fs.name)
        self._verify_mirroring(self.fs.name, "disabled")

    def test_mirroring_command_idempotency(self):
        """test to check idempotency of mirroring family of commands """
        self._enable_mirroring(self.fs.name)
        self._verify_mirroring(self.fs.name, "enabled")
        self._enable_mirroring(self.fs.name)
        # add peer
        self._add_peer(self.fs.name, "client.site-b@site-b", "fs_b")
        uuid_peer_b1 = self._get_peer_uuid(self.fs.name, "client.site-b@site-b")
        self.assertTrue(uuid_peer_b1 is not None)
        # adding the peer again should be idempotent
        self._add_peer(self.fs.name, "client.site-b@site-b", "fs_b")
        uuid_peer_b2 = self._get_peer_uuid(self.fs.name, "client.site-b@site-b")
        self.assertTrue(uuid_peer_b2 is not None)
        self.assertTrue(uuid_peer_b1 == uuid_peer_b2)
        # remove peer
        self._remove_peer(self.fs.name, uuid_peer_b1)
        uuid_peer_b3 = self._get_peer_uuid(self.fs.name, "client.site-b@site-b")
        self.assertTrue(uuid_peer_b3 is None)
        # removing the peer again should be idempotent
        self._remove_peer(self.fs.name, uuid_peer_b1)
        self._disable_mirroring(self.fs.name)
        self._verify_mirroring(self.fs.name, "disabled")
        self._disable_mirroring(self.fs.name)

    def test_mirroring_disable_with_peers(self):
        """test disabling mirroring for a filesystem with active peers"""
        self._enable_mirroring(self.fs.name)
        self._add_peer(self.fs.name, "client.site-b@site-b", "fs_b")
        self._verify_mirroring(self.fs.name, "enabled")
        uuid_peer_b = self._get_peer_uuid(self.fs.name, "client.site-b@site-b")
        self.assertTrue(uuid_peer_b is not None)
        self._disable_mirroring(self.fs.name)
        self._verify_mirroring(self.fs.name, "disabled")
        # enable mirroring to check old peers
        self._enable_mirroring(self.fs.name)
        self._verify_mirroring(self.fs.name, "enabled")
        # peer should be gone
        uuid_peer_b = self._get_peer_uuid(self.fs.name, "client.site-b@site-b")
        self.assertTrue(uuid_peer_b is None)
        self._disable_mirroring(self.fs.name)
        self._verify_mirroring(self.fs.name, "disabled")

    def test_mirroring_with_filesystem_reset(self):
        """test to verify mirroring state post filesystem reset"""
        self._enable_mirroring(self.fs.name)
        self._add_peer(self.fs.name, "client.site-b@site-b", "fs_b")
        self._verify_mirroring(self.fs.name, "enabled")
        uuid_peer_b = self._get_peer_uuid(self.fs.name, "client.site-b@site-b")
        self.assertTrue(uuid_peer_b is not None)
        # reset filesystem
        self.fs.fail()
        self.fs.reset()
        self.fs.wait_for_daemons()
        self._verify_mirroring(self.fs.name, "disabled")


class TestFsAuthorize(CephFSTestCase):
    client_id = 'testuser'
    client_name = 'client.' + client_id

    def test_single_path_r(self):
        PERM = 'r'
        FS_AUTH_CAPS = (('/', PERM),)
        self.captester = CapTester()
        self.setup_test_env(FS_AUTH_CAPS)

        self.captester.run_mon_cap_tests(self.fs, self.client_id)
        self.captester.run_mds_cap_tests(PERM)

    def test_single_path_rw(self):
        PERM = 'rw'
        FS_AUTH_CAPS = (('/', PERM),)
        self.captester = CapTester()
        self.setup_test_env(FS_AUTH_CAPS)

        self.captester.run_mon_cap_tests(self.fs, self.client_id)
        self.captester.run_mds_cap_tests(PERM)

    def test_single_path_rootsquash(self):
        if not isinstance(self.mount_a, FuseMount):
            self.skipTest("only FUSE client has CEPHFS_FEATURE_MDS_AUTH_CAPS "
                          "needed to enforce root_squash MDS caps")

        PERM = 'rw'
        FS_AUTH_CAPS = (('/', PERM, 'root_squash'),)
        self.captester = CapTester()
        self.setup_test_env(FS_AUTH_CAPS)

        # testing MDS caps...
        # Since root_squash is set in client caps, client can read but not
        # write even thought access level is set to "rw".
        self.captester.conduct_pos_test_for_read_caps()
        self.captester.conduct_pos_test_for_open_caps()
        self.captester.conduct_neg_test_for_write_caps(sudo_write=True)
        self.captester.conduct_neg_test_for_chown_caps()
        self.captester.conduct_neg_test_for_truncate_caps()

    def test_single_path_rootsquash_issue_56067(self):
        """
        That a FS client using root squash MDS caps allows non-root user to write data
        to a file. And after client remount, the non-root user can read the data that
        was previously written by it. https://tracker.ceph.com/issues/56067
        """
        if not isinstance(self.mount_a, FuseMount):
            self.skipTest("only FUSE client has CEPHFS_FEATURE_MDS_AUTH_CAPS "
                          "needed to enforce root_squash MDS caps")

        keyring = self.fs.authorize(self.client_id, ('/', 'rw', 'root_squash'))
        keyring_path = self.mount_a.client_remote.mktemp(data=keyring)
        self.mount_a.remount(client_id=self.client_id,
                             client_keyring_path=keyring_path,
                             cephfs_mntpt='/')
        filedata, filename = 'some data on fs 1', 'file_on_fs1'
        filepath = os_path_join(self.mount_a.hostfs_mntpt, filename)
        self.mount_a.write_file(filepath, filedata)

        self.mount_a.remount(client_id=self.client_id,
                             client_keyring_path=keyring_path,
                             cephfs_mntpt='/')
        if filepath.find(self.mount_a.hostfs_mntpt) != -1:
            contents = self.mount_a.read_file(filepath)
            self.assertEqual(filedata, contents)

    def test_single_path_authorize_on_nonalphanumeric_fsname(self):
        """
        That fs authorize command works on filesystems with names having [_.-]
        characters
        """
        self.mount_a.umount_wait(require_clean=True)
        self.mds_cluster.delete_all_filesystems()
        fs_name = "cephfs-_."
        self.fs = self.mds_cluster.newfs(name=fs_name)
        self.fs.wait_for_daemons()
        self.run_ceph_cmd(f'auth caps client.{self.mount_a.client_id} '
                          f'mon "allow r" '
                          f'osd "allow rw pool={self.fs.get_data_pool_name()}" '
                          f'mds allow')
        self.mount_a.remount(cephfs_name=self.fs.name)
        PERM = 'rw'
        FS_AUTH_CAPS = (('/', PERM),)
        self.captester = CapTester()
        self.setup_test_env(FS_AUTH_CAPS)
        self.captester.run_mds_cap_tests(PERM)

    def test_multiple_path_r(self):
        PERM = 'r'
        FS_AUTH_CAPS = (('/dir1/dir12', PERM), ('/dir2/dir22', PERM))
        for c in FS_AUTH_CAPS:
            self.mount_a.run_shell(f'mkdir -p .{c[0]}')
        self.captesters = (CapTester(), CapTester())
        self.setup_test_env(FS_AUTH_CAPS)

        self.run_cap_test_one_by_one(FS_AUTH_CAPS)

    def test_multiple_path_rw(self):
        PERM = 'rw'
        FS_AUTH_CAPS = (('/dir1/dir12', PERM), ('/dir2/dir22', PERM))
        for c in FS_AUTH_CAPS:
            self.mount_a.run_shell(f'mkdir -p .{c[0]}')
        self.captesters = (CapTester(), CapTester())
        self.setup_test_env(FS_AUTH_CAPS)

        self.run_cap_test_one_by_one(FS_AUTH_CAPS)

    def run_cap_test_one_by_one(self, fs_auth_caps):
        keyring = self.run_cluster_cmd(f'auth get {self.client_name}')
        for i, c in enumerate(fs_auth_caps):
            self.assertIn(i, (0, 1))
            PATH = c[0]
            PERM = c[1]
            self._remount(keyring, PATH)
            # actual tests...
            self.captesters[i].run_mon_cap_tests(self.fs, self.client_id)
            self.captesters[i].run_mds_cap_tests(PERM, PATH)

    def tearDown(self):
        self.mount_a.umount_wait()
        self.run_ceph_cmd(f'auth rm {self.client_name}')

        super(type(self), self).tearDown()

    def _remount(self, keyring, path='/'):
        keyring_path = self.mount_a.client_remote.mktemp(data=keyring)
        self.mount_a.remount(client_id=self.client_id,
                             client_keyring_path=keyring_path,
                             cephfs_mntpt=path)

    def setup_for_single_path(self, fs_auth_caps):
        self.captester.write_test_files((self.mount_a,), '/')
        keyring = self.fs.authorize(self.client_id, fs_auth_caps)
        self._remount(keyring)

    def setup_for_multiple_paths(self, fs_auth_caps):
        for i, c in enumerate(fs_auth_caps):
            PATH = c[0]
            self.captesters[i].write_test_files((self.mount_a,), PATH)

        self.fs.authorize(self.client_id, fs_auth_caps)

    def setup_test_env(self, fs_auth_caps):
        if len(fs_auth_caps) == 1:
            self.setup_for_single_path(fs_auth_caps[0])
        else:
            self.setup_for_multiple_paths(fs_auth_caps)


class TestAdminCommandIdempotency(CephFSTestCase):
    """
    Tests for administration command idempotency.
    """

    CLIENTS_REQUIRED = 0
    MDSS_REQUIRED = 1

    def test_rm_idempotency(self):
        """
        That a removing a fs twice is idempotent.
        """

        data_pools = self.fs.get_data_pool_names(refresh=True)
        self.fs.fail()
        self.fs.rm()
        try:
            self.fs.get_mds_map()
        except FSMissing:
            pass
        else:
            self.fail("get_mds_map should raise")
        p = self.fs.rm()
        self.assertIn("does not exist", p.stderr.getvalue())
        self.fs.remove_pools(data_pools)


class TestAdminCommandDumpTree(CephFSTestCase):
    """
    Tests for administration command subtrees.
    """

    CLIENTS_REQUIRED = 0
    MDSS_REQUIRED = 1

    def test_dump_subtrees(self):
        """
        Dump all the subtrees to make sure the MDS daemon won't crash.
        """

        subtrees = self.fs.mds_asok(['get', 'subtrees'])
        log.info(f"dumping {len(subtrees)} subtrees:")
        for subtree in subtrees:
            log.info(f"  subtree: '{subtree['dir']['path']}'")
            self.fs.mds_asok(['dump', 'tree', subtree['dir']['path']])

        log.info("dumping 2 special subtrees:")
        log.info("  subtree: '/'")
        self.fs.mds_asok(['dump', 'tree', '/'])
        log.info("  subtree: '~mdsdir'")
        self.fs.mds_asok(['dump', 'tree', '~mdsdir'])

class TestAdminCommandDumpLoads(CephFSTestCase):
    """
    Tests for administration command dump loads.
    """

    CLIENTS_REQUIRED = 0
    MDSS_REQUIRED = 1

    def test_dump_loads(self):
        """
        make sure depth limit param is considered when dump loads for a MDS daemon.
        """

        log.info("dumping loads")
        loads = self.fs.mds_asok(['dump', 'loads', '1'])
        self.assertIsNotNone(loads)
        self.assertIn("dirfrags", loads)
        for d in loads["dirfrags"]:
            self.assertLessEqual(d["path"].count("/"), 1)

class TestFsBalRankMask(CephFSTestCase):
    """
    Tests ceph fs set <fs_name> bal_rank_mask
    """

    CLIENTS_REQUIRED = 0
    MDSS_REQUIRED = 2

    def test_bal_rank_mask(self):
        """
        check whether a specified bal_rank_mask value is valid or not.
        """
        bal_rank_mask = '0x0'
        log.info(f"set bal_rank_mask {bal_rank_mask}")
        self.fs.set_bal_rank_mask(bal_rank_mask)
        self.assertEqual(bal_rank_mask, self.fs.get_var('bal_rank_mask'))

        bal_rank_mask = '0'
        log.info(f"set bal_rank_mask {bal_rank_mask}")
        self.fs.set_bal_rank_mask(bal_rank_mask)
        self.assertEqual(bal_rank_mask, self.fs.get_var('bal_rank_mask'))

        bal_rank_mask = '-1'
        log.info(f"set bal_rank_mask {bal_rank_mask}")
        self.fs.set_bal_rank_mask(bal_rank_mask)
        self.assertEqual(bal_rank_mask, self.fs.get_var('bal_rank_mask'))

        bal_rank_mask = 'all'
        log.info(f"set bal_rank_mask {bal_rank_mask}")
        self.fs.set_bal_rank_mask(bal_rank_mask)
        self.assertEqual(bal_rank_mask, self.fs.get_var('bal_rank_mask'))

        bal_rank_mask = '0x1'
        log.info(f"set bal_rank_mask {bal_rank_mask}")
        self.fs.set_bal_rank_mask(bal_rank_mask)
        self.assertEqual(bal_rank_mask, self.fs.get_var('bal_rank_mask'))

        bal_rank_mask = '1'
        log.info(f"set bal_rank_mask {bal_rank_mask}")
        self.fs.set_bal_rank_mask(bal_rank_mask)
        self.assertEqual(bal_rank_mask, self.fs.get_var('bal_rank_mask'))

        bal_rank_mask = 'f0'
        log.info(f"set bal_rank_mask {bal_rank_mask}")
        self.fs.set_bal_rank_mask(bal_rank_mask)
        self.assertEqual(bal_rank_mask, self.fs.get_var('bal_rank_mask'))

        bal_rank_mask = 'ab'
        log.info(f"set bal_rank_mask {bal_rank_mask}")
        self.fs.set_bal_rank_mask(bal_rank_mask)
        self.assertEqual(bal_rank_mask, self.fs.get_var('bal_rank_mask'))

        bal_rank_mask = '0xfff0'
        log.info(f"set bal_rank_mask {bal_rank_mask}")
        self.fs.set_bal_rank_mask(bal_rank_mask)
        self.assertEqual(bal_rank_mask, self.fs.get_var('bal_rank_mask'))

        MAX_MDS = 256
        bal_rank_mask = '0x' + 'f' * int(MAX_MDS / 4)
        log.info(f"set bal_rank_mask {bal_rank_mask}")
        self.fs.set_bal_rank_mask(bal_rank_mask)
        self.assertEqual(bal_rank_mask, self.fs.get_var('bal_rank_mask'))

        bal_rank_mask = ''
        log.info("set bal_rank_mask to empty string")
        try:
            self.fs.set_bal_rank_mask(bal_rank_mask)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.EINVAL)

        bal_rank_mask = '0x1' + 'f' * int(MAX_MDS / 4)
        log.info(f"set bal_rank_mask {bal_rank_mask}")
        try:
            self.fs.set_bal_rank_mask(bal_rank_mask)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.EINVAL)
