import errno
import json
import logging
import time
import uuid
from io import StringIO
from os.path import join as os_path_join

from teuthology.orchestra.run import Raw
from teuthology.exceptions import CommandFailedError
from teuthology.contextutil import safe_while

from tasks.cephfs.cephfs_test_case import CephFSTestCase, classhook
from tasks.cephfs.filesystem import FileLayout, FSMissing
from tasks.cephfs.fuse_mount import FuseMount
from tasks.cephfs.caps_helper import CapsHelper

log = logging.getLogger(__name__)

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

    def _get_unhealthy_mds_id(self, health_report, health_warn):
        '''
        Return MDS ID for which health warning in "health_warn" has been
        generated.
        '''
        # variable "msg" should hold string something like this -
        # 'mds.b(mds.0): Behind on trimming (865/10) max_segments: 10,
        # num_segments: 86
        msg = health_report['checks'][health_warn]['detail'][0]['message']
        mds_id = msg.split('(')[0]
        mds_id = mds_id.replace('mds.', '')
        return mds_id

    def wait_till_health_warn(self, health_warn, active_mds_id, sleep=3,
                              tries=10):
        errmsg = (f'Expected health warning "{health_warn}" to eventually '
                  'show up in output of command "ceph health detail". Tried '
                  f'{tries} times with interval of {sleep} seconds but the '
                  'health warning didn\'t turn up.')

        with safe_while(sleep=sleep, tries=tries, action=errmsg) as proceed:
            while proceed():
                self.get_ceph_cmd_stdout(
                    f'tell mds.{active_mds_id} cache status')

                health_report = json.loads(self.get_ceph_cmd_stdout(
                    'health detail --format json'))

                if health_warn in health_report['checks']:
                    return


@classhook('_add_valid_tell')
class TestValidTell(TestAdminCommands):
    @classmethod
    def _add_valid_tell(cls):
        tells = [
          ['cache', 'status'],
          ['damage', 'ls'],
          ['dump_blocked_ops'],
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
            self.assertEqual(ce.exitstatus, errno.EEXIST,
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


class TestFsAuthorize(CapsHelper):
    client_id = 'testuser'
    client_name = 'client.' + client_id

    def test_single_path_r(self):
        perm = 'r'
        filepaths, filedata, mounts, keyring = self.setup_test_env(perm)
        moncap = self.get_mon_cap_from_keyring(self.client_name)

        self.run_mon_cap_tests(moncap, keyring)
        self.run_mds_cap_tests(filepaths, filedata, mounts, perm)

    def test_single_path_rw(self):
        perm = 'rw'
        filepaths, filedata, mounts, keyring = self.setup_test_env(perm)
        moncap = self.get_mon_cap_from_keyring(self.client_name)

        self.run_mon_cap_tests(moncap, keyring)
        self.run_mds_cap_tests(filepaths, filedata, mounts, perm)

    def test_single_path_rootsquash(self):
        filedata, filename = 'some data on fs 1', 'file_on_fs1'
        filepath = os_path_join(self.mount_a.hostfs_mntpt, filename)
        self.mount_a.write_file(filepath, filedata)

        keyring = self.fs.authorize(self.client_id, ('/', 'rw', 'root_squash'))
        keyring_path = self.mount_a.client_remote.mktemp(data=keyring)
        self.mount_a.remount(client_id=self.client_id,
                             client_keyring_path=keyring_path,
                             cephfs_mntpt='/')

        if filepath.find(self.mount_a.hostfs_mntpt) != -1:
            # can read, but not write as root
            contents = self.mount_a.read_file(filepath)
            self.assertEqual(filedata, contents)
            cmdargs = ['echo', 'some random data', Raw('|'), 'sudo', 'tee', filepath]
            self.mount_a.negtestcmd(args=cmdargs, retval=1, errmsg='permission denied')

    def test_single_path_authorize_on_nonalphanumeric_fsname(self):
        """
        That fs authorize command works on filesystems with names having [_.-] characters
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
        perm = 'rw'
        filepaths, filedata, mounts, keyring = self.setup_test_env(perm)
        self.run_mds_cap_tests(filepaths, filedata, mounts, perm)

    def test_multiple_path_r(self):
        perm, paths = 'r', ('/dir1', '/dir2/dir22')
        filepaths, filedata, mounts, keyring = self.setup_test_env(perm, paths)
        moncap = self.get_mon_cap_from_keyring(self.client_name)

        keyring_path = self.mount_a.client_remote.mktemp(data=keyring)
        for path in paths:
            self.mount_a.remount(client_id=self.client_id,
                                 client_keyring_path=keyring_path,
                                 cephfs_mntpt=path)


            # actual tests...
            self.run_mon_cap_tests(moncap, keyring)
            self.run_mds_cap_tests(filepaths, filedata, mounts, perm)

    def test_multiple_path_rw(self):
        perm, paths = 'rw', ('/dir1', '/dir2/dir22')
        filepaths, filedata, mounts, keyring = self.setup_test_env(perm, paths)
        moncap = self.get_mon_cap_from_keyring(self.client_name)

        keyring_path = self.mount_a.client_remote.mktemp(data=keyring)
        for path in paths:
            self.mount_a.remount(client_id=self.client_id,
                                 client_keyring_path=keyring_path,
                                 cephfs_mntpt=path)


            # actual tests...
            self.run_mon_cap_tests(moncap, keyring)
            self.run_mds_cap_tests(filepaths, filedata, mounts, perm)

    def tearDown(self):
        self.mount_a.umount_wait()
        self.run_ceph_cmd(f'auth rm {self.client_name}')

        super(type(self), self).tearDown()

    def setup_for_single_path(self, perm):
        filedata, filename = 'some data on fs 1', 'file_on_fs1'

        filepath = os_path_join(self.mount_a.hostfs_mntpt, filename)
        self.mount_a.write_file(filepath, filedata)

        keyring = self.fs.authorize(self.client_id, ('/', perm))
        keyring_path = self.mount_a.client_remote.mktemp(data=keyring)

        self.mount_a.remount(client_id=self.client_id,
                             client_keyring_path=keyring_path,
                             cephfs_mntpt='/')

        return filepath, filedata, keyring

    def setup_for_multiple_paths(self, perm, paths):
        filedata, filename = 'some data on fs 1', 'file_on_fs1'

        self.mount_a.run_shell('mkdir -p dir1/dir12/dir13 dir2/dir22/dir23')

        filepaths = []
        for path in paths:
            filepath = os_path_join(self.mount_a.hostfs_mntpt, path[1:], filename)
            self.mount_a.write_file(filepath, filedata)
            filepaths.append(filepath.replace(path, ''))
        filepaths = tuple(filepaths)

        keyring = self.fs.authorize(self.client_id, (paths[0], perm, paths[1],
                                                     perm))

        return filepaths, filedata, keyring

    def setup_test_env(self, perm, paths=()):
        filepaths, filedata, keyring = self.setup_for_multiple_paths(perm, paths) if paths \
            else self.setup_for_single_path(perm)

        if not isinstance(filepaths, tuple):
            filepaths = (filepaths, )
        if not isinstance(filedata, tuple):
            filedata = (filedata, )
        mounts = (self.mount_a, )

        return filepaths, filedata, mounts, keyring


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


class TestPermErrMsg(CephFSTestCase):

    CLIENT_NAME = 'client.testuser'
    FS1_NAME, FS2_NAME, FS3_NAME = 'abcd', 'efgh', 'ijkl'

    EXPECTED_ERRNO = 22
    EXPECTED_ERRMSG = ("Permission flags in MDS caps must start with 'r' or "
                       "'rw' or be '*' or 'all'")

    MONCAP = f'allow r fsname={FS1_NAME}'
    OSDCAP = f'allow rw tag cephfs data={FS1_NAME}'
    MDSCAPS = [
        'allow w',
        f'allow w fsname={FS1_NAME}',

        f'allow rw fsname={FS1_NAME}, allow w fsname={FS2_NAME}',
        f'allow w fsname={FS1_NAME}, allow rw fsname={FS2_NAME}',
        f'allow w fsname={FS1_NAME}, allow w fsname={FS2_NAME}',

        (f'allow rw fsname={FS1_NAME}, allow rw fsname={FS2_NAME}, allow '
         f'w fsname={FS3_NAME}'),

        # without space after comma
        f'allow rw fsname={FS1_NAME},allow w fsname={FS2_NAME}',


        'allow wr',
        f'allow wr fsname={FS1_NAME}',

        f'allow rw fsname={FS1_NAME}, allow wr fsname={FS2_NAME}',
        f'allow wr fsname={FS1_NAME}, allow rw fsname={FS2_NAME}',
        f'allow wr fsname={FS1_NAME}, allow wr fsname={FS2_NAME}',

        (f'allow rw fsname={FS1_NAME}, allow rw fsname={FS2_NAME}, allow '
         f'wr fsname={FS3_NAME}'),

        # without space after comma
        f'allow rw fsname={FS1_NAME},allow wr fsname={FS2_NAME}']

    def _negtestcmd(self, SUBCMD, MDSCAP):
        return self.negtest_ceph_cmd(
            args=(f'{SUBCMD} {self.CLIENT_NAME} '
                  f'mon "{self.MONCAP}" osd "{self.OSDCAP}" mds "{MDSCAP}"'),
            retval=self.EXPECTED_ERRNO, errmsgs=self.EXPECTED_ERRMSG)

    def test_auth_add(self):
        for mdscap in self.MDSCAPS:
            self._negtestcmd('auth add', mdscap)

    def test_auth_get_or_create(self):
        for mdscap in self.MDSCAPS:
            self._negtestcmd('auth get-or-create', mdscap)

    def test_auth_get_or_create_key(self):
        for mdscap in self.MDSCAPS:
            self._negtestcmd('auth get-or-create-key', mdscap)

    def test_fs_authorize(self):
        for wrong_perm in ('w', 'wr'):
            self.negtest_ceph_cmd(
                args=(f'fs authorize {self.fs.name} {self.CLIENT_NAME} / '
                      f'{wrong_perm}'), retval=self.EXPECTED_ERRNO,
                errmsgs=self.EXPECTED_ERRMSG)


class TestFSFail(TestAdminCommands):

    MDSS_REQUIRED = 2
    CLIENTS_REQUIRED = 1

    def test_with_health_warn_oversize_cache(self):
        '''
        Test that, when health warning MDS_CACHE_OVERSIZE is present for an
        MDS, command "ceph fs fail" fails without confirmation flag and passes
        when confirmation flag is passed.
        '''
        health_warn = 'MDS_CACHE_OVERSIZED'
        self.config_set('mds', 'mds_cache_memory_limit', '1K')
        self.config_set('mds', 'mds_health_cache_threshold', '1.00000')
        active_mds_id = self.fs.get_active_names()[0]

        self.mount_a.open_n_background('.', 400)
        self.wait_till_health_warn(health_warn, active_mds_id)

        # actual testing begins now.
        errmsg = 'mds_cache_oversized'
        self.negtest_ceph_cmd(args=f'fs fail {self.fs.name}',
                              retval=1, errmsgs=errmsg)
        self.run_ceph_cmd(f'fs fail {self.fs.name} --yes-i-really-mean-it')

        # Bring and wait for MDS to be up since it is needed for unmounting
        # of CephFS in CephFSTestCase.tearDown() to be successful.
        self.fs.set_joinable()
        self.fs.wait_for_daemons()

    def test_with_health_warn_trim(self):
        '''
        Test that, when health warning MDS_TRIM is present for an MDS, command
        "ceph fs fail" fails without confirmation flag and passes when
        confirmation flag is passed.
        '''
        health_warn = 'MDS_TRIM'
        # for generating health warning MDS_TRIM
        self.config_set('mds', 'mds_debug_subtrees', 'true')
        # this will really really slow the trimming, so that MDS_TRIM stays
        # for longer.
        self.config_set('mds', 'mds_log_trim_decay_rate', '60')
        self.config_set('mds', 'mds_log_trim_threshold', '1')
        active_mds_id = self.fs.get_active_names()[0]

        self.mount_a.open_n_background('.', 400)
        self.wait_till_health_warn(health_warn, active_mds_id)

        # actual testing begins now.
        errmsg = 'mds_trim'
        self.negtest_ceph_cmd(args=f'fs fail {self.fs.name}',
                              retval=1, errmsgs=errmsg)
        self.run_ceph_cmd(f'fs fail {self.fs.name} --yes-i-really-mean-it')

        # Bring and wait for MDS to be up since it is needed for unmounting
        # of CephFS in CephFSTestCase.tearDown() to be successful.
        self.fs.set_joinable()
        self.fs.wait_for_daemons()

    def test_with_health_warn_with_2_active_MDSs(self):
        '''
        Test that, when a CephFS has 2 active MDSs and one of them have either
        health warning MDS_TRIM or MDS_CACHE_OVERSIZE, running "ceph fs fail"
        fails without confirmation flag and passes when confirmation flag is
        passed.
        '''
        health_warn = 'MDS_CACHE_OVERSIZED'
        self.fs.set_max_mds(2)
        self.config_set('mds', 'mds_cache_memory_limit', '1K')
        self.config_set('mds', 'mds_health_cache_threshold', '1.00000')
        self.fs.wait_for_daemons()
        mds1_id, mds2_id = self.fs.get_active_names()

        self.mount_a.open_n_background('.', 400)
        # MDS ID for which health warning has been generated.
        self.wait_till_health_warn(health_warn, mds1_id)

        # actual testing begins now.
        errmsg = 'mds_cache_oversized'
        self.negtest_ceph_cmd(args=f'fs fail {self.fs.name}',
                              retval=1, errmsgs=errmsg)
        self.run_ceph_cmd(f'fs fail {self.fs.name} --yes-i-really-mean-it')

        # Bring and wait for MDS to be up since it is needed for unmounting
        # of CephFS in CephFSTestCase.tearDown() to be successful.
        self.fs.set_joinable()
        self.fs.wait_for_daemons()


class TestMDSFail(TestAdminCommands):

    MDSS_REQUIRED = 2
    CLIENTS_REQUIRED = 1

    def test_with_health_warn_oversize_cache(self):
        '''
        Test that, when health warning MDS_CACHE_OVERSIZE is present for an
        MDS, command "ceph mds fail" fails without confirmation flag and
        passes when confirmation flag is passed.
        '''
        health_warn = 'MDS_CACHE_OVERSIZED'
        self.config_set('mds', 'mds_cache_memory_limit', '1K')
        self.config_set('mds', 'mds_health_cache_threshold', '1.00000')
        active_mds_id = self.fs.get_active_names()[0]

        self.mount_a.open_n_background('.', 400)
        self.wait_till_health_warn(health_warn, active_mds_id)

        # actual testing begins now.
        errmsg = 'mds_cache_oversized'
        self.negtest_ceph_cmd(args=f'mds fail {active_mds_id}',
                              retval=1, errmsgs=errmsg)
        self.run_ceph_cmd(f'mds fail {active_mds_id} --yes-i-really-mean-it')

    def test_with_health_warn_trim(self):
        '''
        Test that, when health warning MDS_TRIM is present for an MDS, command
        "ceph mds fail" fails without confirmation flag and passes when
        confirmation is passed.
        '''
        health_warn = 'MDS_TRIM'
        # for generating health warning MDS_TRIM
        self.config_set('mds', 'mds_debug_subtrees', 'true')
        # this will really really slow the trimming, so that MDS_TRIM stays
        # for longer.
        self.config_set('mds', 'mds_log_trim_decay_rate', '60')
        self.config_set('mds', 'mds_log_trim_threshold', '1')
        active_mds_id = self.fs.get_active_names()[0]

        self.mount_a.open_n_background('.', 400)
        self.wait_till_health_warn(health_warn, active_mds_id)

        # actual testing begins now...
        errmsg = 'mds_trim'
        self.negtest_ceph_cmd(args=f'mds fail {active_mds_id}',
                              retval=1, errmsgs=errmsg)
        self.run_ceph_cmd(f'mds fail {active_mds_id} --yes-i-really-mean-it')

    def test_with_health_warn_with_2_active_MDSs(self):
        '''
        Test when a CephFS has 2 active MDSs and one of them have either
        health warning MDS_TRIM or MDS_CACHE_OVERSIZE, running "ceph mds fail"
        fails for both MDSs without confirmation flag and passes for both when
        confirmation flag is passed.
        '''
        health_warn = 'MDS_CACHE_OVERSIZED'
        self.fs.set_max_mds(2)
        self.config_set('mds', 'mds_cache_memory_limit', '1K')
        self.config_set('mds', 'mds_health_cache_threshold', '1.00000')
        self.fs.wait_for_daemons()
        mds1_id, mds2_id = self.fs.get_active_names()

        self.mount_a.open_n_background('.', 400)
        self.wait_till_health_warn(health_warn, mds1_id)

        health_report = json.loads(self.get_ceph_cmd_stdout('health detail '
                                                            '--format json'))
        # MDS ID for which health warning has been generated.
        hw_mds_id = self._get_unhealthy_mds_id(health_report, health_warn)
        if mds1_id == hw_mds_id:
            non_hw_mds_id = mds2_id
        elif mds2_id == hw_mds_id:
            non_hw_mds_id = mds1_id
        else:
            raise RuntimeError('There are only 2 MDSs right now but apparently'
                               'health warning was raised for an MDS other '
                               'than these two. This is definitely an error.')

        # actual testing begins now...
        errmsg = 'mds_cache_oversized'
        self.negtest_ceph_cmd(args=f'mds fail {non_hw_mds_id}', retval=1,
                              errmsgs=errmsg)
        self.negtest_ceph_cmd(args=f'mds fail {hw_mds_id}', retval=1,
                              errmsgs=errmsg)
        self.run_ceph_cmd(f'mds fail {mds1_id} --yes-i-really-mean-it')
        self.run_ceph_cmd(f'mds fail {mds2_id} --yes-i-really-mean-it')


class TestToggleVolumes(CephFSTestCase):
    '''
    Contains code for enabling/disabling mgr/volumes plugin.
    '''

    VOL_MOD_NAME = 'volumes'
    CONFIRM = '--yes-i-really-mean-it'

    def tearDown(self):
        '''
        Ensure that the volumes plugin is enabled after the test has finished
        running since not doing so might affect tearDown() of CephFSTestCase or
        other superclasses.
        '''
        json_output = self.get_ceph_cmd_stdout('mgr module ls --format json')
        json_output = json.loads(json_output)

        if 'volumes' in json_output['force_disabled_modules']:
            self.run_ceph_cmd(f'mgr module enable {self.VOL_MOD_NAME}')

        super(TestToggleVolumes, self).tearDown()

    def test_force_disable_with_confirmation(self):
        '''
        Test that running "ceph mgr module force disable volumes
        --yes-i-really-mean-it" successfully disables volumes plugin.

        Also test "ceph mgr module ls" output after this.
        '''
        self.run_ceph_cmd(f'mgr module force disable {self.VOL_MOD_NAME} '
                          f'{self.CONFIRM}')

        json_output = self.get_ceph_cmd_stdout('mgr module ls --format json')
        json_output = json.loads(json_output)

        self.assertIn(self.VOL_MOD_NAME, json_output['always_on_modules'])
        self.assertIn(self.VOL_MOD_NAME, json_output['force_disabled_modules'])

        self.assertNotIn(self.VOL_MOD_NAME, json_output['enabled_modules'])
        self.assertNotIn(self.VOL_MOD_NAME, json_output['disabled_modules'])

    def test_force_disable_fails_without_confirmation(self):
        '''
        Test that running "ceph mgr module force disable volumes" fails with
        EPERM when confirmation flag is not passed along.

        Also test that output of this command suggests user to pass
        --yes-i-really-mean-it.
        '''
        proc = self.run_ceph_cmd(
            f'mgr module force disable {self.VOL_MOD_NAME}',
            stderr=StringIO(), check_status=False)

        self.assertEqual(proc.returncode, errno.EPERM)

        proc_stderr = proc.stderr.getvalue()
        self.assertIn('EPERM', proc_stderr)
        # ensure that the confirmation flag was recommended
        self.assertIn(self.CONFIRM, proc_stderr)

    def test_force_disable_idempotency(self):
        '''
        Test that running "ceph mgr module force disable volumes" passes when
        volumes plugin was already force disabled.
        '''
        self.run_ceph_cmd(f'mgr module force disable {self.VOL_MOD_NAME} '
                          f'{self.CONFIRM}')
        sleep(5)

        json_output = self.get_ceph_cmd_stdout('mgr module ls --format '
                                              'json-pretty')
        json_output = json.loads(json_output)

        self.assertIn(self.VOL_MOD_NAME, json_output['always_on_modules'])
        self.assertIn(self.VOL_MOD_NAME, json_output['force_disabled_modules'])

        self.assertNotIn(self.VOL_MOD_NAME, json_output['enabled_modules'])
        self.assertNotIn(self.VOL_MOD_NAME, json_output['disabled_modules'])

        # XXX: this this test, running this command 2nd time should pass.
        self.run_ceph_cmd(f'mgr module force disable {self.VOL_MOD_NAME}')

    def test_force_disable_nonexistent_mod(self):
        '''
        Test that passing non-existent name to "ceph mgr module force disable"
        command leads to an error.
        '''
        proc = self.run_ceph_cmd(
            f'mgr module force disable abcd {self.CONFIRM}',
            check_status=False, stderr=StringIO())
        self.assertEqual(proc.returncode, errno.EINVAL)
        self.assertIn('EINVAL', proc.stderr.getvalue())

    def test_force_disable_non_alwayson_mod(self):
        '''
        Test that passing non-existent name to "ceph mgr module force disable"
        command leads to an error.
        '''
        json_output = self.get_ceph_cmd_stdout(
            'mgr module ls --format json-pretty', check_status=False,
            stderr=StringIO())
        output_dict = json.loads(json_output)
        some_non_alwayson_mod = output_dict['enabled_modules'][0]

        proc = self.run_ceph_cmd(
            f'mgr module force disable {some_non_alwayson_mod} {self.CONFIRM}',
            check_status=False, stderr=StringIO())
        self.assertEqual(proc.returncode, errno.EINVAL)
        self.assertIn('EINVAL', proc.stderr.getvalue())

    def test_enabled_by_default(self):
        '''
        Test that volumes plugin is enabled by default and is also reported as
        "always on".
        '''
        json_output = self.get_ceph_cmd_stdout('mgr module ls --format json')
        json_output = json.loads(json_output)

        self.assertIn(self.VOL_MOD_NAME, json_output['always_on_modules'])

        self.assertNotIn(self.VOL_MOD_NAME, json_output['enabled_modules'])
        self.assertNotIn(self.VOL_MOD_NAME, json_output['disabled_modules'])
        self.assertNotIn(self.VOL_MOD_NAME, json_output['force_disabled_modules'])

    def test_disable_fails(self):
        '''
        Test that running "ceph mgr module disable volumes" fails with EPERM.

        This is expected since volumes is an always-on module and therefore
        it can only be disabled using command "ceph mgr module force disable
        volumes".
        '''
        proc = self.run_ceph_cmd(f'mgr module disable {self.VOL_MOD_NAME}',
                                 stderr=StringIO(), check_status=False)
        self.assertEqual(proc.returncode, errno.EPERM)

        proc_stderr = proc.stderr.getvalue()
        self.assertIn('EPERM', proc_stderr)

    def test_enable_idempotency(self):
        '''
        Test that enabling volumes plugin when it is already enabled doesn't
        exit with non-zero return value.

        Also test that it reports plugin as already enabled.
        '''
        proc = self.run_ceph_cmd(f'mgr module enable {self.VOL_MOD_NAME}',
                                 stderr=StringIO())
        self.assertEqual(proc.returncode, 0)

        proc_stderr = proc.stderr.getvalue()
        self.assertIn('already enabled', proc_stderr)
        self.assertIn('always-on', proc_stderr)

    def test_enable_post_disabling(self):
        '''
        Test that enabling volumes plugin after (force-)disabling it works
        successfully.

        Alo test "ceph mgr module ls" output for volumes plugin afterwards.
        '''
        self.run_ceph_cmd(f'mgr module force disable {self.VOL_MOD_NAME} '
                          f'{self.CONFIRM}')
        # give bit of time for plugin to be disabled.
        sleep(5)

        self.run_ceph_cmd(f'mgr module enable {self.VOL_MOD_NAME}')
        # give bit of time for plugin to be functional again
        sleep(5)
        json_output = self.get_ceph_cmd_stdout('mgr module ls --format json')
        json_output = json.loads(json_output)
        self.assertIn(self.VOL_MOD_NAME, json_output['always_on_modules'])
        self.assertNotIn(self.VOL_MOD_NAME, json_output['enabled_modules'])
        self.assertNotIn(self.VOL_MOD_NAME, json_output['disabled_modules'])
        self.assertNotIn(self.VOL_MOD_NAME, json_output['force_disabled_modules'])

        # plugin is reported properly by "ceph mgr module ls" command, check if
        # it is also working fine.
        self.run_ceph_cmd('fs volume ls')
