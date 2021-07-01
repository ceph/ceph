import errno
import json
import uuid
from io import StringIO
from os.path import join as os_path_join

from teuthology.orchestra.run import CommandFailedError, Raw

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.cephfs.filesystem import FileLayout, FSMissing
from tasks.cephfs.fuse_mount import FuseMount
from tasks.cephfs.caps_helper import CapsHelper


class TestAdminCommands(CephFSTestCase):
    """
    Tests for administration command.
    """

    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 1

    def check_pool_application_metadata_key_value(self, pool, app, key, value):
        output = self.fs.mon_manager.raw_cluster_cmd(
            'osd', 'pool', 'application', 'get', pool, app, key)
        self.assertEqual(str(output.strip()), value)

    def setup_ec_pools(self, n, metadata=True, overwrites=True):
        if metadata:
            self.fs.mon_manager.raw_cluster_cmd('osd', 'pool', 'create', n+"-meta", "8")
        cmd = ['osd', 'erasure-code-profile', 'set', n+"-profile", "m=2", "k=2", "crush-failure-domain=osd"]
        self.fs.mon_manager.raw_cluster_cmd(*cmd)
        self.fs.mon_manager.raw_cluster_cmd('osd', 'pool', 'create', n+"-data", "8", "erasure", n+"-profile")
        if overwrites:
            self.fs.mon_manager.raw_cluster_cmd('osd', 'pool', 'set', n+"-data", 'allow_ec_overwrites', 'true')


class TestFsStatus(TestAdminCommands):
    """
    Test "ceph fs status subcommand.
    """

    def test_fs_status(self):
        """
        That `ceph fs status` command functions.
        """

        s = self.fs.mon_manager.raw_cluster_cmd("fs", "status")
        self.assertTrue("active" in s)

        mdsmap = json.loads(self.fs.mon_manager.raw_cluster_cmd("fs", "status", "--format=json-pretty"))["mdsmap"]
        self.assertEqual(mdsmap[0]["state"], "active")

        mdsmap = json.loads(self.fs.mon_manager.raw_cluster_cmd("fs", "status", "--format=json"))["mdsmap"]
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
        mon_cmd = self.fs.mon_manager.raw_cluster_cmd
        mon_cmd('osd', 'pool', 'create', pool_name, str(self.fs.pgs_per_fs_pool))
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

    def test_fsnames_can_only_by_goodchars(self):
        n = 'test_fsnames_can_only_by_goodchars'
        metapoolname, datapoolname = n+'-testmetapool', n+'-testdatapool'
        badname = n+'badname@#'

        self.fs.mon_manager.raw_cluster_cmd('osd', 'pool', 'create',
                                            n+metapoolname)
        self.fs.mon_manager.raw_cluster_cmd('osd', 'pool', 'create',
                                            n+datapoolname)

        # test that fsname not with "goodchars" fails
        args = ['fs', 'new', badname, metapoolname, datapoolname]
        proc = self.fs.mon_manager.run_cluster_cmd(args=args,stderr=StringIO(),
                                                   check_status=False)
        self.assertIn('invalid chars', proc.stderr.getvalue().lower())

        self.fs.mon_manager.raw_cluster_cmd('osd', 'pool', 'rm', metapoolname,
                                            metapoolname,
                                            '--yes-i-really-really-mean-it-not-faking')
        self.fs.mon_manager.raw_cluster_cmd('osd', 'pool', 'rm', datapoolname,
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
            self.fs.mon_manager.raw_cluster_cmd('fs', 'new', n, n+"-meta", n+"-data")
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
        self.fs.mon_manager.raw_cluster_cmd('fs', 'new', n, n+"-meta", n+"-data", "--force")

    def test_new_default_ec_no_overwrite(self):
        """
        That a new file system fails with an EC default data pool without overwrite.
        """

        self.mount_a.umount_wait(require_clean=True)
        self.mds_cluster.delete_all_filesystems()
        n = "test_new_default_ec_no_overwrite"
        self.setup_ec_pools(n, overwrites=False)
        try:
            self.fs.mon_manager.raw_cluster_cmd('fs', 'new', n, n+"-meta", n+"-data")
        except CommandFailedError as e:
            if e.exitstatus == 22:
                pass
            else:
                raise
        else:
            raise RuntimeError("expected failure")
        # and even with --force !
        try:
            self.fs.mon_manager.raw_cluster_cmd('fs', 'new', n, n+"-meta", n+"-data", "--force")
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
        mon_cmd = self.fs.mon_manager.raw_cluster_cmd
        for p in pool_names:
            mon_cmd('osd', 'pool', 'create', p, str(self.fs.pgs_per_fs_pool))
            mon_cmd('osd', 'pool', 'application', 'enable', p, 'cephfs')
        mon_cmd('fs', 'new', fs_name, pool_names[0], pool_names[1])
        for i in range(2):
            self.check_pool_application_metadata_key_value(
                pool_names[i], 'cephfs', keys[i], fs_name)


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

        self.run_cluster_cmd(f'fs rename {orig_fs_name} {new_fs_name} --yes-i-really-mean-it')

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
        self.run_cluster_cmd(f'auth rm client.{client_id}')

    def test_fs_rename_idempotency(self):
        """
        That the file system rename operation is idempotent.
        """
        # Renaming the file system breaks this mount as the client uses
        # file system specific authorization.
        self.mount_a.umount_wait(require_clean=True)
        orig_fs_name = self.fs.name
        new_fs_name = 'new_cephfs'

        self.run_cluster_cmd(f'fs rename {orig_fs_name} {new_fs_name} --yes-i-really-mean-it')
        self.run_cluster_cmd(f'fs rename {orig_fs_name} {new_fs_name} --yes-i-really-mean-it')

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
        self.run_cluster_cmd(f'fs rename {orig_fs_name} {new_fs_name} --yes-i-really-mean-it')

        try:
            self.run_cluster_cmd(f"fs new {orig_fs_name} {metadata_pool} {data_pool}")
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EINVAL,
                "invalid error code on creating a new file system with old "
                "name and existing pools.")
        else:
            self.fail("expected creating new file system with old name and "
                      "existing pools to fail.")

        try:
            self.run_cluster_cmd(f"fs new {orig_fs_name} {metadata_pool} {data_pool} --force")
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EEXIST,
                "invalid error code on creating a new file system with old "
                "name, existing pools and --force flag.")
        else:
            self.fail("expected creating new file system with old name, "
                      "existing pools, and --force flag to fail.")

        try:
            self.run_cluster_cmd(f"fs new {orig_fs_name} {metadata_pool} {data_pool} "
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
            self.run_cluster_cmd(f"fs rename {self.fs.name} new_fs")
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
            self.run_cluster_cmd("fs rename non_existent_fs new_fs --yes-i-really-mean-it")
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
            self.run_cluster_cmd(f"fs rename {self.fs.name} {self.fs2.name} --yes-i-really-mean-it")
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

        self.run_cluster_cmd(f'fs mirror enable {orig_fs_name}')
        try:
            self.run_cluster_cmd(f'fs rename {orig_fs_name} {new_fs_name} --yes-i-really-mean-it')
        except CommandFailedError as ce:
            self.assertEqual(ce.exitstatus, errno.EPERM, "invalid error code on renaming a mirrored file system")
        else:
            self.fail("expected renaming of a mirrored file system to fail")
        self.run_cluster_cmd(f'fs mirror disable {orig_fs_name}')


class TestRequiredClientFeatures(CephFSTestCase):
    CLIENTS_REQUIRED = 0
    MDSS_REQUIRED = 1

    def test_required_client_features(self):
        """
        That `ceph fs required_client_features` command functions.
        """

        def is_required(index):
            out = self.fs.mon_manager.raw_cluster_cmd('fs', 'get', self.fs.name, '--format=json-pretty')
            features = json.loads(out)['mdsmap']['required_client_features']
            if "feature_{0}".format(index) in features:
                return True;
            return False;

        features = json.loads(self.fs.mon_manager.raw_cluster_cmd('fs', 'feature', 'ls', '--format=json-pretty'))
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
            s = self.fs.mon_manager.raw_cluster_cmd("config", "show", "mds."+n)
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
        self.fs.mon_manager.raw_cluster_cmd("fs", "mirror", "enable", fs_name)

    def _disable_mirroring(self, fs_name):
        self.fs.mon_manager.raw_cluster_cmd("fs", "mirror", "disable", fs_name)

    def _add_peer(self, fs_name, peer_spec, remote_fs_name):
        peer_uuid = str(uuid.uuid4())
        self.fs.mon_manager.raw_cluster_cmd("fs", "mirror", "peer_add", fs_name, peer_uuid, peer_spec, remote_fs_name)

    def _remove_peer(self, fs_name, peer_uuid):
        self.fs.mon_manager.raw_cluster_cmd("fs", "mirror", "peer_remove", fs_name, peer_uuid)

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
        self.run_cluster_cmd(f'auth caps client.{self.mount_a.client_id} '
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
        self.run_cluster_cmd(f'auth rm {self.client_name}')

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
