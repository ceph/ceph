from logging import getLogger
from io import StringIO

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.cephfs.caps_helper import CapTester


log = getLogger(__name__)


class FsAuthorizeUpdate(CephFSTestCase):
    def setUp(self):
        super(FsAuthorizeUpdate, self).setUp()
        self.captester = CapTester()

class ClientWithCapsForNoFs(FsAuthorizeUpdate):
    """
    Contains tests for cases where "ceph fs authorize" command is run for a
    client that doesn't have any caps for any FS.
    """

    client_id = 'testuser'
    client_name = f'client.{client_id}'
    MDSS_REQUIRED = 1

    def test_update_for_client_with_no_caps(self):
        """
        Test that "ceph fs authorize" adds caps to the keyring when the
        entity already has a keyring but it contains no caps.
        """
        self.captester.write_test_files((self.mount_a,))
        self.run_cluster_cmd(f'auth add {self.client_name}')

        # testing begins here.
        TEST_PERM = 'rw'
        self.fs.authorize(self.client_id, ('/', TEST_PERM,))

        keyring = self.run_cluster_cmd(f'auth get {self.client_name}')
        mon_cap = f'caps mon = "allow r fsname={self.fs.name}"'
        osd_cap = f'caps osd = "allow rw tag cephfs data={self.fs.name}"'
        mds_cap = f'caps mds = "allow rw fsname={self.fs.name}"'
        for cap in (mon_cap, osd_cap, mds_cap):
            self.assertIn(cap, keyring)
        self.captester.run_cap_tests(TEST_PERM)


class ClientWithCapsForOneFs(FsAuthorizeUpdate):
    """
    Contains tests for the case where "ceph fs authorize" is executed for an
    a client that has caps for one FS.
    """

    client_id = 'testuser'
    client_name = f'client.{client_id}'
    CLIENTS_REQUIRED = 2
    MDSS_REQUIRED = 2

    def test_add_caps_for_second_fs(self):
        """
        Test that "ceph fs authorize" adds caps for a second FS to a keyring
        that already had caps for first FS.
        """
        self.fs1 = self.fs
        self.fs2 = self.mds_cluster.newfs('testcephfs2')
        self.mount_b.remount(cephfs_name=self.fs2.name)
        self.captester.write_test_files((self.mount_a, self.mount_b))

        self.mount_a.umount()
        self.mount_b.umount()
        self.fs1.authorize(self.client_id, ('/', 'rw',))

        # testing begins here.
        TEST_PERM = 'rw'
        self.fs2.authorize(self.client_id, ('/', TEST_PERM,))

        keyring = self.fs.mon_manager.run_cluster_cmd(
                args=f'auth get {self.client_name}', stdout=StringIO()).\
                stdout.getvalue()
        mon_cap = (f'caps mon = "allow r fsname={self.fs1.name}, '
                               f'allow r fsname={self.fs2.name}"')
        osd_cap = (f'caps osd = "allow rw tag cephfs data={self.fs1.name}, '
                               f'allow rw tag cephfs data={self.fs2.name}"')
        mds_cap = (f'caps mds = "allow rw fsname={self.fs1.name}, '
                               f'allow rw fsname={self.fs2.name}"')
        for cap in (mon_cap, osd_cap, mds_cap):
            self.assertIn(cap, keyring)

        keyring_path_a = self.mount_a.client_remote.mktemp(data=keyring)
        keyring_path_b = self.mount_b.client_remote.mktemp(data=keyring)

        self.mount_a.mount(client_id=self.client_id,
                           client_keyring_path=keyring_path_a,
                           cephfs_mntpt='/', cephfs_name=self.fs1.name)
        self.mount_b.mount(client_id=self.client_id,
                           client_keyring_path=keyring_path_b,
                           cephfs_mntpt='/', cephfs_name=self.fs2.name)
        self.captester.run_cap_tests(TEST_PERM)

    def test_change_perms(self):
        """
        Test that "ceph fs authorize" updates the caps for a FS when the caps
        for that FS were already present in that keyring.
        """
        OLD_PERM = 'rw'
        NEW_PERM = 'r'
        self.captester.write_test_files((self.mount_a,))
        self.mount_a.umount()

        self.fs.authorize(self.client_id, ('/', OLD_PERM,))
        keyring = self.fs.mon_manager.run_cluster_cmd(
            args=f'auth get {self.client_name}', stdout=StringIO()).\
            stdout.getvalue()
        keyring_path_a = self.mount_a.client_remote.mktemp(data=keyring)
        self.mount_a.mount(client_id=self.client_id,
                           client_keyring_path=keyring_path_a,
                           cephfs_mntpt='/', cephfs_name=self.fs.name)
        self.mount_a.umount()

        # testing begins here
        self.fs.authorize(self.client_id, ('/', NEW_PERM,))
        keyring = self.fs.mon_manager.run_cluster_cmd(
            args=f'auth get {self.client_name}', stdout=StringIO()).\
            stdout.getvalue()
        mon_cap = f'caps mon = "allow r fsname={self.fs.name}"'
        osd_cap = f'caps osd = "allow {NEW_PERM} tag cephfs data={self.fs.name}"'
        mds_cap = f'caps mds = "allow {NEW_PERM} fsname={self.fs.name}"'
        for cap in (mon_cap, osd_cap, mds_cap):
            self.assertIn(cap, keyring)

        keyring_path_a = self.mount_a.client_remote.mktemp(data=keyring)
        self.mount_a.mount(client_id=self.client_id,
                           client_keyring_path=keyring_path_a,
                           cephfs_mntpt='/', cephfs_name=self.fs.name)
        self.captester.run_cap_tests(NEW_PERM)

    def test_change_path_from_default_path(self):
        """
        "fs authorize" is executed first time to add the default path, the
        root path. So, there's no path mentioned in th MDS cap. Now, test
        that running "fs authorize" second time with a specific path,
        upgrades MDS cap with a new path.
        """
        PERM = 'rw'
        OLD_MNTPT = '/'
        NEW_MNTPT = '/dir1/dir2/dir3'
        # XXX: The "." before "{OLD_MNTPT}" in following line is pretty
        # crucial since otherwise these dirs will be created in "/" of
        # host/local FS.
        self.mount_a.run_shell(f'mkdir -p .{NEW_MNTPT}')
        self.captester.write_test_files((self.mount_a,), NEW_MNTPT)
        self.mount_a.umount()

        # TODO: this block is redundant
        OLD_MNTPT = '/'
        self.fs.authorize(self.client_id, (OLD_MNTPT, PERM))
        keyring = self.fs.mon_manager.run_cluster_cmd(
            args=f'auth get {self.client_name}', stdout=StringIO()).\
            stdout.getvalue()
        keyring_path_a = self.mount_a.client_remote.mktemp(data=keyring)
        self.mount_a.mount(client_id=self.client_id,
                           client_keyring_path=keyring_path_a,
                           cephfs_mntpt=OLD_MNTPT, cephfs_name=self.fs.name)

        self.mount_a.umount()

        # testing begins here
        self.fs.authorize(self.client_id, (NEW_MNTPT, PERM))
        keyring = self.fs.mon_manager.run_cluster_cmd(
            args=f'auth get {self.client_name}', stdout=StringIO()).\
            stdout.getvalue()
        mon_cap = f'caps mon = "allow r fsname={self.fs.name}"'
        osd_cap = f'caps osd = "allow rw tag cephfs data={self.fs.name}"'
        mds_cap = f'caps mds = "allow rw fsname={self.fs.name} path={NEW_MNTPT}"'
        for cap in (mon_cap, osd_cap, mds_cap):
            self.assertIn(cap, keyring)

        keyring_path_a = self.mount_a.client_remote.mktemp(data=keyring)
        self.mount_a.mount(client_id=self.client_id,
                           client_keyring_path=keyring_path_a,
                           cephfs_mntpt=NEW_MNTPT, cephfs_name=self.fs.name)
        self.captester.run_cap_tests(PERM, mntpt=NEW_MNTPT)

    def test_change_path_from_custom_path(self):
        """
        "fs authorize" is executed first time to add a custom path. Test
        that running "fs authorize" second time with a new path, replaces
        the first path by the new one in MDS cap.
        """
        PERM = 'rw'
        OLD_MNTPT = '/dir1/dir2/dir3'
        NEW_MNTPT = '/dir4/dir5/dir6'
        # XXX: The "." before "{OLD_MNTPT}" and "{NEW_MNTPT}" in following
        # two lines respectively is pretty crucial since otherwise these dirs
        # will be created in "/" of host/local FS.
        self.mount_a.run_shell(f'mkdir -p .{OLD_MNTPT}')
        self.mount_a.run_shell(f'mkdir -p .{NEW_MNTPT}')
        self.captester.write_test_files((self.mount_a,), NEW_MNTPT)
        self.mount_a.umount()

        self.fs.authorize(self.client_id, (OLD_MNTPT, PERM))
        keyring = self.fs.mon_manager.run_cluster_cmd(
            args=f'auth get {self.client_name}', stdout=StringIO()).\
            stdout.getvalue()
        keyring_path_a = self.mount_a.client_remote.mktemp(data=keyring)
        self.mount_a.mount(client_id=self.client_id,
                           client_keyring_path=keyring_path_a,
                           cephfs_mntpt=OLD_MNTPT, cephfs_name=self.fs.name)

        self.mount_a.umount()

        # testing begins here
        self.fs.authorize(self.client_id, (NEW_MNTPT, PERM))
        keyring = self.fs.mon_manager.run_cluster_cmd(
            args=f'auth get {self.client_name}', stdout=StringIO()).\
            stdout.getvalue()
        mon_cap = f'caps mon = "allow r fsname={self.fs.name}"'
        osd_cap = f'caps osd = "allow rw tag cephfs data={self.fs.name}"'
        mds_cap = f'caps mds = "allow rw fsname={self.fs.name} path={NEW_MNTPT}"'
        for cap in (mon_cap, osd_cap, mds_cap):
            self.assertIn(cap, keyring)

        keyring_path_a = self.mount_a.client_remote.mktemp(data=keyring)
        self.mount_a.mount(client_id=self.client_id,
                           client_keyring_path=keyring_path_a,
                           cephfs_mntpt=NEW_MNTPT, cephfs_name=self.fs.name)
        self.captester.run_cap_tests(PERM, mntpt=NEW_MNTPT)

    def test_change_both_perm_and_path(self):
        """
        "fs authorize" is executed first time to add a custom path. Test
        that running "fs authorize" second time with a new path, replaces
        the first path by the new one in MDS cap.
        """
        OLD_PERM = 'rw'
        NEW_PERM = 'r'
        OLD_MNTPT = '/dir1/dir2/dir3'
        NEW_MNTPT = '/dir4/dir5/dir6'
        # XXX: The "." before "{OLD_MNTPT}" and "{NEW_MNTPT}" in following
        # two lines respectively is pretty crucial since otherwise these dirs
        # will be created in "/" of host/local FS.
        self.mount_a.run_shell(f'mkdir -p .{OLD_MNTPT}')
        self.mount_a.run_shell(f'mkdir -p .{NEW_MNTPT}')
        self.captester.write_test_files((self.mount_a,), NEW_MNTPT)
        self.mount_a.umount()

        self.fs.authorize(self.client_id, (OLD_MNTPT, OLD_PERM))
        keyring = self.fs.mon_manager.run_cluster_cmd(
            args=f'auth get {self.client_name}', stdout=StringIO()).\
            stdout.getvalue()
        keyring_path_a = self.mount_a.client_remote.mktemp(data=keyring)
        self.mount_a.mount(client_id=self.client_id,
                           client_keyring_path=keyring_path_a,
                           cephfs_mntpt=OLD_MNTPT, cephfs_name=self.fs.name)
        self.mount_a.umount()

        # testing begins here
        self.fs.authorize(self.client_id, (NEW_MNTPT, NEW_PERM))
        keyring = self.fs.mon_manager.run_cluster_cmd(
            args=f'auth get {self.client_name}', stdout=StringIO()).\
            stdout.getvalue()
        mon_cap = f'caps mon = "allow r fsname={self.fs.name}"'
        osd_cap = f'caps osd = "allow {NEW_PERM} tag cephfs data={self.fs.name}"'
        mds_cap = f'caps mds = "allow {NEW_PERM} fsname={self.fs.name} path={NEW_MNTPT}"'
        for cap in (mon_cap, osd_cap, mds_cap):
            self.assertIn(cap, keyring)

        keyring_path_a = self.mount_a.client_remote.mktemp(data=keyring)
        self.mount_a.mount(client_id=self.client_id,
                           client_keyring_path=keyring_path_a,
                           cephfs_mntpt=NEW_MNTPT, cephfs_name=self.fs.name)
        self.captester.run_cap_tests(NEW_PERM, mntpt=NEW_MNTPT)

    ##### Idempotency test

    def test_caps_passed_same_as_current_caps(self):
        """
        Test that "ceph fs authorize" exits with the keyring on stdout and the
        expected error message on stderr when caps supplied to the subcommand
        are already present in the entity's keyring.
        """
        self.fs.authorize(self.client_id, ('/', 'rw')).strip()
        keyring1 = self.fs.mon_manager.get_keyring(f'{self.client_id}')

        # testing begins here.
        proc = self.fs.mon_manager.run_cluster_cmd(
            args=f'fs authorize {self.fs.name} {self.client_name} / rw',
            stdout=StringIO(), stderr=StringIO())
        errmsg = proc.stderr.getvalue()
        keyring2 = self.fs.mon_manager.get_keyring(f'{self.client_id}')

        self.assertIn(keyring1, keyring2)
        self.assertIn(f'{self.client_name} already has caps that are same as '
                       'those supplied', errmsg)


# TODO: add more tests to check change of path and perms in case of caps
# for two FSs
# TODO: add similar tests to check change of path and perms in case of caps
# for 3 FSs? C++ code has 3 4 cases - current MDS cap has only FS name
# (first case), it has multiple name and new FS names matches first cap
# (second case), or last cap (third case) or some cap in middle (fourth case)
# BETTER TO UNDERTAKE THIS AFTER REGEX CODE IS REPLACED BY PARSER CODE.
class ClientWithCapsForTwoFss(FsAuthorizeUpdate):
    """
    Contains tests for the case where "ceph fs authorize" is run against a
    client containing caps for 2 FSs.
    """

    client_id = 'testuser'
    client_name = f'client.{client_id}'
    CLIENTS_REQUIRED = 2
    MDSS_REQUIRED = 2

    # TODO: definitely needs another review and some modifications.
    def test_change_perms(self):
        self.fs1 = self.fs
        self.fs2 = self.mds_cluster.newfs('testcephfs2')
        self.mount_b.remount(cephfs_name=self.fs2.name)
        # TODO: needs update?
        OLD_PERM = 'rw'
        NEW_PERM = 'r'
        self.captester_a = self.captester
        del self.captester # so there's no chance of bug due to typo ever.
        self.captester_a.write_test_files((self.mount_a,))
        self.captester_b = CapTester()
        self.captester_b.write_test_files((self.mount_b,))
        self.mount_a.umount()
        self.mount_b.umount()
        self.fs1.authorize(self.client_id, ('/', OLD_PERM,))
        self.fs2.authorize(self.client_id, ('/', OLD_PERM,))


        # testing begins here.
        self.fs1.authorize(self.client_id, ('/', NEW_PERM,))

        keyring = self.fs1.mon_manager.run_cluster_cmd(
            args=f'auth get {self.client_name}', stdout=StringIO()).\
            stdout.getvalue()
        mon_cap = (f'caps mon = "allow r fsname={self.fs1.name}, '
                               f'allow r fsname={self.fs2.name}"')
        osd_cap = (f'caps osd = "allow {NEW_PERM} tag cephfs data={self.fs1.name}, '
                               f'allow {OLD_PERM} tag cephfs data={self.fs2.name}"')
        mds_cap = (f'caps mds = "allow {NEW_PERM} fsname={self.fs1.name}, '
                               f'allow {OLD_PERM} fsname={self.fs2.name}"')
        for cap in (mon_cap, osd_cap, mds_cap):
            self.assertIn(cap, keyring)

        keyring_path_a = self.mount_a.client_remote.mktemp(data=keyring)
        keyring_path_b = self.mount_b.client_remote.mktemp(data=keyring)
        self.mount_a.remount(client_id=self.client_id,
                             client_keyring_path=keyring_path_a,
                             cephfs_mntpt='/', cephfs_name=self.fs1.name)
        self.mount_b.remount(client_id=self.client_id,
                             client_keyring_path=keyring_path_b,
                             cephfs_mntpt='/', cephfs_name=self.fs2.name)
        self.captester_a.run_cap_tests(NEW_PERM)


        self.fs2.authorize(self.client_id, ('/', NEW_PERM,))

        keyring = self.fs1.mon_manager.run_cluster_cmd(
            args=f'auth get {self.client_name}', stdout=StringIO()).\
            stdout.getvalue()
        mon_cap = (f'caps mon = "allow r fsname={self.fs1.name}, '
                               f'allow r fsname={self.fs2.name}"')
        osd_cap = (f'caps osd = "allow {NEW_PERM} tag cephfs data={self.fs1.name}, '
                               f'allow {NEW_PERM} tag cephfs data={self.fs2.name}"')
        mds_cap = (f'caps mds = "allow {NEW_PERM} fsname={self.fs1.name}, '
                               f'allow {NEW_PERM} fsname={self.fs2.name}"')
        for cap in (mon_cap, osd_cap, mds_cap):
            self.assertIn(cap, keyring)

        keyring_path_a = self.mount_a.client_remote.mktemp(data=keyring)
        keyring_path_b = self.mount_b.client_remote.mktemp(data=keyring)
        self.mount_a.remount(client_id=self.client_id,
                           client_keyring_path=keyring_path_a,
                           cephfs_mntpt='/', cephfs_name=self.fs1.name)
        self.mount_b.remount(client_id=self.client_id,
                           client_keyring_path=keyring_path_b,
                           cephfs_mntpt='/', cephfs_name=self.fs2.name)
        self.captester_b.run_cap_tests(NEW_PERM)


    # XXX: is this redundant given same test in TestUpdateCapsForOneFs?
    # TODO:
    def _test_change_paths(self):
        pass

    ##### Idempotency tests

    def test_fs_one_supplied_again(self):
        """
        For a client holding caps for 2 FSs, pass client caps for first of the
        two FSs to "fs authorize" to test idempotency.
        """
        self.fs1 = self.fs
        self.fs2 = self.mds_cluster.newfs('testcephfs2')
        self.mount_b.remount(cephfs_name=self.fs2.name)
        self.mount_a.umount()
        self.mount_b.umount()
        self.fs1.authorize(self.client_id, ('/', 'rw',))
        self.fs2.authorize(self.client_id, ('/', 'rw',))
        keyring1 = self.fs.mon_manager.get_keyring(f'{self.client_id}')

        # actual testing begings here
        proc = self.fs1.mon_manager.run_cluster_cmd(
            args=f'fs authorize {self.fs1.name} {self.client_name} / rw',
            stdout=StringIO(), stderr=StringIO())
        errmsg = proc.stderr.getvalue()
        keyring2 = self.fs.mon_manager.get_keyring(f'{self.client_id}')

        self.assertIn(keyring1, keyring2)
        self.assertIn(f'{self.client_name} already has caps that are same as '
                       'those supplied', errmsg)

    def test_fs_two_supplied_again(self):
        self.fs1 = self.fs
        self.fs2 = self.mds_cluster.newfs('testcephfs2')
        self.mount_b.remount(cephfs_name=self.fs2.name)
        self.mount_a.umount()
        self.mount_b.umount()
        self.fs1.authorize(self.client_id, ('/', 'rw',))
        self.fs2.authorize(self.client_id, ('/', 'rw',))
        keyring1 = self.fs.mon_manager.get_keyring(f'{self.client_id}')

        proc = self.fs2.mon_manager.run_cluster_cmd(
            args=f'fs authorize {self.fs2.name} {self.client_name} / rw',
            stdout=StringIO(), stderr=StringIO())
        errmsg = proc.stderr.getvalue()
        keyring2 = self.fs.mon_manager.get_keyring(f'{self.client_id}')

        self.assertIn(keyring1, keyring2)
        self.assertIn(f'{self.client_name} already has caps that are same as '
                       'those supplied', errmsg)


# TODO: vstart_runner run classes not starting with 'Test' as well? Is this
# also how teuthology's test runner behaves? IMHO, this is not acceptible.
# TODO: the idea of following class is to be able to group tests in a such
# away that subgroups are in different classes so that they can be run
# individually if and when needed and yet a single class can be used by the
# test runner to run all related test groups together.
#class TestFsAuthorizeUpdate(ClientWithCapsForNoFS, ClientWithCapsForOneFs,
#                            ClientWithCapsForTwoFSs):
#    pass
