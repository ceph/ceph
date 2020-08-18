import json
import errno
import logging

from io import StringIO

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from teuthology.exceptions import CommandFailedError
from teuthology.contextutil import safe_while

log = logging.getLogger(__name__)

class TestMirroring(CephFSTestCase):
    MDSS_REQUIRED = 5
    REQUIRE_BACKUP_FILESYSTEM = True

    MODULE_NAME = "mirroring"

    def setUp(self):
        super(TestMirroring, self).setUp()
        self.primary_fs_name = self.fs.name
        self.secondary_fs_name = self.backup_fs.name
        self.enable_mirroring_module()

    def tearDown(self):
        self.disable_mirroring_module()
        super(TestMirroring, self).tearDown()

    def enable_mirroring_module(self):
        self.mgr_cluster.mon_manager.raw_cluster_cmd("mgr", "module", "enable", TestMirroring.MODULE_NAME)

    def disable_mirroring_module(self):
        self.mgr_cluster.mon_manager.raw_cluster_cmd("mgr", "module", "disable", TestMirroring.MODULE_NAME)

    def enable_mirroring(self, fs_name):
        self.mgr_cluster.mon_manager.raw_cluster_cmd("fs", "snapshot", "mirror", "enable", fs_name)
        # verify via asok
        with safe_while(sleep=1, tries=30, action='wait for mirror enable') as proceed:
            while proceed():
                try:
                    res = self.mirror_daemon_command(f'mirror status for fs: {fs_name}',
                                                     'fs', 'mirror', 'status', fs_name)
                except CommandFailedError as ce:
                    pass
                else:
                    self.assertTrue(res['peers'] == {})
                    self.assertTrue(res['snap_dirs']['dir_count'] == 0)
                    return True

    def disable_mirroring(self, fs_name):
        self.mgr_cluster.mon_manager.raw_cluster_cmd("fs", "snapshot", "mirror", "disable", fs_name)
        # verify via asok
        with safe_while(sleep=1, tries=30, action='wait for mirror disable') as proceed:
            while proceed():
                try:
                    self.mirror_daemon_command(f'mirror status for fs: {fs_name}',
                                               'fs', 'mirror', 'status', fs_name)
                except CommandFailedError as ce:
                    return True

    def peer_add(self, fs_name, peer_spec, remote_fs_name=None):
        if remote_fs_name:
            self.mgr_cluster.mon_manager.raw_cluster_cmd("fs", "snapshot", "mirror", "peer_add", fs_name, peer_spec, remote_fs_name)
        else:
            self.mgr_cluster.mon_manager.raw_cluster_cmd("fs", "snapshot", "mirror", "peer_add", fs_name, peer_spec)

        # verify via asok
        with safe_while(sleep=1, tries=30, action='wait for peer add') as proceed:
            while proceed():
                peer_uuid = self.get_peer_uuid(peer_spec)
                if peer_uuid is None:
                    return
                try:
                    res = self.mirror_daemon_command(f'mirror status for fs: {fs_name}',
                                                     'fs', 'mirror', 'status', fs_name)
                except CommandFailedError as e:
                    pass
                else:
                    if peer_uuid in res['peers']:
                        client_name = res['peers'][peer_uuid]['remote']['client_name']
                        cluster_name = res['peers'][peer_uuid]['remote']['cluster_name']
                        self.assertTrue(peer_spec == f'{client_name}@{cluster_name}')
                        if remote_fs_name:
                            self.assertTrue(self.secondary_fs_name == res['peers'][peer_uuid]['remote']['fs_name'])
                        else:
                            self.assertTrue(self.fs_name == res['peers'][peer_uuid]['remote']['fs_name'])
                        return True

    def peer_remove(self, fs_name, peer_spec):
        peer_uuid = self.get_peer_uuid(peer_spec)
        self.mgr_cluster.mon_manager.raw_cluster_cmd("fs", "snapshot", "mirror", "peer_remove", fs_name, peer_uuid)

        # verify via asok
        with safe_while(sleep=1, tries=30, action='wait for peer remove') as proceed:
            while proceed():
                try:
                    res = self.mirror_daemon_command(f'mirror status for fs: {fs_name}',
                                                     'fs', 'mirror', 'status', fs_name)
                except CommandFailedError as e:
                    pass
                else:
                    if res['peers'] == {} and res['snap_dirs']['dir_count'] == 0:
                        return True

    def add_directory(self, fs_name, dir_name):
        # get initial dir count
        res = self.mirror_daemon_command(f'mirror status for fs: {fs_name}',
                                         'fs', 'mirror', 'status', fs_name)
        dir_count = res['snap_dirs']['dir_count']
        log.debug(f'initial dir_count={dir_count}')

        self.mgr_cluster.mon_manager.raw_cluster_cmd("fs", "snapshot", "mirror", "add", fs_name, dir_name)

        # verify via asok
        with safe_while(sleep=1, tries=30, action='wait for directory add') as proceed:
            while proceed():
                try:
                    res = self.mirror_daemon_command(f'mirror status for fs: {fs_name}',
                                                     'fs', 'mirror', 'status', fs_name)
                except CommandFailedError as e:
                    pass
                else:
                    new_dir_count = res['snap_dirs']['dir_count']
                    log.debug(f'new dir_count={new_dir_count}')
                    if new_dir_count > dir_count:
                        return True

    def remove_directory(self, fs_name, dir_name):
        # get initial dir count
        res = self.mirror_daemon_command(f'mirror status for fs: {fs_name}',
                                         'fs', 'mirror', 'status', fs_name)
        dir_count = res['snap_dirs']['dir_count']
        log.debug(f'initial dir_count={dir_count}')

        self.mgr_cluster.mon_manager.raw_cluster_cmd("fs", "snapshot", "mirror", "remove", fs_name, dir_name)

        # verify via asok
        with safe_while(sleep=1, tries=30, action='wait for directory add') as proceed:
            while proceed():
                try:
                    res = self.mirror_daemon_command(f'mirror status for fs: {fs_name}',
                                                     'fs', 'mirror', 'status', fs_name)
                except CommandFailedError as e:
                    pass
                else:
                    new_dir_count = res['snap_dirs']['dir_count']
                    log.debug(f'new dir_count={new_dir_count}')
                    if new_dir_count < dir_count:
                        return True

    def get_peer_uuid(self, peer_spec):
        status = self.fs.status()
        fs_map = status.get_fsmap_byname(self.primary_fs_name)
        peers = fs_map['mirror_info']['peers']
        for peer_uuid, mirror_info in peers.items():
            client_name = mirror_info['remote']['client_name']
            cluster_name = mirror_info['remote']['cluster_name']
            remote_peer_spec = f'{client_name}@{cluster_name}'
            if peer_spec == remote_peer_spec:
                return peer_uuid
        return None

    def get_daemon_admin_socket(self):
        """overloaded by teuthology override (fs/mirror/clients/mirror.yaml)"""
        return "/var/run/ceph/cephfs-mirror.asok"

    def mirror_daemon_command(self, cmd_label, *args):
        asok_path = self.get_daemon_admin_socket()
        try:
            # use mount_a's remote to execute command
            p = self.mount_a.client_remote.run(args=
                     ['ceph', '--admin-daemon', asok_path] + list(args),
                     stdout=StringIO(), stderr=StringIO(), timeout=30,
                     check_status=True, label=cmd_label)
            p.wait()
        except CommandFailedError:
            log.error(f'mirror daemon command with label {cmd_label} failed')
            raise
        res = p.stdout.getvalue().strip()
        log.debug(f'command return={res}')
        return json.loads(res)

    def test_basic_mirror_commands(self):
        self.enable_mirroring(self.primary_fs_name)
        self.disable_mirroring(self.primary_fs_name)

    def test_mirror_peer_commands(self):
        self.enable_mirroring(self.primary_fs_name)

        # add peer
        self.peer_add(self.primary_fs_name, "client.mirror_remote@ceph", self.secondary_fs_name)
        # remove peer
        self.peer_remove(self.primary_fs_name, "client.mirror_remote@ceph")

        self.disable_mirroring(self.primary_fs_name)

    def test_mirror_disable_with_peer(self):
        self.enable_mirroring(self.primary_fs_name)

        # add peer
        self.peer_add(self.primary_fs_name, "client.mirror_remote@ceph", self.secondary_fs_name)

        self.disable_mirroring(self.primary_fs_name)

    def test_matching_peer(self):
        self.enable_mirroring(self.primary_fs_name)

        try:
            self.peer_add(self.primary_fs_name, "client.mirror_remote@ceph")
        except CommandFailedError as ce:
            if ce.exitstatus != errno.EINVAL:
                raise RuntimeError('invalid errno when adding a matching remote peer')
        else:
            raise RuntimeError('adding a peer matching local spec should fail')

        # verify via asok -- nothing should get added
        res = self.mirror_daemon_command(f'mirror status for fs: {self.primary_fs_name}',
                                         'fs', 'mirror', 'status', self.primary_fs_name)
        self.assertTrue(res['peers'] == {})

        # and explicitly specifying the spec (via filesystem name) should fail too
        try:
            self.peer_add(self.primary_fs_name, "client.mirror_remote@ceph", self.primary_fs_name)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.EINVAL:
                raise RuntimeError('invalid errno when adding a matching remote peer')
        else:
            raise RuntimeError('adding a peer matching local spec should fail')

        # verify via asok -- nothing should get added
        res = self.mirror_daemon_command(f'mirror status for fs: {self.primary_fs_name}',
                                         'fs', 'mirror', 'status', self.primary_fs_name)
        self.assertTrue(res['peers'] == {})

        self.disable_mirroring(self.primary_fs_name)

    def test_mirror_peer_add_existing(self):
        self.enable_mirroring(self.primary_fs_name)

        # add peer
        self.peer_add(self.primary_fs_name, "client.mirror_remote@ceph", self.secondary_fs_name)

        try:
            self.peer_add(self.primary_fs_name, "client.mirror_remote@ceph", self.secondary_fs_name)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.EEXIST:
                raise RuntimeError(-errno.EINVAL, 'incorrect error code when adding an existing peer')
        else:
            raise RuntimeError(-errno.EINVAL, 'expected peer_add to fail when adding an existing peer')

        # remove peer
        self.peer_remove(self.primary_fs_name, "client.mirror_remote@ceph")

        self.disable_mirroring(self.primary_fs_name)

    def test_peer_commands_with_mirroring_disabled(self):
        # try adding peer when mirroring is not enabled
        try:
            self.peer_add(self.primary_fs_name, "client.mirror_remote@ceph", self.secondary_fs_name)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.EINVAL:
                raise RuntimeError(-errno.EINVAL, 'incorrect error code when adding a peer')
        else:
            raise RuntimeError(-errno.EINVAL, 'expected peer_add to fail')

        # try removing peer
        try:
            self.mgr_cluster.mon_manager.raw_cluster_cmd("fs", "snapshot", "mirror", "peer_remove", self.primary_fs_name, 'dummy-uuid')
        except CommandFailedError as ce:
            if ce.exitstatus != errno.EINVAL:
                raise RuntimeError(-errno.EINVAL, 'incorrect error code when removing a peer')
        else:
            raise RuntimeError(-errno.EINVAL, 'expected peer_remove to fail')

    def test_add_directory_with_mirroring_disabled(self):
        # try adding a directory when mirroring is not enabled
        try:
            self.add_directory(self.primary_fs_name, "/d1")
        except CommandFailedError as ce:
            if ce.exitstatus != errno.EINVAL:
                raise RuntimeError(-errno.EINVAL, 'incorrect error code when adding a directory')
        else:
            raise RuntimeError(-errno.EINVAL, 'expected directory add to fail')

    def test_directory_commands(self):
        self.mount_a.run_shell(["mkdir", "d1"])
        self.enable_mirroring(self.primary_fs_name)
        self.add_directory(self.primary_fs_name, '/d1')
        try:
            self.add_directory(self.primary_fs_name, '/d1')
        except CommandFailedError as ce:
            if ce.exitstatus != errno.EEXIST:
                raise RuntimeError(-errno.EINVAL, 'incorrect error code when re-adding a directory')
        else:
            raise RuntimeError(-errno.EINVAL, 'expected directory add to fail')
        self.remove_directory(self.primary_fs_name, '/d1')
        try:
            self.remove_directory(self.primary_fs_name, '/d1')
        except CommandFailedError as ce:
            if ce.exitstatus not in (errno.ENOENT, errno.EINVAL):
                raise RuntimeError(-errno.EINVAL, 'incorrect error code when re-deleting a directory')
        else:
            raise RuntimeError(-errno.EINVAL, 'expected directory removal to fail')
        self.disable_mirroring(self.primary_fs_name)
        self.mount_a.run_shell(["rmdir", "d1"])

    def test_add_non_existing_directory(self):
        self.enable_mirroring(self.primary_fs_name)
        try:
            self.add_directory(self.primary_fs_name, '/d1')
        except CommandFailedError as ce:
            if ce.exitstatus != errno.ENOENT:
                raise RuntimeError(-errno.EINVAL, 'incorrect error code when adding a non-existing directory')
        else:
            raise RuntimeError(-errno.EINVAL, 'expected directory add to fail')
        self.disable_mirroring(self.primary_fs_name)

    def test_add_relative_directory_path(self):
        self.enable_mirroring(self.primary_fs_name)
        try:
            self.add_directory(self.primary_fs_name, './d1')
        except CommandFailedError as ce:
            if ce.exitstatus != errno.EINVAL:
                raise RuntimeError(-errno.EINVAL, 'incorrect error code when adding a relative path dir')
        else:
            raise RuntimeError(-errno.EINVAL, 'expected directory add to fail')
        self.disable_mirroring(self.primary_fs_name)

    def test_add_non_directory(self):
        self.mount_a.run_shell(["touch", "test"])
        self.enable_mirroring(self.primary_fs_name)
        try:
            self.add_directory(self.primary_fs_name, '/test')
        except CommandFailedError as ce:
            if ce.exitstatus != errno.EINVAL:
                raise RuntimeError(-errno.EINVAL, 'incorrect error code when adding a non directory')
        else:
            raise RuntimeError(-errno.EINVAL, 'expected directory add to fail')
        self.disable_mirroring(self.primary_fs_name)
        self.mount_a.run_shell(["rm", "test"])

    def test_add_directory_path_normalization(self):
        self.mount_a.run_shell(["mkdir", "-p", "d1/d2/d3"])
        self.enable_mirroring(self.primary_fs_name)
        self.add_directory(self.primary_fs_name, '/d1/d2/d3')
        def check_add_command_failure(dir_path):
            try:
                self.add_directory(self.primary_fs_name, dir_path)
            except CommandFailedError as ce:
                if ce.exitstatus != errno.EEXIST:
                    raise RuntimeError(-errno.EINVAL, 'incorrect error code when re-adding a directory')
            else:
                raise RuntimeError(-errno.EINVAL, 'expected directory add to fail')

        # everything points for /d1/d2/d3
        check_add_command_failure('/d1/d2/././././././d3')
        check_add_command_failure('/d1/d2/././././././d3//////')
        check_add_command_failure('/d1/d2/../d2/././././d3')
        check_add_command_failure('/././././d1/./././d2/./././d3//////')
        check_add_command_failure('/./d1/./d2/./d3/../../../d1/d2/d3')

        self.disable_mirroring(self.primary_fs_name)
        self.mount_a.run_shell(["rm", "-rf", "d1"])

    def test_add_ancestor_and_child_directory(self):
        self.mount_a.run_shell(["mkdir", "-p", "d1/d2/d3"])
        self.mount_a.run_shell(["mkdir", "-p", "d1/d4"])
        self.enable_mirroring(self.primary_fs_name)
        self.add_directory(self.primary_fs_name, '/d1/d2/')
        def check_add_command_failure(dir_path):
            try:
                self.add_directory(self.primary_fs_name, dir_path)
            except CommandFailedError as ce:
                if ce.exitstatus != errno.EINVAL:
                    raise RuntimeError(-errno.EINVAL, 'incorrect error code when adding a directory')
            else:
                raise RuntimeError(-errno.EINVAL, 'expected directory add to fail')

        # cannot add ancestors or a subtree for an existing directory
        check_add_command_failure('/')
        check_add_command_failure('/d1')
        check_add_command_failure('/d1/d2/d3')

        # obviously one can add a non-ancestor or non-subtree
        self.add_directory(self.primary_fs_name, '/d1/d4/')

        self.disable_mirroring(self.primary_fs_name)
        self.mount_a.run_shell(["rm", "-rf", "d1"])
