import json
import errno
import logging
import time
import functools

from io import StringIO

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from teuthology.exceptions import CommandFailedError
from teuthology.contextutil import safe_while

log = logging.getLogger(__name__)


# Exceptions to retry in test assertions
RETRY_EXCEPTIONS = (AssertionError, KeyError, IndexError, CommandFailedError)
# retry decorator
def retry_assert(timeout=60, interval=1):
    """
    Retry a test helper until assertions inside it pass or timeout expires.
    Prints retry count on each failure.
    """
    tries = int(timeout/interval)

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exc = None
            attempt = 1

            with safe_while(sleep=interval, tries=tries, action=f"retry {func.__name__}") as proceed:
                while proceed():
                    try:
                        return func(*args, **kwargs)
                    except RETRY_EXCEPTIONS as e:
                        last_exc = e
                        log.debug(
                            f"[retry_assert] {func.__name__}: "
                            f"attempt {attempt} failed ({type(e).__name__}), retrying..."
                        )
                        attempt += 1
            # Final failure
            if last_exc is not None and hasattr(last_exc, "res"):
                log.error("\n--- Last peer status (res) ---")
                log.error(last_exc.res)

            raise AssertionError(
                f"{func.__name__} did not succeed within {timeout}s "
                f"after {attempt - 1} attempts"
            ) from last_exc

        return wrapper
    return decorator


class TestMirroring(CephFSTestCase):
    MDSS_REQUIRED = 5
    CLIENTS_REQUIRED = 2
    REQUIRE_BACKUP_FILESYSTEM = True

    MODULE_NAME = "mirroring"

    PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR = "cephfs_mirror"
    PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR_FS = "cephfs_mirror_mirrored_filesystems"

    def setUp(self):
        super(TestMirroring, self).setUp()
        self.primary_fs_name = self.fs.name
        self.primary_fs_id = self.fs.id
        self.secondary_fs_name = self.backup_fs.name
        self.secondary_fs_id = self.backup_fs.id
        self.enable_mirroring_module()
        self.config_set('client.mirror', 'cephfs_mirror_directory_scan_interval', 1)

    def tearDown(self):
        self.disable_mirroring_module()
        super(TestMirroring, self).tearDown()

    def enable_mirroring_module(self):
        self.run_ceph_cmd("mgr", "module", "enable", TestMirroring.MODULE_NAME)

    def disable_mirroring_module(self):
        self.run_ceph_cmd("mgr", "module", "disable", TestMirroring.MODULE_NAME)

    def enable_mirroring(self, fs_name, fs_id):
        res = self.mirror_daemon_command(f'counter dump for fs: {fs_name}', 'counter', 'dump')
        vbefore = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR][0]

        self.run_ceph_cmd("fs", "snapshot", "mirror", "enable", fs_name)
        time.sleep(10)
        # verify via asok
        res = self.mirror_daemon_command(f'mirror status for fs: {fs_name}',
                                         'fs', 'mirror', 'status', f'{fs_name}@{fs_id}')
        self.assertTrue(res['peers'] == {})
        self.assertTrue(res['snap_dirs']['dir_count'] == 0)

        # verify labelled perf counter
        res = self.mirror_daemon_command(f'counter dump for fs: {fs_name}', 'counter', 'dump')
        self.assertEqual(res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR_FS][0]["labels"]["filesystem"],
                         fs_name)
        vafter = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR][0]

        self.assertGreater(vafter["counters"]["mirrored_filesystems"],
                           vbefore["counters"]["mirrored_filesystems"])

    def disable_mirroring(self, fs_name, fs_id):
        res = self.mirror_daemon_command(f'counter dump for fs: {fs_name}', 'counter', 'dump')
        vbefore = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR][0]

        self.run_ceph_cmd("fs", "snapshot", "mirror", "disable", fs_name)
        time.sleep(10)
        # verify via asok
        try:
            self.mirror_daemon_command(f'mirror status for fs: {fs_name}',
                                       'fs', 'mirror', 'status', f'{fs_name}@{fs_id}')
        except CommandFailedError:
            pass
        else:
            raise RuntimeError('expected admin socket to be unavailable')

        # verify labelled perf counter
        res = self.mirror_daemon_command(f'counter dump for fs: {fs_name}', 'counter', 'dump')
        vafter = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR][0]

        self.assertLess(vafter["counters"]["mirrored_filesystems"],
                        vbefore["counters"]["mirrored_filesystems"])

    def verify_peer_added(self, fs_name, fs_id, peer_spec, remote_fs_name=None):
        # verify via asok
        res = self.mirror_daemon_command(f'mirror status for fs: {fs_name}',
                                         'fs', 'mirror', 'status', f'{fs_name}@{fs_id}')
        peer_uuid = self.get_peer_uuid(peer_spec)
        self.assertTrue(peer_uuid in res['peers'])
        client_name = res['peers'][peer_uuid]['remote']['client_name']
        cluster_name = res['peers'][peer_uuid]['remote']['cluster_name']
        self.assertTrue(peer_spec == f'{client_name}@{cluster_name}')
        if remote_fs_name:
            self.assertTrue(self.secondary_fs_name == res['peers'][peer_uuid]['remote']['fs_name'])
        else:
            self.assertTrue(self.fs_name == res['peers'][peer_uuid]['remote']['fs_name'])

    def peer_add(self, fs_name, fs_id, peer_spec, remote_fs_name=None, check_perf_counter=True):
        if check_perf_counter:
            res = self.mirror_daemon_command(f'counter dump for fs: {fs_name}', 'counter', 'dump')
            vbefore = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR_FS][0]

        if remote_fs_name:
            self.run_ceph_cmd("fs", "snapshot", "mirror", "peer_add", fs_name, peer_spec, remote_fs_name)
        else:
            self.run_ceph_cmd("fs", "snapshot", "mirror", "peer_add", fs_name, peer_spec)
        time.sleep(10)
        self.verify_peer_added(fs_name, fs_id, peer_spec, remote_fs_name)

        if check_perf_counter:
            res = self.mirror_daemon_command(f'counter dump for fs: {fs_name}', 'counter', 'dump')
            vafter = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR_FS][0]
            self.assertGreater(vafter["counters"]["mirroring_peers"], vbefore["counters"]["mirroring_peers"])

    def peer_remove(self, fs_name, fs_id, peer_spec):
        res = self.mirror_daemon_command(f'counter dump for fs: {fs_name}', 'counter', 'dump')
        vbefore = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR_FS][0]

        peer_uuid = self.get_peer_uuid(peer_spec)
        self.run_ceph_cmd("fs", "snapshot", "mirror", "peer_remove", fs_name, peer_uuid)
        time.sleep(10)
        # verify via asok
        res = self.mirror_daemon_command(f'mirror status for fs: {fs_name}',
                                         'fs', 'mirror', 'status', f'{fs_name}@{fs_id}')
        self.assertTrue(res['peers'] == {} and res['snap_dirs']['dir_count'] == 0)

        res = self.mirror_daemon_command(f'counter dump for fs: {fs_name}', 'counter', 'dump')
        vafter = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR_FS][0]

        self.assertLess(vafter["counters"]["mirroring_peers"], vbefore["counters"]["mirroring_peers"])

    def add_directory(self, fs_name, fs_id, dir_name, check_perf_counter=True):
        if check_perf_counter:
            res = self.mirror_daemon_command(f'counter dump for fs: {fs_name}', 'counter', 'dump')
            vbefore = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR_FS][0]

        # get initial dir count
        res = self.mirror_daemon_command(f'mirror status for fs: {fs_name}',
                                         'fs', 'mirror', 'status', f'{fs_name}@{fs_id}')
        dir_count = res['snap_dirs']['dir_count']
        log.debug(f'initial dir_count={dir_count}')

        self.run_ceph_cmd("fs", "snapshot", "mirror", "add", fs_name, dir_name)

        time.sleep(10)
        # verify via asok
        res = self.mirror_daemon_command(f'mirror status for fs: {fs_name}',
                                         'fs', 'mirror', 'status', f'{fs_name}@{fs_id}')
        new_dir_count = res['snap_dirs']['dir_count']
        log.debug(f'new dir_count={new_dir_count}')
        self.assertTrue(new_dir_count > dir_count)

        if check_perf_counter:
            res = self.mirror_daemon_command(f'counter dump for fs: {fs_name}', 'counter', 'dump')
            vafter = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR_FS][0]
            self.assertGreater(vafter["counters"]["directory_count"], vbefore["counters"]["directory_count"])

    def remove_directory(self, fs_name, fs_id, dir_name):
        res = self.mirror_daemon_command(f'counter dump for fs: {fs_name}', 'counter', 'dump')
        vbefore = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR_FS][0]
        # get initial dir count
        res = self.mirror_daemon_command(f'mirror status for fs: {fs_name}',
                                         'fs', 'mirror', 'status', f'{fs_name}@{fs_id}')
        dir_count = res['snap_dirs']['dir_count']
        log.debug(f'initial dir_count={dir_count}')

        self.run_ceph_cmd("fs", "snapshot", "mirror", "remove", fs_name, dir_name)

        time.sleep(10)
        # verify via asok
        res = self.mirror_daemon_command(f'mirror status for fs: {fs_name}',
                                         'fs', 'mirror', 'status', f'{fs_name}@{fs_id}')
        new_dir_count = res['snap_dirs']['dir_count']
        log.debug(f'new dir_count={new_dir_count}')
        self.assertTrue(new_dir_count < dir_count)

        res = self.mirror_daemon_command(f'counter dump for fs: {fs_name}', 'counter', 'dump')
        vafter = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR_FS][0]

        self.assertLess(vafter["counters"]["directory_count"], vbefore["counters"]["directory_count"])

    def peer_dir_status(self, res, dir_name, peer_uuid):
        self.assertIn('metrics', res)
        return res['metrics'][dir_name]['peer'][peer_uuid]

    @retry_assert(timeout=600, interval=10)
    def check_peer_status(self, fs_name, fs_id, peer_spec, dir_name, expected_snap_name,
                          expected_snap_count):
        peer_uuid = self.get_peer_uuid(peer_spec)
        res = self.mirror_daemon_command(f'peer status for fs: {fs_name}',
                                         'fs', 'mirror', 'peer', 'status',
                                         f'{fs_name}@{fs_id}', peer_uuid)
        try:
            dir_stat = self.peer_dir_status(res, dir_name, peer_uuid)
            self.assertTrue(dir_stat['last_synced_snap']['name'] == expected_snap_name)
            self.assertTrue(dir_stat['snaps_synced'] == expected_snap_count)
        except RETRY_EXCEPTIONS as e:
            e.res = res
            raise

    def verify_snapshot(self, dir_name, snap_name):
        snap_list = self.mount_b.ls(path=f'{dir_name}/.snap')
        self.assertTrue(snap_name in snap_list)

        source_res = self.mount_a.dir_checksum(path=f'{dir_name}/.snap/{snap_name}',
                                               follow_symlinks=True)
        log.debug(f'source snapshot checksum {snap_name} {source_res}')

        dest_res = self.mount_b.dir_checksum(path=f'{dir_name}/.snap/{snap_name}',
                                             follow_symlinks=True)
        log.debug(f'destination snapshot checksum {snap_name} {dest_res}')
        self.assertTrue(source_res == dest_res)

    @retry_assert(timeout=120, interval=2)
    def _wait_remote_dir_without_snap(self, fs_name, fs_id, peer_spec,
                                      dir_path, snap_name):
        """Wait until sync has started but the snap is not on the secondary yet."""
        dir_name = dir_path.lstrip('/')
        try:
            self.mount_b.run_shell(['test', '-d', dir_name])
        except CommandFailedError:
            raise AssertionError(f'remote dir {dir_name!r} does not exist yet')

        snap_path = f'{dir_name}/.snap/{snap_name}'
        try:
            self.mount_b.run_shell(['test', '-d', snap_path])
        except CommandFailedError:
            pass
        else:
            raise RuntimeError(
                f'snapshot {snap_name!r} synced before caps could be restricted')

        peer_uuid = self.get_peer_uuid(peer_spec)
        res = self.mirror_daemon_command(f'peer status for fs: {fs_name}',
                                         'fs', 'mirror', 'peer', 'status',
                                         f'{fs_name}@{fs_id}', peer_uuid)
        dir_stat = self.peer_dir_status(res, dir_path, peer_uuid)
        if dir_stat['state'] != 'syncing':
            raise AssertionError(
                f'expected syncing, got {dir_stat["state"]!r}')
        if dir_stat['current_syncing_snap']['name'] != snap_name:
            raise AssertionError(
                f'expected syncing snap {snap_name!r}, got '
                f'{dir_stat["current_syncing_snap"]["name"]!r}')

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

    def get_mirror_daemon_pid(self):
        """pid file overloaded in fs/mirror/clients/mirror.yaml"""
        return self.mount_a.run_shell(['cat', '/var/run/ceph/cephfs-mirror.pid']).stdout.getvalue().strip()

    def mirror_daemon_command(self, cmd_label, *args):
        asok_path = self.get_daemon_admin_socket()
        try:
            # use mount_a's remote to execute command
            p = self.mount_a.client_remote.run(args=
                     ['ceph', '--admin-daemon', asok_path] + list(args),
                     stdout=StringIO(), stderr=StringIO(), timeout=30,
                     check_status=True, label=cmd_label)
            p.wait()
        except CommandFailedError as ce:
            log.warn(f'mirror daemon command with label "{cmd_label}" failed: {ce}')
            raise
        res = p.stdout.getvalue().strip()
        log.debug(f'command returned={res}')
        return json.loads(res)

    def checkpoint_list(self, fs_name, dir_path):
        return json.loads(self.get_ceph_cmd_stdout(
            "fs", "snapshot", "mirror", "checkpoint", "ls",
            fs_name, dir_path))

    def checkpoint_add(self, fs_name, dir_path, snap_name):
        out = json.loads(self.get_ceph_cmd_stdout(
            "fs", "snapshot", "mirror", "checkpoint", "add",
            fs_name, dir_path, snap_name))
        self.assertEqual(out['status'], 'success')
        return out

    def checkpoint_remove(self, fs_name, dir_path, snap_name):
        out = json.loads(self.get_ceph_cmd_stdout(
            "fs", "snapshot", "mirror", "checkpoint", "remove",
            fs_name, dir_path, snap_name))
        self.assertEqual(out['status'], 'success')
        return out

    def checkpoint_now(self, fs_name, dir_path):
        out = json.loads(self.get_ceph_cmd_stdout(
            "fs", "snapshot", "mirror", "checkpoint", "now",
            fs_name, dir_path))
        self.assertEqual(out['status'], 'success')
        return out

    @staticmethod
    def find_checkpoint(checkpoints, snap_name):
        for cp in checkpoints:
            if cp['snap_name'] == snap_name:
                return cp
        return None

    def assert_checkpoint_listed(self, fs_name, dir_path, snap_name):
        res = self.checkpoint_list(fs_name, dir_path)
        cp = self.find_checkpoint(res['checkpoints'], snap_name)
        self.assertIsNotNone(cp, f'checkpoint {snap_name} should be listed')

    def assert_checkpoint_not_listed(self, fs_name, dir_path, snap_name):
        res = self.checkpoint_list(fs_name, dir_path)
        cp = self.find_checkpoint(res['checkpoints'], snap_name)
        self.assertIsNone(cp, f'checkpoint {snap_name} should not be listed')

    def start_mirror_daemon(self):
        self.mount_a.run_shell_payload(
            'nohup cephfs-mirror --id mirror </dev/null >/dev/null 2>&1 &')

        @retry_assert(timeout=60, interval=2)
        def wait_ready():
            self.mirror_daemon_command(
                f'counter dump for fs: {self.primary_fs_name}', 'counter', 'dump')

        wait_ready()

    def stop_mirror_daemon(self, wait=5):
        pid = self.get_mirror_daemon_pid()
        log.debug(f'stopping cephfs-mirror (pid={pid})')
        self.mount_a.run_shell_payload(f'kill -TERM {pid} || true')
        time.sleep(wait)

    def restart_mirror_daemon(self):
        self.stop_mirror_daemon()
        self.start_mirror_daemon()

    def restart_mirroring_module(self, wait=10):
        log.debug('restarting mirroring mgr module')
        self.run_ceph_cmd("mgr", "module", "disable", self.MODULE_NAME)
        time.sleep(2)
        self.run_ceph_cmd("mgr", "module", "enable", self.MODULE_NAME)
        time.sleep(wait)

    def _setup_mirrored_directory(self, dir_path, peer_spec=None, mount_b=False):
        """Enable mirroring and track a directory; optionally mount backup FS and add peer."""
        if mount_b:
            self.setup_mount_b(mds_perm='rw')
        self.mount_a.run_shell(["mkdir", "-p", dir_path.lstrip('/')])
        self.enable_mirroring(self.primary_fs_name, self.primary_fs_id)
        self.add_directory(self.primary_fs_name, self.primary_fs_id, dir_path)
        if peer_spec:
            self.peer_add(self.primary_fs_name, self.primary_fs_id, peer_spec,
                          self.secondary_fs_name)

    def _add_checkpoint_snapshot(self, dir_path, snap_name, verify_listed=False):
        """Create a snapshot and add a checkpoint on it."""
        self.mount_a.run_shell(["mkdir", f"{dir_path.lstrip('/')}/.snap/{snap_name}"])
        self.checkpoint_add(self.primary_fs_name, dir_path, snap_name)
        if verify_listed:
            self.assert_checkpoint_listed(self.primary_fs_name, dir_path, snap_name)

    def _setup_checkpoint_dir(self, dir_path, snap_name, peer_spec=None, mount_b=True):
        """Create a tracked directory with a checkpointed snapshot."""
        self._setup_mirrored_directory(dir_path, peer_spec=peer_spec, mount_b=mount_b)
        self._add_checkpoint_snapshot(dir_path, snap_name, verify_listed=True)

    def _teardown_checkpoint_dir(self, dir_path, snap_names, peer_spec=None):
        self.remove_directory(self.primary_fs_name, self.primary_fs_id, dir_path)
        if peer_spec:
            self.peer_remove(self.primary_fs_name, self.primary_fs_id, peer_spec)
        self.disable_mirroring(self.primary_fs_name, self.primary_fs_id)
        for snap_name in snap_names:
            snap_path = f"{dir_path.lstrip('/')}/.snap/{snap_name}"
            try:
                self.mount_a.run_shell(["rmdir", snap_path])
            except CommandFailedError:
                pass
        self.mount_a.run_shell(["rmdir", dir_path.lstrip('/')])

    @retry_assert(timeout=600, interval=10)
    def check_checkpoint_status(self, fs_name, dir_path, snap_name, expected_status):
        res = self.checkpoint_list(fs_name, dir_path)
        cp = self.find_checkpoint(res['checkpoints'], snap_name)
        if cp is None or cp['status'] != expected_status:
            raise AssertionError(
                f'checkpoint {snap_name}: expected status {expected_status}, '
                f'got {None if cp is None else cp["status"]!r}')

    def check_checkpoint_statuses(self, fs_name, dir_path, snap_names, expected_status):
        for snap_name in snap_names:
            self.check_checkpoint_status(fs_name, dir_path, snap_name, expected_status)

    def setup_mount_b(self, mds_perm):
        log.debug('reconfigure client auth caps')
        self.get_ceph_cmd_result(
            'auth', 'caps', f"client.{self.mount_b.client_id}",
            'mds', f'allow {mds_perm}',
            'mon', 'allow r',
            'osd', f"allow rw pool={self.backup_fs.get_data_pool_name()}")
        self.mount_b.umount_wait()
        log.debug(f'mounting filesystem {self.secondary_fs_name}')
        self.mount_b.mount_wait(cephfs_name=self.secondary_fs_name)

    def _restrict_mirror_remote_caps(self):
        """Restrict mirror remote client OSD write to force snapshot sync failure.

        The remote mirrored directory must already exist (created while the
        client still has OSD write).  OSD read-only blocks file data transfer
        during snapshot sync, where checkpoint_sync_failed() is recorded.
        """
        self.get_ceph_cmd_result(
            'auth', 'caps', 'client.mirror_remote',
            'mds', 'allow rwps',
            'mon', 'allow r',
            'osd', "allow r tag cephfs *=*",
            'mgr', 'allow r')

    def _restore_mirror_remote_caps(self):
        """Restore mirror remote client caps used by the mirror test suite."""
        self.get_ceph_cmd_result(
            'auth', 'caps', 'client.mirror_remote',
            'mds', 'allow rwps',
            'mon', 'allow r',
            'osd', "allow rw tag cephfs *=*",
            'mgr', 'allow r')

    def test_checkpoint_cli_add_list_remove_now(self):
        """Test mgr checkpoint add/list/remove/now on snapshot metadata."""
        dir_path = '/cp_cli'
        snap0 = 'snap0'
        snap1 = 'snap1'

        self._setup_mirrored_directory(dir_path)
        self._add_checkpoint_snapshot(dir_path, snap0)

        res = self.checkpoint_list(self.primary_fs_name, dir_path)
        self.assertEqual(res['dir_root'], dir_path)
        cp = self.find_checkpoint(res['checkpoints'], snap0)
        self.assertIsNotNone(cp)
        self.assertEqual(cp['status'], 'created')
        self.assertTrue(cp['created_at'])
        self.assertTrue(cp['updated_at'])

        self.mount_a.run_shell(["mkdir", f"{dir_path.lstrip('/')}/.snap/{snap1}"])
        out = self.checkpoint_now(self.primary_fs_name, dir_path)
        self.assertEqual(out['snap_name'], snap1)

        res = self.checkpoint_list(self.primary_fs_name, dir_path)
        self.assertEqual(len(res['checkpoints']), 2)

        self.checkpoint_remove(self.primary_fs_name, dir_path, snap0)
        res = self.checkpoint_list(self.primary_fs_name, dir_path)
        self.assertEqual(len(res['checkpoints']), 1)
        self.assertEqual(res['checkpoints'][0]['snap_name'], snap1)

        self.checkpoint_remove(self.primary_fs_name, dir_path, snap1)
        res = self.checkpoint_list(self.primary_fs_name, dir_path)
        self.assertEqual(res['checkpoints'], [])

        self._teardown_checkpoint_dir(dir_path, [snap0, snap1])

    def test_checkpoint_cli_errors(self):
        """Test checkpoint CLI validation errors."""
        dir_path = '/cp_err'
        tracked = '/cp_err/tracked'
        snap = 'snap0'

        self._setup_mirrored_directory(tracked)
        self.mount_a.run_shell(["mkdir", f"{tracked.lstrip('/')}/.snap/{snap}"])

        try:
            self.checkpoint_add(self.primary_fs_name, dir_path, snap)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.ENOENT:
                raise RuntimeError(
                    f'expected ENOENT for untracked directory, got {ce.exitstatus}')
        else:
            raise RuntimeError('expected checkpoint add to fail for untracked directory')

        try:
            self.checkpoint_remove(self.primary_fs_name, tracked, snap)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.ENOENT:
                raise RuntimeError(
                    f'expected ENOENT when removing non-checkpoint snap, got {ce.exitstatus}')
        else:
            raise RuntimeError('expected checkpoint remove to fail')

        self.checkpoint_add(self.primary_fs_name, tracked, snap)
        try:
            self.checkpoint_add(self.primary_fs_name, tracked, snap)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.EEXIST:
                raise RuntimeError(
                    f'expected EEXIST when re-adding checkpoint, got {ce.exitstatus}')
        else:
            raise RuntimeError('expected checkpoint add to fail for duplicate')
        self.checkpoint_remove(self.primary_fs_name, tracked, snap)

        self.remove_directory(self.primary_fs_name, self.primary_fs_id, tracked)
        self.disable_mirroring(self.primary_fs_name, self.primary_fs_id)
        try:
            self.checkpoint_list(self.primary_fs_name, tracked)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.EINVAL:
                raise RuntimeError(
                    f'expected EINVAL with mirroring disabled, got {ce.exitstatus}')
        else:
            raise RuntimeError('expected checkpoint list to fail')

        try:
            self.mount_a.run_shell(["rmdir", f"{tracked.lstrip('/')}/.snap/{snap}"])
        except CommandFailedError:
            pass
        self.mount_a.run_shell(["rmdir", tracked.lstrip('/')])

    def test_checkpoint_sync_status_reaches_complete(self):
        """Test mirror daemon updates checkpoint status after snapshot sync."""
        dir_path = '/cp_sync'
        snap_name = 'snap0'
        peer_spec = "client.mirror_remote@ceph"

        self._setup_mirrored_directory(dir_path, mount_b=True)
        self._add_checkpoint_snapshot(dir_path, snap_name)
        self.check_checkpoint_status(self.primary_fs_name, dir_path, snap_name, 'created')

        res = self.checkpoint_list(self.primary_fs_name, dir_path)
        cp = self.find_checkpoint(res['checkpoints'], snap_name)
        created_at = cp['created_at']

        self.peer_add(self.primary_fs_name, self.primary_fs_id, peer_spec, self.secondary_fs_name)
        self.check_peer_status(self.primary_fs_name, self.primary_fs_id,
                               peer_spec, dir_path, snap_name, 1)
        self.verify_snapshot(dir_path.lstrip('/'), snap_name)
        self.check_checkpoint_status(self.primary_fs_name, dir_path, snap_name, 'complete')

        res = self.checkpoint_list(self.primary_fs_name, dir_path)
        cp = self.find_checkpoint(res['checkpoints'], snap_name)
        self.assertEqual(cp['created_at'], created_at)
        self.assertTrue(cp['updated_at'])

        self._teardown_checkpoint_dir(dir_path, [snap_name], peer_spec)

    def test_checkpoint_deleted_snapshot_not_listed(self):
        """Deleted checkpointed snapshots must not appear in checkpoint ls."""
        dir_path = '/cp_del'
        snap_name = 'snap0'

        self._setup_mirrored_directory(dir_path)
        self._add_checkpoint_snapshot(dir_path, snap_name)

        res = self.checkpoint_list(self.primary_fs_name, dir_path)
        self.assertEqual(len(res['checkpoints']), 1)

        self.mount_a.run_shell(["rmdir", f"{dir_path.lstrip('/')}/.snap/{snap_name}"])
        res = self.checkpoint_list(self.primary_fs_name, dir_path)
        self.assertEqual(res['checkpoints'], [])

        self._teardown_checkpoint_dir(dir_path, [snap_name])

    def test_checkpoint_renamed_snapshot_shows_new_name(self):
        """Renamed checkpointed snapshots must appear under the new name in ls."""
        dir_path = '/cp_rename'
        old_name = 'snap0'
        new_name = 'snap1'

        self._setup_mirrored_directory(dir_path)
        self._add_checkpoint_snapshot(dir_path, old_name)

        self.mount_a.run_shell([
            "mv",
            f"{dir_path.lstrip('/')}/.snap/{old_name}",
            f"{dir_path.lstrip('/')}/.snap/{new_name}",
        ])

        self.assert_checkpoint_not_listed(self.primary_fs_name, dir_path, old_name)
        self.check_checkpoint_status(self.primary_fs_name, dir_path, new_name, 'created')

        self._teardown_checkpoint_dir(dir_path, [new_name])

    def test_checkpoint_add_after_rename_allows_same_snap_name(self):
        """A new snapshot can reuse a name after the checkpointed one is renamed."""
        dir_path = '/cp_reuse_name'
        old_name = 'snap0'
        new_name = 'snap1'

        self._setup_mirrored_directory(dir_path)
        self._add_checkpoint_snapshot(dir_path, old_name)

        self.mount_a.run_shell([
            "mv",
            f"{dir_path.lstrip('/')}/.snap/{old_name}",
            f"{dir_path.lstrip('/')}/.snap/{new_name}",
        ])
        self.mount_a.run_shell(["mkdir", f"{dir_path.lstrip('/')}/.snap/{old_name}"])
        self.checkpoint_add(self.primary_fs_name, dir_path, old_name)

        res = self.checkpoint_list(self.primary_fs_name, dir_path)
        self.assertEqual(len(res['checkpoints']), 2)
        self.check_checkpoint_status(self.primary_fs_name, dir_path, old_name, 'created')
        self.check_checkpoint_status(self.primary_fs_name, dir_path, new_name, 'created')

        self._teardown_checkpoint_dir(dir_path, [old_name, new_name])

    def test_checkpoint_persisted_across_mirror_daemon_restart(self):
        """Checkpoints survive mirror daemon restart and reach complete after sync."""
        dir_path = '/cp_mirror_restart'
        snap_name = 'snap0'
        peer_spec = "client.mirror_remote@ceph"

        self._setup_checkpoint_dir(dir_path, snap_name)
        res = self.checkpoint_list(self.primary_fs_name, dir_path)
        self.assertEqual(len(res['checkpoints']), 1)

        self.restart_mirror_daemon()

        res = self.checkpoint_list(self.primary_fs_name, dir_path)
        self.assertEqual(len(res['checkpoints']), 1)
        self.check_checkpoint_status(self.primary_fs_name, dir_path, snap_name, 'created')

        self.peer_add(self.primary_fs_name, self.primary_fs_id, peer_spec, self.secondary_fs_name)
        self.check_peer_status(self.primary_fs_name, self.primary_fs_id,
                               peer_spec, dir_path, snap_name, 1)
        self.verify_snapshot(dir_path.lstrip('/'), snap_name)
        self.check_checkpoint_status(self.primary_fs_name, dir_path, snap_name, 'complete')

        self._teardown_checkpoint_dir(dir_path, [snap_name], peer_spec)

    def test_checkpoint_persisted_across_mgr_module_restart(self):
        """Checkpoints survive mirroring mgr module restart and reach complete after sync."""
        dir_path = '/cp_mgr_restart'
        snap_name = 'snap0'
        peer_spec = "client.mirror_remote@ceph"

        self._setup_checkpoint_dir(dir_path, snap_name)
        res = self.checkpoint_list(self.primary_fs_name, dir_path)
        self.assertEqual(len(res['checkpoints']), 1)

        self.restart_mirroring_module()

        res = self.checkpoint_list(self.primary_fs_name, dir_path)
        self.assertEqual(len(res['checkpoints']), 1)
        self.check_checkpoint_status(self.primary_fs_name, dir_path, snap_name, 'created')

        self.peer_add(self.primary_fs_name, self.primary_fs_id, peer_spec, self.secondary_fs_name)
        self.check_peer_status(self.primary_fs_name, self.primary_fs_id,
                               peer_spec, dir_path, snap_name, 1)
        self.verify_snapshot(dir_path.lstrip('/'), snap_name)
        self.check_checkpoint_status(self.primary_fs_name, dir_path, snap_name, 'complete')

        self._teardown_checkpoint_dir(dir_path, [snap_name], peer_spec)

    def test_checkpoint_add_on_already_synced_snapshot_is_complete(self):
        """Checkpoint added after sync should be marked complete by the daemon."""
        dir_path = '/cp_already_synced'
        snap_name = 'snap0'
        peer_spec = "client.mirror_remote@ceph"

        self._setup_mirrored_directory(dir_path, peer_spec=peer_spec, mount_b=True)
        self.mount_a.run_shell(["mkdir", f"{dir_path.lstrip('/')}/.snap/{snap_name}"])

        self.check_peer_status(self.primary_fs_name, self.primary_fs_id,
                               peer_spec, dir_path, snap_name, 1)
        self.verify_snapshot(dir_path.lstrip('/'), snap_name)

        self.checkpoint_add(self.primary_fs_name, dir_path, snap_name)
        self.check_checkpoint_status(self.primary_fs_name, dir_path, snap_name, 'complete')

        self._teardown_checkpoint_dir(dir_path, [snap_name], peer_spec)

    def test_checkpoint_on_synced_snaps_complete_after_daemon_restart(self):
        """Checkpoints added on synced snaps while daemon is down reach complete after restart."""
        dir_path = '/cp_daemon_down'
        snap_names = [f'snap{i}' for i in range(5)]
        checkpoint_snaps = ['snap0', 'snap2', 'snap4']
        peer_spec = "client.mirror_remote@ceph"

        self._setup_mirrored_directory(dir_path, peer_spec=peer_spec, mount_b=True)

        for snap_name in snap_names:
            self.mount_a.run_shell(["mkdir", f"{dir_path.lstrip('/')}/.snap/{snap_name}"])

        self.check_peer_status(self.primary_fs_name, self.primary_fs_id,
                               peer_spec, dir_path, snap_names[-1], len(snap_names))
        self.verify_snapshot(dir_path.lstrip('/'), snap_names[-1])

        self.stop_mirror_daemon()
        for snap_name in checkpoint_snaps:
            self.checkpoint_add(self.primary_fs_name, dir_path, snap_name)
        self.check_checkpoint_statuses(
            self.primary_fs_name, dir_path, checkpoint_snaps, 'created')

        self.restart_mirror_daemon()
        self.check_checkpoint_statuses(
            self.primary_fs_name, dir_path, checkpoint_snaps, 'complete')

        self._teardown_checkpoint_dir(dir_path, snap_names, peer_spec)

    def test_checkpoint_failed_then_complete(self):
        """Checkpoint is marked failed on sync error and complete after recovery."""
        dir_path = '/cp_failed'
        snap_name = 'snap0'
        peer_spec = "client.mirror_remote@ceph"

        self._setup_mirrored_directory(dir_path, mount_b=True)
        self.mount_a.create_n_files(f'{dir_path.lstrip("/")}/file', 5000, sync=True)
        self._add_checkpoint_snapshot(dir_path, snap_name)
        self.check_checkpoint_status(self.primary_fs_name, dir_path, snap_name, 'created')

        # peer_add needs write access on the remote MDS (setxattr on ceph.mirror.info).
        # Let the daemon create the remote dir with full caps, then restrict OSD
        # write before mksnap (checkpoint_sync_failed() path).
        self.peer_add(self.primary_fs_name, self.primary_fs_id, peer_spec,
                      self.secondary_fs_name)
        self._wait_remote_dir_without_snap(self.primary_fs_name, self.primary_fs_id,
                                           peer_spec, dir_path, snap_name)
        self.stop_mirror_daemon()
        self._restrict_mirror_remote_caps()
        self.restart_mirror_daemon()

        self.check_checkpoint_status(self.primary_fs_name, dir_path, snap_name, 'failed')
        res = self.checkpoint_list(self.primary_fs_name, dir_path)
        cp = self.find_checkpoint(res['checkpoints'], snap_name)
        self.assertIn('error_msg', cp)
        self.assertTrue(cp['error_msg'])

        self._restore_mirror_remote_caps()
        self.restart_mirror_daemon()

        self.check_peer_status(self.primary_fs_name, self.primary_fs_id,
                               peer_spec, dir_path, snap_name, 1)
        self.verify_snapshot(dir_path.lstrip('/'), snap_name)
        self.check_checkpoint_status(self.primary_fs_name, dir_path, snap_name, 'complete')

        res = self.checkpoint_list(self.primary_fs_name, dir_path)
        cp = self.find_checkpoint(res['checkpoints'], snap_name)
        self.assertNotIn('error_msg', cp)

        self._teardown_checkpoint_dir(dir_path, [snap_name], peer_spec)
