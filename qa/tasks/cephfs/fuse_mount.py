import json
import signal
import time
import logging

from io import StringIO, IOBase
from textwrap import dedent

from teuthology.contextutil import MaxWhileTries
from teuthology.contextutil import safe_while
from teuthology.orchestra import run
from teuthology.exceptions import CommandFailedError
from tasks.ceph_manager import get_valgrind_args
from tasks.cephfs.mount import CephFSMount, UMOUNT_TIMEOUT

log = logging.getLogger(__name__)

# Refer mount.py for docstrings.
class FuseMount(CephFSMount):
    def __init__(self, ctx, test_dir, client_id, client_remote,
                 client_keyring_path=None, cephfs_name=None,
                 cephfs_mntpt=None, hostfs_mntpt=None, brxnet=None,
                 client_config={}):
        super(FuseMount, self).__init__(ctx=ctx, test_dir=test_dir,
            client_id=client_id, client_remote=client_remote,
            client_keyring_path=client_keyring_path, hostfs_mntpt=hostfs_mntpt,
            cephfs_name=cephfs_name, cephfs_mntpt=cephfs_mntpt, brxnet=brxnet,
            client_config=client_config)

        self.fuse_daemon = None
        self._fuse_conn = None
        self.id = None
        self.inst = None
        self.addr = None
        self.mount_timeout = int(self.client_config.get('mount_timeout', 30))

        self._mount_bin = [
            'ceph-fuse', "-f",
            "--admin-socket", "/var/run/ceph/$cluster-$name.$pid.asok"]
        self._mount_cmd_cwd = self.test_dir
        if self.client_config.get('valgrind') is not None:
            self.cwd = None # get_valgrind_args chdir for us
        self._mount_cmd_logger = log.getChild('ceph-fuse.{id}'.format(id=self.client_id))
        self._mount_cmd_stdin = run.PIPE

    def mount(self, mntopts=None, check_status=True, mntargs=None, **kwargs):
        self.update_attrs(**kwargs)
        self.assert_and_log_minimum_mount_details()

        self.setup_netns()

        try:
            return self._mount(mntopts, mntargs, check_status)
        except RuntimeError:
            # Catch exceptions by the mount() logic (i.e. not remote command
            # failures) and ensure the mount is not left half-up.
            # Otherwise we might leave a zombie mount point that causes
            # anyone traversing cephtest/ to get hung up on.
            log.warning("Trying to clean up after failed mount")
            self.umount_wait(force=True)
            raise

    def _mount(self, mntopts, mntargs, check_status):
        log.info("Client client.%s config is %s" % (self.client_id,
                                                    self.client_config))

        self._create_mntpt()

        retval = self._run_mount_cmd(mntopts, mntargs, check_status)
        if retval:
            return retval

        self.gather_mount_info()

    def _run_mount_cmd(self, mntopts, mntargs, check_status):
        mount_cmd = self._get_mount_cmd(mntopts, mntargs)
        mountcmd_stdout, mountcmd_stderr = StringIO(), StringIO()

        # Before starting ceph-fuse process, note the contents of
        # /sys/fs/fuse/connections
        pre_mount_conns = self._list_fuse_conns()
        log.info("Pre-mount connections: {0}".format(pre_mount_conns))

        self.fuse_daemon = self.client_remote.run(
            args=mount_cmd,
            cwd=self._mount_cmd_cwd,
            logger=self._mount_cmd_logger,
            stdin=self._mount_cmd_stdin,
            stdout=mountcmd_stdout,
            stderr=mountcmd_stderr,
            wait=False,
            omit_sudo=False
        )

        return self._wait_and_record_our_fuse_conn(
            check_status, pre_mount_conns, mountcmd_stdout, mountcmd_stderr)

    def _get_mount_cmd(self, mntopts, mntargs):
        if self.client_config.get('coverage') or \
           self.client_config.get('valgrind') is not None:
            daemon_signal = int(signal.SIGTERM)
        else:
            daemon_signal = int(signal.SIGKILL)

        # Instead of using self._runt, setup stdin-killer here because we want
        # to wrap the command with extra hooks (adjust-ulimits, etc.)
        mount_cmd = [
            'sudo',
            'adjust-ulimits',
            'ceph-coverage',
            f'{self.test_dir}/archive/coverage',
            'stdin-killer',
            f'--timeout={UMOUNT_TIMEOUT}',
            f'--signal={daemon_signal}',
            '--'
        ]

        mount_cmd = self._add_valgrind_args(mount_cmd)
        mount_cmd = ['sudo'] + self._nsenter_args + mount_cmd

        mount_cmd += self._mount_bin + [self.hostfs_mntpt]
        if self.client_id:
            mount_cmd += ['--id', self.client_id]
        if self.client_keyring_path and self.client_id:
            mount_cmd += ['-k', self.client_keyring_path]

        self.validate_subvol_options()

        if self.cephfs_mntpt:
            mount_cmd += ["--client_mountpoint=" + self.cephfs_mntpt]

        if self.cephfs_name:
            mount_cmd += ["--client_fs=" + self.cephfs_name]
        if mntopts:
            mount_cmd.extend(('-o', ','.join(mntopts)))
        if mntargs:
            mount_cmd.extend(mntargs)

        return mount_cmd

    def _add_valgrind_args(self, mount_cmd):
        if self.client_config.get('valgrind') is not None:
            mount_cmd = get_valgrind_args(
                self.test_dir,
                'client.{id}'.format(id=self.client_id),
                mount_cmd,
                self.client_config.get('valgrind'),
                cd=False
            )

        return mount_cmd

    def _list_fuse_conns(self):
        conn_dir = "/sys/fs/fuse/connections"

        if not self.client_remote.is_mounted(conn_dir):
            self._runt(args=["mount", "-t", "fusectl", conn_dir, conn_dir], check_status=False)

        try:
            ls_str = self.client_remote.sh("ls " + conn_dir,
                                           stdout=StringIO(),
                                           timeout=300).strip()
        except CommandFailedError:
            return []

        if ls_str:
            return [int(n) for n in ls_str.split("\n")]
        else:
            return []

    def _wait_and_record_our_fuse_conn(self, check_status, pre_mount_conns,
                                       mountcmd_stdout, mountcmd_stderr):
        """
        Wait for the connection reference to appear in /sys
        """
        waited = 0

        post_mount_conns = self._list_fuse_conns()
        while len(post_mount_conns) <= len(pre_mount_conns):
            if self.fuse_daemon.finished:
                # Did mount fail?  Raise the CommandFailedError instead of
                # hitting the "failed to populate /sys/" timeout
                try:
                    self.fuse_daemon.wait()
                except CommandFailedError as e:
                    log.info('mount command failed.')
                    if check_status:
                        raise
                    else:
                        return (e, mountcmd_stdout.getvalue(),
                                mountcmd_stderr.getvalue())
            time.sleep(1)
            waited += 1
            if waited > self._fuse_conn_check_timeout:
                raise RuntimeError(
                    "Fuse mount failed to populate/sys/ after {} "
                    "seconds".format(waited))
            else:
                post_mount_conns = self._list_fuse_conns()

        log.info("Post-mount connections: {0}".format(post_mount_conns))

        self._record_our_fuse_conn(pre_mount_conns, post_mount_conns)

    @property
    def _fuse_conn_check_timeout(self):
        mount_wait = self.client_config.get('mount_wait', 0)
        if mount_wait > 0:
            log.info("Fuse mount waits {0} seconds before checking /sys/".format(mount_wait))
            time.sleep(mount_wait)
        timeout = int(self.client_config.get('mount_timeout', 30))
        return timeout

    def _record_our_fuse_conn(self, pre_mount_conns, post_mount_conns):
        """
        Record our fuse connection number so that we can use it when forcing
        an unmount.
        """
        new_conns = list(set(post_mount_conns) - set(pre_mount_conns))
        if len(new_conns) == 0:
            raise RuntimeError("New fuse connection directory not found ({0})".format(new_conns))
        elif len(new_conns) > 1:
            raise RuntimeError("Unexpectedly numerous fuse connections {0}".format(new_conns))
        else:
            self._fuse_conn = new_conns[0]

    def gather_mount_info(self):
        status = self.admin_socket(['status'])
        self.id = status['id']
        self.client_pid = status['metadata']['pid']
        try:
            self.inst = status['inst_str']
            self.addr = status['addr_str']
        except KeyError:
            sessions = self.fs.rank_asok(['session', 'ls'])
            for s in sessions:
                if s['id'] == self.id:
                    self.inst = s['inst']
                    self.addr = self.inst.split()[1]
            if self.inst is None:
                raise RuntimeError("cannot find client session")

    def check_mounted_state(self):
        args = [
            'stat',
            '--file-system',
            '--printf=%T\n',
            '--',
            self.hostfs_mntpt,
        ]
        proc = self._runt(args=args, stdout=StringIO(), stderr=StringIO(), wait=False)
        try:
            proc.stdin.close()
            proc.wait()
        except CommandFailedError:
            error = proc.stderr.getvalue()
            if ("endpoint is not connected" in error
            or "Software caused connection abort" in error):
                # This happens is fuse is killed without unmount
                log.warning("Found stale mount point at {0}".format(self.hostfs_mntpt))
                return True
            else:
                # This happens if the mount directory doesn't exist
                log.info('mount point does not exist: %s', self.hostfs_mntpt)
                return False

        fstype = proc.stdout.getvalue().rstrip('\n')
        if fstype == 'fuseblk':
            log.info('ceph-fuse is mounted on %s', self.hostfs_mntpt)
            return True
        else:
            log.debug('ceph-fuse not mounted, got fs type {fstype!r}'.format(
                fstype=fstype))
            return False

    def _runt(self, timeout=300, **kwargs):
        args = kwargs.pop('args')
        args = [
            'sudo',
            'stdin-killer',
            f'--timeout={timeout}',
            '--',
        ] + args
        wait = kwargs.pop('wait', True)
        kwargs.pop('omit_sudo', False)
        stdin = kwargs.pop('stdin', run.PIPE)
        # N.B.: timeout here is just an SSH timeout
        p = self.client_remote.run(args=args, wait=False, omit_sudo=False, timeout=timeout*2, stdin=stdin, **kwargs)
        if wait:
            if isinstance(p.stdin, IOBase):
                p.stdin.close()
            p.wait()
        return p

    def wait_until_mounted(self):
        """
        Check to make sure that fuse is mounted on mountpoint.  If not,
        sleep for 5 seconds and check again.
        """

        while not self.check_mounted_state():
            # Even if it's not mounted, it should at least
            # be running: catch simple failures where it has terminated.
            assert not self.fuse_daemon.poll()

            time.sleep(5)

        # Now that we're mounted, set permissions so that the rest of the test
        # will have unrestricted access to the filesystem mount.
        for retry in range(10):
            try:
                args = [
                    'chmod',
                    '1777',
                    self.hostfs_mntpt,
                ]

                p = self._runt(args=args, stderr=StringIO(), wait=False)
                p.stdin.close()
                p.wait()
                break
            except run.CommandFailedError:
                stderr = p.stderr.getvalue().lower()
                if "read-only file system" in stderr:
                    break
                elif "permission denied" in stderr:
                    time.sleep(5)
                else:
                    raise

    def _mountpoint_exists(self):
        p = self._runt(args=['ls', '-d', self.hostfs_mntpt], check_status=False)
        return p.exitstatus == 0

    def umount(self, cleanup=True):
        """
        umount() must not run cleanup() when it's called by umount_wait()
        since "run.wait([self.fuse_daemon], timeout)" would hang otherwise.
        """
        if not self.is_mounted():
            if cleanup:
                self.cleanup()
            return
        if self.is_blocked():
            self._run_umount_lf()
            if cleanup:
                self.cleanup()
            return

        try:
            log.info('Running fusermount -u on {name}...'.format(name=self.client_remote.name))
            c = [
                'fusermount',
                '-u',
                self.hostfs_mntpt
            ]
            p = self._runt(args=c, stderr=StringIO(), wait=False, timeout=UMOUNT_TIMEOUT)
            p.stdin.close()
            p.wait()
        except run.CommandFailedError:
            if "mountpoint not found" in p.stderr.getvalue():
                # This happens if the mount directory doesn't exist
                log.info('mount point does not exist: %s', self.mountpoint)
            elif "not mounted" in p.stderr.getvalue():
                # This happens if the mount directory already unmouted
                log.info('mount point not mounted: %s', self.mountpoint)
            else:
                log.info('Failed to unmount ceph-fuse on {name}, aborting...'.format(name=self.client_remote.name))

                c = [
                    'env',
                    run.Raw('PATH=/usr/sbin:$PATH'),
                    'lsof',
                    run.Raw(';'),
                    'ps',
                    'auxf'
                ]
                self._runt(args=c, timeout=UMOUNT_TIMEOUT)

                # abort the fuse mount, killing all hung processes
                if self._fuse_conn:
                    self.run_python(dedent("""
                    import os
                    path = "/sys/fs/fuse/connections/{0}/abort"
                    if os.path.exists(path):
                        open(path, "w").write("1")
                    """).format(self._fuse_conn))
                    self._fuse_conn = None

                # make sure its unmounted
                self._run_umount_lf()

        self._fuse_conn = None
        self.id = None
        self.inst = None
        self.addr = None
        if cleanup:
            self.cleanup()

    def umount_wait(self, force=False, require_clean=False,
                    timeout=UMOUNT_TIMEOUT):
        """
        :param force: Complete cleanly even if the MDS is offline
        """
        if not (self.is_mounted() and self.fuse_daemon):
            log.debug('ceph-fuse client.{id} is not mounted at {remote} '
                      '{mnt}'.format(id=self.client_id,
                                     remote=self.client_remote,
                                     mnt=self.hostfs_mntpt))
            self.cleanup()
            return

        if force:
            assert not require_clean  # mutually exclusive

            # When we expect to be forcing, kill the ceph-fuse process directly.
            # This should avoid hitting the more aggressive fallback killing
            # in umount() which can affect other mounts too.
            self.fuse_daemon.stdin.close()

            # However, we will still hit the aggressive wait if there is an ongoing
            # mount -o remount (especially if the remount is stuck because MDSs
            # are unavailable)

        if self.is_blocked():
            self._run_umount_lf()
            self.cleanup()
            return

        # cleanup is set to to fail since cleanup must happen after umount is
        # complete; otherwise following call to run.wait hangs.
        self.umount(cleanup=False)

        try:
            # Permit a timeout, so that we do not block forever
            run.wait([self.fuse_daemon], timeout)

        except MaxWhileTries:
            log.error("process failed to terminate after unmount. This probably"
                      " indicates a bug within ceph-fuse.")
            raise
        except CommandFailedError:
            if require_clean:
                raise

        self.cleanup()

    def teardown(self):
        """
        Whatever the state of the mount, get it gone.
        """
        super(FuseMount, self).teardown()

        self.umount()

        if self.fuse_daemon and not self.fuse_daemon.finished:
            self.fuse_daemon.stdin.close()
            try:
                self.fuse_daemon.wait()
            except CommandFailedError:
                pass

    def _asok_path(self):
        return "/var/run/ceph/ceph-client.{0}.*.asok".format(self.client_id)

    @property
    def _prefix(self):
        return ""

    def find_admin_socket(self):
        pyscript = """
import glob
import re
import os
import subprocess

def _find_admin_socket(client_name):
        asok_path = "{asok_path}"
        files = glob.glob(asok_path)
        mountpoint = "{mountpoint}"

        # Given a non-glob path, it better be there
        if "*" not in asok_path:
            assert(len(files) == 1)
            return files[0]

        for f in files:
                pid = re.match(".*\.(\d+)\.asok$", f).group(1)
                if os.path.exists("/proc/{{0}}".format(pid)):
                    with open("/proc/{{0}}/cmdline".format(pid), 'r') as proc_f:
                        contents = proc_f.read()
                        if mountpoint in contents:
                            return f
        raise RuntimeError("Client socket {{0}} not found".format(client_name))

print(_find_admin_socket("{client_name}"))
""".format(
            asok_path=self._asok_path(),
            client_name="client.{0}".format(self.client_id),
            mountpoint=self.mountpoint)

        asok_path = self.run_python(pyscript, sudo=True)
        log.info("Found client admin socket at {0}".format(asok_path))
        return asok_path

    def admin_socket(self, args):
        asok_path = self.find_admin_socket()

        # Query client ID from admin socket, wait 2 seconds
        # and retry 10 times if it is not ready
        c = [
            self._prefix + 'ceph',
            '--admin-daemon',
            asok_path
        ] + args
        with safe_while(sleep=2, tries=10) as proceed:
            while proceed():
                try:
                    p = self._runt(args=c, stdout=StringIO(), stderr=StringIO(), wait=False)
                    p.stdin.close()
                    p.wait()
                    break
                except CommandFailedError:
                    if "connection refused" in p.stderr.getvalue().lower():
                        pass

        return json.loads(p.stdout.getvalue().strip())

    def get_global_id(self):
        """
        Look up the CephFS client ID for this mount
        """
        return self.admin_socket(['mds_sessions'])['id']

    def get_global_inst(self):
        """
        Look up the CephFS client instance for this mount
        """
        return self.inst

    def get_global_addr(self):
        """
        Look up the CephFS client addr for this mount
        """
        return self.addr

    def get_client_pid(self):
        """
        return pid of ceph-fuse process
        """
        status = self.admin_socket(['status'])
        return status['metadata']['pid']

    def get_osd_epoch(self):
        """
        Return 2-tuple of osd_epoch, osd_epoch_barrier
        """
        status = self.admin_socket(['status'])
        return status['osd_epoch'], status['osd_epoch_barrier']

    def get_dentry_count(self):
        """
        Return 2-tuple of dentry_count, dentry_pinned_count
        """
        status = self.admin_socket(['status'])
        return status['dentry_count'], status['dentry_pinned_count']

    def set_cache_size(self, size):
        return self.admin_socket(['config', 'set', 'client_cache_size', str(size)])

    def get_op_read_count(self):
        return self.admin_socket(['perf', 'dump', 'objecter'])['objecter']['osdop_read']
