from io import StringIO
import json
import time
import logging
import re
import six

from textwrap import dedent

from teuthology import misc
from teuthology.contextutil import MaxWhileTries
from teuthology.contextutil import safe_while
from teuthology.orchestra import run
from teuthology.orchestra.run import CommandFailedError
from tasks.cephfs.mount import CephFSMount

log = logging.getLogger(__name__)


class FuseMount(CephFSMount):
    def __init__(self, ctx, client_config, test_dir, client_id, client_remote, brxnet):
        super(FuseMount, self).__init__(ctx, test_dir, client_id, client_remote, brxnet)

        self.client_config = client_config if client_config else {}
        self.fuse_daemon = None
        self._fuse_conn = None
        self.id = None
        self.inst = None
        self.addr = None

    def mount(self, mount_path=None, mount_fs_name=None, mountpoint=None, mount_options=[]):
        if mountpoint is not None:
            self.mountpoint = mountpoint
        self.setupfs(name=mount_fs_name)
        self.setup_netns()

        try:
            return self._mount(mount_path, mount_fs_name, mount_options)
        except RuntimeError:
            # Catch exceptions by the mount() logic (i.e. not remote command
            # failures) and ensure the mount is not left half-up.
            # Otherwise we might leave a zombie mount point that causes
            # anyone traversing cephtest/ to get hung up on.
            log.warning("Trying to clean up after failed mount")
            self.umount_wait(force=True)
            raise

    def _mount(self, mount_path, mount_fs_name, mount_options):
        log.info("Client client.%s config is %s" % (self.client_id, self.client_config))

        daemon_signal = 'kill'
        if self.client_config.get('coverage') or self.client_config.get('valgrind') is not None:
            daemon_signal = 'term'

        log.info('Mounting ceph-fuse client.{id} at {remote} {mnt}...'.format(
            id=self.client_id, remote=self.client_remote, mnt=self.mountpoint))

        self.client_remote.run(args=['mkdir', '-p', self.mountpoint],
                               timeout=(15*60), cwd=self.test_dir)

        run_cmd = [
            'sudo',
            'adjust-ulimits',
            'ceph-coverage',
            '{tdir}/archive/coverage'.format(tdir=self.test_dir),
            'daemon-helper',
            daemon_signal,
        ]

        fuse_cmd = ['ceph-fuse', "-f"]

        if mount_path is not None:
            fuse_cmd += ["--client_mountpoint={0}".format(mount_path)]

        if mount_fs_name is not None:
            fuse_cmd += ["--client_fs={0}".format(mount_fs_name)]

        fuse_cmd += mount_options

        fuse_cmd += [
            '--name', 'client.{id}'.format(id=self.client_id),
            # TODO ceph-fuse doesn't understand dash dash '--',
            self.mountpoint,
        ]

        cwd = self.test_dir
        if self.client_config.get('valgrind') is not None:
            run_cmd = misc.get_valgrind_args(
                self.test_dir,
                'client.{id}'.format(id=self.client_id),
                run_cmd,
                self.client_config.get('valgrind'),
            )
            cwd = None # misc.get_valgrind_args chdir for us

        netns_prefix = ['sudo', 'nsenter',
                        '--net=/var/run/netns/{0}'.format(self.netns_name)]
        run_cmd = netns_prefix + run_cmd

        run_cmd.extend(fuse_cmd)

        def list_connections():
            from teuthology.misc import get_system_type

            conn_dir = "/sys/fs/fuse/connections"

            self.client_remote.run(args=['sudo', 'modprobe', 'fuse'],
                                   check_status=False)
            self.client_remote.run(
                args=["sudo", "mount", "-t", "fusectl", conn_dir, conn_dir],
                check_status=False, timeout=(30))

            try:
                ls_str = self.client_remote.sh("ls " + conn_dir,
                                               stdout=StringIO(),
                                               timeout=(15*60)).strip()
            except CommandFailedError:
                return []

            if ls_str:
                return [int(n) for n in ls_str.split("\n")]
            else:
                return []

        # Before starting ceph-fuse process, note the contents of
        # /sys/fs/fuse/connections
        pre_mount_conns = list_connections()
        log.info("Pre-mount connections: {0}".format(pre_mount_conns))

        proc = self.client_remote.run(
            args=run_cmd,
            cwd=cwd,
            logger=log.getChild('ceph-fuse.{id}'.format(id=self.client_id)),
            stdin=run.PIPE,
            wait=False,
        )
        self.fuse_daemon = proc

        # Wait for the connection reference to appear in /sys
        mount_wait = self.client_config.get('mount_wait', 0)
        if mount_wait > 0:
            log.info("Fuse mount waits {0} seconds before checking /sys/".format(mount_wait))
            time.sleep(mount_wait)            
        timeout = int(self.client_config.get('mount_timeout', 30))
        waited = 0

        post_mount_conns = list_connections()
        while len(post_mount_conns) <= len(pre_mount_conns):
            if self.fuse_daemon.finished:
                # Did mount fail?  Raise the CommandFailedError instead of
                # hitting the "failed to populate /sys/" timeout
                self.fuse_daemon.wait()
            time.sleep(1)
            waited += 1
            if waited > timeout:
                raise RuntimeError("Fuse mount failed to populate /sys/ after {0} seconds".format(
                    waited
                ))
            else:
                post_mount_conns = list_connections()

        log.info("Post-mount connections: {0}".format(post_mount_conns))

        # Record our fuse connection number so that we can use it when
        # forcing an unmount
        new_conns = list(set(post_mount_conns) - set(pre_mount_conns))
        if len(new_conns) == 0:
            raise RuntimeError("New fuse connection directory not found ({0})".format(new_conns))
        elif len(new_conns) > 1:
            raise RuntimeError("Unexpectedly numerous fuse connections {0}".format(new_conns))
        else:
            self._fuse_conn = new_conns[0]

        self.gather_mount_info()

        self.mounted = True

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
        proc = self.client_remote.run(
            args=[
                'stat',
                '--file-system',
                '--printf=%T\n',
                '--',
                self.mountpoint,
            ],
            cwd=self.test_dir,
            stdout=StringIO(),
            stderr=StringIO(),
            wait=False,
            timeout=(15*60)
        )
        try:
            proc.wait()
        except CommandFailedError:
            error = proc.stderr.getvalue()
            if ("endpoint is not connected" in error
            or "Software caused connection abort" in error):
                # This happens is fuse is killed without unmount
                log.warning("Found stale moutn point at {0}".format(self.mountpoint))
                return True
            else:
                # This happens if the mount directory doesn't exist
                log.info('mount point does not exist: %s', self.mountpoint)
                return False

        fstype = six.ensure_str(proc.stdout.getvalue()).rstrip('\n')
        if fstype == 'fuseblk':
            log.info('ceph-fuse is mounted on %s', self.mountpoint)
            return True
        else:
            log.debug('ceph-fuse not mounted, got fs type {fstype!r}'.format(
                fstype=fstype))
            return False

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

        self.mounted = True

        # Now that we're mounted, set permissions so that the rest of the test will have
        # unrestricted access to the filesystem mount.
        for retry in range(10):
            try:
                stderr = StringIO()
                self.client_remote.run(args=['sudo', 'chmod', '1777', self.mountpoint],
                                       timeout=(15*60), cwd=self.test_dir, stderr=stderr)
                break
            except run.CommandFailedError:
                stderr = stderr.getvalue()
                if "Read-only file system".lower() in stderr.lower():
                    break
                elif "Permission denied".lower() in stderr.lower():
                    time.sleep(5)
                else:
                    raise

    def _mountpoint_exists(self):
        return self.client_remote.run(args=["ls", "-d", self.mountpoint], check_status=False, cwd=self.test_dir, timeout=(15*60)).exitstatus == 0

    def umount(self, cleanup=True):
        """
        umount() must not run cleanup() when it's called by umount_wait()
        since "run.wait([self.fuse_daemon], timeout)" would hang otherwise.
        """
        if not self.is_mounted():
            if cleanup:
                self.cleanup()
            return

        try:
            log.info('Running fusermount -u on {name}...'.format(name=self.client_remote.name))
            stderr = StringIO()
            self.client_remote.run(
                args = [
                    'sudo',
                    'fusermount',
                    '-u',
                    self.mountpoint,
                ],
                cwd=self.test_dir,
                stderr=stderr,
                timeout=(30*60),
            )
        except run.CommandFailedError:
            if "mountpoint not found" in stderr.getvalue():
                # This happens if the mount directory doesn't exist
                log.info('mount point does not exist: %s', self.mountpoint)
            elif "not mounted" in stderr.getvalue():
                # This happens if the mount directory already unmouted
                log.info('mount point not mounted: %s', self.mountpoint)
            else:
                log.info('Failed to unmount ceph-fuse on {name}, aborting...'.format(name=self.client_remote.name))

                self.client_remote.run(args=[
                    'sudo',
                    run.Raw('PATH=/usr/sbin:$PATH'),
                    'lsof',
                    run.Raw(';'),
                    'ps',
                    'auxf',
                ], timeout=(60*15))

                # abort the fuse mount, killing all hung processes
                if self._fuse_conn:
                    self.run_python(dedent("""
                    import os
                    path = "/sys/fs/fuse/connections/{0}/abort"
                    if os.path.exists(path):
                        open(path, "w").write("1")
                    """).format(self._fuse_conn))
                    self._fuse_conn = None

                stderr = StringIO()
                # make sure its unmounted
                try:
                    self.client_remote.run(
                        args=[
                            'sudo',
                            'umount',
                            '-l',
                            '-f',
                            self.mountpoint,
                        ],
                        stderr=stderr,
                        timeout=(60*15)
                    )
                except CommandFailedError:  
                    if self.is_mounted():   
                        raise

        self.mounted = False
        self._fuse_conn = None
        self.id = None
        self.inst = None
        self.addr = None
        if cleanup:
            self.cleanup()

    def umount_wait(self, force=False, require_clean=False, timeout=900):
        """
        :param force: Complete cleanly even if the MDS is offline
        """
        if not (self.is_mounted() and self.fuse_daemon):
            log.debug('ceph-fuse client.{id} is not mounted at {remote} {mnt}'.format(id=self.client_id,
                                                                                      remote=self.client_remote,
                                                                                      mnt=self.mountpoint))
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

        # cleanup is set to to fail since clieanup must happen after umount is
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

        self.mounted = False
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

        self.mounted = False

        # Indiscriminate, unlike the touchier cleanup()
        self.client_remote.run(
            args=[
                'rm',
                '-rf',
                self.mountpoint,
            ],
            cwd=self.test_dir,
            timeout=(60*5)
        )

    def _asok_path(self):
        return "/var/run/ceph/ceph-client.{0}.*.asok".format(self.client_id)

    @property
    def _prefix(self):
        return ""

    def admin_socket(self, args):
        pyscript = """
import glob
import re
import os
import subprocess

def find_socket(client_name):
        asok_path = "{asok_path}"
        files = glob.glob(asok_path)

        # Given a non-glob path, it better be there
        if "*" not in asok_path:
            assert(len(files) == 1)
            return files[0]

        for f in files:
                pid = re.match(".*\.(\d+)\.asok$", f).group(1)
                if os.path.exists("/proc/{{0}}".format(pid)):
                        return f
        raise RuntimeError("Client socket {{0}} not found".format(client_name))

print(find_socket("{client_name}"))
""".format(
            asok_path=self._asok_path(),
            client_name="client.{0}".format(self.client_id))

        # Find the admin socket
        asok_path = self.client_remote.sh(
            ['sudo', 'python3', '-c', pyscript],
            stdout=StringIO(),
            timeout=(15*60)).strip()
        log.info("Found client admin socket at {0}".format(asok_path))

        # Query client ID from admin socket, wait 2 seconds
        # and retry 10 times if it is not ready
        with safe_while(sleep=2, tries=10) as proceed:
            while proceed():
                try:
                    p = self.client_remote.run(args=
                        ['sudo', self._prefix + 'ceph', '--admin-daemon', asok_path] + args,
                        stdout=StringIO(), stderr=StringIO(),
                        timeout=(15*60))
                    break
                except CommandFailedError:
                    if "Connection refused" in stderr.getvalue():
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
