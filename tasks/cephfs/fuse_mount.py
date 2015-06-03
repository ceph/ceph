
from StringIO import StringIO
import json
import time
import os
import logging
from textwrap import dedent

from teuthology import misc
from teuthology.contextutil import MaxWhileTries
from teuthology.orchestra import run
from teuthology.orchestra.run import CommandFailedError
from .mount import CephFSMount

log = logging.getLogger(__name__)


class FuseMount(CephFSMount):
    def __init__(self, client_config, test_dir, client_id, client_remote):
        super(FuseMount, self).__init__(test_dir, client_id, client_remote)

        self.client_config = client_config if client_config else {}
        self.fuse_daemon = None
        self._fuse_conn = None

    def mount(self):
        log.info("Client client.%s config is %s" % (self.client_id, self.client_config))

        daemon_signal = 'kill'
        if self.client_config.get('coverage') or self.client_config.get('valgrind') is not None:
            daemon_signal = 'term'

        mnt = os.path.join(self.test_dir, 'mnt.{id}'.format(id=self.client_id))
        log.info('Mounting ceph-fuse client.{id} at {remote} {mnt}...'.format(
            id=self.client_id, remote=self.client_remote, mnt=mnt))

        self.client_remote.run(
            args=[
                'mkdir',
                '--',
                mnt,
            ],
        )

        run_cmd = [
            'sudo',
            'adjust-ulimits',
            'ceph-coverage',
            '{tdir}/archive/coverage'.format(tdir=self.test_dir),
            'daemon-helper',
            daemon_signal,
        ]
        run_cmd_tail = [
            'ceph-fuse',
            '-f',
            '--name', 'client.{id}'.format(id=self.client_id),
            # TODO ceph-fuse doesn't understand dash dash '--',
            mnt,
        ]

        if self.client_config.get('valgrind') is not None:
            run_cmd = misc.get_valgrind_args(
                self.test_dir,
                'client.{id}'.format(id=self.client_id),
                run_cmd,
                self.client_config.get('valgrind'),
            )

        run_cmd.extend(run_cmd_tail)

        def list_connections():
            self.client_remote.run(
                args=["sudo", "mount", "-t", "fusectl", "/sys/fs/fuse/connections", "/sys/fs/fuse/connections"],
                check_status=False
            )
            p = self.client_remote.run(
                args=["ls", "/sys/fs/fuse/connections"],
                stdout=StringIO(),
                check_status=False
            )
            if p.exitstatus != 0:
                return []

            ls_str = p.stdout.getvalue().strip()
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
            logger=log.getChild('ceph-fuse.{id}'.format(id=self.client_id)),
            stdin=run.PIPE,
            wait=False,
        )
        self.fuse_daemon = proc

        # Wait for the connection reference to appear in /sys
        waited = 0
        while list_connections() == pre_mount_conns:
            time.sleep(1)
            waited += 1
            if waited > 30:
                raise RuntimeError("Fuse mount failed to populate /sys/ after {0} seconds".format(
                    waited
                ))

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

    def is_mounted(self):
        try:
            proc = self.client_remote.run(
                args=[
                    'stat',
                    '--file-system',
                    '--printf=%T\n',
                    '--',
                    self.mountpoint,
                ],
                stdout=StringIO(),
            )
        except CommandFailedError:
            # This happens if the mount directory doesn't exist
            log.info('mount point does not exist: %s', self.mountpoint)
            return False

        fstype = proc.stdout.getvalue().rstrip('\n')
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

        while not self.is_mounted():
            # Even if it's not mounted, it should at least
            # be running: catch simple failures where it has terminated.
            assert not self.fuse_daemon.poll()

            time.sleep(5)

        # Now that we're mounted, set permissions so that the rest of the test will have
        # unrestricted access to the filesystem mount.
        self.client_remote.run(
            args=['sudo', 'chmod', '1777', self.mountpoint])

    def _mountpoint_exists(self):
        return self.client_remote.run(args=["ls", "-d", self.mountpoint], check_status=False).exitstatus == 0

    def umount(self):
        try:
            log.info('Running fusermount -u on {name}...'.format(name=self.client_remote.name))
            self.client_remote.run(
                args=[
                    'sudo',
                    'fusermount',
                    '-u',
                    self.mountpoint,
                ],
            )
        except run.CommandFailedError:
            log.info('Failed to unmount ceph-fuse on {name}, aborting...'.format(name=self.client_remote.name))

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
            try:
                # make sure its unmounted
                self.client_remote.run(
                    args=[
                        'sudo',
                        'umount',
                        '-l',
                        '-f',
                        self.mountpoint,
                    ],
                    stderr=stderr
                )
            except CommandFailedError:
                if "not found" in stderr.getvalue():
                    # Missing mount point, so we are unmounted already, yay.
                    pass
                else:
                    raise

        assert not self.is_mounted()
        self._fuse_conn = None

    def umount_wait(self, force=False):
        """
        :param force: Complete cleanly even if the MDS is offline
        """
        if force:
            # When we expect to be forcing, kill the ceph-fuse process directly.
            # This should avoid hitting the more aggressive fallback killing
            # in umount() which can affect other mounts too.
            self.fuse_daemon.stdin.close()

            # However, we will still hit the aggressive wait if there is an ongoing
            # mount -o remount (especially if the remount is stuck because MDSs
            # are unavailable)

        self.umount()

        try:
            if self.fuse_daemon:
                # Permit a timeout, so that we do not block forever
                run.wait([self.fuse_daemon], 30)
        except MaxWhileTries:
            log.error("process failed to terminate after unmount.  This probably"
                      "indicates a bug within ceph-fuse.")
            raise
        except CommandFailedError:
            pass

        self.cleanup()

    def cleanup(self):
        """
        Remove the mount point.

        Prerequisite: the client is not mounted.
        """
        stderr = StringIO()
        try:
            self.client_remote.run(
                args=[
                    'rmdir',
                    '--',
                    self.mountpoint,
                ],
                stderr=stderr
            )
        except CommandFailedError:
            if "No such file or directory" in stderr.getvalue():
                pass
            else:
                raise

    def kill(self):
        """
        Terminate the client without removing the mount point.
        """
        self.fuse_daemon.stdin.close()
        try:
            self.fuse_daemon.wait()
        except CommandFailedError:
            pass

    def kill_cleanup(self):
        """
        Follow up ``kill`` to get to a clean unmounted state.
        """
        self.umount()
        self.cleanup()

    def teardown(self):
        """
        Whatever the state of the mount, get it gone.
        """
        super(FuseMount, self).teardown()

        self.umount()

        if not self.fuse_daemon.finished:
            self.fuse_daemon.stdin.close()
            try:
                self.fuse_daemon.wait()
            except CommandFailedError:
                pass

        # Indiscriminate, unlike the touchier cleanup()
        self.client_remote.run(
            args=[
                'rm',
                '-rf',
                self.mountpoint,
            ],
        )

    def _admin_socket(self, args):
        pyscript = """
import glob
import re
import os
import subprocess

def find_socket(client_name):
        files = glob.glob("/var/run/ceph/ceph-{{client_name}}.*.asok".format(client_name=client_name))
        for f in files:
                pid = re.match(".*\.(\d+)\.asok$", f).group(1)
                if os.path.exists("/proc/{{0}}".format(pid)):
                        return f
        raise RuntimeError("Client socket {{0}} not found".format(client_name))

print find_socket("{client_name}")
""".format(client_name="client.{0}".format(self.client_id))

        # Find the admin socket
        p = self.client_remote.run(args=[
            'python', '-c', pyscript
        ], stdout=StringIO())
        asok_path = p.stdout.getvalue().strip()
        log.info("Found client admin socket at {0}".format(asok_path))

        # Query client ID from admin socket
        p = self.client_remote.run(
            args=['sudo', 'ceph', '--admin-daemon', asok_path] + args,
            stdout=StringIO())
        return json.loads(p.stdout.getvalue())

    def get_global_id(self):
        """
        Look up the CephFS client ID for this mount
        """

        return self._admin_socket(['mds_sessions'])['id']

    def get_osd_epoch(self):
        """
        Return 2-tuple of osd_epoch, osd_epoch_barrier
        """
        status = self._admin_socket(['status'])
        return status['osd_epoch'], status['osd_epoch_barrier']

    def get_dentry_count(self):
        """
        Return 2-tuple of dentry_count, dentry_pinned_count
        """
        status = self._admin_socket(['status'])
        return status['dentry_count'], status['dentry_pinned_count']

    def set_cache_size(self, size):
        return self._admin_socket(['config', 'set', 'client_cache_size', str(size)])
