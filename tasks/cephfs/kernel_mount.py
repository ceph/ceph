from StringIO import StringIO
import json
import logging
from textwrap import dedent
from teuthology.orchestra.run import CommandFailedError
from teuthology import misc

from teuthology.orchestra import remote as orchestra_remote
from teuthology.orchestra import run
from .mount import CephFSMount

log = logging.getLogger(__name__)


class KernelMount(CephFSMount):
    def __init__(self, mons, test_dir, client_id, client_remote,
                 ipmi_user, ipmi_password, ipmi_domain):
        super(KernelMount, self).__init__(test_dir, client_id, client_remote)
        self.mons = mons

        self.mounted = False
        self.ipmi_user = ipmi_user
        self.ipmi_password = ipmi_password
        self.ipmi_domain = ipmi_domain

    def write_secret_file(self, remote, role, keyring, filename):
        """
        Stash the keyring in the filename specified.
        """
        remote.run(
            args=[
                'adjust-ulimits',
                'ceph-coverage',
                '{tdir}/archive/coverage'.format(tdir=self.test_dir),
                'ceph-authtool',
                '--name={role}'.format(role=role),
                '--print-key',
                keyring,
                run.Raw('>'),
                filename,
            ],
        )

    def mount(self, mount_path=None):
        log.info('Mounting kclient client.{id} at {remote} {mnt}...'.format(
            id=self.client_id, remote=self.client_remote, mnt=self.mountpoint))

        keyring = self.get_keyring_path()
        secret = '{tdir}/ceph.data/client.{id}.secret'.format(tdir=self.test_dir, id=self.client_id)
        self.write_secret_file(self.client_remote, 'client.{id}'.format(id=self.client_id),
                               keyring, secret)

        self.client_remote.run(
            args=[
                'mkdir',
                '--',
                self.mountpoint,
            ],
        )

        if mount_path is None:
            mount_path = "/"

        self.client_remote.run(
            args=[
                'sudo',
                'adjust-ulimits',
                'ceph-coverage',
                '{tdir}/archive/coverage'.format(tdir=self.test_dir),
                '/sbin/mount.ceph',
                '{mons}:{mount_path}'.format(mons=','.join(self.mons), mount_path=mount_path),
                self.mountpoint,
                '-v',
                '-o',
                'name={id},secretfile={secret}'.format(id=self.client_id,
                                                       secret=secret),
            ],
        )

        self.client_remote.run(
            args=['sudo', 'chmod', '1777', self.mountpoint])

        self.mounted = True

    def umount(self):
        log.debug('Unmounting client client.{id}...'.format(id=self.client_id))
        self.client_remote.run(
            args=[
                'sudo',
                'umount',
                self.mountpoint,
            ],
        )
        self.client_remote.run(
            args=[
                'rmdir',
                '--',
                self.mountpoint,
            ],
        )
        self.mounted = False

    def cleanup(self):
        pass

    def umount_wait(self, force=False, require_clean=False):
        """
        Unlike the fuse client, the kernel client's umount is immediate
        """
        try:
            self.umount()
        except CommandFailedError:
            if not force:
                raise

            self.kill()
            self.kill_cleanup()

        self.mounted = False

    def is_mounted(self):
        return self.mounted

    def wait_until_mounted(self):
        """
        Unlike the fuse client, the kernel client is up and running as soon
        as the initial mount() function returns.
        """
        assert self.mounted

    def teardown(self):
        super(KernelMount, self).teardown()
        if self.mounted:
            self.umount()

    def kill(self):
        """
        The Ceph kernel client doesn't have a mechanism to kill itself (doing
        that in side the kernel would be weird anyway), so we reboot the whole node
        to get the same effect.

        We use IPMI to reboot, because we don't want the client to send any
        releases of capabilities.
        """

        con = orchestra_remote.getRemoteConsole(self.client_remote.hostname,
                                                self.ipmi_user,
                                                self.ipmi_password,
                                                self.ipmi_domain)
        con.power_off()

        self.mounted = False

    def kill_cleanup(self):
        assert not self.mounted

        con = orchestra_remote.getRemoteConsole(self.client_remote.hostname,
                                                self.ipmi_user,
                                                self.ipmi_password,
                                                self.ipmi_domain)
        con.power_on()

        # Wait for node to come back up after reboot
        misc.reconnect(None, 300, [self.client_remote])

        # Remove mount directory
        self.client_remote.run(
            args=[
                'rmdir',
                '--',
                self.mountpoint,
            ],
        )

    def _find_debug_dir(self):
        """
        Find the debugfs folder for this mount
        """
        pyscript = dedent("""
            import glob
            import os
            import json

            def get_id_to_dir():
                result = {}
                for dir in glob.glob("/sys/kernel/debug/ceph/*"):
                    mds_sessions_lines = open(os.path.join(dir, "mds_sessions")).readlines()
                    client_id = mds_sessions_lines[1].split()[1].strip('"')

                    result[client_id] = dir
                return result

            print json.dumps(get_id_to_dir())
            """)

        p = self.client_remote.run(args=[
            'sudo', 'python', '-c', pyscript
        ], stdout=StringIO())
        client_id_to_dir = json.loads(p.stdout.getvalue())

        try:
            return client_id_to_dir[self.client_id]
        except KeyError:
            log.error("Client id '{0}' debug dir not found (clients seen were: {1})".format(
                self.client_id, ",".join(client_id_to_dir.keys())
            ))
            raise

    def _read_debug_file(self, filename):
        debug_dir = self._find_debug_dir()

        pyscript = dedent("""
            import os

            print open(os.path.join("{debug_dir}", "{filename}")).read()
            """).format(debug_dir=debug_dir, filename=filename)

        p = self.client_remote.run(args=[
            'sudo', 'python', '-c', pyscript
        ], stdout=StringIO())
        return p.stdout.getvalue()

    def get_global_id(self):
        """
        Look up the CephFS client ID for this mount, using debugfs.
        """

        assert self.mounted

        mds_sessions = self._read_debug_file("mds_sessions")
        lines = mds_sessions.split("\n")
        return int(lines[0].split()[1])

    def get_osd_epoch(self):
        """
        Return 2-tuple of osd_epoch, osd_epoch_barrier
        """
        osd_map = self._read_debug_file("osdmap")
        lines = osd_map.split("\n")
        epoch = int(lines[0].split()[1])

        mds_sessions = self._read_debug_file("mds_sessions")
        lines = mds_sessions.split("\n")
        epoch_barrier = int(lines[2].split()[1].strip('"'))

        return epoch, epoch_barrier
