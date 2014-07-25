import logging
import os

from teuthology.orchestra import run
from teuthology.task.cephfs.mount import CephFSMount

log = logging.getLogger(__name__)


class KernelMount(CephFSMount):
    def __init__(self, mons, test_dir, client_id, client_remote):
        super(KernelMount, self).__init__(test_dir, client_id, client_remote)
        self.mons = mons

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

    def mount(self):
        log.info('Mounting kclient client.{id} at {remote} {mnt}...'.format(
            id=self.client_id, remote=self.client_remote, mnt=self.mountpoint))

        keyring = '/etc/ceph/ceph.client.{id}.keyring'.format(id=self.client_id)
        secret = '{tdir}/data/client.{id}.secret'.format(tdir=self.test_dir, id=self.client_id)
        self.write_secret_file(self.client_remote, 'client.{id}'.format(id=self.client_id),
                               keyring, secret)

        self.client_remote.run(
            args=[
                'mkdir',
                '--',
                self.mountpoint,
            ],
        )

        self.client_remote.run(
            args=[
                'sudo',
                'adjust-ulimits',
                'ceph-coverage',
                '{tdir}/archive/coverage'.format(tdir=self.test_dir),
                '/sbin/mount.ceph',
                '{mons}:/'.format(mons=','.join(self.mons)),
                self.mountpoint,
                '-v',
                '-o',
                'name={id},secretfile={secret}'.format(id=self.client_id,
                                                       secret=secret),
            ],
        )

    def umount(self):
        log.debug('Unmounting client client.{id}...'.format(id=self.client_id))
        mnt = os.path.join(self.test_dir, 'mnt.{id}'.format(id=self.client_id))
        self.client_remote.run(
            args=[
                'sudo',
                'umount',
                mnt,
            ],
        )
        self.client_remote.run(
            args=[
                'rmdir',
                '--',
                mnt,
            ],
        )

    def cleanup(self):
        pass

    def umount_wait(self):
        pass

    def is_mounted(self):
        return True

    def wait_until_mounted(self):
        pass

    def teardown(self):
        super(KernelMount, self).teardown()
        self.umount()
