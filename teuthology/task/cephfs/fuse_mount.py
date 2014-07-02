
from StringIO import StringIO
import time
import os
import logging

from teuthology import misc
from ...orchestra import run

log = logging.getLogger(__name__)


class FuseMount(object):
    def __init__(self, client_config, test_dir, client_id, client_remote):
        """
        :param client_config: Configuration dictionary for this particular client
        :param test_dir: Global teuthology test dir
        :param client_id: Client ID, the 'foo' in client.foo
        :param client_remote: Remote instance for the host where client will run
        """

        self.client_config = client_config
        self.test_dir = test_dir
        self.client_id = client_id
        self.client_remote = client_remote
        self.fuse_daemon = None

        self.mountpoint = os.path.join(self.test_dir, 'mnt.{id}'.format(id=self.client_id))


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

        proc = self.client_remote.run(
            args=run_cmd,
            logger=log.getChild('ceph-fuse.{id}'.format(id=self.client_id)),
            stdin=run.PIPE,
            wait=False,
        )
        self.fuse_daemon = proc

    def is_mounted(self):
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
        fstype = proc.stdout.getvalue().rstrip('\n')
        if fstype == 'fuseblk':
            log.info('ceph-fuse is mounted on %s', self.mountpoint)
            return True
        else:
            log.debug('ceph-fuse not mounted, got fs type {fstype!r}'.format(
                fstype=fstype))

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
            args=['sudo', 'chmod', '1777', '{tdir}/mnt.{id}'.format(tdir=self.test_dir, id=self.client_id)], )

    def umount(self):
        mnt = os.path.join(self.test_dir, 'mnt.{id}'.format(id=self.client_id))
        try:
            self.client_remote.run(
                args=[
                    'sudo',
                    'fusermount',
                    '-u',
                    mnt,
                ],
            )
        except run.CommandFailedError:
            log.info('Failed to unmount ceph-fuse on {name}, aborting...'.format(name=self.client_remote.name))
            # abort the fuse mount, killing all hung processes
            self.client_remote.run(
                args=[
                    'if', 'test', '-e', '/sys/fs/fuse/connections/*/abort',
                    run.Raw(';'), 'then',
                    'echo',
                    '1',
                    run.Raw('>'),
                    run.Raw('/sys/fs/fuse/connections/*/abort'),
                    run.Raw(';'), 'fi',
                ],
            )
            # make sure its unmounted
            self.client_remote.run(
                args=[
                    'sudo',
                    'umount',
                    '-l',
                    '-f',
                    mnt,
                ],
            )

    def cleanup(self):
        """
        Remove the mount point.

        Prerequisite: the client is not mounted.
        """
        mnt = os.path.join(self.test_dir, 'mnt.{id}'.format(id=self.client_id))
        self.client_remote.run(
            args=[
                'rmdir',
                '--',
                mnt,
            ],
        )
