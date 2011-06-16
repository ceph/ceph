import contextlib
import logging
import os

from teuthology import misc as teuthology

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    Mount/unmount a ``kernel`` client.

    The config is expected to be a list of clients to do this
    operation on. This lets you e.g. set up one client with ``cfuse``
    and another with ``kclient``.

        tasks:
        - ceph:
        - cfuse: [client.0]
        - kclient: [client.1]
        - interactive:
    """
    log.info('Mounting kernel clients...')
    assert isinstance(config, list), \
        "task kclient automatic configuration not supported yet, list all clients"

    for role in config:
        log.debug('Mounting client {role}...'.format(role=role))
        assert isinstance(role, basestring)
        PREFIX = 'client.'
        assert role.startswith(PREFIX)
        id_ = role[len(PREFIX):]
        (remote,) = ctx.cluster.only(role).remotes.iterkeys()
        remotes_and_roles = ctx.cluster.remotes.items()
        roles = [roles for (remote, roles) in remotes_and_roles]
        ips = [host for (host, port) in (remote.ssh.get_transport().getpeername() for (remote, roles) in remotes_and_roles)]
        mons = teuthology.get_mons(roles, ips).values()
        mnt = os.path.join('/tmp/cephtest', 'mnt.{id}'.format(id=id_))
        secret = '/tmp/cephtest/data/{role}.secret'.format(role=role)
        teuthology.write_secret_file(remote, role, secret)

        remote.run(
            args=[
                'mkdir',
                '--',
                mnt,
                ],
            )

        remote.run(
            args=[
                'sudo',
                '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
                '/tmp/cephtest/archive/coverage',
                '/tmp/cephtest/binary/usr/local/sbin/mount.ceph',
                '{mons}:/'.format(mons=','.join(mons)),
                mnt,
                '-v',
                '-o',
                'name={id},secretfile={secret}'.format(id=id_,
                                                       secret=secret),
                ],
            )

    try:
        yield
    finally:
        log.info('Unmounting kernel clients...')
        for role in config:
            log.debug('Unmounting client {role}...'.format(role=role))
            assert isinstance(role, basestring)
            PREFIX = 'client.'
            assert role.startswith(PREFIX)
            id_ = role[len(PREFIX):]
            (remote,) = ctx.cluster.only(role).remotes.iterkeys()
            mnt = os.path.join('/tmp/cephtest', 'mnt.{id}'.format(id=id_))
            remote.run(
                args=[
                    'sudo',
                    'umount',
                    mnt,
                    ],
                )
            remote.run(
                args=[
                    'rmdir',
                    '--',
                    mnt,
                    ],
                )
