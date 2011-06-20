import contextlib
import logging
import os

from teuthology import misc as teuthology
from orchestra import run

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    Mount/unmount a ``cfuse`` client.

    The config is optional and defaults to mounting on all clients. If
    a config is given, it is expected to be a list of clients to do
    this operation on. This lets you e.g. set up one client with
    ``cfuse`` and another with ``kclient``.

    Example that mounts all clients::

        tasks:
        - ceph:
        - cfuse:
        - interactive:

    Example that uses both ``kclient` and ``cfuse``::

        tasks:
        - ceph:
        - cfuse: [client.0]
        - kclient: [client.1]
        - interactive:
    """
    log.info('Mounting cfuse clients...')
    assert config is None or isinstance(config, list), \
        "task cfuse got invalid config"
    cfuse_daemons = {}

    if config is None:
        config = ['client.{id}'.format(id=id_)
                  for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client')]
    clients = list(teuthology.get_clients(ctx=ctx, roles=config))

    for id_, remote in clients:
        mnt = os.path.join('/tmp/cephtest', 'mnt.{id}'.format(id=id_))
        remote.run(
            args=[
                'mkdir',
                '--',
                mnt,
                ],
            )
        proc = remote.run(
            args=[
                '/tmp/cephtest/enable-coredump',
                '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
                '/tmp/cephtest/archive/coverage',
                '/tmp/cephtest/daemon-helper',
                'kill',
                '/tmp/cephtest/binary/usr/local/bin/cfuse',
                '-f',
                '--name', 'client.{id}'.format(id=id_),
                '-c', '/tmp/cephtest/ceph.conf',
                # TODO cfuse doesn't understand dash dash '--',
                mnt,
                ],
            logger=log.getChild('cfuse.{id}'.format(id=id_)),
            stdin=run.PIPE,
            wait=False,
            )
        cfuse_daemons[id_] = proc

    for id_, remote in clients:
        mnt = os.path.join('/tmp/cephtest', 'mnt.{id}'.format(id=id_))
        teuthology.wait_until_fuse_mounted(
            remote=remote,
            fuse=cfuse_daemons[id_],
            mountpoint=mnt,
            )

    try:
        yield
    finally:
        log.info('Unmounting cfuse clients...')
        for id_, remote in clients:
            mnt = os.path.join('/tmp/cephtest', 'mnt.{id}'.format(id=id_))
            remote.run(
                args=[
                    'fusermount',
                    '-u',
                    mnt,
                    ],
                )
        run.wait(cfuse_daemons.itervalues())

        for id_, remote in clients:
            mnt = os.path.join('/tmp/cephtest', 'mnt.{id}'.format(id=id_))
            remote.run(
                args=[
                    'rmdir',
                    '--',
                    mnt,
                    ],
                )
