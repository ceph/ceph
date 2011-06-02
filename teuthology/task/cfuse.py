import contextlib
import logging
import os

from teuthology import misc as teuthology
from orchestra import run

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    log.info('Mounting cfuse clients...')
    assert isinstance(config, list), \
        "task fuse automatic configuration not supported yet, list all clients"
    cfuse_daemons = {}

    for role in config:
        assert isinstance(role, basestring)
        PREFIX = 'client.'
        assert role.startswith(PREFIX)
        id_ = role[len(PREFIX):]
        (remote,) = ctx.cluster.only(role).remotes.iterkeys()
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
                '/tmp/cephtest/daemon-helper',
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

    for role in config:
        assert isinstance(role, basestring)
        PREFIX = 'client.'
        assert role.startswith(PREFIX)
        id_ = role[len(PREFIX):]
        (remote,) = ctx.cluster.only(role).remotes.iterkeys()
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
        for role in config:
            assert isinstance(role, basestring)
            PREFIX = 'client.'
            assert role.startswith(PREFIX)
            id_ = role[len(PREFIX):]
            (remote,) = ctx.cluster.only(role).remotes.iterkeys()
            mnt = os.path.join('/tmp/cephtest', 'mnt.{id}'.format(id=id_))
            remote.run(
                args=[
                    'fusermount',
                    '-u',
                    mnt,
                    ],
                )
        run.wait(cfuse_daemons.itervalues())
