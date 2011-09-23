import contextlib
import logging
import os

from teuthology import misc as teuthology
from ..orchestra import run

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    Mount/unmount a ``ceph-fuse`` client.

    The config is optional and defaults to mounting on all clients. If
    a config is given, it is expected to be a list of clients to do
    this operation on. This lets you e.g. set up one client with
    ``ceph-fuse`` and another with ``kclient``.

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

    Example that enables valgrind:

        tasks:
        - ceph:
        - cfuse:
            client.0:
              valgrind: --tool=memcheck
        - interactive:

    """
    log.info('Mounting ceph-fuse clients...')
    cfuse_daemons = {}

    if config is None:
        config = dict(('client.{id}'.format(id=id_), None)
                  for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client'))
    elif isinstance(config, list):
        config = dict((name, None) for name in config)

    clients = list(teuthology.get_clients(ctx=ctx, roles=config.keys()))

    for id_, remote in clients:
        mnt = os.path.join('/tmp/cephtest', 'mnt.{id}'.format(id=id_))
        log.info('Mounting ceph-fuse client.{id} at {remote} {mnt}...'.format(
                id=id_, remote=remote,mnt=mnt))

        client_config = config.get("client.%s" % id_)
        if client_config is None:
            client_config = {}
        log.info("Client client.%s config is %s" % (id_, client_config))

        daemon_signal = 'kill'
        if client_config.get('coverage'):
            log.info('Recording coverage for this run.')
            daemon_signal = 'term'

        remote.run(
            args=[
                'mkdir',
                '--',
                mnt,
                ],
            )

        run_cmd=[
            '/tmp/cephtest/enable-coredump',
            '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
            '/tmp/cephtest/archive/coverage',
            '/tmp/cephtest/daemon-helper',
            ]
        run_cmd_tail=[
            '/tmp/cephtest/binary/usr/local/bin/ceph-fuse',
            '-f',
            '--name', 'client.{id}'.format(id=id_),
            '-c', '/tmp/cephtest/ceph.conf',
            # TODO ceph-fuse doesn't understand dash dash '--',
            mnt,
            ]

        extra_args = None
        if client_config.get('valgrind') is not None:
                log.debug('Running client.{id} under valgrind'.format(id=id_))
                val_path = '/tmp/cephtest/archive/log/valgrind'
                daemon_signal = 'term'
                remote.run(
                    args=[
                        'mkdir', '-p', '--', val_path,
                        ],
                    wait=True,
                    )
                extra_args = [
                    'valgrind',
                    '--log-file={vdir}/client.{id}.log'.format(vdir=val_path,
                                                               id=id_),
                    client_config.get('valgrind')
                    ]
        
        run_cmd.append(daemon_signal)
        if extra_args is not None:
            run_cmd.extend(extra_args)
        run_cmd.extend(run_cmd_tail)

        proc = remote.run(
            args=run_cmd,
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
        log.info('Unmounting ceph-fuse clients...')
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
