import contextlib
import logging

from ..orchestra import run
from teuthology import misc as teuthology

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    Run radosbench

    The config should be as follows:

    radosbench:
        clients: [client list]
        time: <seconds to run>
        pool: <pool to use>

    example:

    tasks:
    - ceph:
    - radosbench:
        clients: [client.0]
        time: 360
    - interactive:
    """
    log.info('Beginning radosbench...')
    assert isinstance(config, dict), \
        "please list clients to run on"
    radosbench = {}

    testdir = teuthology.get_testdir(ctx)

    for role in config.get('clients', ['client.0']):
        assert isinstance(role, basestring)
        PREFIX = 'client.'
        assert role.startswith(PREFIX)
        id_ = role[len(PREFIX):]
        (remote,) = ctx.cluster.only(role).remotes.iterkeys()

        if config.get('pool', 'data') is not "data":
            proc = remote.run(
                args=[
                    "/bin/sh", "-c",
                    " ".join(['LD_LIBRARY_PATH={tdir}/binary/usr/local/lib',
                              '{tdir}/enable-coredump',
                              '{tdir}/binary/usr/local/bin/ceph-coverage',
                              '{tdir}/archive/coverage',
                              '{tdir}/binary/usr/local/bin/rados',
                              '-c', '{tdir}/ceph.conf',
                              '-k', '{tdir}/data/{role}.keyring'.format(role=role),
                              '--name', role,
                              'mkpool', str(config.get('pool', 'data'))
                              ]).format(tdir=testdir),
                    ],
                logger=log.getChild('radosbench.{id}'.format(id=id_)),
                stdin=run.PIPE,
                wait=False
                )
            run.wait([proc])

        proc = remote.run(
            args=[
                "/bin/sh", "-c",
                " ".join(['LD_LIBRARY_PATH={tdir}/binary/usr/local/lib',
                          '{tdir}/enable-coredump',
                          '{tdir}/binary/usr/local/bin/ceph-coverage',
                          '{tdir}/archive/coverage',
                          '{tdir}/binary/usr/local/bin/rados',
                          '-c', '{tdir}/ceph.conf',
                          '-k', '{tdir}/data/{role}.keyring'.format(role=role),
                          '--name', role,
                          '-p' , str(config.get('pool', 'data')),
                          'bench', str(config.get('time', 360)), 'write',
                          ]).format(tdir=testdir),
                ],
            logger=log.getChild('radosbench.{id}'.format(id=id_)),
            stdin=run.PIPE,
            wait=False
            )
        radosbench[id_] = proc

    try:
        yield
    finally:
        log.info('joining radosbench')
        run.wait(radosbench.itervalues())

        if config.get('pool', 'data') is not "data":
            proc = remote.run(
                args=[
                    "/bin/sh", "-c",
                    " ".join(['LD_LIBRARY_PATH={tdir}/binary/usr/local/lib',
                              '{tdir}/enable-coredump',
                              '{tdir}/binary/usr/local/bin/ceph-coverage',
                              '{tdir}/archive/coverage',
                              '{tdir}/binary/usr/local/bin/rados',
                              '-c', '{tdir}/ceph.conf',
                              '-k', '{tdir}/data/{role}.keyring'.format(role=role),
                              '--name', role,
                              'rmpool', str(config.get('pool', 'data'))
                              ]).format(tdir=testdir),
                    ],
                logger=log.getChild('radosbench.{id}'.format(id=id_)),
                stdin=run.PIPE,
                wait=False
                )
            run.wait([proc])
