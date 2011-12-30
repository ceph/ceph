import contextlib
import logging

from ..orchestra import run

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    Run RadosModel-based integration tests.

    The config should be as follows::

        rados:
          clients: [client list]
          ops: <number of ops>
          objects: <number of objects to use>
          maxinflight: <max number of operations in flight>
          snaps: <create/remove/rollback snaps>

    For example::

        tasks:
        - ceph:
        - rados:
            clients: [client.0]
            ops: 1000
            objects: 25
            maxinflight: 16
            snaps: true
        - interactive:
    """
    log.info('Beginning rados...')
    assert isinstance(config, dict), \
        "please list clients to run on"
    tests = {}

    args = [
        'CEPH_CONF=/tmp/cephtest/ceph.conf',
        'LD_LIBRARY_PATH=/tmp/cephtest/binary/usr/local/lib',
        '/tmp/cephtest/enable-coredump',
        '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
        '/tmp/cephtest/archive/coverage',
        ]
    if config.get('snaps', False):
        args.extend(
            '/tmp/cephtest/binary/usr/local/bin/testreadwrite',
            str(config.get('ops', '10000')),
            str(config.get('objects', '500')),
            str(50),
            str(config.get('maxinflight', '16'))
            )
    else:
        args.extend(
            '/tmp/cephtest/binary/usr/local/bin/testsnaps',
            str(config.get('ops', '10000')),
            str(config.get('objects', '500')),
            str(config.get('maxinflight', '16'))
            )

    (mon,) = ctx.cluster.only('mon.0').remotes.iterkeys()
    for role in config.get('clients', ['client.0']):
        assert isinstance(role, basestring)
        PREFIX = 'client.'
        assert role.startswith(PREFIX)
        id_ = role[len(PREFIX):]
        (remote,) = ctx.cluster.only(role).remotes.iterkeys()
        proc = remote.run(
            args=['CEPH_CLIENT_ID={id_}'.format(id_=id_)] + args,
            logger=log.getChild('rados.{id}'.format(id=id_)),
            stdin=run.PIPE,
            wait=False
            )
        tests[id_] = proc

    try:
        yield
    finally:
        log.info('joining rados')
        run.wait(tests.itervalues())
