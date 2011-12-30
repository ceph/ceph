import contextlib
import logging

from ..orchestra import run

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    Run RadosModel-based integration tests.

    The config should be as follows::

        testrados:
          clients: [client list]
          ops: <number of ops>
          objects: <number of objects to use>
          maxinflight: <max number of operations in flight>
          snaps: <create/remove/rollback snaps>

    For example::

        tasks:
        - ceph:
        - testrados:
            clients: [client.0]
            ops: 1000
            objects: 25
            maxinflight: 16
            snaps: true
        - interactive:
    """
    log.info('Beginning testrados...')
    assert isinstance(config, dict), \
        "please list clients to run on"
    testrados = {}

    (mon,) = ctx.cluster.only('mon.0').remotes.iterkeys()
    remotes = []
    for role in config.get('clients', ['client.0']):
        assert isinstance(role, basestring)
        PREFIX = 'client.'
        assert role.startswith(PREFIX)
        id_ = role[len(PREFIX):]
        (remote,) = ctx.cluster.only(role).remotes.iterkeys()
        remotes.append(remote)

        args = []
        if not config.get('snaps', False):
            args = [
                'CEPH_CLIENT_ID={id_}'.format(id_=id_),
                'CEPH_CONF=/tmp/cephtest/ceph.conf',
                'LD_PRELOAD=/tmp/cephtest/binary/usr/local/lib/librados.so.2',
                '/tmp/cephtest/binary/usr/local/bin/testreadwrite',
                str(config.get('ops', '10000')),
                str(config.get('objects', '500')),
                str(50),
                str(config.get('maxinflight', '16'))
                ]
        else:
            args = [
                'CEPH_CLIENT_ID={id_}'.format(id_=id_),
                'CEPH_CONF=/tmp/cephtest/ceph.conf',
                'LD_PRELOAD=/tmp/cephtest/binary/usr/local/lib/librados.so.2',
                '/tmp/cephtest/binary/usr/local/bin/testsnaps',
                str(config.get('ops', '10000')),
                str(config.get('objects', '500')),
                str(config.get('maxinflight', '16'))
                ]

        proc = remote.run(
            args=args,
            logger=log.getChild('testrados.{id}'.format(id=id_)),
            stdin=run.PIPE,
            wait=False
            )
        testrados[id_] = proc

    try:
        yield
    finally:
        log.info('joining testrados')
        run.wait(testrados.itervalues())
