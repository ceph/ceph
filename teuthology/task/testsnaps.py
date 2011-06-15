import contextlib
import logging

from orchestra import run

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    Run testsnaps

    The config should be as follows:

    testsnaps:
        clients: [client list]
        ops: <number of ops>
        objects: <number of objects to use>
        maxinflight: <max number of operations in flight>

    example:

    tasks:
    - ceph:
    - testsnaps: 
        clients: [client.0]
        ops: 1000
        objects: 25
        maxinflight: 16
    - interactive:
    """
    log.info('Beginning testsnaps...')
    assert isinstance(config, dict), \
        "please list clients to run on"
    testsnaps = {}

    (mon,) = ctx.cluster.only('mon.0').remotes.iterkeys()
    remotes = []
    for role in config.get('clients', ['client.0']):
        assert isinstance(role, basestring)
        PREFIX = 'client.'
        assert role.startswith(PREFIX)
        id_ = role[len(PREFIX):]
        (remote,) = ctx.cluster.only(role).remotes.iterkeys()
        remotes.append(remote)

        remote.run(
            args= [
                'cp',
                '/tmp/cephtest/ceph.conf',
                '/tmp/cephtest/data/ceph.conf',
                ],
            logger=log.getChild('testsnaps.{id}'.format(id=id_)),
            wait=True,
            )

        proc = remote.run(
            args= [
                '/bin/sh', '-c',
                " ".join([
                    'cd', '/tmp/cephtest/data;',
                    'export CEPH_CLIENT_ID={id_}; /tmp/cephtest/binary/usr/local/bin/testsnaps'.format(
                        id_=id_),
                    str(config.get('ops', '1000')),
                    str(config.get('objects', '25')),
                    str(config.get('maxinflight', '16')),
                    ])
                ],
            logger=log.getChild('testsnaps.{id}'.format(id=id_)),
            stdin=run.PIPE,
            wait=False
            )
        testsnaps[id_] = proc

    try:
        yield
    finally:
        for i in remotes:
            i.run(
                args=[
                    'rm',
                    '/tmp/cephtest/data/ceph.conf'
                    ],
                logger=log.getChild('testsnaps.{id}'.format(id=id_)),
                wait=True,
                )
                
        log.info('joining testrados')
        run.wait(testsnaps.itervalues())
