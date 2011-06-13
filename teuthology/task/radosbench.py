import contextlib
import logging
import os

from teuthology import misc as teuthology
from orchestra import run

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    Run radosbench

    The config should be as follows:

    radosbench:
        clients: [client list]
        time: <seconds to run>

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

    (mon,) = ctx.cluster.only('mon.0').remotes.iterkeys()
    remotes = []
    for role in config.get('clients', ['client.0']):
        assert isinstance(role, basestring)
        PREFIX = 'client.'
        assert role.startswith(PREFIX)
        id_ = role[len(PREFIX):]
        (remote,) = ctx.cluster.only(role).remotes.iterkeys()

        proc = remote.run(
            args= [
                "/bin/sh", "-c",
                " ".join(['LD_LIBRARY_PATH=/tmp/cephtest/binary/usr/local/lib',
                          '/tmp/cephtest/binary/usr/local/bin/rados',
                          '-c', '/tmp/cephtest/ceph.conf',
                          '-k', '/tmp/cephtest/data/{role}.keyring'.format(role=role),
                          '--name', role,
                          '-p' , 'data',
                          'bench', str(config.get('time', 360)), 'write',
                          ]),
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
