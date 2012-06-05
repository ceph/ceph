import contextlib
import logging

from ..orchestra import run

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    Run omapbench

    The config should be as follows::

		  omapbench:
		      clients: [client list]
		      threads: <threads at once>
		      objects: <number of objects to write>
		      entries: <number of entries per object map>
		      keysize: <number of characters per object map key>
		      valsize: <number of characters per object map val>
		      increment: <interval to show in histogram (in ms)>
		      omaptype: <how the omaps should be generated>

    example::

		  tasks:
		  - ceph:
		  - omapbench:
		      clients: [client.0]
		      threads: 30
		      objects: 1000
		      entries: 10
		      keysize: 10
		      valsize: 100
		      increment: 100
		      omaptype: uniform
		  - interactive:
    """
    log.info('Beginning omapbench...')
    assert isinstance(config, dict), \
        "please list clients to run on"
    omapbench = {}
    print(str(config.get('increment',-1)))
    for role in config.get('clients', ['client.0']):
        assert isinstance(role, basestring)
        PREFIX = 'client.'
        assert role.startswith(PREFIX)
        id_ = role[len(PREFIX):]
        (remote,) = ctx.cluster.only(role).remotes.iterkeys()
        proc = remote.run(
            args=[
                "/bin/sh", "-c",
                " ".join(['CEPH_CONF=/tmp/cephtest/ceph.conf',
                          'LD_LIBRARY_PATH=/tmp/cephtest/binary/usr/local/lib',
                          '/tmp/cephtest/enable-coredump',
                          '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
                          '/tmp/cephtest/archive/coverage',
                          '/tmp/cephtest/binary/usr/local/bin/omapbench',
                          '-k', '/tmp/cephtest/data/{role}.keyring'.format(role=role),
                          '--name', role[len(PREFIX):],
                          '-t', str(config.get('threads', 30)),
                          '-o', str(config.get('objects', 1000)),
                          '--entries', str(config.get('entries',10)),
                          '--keysize', str(config.get('keysize',10)),
                          '--valsize', str(config.get('valsize',1000)),
                          '--inc', str(config.get('increment',10)),
                          '--omaptype', str(config.get('omaptype','uniform'))
                          ]),
                ],
            logger=log.getChild('omapbench.{id}'.format(id=id_)),
            stdin=run.PIPE,
            wait=False
            )
        omapbench[id_] = proc

    try:
        yield
    finally:
        log.info('joining omapbench')
        run.wait(omapbench.itervalues())
