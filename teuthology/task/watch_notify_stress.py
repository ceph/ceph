import contextlib
import logging

from ..orchestra import run

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    Run test_stress_watch

    The config should be as follows:

    test_stress_watch:
        clients: [client list]

    example:

    tasks:
    - ceph:
    - test_stress_watch:
        clients: [client.0]
    - interactive:
    """
    log.info('Beginning test_stress_watch...')
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
            args=[
                'cp',
                '/tmp/cephtest/ceph.conf',
                '/tmp/cephtest/data/ceph.conf',
                ],
            logger=log.getChild('test_stress_watch.{id}'.format(id=id_)),
            wait=True,
            )

        args =[ '/bin/sh', '-c',
                " ".join([
                    'cd', '/tmp/cephtest/data;',
                    'export CEPH_CLIENT_ID={id_}; export CEPH_CONF=ceph.conf; export CEPH_ARGS="{flags}"; LD_PRELOAD=/tmp/cephtest/binary/usr/local/lib/librados.so.2 /tmp/cephtest/binary/usr/local/bin/test_stress_watch {flags}'.format(
                        id_=id_,
                        flags=config.get('flags', ''),
                        )
                    ])
                ]

        log.info("args are %s" % (args,))

        proc = remote.run(
            args=args,
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

        log.info('joining watch_notify_stress')
        run.wait(testsnaps.itervalues())
