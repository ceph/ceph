import logging

from orchestra import run

log = logging.getLogger(__name__)

def task(ctx, config):
    """
    Run locktests, from the xfstests suite, on the given
    clients. Whether the clients are cfuse or kernel does not
    matter, and the two clients can refer to the same mount.

    The config is a list of two clients to run the locktest on. The
    first client will be the host.

    For example:
       tasks:
       - ceph:
       - cfuse: [client.0, client.1]
       - locktest:
           [client.0, client.1]

    This task does not yield; there would be little point.
    """

    assert isinstance(config, list)
    log.info('fetching and building locktests...')
    (host,) = ctx.cluster.only(config[0]).remotes
    (client,) = ctx.cluster.only(config[1]).remotes

    try:
        for client_name in config:
            log.info('building on {client_}'.format(client_=client_name))
            ctx.cluster.only(client_name).run(
                args=[
                    # explicitly does not support multiple autotest tasks
                    # in a single run; the result archival would conflict
                    'mkdir', '/tmp/cephtest/archive/locktest',
                    run.Raw('&&'),
                    'mkdir', '/tmp/cephtest/locktest',
                    run.Raw('&&'),
                    'wget',
                    '-nv',
                    '--no-check-certificate',
                    'https://raw.github.com/gregsfortytwo/xfstests-ceph/master/src/locktest.c',
                    '-O', '/tmp/cephtest/locktest/locktest.c',
                    run.Raw('&&'),
                    'g++', '/tmp/cephtest/locktest/locktest.c',
                    '-o', '/tmp/cephtest/locktest/locktest'
                    ],
                logger=log.getChild('locktest_client.{id}'.format(id=client_name)),
                )

        log.info('built locktest on each client')
        
        log.info('starting on host')
        hostproc = host.run(
            args=[
                '/tmp/cephtest/enable-coredump',
                '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
                '/tmp/cephtest/archive/coverage',
                '/tmp/cephtest/daemon-helper',
                'kill',
                '/tmp/cephtest/locktest/locktest',
                '-p', '6788',
                '-d'
                ],
            wait=False,
            logger=log.getChild('locktest.host'),
            )
        log.info('starting on client')
        client.run(
            args=[
                '/tmp/cephtest/enable-coredump',
                '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
                '/tmp/cephtest/archive/coverage',
                '/tmp/cephtest/daemon-helper',
                'kill',
                '/tmp/cephtest/locktest/locktest',
                '-p', '6788',
                '-d',
                '-h', host.name
                ],
            logger=log.getChild('locktest.client'),
            )
        
        run.wait([hostproc])
        log.info('finished running locktest executable')
        
    finally:
        log.info('cleaning up host dir')
        host.run(
            args=[
                'rm', '-f', '/tmp/cephtest/locktest/locktest.c',
                run.Raw('&&'),
                'rm', '-f', '/tmp/cephtest/locktest/locktest',
                run.Raw('&&'),
                'rmdir', '/tmp/cephtest/locktest'
                ],
            logger=log.getChild('.{id}'.format(id=config[0])),
            )
        log.info('cleaning up client dir')
        client.run(
            args=[
                'rm', '-f', '/tmp/cephtest/locktest/locktest.c',
                run.Raw('&&'),
                'rm', '-f', '/tmp/cephtest/locktest/locktest',
                run.Raw('&&'),
                'rmdir', '/tmp/cephtest/locktest'
                ],
            logger=log.getChild('.{id}'.format(\
                    id=config[1])),
            )
