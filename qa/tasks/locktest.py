"""
locktests
"""
import logging

from teuthology.orchestra import run
from teuthology import misc as teuthology

log = logging.getLogger(__name__)

def task(ctx, config):
    """
    Run locktests, from the xfstests suite, on the given
    clients. Whether the clients are ceph-fuse or kernel does not
    matter, and the two clients can refer to the same mount.

    The config is a list of two clients to run the locktest on. The
    first client will be the host.

    For example:
       tasks:
       - ceph:
       - ceph-fuse: [client.0, client.1]
       - locktest:
           [client.0, client.1]

    This task does not yield; there would be little point.

    :param ctx: Context
    :param config: Configuration
    """

    assert isinstance(config, list)
    log.info('fetching and building locktests...')
    (host,) = ctx.cluster.only(config[0]).remotes
    (client,) = ctx.cluster.only(config[1]).remotes
    ( _, _, host_id) = config[0].partition('.')
    ( _, _, client_id) = config[1].partition('.')
    testdir = teuthology.get_testdir(ctx)
    hostmnt = '{tdir}/mnt.{id}'.format(tdir=testdir, id=host_id)
    clientmnt = '{tdir}/mnt.{id}'.format(tdir=testdir, id=client_id)

    try:
        for client_name in config:
            log.info('building on {client_}'.format(client_=client_name))
            ctx.cluster.only(client_name).run(
                args=[
                    # explicitly does not support multiple autotest tasks
                    # in a single run; the result archival would conflict
                    'mkdir', '{tdir}/archive/locktest'.format(tdir=testdir),
                    run.Raw('&&'),
                    'mkdir', '{tdir}/locktest'.format(tdir=testdir),
                    run.Raw('&&'),
                    'wget',
                    '-nv',
                    'https://raw.github.com/gregsfortytwo/xfstests-ceph/master/src/locktest.c',
                    '-O', '{tdir}/locktest/locktest.c'.format(tdir=testdir),
                    run.Raw('&&'),
                    'g++', '{tdir}/locktest/locktest.c'.format(tdir=testdir),
                    '-o', '{tdir}/locktest/locktest'.format(tdir=testdir)
                    ],
                logger=log.getChild('locktest_client.{id}'.format(id=client_name)),
                )

        log.info('built locktest on each client')

        host.run(args=['sudo', 'touch',
                       '{mnt}/locktestfile'.format(mnt=hostmnt),
                       run.Raw('&&'),
                       'sudo', 'chown', 'ubuntu.ubuntu',
                       '{mnt}/locktestfile'.format(mnt=hostmnt)
                       ]
                 )

        log.info('starting on host')
        hostproc = host.run(
            args=[
                '{tdir}/locktest/locktest'.format(tdir=testdir),
                '-p', '6788',
                '-d',
                '{mnt}/locktestfile'.format(mnt=hostmnt),
                ],
            wait=False,
            logger=log.getChild('locktest.host'),
            )
        log.info('starting on client')
        (_,_,hostaddr) = host.name.partition('@')
        clientproc = client.run(
            args=[
                '{tdir}/locktest/locktest'.format(tdir=testdir),
                '-p', '6788',
                '-d',
                '-h', hostaddr,
                '{mnt}/locktestfile'.format(mnt=clientmnt),
                ],
            logger=log.getChild('locktest.client'),
            wait=False
            )

        hostresult = hostproc.wait()
        clientresult = clientproc.wait()
        if (hostresult != 0) or (clientresult != 0):
            raise Exception("Did not pass locking test!")
        log.info('finished locktest executable with results {r} and {s}'. \
                     format(r=hostresult, s=clientresult))

    finally:
        log.info('cleaning up host dir')
        host.run(
            args=[
                'mkdir', '-p', '{tdir}/locktest'.format(tdir=testdir),
                run.Raw('&&'),
                'rm', '-f', '{tdir}/locktest/locktest.c'.format(tdir=testdir),
                run.Raw('&&'),
                'rm', '-f', '{tdir}/locktest/locktest'.format(tdir=testdir),
                run.Raw('&&'),
                'rmdir', '{tdir}/locktest'
                ],
            logger=log.getChild('.{id}'.format(id=config[0])),
            )
        log.info('cleaning up client dir')
        client.run(
            args=[
                'mkdir', '-p', '{tdir}/locktest'.format(tdir=testdir),
                run.Raw('&&'),
                'rm', '-f', '{tdir}/locktest/locktest.c'.format(tdir=testdir),
                run.Raw('&&'),
                'rm', '-f', '{tdir}/locktest/locktest'.format(tdir=testdir),
                run.Raw('&&'),
                'rmdir', '{tdir}/locktest'.format(tdir=testdir)
                ],
            logger=log.getChild('.{id}'.format(\
                    id=config[1])),
            )
