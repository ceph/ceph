import contextlib
import logging
import os

from teuthology import misc as teuthology
from teuthology import contextutil
from ..orchestra import run

log = logging.getLogger(__name__)


@contextlib.contextmanager
def create_dirs(ctx, config):
    log.info('Creating apache directories...')
    testdir = teuthology.get_testdir(ctx)
    for client in config.iterkeys():
        ctx.cluster.only(client).run(
            args=[
                'mkdir',
                '-p',
                '{tdir}/apache/htdocs'.format(tdir=testdir),
                '{tdir}/apache/tmp'.format(tdir=testdir),
                run.Raw('&&'),
                'mkdir',
                '{tdir}/archive/apache'.format(tdir=testdir),
                ],
            )
    try:
        yield
    finally:
        log.info('Cleaning up apache directories...')
        for client in config.iterkeys():
            ctx.cluster.only(client).run(
                args=[
                    'rm',
                    '-rf',
                    '{tdir}/apache/tmp'.format(tdir=testdir),
                    run.Raw('&&'),
                    'rmdir',
                    '{tdir}/apache/htdocs'.format(tdir=testdir),
                    run.Raw('&&'),
                    'rmdir',
                    '{tdir}/apache'.format(tdir=testdir),
                    ],
                )


@contextlib.contextmanager
def ship_config(ctx, config):
    assert isinstance(config, dict)
    testdir = teuthology.get_testdir(ctx)
    log.info('Shipping apache config and rgw.fcgi...')
    src = os.path.join(os.path.dirname(__file__)
                       , 'apache.conf.template').format(
        testdir=testdir
        )
    for client in config.iterkeys():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        with file(src, 'rb') as f:
            conf = f.read().format(testdir=testdir)
            teuthology.write_file(
                remote=remote,
                path='{tdir}/apache/apache.conf'.format(tdir=testdir),
                data=conf,
                )
        teuthology.write_file(
            remote=remote,
            path='{tdir}/apache/htdocs/rgw.fcgi'.format(tdir=testdir),
            data="""#!/bin/sh
ulimit -c unlimited
exec radosgw -f
""".format(tdir=testdir)
            )
        remote.run(
            args=[
                'chmod',
                'a=rx',
                '{tdir}/apache/htdocs/rgw.fcgi'.format(tdir=testdir),
                ],
            )
    try:
        yield
    finally:
        log.info('Removing apache config...')
        for client in config.iterkeys():
            ctx.cluster.only(client).run(
                args=[
                    'rm',
                    '-f',
                    '{tdir}/apache/apache.conf'.format(tdir=testdir),
                    run.Raw('&&'),
                    'rm',
                    '-f',
                    '{tdir}/apache/htdocs/rgw.fcgi'.format(tdir=testdir),
                    ],
                )


@contextlib.contextmanager
def start_rgw(ctx, config):
    log.info('Starting rgw...')
    testdir = teuthology.get_testdir(ctx)
    for client in config.iterkeys():
        (remote,) = ctx.cluster.only(client).remotes.iterkeys()

        client_config = config.get(client)
        if client_config is None:
            client_config = {}
        log.info("rgw %s config is %s", client, client_config)
 
        run_cmd=[
            'sudo',
                '{tdir}/adjust-ulimits'.format(tdir=testdir),
                'ceph-coverage',
                '{tdir}/archive/coverage'.format(tdir=testdir),
                '{tdir}/daemon-helper'.format(tdir=testdir),
                'term',
            ]
        run_cmd_tail=[
                'radosgw',
                # authenticate as client.admin and use system keyring
                '-k', '/etc/ceph/ceph.keyring',
                '--log-file', '/var/log/ceph/rgw.log',
                '--rgw_ops_log_socket_path', '{tdir}/rgw.opslog.sock'.format(tdir=testdir),
                '{tdir}/apache/apache.conf'.format(tdir=testdir),
                '--foreground',
                run.Raw('|'),
                'sudo',
                'tee',
                '/var/log/ceph/rgw.stdout'.format(tdir=testdir),
                run.Raw('2>&1'),
            ]

        run_cmd.extend(
            teuthology.get_valgrind_args(
                testdir,
                client,
                client_config.get('valgrind')
                )
            )

        run_cmd.extend(run_cmd_tail)

        ctx.daemons.add_daemon(
            remote, 'rgw', client,
            args=run_cmd,
            logger=log.getChild(client),
            stdin=run.PIPE,
            wait=False,
            )

    try:
        yield
    finally:
        teuthology.stop_daemons_of_type(ctx, 'rgw')
        for client in config.iterkeys():
            ctx.cluster.only(client).run(
                args=[
                    'rm',
                    '-f',
                    '{tdir}/rgw.opslog.sock'.format(tdir=testdir),
                    ],
                )


@contextlib.contextmanager
def start_apache(ctx, config):
    log.info('Starting apache...')
    testdir = teuthology.get_testdir(ctx)
    apaches = {}
    for client in config.iterkeys():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        proc = remote.run(
            args=[
                '{tdir}/adjust-ulimits'.format(tdir=testdir),
                '{tdir}/daemon-helper'.format(tdir=testdir),
                'kill'.format(tdir=testdir),
                'apache2'.format(tdir=testdir),
                '-X'.format(tdir=testdir),
                '-f'.format(tdir=testdir),
                '{tdir}/apache/apache.conf'.format(tdir=testdir),
                ],
            logger=log.getChild(client),
            stdin=run.PIPE,
            wait=False,
            )
        apaches[client] = proc

    try:
        yield
    finally:
        log.info('Stopping apache...')
        for client, proc in apaches.iteritems():
            proc.stdin.close()

        run.wait(apaches.itervalues())


@contextlib.contextmanager
def task(ctx, config):
    """
    Spin up apache configured to run a rados gateway.
    Only one should be run per machine, since it uses a hard-coded port for now.

    For example, to run rgw on all clients::

        tasks:
        - ceph:
        - rgw:

    To only run on certain clients::

        tasks:
        - ceph:
        - rgw: [client.0, client.3]

    or

        tasks:
        - ceph:
        - rgw:
            client.0:
            client.3:

    To run radosgw through valgrind:

        tasks:
        - ceph:
        - rgw:
            client.0:
              valgrind: [--tool=memcheck]
            client.3:
              valgrind: [--tool=memcheck]

    """
    if config is None:
        config = dict(('client.{id}'.format(id=id_), None)
                  for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client'))
    elif isinstance(config, list):
        config = dict((name, None) for name in config)

    for _, roles_for_host in ctx.cluster.remotes.iteritems():
        running_rgw = False
        for role in roles_for_host:
            if role in config.iterkeys():
                assert not running_rgw, "Only one client per host can run rgw."
                running_rgw = True

    with contextutil.nested(
        lambda: create_dirs(ctx=ctx, config=config),
        lambda: ship_config(ctx=ctx, config=config),
        lambda: start_rgw(ctx=ctx, config=config),
        lambda: start_apache(ctx=ctx, config=config),
        ):
        yield
