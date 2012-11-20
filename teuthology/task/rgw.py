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
    for client in config.iterkeys():
        ctx.cluster.only(client).run(
            args=[
                'mkdir',
                '-p',
                '/tmp/cephtest/apache/htdocs',
                '/tmp/cephtest/apache/tmp',
                run.Raw('&&'),
                'mkdir',
                '/tmp/cephtest/archive/apache',
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
                    '/tmp/cephtest/apache/tmp',
                    run.Raw('&&'),
                    'rmdir',
                    '/tmp/cephtest/apache/htdocs',
                    run.Raw('&&'),
                    'rmdir',
                    '/tmp/cephtest/apache',
                    ],
                )


@contextlib.contextmanager
def ship_config(ctx, config):
    assert isinstance(config, dict)
    log.info('Shipping apache config and rgw.fcgi...')
    src = os.path.join(os.path.dirname(__file__), 'apache.conf')
    for client in config.iterkeys():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        with file(src, 'rb') as f:
            teuthology.write_file(
                remote=remote,
                path='/tmp/cephtest/apache/apache.conf',
                data=f,
                )
        teuthology.write_file(
            remote=remote,
            path='/tmp/cephtest/apache/htdocs/rgw.fcgi',
            data="""#!/bin/sh
ulimit -c unlimited
export LD_LIBRARY_PATH=/tmp/cephtest/binary/usr/local/lib
exec /tmp/cephtest/binary/usr/local/bin/radosgw -f -c /tmp/cephtest/ceph.conf
"""
            )
        remote.run(
            args=[
                'chmod',
                'a=rx',
                '/tmp/cephtest/apache/htdocs/rgw.fcgi',
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
                    '/tmp/cephtest/apache/apache.conf',
                    run.Raw('&&'),
                    'rm',
                    '-f',
                    '/tmp/cephtest/apache/htdocs/rgw.fcgi',
                    ],
                )


@contextlib.contextmanager
def start_rgw(ctx, config):
    log.info('Starting rgw...')
    rgws = {}
    for client in config.iterkeys():
        (remote,) = ctx.cluster.only(client).remotes.iterkeys()

        client_config = config.get(client)
        if client_config is None:
            client_config = {}
        log.info("rgw %s config is %s", client, client_config)
 
        run_cmd=[
                'LD_LIBRARY_PATH=/tmp/cephtest/binary/usr/local/lib',
                '/tmp/cephtest/enable-coredump',
                '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
                '/tmp/cephtest/archive/coverage',
                '/tmp/cephtest/daemon-helper',
                'term',
            ]
        run_cmd_tail=[
                '/tmp/cephtest/binary/usr/local/bin/radosgw',
                '-c', '/tmp/cephtest/ceph.conf',
                '--log-file', '/tmp/cephtest/archive/log/rgw.log',
                '--rgw_ops_log_socket_path', '/tmp/cephtest/rgw.opslog.sock',
                '/tmp/cephtest/apache/apache.conf',
                '--foreground',
                run.Raw('>'),
                '/tmp/cephtest/archive/log/rgw.stdout',
                run.Raw('2>&1'),
            ]

        run_cmd.extend(
            teuthology.get_valgrind_args(
                client,
                client_config.get('valgrind')
                )
            )

        run_cmd.extend(run_cmd_tail)

        proc = remote.run(
            args=run_cmd,
            logger=log.getChild(client),
            stdin=run.PIPE,
            wait=False,
            )
        rgws[client] = proc

    try:
        yield
    finally:
        log.info('Stopping rgw...')
        for client, proc in rgws.iteritems():
            proc.stdin.close()

            ctx.cluster.only(client).run(
                args=[
                    'rm',
                    '-rf',
                    '/tmp/cephtest/rgw.opslog.sock',
                     ],
             )

        run.wait(rgws.itervalues())


@contextlib.contextmanager
def start_apache(ctx, config):
    log.info('Starting apache...')
    apaches = {}
    for client in config.iterkeys():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        proc = remote.run(
            args=[
                '/tmp/cephtest/enable-coredump',
                '/tmp/cephtest/daemon-helper',
                'kill',
                'apache2',
                '-X',
                '-f',
                '/tmp/cephtest/apache/apache.conf',
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
