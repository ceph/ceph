from cStringIO import StringIO

import json
import logging
import os

from ..orchestra import run
from teuthology import misc as teuthology
from teuthology.parallel import parallel

log = logging.getLogger(__name__)

def task(ctx, config):
    """
    Run an admin socket command, make sure the output is json, and run
    a test program on it. The test program should read json from
    stdin. This task succeeds if the test program exits with status 0.

    To run the same test on all clients::

        tasks:
        - ceph:
        - rados:
        - admin_socket:
            all:
              dump_requests:
                test: http://example.com/script

    To restrict it to certain clients::

        tasks:
        - ceph:
        - rados: [client.1]
        - admin_socket:
            client.1:
              dump_requests:
                test: http://example.com/script

    If an admin socket command has arguments, they can be specified as
    a list::

        tasks:
        - ceph:
        - rados: [client.0]
        - admin_socket:
            client.0:
              dump_requests:
                test: http://example.com/script
              help:
                test: http://example.com/test_help_version
                args: [version]

    Note that there must be a ceph client with an admin socket running
    before this task is run. The tests are parallelized at the client
    level. Tests for a single client are run serially.
    """
    assert isinstance(config, dict), \
        'admin_socket task requires a dict for configuration'
    teuthology.replace_all_with_clients(ctx.cluster, config)

    with parallel() as p:
        for client, tests in config.iteritems():
            p.spawn(_run_tests, ctx, client, tests)

def _socket_command(ctx, remote, socket_path, command, args):
    """
    Run an admin socket command and return the result as a string.
    """
    json_fp = StringIO()
    testdir = teuthology.get_testdir(ctx)
    remote.run(
        args=[
            'sudo',
            '{tdir}/adjust-ulimits'.format(tdir=testdir),
            'ceph-coverage',
            '{tdir}/archive/coverage'.format(tdir=testdir),
            'ceph',
            '--admin-daemon', socket_path,
            command,
            ] + args,
        stdout=json_fp,
        )
    out = json_fp.getvalue()
    json_fp.close()
    log.debug('admin socket command %s returned %s', command, out)
    return json.loads(out)

def _run_tests(ctx, client, tests):
    testdir = teuthology.get_testdir(ctx)
    log.debug('Running admin socket tests on %s', client)
    (remote,) = ctx.cluster.only(client).remotes.iterkeys()
    socket_path = '/var/run/ceph/ceph-{name}.asok'.format(name=client)
    overrides = ctx.config.get('overrides', {}).get('admin_socket', {})

    try:
        tmp_dir = os.path.join(
            testdir,
            'admin_socket_{client}'.format(client=client),
            )
        remote.run(
            args=[
                'mkdir',
                '--',
                tmp_dir,
                run.Raw('&&'),
                # wait for client process to create the socket
                'while', 'test', '!', '-e', socket_path, run.Raw(';'),
                'do', 'sleep', '1', run.Raw(';'), 'done',
                ],
            )

        for command, config in tests.iteritems():
            if config is None:
                config = {}
            teuthology.deep_merge(config, overrides)
            log.debug('Testing %s with config %s', command, str(config))

            test_path = None
            if 'test' in config:
                url = config['test'].format(
                    branch=config.get('branch', 'master')
                    )
                test_path = os.path.join(tmp_dir, command)
                remote.run(
                    args=[
                        'wget',
                        '-q',
                        '-O',
                        test_path,
                        '--',
                        url,
                        run.Raw('&&'),
                        'chmod',
                        'u=rx',
                        '--',
                        test_path,
                        ],
                    )

            args = config.get('args', [])
            assert isinstance(args, list), \
                'admin socket command args must be a list'
            sock_out = _socket_command(ctx, remote, socket_path, command, args)
            if test_path is not None:
                remote.run(
                    args=[
                        test_path,
                        ],
                    stdin=json.dumps(sock_out),
                    )

    finally:
        remote.run(
            args=[
                'rm', '-rf', '--', tmp_dir,
                ],
            )

