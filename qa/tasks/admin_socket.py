"""
Admin Socket task -- used in rados, powercycle, and smoke testing
"""

import json
import logging
import os
import time

from teuthology.exceptions import CommandFailedError
from teuthology.orchestra import run
from teuthology import misc as teuthology
from teuthology.parallel import parallel
from teuthology.config import config as teuth_config

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

    :param ctx: Context
    :param config: Configuration
    """
    assert isinstance(config, dict), \
        'admin_socket task requires a dict for configuration'
    teuthology.replace_all_with_clients(ctx.cluster, config)

    with parallel() as ptask:
        for client, tests in config.items():
            ptask.spawn(_run_tests, ctx, client, tests)


def _socket_command(ctx, remote, socket_path, command, args):
    """
    Run an admin socket command and return the result as a string.

    :param ctx: Context
    :param remote: Remote site
    :param socket_path: path to socket
    :param command: command to be run remotely
    :param args: command arguments

    :returns: output of command in json format
    """
    testdir = teuthology.get_testdir(ctx)
    max_tries = 120
    sub_commands = [c.strip() for c in command.split('||')]
    ex = None
    for _ in range(max_tries):
        for sub_command in sub_commands:
            try:
                out = remote.sh([
                    'sudo',
                    'adjust-ulimits',
                    'ceph-coverage',
                    '{tdir}/archive/coverage'.format(tdir=testdir),
                    'ceph',
                    '--admin-daemon', socket_path,
                    ] + sub_command.split(' ') + args)
            except CommandFailedError as e:
                ex = e
                log.info('ceph cli "%s" returned an error %s, '
                         'command not registered yet?', sub_command, e)
            else:
                log.debug('admin socket command %s returned %s',
                          sub_command, out)
                return json.loads(out)
        else:
            # exhausted all commands
            log.info('sleeping and retrying ...')
            time.sleep(1)
    else:
        # i tried max_tries times..
        assert ex is not None
        raise ex


def _run_tests(ctx, client, tests):
    """
    Create a temp directory and wait for a client socket to be created.
    For each test, copy the executable locally and run the test.
    Remove temp directory when finished.

    :param ctx: Context
    :param client: client machine to run the test
    :param tests: list of tests to run
    """
    testdir = teuthology.get_testdir(ctx)
    log.debug('Running admin socket tests on %s', client)
    (remote,) = ctx.cluster.only(client).remotes.keys()
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

        for command, config in tests.items():
            if config is None:
                config = {}
            teuthology.deep_merge(config, overrides)
            log.debug('Testing %s with config %s', command, str(config))

            test_path = None
            if 'test' in config:
                # hack: the git_url is always ceph-ci or ceph
                git_url = teuth_config.get_ceph_git_url()
                repo_name = 'ceph.git'
                if git_url.count('ceph-ci'):
                    repo_name = 'ceph-ci.git'
                url = config['test'].format(
                    branch=config.get('branch', 'master'),
                    repo=repo_name,
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
