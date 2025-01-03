"""
Cram tests
"""
import logging
import os

from tasks.util.workunit import get_refspec_after_overrides

from teuthology import misc as teuthology
from teuthology.parallel import parallel
from teuthology.orchestra import run
from teuthology.config import config as teuth_config

log = logging.getLogger(__name__)

def task(ctx, config):
    """
    Run all cram tests from the specified paths on the specified
    clients. Each client runs tests in parallel as default, and
    you can also disable it by adding "parallel: False" option.

    Limitations:
    Tests must have a .t suffix. Tests with duplicate names will
    overwrite each other, so only the last one will run.

    For example::

        tasks:
        - ceph:
        - cram:
            clients:
              client.0:
              - qa/test.t
              - qa/test2.t]
              client.1: [qa/test.t]
            branch: foo
            parallel: False

    You can also run a list of cram tests on all clients::

        tasks:
        - ceph:
        - cram:
            clients:
              all: [qa/test.t]

    :param ctx: Context
    :param config: Configuration
    """
    assert isinstance(config, dict)
    assert 'clients' in config and isinstance(config['clients'], dict), \
           'configuration must contain a dictionary of clients'

    clients = teuthology.replace_all_with_clients(ctx.cluster,
                                                  config['clients'])
    testdir = teuthology.get_testdir(ctx)

    overrides = ctx.config.get('overrides', {})
    refspec = get_refspec_after_overrides(config, overrides)

    _parallel = config.get('parallel', True)

    git_url = teuth_config.get_ceph_qa_suite_git_url()
    log.info('Pulling tests from %s ref %s', git_url, refspec)

    try:
        for client, tests in clients.items():
            (remote,) = (ctx.cluster.only(client).remotes.keys())
            client_dir = '{tdir}/archive/cram.{role}'.format(tdir=testdir, role=client)
            remote.run(
                args=[
                    'mkdir', '--', client_dir,
                    run.Raw('&&'),
                    'python3', '-m', 'venv', '{tdir}/virtualenv'.format(tdir=testdir),
                    run.Raw('&&'),
                    '{tdir}/virtualenv/bin/pip'.format(tdir=testdir),
                    'install', 'cram==0.6',
                    ],
                )
            clone_dir = '{tdir}/clone.{role}'.format(tdir=testdir, role=client)
            remote.run(args=refspec.clone(git_url, clone_dir))

            for test in tests:
                assert test.endswith('.t'), 'tests must end in .t'
                remote.run(
                    args=[
                        'cp', '--', os.path.join(clone_dir, test), client_dir,
                        ],
                    )

        if _parallel:
            with parallel() as p:
                for role in clients.keys():
                    p.spawn(_run_tests, ctx, role)
        else:
            for role in clients.keys():
                _run_tests(ctx, role)
    finally:
        for client, tests in clients.items():
            (remote,) = (ctx.cluster.only(client).remotes.keys())
            client_dir = '{tdir}/archive/cram.{role}'.format(tdir=testdir, role=client)
            test_files = set([test.rsplit('/', 1)[1] for test in tests])

            # remove test files unless they failed
            for test_file in test_files:
                abs_file = os.path.join(client_dir, test_file)
                remote.run(
                    args=[
                        'test', '-f', abs_file + '.err',
                        run.Raw('||'),
                        'rm', '-f', '--', abs_file,
                        ],
                    )

            # ignore failure since more than one client may
            # be run on a host, and the client dir should be
            # non-empty if the test failed
            clone_dir = '{tdir}/clone.{role}'.format(tdir=testdir, role=client)
            remote.run(
                args=[
                    'rm', '-rf', '--',
                    '{tdir}/virtualenv'.format(tdir=testdir),
                    clone_dir,
                    run.Raw(';'),
                    'rmdir', '--ignore-fail-on-non-empty', client_dir,
                    ],
                )

def _run_tests(ctx, role):
    """
    For each role, check to make sure it's a client, then run the cram on that client

    :param ctx: Context
    :param role: Roles
    """
    assert isinstance(role, str)
    PREFIX = 'client.'
    if role.startswith(PREFIX):
        id_ = role[len(PREFIX):]
    else:
        id_ = role
    (remote,) = (ctx.cluster.only(role).remotes.keys())
    ceph_ref = ctx.summary.get('ceph-sha1', 'master')

    testdir = teuthology.get_testdir(ctx)
    log.info('Running tests for %s...', role)
    remote.run(
        args=[
            run.Raw('CEPH_REF={ref}'.format(ref=ceph_ref)),
            run.Raw('CEPH_ID="{id}"'.format(id=id_)),
            run.Raw('PATH=$PATH:/usr/sbin'),
            'adjust-ulimits',
            'ceph-coverage',
            '{tdir}/archive/coverage'.format(tdir=testdir),
            '{tdir}/virtualenv/bin/cram'.format(tdir=testdir),
            '-v', '--',
            run.Raw('{tdir}/archive/cram.{role}/*.t'.format(tdir=testdir, role=role)),
            ],
        logger=log.getChild(role),
        )
