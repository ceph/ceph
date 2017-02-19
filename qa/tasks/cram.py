"""
Cram tests
"""
import logging
import os

from teuthology import misc as teuthology
from teuthology.parallel import parallel
from teuthology.orchestra import run
from teuthology.config import config as teuth_config

log = logging.getLogger(__name__)

def task(ctx, config):
    """
    Run all cram tests from the specified urls on the specified
    clients. Each client runs tests in parallel.

    Limitations:
    Tests must have a .t suffix. Tests with duplicate names will
    overwrite each other, so only the last one will run.

    For example::

        tasks:
        - ceph:
        - cram:
            clients:
              client.0:
              - http://download.ceph.com/qa/test.t
              - http://download.ceph.com/qa/test2.t]
              client.1: [http://download.ceph.com/qa/test.t]
            branch: foo

    You can also run a list of cram tests on all clients::

        tasks:
        - ceph:
        - cram:
            clients:
              all: [http://download.ceph.com/qa/test.t]

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
    teuthology.deep_merge(config, overrides.get('workunit', {}))

    refspec = config.get('branch')
    if refspec is None:
        refspec = config.get('tag')
    if refspec is None:
        refspec = config.get('sha1')
    if refspec is None:
        refspec = 'HEAD'

    # hack: the git_url is always ceph-ci or ceph
    git_url = teuth_config.get_ceph_git_url()
    repo_name = 'ceph.git'
    if git_url.count('ceph-ci'):
        repo_name = 'ceph-ci.git'

    try:
        for client, tests in clients.iteritems():
            (remote,) = ctx.cluster.only(client).remotes.iterkeys()
            client_dir = '{tdir}/archive/cram.{role}'.format(tdir=testdir, role=client)
            remote.run(
                args=[
                    'mkdir', '--', client_dir,
                    run.Raw('&&'),
                    'virtualenv', '{tdir}/virtualenv'.format(tdir=testdir),
                    run.Raw('&&'),
                    '{tdir}/virtualenv/bin/pip'.format(tdir=testdir),
                    'install', 'cram==0.6',
                    ],
                )
            for test in tests:
                url = test.format(repo=repo_name, branch=refspec)
                log.info('fetching test %s for %s', url, client)
                assert test.endswith('.t'), 'tests must end in .t'
                remote.run(
                    args=[
                        'wget', '-nc', '-nv', '-P', client_dir, '--', url,
                        ],
                    )

        with parallel() as p:
            for role in clients.iterkeys():
                p.spawn(_run_tests, ctx, role)
    finally:
        for client, tests in clients.iteritems():
            (remote,) = ctx.cluster.only(client).remotes.iterkeys()
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
            remote.run(
                args=[
                    'rm', '-rf', '--',
                    '{tdir}/virtualenv'.format(tdir=testdir),
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
    assert isinstance(role, basestring)
    PREFIX = 'client.'
    assert role.startswith(PREFIX)
    id_ = role[len(PREFIX):]
    (remote,) = ctx.cluster.only(role).remotes.iterkeys()
    ceph_ref = ctx.summary.get('ceph-sha1', 'master')

    testdir = teuthology.get_testdir(ctx)
    log.info('Running tests for %s...', role)
    remote.run(
        args=[
            run.Raw('CEPH_REF={ref}'.format(ref=ceph_ref)),
            run.Raw('CEPH_ID="{id}"'.format(id=id_)),
            'adjust-ulimits',
            'ceph-coverage',
            '{tdir}/archive/coverage'.format(tdir=testdir),
            '{tdir}/virtualenv/bin/cram'.format(tdir=testdir),
            '-v', '--',
            run.Raw('{tdir}/archive/cram.{role}/*.t'.format(tdir=testdir, role=role)),
            ],
        logger=log.getChild(role),
        )
