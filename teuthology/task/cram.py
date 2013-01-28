import logging
import os

from teuthology import misc as teuthology
from teuthology.parallel import parallel
from ..orchestra import run

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
              - http://ceph.com/qa/test.t
              - http://ceph.com/qa/test2.t]
              client.1: [http://ceph.com/qa/test.t]

    You can also run a list of cram tests on all clients::

        tasks:
        - ceph:
        - cram:
            clients:
              all: [http://ceph.com/qa/test.t]
    """
    assert isinstance(config, dict)
    assert 'clients' in config and isinstance(config['clients'], dict), \
           'configuration must contain a dictionary of clients'

    clients = teuthology.replace_all_with_clients(ctx.cluster,
                                                  config['clients'])
    try:
        for client, tests in clients.iteritems():
            (remote,) = ctx.cluster.only(client).remotes.iterkeys()
            client_dir = '/tmp/cephtest/archive/cram.{role}'.format(role=client)
            remote.run(
                args=[
                    'mkdir', '--', client_dir,
                    run.Raw('&&'),
                    'virtualenv', '/tmp/cephtest/virtualenv',
                    run.Raw('&&'),
                    '/tmp/cephtest/virtualenv/bin/pip',
                    'install', 'cram',
                    ],
                )
            for test in tests:
                log.info('fetching test %s for %s', test, client)
                assert test.endswith('.t'), 'tests must end in .t'
                remote.run(
                    args=[
                        'wget', '-nc', '-nv', '-P', client_dir, '--', test,
                        ],
                    )

        with parallel() as p:
            for role in clients.iterkeys():
                p.spawn(_run_tests, ctx, role)
    finally:
        for client, tests in clients.iteritems():
            (remote,) = ctx.cluster.only(client).remotes.iterkeys()
            client_dir = '/tmp/cephtest/archive/cram.{role}'.format(role=client)
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
                    '/tmp/cephtest/virtualenv',
                    run.Raw(';'),
                    'rmdir', '--ignore-fail-on-non-empty', client_dir,
                    ],
                )

def _run_tests(ctx, role):
    assert isinstance(role, basestring)
    PREFIX = 'client.'
    assert role.startswith(PREFIX)
    id_ = role[len(PREFIX):]
    (remote,) = ctx.cluster.only(role).remotes.iterkeys()
    ceph_ref = ctx.summary.get('ceph-sha1', 'master')

    log.info('Running tests for %s...', role)
    remote.run(
        args=[
            run.Raw('CEPH_REF={ref}'.format(ref=ceph_ref)),
            run.Raw('PATH="$PATH:/tmp/cephtest/binary/usr/local/bin"'),
            run.Raw('LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/tmp/cephtest/binary/usr/local/lib"'),
            run.Raw('CEPH_CONF="/tmp/cephtest/ceph.conf"'),
            run.Raw('CEPH_ID="{id}"'.format(id=id_)),
            run.Raw('PYTHONPATH="$PYTHONPATH:/tmp/cephtest/binary/usr/local/lib/python2.7/dist-packages:/tmp/cephtest/binary/usr/local/lib/python2.6/dist-packages"'),
            '/tmp/cephtest/enable-coredump',
            '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
            '/tmp/cephtest/archive/coverage',
            '/tmp/cephtest/virtualenv/bin/cram',
            '-v', '--',
            run.Raw('/tmp/cephtest/archive/cram.{role}/*.t'.format(role=role)),
            ],
        logger=log.getChild(role),
        )
