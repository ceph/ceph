""" 
Run an autotest test on the ceph cluster.
"""
import json
import logging
import os

from teuthology import misc as teuthology
from teuthology.parallel import parallel
from ..orchestra import run

log = logging.getLogger(__name__)

def task(ctx, config):
    """
    Run an autotest test on the ceph cluster.

    Only autotest client tests are supported.

    The config is a mapping from role name to list of tests to run on
    that client.

    For example::

        tasks:
        - ceph:
        - ceph-fuse: [client.0, client.1]
        - autotest:
            client.0: [dbench]
            client.1: [bonnie]

    You can also specify a list of tests to run on all clients::

        tasks:
        - ceph:
        - ceph-fuse:
        - autotest:
            all: [dbench]
    """
    assert isinstance(config, dict)
    config = teuthology.replace_all_with_clients(ctx.cluster, config)
    log.info('Setting up autotest...')
    testdir = teuthology.get_testdir(ctx)
    with parallel() as p:
        for role in config.iterkeys():
            (remote,) = ctx.cluster.only(role).remotes.keys()
            p.spawn(_download, testdir, remote)

    log.info('Making a separate scratch dir for every client...')
    for role in config.iterkeys():
        assert isinstance(role, basestring)
        PREFIX = 'client.'
        assert role.startswith(PREFIX)
        id_ = role[len(PREFIX):]
        (remote,) = ctx.cluster.only(role).remotes.iterkeys()
        mnt = os.path.join(testdir, 'mnt.{id}'.format(id=id_))
        scratch = os.path.join(mnt, 'client.{id}'.format(id=id_))
        remote.run(
            args=[
                'sudo',
                'install',
                '-d',
                '-m', '0755',
                '--owner={user}'.format(user='ubuntu'), #TODO
                '--',
                scratch,
                ],
            )

    with parallel() as p:
        for role, tests in config.iteritems():
            (remote,) = ctx.cluster.only(role).remotes.keys()
            p.spawn(_run_tests, testdir, remote, role, tests)

def _download(testdir, remote):
    """
    Download.  Does not explicitly support muliple tasks in a single run.
    """
    remote.run(
        args=[
            # explicitly does not support multiple autotest tasks
            # in a single run; the result archival would conflict
            'mkdir', '{tdir}/archive/autotest'.format(tdir=testdir),
            run.Raw('&&'),
            'mkdir', '{tdir}/autotest'.format(tdir=testdir),
            run.Raw('&&'),
            'wget',
            '-nv',
            '--no-check-certificate',
            'https://github.com/ceph/autotest/tarball/ceph',
            '-O-',
            run.Raw('|'),
            'tar',
            '-C', '{tdir}/autotest'.format(tdir=testdir),
            '-x',
            '-z',
            '-f-',
            '--strip-components=1',
            ],
        )

def _run_tests(testdir, remote, role, tests):
    """
    Spawned to run test on remote site
    """
    assert isinstance(role, basestring)
    PREFIX = 'client.'
    assert role.startswith(PREFIX)
    id_ = role[len(PREFIX):]
    mnt = os.path.join(testdir, 'mnt.{id}'.format(id=id_))
    scratch = os.path.join(mnt, 'client.{id}'.format(id=id_))

    assert isinstance(tests, list)
    for idx, testname in enumerate(tests):
        log.info('Running autotest client test #%d: %s...', idx, testname)

        tag = 'client.{id}.num{idx}.{testname}'.format(
            idx=idx,
            testname=testname,
            id=id_,
            )
        control = '{tdir}/control.{tag}'.format(tdir=testdir, tag=tag)
        teuthology.write_file(
            remote=remote,
            path=control,
            data='import json; data=json.loads({data!r}); job.run_test(**data)'.format(
                data=json.dumps(dict(
                        url=testname,
                        dir=scratch,
                        # TODO perhaps tag
                        # results will be in {testdir}/autotest/client/results/dbench
                        # or {testdir}/autotest/client/results/dbench.{tag}
                        )),
                ),
            )
        remote.run(
            args=[
                '{tdir}/autotest/client/bin/autotest'.format(tdir=testdir),
                '--verbose',
                '--harness=simple',
                '--tag={tag}'.format(tag=tag),
                control,
                run.Raw('3>&1'),
                ],
            )

        remote.run(
            args=[
                'rm', '-rf', '--', control,
                ],
            )

        remote.run(
            args=[
                'mv',
                '--',
                '{tdir}/autotest/client/results/{tag}'.format(tdir=testdir, tag=tag),
                '{tdir}/archive/autotest/{tag}'.format(tdir=testdir, tag=tag),
                ],
            )

    remote.run(
        args=[
            'rm', '-rf', '--', '{tdir}/autotest'.format(tdir=testdir),
            ],
        )
