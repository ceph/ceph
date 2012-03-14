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
        - cfuse: [client.0, client.1]
        - autotest:
            client.0: [dbench]
            client.1: [bonnie]

    You can also specify a list of tests to run on all clients::

        tasks:
        - ceph:
        - cfuse:
        - autotest:
            all: [dbench]
    """
    assert isinstance(config, dict)
    config = teuthology.replace_all_with_clients(ctx.cluster, config)
    log.info('Setting up autotest...')
    with parallel() as p:
        for role in config.iterkeys():
            (remote,) = ctx.cluster.only(role).remotes.keys()
            p.spawn(_download, remote)

    log.info('Making a separate scratch dir for every client...')
    for role in config.iterkeys():
        assert isinstance(role, basestring)
        PREFIX = 'client.'
        assert role.startswith(PREFIX)
        id_ = role[len(PREFIX):]
        (remote,) = ctx.cluster.only(role).remotes.iterkeys()
        mnt = os.path.join('/tmp/cephtest', 'mnt.{id}'.format(id=id_))
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
            p.spawn(_run_tests, remote, role, tests)

def _download(remote):
    remote.run(
        args=[
            # explicitly does not support multiple autotest tasks
            # in a single run; the result archival would conflict
            'mkdir', '/tmp/cephtest/archive/autotest',
            run.Raw('&&'),
            'mkdir', '/tmp/cephtest/autotest',
            run.Raw('&&'),
            'wget',
            '-nv',
            '--no-check-certificate',
            'https://github.com/ceph/autotest/tarball/ceph',
            '-O-',
            run.Raw('|'),
            'tar',
            '-C', '/tmp/cephtest/autotest',
            '-x',
            '-z',
            '-f-',
            '--strip-components=1',
            ],
        )

def _run_tests(remote, role, tests):
    assert isinstance(role, basestring)
    PREFIX = 'client.'
    assert role.startswith(PREFIX)
    id_ = role[len(PREFIX):]
    mnt = os.path.join('/tmp/cephtest', 'mnt.{id}'.format(id=id_))
    scratch = os.path.join(mnt, 'client.{id}'.format(id=id_))

    assert isinstance(tests, list)
    for idx, testname in enumerate(tests):
        log.info('Running autotest client test #%d: %s...', idx, testname)

        tag = 'client.{id}.num{idx}.{testname}'.format(
            idx=idx,
            testname=testname,
            id=id_,
            )
        control = '/tmp/cephtest/control.{tag}'.format(tag=tag)
        teuthology.write_file(
            remote=remote,
            path=control,
            data='import json; data=json.loads({data!r}); job.run_test(**data)'.format(
                data=json.dumps(dict(
                        url=testname,
                        dir=scratch,
                        # TODO perhaps tag
                        # results will be in /tmp/cephtest/autotest/client/results/dbench
                        # or /tmp/cephtest/autotest/client/results/dbench.{tag}
                        )),
                ),
            )
        remote.run(
            args=[
                '/tmp/cephtest/autotest/client/bin/autotest',
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
                '/tmp/cephtest/autotest/client/results/{tag}'.format(tag=tag),
                '/tmp/cephtest/archive/autotest/{tag}'.format(tag=tag),
                ],
            )

    remote.run(
        args=[
            'rm', '-rf', '--', '/tmp/cephtest/autotest',
            ],
        )
