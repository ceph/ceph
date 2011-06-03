import contextlib
import json
import logging
import os

from teuthology import misc as teuthology
from orchestra import run

log = logging.getLogger(__name__)

def task(ctx, config):
    assert isinstance(config, dict)

    log.info('Setting up autotest...')
    for role in config.iterkeys():
        # TODO parallelize
        ctx.cluster.only(role).run(
            args=[
                'mkdir', '/tmp/cephtest/autotest',
                run.Raw('&&'),
                'wget',
                '-nv',
                '--no-check-certificate',
                'https://github.com/tv42/autotest/tarball/ceph',
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

    # TODO parallelize
    for role, tests in config.iteritems():
        assert isinstance(role, basestring)
        PREFIX = 'client.'
        assert role.startswith(PREFIX)
        id_ = role[len(PREFIX):]
        (remote,) = ctx.cluster.only(role).remotes.iterkeys()
        mnt = os.path.join('/tmp/cephtest', 'mnt.{id}'.format(id=id_))
        scratch = os.path.join(mnt, 'client.{id}'.format(id=id_))

        assert isinstance(tests, list)
        for testname in tests:
            log.info('Running autotest client test %s...', testname)

            tag = '{testname}.client.{id}'.format(
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
