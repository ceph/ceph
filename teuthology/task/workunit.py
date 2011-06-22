import logging
import os

from teuthology import misc as teuthology
from teuthology import parallel
from orchestra import run

log = logging.getLogger(__name__)

def task(ctx, config):
    """
    Run ceph all workunits found under the specified path.

    For example::

        tasks:
        - ceph:
        - cfuse: [client.0]
        - workunit:
            client.0: [direct_io, xattrs.sh]
            client.1: [snaps]
    """
    assert isinstance(config, dict)

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

    args = [[ctx, role, tests] for role, tests in config.iteritems()]
    parallel.run(_run_tests, args)

def _run_tests(ctx, role, tests):
    assert isinstance(role, basestring)
    PREFIX = 'client.'
    assert role.startswith(PREFIX)
    id_ = role[len(PREFIX):]
    (remote,) = ctx.cluster.only(role).remotes.iterkeys()
    mnt = os.path.join('/tmp/cephtest', 'mnt.{id}'.format(id=id_))
    # subdir so we can remove and recreate this a lot without sudo
    scratch_tmp = os.path.join(mnt, 'client.{id}'.format(id=id_), 'tmp')
    srcdir = '/tmp/cephtest/workunit.{role}'.format(role=role)

    remote.run(
        logger=log.getChild(role),
        args=[
            'mkdir', '--', srcdir,
            run.Raw('&&'),
            'git',
            'archive',
            '--format=tar',
            '--remote=git://ceph.newdream.net/git/ceph.git',
            '--',
            # TODO make branch/tag/sha1 used configurable
            'HEAD:qa/workunits/',
            run.Raw('|'),
            'tar',
            '-C', srcdir,
            '-x',
            '-f-',
            run.Raw('&&'),
            'cd', '--', srcdir,
            run.Raw('&&'),
            'if', 'test', '-e', 'Makefile', run.Raw(';'), 'then', 'make', run.Raw(';'), 'fi',
            run.Raw('&&'),
            'find', '-executable', '-type', 'f', '-printf', r'%P\0'.format(srcdir=srcdir),
            run.Raw('>/tmp/cephtest/workunits.list'),
            ],
        )

    workunits = sorted(teuthology.get_file(remote, '/tmp/cephtest/workunits.list').split('\0'))
    assert workunits

    try:
        assert isinstance(tests, list)
        for spec in tests:
            log.info('Running workunits matching %s on %s...', spec, role)
            prefix = '{spec}/'.format(spec=spec)
            to_run = [w for w in workunits if w == spec or w.startswith(prefix)]
            if not to_run:
                raise RuntimeError('Spec did not match any workunits: {spec!r}'.format(spec=spec))
            for workunit in to_run:
                log.info('Running workunit %s...', workunit)
                remote.run(
                    logger=log.getChild(role),
                    args=[
                        'mkdir', '--', scratch_tmp,
                        run.Raw('&&'),
                        'cd', '--', scratch_tmp,
                        run.Raw('&&'),
                        run.Raw('PATH="$PATH:/tmp/cephtest/binary/usr/local/bin"'),
                        run.Raw('LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/tmp/cephtest/binary/usr/local/lib"'),
                        run.Raw('CEPH_ARGS="-c /tmp/cephtest/ceph.conf"'),
                        '{srcdir}/{workunit}'.format(
                            srcdir=srcdir,
                            workunit=workunit,
                            ),
                        run.Raw('&&'),
                        'rm', '-rf', '--', scratch_tmp,
                        ],
                    )
    finally:
        remote.run(
            logger=log.getChild(role),
            args=[
                'rm', '-rf', '--', '/tmp/cephtest/workunits.list', srcdir,
                ],
            )
