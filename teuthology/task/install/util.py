import contextlib
import logging
import os

from teuthology import misc as teuthology
from teuthology import packaging
from teuthology.orchestra import run

log = logging.getLogger(__name__)


def _get_builder_project(ctx, remote, config):
    return packaging.get_builder_project()(
        config.get('project', 'ceph'),
        config,
        remote=remote,
        ctx=ctx
    )


def _get_local_dir(config, remote):
    """
    Extract local directory name from the task lists.
    Copy files over to the remote site.
    """
    ldir = config.get('local', None)
    if ldir:
        remote.run(args=['sudo', 'mkdir', '-p', ldir])
        for fyle in os.listdir(ldir):
            fname = "%s/%s" % (ldir, fyle)
            teuthology.sudo_write_file(
                remote, fname, open(fname).read(), '644')
    return ldir


def get_flavor(config):
    """
    Determine the flavor to use.
    """
    config = config or dict()
    flavor = config.get('flavor', 'basic')

    if config.get('path'):
        # local dir precludes any other flavors
        flavor = 'local'
    else:
        if config.get('valgrind'):
            flavor = 'notcmalloc'
        else:
            if config.get('coverage'):
                flavor = 'gcov'
    return flavor

def _ship_utilities(ctx):
    """
    Write a copy of valgrind.supp to each of the remote sites.  Set executables
    used by Ceph in /usr/local/bin.  When finished (upon exit of the teuthology
    run), remove these files.

    :param ctx: Context
    """
    testdir = teuthology.get_testdir(ctx)
    filenames = []

    log.info('Shipping valgrind.supp...')
    assert 'suite_path' in ctx.config
    try:
        with open(
            os.path.join(ctx.config['suite_path'], 'valgrind.supp'),
            'rb'
                ) as f:
            fn = os.path.join(testdir, 'valgrind.supp')
            filenames.append(fn)
            for rem in ctx.cluster.remotes.keys():
                teuthology.sudo_write_file(
                    remote=rem,
                    path=fn,
                    data=f,
                    )
                f.seek(0)
    except IOError as e:
        log.info('Cannot ship supression file for valgrind: %s...', e.strerror)

    FILES = ['daemon-helper', 'adjust-ulimits']
    destdir = '/usr/bin'
    for filename in FILES:
        log.info('Shipping %r...', filename)
        src = os.path.join(os.path.dirname(__file__), filename)
        dst = os.path.join(destdir, filename)
        filenames.append(dst)
        with open(src, 'rb') as f:
            for rem in ctx.cluster.remotes.keys():
                teuthology.sudo_write_file(
                    remote=rem,
                    path=dst,
                    data=f,
                )
                f.seek(0)
                rem.run(
                    args=[
                        'sudo',
                        'chmod',
                        'a=rx',
                        '--',
                        dst,
                    ],
                )
    return filenames

def _remove_utilities(ctx, filenames):
    """
    Remove the shipped utilities.

    :param ctx: Context
    :param filenames: The utilities install paths
    """
    log.info('Removing shipped files: %s...', ' '.join(filenames))
    if filenames == []:
        return
    run.wait(
        ctx.cluster.run(
            args=[
                'sudo',
                'rm',
                '-f',
                '--',
            ] + list(filenames),
            wait=False,
        ),
    )

@contextlib.contextmanager
def ship_utilities(ctx, config):
    """
    Ship utilities during the first call, and skip it in the following ones.
    See also `_ship_utilities`.

    :param ctx: Context
    :param config: Configuration
    """
    assert config is None

    do_ship_utilities = ctx.get('do_ship_utilities', True)
    if do_ship_utilities:
        ctx['do_ship_utilities'] = False
        filenames = _ship_utilities(ctx)
        try:
            yield
        finally:
            _remove_utilities(ctx, filenames)
    else:
        log.info('Utilities already shipped, skip it...')
        yield
