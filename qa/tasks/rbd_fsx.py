"""
Run fsx on an rbd image
"""
import contextlib
import logging

from teuthology.exceptions import ConfigError
from teuthology.parallel import parallel
from teuthology import misc as teuthology
from tasks.ceph_manager import get_valgrind_args

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    Run fsx on an rbd image.

    Currently this requires running as client.admin
    to create a pool.

    Specify which clients to run on as a list::

      tasks:
        ceph:
        rbd_fsx:
          clients: [client.0, client.1]

    You can optionally change some properties of fsx:

      tasks:
        ceph:
        rbd_fsx:
          clients: <list of clients>
          seed: <random seed number, or 0 to use the time>
          ops: <number of operations to do>
          size: <maximum image size in bytes>
          valgrind: [--tool=<valgrind tool>]
    """
    log.info('starting rbd_fsx...')
    with parallel() as p:
        for role in config['clients']:
            p.spawn(_run_one_client, ctx, config, role)
    yield

def _run_one_client(ctx, config, role):
    """Spawned task that runs the client"""
    krbd = config.get('krbd', False)
    nbd = config.get('nbd', False)
    testdir = teuthology.get_testdir(ctx)
    (remote,) = ctx.cluster.only(role).remotes.keys()

    args = []
    if krbd or nbd:
        args.append('sudo') # rbd(-nbd) map/unmap need privileges
    args.extend([
        'adjust-ulimits',
        'ceph-coverage',
        '{tdir}/archive/coverage'.format(tdir=testdir)
    ])

    overrides = ctx.config.get('overrides', {})
    teuthology.deep_merge(config, overrides.get('rbd_fsx', {}))

    if config.get('valgrind'):
        args = get_valgrind_args(
            testdir,
            'fsx_{id}'.format(id=role),
            args,
            config.get('valgrind')
        )

    cluster_name, type_, client_id = teuthology.split_role(role)
    if type_ != 'client':
        msg = 'client role ({0}) must be a client'.format(role)
        raise ConfigError(msg)

    args.extend([
        'ceph_test_librbd_fsx',
        '--cluster', cluster_name,
        '--id', client_id,
        '-d', # debug output for all operations
        '-W', '-R', # mmap doesn't work with rbd
        '-p', str(config.get('progress_interval', 100)), # show progress
        '-P', '{tdir}/archive'.format(tdir=testdir),
        '-r', str(config.get('readbdy',1)),
        '-w', str(config.get('writebdy',1)),
        '-t', str(config.get('truncbdy',1)),
        '-h', str(config.get('holebdy',1)),
        '-l', str(config.get('size', 250000000)),
        '-S', str(config.get('seed', 0)),
        '-N', str(config.get('ops', 1000)),
    ])
    if krbd:
        args.append('-K') # -K enables krbd mode
    if nbd:
        args.append('-M') # -M enables nbd mode
    if config.get('direct_io', False):
        args.append('-Z') # -Z use direct IO
    if not config.get('randomized_striping', True):
        args.append('-U') # -U disables randomized striping
    if not config.get('punch_holes', True):
        args.append('-H') # -H disables discard ops
    if config.get('deep_copy', False):
        args.append('-g') # -g deep copy instead of clone
    if config.get('journal_replay', False):
        args.append('-j') # -j replay all IO events from journal
    if config.get('keep_images', False):
        args.append('-k') # -k keep images on success
    args.extend([
        config.get('pool_name', 'pool_{pool}'.format(pool=role)),
        'image_{image}'.format(image=role),
    ])

    remote.run(args=args)
