"""
Run fsx on an rbd image
"""
import contextlib
import logging

from teuthology.parallel import parallel
from teuthology import misc as teuthology

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
    """
    log.info('starting rbd_fsx...')
    with parallel() as p:
        for role in config['clients']:
            p.spawn(_run_one_client, ctx, config, role)
    yield

def _run_one_client(ctx, config, role):
    """Spawned task that runs the client"""
    testdir = teuthology.get_testdir(ctx)
    remote = teuthology.get_single_remote_value(ctx, role)
    remote.run(
        args=[
            'adjust-ulimits',
            'ceph-coverage',
            '{tdir}/archive/coverage'.format(tdir=testdir),
            'ceph_test_librbd_fsx',
            '-d',
            '-W', '-R', # mmap doesn't work with rbd
            '-p', str(config.get('progress_interval', 100)),  # show progress
            '-P', '{tdir}/archive'.format(tdir=testdir),
            '-t', str(config.get('truncbdy',1)),
            '-l', str(config.get('size', 250000000)),
            '-S', str(config.get('seed', 0)),
            '-N', str(config.get('ops', 1000)),
            'pool_{pool}'.format(pool=role),
            'image_{image}'.format(image=role),
            ],
        )
