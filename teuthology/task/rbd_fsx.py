import contextlib
import logging

from ..orchestra import run
from teuthology.parallel import parallel

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
    (remote,) = ctx.cluster.only(role).remotes.iterkeys()
    remote.run(
        args=[
            'CEPH_CONF=/tmp/cephtest/ceph.conf',
            'LD_LIBRARY_PATH=/tmp/cephtest/binary/usr/local/lib',
            '/tmp/cephtest/enable-coredump',
            '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
            '/tmp/cephtest/archive/coverage',
            '/tmp/cephtest/binary/usr/local/bin/test_librbd_fsx',
            '-d',
            '-W', '-R', # mmap doesn't work with rbd
            '-p', str(config.get('progress_interval', 100)),  # show progress
            '-P', '/tmp/cephtest/archive',
            '-t', str(config.get('truncbdy',1)),
            '-l', str(config.get('size', 1073741824)),
            '-S', str(config.get('seed', 0)),
            '-N', str(config.get('ops', 1000)),
            'pool_{pool}'.format(pool=role),
            'image_{image}'.format(image=role),
            ],
        )
