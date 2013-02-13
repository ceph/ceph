import contextlib
import logging

from ..orchestra import run
from teuthology import misc as teuthology

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    Run RadosModel-based integration tests.

    The config should be as follows::

        rados:
          clients: [client list]
          ops: <number of ops>
          objects: <number of objects to use>
          max_in_flight: <max number of operations in flight>
          object_size: <size of objects in bytes>
          min_stride_size: <minimum write stride size in bytes>
          max_stride_size: <maximum write stride size in bytes>
          op_weights: <dictionary mapping operation type to integer weight>

    For example::

        tasks:
        - ceph:
        - rados:
            clients: [client.0]
            ops: 1000
            max_seconds: 0   # 0 for no limit
            objects: 25
            max_in_flight: 16
            object_size: 4000000
            min_stride_size: 1024
            max_stride_size: 4096
            op_weights:
              read: 20
              write: 10
              delete: 2
              snap_create: 3
              rollback: 2
              snap_remove: 0
        - interactive:
    """
    log.info('Beginning rados...')
    assert isinstance(config, dict), \
        "please list clients to run on"

    object_size = int(config.get('object_size', 4000000))
    op_weights = config.get('op_weights', {})
    testdir = teuthology.get_testdir(ctx)
    args = [
        'CEPH_CONF={tdir}/ceph.conf'.format(tdir=testdir),
        'LD_LIBRARY_PATH={tdir}/binary/usr/local/lib'.format(tdir=testdir),
        '{tdir}/enable-coredump'.format(tdir=testdir),
        '{tdir}/binary/usr/local/bin/ceph-coverage'.format(tdir=testdir),
        '{tdir}/archive/coverage'.format(tdir=testdir),
        '{tdir}/binary/usr/local/bin/ceph_test_rados'.format(tdir=testdir),
        '--op', 'read', str(op_weights.get('read', 100)),
        '--op', 'write', str(op_weights.get('write', 100)),
        '--op', 'delete', str(op_weights.get('delete', 10)),
        '--op', 'snap_create', str(op_weights.get('snap_create', 0)),
        '--op', 'snap_remove', str(op_weights.get('snap_remove', 0)),
        '--op', 'rollback', str(op_weights.get('rollback', 0)),
        '--op', 'setattr', str(op_weights.get('setattr', 0)),
        '--op', 'rmattr', str(op_weights.get('rmattr', 0)),
        '--op', 'watch', str(op_weights.get('watch', 0)),
        '--max-ops', str(config.get('ops', 10000)),
        '--objects', str(config.get('objects', 500)),
        '--max-in-flight', str(config.get('max_in_flight', 16)),
        '--size', str(object_size),
        '--min-stride-size', str(config.get('min_stride_size', object_size / 10)),
        '--max-stride-size', str(config.get('max_stride_size', object_size / 5)),
        '--max-seconds', str(config.get('max_seconds', 0))
        ]

    tests = {}
    for role in config.get('clients', ['client.0']):
        assert isinstance(role, basestring)
        PREFIX = 'client.'
        assert role.startswith(PREFIX)
        id_ = role[len(PREFIX):]
        (remote,) = ctx.cluster.only(role).remotes.iterkeys()
        proc = remote.run(
            args=['CEPH_CLIENT_ID={id_}'.format(id_=id_)] + args,
            logger=log.getChild('rados.{id}'.format(id=id_)),
            stdin=run.PIPE,
            wait=False
            )
        tests[id_] = proc

    try:
        yield
    finally:
        log.info('joining rados')
        run.wait(tests.itervalues())
