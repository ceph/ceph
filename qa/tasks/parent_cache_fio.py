"""
parent cache fio task

"""
import contextlib
import json
import logging
import os
import StringIO

from teuthology.parallel import parallel
from teuthology import misc as teuthology
from tempfile import NamedTemporaryFile
from teuthology.orchestra import run
from teuthology.packaging import install_package, remove_package

from rbd_fio import run_fio

log = logging.getLogger(__name__)

def setup_parent_cache(ctx, config):
    """
    setup ceph-immutable-object-cache daemon
    """
    for client, client_config in config.items():
        parent_cache_enabled = client_config.get('rbd_parent_cache_enabled', True)
        (remote,) = ctx.cluster.only(client).remotes.keys()
        if parent_cache_enabled:
            remote.run(args=[
                'sudo', 'killall', '-s', '9', 'ceph-immutable-object-cache', run.Raw('||'), 'true',
            ])
            remote.run(args=[
                'sudo', 'ceph-immutable-object-cache', '-b',
            ])

def teardown_parent_cache(ctx, config):
    """
    teardown ceph-immutable-object-cache daemon
    """
    for client, client_config in config.items():
        parent_cache_enabled = client_config.get('rbd_parent_cache_enabled', True)
        (remote,) = ctx.cluster.only(client).remotes.keys()
        if parent_cache_enabled:
            remote.run(args=[
                'sudo', 'killall', '-s', '9', 'ceph-immutable-object-cache', run.Raw('||'), 'true',
            ])
            cache_path = client_config.get('immutable_object_cache_path', '/tmp/ceph-immutable-object-cache')
            remote.run(args=[
                'sudo', 'rm', '-rf', cache_path, run.Raw('||'), 'true',
            ])


@contextlib.contextmanager
def task(ctx, config):
    """
    use fio to test parent cache
    """
    if config.get('all'):
        client_config = config['all']
    clients = ctx.cluster.only(teuthology.is_type('client'))
    rbd_test_dir = teuthology.get_testdir(ctx) + "/rbd_fio_test"
    for remote,role in clients.remotes.items():
        setup_parent_cache(ctx, config)
        if 'client_config' in locals():
           with parallel() as p:
               p.spawn(run_fio, remote, client_config, rbd_test_dir)
        else:
           for client_config in config:
              if client_config in role:
                 with parallel() as p:
                     p.spawn(run_fio, remote, config[client_config], rbd_test_dir)

    yield
    teardown_parent_cache(ctx, config)

