"""
Ceph FUSE client task
"""

import contextlib
import logging

from teuthology import misc as teuthology
from ..orchestra import run
from teuthology.task.cephfs.fuse_mount import FuseMount

log = logging.getLogger(__name__)


def get_client_configs(ctx, config):
    """
    Get a map of the configuration for each FUSE client in the configuration
    by combining the configuration of the current task with any global overrides.

    :param ctx: Context instance
    :param config: configuration for this task
    :return: dict of client name to config or to None
    """
    if config is None:
        config = dict(('client.{id}'.format(id=id_), None)
                      for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client'))
    elif isinstance(config, list):
        config = dict((name, None) for name in config)

    overrides = ctx.config.get('overrides', {})
    teuthology.deep_merge(config, overrides.get('ceph-fuse', {}))

    return config


@contextlib.contextmanager
def task(ctx, config):
    """
    Mount/unmount a ``ceph-fuse`` client.

    The config is optional and defaults to mounting on all clients. If
    a config is given, it is expected to be a list of clients to do
    this operation on. This lets you e.g. set up one client with
    ``ceph-fuse`` and another with ``kclient``.

    Example that mounts all clients::

        tasks:
        - ceph:
        - ceph-fuse:
        - interactive:

    Example that uses both ``kclient` and ``ceph-fuse``::

        tasks:
        - ceph:
        - ceph-fuse: [client.0]
        - kclient: [client.1]
        - interactive:

    Example that enables valgrind:

        tasks:
        - ceph:
        - ceph-fuse:
            client.0:
              valgrind: [--tool=memcheck, --leak-check=full, --show-reachable=yes]
        - interactive:

    :param ctx: Context
    :param config: Configuration
    """
    log.info('Mounting ceph-fuse clients...')

    testdir = teuthology.get_testdir(ctx)
    config = get_client_configs(ctx, config)

    clients = list(teuthology.get_clients(ctx=ctx, roles=config.keys()))

    fuse_mounts = []
    for id_, remote in clients:
        client_config = config.get("client.%s" % id_)
        if client_config is None:
            client_config = {}

        fuse_mount = FuseMount(client_config, testdir, id_, remote)
        fuse_mounts.append(fuse_mount)

        fuse_mount.mount()

    for mount in fuse_mounts:
        mount.wait_until_mounted()

    try:
        yield
    finally:
        log.info('Unmounting ceph-fuse clients...')
        for mount in fuse_mounts:
            mount.umount()

        run.wait([m.fuse_daemon for m in fuse_mounts.values()], timeout=600)

        for mount in fuse_mounts:
            mount.cleanup()
