"""
Ceph FUSE client task
"""

import contextlib
import logging

from teuthology import misc as teuthology
from cephfs.fuse_mount import FuseMount

log = logging.getLogger(__name__)


def get_client_configs(ctx, config):
    """
    Get a map of the configuration for each FUSE client in the configuration by
    combining the configuration of the current task with any global overrides.

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

    Example that stops an already-mounted client:

    ::

        tasks:
            - ceph:
            - ceph-fuse: [client.0]
            - ... do something that requires the FS mounted ...
            - ceph-fuse:
                client.0:
                    mounted: false
            - ... do something that requires the FS unmounted ...

    Example that adds more generous wait time for mount (for virtual machines):

        tasks:
        - ceph:
        - ceph-fuse:
            client.0:
              mount_wait: 60 # default is 0, do not wait before checking /sys/
              mount_timeout: 120 # default is 30, give up if /sys/ is not populated
        - interactive:

    :param ctx: Context
    :param config: Configuration
    """
    log.info('Mounting ceph-fuse clients...')

    testdir = teuthology.get_testdir(ctx)
    config = get_client_configs(ctx, config)

    # List clients we will configure mounts for, default is all clients
    clients = list(teuthology.get_clients(ctx=ctx, roles=config.keys()))

    all_mounts = getattr(ctx, 'mounts', {})
    mounted_by_me = {}

    # Construct any new FuseMount instances
    for id_, remote in clients:
        client_config = config.get("client.%s" % id_)
        if client_config is None:
            client_config = {}

        if id_ not in all_mounts:
            fuse_mount = FuseMount(client_config, testdir, id_, remote)
            all_mounts[id_] = fuse_mount
        else:
            # Catch bad configs where someone has e.g. tried to use ceph-fuse and kcephfs for the same client
            assert isinstance(all_mounts[id_], FuseMount)

        if client_config.get('mounted', True):
            mounted_by_me[id_] = all_mounts[id_]

    ctx.mounts = all_mounts

    # Mount any clients we have been asked to (default to mount all)
    for mount in mounted_by_me.values():
        mount.mount()

    for mount in mounted_by_me.values():
        mount.wait_until_mounted()

    # Umount any pre-existing clients that we have not been asked to mount
    for client_id in set(all_mounts.keys()) - set(mounted_by_me.keys()):
        mount = all_mounts[client_id]
        if mount.is_mounted():
            mount.umount_wait()

    try:
        yield all_mounts
    finally:
        log.info('Unmounting ceph-fuse clients...')

        for mount in mounted_by_me.values():
            # Conditional because an inner context might have umounted it
            if mount.is_mounted():
                mount.umount_wait()
