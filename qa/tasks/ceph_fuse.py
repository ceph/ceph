"""
Ceph FUSE client task
"""

import contextlib
import logging

from teuthology import misc
from tasks.cephfs.fuse_mount import FuseMount

log = logging.getLogger(__name__)


@contextlib.contextmanager
def task(ctx, config):
    """
    Mount/unmount a ``ceph-fuse`` client.

    The config is optional and defaults to mounting on all clients. If
    a config is given, it is expected to be a list of clients to do
    this operation on. This lets you e.g. set up one client with
    ``ceph-fuse`` and another with ``kclient``.

    ``brxnet`` should be a Private IPv4 Address range, default range is
    [192.168.0.0/16]

    Example that mounts all clients::

        tasks:
        - ceph:
        - ceph-fuse:
        - interactive:
        - brxnet: [192.168.0.0/16]

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

    Example that creates and mounts a subvol:

        overrides:
          ceph:
            subvols:
              create: 2
              subvol_options: "--namespace-isolated --size 25000000000"
          ceph-fuse:
            client.0:
              mount_subvol_num: 0
          kclient:
            client.1:
              mount_subvol_num: 1

    :param ctx: Context
    :param config: Configuration
    """
    log.info('Running ceph_fuse task...')

    if config is None:
        ids = misc.all_roles_of_type(ctx.cluster, 'client')
        client_roles = [f'client.{id_}' for id_ in ids]
        config = dict([r, dict()] for r in client_roles)
    elif isinstance(config, list):
        client_roles = config
        config = dict([r, dict()] for r in client_roles)
    elif isinstance(config, dict):
        client_roles = filter(lambda x: 'client.' in x, config.keys())
    else:
        raise ValueError(f"Invalid config object: {config} ({config.__class__})")
    log.info(f"config is {config}")

    clients = list(misc.get_clients(ctx=ctx, roles=client_roles))
    testdir = misc.get_testdir(ctx)
    all_mounts = getattr(ctx, 'mounts', {})
    mounted_by_me = {}
    skipped = {}
    remotes = set()

    brxnet = config.get("brxnet", None)

    # Construct any new FuseMount instances
    overrides = ctx.config.get('overrides', {}).get('ceph-fuse', {})
    top_overrides = dict(filter(lambda x: 'client.' not in x[0], overrides.items()))
    for id_, remote in clients:
        entity = f"client.{id_}"
        client_config = config.get(entity)
        if client_config is None:
            client_config = {}
        # top level overrides
        misc.deep_merge(client_config, top_overrides)
        # mount specific overrides
        client_config_overrides = overrides.get(entity)
        misc.deep_merge(client_config, client_config_overrides)
        log.info(f"{entity} config is {client_config}")

        remotes.add(remote)
        auth_id = client_config.get("auth_id", id_)
        cephfs_name = client_config.get("cephfs_name")

        skip = client_config.get("skip", False)
        if skip:
            skipped[id_] = skip
            continue

        if id_ not in all_mounts:
            fuse_mount = FuseMount(ctx=ctx, client_config=client_config,
                                   test_dir=testdir, client_id=auth_id,
                                   client_remote=remote, brxnet=brxnet,
                                   cephfs_name=cephfs_name)
            all_mounts[id_] = fuse_mount
        else:
            # Catch bad configs where someone has e.g. tried to use ceph-fuse and kcephfs for the same client
            assert isinstance(all_mounts[id_], FuseMount)

        if not config.get("disabled", False) and client_config.get('mounted', True):
            mounted_by_me[id_] = {"config": client_config, "mount": all_mounts[id_]}

    ctx.mounts = all_mounts

    # Umount any pre-existing clients that we have not been asked to mount
    for client_id in set(all_mounts.keys()) - set(mounted_by_me.keys()) - set(skipped.keys()):
        mount = all_mounts[client_id]
        if mount.is_mounted():
            mount.umount_wait()

    for remote in remotes:
        FuseMount.cleanup_stale_netnses_and_bridge(remote)

    # Mount any clients we have been asked to (default to mount all)
    log.info('Mounting ceph-fuse clients...')
    for info in mounted_by_me.values():
        config = info["config"]
        mount_x = info['mount']
        if config.get("mount_path"):
            mount_x.cephfs_mntpt = config.get("mount_path")
        if config.get("mountpoint"):
            mount_x.hostfs_mntpt = config.get("mountpoint")
        mount_x.mount()

    for info in mounted_by_me.values():
        info["mount"].wait_until_mounted()

    try:
        yield all_mounts
    finally:
        log.info('Unmounting ceph-fuse clients...')

        for info in mounted_by_me.values():
            # Conditional because an inner context might have umounted it
            mount = info["mount"]
            if mount.is_mounted():
                mount.umount_wait()
        for remote in remotes:
            FuseMount.cleanup_stale_netnses_and_bridge(remote)
