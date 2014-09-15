"""
Mount/unmount a ``kernel`` client.
"""
import contextlib
import logging

from teuthology import misc
from cephfs.kernel_mount import KernelMount

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    Mount/unmount a ``kernel`` client.

    The config is optional and defaults to mounting on all clients. If
    a config is given, it is expected to be a list of clients to do
    this operation on. This lets you e.g. set up one client with
    ``ceph-fuse`` and another with ``kclient``.

    Example that mounts all clients::

        tasks:
        - ceph:
        - kclient:
        - interactive:

    Example that uses both ``kclient` and ``ceph-fuse``::

        tasks:
        - ceph:
        - ceph-fuse: [client.0]
        - kclient: [client.1]
        - interactive:

    :param ctx: Context
    :param config: Configuration
    """
    log.info('Mounting kernel clients...')
    assert config is None or isinstance(config, list), \
        "task kclient got invalid config"

    if config is None:
        config = ['client.{id}'.format(id=id_)
                  for id_ in misc.all_roles_of_type(ctx.cluster, 'client')]
    clients = list(misc.get_clients(ctx=ctx, roles=config))

    test_dir = misc.get_testdir(ctx)

    # Assemble mon addresses
    remotes_and_roles = ctx.cluster.remotes.items()
    roles = [roles for (remote_, roles) in remotes_and_roles]
    ips = [remote_.ssh.get_transport().getpeername()[0]
           for (remote_, _) in remotes_and_roles]
    mons = misc.get_mons(roles, ips).values()

    mounts = {}
    for id_, remote in clients:
        kernel_mount = KernelMount(
            mons,
            test_dir,
            id_,
            remote,
            ctx.teuthology_config.get('ipmi_user', None),
            ctx.teuthology_config.get('ipmi_password', None),
            ctx.teuthology_config.get('ipmi_domain', None)
        )

        mounts[id_] = kernel_mount

        kernel_mount.mount()

    ctx.mounts = mounts
    try:
        yield mounts
    finally:
        log.info('Unmounting kernel clients...')
        for mount in mounts.values():
            mount.umount()
