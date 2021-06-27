"""
Mount/unmount a ``kernel`` client.
"""
import contextlib
import logging

from teuthology.misc import deep_merge
from teuthology.orchestra.run import CommandFailedError
from teuthology import misc
from teuthology.contextutil import MaxWhileTries
from tasks.cephfs.kernel_mount import KernelMount

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    Mount/unmount a ``kernel`` client.

    The config is optional and defaults to mounting on all clients. If
    a config is given, it is expected to be a list of clients to do
    this operation on. This lets you e.g. set up one client with
    ``ceph-fuse`` and another with ``kclient``.

    ``brxnet`` should be a Private IPv4 Address range, default range is
    [192.168.0.0/16]

    Example that mounts all clients::

        tasks:
        - ceph:
        - kclient:
        - interactive:
        - brxnet: [192.168.0.0/16]

    Example that uses both ``kclient` and ``ceph-fuse``::

        tasks:
        - ceph:
        - ceph-fuse: [client.0]
        - kclient: [client.1]
        - interactive:


    Pass a dictionary instead of lists to specify per-client config:

        tasks:
        -kclient:
            client.0:
                debug: true
                mntopts: ["nowsync"]

    :param ctx: Context
    :param config: Configuration
    """
    log.info('Mounting kernel clients...')

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

    test_dir = misc.get_testdir(ctx)

    for id_, remote in clients:
        KernelMount.cleanup_stale_netnses_and_bridge(remote)

    mounts = {}
    overrides = ctx.config.get('overrides', {}).get('kclient', {})
    top_overrides = dict(filter(lambda x: 'client.' not in x[0], overrides.items()))
    for id_, remote in clients:
        entity = f"client.{id_}"
        client_config = config.get(entity)
        if client_config is None:
            client_config = {}
        # top level overrides
        deep_merge(client_config, top_overrides)
        # mount specific overrides
        client_config_overrides = overrides.get(entity)
        deep_merge(client_config, client_config_overrides)
        log.info(f"{entity} config is {client_config}")

        cephfs_name = client_config.get("cephfs_name")
        if config.get("disabled", False) or not client_config.get('mounted', True):
            continue

        kernel_mount = KernelMount(
            ctx=ctx,
            test_dir=test_dir,
            client_id=id_,
            client_remote=remote,
            brxnet=ctx.teuthology_config.get('brxnet', None),
            config=client_config,
            cephfs_name=cephfs_name)

        mounts[id_] = kernel_mount

        if client_config.get('debug', False):
            remote.run(args=["sudo", "bash", "-c", "echo 'module ceph +p' > /sys/kernel/debug/dynamic_debug/control"])
            remote.run(args=["sudo", "bash", "-c", "echo 'module libceph +p' > /sys/kernel/debug/dynamic_debug/control"])

        kernel_mount.mount(mntopts=client_config.get('mntopts', []))

    def umount_all():
        log.info('Unmounting kernel clients...')

        forced = False
        for mount in mounts.values():
            if mount.is_mounted():
                try:
                    mount.umount()
                except (CommandFailedError, MaxWhileTries):
                    log.warning("Ordinary umount failed, forcing...")
                    forced = True
                    mount.umount_wait(force=True)

        for id_, remote in clients:
            KernelMount.cleanup_stale_netnses_and_bridge(remote)

        return forced

    ctx.mounts = mounts
    try:
        yield mounts
    except:
        umount_all()  # ignore forced retval, we are already in error handling
    finally:

        forced = umount_all()
        if forced:
            # The context managers within the kclient manager worked (i.e.
            # the test workload passed) but for some reason we couldn't
            # umount, so turn this into a test failure.
            raise RuntimeError("Kernel mounts did not umount cleanly")
