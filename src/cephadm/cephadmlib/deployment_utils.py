import os

from .container_types import CephContainer
from .context import CephadmContext
from cephadmlib.context_getters import fetch_custom_config_files


def to_deployment_container(
    ctx: CephadmContext, ctr: CephContainer
) -> CephContainer:
    """Given a standard ceph container instance return a CephContainer
    prepared for a deployment as a daemon, having the extra args and
    custom configurations added.
    NOTE: The `ctr` object is mutated before being returned.
    """
    if 'extra_container_args' in ctx and ctx.extra_container_args:
        ctr.container_args.extend(ctx.extra_container_args)
    if 'extra_entrypoint_args' in ctx and ctx.extra_entrypoint_args:
        ctr.args.extend(ctx.extra_entrypoint_args)
    ccfiles = fetch_custom_config_files(ctx)
    if ccfiles:
        mandatory_keys = ['mount_path', 'content']
        for conf in ccfiles:
            if all(k in conf for k in mandatory_keys):
                mount_path = conf['mount_path']
                assert ctr.identity
                file_path = os.path.join(
                    ctx.data_dir,
                    ctr.identity.fsid,
                    'custom_config_files',
                    ctr.identity.daemon_name,
                    os.path.basename(mount_path),
                )
                ctr.volume_mounts[file_path] = mount_path
    return ctr
