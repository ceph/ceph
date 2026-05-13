import os

from collections import Counter
from typing import List, Optional

from .container_types import CephContainer, BasicContainer
from .context import CephadmContext
from cephadmlib.context_getters import fetch_custom_config_files


def _entrypoint_flag_name(arg: str) -> Optional[str]:
    """Return the flag name (e.g. ``--web.external-url``) from an entrypoint
    arg, or None for non-flag tokens. The name is the prefix before the
    first ``=`` or whitespace separator, matching how callers expect
    ``--name=value`` and ``--name value`` to behave; a bare ``--name`` token
    returns its own name unchanged.
    """
    if not arg.startswith('--'):
        return None
    for sep in '= \t':
        idx = arg.find(sep)
        if idx > 0:
            return arg[:idx]
    return arg


def _drop_overridden_flags(args: List[str], overrides: List[str]) -> List[str]:
    """Filter ``args`` to drop a cephadm-injected ``--flag`` when the user
    supplies the same flag in ``overrides``, so the user's value wins
    instead of producing a duplicate-flag error from the container's
    binary (Prometheus refuses to start on two ``--web.external-url``).

    Only single-instance built-in flags are dropped. If cephadm itself
    emitted the same flag more than once (alertmanager's per-peer
    ``--cluster.peer``), the flag is treated as multi-valued and the
    built-in copies are kept; the user's extras still ``append`` and
    accumulate, which matches the typical "I want to add a peer"
    intent.
    """
    override_names = {
        name for name in (_entrypoint_flag_name(a) for a in overrides)
        if name is not None
    }
    if not override_names:
        return args
    name_counts = Counter(
        name for name in (_entrypoint_flag_name(a) for a in args)
        if name is not None
    )
    filter_names = {
        name for name in override_names if name_counts.get(name, 0) == 1
    }
    if not filter_names:
        return args
    return [a for a in args if _entrypoint_flag_name(a) not in filter_names]


def enhance_container(ctx: CephadmContext, ctr: BasicContainer) -> None:
    """Given a context and a basic container object, update the container
    object such that it's arguments, entrypoint arguments, and volume mounts
    'inherit' global "extra" -container args, -entrypoint args, and config
    files in a common manner.
    """
    if 'extra_container_args' in ctx and ctx.extra_container_args:
        ctr.container_args.extend(ctx.extra_container_args)
    if 'extra_entrypoint_args' in ctx and ctx.extra_entrypoint_args:
        # Where a user-supplied flag name collides with one cephadm injected,
        # drop the built-in copy so the user's value wins. Without this, the
        # container engine sees ``--flag X --flag Y`` and tools like
        # Prometheus refuse to start ("flag 'X' cannot be repeated").
        ctr.args = _drop_overridden_flags(ctr.args, ctx.extra_entrypoint_args)
        ctr.args.extend(ctx.extra_entrypoint_args)
    ccfiles = fetch_custom_config_files(ctx)
    assert ctr.identity
    if parentfn := getattr(ctr.identity, 'parent_identity', None):
        identity = parentfn()
    else:
        identity = ctr.identity
    if ccfiles:
        mandatory_keys = ['mount_path', 'content']
        for conf in ccfiles:
            if all(k in conf for k in mandatory_keys):
                mount_path = conf['mount_path']
                file_path = os.path.join(
                    ctx.data_dir,
                    identity.fsid,
                    'custom_config_files',
                    identity.daemon_name,
                    os.path.basename(mount_path),
                )
                ctr.volume_mounts[file_path] = mount_path


def to_deployment_container(
    ctx: CephadmContext, ctr: CephContainer
) -> CephContainer:
    """Given a standard ceph container instance return a CephContainer
    prepared for a deployment as a daemon, having the extra args and
    custom configurations added.
    NOTE: The `ctr` object is mutated before being returned.
    """
    enhance_container(ctx, ctr)
    return ctr
