import contextlib
import json
import pathlib
import shlex

from typing import Any, Dict, Union, List, IO, TextIO, Optional, cast

from . import templating
from .container_engines import Podman
from .container_types import CephContainer, InitContainer, SidecarContainer
from .context import CephadmContext
from .context_getters import fetch_meta
from .daemon_identity import DaemonIdentity, DaemonSubIdentity
from .file_utils import write_new
from .net_utils import EndPoint


# Ideally, all ContainerCommands would be converted to init containers. Until
# that is done one can wrap a CephContainer in a ContainerCommand object and
# pass that as a pre- or post- command to run arbitrary container based
# commands in the script.
class ContainerCommand:
    def __init__(
        self,
        container: CephContainer,
        comment: str = '',
        background: bool = False,
    ):
        self.container = container
        self.comment = comment
        self.background = background


Command = Union[List[str], str, ContainerCommand]


def write_service_scripts(
    ctx: CephadmContext,
    ident: DaemonIdentity,
    *,
    container: CephContainer,
    init_containers: Optional[List[InitContainer]] = None,
    sidecars: Optional[List[SidecarContainer]] = None,
    endpoints: Optional[List[EndPoint]] = None,
    pre_start_commands: Optional[List[Command]] = None,
    post_stop_commands: Optional[List[Command]] = None,
    timeout: Optional[int] = None,
) -> None:
    """Write the scripts that systemd services will call in order to
    start/stop/etc components of a cephadm managed daemon. Also writes some
    metadata about the service getting deployed.
    """
    data_dir = pathlib.Path(ident.data_dir(ctx.data_dir))
    run_file_path = data_dir / 'unit.run'
    meta_file_path = data_dir / 'unit.meta'
    post_stop_file_path = data_dir / 'unit.poststop'
    stop_file_path = data_dir / 'unit.stop'
    image_file_path = data_dir / 'unit.image'
    initctr_file_path = data_dir / 'init_containers.run'
    # use an ExitStack to make writing the files an all-or-nothing affair. If
    # any file fails to write then the write_new'd file will not get renamed
    # into place
    with contextlib.ExitStack() as estack:
        # write out the main file to run (start) a service
        runf = estack.enter_context(write_new(run_file_path))
        runf.write('set -e\n')
        for command in pre_start_commands or []:
            _write_command(ctx, runf, command)
        _write_container_cmd_to_bash(ctx, runf, container, ident.daemon_name)

        # some metadata about the deploy
        metaf = estack.enter_context(write_new(meta_file_path))
        meta: Dict[str, Any] = fetch_meta(ctx)
        meta.update(
            {
                'memory_request': int(ctx.memory_request)
                if ctx.memory_request
                else None,
                'memory_limit': int(ctx.memory_limit)
                if ctx.memory_limit
                else None,
            }
        )
        if not meta.get('ports'):
            if endpoints:
                meta['ports'] = [e.port for e in endpoints]
            else:
                meta['ports'] = []
        metaf.write(json.dumps(meta, indent=4) + '\n')

        # init-container commands
        if init_containers:
            initf = estack.enter_context(write_new(initctr_file_path))
            _write_init_containers_script(ctx, initf, init_containers)

        # sidecar container scripts
        for sidecar in sidecars or []:
            assert isinstance(sidecar.identity, DaemonSubIdentity)
            script_path = sidecar.identity.sidecar_script(ctx.data_dir)
            scsf = estack.enter_context(write_new(script_path))
            _write_sidecar_script(
                ctx,
                scsf,
                sidecar,
                f'sidecar: {sidecar.identity.subcomponent}',
            )

        # post-stop command(s)
        pstopf = estack.enter_context(write_new(post_stop_file_path))
        # this is a fallback to eventually stop any underlying container that
        # was not stopped properly by unit.stop, this could happen in very slow
        # setups as described in the issue
        # https://tracker.ceph.com/issues/58242.
        _write_stop_actions(ctx, cast(TextIO, pstopf), container, timeout)
        for command in post_stop_commands or []:
            _write_command(ctx, pstopf, command)

        # stop command(s)
        stopf = estack.enter_context(write_new(stop_file_path))
        _write_stop_actions(ctx, cast(TextIO, stopf), container, timeout)

        if container:
            imgf = estack.enter_context(write_new(image_file_path))
            imgf.write(container.image + '\n')


def _write_container_cmd_to_bash(
    ctx: CephadmContext,
    file_obj: IO[str],
    container: 'CephContainer',
    comment: Optional[str] = None,
    background: Optional[bool] = False,
) -> None:
    if comment:
        # Sometimes adding a comment, especially if there are multiple containers in one
        # unit file, makes it easier to read and grok.
        assert '\n' not in comment
        file_obj.write(f'# {comment}\n')
    # Sometimes, adding `--rm` to a run_cmd doesn't work. Let's remove the container manually
    _bash_cmd(
        file_obj, container.rm_cmd(old_cname=True), check=False, stderr=False
    )
    _bash_cmd(file_obj, container.rm_cmd(), check=False, stderr=False)

    # Sometimes, `podman rm` doesn't find the container. Then you'll have to add `--storage`
    if isinstance(ctx.container_engine, Podman):
        _bash_cmd(
            file_obj,
            container.rm_cmd(storage=True),
            check=False,
            stderr=False,
        )
        _bash_cmd(
            file_obj,
            container.rm_cmd(old_cname=True, storage=True),
            check=False,
            stderr=False,
        )

    # container run command
    _bash_cmd(file_obj, container.run_cmd(), background=bool(background))


def _write_stop_actions(
    ctx: CephadmContext,
    f: TextIO,
    container: 'CephContainer',
    timeout: Optional[int],
) -> None:
    # following generated script basically checks if the container exists
    # before stopping it. Exit code will be success either if it doesn't
    # exist or if it exists and is stopped successfully.
    container_exists = f'{ctx.container_engine.path} inspect %s &>/dev/null'
    f.write(
        f'! {container_exists % container.old_cname} || {" ".join(container.stop_cmd(old_cname=True, timeout=timeout))} \n'
    )
    f.write(
        f'! {container_exists % container.cname} || {" ".join(container.stop_cmd(timeout=timeout))} \n'
    )


def _write_init_containers_script(
    ctx: CephadmContext,
    file_obj: IO[str],
    init_containers: List[InitContainer],
    comment: str = 'start and stop init containers',
) -> None:
    has_podman_engine = isinstance(ctx.container_engine, Podman)
    templating.render_to_file(
        file_obj,
        ctx,
        templating.Templates.init_ctr_run,
        init_containers=init_containers,
        comment=comment,
        has_podman_engine=has_podman_engine,
    )


def _write_sidecar_script(
    ctx: CephadmContext,
    file_obj: IO[str],
    sidecar: SidecarContainer,
    comment: str = '',
) -> None:
    has_podman_engine = isinstance(ctx.container_engine, Podman)
    templating.render_to_file(
        file_obj,
        ctx,
        templating.Templates.sidecar_run,
        sidecar=sidecar,
        comment=comment,
        has_podman_engine=has_podman_engine,
    )


def _bash_cmd(
    fh: IO[str],
    cmd: List[str],
    check: bool = True,
    background: bool = False,
    stderr: bool = True,
) -> None:
    line = ' '.join(shlex.quote(arg) for arg in cmd)
    if not check:
        line = f'! {line}'
    if not stderr:
        line = f'{line} 2> /dev/null'
    if background:
        line = f'{line} &'
    fh.write(line)
    fh.write('\n')


def _write_command(
    ctx: CephadmContext,
    fh: IO[str],
    cmd: Command,
) -> None:
    """Wrapper func for turning a command list or string into something suitable
    for appending to a run script.
    """
    if isinstance(cmd, list):
        _bash_cmd(fh, cmd)
    elif isinstance(cmd, ContainerCommand):
        _write_container_cmd_to_bash(
            ctx,
            fh,
            cmd.container,
            comment=cmd.comment,
            background=cmd.background,
        )
    else:
        fh.write(cmd)
        if not cmd.endswith('\n'):
            fh.write('\n')
