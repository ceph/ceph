# container_engines.py - container engine types and selection funcs

import os
import logging

from typing import Tuple, List, Optional, Dict, Any

from .call_wrappers import call_throws, call, CallVerbosity
from .context import CephadmContext
from .container_engine_base import ContainerEngine
from .constants import (
    CGROUPS_SPLIT_PODMAN_VERSION,
    DEFAULT_MODE,
    MIN_PODMAN_VERSION,
    PIDS_LIMIT_UNLIMITED_PODMAN_VERSION,
)
from .data_utils import with_units_to_int
from .exceptions import Error


logger = logging.getLogger()


class Podman(ContainerEngine):
    EXE = 'podman'

    def __init__(self) -> None:
        super().__init__()
        self._version: Optional[Tuple[int, ...]] = None

    @property
    def version(self) -> Tuple[int, ...]:
        if self._version is None:
            raise RuntimeError('Please call `get_version` first')
        return self._version

    def get_version(self, ctx: CephadmContext) -> None:
        out, _, _ = call_throws(
            ctx,
            [self.path, 'version', '--format', '{{.Client.Version}}'],
            verbosity=CallVerbosity.QUIET,
        )
        self._version = _parse_podman_version(out)

    def __str__(self) -> str:
        version = '.'.join(map(str, self.version))
        return f'{self.EXE} ({self.path}) version {version}'

    @property
    def supports_split_cgroups(self) -> bool:
        """Return true if this version of podman supports split cgroups."""
        return self.version >= CGROUPS_SPLIT_PODMAN_VERSION

    @property
    def unlimited_pids_option(self) -> str:
        """The option to pass to the container engine for allowing unlimited
        pids (processes).
        """
        if self.version >= PIDS_LIMIT_UNLIMITED_PODMAN_VERSION:
            return '--pids-limit=-1'
        return '--pids-limit=0'

    def service_args(
        self, ctx: CephadmContext, service_name: str
    ) -> List[str]:
        """Return a list of arguments that should be added to the engine's run
        command when starting a long-term service (aka daemon) container.
        """
        args = []
        # if using podman, set -d, --conmon-pidfile & --cidfile flags
        # so service can have Type=Forking
        runtime_dir = '/run'
        args.extend(
            [
                '-d',
                '--log-driver',
                'journald',
                '--conmon-pidfile',
                f'{runtime_dir}/{service_name}-pid',
                '--cidfile',
                f'{runtime_dir}/{service_name}-cid',
            ]
        )
        if self.supports_split_cgroups and not ctx.no_cgroups_split:
            args.append('--cgroups=split')
        # if /etc/hosts doesn't exist, we can be confident
        # users aren't using it for host name resolution
        # and adding --no-hosts avoids bugs created in certain daemons
        # by modifications podman makes to /etc/hosts
        # https://tracker.ceph.com/issues/58532
        # https://tracker.ceph.com/issues/57018
        if not os.path.exists('/etc/hosts'):
            args.append('--no-hosts')
        return args

    def update_mounts(
        self, ctx: CephadmContext, mounts: Dict[str, str]
    ) -> None:
        """Update mounts adding entries that are specific to podman."""
        # Modifications podman makes to /etc/hosts causes issues with certain
        # daemons (specifically referencing "host.containers.internal" entry
        # being added to /etc/hosts in this case). To avoid that, but still
        # allow users to use /etc/hosts for hostname resolution, we can mount
        # the host's /etc/hosts file.
        # https://tracker.ceph.com/issues/58532
        # https://tracker.ceph.com/issues/57018
        if os.path.exists('/etc/hosts'):
            if '/etc/hosts' not in mounts:
                mounts['/etc/hosts'] = '/etc/hosts:ro'


class Docker(ContainerEngine):
    EXE = 'docker'


CONTAINER_PREFERENCE = (Podman, Docker)  # prefer podman to docker


def find_container_engine(ctx: CephadmContext) -> Optional[ContainerEngine]:
    if ctx.docker:
        return Docker()
    else:
        for i in CONTAINER_PREFERENCE:
            try:
                return i()
            except Exception:
                pass
    return None


def check_container_engine(ctx: CephadmContext) -> ContainerEngine:
    engine = ctx.container_engine
    if not isinstance(engine, CONTAINER_PREFERENCE):
        # See https://github.com/python/mypy/issues/8993
        exes: List[str] = [i.EXE for i in CONTAINER_PREFERENCE]  # type: ignore
        raise Error(
            'No container engine binary found ({}). Try run `apt/dnf/yum/zypper install <container engine>`'.format(
                ' or '.join(exes)
            )
        )
    elif isinstance(engine, Podman):
        engine.get_version(ctx)
        if engine.version < MIN_PODMAN_VERSION:
            raise Error(
                'podman version %d.%d.%d or later is required'
                % MIN_PODMAN_VERSION
            )
    return engine


def _parse_podman_version(version_str):
    # type: (str) -> Tuple[int, ...]
    def to_int(val: str, org_e: Optional[Exception] = None) -> int:
        if not val and org_e:
            raise org_e
        try:
            return int(val)
        except ValueError as e:
            return to_int(val[0:-1], org_e or e)

    return tuple(map(to_int, version_str.split('.')))


def registry_login(
    ctx: CephadmContext,
    url: Optional[str],
    username: Optional[str],
    password: Optional[str],
) -> None:
    try:
        engine = ctx.container_engine
        cmd = [engine.path, 'login', '-u', username, '-p', password, url]
        if isinstance(engine, Podman):
            cmd.append('--authfile=/etc/ceph/podman-auth.json')
        out, _, _ = call_throws(ctx, cmd)
        if isinstance(engine, Podman):
            os.chmod('/etc/ceph/podman-auth.json', DEFAULT_MODE)
    except Exception:
        raise Error(
            'Failed to login to custom registry @ %s as %s with given password'
            % (ctx.registry_url, ctx.registry_username)
        )


def pull_command(
    ctx: CephadmContext, image: str, insecure: bool = False
) -> List[str]:
    """Return a command that can be run to pull an image."""
    cmd = [ctx.container_engine.path, 'pull', image]
    if isinstance(ctx.container_engine, Podman):
        if insecure:
            cmd.append('--tls-verify=false')

        if os.path.exists('/etc/ceph/podman-auth.json'):
            cmd.append('--authfile=/etc/ceph/podman-auth.json')
    return cmd


def _container_mem_usage(
    ctx: CephadmContext,
    *,
    container_path: str = '',
    verbosity: CallVerbosity = CallVerbosity.QUIET,
) -> Tuple[str, str, int]:
    container_path = container_path or ctx.container_engine.path
    out, err, code = call(
        ctx,
        [
            container_path,
            'stats',
            '--format',
            '{{.ID}},{{.MemUsage}}',
            '--no-stream',
        ],
        verbosity=verbosity,
    )
    return out, err, code


def _parse_mem_usage(code: int, out: str) -> Tuple[int, Dict[str, int]]:
    # keep track of memory usage we've seen
    seen_memusage = {}  # type: Dict[str, int]
    seen_memusage_cid_len = 0
    if not code:
        for line in out.splitlines():
            (cid, usage) = line.split(',')
            (used, limit) = usage.split(' / ')
            try:
                seen_memusage[cid] = with_units_to_int(used)
                if not seen_memusage_cid_len:
                    seen_memusage_cid_len = len(cid)
            except ValueError:
                logger.info(
                    'unable to parse memory usage line\n>{}'.format(line)
                )
                pass
    return seen_memusage_cid_len, seen_memusage


def parsed_container_mem_usage(
    ctx: CephadmContext,
    *,
    container_path: str = '',
    verbosity: CallVerbosity = CallVerbosity.QUIET,
) -> Tuple[int, Dict[str, int]]:
    """Return memory useage values parsed from the container engine's container status."""
    out, _, code = _container_mem_usage(
        ctx, container_path=container_path, verbosity=verbosity
    )
    return _parse_mem_usage(code, out)


def _container_cpu_perc(
    ctx: CephadmContext,
    *,
    container_path: str = '',
    verbosity: CallVerbosity = CallVerbosity.QUIET,
) -> Tuple[str, str, int]:
    container_path = container_path or ctx.container_engine.path
    out, err, code = call(
        ctx,
        [
            container_path,
            'stats',
            '--format',
            '{{.ID}},{{.CPUPerc}}',
            '--no-stream',
        ],
        verbosity=CallVerbosity.QUIET,
    )
    return out, err, code


def _parse_cpu_perc(code: int, out: str) -> Tuple[int, Dict[str, str]]:
    seen_cpuperc = {}
    seen_cpuperc_cid_len = 0
    if not code:
        for line in out.splitlines():
            (cid, cpuperc) = line.split(',')
            try:
                seen_cpuperc[cid] = cpuperc
                if not seen_cpuperc_cid_len:
                    seen_cpuperc_cid_len = len(cid)
            except ValueError:
                logger.info(
                    'unable to parse cpu percentage line\n>{}'.format(line)
                )
                pass
    return seen_cpuperc_cid_len, seen_cpuperc


def parsed_container_cpu_perc(
    ctx: CephadmContext,
    *,
    container_path: str = '',
    verbosity: CallVerbosity = CallVerbosity.QUIET,
) -> Tuple[int, Dict[str, str]]:
    """Return cpu percentage used values parsed from the container engine's
    container status.
    """
    out, _, code = _container_cpu_perc(
        ctx, container_path=container_path, verbosity=verbosity
    )
    return _parse_cpu_perc(code, out)


class ContainerInfo:
    def __init__(
        self,
        container_id: str,
        image_name: str,
        image_id: str,
        start: str,
        version: str,
    ) -> None:
        self.container_id = container_id
        self.image_name = image_name
        self.image_id = image_id
        self.start = start
        self.version = version

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, ContainerInfo):
            return NotImplemented
        return (
            self.container_id == other.container_id
            and self.image_name == other.image_name
            and self.image_id == other.image_id
            and self.start == other.start
            and self.version == other.version
        )


def _container_stats(
    ctx: CephadmContext,
    container_name: str,
    *,
    container_path: str,
) -> Tuple[str, str, int]:
    """returns container id, image name, image id, created time, and ceph version if available"""
    container_path = container_path or ctx.container_engine.path
    out, err, code = '', '', -1
    cmd = [
        container_path,
        'inspect',
        '--format',
        '{{.Id}},{{.Config.Image}},{{.Image}},{{.Created}},{{index .Config.Labels "io.ceph.version"}}',
        container_name,
    ]
    out, err, code = call(ctx, cmd, verbosity=CallVerbosity.QUIET)
    return out, err, code


def _parse_container_stats(
    out: str, err: str, code: int
) -> Optional[ContainerInfo]:
    if code != 0:
        return None
    # container_id, image_name, image_id, start, version
    return ContainerInfo(*list(out.strip().split(',')))


def parsed_container_stats(
    ctx: CephadmContext,
    container_name: str,
    *,
    container_path: str,
) -> Optional[ContainerInfo]:
    out, err, code = _container_stats(
        ctx, container_name, container_path=container_path
    )
    return _parse_container_stats(out, err, code)


def _container_image_stats(
    ctx: CephadmContext, image_name: str, *, container_path: str = ''
) -> Tuple[str, str, int]:
    """returns image id, created time, and ceph version if available"""
    container_path = container_path or ctx.container_engine.path
    cmd = [
        container_path,
        'image',
        'inspect',
        '--format',
        '{{.Id}},{{.Created}},{{index .Config.Labels "io.ceph.version"}}',
        image_name,
    ]
    out, err, code = call(ctx, cmd, verbosity=CallVerbosity.QUIET)
    return out, err, code


def _parse_container_image_stats(
    image_name: str,
    out: str,
    err: str,
    code: int,
) -> Optional[ContainerInfo]:
    if code != 0:
        return None
    (image_id, start, version) = out.strip().split(',')
    # keep in mind, the daemon container is not running, so no container id here
    return ContainerInfo(
        container_id='',
        image_name=image_name,
        image_id=image_id,
        start=start,
        version=version,
    )


def parsed_container_image_stats(
    ctx: CephadmContext, image_name: str, *, container_path: str = ''
) -> Optional[ContainerInfo]:
    out, err, code = _container_image_stats(
        ctx, image_name, container_path=container_path
    )
    return _parse_container_image_stats(image_name, out, err, code)
