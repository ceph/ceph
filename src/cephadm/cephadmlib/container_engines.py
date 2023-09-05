# container_engines.py - container engine types and selection funcs


from typing import Tuple, List, Optional

from .call_wrappers import call_throws, CallVerbosity
from .context import CephadmContext
from .container_engine_base import ContainerEngine
from .constants import MIN_PODMAN_VERSION
from .exceptions import Error


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
