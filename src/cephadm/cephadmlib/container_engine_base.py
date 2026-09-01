# container_engine_base.py - container engine base class

from typing import List, Dict

from .context import CephadmContext
from .exe_utils import find_program


class ContainerEngine:
    def __init__(self) -> None:
        self.path = find_program(self.EXE)

    @property
    def EXE(self) -> str:
        raise NotImplementedError()

    @property
    def unlimited_pids_option(self) -> str:
        """The option to pass to the container engine for allowing unlimited
        pids (processes).
        """
        return '--pids-limit=0'

    def __str__(self) -> str:
        return f'{self.EXE} ({self.path})'

    def service_args(
        self, ctx: CephadmContext, service_name: str
    ) -> List[str]:
        return []

    def update_mounts(
        self, ctx: CephadmContext, mounts: Dict[str, str]
    ) -> None:
        pass
