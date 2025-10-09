# container_engine_base.py - container engine base class

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
