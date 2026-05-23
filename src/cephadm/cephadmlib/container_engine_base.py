# container_engine_base.py - container engine base class

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

    def is_live_restore_enabled(self, ctx: CephadmContext) -> bool:
        """Return True if the container engine supports and has enabled live-restore.
        Live-restore is a feature that keeps containers running when the container
        engine daemon is stopped or restarted, preventing service interruption.
        Returns False by default; container engines that support this feature
        should override this method."""
        return False

    def __str__(self) -> str:
        return f'{self.EXE} ({self.path})'
