# deamon_identity.py - classes for identifying daemons & services

from typing import Union, Optional

from .context import CephadmContext


class DaemonIdentity:
    def __init__(
        self,
        fsid: str,
        daemon_type: str,
        daemon_id: Union[int, str],
        subcomponent: str = '',
    ) -> None:
        self._fsid = fsid
        self._daemon_type = daemon_type
        self._daemon_id = str(daemon_id)
        self._subcomponent = subcomponent

    @property
    def fsid(self) -> str:
        return self._fsid

    @property
    def daemon_type(self) -> str:
        return self._daemon_type

    @property
    def daemon_id(self) -> str:
        return self._daemon_id

    @property
    def subcomponent(self) -> str:
        return self._subcomponent

    @property
    def legacy_container_name(self) -> str:
        return 'ceph-%s-%s.%s' % (self.fsid, self.daemon_type, self.daemon_id)

    @property
    def container_name(self) -> str:
        name = f'ceph-{self.fsid}-{self.daemon_type}-{self.daemon_id}'
        if self.subcomponent:
            name = f'{name}-{self.subcomponent}'
        return name.replace('.', '-')

    def _replace(
        self,
        *,
        fsid: Optional[str] = None,
        daemon_type: Optional[str] = None,
        daemon_id: Union[None, int, str] = None,
        subcomponent: Optional[str] = None,
    ) -> 'DaemonIdentity':
        return self.__class__(
            fsid=self.fsid if fsid is None else fsid,
            daemon_type=(
                self.daemon_type if daemon_type is None else daemon_type
            ),
            daemon_id=self.daemon_id if daemon_id is None else daemon_id,
            subcomponent=(
                self.subcomponent if subcomponent is None else subcomponent
            ),
        )

    @classmethod
    def from_name(cls, fsid: str, name: str) -> 'DaemonIdentity':
        daemon_type, daemon_id = name.split('.', 1)
        return cls(fsid, daemon_type, daemon_id)

    @classmethod
    def from_context(cls, ctx: 'CephadmContext') -> 'DaemonIdentity':
        return cls.from_name(ctx.fsid, ctx.name)
