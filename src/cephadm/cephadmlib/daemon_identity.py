# deamon_identity.py - classes for identifying daemons & services

import enum
import os
import pathlib
import re

from typing import Union, Optional, Tuple

from .context import CephadmContext


class Categories(str, enum.Enum):
    SIDECAR = 'sidecar'
    INIT = 'init'

    def __str__(self) -> str:
        return self.value


class DaemonIdentity:
    def __init__(
        self,
        fsid: str,
        daemon_type: str,
        daemon_id: Union[int, str],
    ) -> None:
        self._fsid = fsid
        self._daemon_type = daemon_type
        self._daemon_id = str(daemon_id)
        assert self._fsid
        assert self._daemon_type
        assert self._daemon_id

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
    def daemon_name(self) -> str:
        return f'{self.daemon_type}.{self.daemon_id}'

    @property
    def legacy_container_name(self) -> str:
        return 'ceph-%s-%s.%s' % (self.fsid, self.daemon_type, self.daemon_id)

    @property
    def container_name(self) -> str:
        name = f'ceph-{self.fsid}-{self.daemon_type}-{self.daemon_id}'
        return name.replace('.', '-')

    def _systemd_name(
        self,
        *,
        framework: str = 'ceph',
        category: str = '',
        suffix: str = '',
        extension: str = '',
    ) -> str:
        if category:
            # validate the category value
            category = Categories(category)
        template_terms = [framework, self.fsid, category]
        instance_terms = [self.daemon_type]
        instance_terms.append(
            f'{self.daemon_id}:{suffix}' if suffix else self.daemon_id
        )
        instance_terms.append(extension)
        # use a comprehension to filter out terms that are blank
        base = '-'.join(v for v in template_terms if v)
        svc = '.'.join(v for v in instance_terms if v)
        return f'{base}@{svc}'

    @property
    def unit_name(self) -> str:
        return self._systemd_name()

    @property
    def service_name(self) -> str:
        return self._systemd_name(extension='service')

    @property
    def init_service_name(self) -> str:
        # all init contaienrs are run as a single systemd service
        return self._systemd_name(category='init', extension='service')

    def data_dir(self, base_data_dir: Union[str, os.PathLike]) -> str:
        # do not use self.daemon_name as that may be overridden in subclasses
        dn = f'{self.daemon_type}.{self.daemon_id}'
        return str(pathlib.Path(base_data_dir) / self.fsid / dn)

    @classmethod
    def from_name(cls, fsid: str, name: str) -> 'DaemonIdentity':
        daemon_type, daemon_id = name.split('.', 1)
        return cls(fsid, daemon_type, daemon_id)

    @classmethod
    def from_context(cls, ctx: 'CephadmContext') -> 'DaemonIdentity':
        return cls.from_name(ctx.fsid, ctx.name)


class DaemonSubIdentity(DaemonIdentity):
    def __init__(
        self,
        fsid: str,
        daemon_type: str,
        daemon_id: Union[int, str],
        subcomponent: str = '',
    ) -> None:
        super().__init__(fsid, daemon_type, daemon_id)
        self._subcomponent = subcomponent
        if not re.match('^[a-zA-Z0-9]{1,32}$', self._subcomponent):
            raise ValueError(
                f'invalid subcomponent; invalid characters: {subcomponent!r}'
            )

    @property
    def subcomponent(self) -> str:
        return self._subcomponent

    @property
    def daemon_name(self) -> str:
        return f'{self.daemon_type}.{self.daemon_id}.{self.subcomponent}'

    @property
    def container_name(self) -> str:
        name = f'ceph-{self.fsid}-{self.daemon_type}-{self.daemon_id}-{self.subcomponent}'
        return name.replace('.', '-')

    @property
    def unit_name(self) -> str:
        # NB: This is a minor hack because a subcomponent may be running as part
        # of the same unit as the primary. However, to fix a bug with iscsi
        # this is a quick and dirty workaround for distinguishing the two types
        # when generating --cidfile and --conmon-pidfile values.
        return self._systemd_name(suffix=self.subcomponent)

    @property
    def service_name(self) -> str:
        # use the parent's service_name to get the service. sub-identities
        # must use other specific methods (like sidecar_service_name) for
        # sub-identity based services
        raise ValueError('called service_name on DaemonSubIdentity')

    @property
    def sidecar_service_name(self) -> str:
        return self._systemd_name(
            category='sidecar', suffix=self.subcomponent, extension='service'
        )

    def sidecar_script(self, base_data_dir: Union[str, os.PathLike]) -> str:
        sname = f'sidecar-{ self.subcomponent }.run'
        return str(pathlib.Path(self.data_dir(base_data_dir)) / sname)

    @property
    def legacy_container_name(self) -> str:
        raise ValueError(
            'legacy_container_name not valid for DaemonSubIdentity'
        )

    @classmethod
    def from_parent(
        cls, parent: 'DaemonIdentity', subcomponent: str
    ) -> 'DaemonSubIdentity':
        return cls(
            parent.fsid,
            parent.daemon_type,
            parent.daemon_id,
            subcomponent,
        )

    @classmethod
    def from_service_name(
        cls, service_name: str
    ) -> Tuple['DaemonSubIdentity', str]:
        """Return a DaemonSubIdentity and category value by parsing the
        contents of a systemd service name for a sidecar container.
        """
        # ceph services always have the template@instance form
        tpart, ipart = service_name.split('@', 1)
        # drop the .service if it exists
        if ipart.endswith('.service'):
            ipart = ipart[:-8]
        # verify the service name starts with 'ceph' -- our framework
        framework, tpart = tpart.split('-', 1)
        if framework != 'ceph':
            raise ValueError(f'Invalid framework value: {service_name}')
        # we're parsing only services for subcomponents. it must take the
        # form <FSID>-<CATEGORY>. Where categories are sidecar or init.
        fsid, category = tpart.rsplit('-', 1)
        try:
            Categories(category)
        except ValueError:
            raise ValueError(f'Invalid service category: {service_name}')
        # if it is a sidecar it will have a subcomponent name following a colon
        svcparts = ipart.split(':')
        if len(svcparts) == 1:
            subc = ''
        elif len(svcparts) == 2:
            subc = svcparts[1]
        else:
            raise ValueError(f'Unexpected instance value: {ipart}')
        # only services based on sidecars currently have named subcomponents
        # init subcomponents are all "hidden" within a single init service
        if subc and not category == Categories.SIDECAR:
            raise ValueError(
                f'Unexpected subcomponent {subc!r} for category {category}'
            )
        elif not subc:
            # because we return a DaemonSubIdentity we need some value for
            # the subcomponent on init services. Just repeat the category
            subc = str(category)
        daemon_type, daemon_id = svcparts[0].split('.', 1)
        return cls(fsid, daemon_type, daemon_id, subc), category

    @classmethod
    def must(cls, value: Optional[DaemonIdentity]) -> 'DaemonSubIdentity':
        """Helper to assert value is of the correct type.  Mostly to make mypy
        happy.
        """
        if not isinstance(value, cls):
            raise TypeError(f'{value!r} is not a {cls}')
        return value
