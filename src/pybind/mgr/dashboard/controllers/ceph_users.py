from typing import NamedTuple

from ..security import Scope
from ..services.ceph_service import CephService
from . import APIDoc, APIRouter, CRUDCollectionMethod, CRUDEndpoint, EndpointDoc, SecretStr


class CephUserCaps(NamedTuple):
    mon: str
    osd: str
    mgr: str
    mds: str


@CRUDEndpoint(
    router=APIRouter('/cluster/user', Scope.CONFIG_OPT),
    doc=APIDoc("Get Ceph Users", "Cluster"),
    set_column={"caps": {"cellTemplate": "badgeDict"}},
    get_all=CRUDCollectionMethod(
        func=lambda **_: CephService.send_command('mon', 'auth ls')["auth_dump"],
        doc=EndpointDoc("Get Ceph Users")
    )
)
class CephUser(NamedTuple):
    entity: str
    caps: CephUserCaps
    key: SecretStr
