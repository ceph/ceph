import logging
from errno import EINVAL
from typing import List, NamedTuple

from ..exceptions import DashboardException
from ..security import Scope
from ..services.ceph_service import CephService, SendCommandError
from . import APIDoc, APIRouter, CRUDCollectionMethod, CRUDEndpoint, EndpointDoc, SecretStr
from ._crud import ArrayHorizontalContainer, CRUDMeta, Form, FormField, Icon, \
    TableAction, VerticalContainer

logger = logging.getLogger("controllers.ceph_users")


class CephUserCaps(NamedTuple):
    mon: str
    osd: str
    mgr: str
    mds: str


class Cap(NamedTuple):
    entity: str
    cap: str


class CephUserEndpoints:
    @staticmethod
    def user_list(_):
        """
        Get list of ceph users and its respective data
        """
        return CephService.send_command('mon', 'auth ls')["auth_dump"]

    @staticmethod
    def user_create(_, user_entity: str, capabilities: List[Cap]):
        """
        Add a ceph user with its defined capabilities.
        :param user_entity: Entity to change
        :param capabilities: List of capabilities to add to user_entity
        """
        # Caps are represented as a vector in mon auth add commands.
        # Look at AuthMonitor.cc::valid_caps for reference.
        caps = []
        for cap in capabilities:
            caps.append(cap['entity'])
            caps.append(cap['cap'])

        logger.debug("Sending command 'auth add' of entity '%s' with caps '%s'",
                     user_entity, str(caps))
        try:
            CephService.send_command('mon', 'auth add', entity=user_entity, caps=caps)
        except SendCommandError as ex:
            msg = f'{ex} in command {ex.prefix}'
            if ex.errno == -EINVAL:
                raise DashboardException(msg, code=400)
            raise DashboardException(msg, code=500)
        return f"Successfully created user '{user_entity}'"


create_cap_container = ArrayHorizontalContainer('Capabilities', 'capabilities', fields=[
    FormField('Entity', 'entity',
              field_type=str),
    FormField('Entity Capabilities',
              'cap', field_type=str)
], min_items=1)
create_container = VerticalContainer('Create User', 'create_user', fields=[
    FormField('User entity', 'user_entity',
              field_type=str),
    create_cap_container,
])

create_form = Form(path='/cluster/user/create',
                   root_container=create_container)


@CRUDEndpoint(
    router=APIRouter('/cluster/user', Scope.CONFIG_OPT),
    doc=APIDoc("Get Ceph Users", "Cluster"),
    set_column={"caps": {"cellTemplate": "badgeDict"}},
    actions=[
        TableAction(name='create', permission='create', icon=Icon.add.value,
                    routerLink='/cluster/user/create')
    ],
    permissions=[Scope.CONFIG_OPT],
    forms=[create_form],
    get_all=CRUDCollectionMethod(
        func=CephUserEndpoints.user_list,
        doc=EndpointDoc("Get Ceph Users")
    ),
    create=CRUDCollectionMethod(
        func=CephUserEndpoints.user_create,
        doc=EndpointDoc("Create Ceph User")
    ),
    meta=CRUDMeta()
)
class CephUser(NamedTuple):
    entity: str
    caps: List[CephUserCaps]
    key: SecretStr
