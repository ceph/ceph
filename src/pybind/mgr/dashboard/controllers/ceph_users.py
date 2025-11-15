import logging
from errno import EINVAL
from typing import List, NamedTuple, Optional

from ..exceptions import DashboardException
from ..security import Scope
from ..services.ceph_service import CephService, SendCommandError
from . import APIDoc, APIRouter, CRUDCollectionMethod, CRUDEndpoint, \
    EndpointDoc, Param, RESTController, SecretStr
from ._crud import ArrayHorizontalContainer, CRUDMeta, Form, FormField, \
    FormTaskInfo, Icon, MethodType, SelectionType, TableAction, Validator, \
    VerticalContainer

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
    def _run_auth_command(command: str, *args, **kwargs):
        try:
            return CephService.send_command('mon', command, *args, **kwargs)
        except SendCommandError as ex:
            msg = f'{ex} in command {ex.prefix}'
            if ex.errno == -EINVAL:
                raise DashboardException(msg, code=400)
            raise DashboardException(msg, code=500)

    @staticmethod
    def user_list(_):
        """
        Get list of ceph users and its associated data
        """
        return CephUserEndpoints._run_auth_command('auth ls')["auth_dump"]

    @staticmethod
    def user_create(_, user_entity: str = '', capabilities: Optional[List[Cap]] = None,
                    import_data: str = ''):
        """
        Add a Ceph user, with its defined capabilities.
        """
        # Caps are represented as a vector in mon auth add commands.
        # Look at AuthMonitor.cc::valid_caps for reference.
        if import_data:
            logger.debug("Sending import command 'auth import' \n%s", import_data)
            CephUserEndpoints._run_auth_command('auth import', inbuf=import_data)
            return "Successfully imported user"

        assert user_entity
        caps = []
        for cap in capabilities:
            caps.append(cap['entity'])
            caps.append(cap['cap'])

        logger.debug("Sending command 'auth add' of entity '%s' with caps '%s'",
                     user_entity, str(caps))
        CephUserEndpoints._run_auth_command('auth add', entity=user_entity, caps=caps)

        return f"Successfully created user '{user_entity}'"

    @staticmethod
    def get_or_create(_, user_entity: str = '', capabilities: Optional[List[Cap]] = None):
        """
        Get or create a ceph user, with its defined capabilities.
        """
        assert user_entity
        caps = []
        for cap in capabilities:
            caps.append(cap['entity'])
            caps.append(cap['cap'])
        logger.debug("Sending command 'auth get-or-create' of entity '%s' with caps '%s'",
                     user_entity, str(caps))
        return CephUserEndpoints._run_auth_command('auth get-or-create', entity=user_entity, caps=caps)

    @staticmethod
    def user_delete(_, user_entity: str):
        """
        Delete one or more ceph users and their defined capabilities.
        user_entities: comma-separated string of users to delete
        """
        users = user_entities.split(',')
        for user in users:
            logger.debug("Sending command 'auth del' of entity '%s'", user)
            CephUserEndpoints._run_auth_command('auth del', entity=user)
        return f"Successfully deleted user(s) '{user_entities}'"

    @staticmethod
    def export(_, entities: List[str]):
        export_string = ""
        for entity in entities:
            out = CephUserEndpoints._run_auth_command('auth export', entity=entity, to_json=False)
            export_string += f'{out}\n'
        return export_string

    @staticmethod
    def user_edit(_, user_entity: str = '', capabilities: List[Cap] = None):
        """
        Change the ceph user capabilities.
        Setting new capabilities will overwrite current ones.
        """
        caps = []
        for cap in capabilities:
            caps.append(cap['entity'])
            caps.append(cap['cap'])

        logger.debug("Sending command 'auth caps' of entity '%s' with caps '%s'",
                     user_entity, str(caps))
        CephUserEndpoints._run_auth_command('auth caps', entity=user_entity, caps=caps)
        return f"Successfully edited user '{user_entity}'"

    @staticmethod
    def model(user_entity: str):
        user_data = CephUserEndpoints._run_auth_command('auth get', entity=user_entity)[0]
        model = {'user_entity': '', 'capabilities': []}
        model['user_entity'] = user_data['entity']
        for entity, cap in user_data['caps'].items():
            model['capabilities'].append({'entity': entity, 'cap': cap})
        return model


cap_container = ArrayHorizontalContainer('Capabilities', 'capabilities', fields=[
    FormField('Entity', 'entity',
              field_type=str),
    FormField('Entity Capabilities',
              'cap', field_type=str)
], min_items=1)
create_container = VerticalContainer('Create User', 'create_user', fields=[
    FormField('User entity', 'user_entity',
              field_type=str),
    cap_container,
])

edit_container = VerticalContainer('Edit User', 'edit_user', fields=[
    FormField('User entity', 'user_entity',
              field_type=str, readonly=True),
    cap_container,
])

create_form = Form(path='/cluster/user/create',
                   root_container=create_container,
                   method_type=MethodType.POST.value,
                   task_info=FormTaskInfo("Ceph user '{user_entity}' successfully",
                                          ['user_entity']))

# pylint: disable=C0301
import_user_help = (
    'The imported file should be a keyring file and it  must follow the schema described <a '  # noqa: E501
    'href="https://docs.ceph.com/en/latest/rados/operations/user-management/#authorization-capabilities"'  # noqa: E501
    'target="_blank">here.</a>'
)
import_container = VerticalContainer('Import User', 'import_user', fields=[
    FormField('User file import', 'import_data',
              field_type="file", validators=[Validator.FILE],
              help=import_user_help),
])

import_user_form = Form(path='/cluster/user/import',
                        root_container=import_container,
                        task_info=FormTaskInfo("successfully", []),
                        method_type=MethodType.POST.value)

edit_form = Form(path='/cluster/user/edit',
                 root_container=edit_container,
                 method_type=MethodType.PUT.value,
                 task_info=FormTaskInfo("Ceph user '{user_entity}' successfully",
                                        ['user_entity']),
                 model_callback=CephUserEndpoints.model)


@CRUDEndpoint(
    router=APIRouter('/cluster/user', Scope.CONFIG_OPT),
    doc=APIDoc("Get Ceph Users", "Cluster"),
    set_column={"caps": {"cellTemplate": "badgeDict"}},
    actions=[
        TableAction(name='Create', permission='create', icon=Icon.ADD.value,
                    routerLink='/cluster/user/create'),
        TableAction(name='Get or Create', permission='create', icon=Icon.ADD.value,
                    routerLink='/cluster/user/get-or-create'),
        TableAction(name='Edit', permission='update', icon=Icon.EDIT.value,
                    click='edit', routerLink='/cluster/user/edit'),
        TableAction(name='Delete', permission='delete', icon=Icon.DESTROY.value,
                    click='delete', disable=True),
        TableAction(name='Import', permission='create', icon=Icon.IMPORT.value,
                    routerLink='/cluster/user/import'),
        TableAction(name='Export', permission='read', icon=Icon.EXPORT.value,
                    click='authExport', disable=True)
    ],
    permissions=[Scope.CONFIG_OPT],
    forms=[create_form, edit_form, import_user_form],
    column_key='entity',
    resource='user',
    get_all=CRUDCollectionMethod(
        func=CephUserEndpoints.user_list,
        doc=EndpointDoc("Get list of ceph users")
    ),
    create=CRUDCollectionMethod(
        func=CephUserEndpoints.user_create,
        doc=EndpointDoc("Create Ceph User",
                        parameters={
                            "user_entity": Param(str, "Entity to add"),
                            'capabilities': Param([{
                                "entity": (str, "Entity to add"),
                                "cap": (str, "Capability to add; eg. allow *")
                            }], 'List of capabilities to add to user_entity')
                        })
    ),
    get_or_create=CRUDCollectionMethod(
        func=CephUserEndpoints.get_or_create,
        doc=EndpointDoc("Get or Create Ceph User",
                        parameters={
                            "user_entity": Param(str, "Entity to get or create"),
                            'capabilities': Param([{
                                "entity": (str, "Entity to get or create"),
                                "cap": (str, "Capability to get or create; eg. allow *")
                            }], 'List of capabilities to get or create to user_entity')
                        })
    ),
    edit=CRUDCollectionMethod(
        func=CephUserEndpoints.user_edit,
        doc=EndpointDoc("Edit Ceph User Capabilities",
                        parameters={
                            "user_entity": Param(str, "Entity to edit"),
                            'capabilities': Param([{
                                "entity": (str, "Entity to edit"),
                                "cap": (str, "Capability to edit; eg. allow *")
                            }], 'List of updated capabilities to user_entity')
                        })
    ),
    delete=CRUDCollectionMethod(
        func=CephUserEndpoints.user_delete,
        doc=EndpointDoc("Delete one or more Ceph Users",
                        parameters={
                            "user_entity": Param(str, "Entity to delete")
                        })
    ),
    extra_endpoints=[
        ('export', CRUDCollectionMethod(
            func=RESTController.Collection('POST', 'export')(CephUserEndpoints.export),
            doc=EndpointDoc("Export Ceph Users",
                            parameters={
                                "entities": Param([str], "List of entities to export")
                            })
        ))
    ],
    selection_type=SelectionType.MULTI,
    meta=CRUDMeta()
)
class CephUser(NamedTuple):
    entity: str
    caps: List[CephUserCaps]
    key: SecretStr
