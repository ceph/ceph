import logging
from errno import EINVAL
from typing import List, NamedTuple, Optional

from ..exceptions import DashboardException
from ..security import Scope
from ..services.ceph_service import CephService, SendCommandError
from . import APIDoc, APIRouter, CRUDCollectionMethod, CRUDEndpoint, \
    EndpointDoc, RESTController, SecretStr
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
        Get list of ceph users and its respective data
        """
        return CephUserEndpoints._run_auth_command('auth ls')["auth_dump"]

    @staticmethod
    def user_create(_, user_entity: str = '', capabilities: Optional[List[Cap]] = None,
                    import_data: str = ''):
        """
        Add a ceph user with its defined capabilities.
        :param user_entity: Entity to change
        :param capabilities: List of capabilities to add to user_entity
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
    def user_delete(_, user_entity: str):
        """
        Delete a ceph user and it's defined capabilities.
        :param user_entity: Entity to delete
        """
        logger.debug("Sending command 'auth del' of entity '%s'", user_entity)
        CephUserEndpoints._run_auth_command('auth del', entity=user_entity)
        return f"Successfully deleted user '{user_entity}'"

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
        :param user_entity: Entity to change
        :param capabilities: List of updated capabilities to user_entity
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
        doc=EndpointDoc("Get Ceph Users")
    ),
    create=CRUDCollectionMethod(
        func=CephUserEndpoints.user_create,
        doc=EndpointDoc("Create Ceph User")
    ),
    edit=CRUDCollectionMethod(
        func=CephUserEndpoints.user_edit,
        doc=EndpointDoc("Edit Ceph User")
    ),
    delete=CRUDCollectionMethod(
        func=CephUserEndpoints.user_delete,
        doc=EndpointDoc("Delete Ceph User")
    ),
    extra_endpoints=[
        ('export', CRUDCollectionMethod(
            func=RESTController.Collection('POST', 'export')(CephUserEndpoints.export),
            doc=EndpointDoc("Export Ceph Users")
        ))
    ],
    selection_type=SelectionType.MULTI,
    meta=CRUDMeta()
)
class CephUser(NamedTuple):
    entity: str
    caps: List[CephUserCaps]
    key: SecretStr
