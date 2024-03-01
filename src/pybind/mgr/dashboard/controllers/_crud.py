from enum import Enum
from functools import wraps
from inspect import isclass
from typing import Any, Callable, Dict, Generator, Iterable, Iterator, List, \
    NamedTuple, Optional, Tuple, Union, get_type_hints

from ._api_router import APIRouter
from ._docs import APIDoc, EndpointDoc
from ._rest_controller import RESTController
from ._ui_router import UIRouter


class SecretStr(str):
    pass


class MethodType(Enum):
    POST = 'post'
    PUT = 'put'


def isnamedtuple(o):
    return isinstance(o, tuple) and hasattr(o, '_asdict') and hasattr(o, '_fields')


class SerializableClass:
    def __iter__(self):
        for attr in self.__dict__:
            if not attr.startswith("__"):
                yield attr, getattr(self, attr)

    def __contains__(self, value):
        return value in self.__dict__

    def __len__(self):
        return len(self.__dict__)


def serialize(o, expected_type=None):
    # pylint: disable=R1705,W1116
    if isnamedtuple(o):
        hints = get_type_hints(o)
        return {k: serialize(v, hints[k]) for k, v in zip(o._fields, o)}
    elif isinstance(o, (list, tuple, set)):
        # json serializes list and tuples to arrays, hence we also serialize
        # sets to lists.
        # NOTE: we could add a metadata value in a list to indentify tuples and,
        # sets if we wanted but for now let's go for lists.
        return [serialize(i) for i in o]
    elif isinstance(o, SerializableClass):
        return {serialize(k): serialize(v) for k, v in o}
    elif isinstance(o, (Iterator, Generator)):
        return [serialize(i) for i in o]
    elif expected_type and isclass(expected_type) and issubclass(expected_type, SecretStr):
        return "***********"
    else:
        return o


class TableColumn(NamedTuple):
    prop: str
    cellTemplate: str = ''
    isHidden: bool = False
    filterable: bool = True
    flexGrow: int = 1


class TableAction(NamedTuple):
    name: str
    permission: str
    icon: str
    routerLink: str = ''  # redirect to...
    click: str = ''
    disable: bool = False  # disable without selection


class SelectionType(Enum):
    NONE = ''
    SINGLE = 'single'
    MULTI = 'multiClick'


class TableComponent(SerializableClass):
    def __init__(self) -> None:
        self.columns: List[TableColumn] = []
        self.columnMode: str = 'flex'
        self.toolHeader: bool = True
        self.selectionType: str = SelectionType.SINGLE.value

    def set_selection_type(self, type_: SelectionType):
        self.selectionType = type_.value


class Icon(Enum):
    ADD = 'fa fa-plus'
    DESTROY = 'fa fa-times'
    IMPORT = 'fa fa-upload'
    EXPORT = 'fa fa-download'
    EDIT = 'fa fa-pencil'


class Validator(Enum):
    JSON = 'json'
    RGW_ROLE_NAME = 'rgwRoleName'
    RGW_ROLE_PATH = 'rgwRolePath'
    FILE = 'file'
    RGW_ROLE_SESSION_DURATION = 'rgwRoleSessionDuration'


class FormField(NamedTuple):
    """
    The key of a FormField is then used to send the data related to that key into the
    POST and PUT endpoints. It is imperative for the developer to map keys of fields and containers
    to the input of the POST and PUT endpoints.
    """
    name: str
    key: str
    field_type: Any = str
    default_value: Optional[Any] = None
    optional: bool = False
    readonly: bool = False
    help: str = ''
    validators: List[Validator] = []

    def get_type(self):
        _type = ''
        if self.field_type == str:
            _type = 'string'
        elif self.field_type == int:
            _type = 'int'
        elif self.field_type == bool:
            _type = 'boolean'
        elif self.field_type == 'textarea':
            _type = 'textarea'
        elif self.field_type == "file":
            _type = 'file'
        else:
            raise NotImplementedError(f'Unimplemented type {self.field_type}')
        return _type


class Container:
    def __init__(self, name: str, key: str, fields: List[Union[FormField, "Container"]],
                 optional: bool = False, readonly: bool = False, min_items=1):
        self.name = name
        self.key = key
        self.fields = fields
        self.optional = optional
        self.readonly = readonly
        self.min_items = min_items

    def layout_type(self):
        raise NotImplementedError

    def _property_type(self):
        raise NotImplementedError

    def to_dict(self, key=''):
        # initialize the schema of this container
        ui_schemas = []
        control_schema = {
            'type': self._property_type(),
            'title': self.name
        }
        items = None  # layout items alias as it depends on the type of container
        properties = None  # control schema properties alias
        required = None
        if self._property_type() == 'array':
            control_schema['required'] = []
            control_schema['minItems'] = self.min_items
            control_schema['items'] = {
                'type': 'object',
                'properties': {},
                'required': []
            }
            properties = control_schema['items']['properties']
            required = control_schema['required']
            control_schema['items']['required'] = required

            ui_schemas.append({
                'key': key,
                'templateOptions': {
                    'objectTemplateOptions': {
                        'layoutType': self.layout_type()
                    }
                },
                'items': []
            })
            items = ui_schemas[-1]['items']
        else:
            control_schema['properties'] = {}
            control_schema['required'] = []
            required = control_schema['required']
            properties = control_schema['properties']
            ui_schemas.append({
                'templateOptions': {
                    'layoutType': self.layout_type()
                },
                'key': key,
                'items': []
            })
            if key:
                items = ui_schemas[-1]['items']
            else:
                items = ui_schemas

        assert items is not None
        assert properties is not None
        assert required is not None

        # include fields in this container's schema
        for field in self.fields:
            field_ui_schema: Dict[str, Any] = {}
            properties[field.key] = {}
            field_key = field.key
            if key:
                if self._property_type() == 'array':
                    field_key = key + '[].' + field.key
                else:
                    field_key = key + '.' + field.key

            if isinstance(field, FormField):
                _type = field.get_type()
                properties[field.key]['type'] = _type
                properties[field.key]['title'] = field.name
                field_ui_schema['key'] = field_key
                field_ui_schema['readonly'] = field.readonly
                if field.readonly:
                    field_ui_schema['templateOptions'] = {
                        'disabled': True
                    }
                field_ui_schema['help'] = f'{field.help}'
                field_ui_schema['validators'] = [i.value for i in field.validators]
                items.append(field_ui_schema)
            elif isinstance(field, Container):
                container_schema = field.to_dict(key+'.'+field.key if key else field.key)
                properties[field.key] = container_schema['control_schema']
                ui_schemas.extend(container_schema['ui_schema'])
            if not field.optional:
                required.append(field.key)
        return {
            'ui_schema': ui_schemas,
            'control_schema': control_schema,
        }


class VerticalContainer(Container):
    def layout_type(self):
        return 'column'

    def _property_type(self):
        return 'object'


class HorizontalContainer(Container):
    def layout_type(self):
        return 'row'

    def _property_type(self):
        return 'object'


class ArrayVerticalContainer(Container):
    def layout_type(self):
        return 'column'

    def _property_type(self):
        return 'array'


class ArrayHorizontalContainer(Container):
    def layout_type(self):
        return 'row'

    def _property_type(self):
        return 'array'


class FormTaskInfo:
    def __init__(self, message: str, metadata_fields: List[str]) -> None:
        self.message = message
        self.metadata_fields = metadata_fields

    def to_dict(self):
        return {'message': self.message, 'metadataFields': self.metadata_fields}


class Form:
    def __init__(self, path, root_container, method_type='',
                 task_info: FormTaskInfo = FormTaskInfo("Unknown task", []),
                 model_callback=None):
        self.path = path
        self.root_container: Container = root_container
        self.method_type = method_type
        self.task_info = task_info
        self.model_callback = model_callback

    def to_dict(self):
        res = self.root_container.to_dict()
        res['method_type'] = self.method_type
        res['task_info'] = self.task_info.to_dict()
        res['path'] = self.path
        res['ask'] = self.path
        return res


class CRUDMeta(SerializableClass):
    def __init__(self):
        self.table = TableComponent()
        self.permissions = []
        self.actions = []
        self.forms = []
        self.columnKey = ''
        self.detail_columns = []
        self.resource = ''


class CRUDCollectionMethod(NamedTuple):
    func: Callable[..., Iterable[Any]]
    doc: EndpointDoc


class CRUDResourceMethod(NamedTuple):
    func: Callable[..., Any]
    doc: EndpointDoc


# pylint: disable=R0902
class CRUDEndpoint:
    # for testing purposes
    CRUDClass: Optional[RESTController] = None
    CRUDClassMetadata: Optional[RESTController] = None

    def __init__(self, router: APIRouter, doc: APIDoc,
                 set_column: Optional[Dict[str, Dict[str, Union[str, bool]]]] = None,
                 actions: Optional[List[TableAction]] = None,
                 permissions: Optional[List[str]] = None, forms: Optional[List[Form]] = None,
                 column_key: Optional[str] = None,
                 resource: Optional[str] = None,
                 meta: CRUDMeta = CRUDMeta(), get_all: Optional[CRUDCollectionMethod] = None,
                 create: Optional[CRUDCollectionMethod] = None,
                 delete: Optional[CRUDCollectionMethod] = None,
                 selection_type: SelectionType = SelectionType.SINGLE,
                 extra_endpoints: Optional[List[Tuple[str, CRUDCollectionMethod]]] = None,
                 edit: Optional[CRUDCollectionMethod] = None,
                 detail_columns: Optional[List[str]] = None):
        self.router = router
        self.doc = doc
        self.set_column = set_column
        self.actions = actions if actions is not None else []
        self.forms = forms if forms is not None else []
        self.meta = meta
        self.get_all = get_all
        self.create = create
        self.delete = delete
        self.edit = edit
        self.permissions = permissions if permissions is not None else []
        self.column_key = column_key if column_key is not None else ''
        self.detail_columns = detail_columns if detail_columns is not None else []
        self.extra_endpoints = extra_endpoints if extra_endpoints is not None else []
        self.selection_type = selection_type
        self.resource = resource

    def __call__(self, cls: Any):
        self.create_crud_class(cls)

        self.meta.table.columns.extend(TableColumn(prop=field) for field in cls._fields)
        self.create_meta_class(cls)
        return cls

    def create_crud_class(self, cls):
        outer_self: CRUDEndpoint = self

        funcs = {}
        if self.get_all:
            @self.get_all.doc
            @wraps(self.get_all.func)
            def _list(self, *args, **kwargs):
                items = []
                for item in outer_self.get_all.func(self, *args, **kwargs):  # type: ignore
                    items.append(serialize(cls(**item)))
                return items
            funcs['list'] = _list

        if self.create:
            @self.create.doc
            @wraps(self.create.func)
            def _create(self, *args, **kwargs):
                return outer_self.create.func(self, *args, **kwargs)  # type: ignore
            funcs['create'] = _create

        if self.delete:
            @self.delete.doc
            @wraps(self.delete.func)
            def delete(self, *args, **kwargs):
                return outer_self.delete.func(self, *args, **kwargs)  # type: ignore
            funcs['delete'] = delete

        if self.edit:
            @self.edit.doc
            @wraps(self.edit.func)
            def singleton_set(self, *args, **kwargs):
                return outer_self.edit.func(self, *args, **kwargs)  # type: ignore
            funcs['singleton_set'] = singleton_set

        for extra_endpoint in self.extra_endpoints:
            funcs[extra_endpoint[0]] = extra_endpoint[1].doc(extra_endpoint[1].func)

        class_name = self.router.path.replace('/', '')
        crud_class = type(f'{class_name}_CRUDClass',
                          (RESTController,),
                          {
                              **funcs,
                              'outer_self': self,
                          })
        self.router(self.doc(crud_class))
        cls.CRUDClass = crud_class

    def create_meta_class(self, cls):
        def _list(self, model_key: str = ''):
            self.update_columns()
            self.generate_actions()
            self.generate_forms(model_key)
            self.set_permissions()
            self.set_column_key()
            self.set_table_resource()
            self.get_detail_columns()
            selection_type = self.__class__.outer_self.selection_type
            self.__class__.outer_self.meta.table.set_selection_type(selection_type)
            return serialize(self.__class__.outer_self.meta)

        def get_detail_columns(self):
            columns = self.__class__.outer_self.detail_columns
            self.__class__.outer_self.meta.detail_columns = columns

        def update_columns(self):
            if self.__class__.outer_self.set_column:
                for i, column in enumerate(self.__class__.outer_self.meta.table.columns):
                    if column.prop in dict(self.__class__.outer_self.set_column):
                        prop = self.__class__.outer_self.set_column[column.prop]
                        new_template = ""
                        if "cellTemplate" in prop:
                            new_template = prop["cellTemplate"]
                        hidden = prop['isHidden'] if 'isHidden' in prop else False
                        flex_grow = prop['flexGrow'] if 'flexGrow' in prop else column.flexGrow
                        new_column = TableColumn(column.prop,
                                                 new_template,
                                                 hidden,
                                                 column.filterable,
                                                 flex_grow)
                        self.__class__.outer_self.meta.table.columns[i] = new_column

        def generate_actions(self):
            self.__class__.outer_self.meta.actions.clear()

            for action in self.__class__.outer_self.actions:
                self.__class__.outer_self.meta.actions.append(action._asdict())

        def generate_forms(self, model_key):
            self.__class__.outer_self.meta.forms.clear()

            for form in self.__class__.outer_self.forms:
                form_as_dict = form.to_dict()
                model = {}
                if form.model_callback and model_key:
                    model = form.model_callback(model_key)
                    form_as_dict['model'] = model
                self.__class__.outer_self.meta.forms.append(form_as_dict)

        def set_permissions(self):
            self.__class__.outer_self.meta.permissions.clear()

            if self.__class__.outer_self.permissions:
                self.outer_self.meta.permissions.extend(self.__class__.outer_self.permissions)

        def set_column_key(self):
            if self.__class__.outer_self.column_key:
                self.outer_self.meta.columnKey = self.__class__.outer_self.column_key

        def set_table_resource(self):
            if self.__class__.outer_self.resource:
                self.outer_self.meta.resource = self.__class__.outer_self.resource

        class_name = self.router.path.replace('/', '')
        meta_class = type(f'{class_name}_CRUDClassMetadata',
                          (RESTController,),
                          {
                              'list': _list,
                              'update_columns': update_columns,
                              'generate_actions': generate_actions,
                              'generate_forms': generate_forms,
                              'set_permissions': set_permissions,
                              'set_column_key': set_column_key,
                              'set_table_resource': set_table_resource,
                              'get_detail_columns': get_detail_columns,
                              'outer_self': self,
                          })
        UIRouter(self.router.path, self.router.security_scope)(meta_class)
        cls.CRUDClassMetadata = meta_class
