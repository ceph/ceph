from enum import Enum
from functools import wraps
from inspect import isclass
from typing import Any, Callable, Dict, Generator, Iterable, Iterator, List, \
    NamedTuple, Optional, Union, get_type_hints

from ._api_router import APIRouter
from ._docs import APIDoc, EndpointDoc
from ._rest_controller import RESTController
from ._ui_router import UIRouter


class SecretStr(str):
    pass


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
    routerLink: str  # redirect to...


class TableComponent(SerializableClass):
    def __init__(self) -> None:
        self.columns: List[TableColumn] = []
        self.columnMode: str = 'flex'
        self.toolHeader: bool = True


class Icon(Enum):
    add = 'fa fa-plus'


class FormField(NamedTuple):
    """
    The key of a FromField is then used to send the data related to that key into the
    POST and PUT endpoints. It is imperative for the developer to map keys of fields and containers
    to the input of the POST and PUT endpoints.
    """
    name: str
    key: str
    field_type: Any = str
    default_value: Optional[Any] = None
    optional: bool = False

    def get_type(self):
        _type = ''
        if self.field_type == str:
            _type = 'string'
        elif self.field_type == int:
            _type = 'int'
        elif self.field_type == bool:
            _type = 'boolean'
        else:
            raise NotImplementedError(f'Unimplemented type {self.field_type}')
        return _type


class Container:
    def __init__(self, name: str, key: str, fields: List[Union[FormField, "Container"]],
                 optional: bool = False, min_items=1):
        self.name = name
        self.key = key
        self.fields = fields
        self.optional = optional
        self.min_items = min_items

    def layout_type(self):
        raise NotImplementedError

    def _property_type(self):
        raise NotImplementedError

    def to_dict(self, key=''):
        # intialize the schema of this container
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
            field_ui_schema = {}
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


class Form:
    def __init__(self, path, root_container):
        self.path = path
        self.root_container = root_container

    def to_dict(self):
        return self.root_container.to_dict()


class CRUDMeta(SerializableClass):
    def __init__(self):
        self.table = TableComponent()
        self.permissions = []
        self.actions = []
        self.forms = []


class CRUDCollectionMethod(NamedTuple):
    func: Callable[..., Iterable[Any]]
    doc: EndpointDoc


class CRUDResourceMethod(NamedTuple):
    func: Callable[..., Any]
    doc: EndpointDoc


class CRUDEndpoint:
    # for testing purposes
    CRUDClass: Optional[RESTController] = None
    CRUDClassMetadata: Optional[RESTController] = None

    # pylint: disable=R0902
    def __init__(self, router: APIRouter, doc: APIDoc,
                 set_column: Optional[Dict[str, Dict[str, str]]] = None,
                 actions: Optional[List[TableAction]] = None,
                 permissions: Optional[List[str]] = None, forms: Optional[List[Form]] = None,
                 meta: CRUDMeta = CRUDMeta(), get_all: Optional[CRUDCollectionMethod] = None,
                 create: Optional[CRUDCollectionMethod] = None):
        self.router = router
        self.doc = doc
        self.set_column = set_column
        if actions:
            self.actions = actions
        else:
            self.actions = []

        if forms:
            self.forms = forms
        else:
            self.forms = []
        self.meta = meta
        self.get_all = get_all
        self.create = create
        if permissions:
            self.permissions = permissions
        else:
            self.permissions = []

    def __call__(self, cls: Any):
        self.create_crud_class(cls)

        self.meta.table.columns.extend(TableColumn(prop=field) for field in cls._fields)
        self.create_meta_class(cls)
        return cls

    def create_crud_class(self, cls):
        outer_self: CRUDEndpoint = self

        @self.router
        @self.doc
        class CRUDClass(RESTController):

            if self.get_all:
                @self.get_all.doc
                @wraps(self.get_all.func)
                def list(self, *args, **kwargs):
                    items = []
                    for item in outer_self.get_all.func(self, *args, **kwargs):  # type: ignore
                        items.append(serialize(cls(**item)))
                    return items

            if self.create:
                @self.create.doc
                @wraps(self.create.func)
                def create(self, *args, **kwargs):
                    return outer_self.create.func(self, *args, **kwargs)  # type: ignore

        cls.CRUDClass = CRUDClass

    def create_meta_class(self, cls):
        def _list(self):
            self.update_columns()
            self.generate_actions()
            self.generate_forms()
            self.set_permissions()
            return serialize(self.__class__.outer_self.meta)

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

        def generate_forms(self):
            self.__class__.outer_self.meta.forms.clear()

            for form in self.__class__.outer_self.forms:
                self.__class__.outer_self.meta.forms.append(form.to_dict())

        def set_permissions(self):
            if self.__class__.outer_self.permissions:
                self.outer_self.meta.permissions.extend(self.__class__.outer_self.permissions)
        class_name = self.router.path.replace('/', '')
        meta_class = type(f'{class_name}_CRUDClassMetadata',
                          (RESTController,),
                          {
                              'list': _list,
                              'update_columns': update_columns,
                              'generate_actions': generate_actions,
                              'generate_forms': generate_forms,
                              'set_permissions': set_permissions,
                              'outer_self': self,
                          })
        UIRouter(self.router.path, self.router.security_scope)(meta_class)
        cls.CRUDClassMetadata = meta_class
