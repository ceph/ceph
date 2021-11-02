from typing import Any, Callable, Dict, Generator, Iterable, Iterator, List, \
    NamedTuple, Optional, get_type_hints

from ._api_router import APIRouter
from ._docs import APIDoc, EndpointDoc
from ._rest_controller import RESTController
from ._ui_router import UIRouter


class SecretStr(str):
    pass


def isnamedtuple(o):
    return isinstance(o, tuple) and hasattr(o, '_asdict') and hasattr(o, '_fields')


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
    elif isinstance(o, (Iterator, Generator)):
        return [serialize(i) for i in o]
    elif expected_type and issubclass(expected_type, SecretStr):
        return "***********"
    else:
        return o


class TableColumn(NamedTuple):
    prop: str
    cellTemplate: str = ''
    isHidden: bool = False
    filterable: bool = True


class TableComponent(NamedTuple):
    columns: List[TableColumn] = []
    columnMode: str = 'flex'
    toolHeader: bool = True


class CRUDMeta(NamedTuple):
    table: TableComponent = TableComponent()


class CRUDCollectionMethod(NamedTuple):
    func: Callable[..., Iterable[Any]]
    doc: EndpointDoc


class CRUDResourceMethod(NamedTuple):
    func: Callable[..., Any]
    doc: EndpointDoc


class CRUDEndpoint(NamedTuple):
    router: APIRouter
    doc: APIDoc
    set_column: Optional[Dict[str, Dict[str, str]]] = None
    meta: CRUDMeta = CRUDMeta()
    get_all: Optional[CRUDCollectionMethod] = None

    # for testing purposes
    CRUDClass: Optional[RESTController] = None
    CRUDClassMetadata: Optional[RESTController] = None
    # ---------------------

    def __call__(self, cls: NamedTuple):
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
                def list(self):
                    return serialize(cls(**item)
                                     for item in outer_self.get_all.func())  # type: ignore
        cls.CRUDClass = CRUDClass

    def create_meta_class(self, cls):
        outer_self: CRUDEndpoint = self

        @UIRouter(self.router.path, self.router.security_scope)
        class CRUDClassMetadata(RESTController):
            def list(self):
                self.update_columns()
                return serialize(outer_self.meta)

            def update_columns(self):
                if outer_self.set_column:
                    for i, column in enumerate(outer_self.meta.table.columns):
                        if column.prop in dict(outer_self.set_column):
                            new_template = outer_self.set_column[column.prop]["cellTemplate"]
                            new_column = TableColumn(column.prop,
                                                     new_template,
                                                     column.isHidden,
                                                     column.filterable)
                            outer_self.meta.table.columns[i] = new_column

        cls.CRUDClassMetadata = CRUDClassMetadata
