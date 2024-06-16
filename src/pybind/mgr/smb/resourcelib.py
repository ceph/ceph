"""resourcelib - a semi-automated library for structured objects

This library aims to be both compatible with the mgr's object_format library
and be a base for a cleaner looking & easier working way to accomplish things
like the `ceph nfs export apply` command and the `ceph orch apply` command
but for smb. This only defines the tools to do semi-automated structuring/unstructuring
- serialization into JSON/YAML can be handled at higher levels but without
needing to get fancy in those libraries.

Quick Example:
>>> from . import resourcelib
>>> @resourcelib.component()
... class Rect:
...     x_pos: int
...     y_pos: int
...     width: int
...     height: int
>>> @resourcelib.resource('art')
... class Art:
...    name: str
...    rectangles: List[Rect]
>>> rdata = {
...     'resource_type': 'art',
...     'name': 'My Paintings',
...     'rectangles': [
...         {'x_pos': 5, 'y_pos': 0, 'width': 12, 'height': 9},
...         {'x_pos': 20, 'y_pos': 35, 'width': 10, 'height': 40},
...     ],
... }
>>> a = resourcelib.load(rdata)
>>> isinstance(a[0], Art)
True

That is, a dictionary containing a `resource_type` field and other data
fields can be easily converted into a more structured python object
based on dataclasses and a few decorators. This object can also be
converted back into a less-structured dict using the automatically
created `to_simplified` method.

One can also work directly with the Resource objects that are created
for each class. First, the special method `_customize_resource` with
the `customize` decorator can be used to alter the Resource immediately
after it is created. Alternatively, the property `_resource_config` is
added to every resourcelib component or resource.

Example:
>>> @resourcelib.resource('widget')
... class Widget:
...    name: str
...    description: str = ''
...    labels: Optional[List[str]] = None
...    @resourcelib.customize
...    def _customize_resource(rc: resourcelib.Resource) -> resourcelib.Resource:
...        # set description field quiet property to true. this will hide
...        # empty strings from the unstructured dict
...        rc.description.quiet = True
...        return rc
>>> def do_something():
...     "Do something silly with the widget class resource."
...     w = Widget('default', 'A generic widget')
...     return Widget._resource_config.object_to_simplified(w)
>>> do_something()
{'resource_type': 'widget', 'name': 'default', 'description': 'A generic widget'}

Technically, in the _customize_resource method, you can manipulate the Resource
object deeply or even return a truly customized Resource subclass object
if you want to!

Multiple classes can share a single `resource_type` name if, and only if,
all resources are configured to have a condition. A condition is a function
that takes the raw dict and returns true/false indicating that the resource
and the dataclass it handles are appropriate for the data in question.
One can set the condition function using the `Resource.on_condition` method.
This can be invoked via the `_customize_resource` method.

The library doesn't check if the conditions are comprehensive. If you have
two classes mapped to resource_type "x" and neither condition returns true
the library will simply raise an exception.
"""
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Hashable,
    List,
    Optional,
    Tuple,
)

import dataclasses
import logging
import sys
from itertools import chain

from .proto import Self, Simplified

log = logging.getLogger(__name__)

_RESOURCE_TYPE = 'resource_type'
_RESOURCES = 'resources'
_DEBUG = False


if _DEBUG:

    def _xt(f: Callable) -> Callable:
        def _func(*args: Any, **kwargs: Any) -> Any:
            log.debug(f'\nCALL({f}): {args!r}, {kwargs!r}')
            try:
                result = f(*args, **kwargs)
                log.debug(f'\n RET({f}): result={result!r}')
                return result
            except Exception as err:
                log.debug(f'\n EXC({f}): {err!r}')
                raise

        return _func

else:

    def _xt(f: Callable) -> Callable:
        return f


if sys.version_info >= (3, 11):
    from typing import dataclass_transform
elif TYPE_CHECKING:
    from typing_extensions import dataclass_transform
else:
    # no-op decorator, placeholder for dataclass_transform
    def dataclass_transform(*args: Any, **kwargs: Any) -> Callable:
        def _phony(f: Callable) -> Callable:
            return f

        return _phony


# ---- Error Types ----


class ResourceTypeError(ValueError):
    """Generic error working with smb resource types."""

    pass


class MissingResourceTypeError(KeyError, ResourceTypeError):
    """Exception raised when converting from unstructured data and a required
    `resource_type` field is missing.
    """

    def __init__(self, data: Simplified) -> None:
        self.data = data

    def __str__(self) -> str:
        return 'source data is missing a resource_type field'


class InvalidResourceTypeError(ResourceTypeError):
    """Exception raised when an object can not be converted from unstructured
    data.
    """

    def __init__(self, *, expected: Any = None, actual: Any = None) -> None:
        self.expected = expected
        self.actual = actual

    def __str__(self) -> str:
        msg = f'invalid resource type value: {self.actual!r}'
        if self.expected:
            msg += f'; expected: {self.expected!r}'
        return msg


class MissingRequiredFieldError(KeyError, ResourceTypeError):
    """Exception raised when an object can not be converted from unstructured
    data due to a missing required field.
    """

    def __init__(self, key: str) -> None:
        self.key = key

    def __str__(self) -> str:
        return f'data object missing required field: {self.key}'


# ---- Internal Resource Types ----

# Sentinel object for unset/missing value conditions.
_unset = object()


def _unwrap_type(atype: Any) -> Tuple[Tuple, bool]:
    """Given an Optional[T] type return (T, True). Given a non-optional type
    return the  (T, false).
    """
    args = _get_args(atype)
    uargs: Tuple[Any, ...] = tuple(a for a in args if a is not type(None))
    return uargs, bool(args and len(args) != len(uargs))


def _get_args(atype: Any) -> Tuple:
    """Given a type object return the types args.
    Example: List[T] -> (T,), Union[A, B] -> (A, B).
    """
    return getattr(atype, '__args__', tuple())


def _get_origin(atype: Any) -> Any:
    """Given a type such as List[T] return the outer type (List)."""
    return getattr(atype, '__origin__', None)


@dataclasses.dataclass
class Field:
    """Metadata about a field (member) of a resource type class."""

    name: str
    field_type: Any
    default: Any = _unset
    quiet: bool = False  # customization value
    keep_none: bool = False  # customization value

    def optional(self) -> bool:
        """Return true if the type of the field is Optional."""
        _, optional = _unwrap_type(self.field_type)
        return optional

    def inner_type(self) -> Any:
        """For a field with an optional type (Optional[T]) return the
        nested type (T). Otherwise return the current field type.
        """
        args, optional = _unwrap_type(self.field_type)
        if optional:
            assert len(args) == 1
            return args[0]
        return self.field_type

    @_xt
    def takes(self, dest_type: Any) -> bool:
        """Returns true if the field's type is composed of a matching container
        type. It supports field types of the form `List[T]`, `Optional[List[T]]`,
        `Dict[T, V]`, `Optional[Dict[T, V]]`.

        For example a field with type `List[str]` returns true for
        `f.takes(list)`. Similarly, `Optional[List[str]]` returns true for
        `f.takes(list)`. A field `Dict[int, int]` returns true for `f.takes(dict)`,
        and so on.

        Ideally newer-style types like `list[T] | None` supported by later
        python versions should also work but this has not been tested.
        """
        otype = _get_origin(self.inner_type())
        if otype is None:
            return False

        if dest_type is list:
            dest_type = (list, List)
        elif dest_type is dict:
            dest_type = (dict, Dict)
        elif not isinstance(dest_type, tuple):
            dest_type = (dest_type,)
        return otype in dest_type

    def list_element_type(self) -> Any:
        """Assuming the field type is a list (List[T]) return the type
        of the list's elements (T).
        """
        args, optional = _unwrap_type(self.inner_type())
        assert not optional
        assert len(args) == 1
        return args[0]

    def dict_element_types(self) -> Tuple[Any, Any]:
        """Assuming the field type is a dict (List[KT, VT]) return the types
        of the dict's keys & values (KT, VT).
        """
        args, optional = _unwrap_type(self.inner_type())
        assert not optional
        assert len(args) == 2
        return args[0], args[1]

    @classmethod
    def create(cls, fld: dataclasses.Field) -> Self:
        """Contructor converting from a dataclasses.Field."""
        default = _unset
        if fld.default is not dataclasses.MISSING:
            default = fld.default
        return cls(
            name=fld.name,
            field_type=fld.type,
            default=default,
        )


class Resource:
    """Type converter that maintains metadata about a structured python object
    and can be used to convert to / from the structured object and unstructured
    (JSON/YAML-safe dict) data.
    """

    def __init__(self, cls: Any) -> None:
        self.resource_cls = cls
        self.fields: Dict[str, Field] = {}
        self._on_condition: Optional[Callable[..., bool]] = None

        for fld in dataclasses.fields(self.resource_cls):
            self.fields[fld.name] = Field.create(fld)

    @property
    def conditional(self) -> bool:
        """Return true if the resource is selectable based on a condition."""
        return self._on_condition is not None

    def on_condition(self, cond: Callable[..., bool]) -> None:
        """Set a condition function."""
        self._on_condition = cond

    def type_name(self) -> str:
        """Return the name of the type managed by this resource."""
        return self.resource_cls.__name__

    def __getattr__(self, name: str) -> Field:
        """Return a field metadata object for the type managed by this resource."""
        return self.fields[name]

    @_xt
    def object_from_simplified(self, data: Simplified) -> Any:
        """Given a dict-based unstructured data object return the structured
        object-based equivalent.
        """
        kw = {}
        for fld in self.fields.values():
            value = self._object_field_from_simplified(fld, data)
            if value is not _unset:
                kw[fld.name] = value
        obj = self.resource_cls(**kw)
        validate = getattr(obj, 'validate', None)
        if validate:
            validate()
        return obj

    @_xt
    def _object_field_from_simplified(
        self, fld: Field, data: Simplified
    ) -> Any:
        if fld.name not in data and fld.default is not _unset:
            return _unset
        elif fld.name not in data:
            raise MissingRequiredFieldError(fld.name)

        value = data[fld.name]
        if value is None and fld.optional():
            return None
        inner_type = fld.inner_type()

        _rconfig = getattr(inner_type, '_resource_config', None)
        if _rconfig:
            return _rconfig.object_from_simplified(value)
        _fs = getattr(inner_type, 'from_simplified', None)
        if _fs:
            return _fs(value)

        if fld.takes(list):
            subtype = fld.list_element_type()
            return [
                self._object_sub_from_simplified(subtype, v) for v in value
            ]
        if fld.takes(dict):
            ktype, vtype = fld.dict_element_types()
            # keys must be simple types right now so we just
            # cast it directly
            return {
                ktype(k): self._object_sub_from_simplified(vtype, v)
                for k, v in value.items()
            }

        return inner_type(value)

    @_xt
    def _object_sub_from_simplified(
        self, subtype: Any, data: Simplified
    ) -> Any:
        _rconfig = getattr(subtype, '_resource_config', None)
        if _rconfig:
            return _rconfig.object_from_simplified(data)
        if _get_origin(subtype) in (list, List):
            return list(data)
        if _get_origin(subtype) in (dict, Dict):
            return dict(data)
        return subtype(data)

    @_xt
    def object_to_simplified(self, obj: Any) -> Simplified:
        """Given a python object tagged as a resource type return the
        unstructured data equivalent.
        """
        result: Simplified = {}
        rt = getattr(obj, _RESOURCE_TYPE, None)
        if rt is not None:
            result[_RESOURCE_TYPE] = rt
        for fld in self.fields.values():
            self._object_field_to_simplified(obj, fld, result)
        return result

    @_xt
    def _object_field_to_simplified(
        self, obj: Any, fld: Field, data: Simplified
    ) -> None:
        value = getattr(obj, fld.name)
        assert fld.optional() or value is not None
        if (value is None and not fld.keep_none) or (
            fld.quiet and value is not None and not value
        ):
            return

        _rconfig = getattr(fld.inner_type(), '_resource_config', None)
        if _rconfig:
            data[fld.name] = _rconfig.object_to_simplified(value)
            return
        _ts = getattr(fld.inner_type(), 'to_simplified', None)
        if _ts:
            data[fld.name] = _ts(value)
            return

        if isinstance(value, list):
            assert fld.takes(list)
            subtype = fld.list_element_type()
            data[fld.name] = [
                self._object_sub_to_simplified(subtype, v) for v in value
            ]
            return
        if isinstance(value, dict):
            assert fld.takes(dict)
            ktype, vtype = fld.dict_element_types()
            data[fld.name] = {
                ktype(k): self._object_sub_to_simplified(vtype, v)
                for k, v in value.items()
            }
            return

        if isinstance(value, str):
            data[fld.name] = str(value)
            return
        if isinstance(value, (int, float)):
            data[fld.name] = value
            return
        raise ResourceTypeError(f'unexpected type for field {fld.name}')

    @_xt
    def _object_sub_to_simplified(self, subtype: Any, value: Any) -> Any:
        _rconfig = getattr(subtype, '_resource_config', None)
        if _rconfig:
            return _rconfig.object_to_simplified(value)
        if isinstance(value, dict):
            return value
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            return str(value)
        if isinstance(value, (int, float)):
            return value
        raise ResourceTypeError(f'unexpected type: {type(value)}')

    def __repr__(self) -> str:
        return (
            f'{self.__class__.__name__}<'
            f'{self.type_name()}, conditional={self.conditional}>'
        )

    @classmethod
    def create(cls, resource_cls: Any) -> Self:
        """Constructor that creates a Resource for a given python class."""
        resource = cls(resource_cls)
        _customize = getattr(resource_cls, '_customize_resource', None)
        if _customize is not None:
            resource = _customize(resource)
        return resource


class Registry:
    """Registry to track resource objects."""

    def __init__(self) -> None:
        self.resources: Dict[str, List[Resource]] = {}
        self.types: Dict[Hashable, Resource] = {}

    def enable(self, cls: Any) -> Resource:
        """Given a python class create and record resource for it.  Return the
        new resource.
        """
        resource = Resource.create(cls)
        self.types[cls] = resource
        return resource

    def track(self, key: str, resource: Resource) -> None:
        """Given a resource-type-name and a resource object, save these items
        into the registry. A key may be repeated creating a "discriminating
        union" so long as the resource type provides a condition function to
        determine what resource to use.
        """
        if key not in self.resources:
            self.resources[key] = []
        self.resources[key].append(resource)
        if len(self.resources[key]) > 1:
            if any(not c.conditional for c in self.resources[key]):
                raise ResourceTypeError(
                    'multiplexed resources must be conditional'
                )

    def select(self, data: Simplified) -> Resource:
        """Given an unstructured data object use the resource_type field and
        any condition functions to return the resource object that matches.
        """
        try:
            rt = data[_RESOURCE_TYPE]
        except KeyError:
            raise MissingResourceTypeError(data)
        try:
            matches = self.resources[rt]
        except KeyError:
            raise InvalidResourceTypeError(actual=rt)
        if len(matches) == 1:
            return matches[0]
        for rc in matches:
            if not rc._on_condition:
                raise ResourceTypeError('resource not conditional')
            if rc._on_condition(data):
                return rc
        raise ResourceTypeError('no resource type conditions met')


class _ResourceType:
    """A read-only property of a class acting as a resource type. When accessed,
    returns the name of the resource type.
    """

    def __init__(self, name: str) -> None:
        self.name = name

    def __get__(self, obj: Any, objtype: Any = None) -> str:
        return self.name


class _ResourceConfig:
    """A read-only property of a class acting as a resource type that, when
    accessed, returns the resource object.
    """

    def __init__(self, resource: Resource) -> None:
        self.resource = resource

    def __get__(self, obj: Any, objtype: Any = None) -> Resource:
        return self.resource


# _REGISTRY is our internal registry object
_REGISTRY = Registry()


# ---- decorators ----


def _to_simplified(obj: Any) -> Simplified:
    rc = getattr(obj, '_resource_config')
    return rc.object_to_simplified(obj)


def _make_resource(cls: Any, resource_name: str = '') -> Any:
    cls = dataclasses.dataclass(cls)
    rconfig = _REGISTRY.enable(cls)
    cls._resource_config = _ResourceConfig(rconfig)
    if resource_name:
        cls.resource_type = _ResourceType(resource_name)
        _REGISTRY.track(resource_name, rconfig)
    if getattr(cls, 'to_simplified', None) is None:
        cls.to_simplified = _to_simplified
    return cls


@dataclass_transform()
def resource(resource_name: str) -> Any:
    """Class-decorator that establishes a new dataclass/resource type.
    These types can be converted from raw data using the `load` function.
    """
    assert resource_name

    def _decorator(cls: Any) -> Any:
        return _make_resource(cls, resource_name)

    return _decorator


@dataclass_transform()
def component() -> Any:
    """Class-decorator that establishes a new dataclass/resource type without a
    top-level name. These types may be manually converted from raw or embedded
    within a resource type.
    """

    def _decorator(cls: Any) -> Any:
        return _make_resource(cls)

    return _decorator


# TODO: make customize validate the function and not just a wrapper over
# staticmethod
customize = staticmethod


# --- resource load function ----


@_xt
def load(data: Simplified) -> List[Any]:
    """Given a simple unstructured data object (python dict/list) containing
    only other simple data types, use the `resource_type` metadata to
    convert the input to a list of structured and typed objects.

    The input may be:
     * a single item: {"resource_type": "foo", ...} which will return
       a single valued list
     * a python list containing dicts like the above
     * a dict containing the key "resources" that maps to a list containing dicts
       like the 1st item
    """
    # Given a bare list/iterator. Assume it contains loadable objects.
    if not isinstance(data, dict):
        return list(chain.from_iterable(load(v) for v in data))
    # Given a "list object"
    if _RESOURCE_TYPE not in data and _RESOURCES in data:
        rl = data[_RESOURCES]
        if not isinstance(rl, list):
            raise TypeError('expected resources list')
        return list(chain.from_iterable(load(v) for v in rl))
    # anything else must be a "self describing" object with a resource_type
    # value
    resource = _REGISTRY.select(data)
    return [resource.object_from_simplified(data)]
