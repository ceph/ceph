# object_format.py provides types and functions for working with
# requested output formats such as JSON, YAML, etc.
"""tools for writing formatting-friendly mgr module functions

Currently, the ceph mgr code in python is most commonly written by adding mgr
modules and corresponding classes and then adding methods to those classes that
are decorated using `@CLICommand` from  `mgr_module.py`.  These methods (that
will be called endpoints subsequently) then implement the logic that is
executed when the mgr receives a command from a client.  These endpoints are
currently responsible for forming a response tuple of (int, str, str) where the
int represents a return value (error code) and the first string the "body" of
the response. The mgr supports a generic `format` parameter (`--format` on the
ceph cli) that each endpoint must then explicitly handle. At the time of this
writing, many endpoints do not handle alternate formats and are each
implementing formatting/serialization of values in various different ways.

The `object_format` module aims to make the process of writing endpoint
functions easier, more consistent, and (hopefully) better documented.  At the
highest level, the module provides a new decorator `Responder` that must be
placed below the `CLICommand` decorator (so that it decorates the endpoint
before `CLICommand`). This decorator helps automatically convert Python objects
to response tuples expected by the manager, while handling the `format`
parameter automatically.

In addition to the decorator the module provides a few other types and methods
that intended to interoperate with the decorator and make small customizations
and error handling easier.

== Using Responder ==

The simple and intended way to use the decorator is as follows:
    @CLICommand("command name", perm="r")
    Responder()
    def create_something(self, name: str) -> Dict[str, str]:
        ...  # implementation
        return {"name": name, "id": new_id}

In this case the `create_something` method return a python dict,
and does not return a response tuple directly. Instead, the
dict is converted to either JSON or YAML depending on what the
client requested. Assuming no exception is raised by the
implementation then the response code is always zero (success).

The object_format module provides an exception type `ErrorResponse`
that assists in returning "clean" error conditions to the client.
Extending the previous example to use this exception:
    @CLICommand("command name", perm="r")
    Responder()
    def create_something(self, name: str) -> Dict[str, str]:
        try:
           ...  # implementation
           return {"name": name, "id": new_id}
        except KeyError as kerr:
           # explicitly set the return value to ENOENT for KeyError
           raise ErrorResponse.wrap(kerr, return_value=-errno.ENOENT)
        except (BusinessLogcError, OSError) as err:
           # return value is based on err when possible
           raise ErrorResponse.wrap(err)

Most uses of ErrorResponse are expected to use the `wrap` classmethod,
as it will aid in the handling of an existing exception but `ErrorResponse`
can be used directly too.

== Customizing Response Formatting ==

The `Responder` is built using two additional mid-layer types. The
`ObjectFormatAdapter` and the `ReturnValueAdapter` by default. These types
implement the `CommonFormatter` protocol and `ReturnValueProvider` protocols
respectively. Most cases will not need to customize the `ReturnValueAdapter` as
returning zero on success is expected.  However, if there's a need to return a
non-zero error code outside of an exception, you can add the `mgr_return_value`
function to the returned type of the endpoint function - causing it to meet the
`ReturnValueProvider` protocol. Whatever integer that function returns will
then be used in the response tuple.

The `ObjectFormatAdapter` can operate in two modes. By default, any type
returned from the endpoint function will be checked for a `to_simplified`
method (the type matches the SimpleDataProvider` protocol) and if it exists
the method will be called and the result serialized. Example:
    class CoolStuff:
       def __init__(self, temperature: int, quantity: int) -> None:
           self.temperature = temperature
           self.quantity = quantity
      def to_simplified(self) -> Dict[str, int]:
         return {"temp": self.temperature, "qty": self.quantity}

    @CLICommand("command name", perm="r")
    Responder()
    def create_something_cool(self) -> CoolStuff:
       cool_stuff: CoolStuff = self._make_cool_stuff()  # implementation
       return cool_stuff

In order to serialize the result, the object returned from the wrapped
function must provide the `to_simplified` method (or the compatibility methods,
see below) or already be a "simplified type". Valid types include lists and
dicts that contain other lists and dicts and ints, strs, bools -- basic objects
that can be directly converted to json (via json.dumps) without any additional
conversions. The `to_simplified` method must always return such types.

To be compatible with many existing types in the ceph mgr codebase one can pass
`compatible=True` to the `ObjectFormatAdapter`. If the type provides a
`to_json` and/or `to_yaml` method that returns basic python types (dict, list,
str, etc...) but *not* already serialized JSON or YAML this flag can be
enabled. Note that Responder takes as an argument any callable that returns a
`CommonFormatter`. In this example below we enable the flag using
`functools.partial`:
    class MyExistingClass:
      def to_json(self) -> Dict[str, Any]:
         return {"name": self.name, "height": self.height}

    @CLICommand("command name", perm="r")
    Responder(functools.partial(ObjectFormatAdapter, compatible=True))
    def create_an_item(self) -> MyExistingClass:
       item: MyExistingClass = self._new_item()  # implementation
       return item


For cases that need to return xml or plain text formatted responses one can
create a new class that matches the `CommonFormatter` protocol (provides a
valid_formats method) and one or more `format_x` method where x is the name of
a format ("json", "yaml", "xml", "plain", etc...).
    class MyCustomFormatAdapter:
       def __init__(self, obj_to_format: Any) -> None:
           ...
       def valid_formats(self) -> Iterable[str]:
           ...
       def format_json(self) -> str:
           ...
       def format_xml(self) -> str:
           ...


Of course, the Responder itself can be used as a base class and aspects of the
Responder altered for specific use cases. Inheriting from `Responder` and
customizing it is an exercise left for those brave enough to read the code in
`object_format.py` :-).
"""

import enum
import errno
import json
import sys

from functools import wraps
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    TYPE_CHECKING,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import yaml

# this uses a version check as opposed to a try/except because this
# form makes mypy happy and try/except doesn't.
if sys.version_info >= (3, 8):
    from typing import Protocol
elif TYPE_CHECKING:
    # typing_extensions will not be available for the real mgr server
    from typing_extensions import Protocol
else:
    # fallback type that is acceptable to older python on prod. builds
    class Protocol:  # type: ignore
        pass

from mgr_module import HandlerFuncType


DEFAULT_JSON_INDENT: int = 2


class Format(str, enum.Enum):
    plain = "plain"
    json = "json"
    json_pretty = "json-pretty"
    yaml = "yaml"
    xml_pretty = "xml-pretty"
    xml = "xml"


# SimpleData is a type alias for Any unless we can determine the
# exact set of subtypes we want to support. But it is explicit!
SimpleData = Any


class SimpleDataProvider(Protocol):
    def to_simplified(self) -> SimpleData:
        """Return a simplified representation of the current object.
        The simplified representation should be trivially serializable.
        """
        ...  # pragma: no cover


class JSONDataProvider(Protocol):
    def to_json(self) -> Any:
        """Return a python object that can be serialized into JSON.
        This function does _not_ return a JSON string.
        """
        ...  # pragma: no cover


class YAMLDataProvider(Protocol):
    def to_yaml(self) -> Any:
        """Return a python object that can be serialized into YAML.
        This function does _not_ return a string of YAML.
        """
        ...  # pragma: no cover


class JSONFormatter(Protocol):
    def format_json(self) -> str:
        """Return a JSON formatted representation of an object."""
        ...  # pragma: no cover


class YAMLFormatter(Protocol):
    def format_yaml(self) -> str:
        """Return a JSON formatted representation of an object."""
        ...  # pragma: no cover


class ReturnValueProvider(Protocol):
    def mgr_return_value(self) -> int:
        """Return an integer value to provide the Ceph MGR with an error code
        for the MGR's response tuple. Zero means success. Return a negative
        errno otherwise.
        """
        ...  # pragma: no cover


class StatusValueProvider(Protocol):
    def mgr_status_value(self) -> str:
        """Return a string value to provide the Ceph MGR with an error status
        for the MGR's response tuple. Empty string means success. Return a string
        containing error info otherwise.
        """
        ...  # pragma: no cover


class CommonFormatter(Protocol):
    """A protocol that indicates the type is a formatter for multiple
    possible formats.
    """

    def valid_formats(self) -> Iterable[str]:
        """Return the names of known valid formats."""
        ...  # pragma: no cover


# The _is_name_of_protocol_type functions below are here because the production
# builds of the ceph manager are lower than python 3.8 and do not have
# typing_extensions available in the resulting images. This means that
# runtime_checkable is not available and isinstance can not be used with a
# protocol type.  These could be replaced by isinstance in a later version of
# python.  Note that these functions *can not* be methods of the protocol types
# for neatness - including methods on the protocl types makes mypy consider
# those methods as part of the protcol & a required method. Using decorators
# did not change that - I checked.


def _is_simple_data_provider(obj: SimpleDataProvider) -> bool:
    """Return true if obj is usable as a SimpleDataProvider."""
    return callable(getattr(obj, 'to_simplified', None))


def _is_json_data_provider(obj: JSONDataProvider) -> bool:
    """Return true if obj is usable as a JSONDataProvider."""
    return callable(getattr(obj, 'to_json', None))


def _is_yaml_data_provider(obj: YAMLDataProvider) -> bool:
    """Return true if obj is usable as a YAMLDataProvider."""
    return callable(getattr(obj, 'to_yaml', None))


def _is_return_value_provider(obj: ReturnValueProvider) -> bool:
    """Return true if obj is usable as a ReturnValueProvider."""
    return callable(getattr(obj, 'mgr_return_value', None))


def _is_status_value_provider(obj: StatusValueProvider) -> bool:
    """Return true if obj is usable as a StatusValueProvider"""
    return callable(getattr(obj, 'mgr_status_value', None))


class ObjectFormatAdapter:
    """A format adapater for a single object.
    Given an input object, this type will adapt the object, or a simplified
    representation of the object, to either JSON or YAML when the format_json or
    format_yaml methods are used.

    If the compatible flag is true and the object provided to the adapter has
    methods such as `to_json` and/or `to_yaml` these methods will be called in
    order to get a JSON/YAML compatible simplified representation of the
    object.

    If the above case is not satisfied and the object provided to the adapter
    has a method `to_simplified`, this method will be called to acquire a
    simplified representation of the object.

    If none of the above cases is true, the object itself will be used for
    serialization. If the object can not be safely serialized an exception will
    be raised.

    NOTE: Some code may use methods named like `to_json` to return a JSON
    string. If that is the case, you should not use that method with the
    ObjectFormatAdapter. Do not set compatible=True for objects of this type.
    """

    def __init__(
        self,
        obj: Any,
        json_indent: Optional[int] = DEFAULT_JSON_INDENT,
        compatible: bool = False,
    ) -> None:
        self.obj = obj
        self._compatible = compatible
        self.json_indent = json_indent

    def _fetch_json_data(self) -> Any:
        # if the data object provides a specific simplified representation for
        # JSON (and compatible mode is enabled) get the data via that method
        if self._compatible and _is_json_data_provider(self.obj):
            return self.obj.to_json()
        # otherwise we use our specific method `to_simplified` if it exists
        if _is_simple_data_provider(self.obj):
            return self.obj.to_simplified()
        # and fall back to the "raw" object
        return self.obj

    def format_json(self) -> str:
        """Return a JSON formatted string representing the input object."""
        return json.dumps(
            self._fetch_json_data(), indent=self.json_indent, sort_keys=True
        )

    def _fetch_yaml_data(self) -> Any:
        if self._compatible and _is_yaml_data_provider(self.obj):
            return self.obj.to_yaml()
        # nothing specific to YAML was found. use the simplified representation
        # for JSON, as all valid JSON is valid YAML.
        return self._fetch_json_data()

    def format_yaml(self) -> str:
        """Return a YAML formatted string representing the input object."""
        return yaml.safe_dump(self._fetch_yaml_data())

    format_json_pretty = format_json

    def valid_formats(self) -> Iterable[str]:
        """Return valid format names."""
        return set(str(v) for v in Format.__members__)


class ReturnValueAdapter:
    """A return-value adapter for an object.
    Given an input object, this type will attempt to get a mgr return value
    from the object if provides a `mgr_return_value` function.
    If not it returns a default return value, typically 0.
    """

    def __init__(
        self,
        obj: Any,
        default: int = 0,
    ) -> None:
        self.obj = obj
        self.default_return_value = default

    def mgr_return_value(self) -> int:
        if _is_return_value_provider(self.obj):
            return int(self.obj.mgr_return_value())
        return self.default_return_value


class StatusValueAdapter:
    """A status-value adapter for an object.
    Given an input object, this type will attempt to get a mgr status value
    from the object if provides a `mgr_status_value` function.
    If not it returns a default status value, typically an empty string.
    """

    def __init__(
            self,
            obj: Any,
            default: str = "",
    ) -> None:
        self.obj = obj
        self.default_status = default

    def mgr_status_value(self) -> str:
        if _is_status_value_provider(self.obj):
            return str(self.obj.mgr_status_value())
        return self.default_status


class ErrorResponseBase(Exception):
    """An exception that can directly be converted to a mgr reponse."""

    def format_response(self) -> Tuple[int, str, str]:
        raise NotImplementedError()


class UnknownFormat(ErrorResponseBase):
    """Raised if the format name is unexpected.
    This can help distinguish typos from formats that are known but
    not implemented.
    """

    def __init__(self, format_name: str) -> None:
        self.format_name = format_name

    def format_response(self) -> Tuple[int, str, str]:
        return -errno.EINVAL, "", f"Unknown format name: {self.format_name}"


class UnsupportedFormat(ErrorResponseBase):
    """Raised if the format name does not correspond to any valid
    conversion functions.
    """

    def __init__(self, format_name: str) -> None:
        self.format_name = format_name

    def format_response(self) -> Tuple[int, str, str]:
        return -errno.EINVAL, "", f"Unsupported format: {self.format_name}"


class ErrorResponse(ErrorResponseBase):
    """General exception convertible to a mgr response."""

    E = TypeVar("E", bound="ErrorResponse")

    def __init__(self, status: str, return_value: Optional[int] = None) -> None:
        self.return_value = (
            return_value if return_value is not None else -errno.EINVAL
        )
        self.status = status

    def format_response(self) -> Tuple[int, str, str]:
        return (self.return_value, "", self.status)

    def mgr_return_value(self) -> int:
        return self.return_value

    @property
    def errno(self) -> int:
        rv = self.return_value
        return -rv if rv < 0 else rv

    def __repr__(self) -> str:
        return f"ErrorResponse({self.status!r}, {self.return_value!r})"

    @classmethod
    def wrap(
        cls: Type[E], exc: Exception, return_value: Optional[int] = None
    ) -> ErrorResponseBase:
        if isinstance(exc, ErrorResponseBase):
            return exc
        if return_value is None:
            try:
                return_value = int(getattr(exc, "errno"))
                if return_value > 0:
                    return_value = -return_value
            except (AttributeError, ValueError):
                pass
        err = cls(str(exc), return_value=return_value)
        setattr(err, "__cause__", exc)
        return err


ObjectResponseFuncType = Union[
    Callable[..., Dict[Any, Any]],
    Callable[..., List[Any]],
    Callable[..., SimpleDataProvider],
    Callable[..., JSONDataProvider],
    Callable[..., YAMLDataProvider],
    Callable[..., ReturnValueProvider],
    Callable[..., StatusValueProvider],
]


def _get_requested_format(f: ObjectResponseFuncType, kw: Dict[str, Any]) -> str:
    # todo: leave 'format' in kw dict iff its part of f's signature
    return kw.pop("format", None)


class Responder:
    """A decorator type intended to assist in converting Python return types
    into valid responses for the Ceph MGR.

    A function that returns a Python object will have the object converted into
    a return value and formatted response body, based on the `format` argument
    passed to the mgr. When used from the ceph cli tool the `--format=[name]`
    argument is mapped to a `format` keyword argument. The decorated function
    may provide a `format` argument (type str). If the decorated function does
    not provide a `format` argument itself, the Responder decorator will
    implicitly add one to the MGR's "CLI arguments" handling stack.

    The Responder object is callable and is expected to be used as a decorator.
    """

    def __init__(
        self, formatter: Optional[Callable[..., CommonFormatter]] = None
    ) -> None:
        self.formatter = formatter
        self.default_format = "json"

    def _formatter(self, obj: Any) -> CommonFormatter:
        """Return the formatter/format-adapter for the object."""
        if self.formatter is not None:
            return self.formatter(obj)
        return ObjectFormatAdapter(obj)

    def _retval_provider(self, obj: Any) -> ReturnValueProvider:
        """Return a ReturnValueProvider for the given object."""
        return ReturnValueAdapter(obj)

    def _statusval_provider(self, obj: Any) -> StatusValueProvider:
        """Return a StatusValueProvider for the given object."""
        return StatusValueAdapter(obj)

    def _get_format_func(
        self, obj: Any, format_req: Optional[str] = None
    ) -> Callable:
        formatter = self._formatter(obj)
        if format_req is None:
            format_req = self.default_format
        if format_req not in formatter.valid_formats():
            raise UnknownFormat(format_req)
        req = str(format_req).replace("-", "_")
        ffunc = getattr(formatter, f"format_{req}", None)
        if ffunc is None:
            raise UnsupportedFormat(format_req)
        return ffunc

    def _dry_run(self, format_req: Optional[str] = None) -> None:
        """Raise an exception if the format_req is not supported."""
        # call with an empty dict to see if format_req is valid and supported
        self._get_format_func({}, format_req)

    def _formatted(self, obj: Any, format_req: Optional[str] = None) -> str:
        """Return the object formatted/serialized."""
        ffunc = self._get_format_func(obj, format_req)
        return ffunc()

    def _return_value(self, obj: Any) -> int:
        """Return a mgr return-value for the given object (usually zero)."""
        return self._retval_provider(obj).mgr_return_value()

    def _return_status(self, obj: Any) -> str:
        """Return a mgr status-value for the given object (usually empty
        string).
        """
        return self._statusval_provider(obj).mgr_status_value()

    def __call__(self, f: ObjectResponseFuncType) -> HandlerFuncType:
        """Wrap a python function so that the original function's return value
        becomes the source for an automatically formatted mgr response.
        """

        @wraps(f)
        def _format_response(*args: Any, **kwargs: Any) -> Tuple[int, str, str]:
            format_req = _get_requested_format(f, kwargs)
            try:
                self._dry_run(format_req)
                robj = f(*args, **kwargs)
                body = self._formatted(robj, format_req)
                retval = self._return_value(robj)
                statusval = self._return_status(robj)
            except ErrorResponseBase as e:
                return e.format_response()
            return retval, body, statusval

        # set the extra args on our wrapper function. this will be consumed by
        # the CLICommand decorator and added to the set of optional arguments
        # on the ceph cli/api
        setattr(_format_response, "extra_args", {"format": str})
        return _format_response


class ErrorResponseHandler:
    """ErrorResponseHandler is a very simple decorator that handles functions that
    raise exceptions inheriting from ErrorResponseBase. If such an exception
    is raised that exception can and will be converted to a mgr response tuple.
    This is similar to Responder but error handling is all this decorator does.
    """

    def __call__(self, f: Callable[..., Tuple[int, str, str]]) -> HandlerFuncType:
        """Wrap a python function so that if the function raises an exception inheriting
        ErrorResponderBase the error is correctly converted to a mgr response.
        """

        @wraps(f)
        def _format_response(*args: Any, **kwargs: Any) -> Tuple[int, str, str]:
            try:
                retval, body, sts = f(*args, **kwargs)
            except ErrorResponseBase as e:
                return e.format_response()
            return retval, body, sts

        return _format_response


class ConstantResponderBase:
    """The constant responder base assumes that a wrapped function should not
    be passing data back to the manager. It only responds with the default
    (constant) values provided. The process_response function allows a subclass
    to handle/log/validate any values that were returned from the wrapped
    function.

    This class can be used a building block for special decorators that
    do not normally emit response data.
    """

    def mgr_return_value(self) -> int:
        return 0

    def mgr_body_value(self) -> str:
        return ""

    def mgr_status_value(self) -> str:
        return ""

    def process_response(self, result: Any) -> None:
        return None

    def __call__(self, f: Callable) -> HandlerFuncType:
        """Wrap a python function so that if the function raises an exception
        inheriting ErrorResponderBase the error is correctly converted to a mgr
        response. Otherwise, it returns a default set of constant values.
        """

        @wraps(f)
        def _format_response(*args: Any, **kwargs: Any) -> Tuple[int, str, str]:
            try:
                self.process_response(f(*args, **kwargs))
            except ErrorResponseBase as e:
                return e.format_response()
            return self.mgr_return_value(), self.mgr_body_value(), self.mgr_status_value()
        return _format_response


class EmptyResponder(ConstantResponderBase):
    """Always respond with an empty (string) body. Checks that the wrapped function
    returned None in order to ensure it is not being used on functions that
    return data objects.
    """

    def process_response(self, result: Any) -> None:
        if result is not None:
            raise ValueError("EmptyResponder expects None from wrapped functions")
