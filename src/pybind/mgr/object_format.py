# object_format.py provides types and functions for working with
# requested output formats such as JSON, YAML, etc.

import enum
import json
import sys

from typing import (
    Any,
    Dict,
    Iterable,
    Optional,
    TYPE_CHECKING,
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
        """Return an integer value to provide the Ceph MGR with a error code
        for the MGR's response tuple. Zero means success. Return an negative
        errno otherwise.
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
    """Return true if obj is usable as a YAMLDataProvider."""
    return callable(getattr(obj, 'mgr_return_value', None))


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
