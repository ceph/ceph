from typing import (
    Any,
    Dict,
    Optional,
    Tuple,
    Type,
    TypeVar,
)

import pytest

import object_format


T = TypeVar("T", bound="Parent")


class Simpler:
    def __init__(self, name, val=None):
        self.name = name
        self.val = val or {}
        self.version = 1

    def to_simplified(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "name": self.name,
            "value": self.val,
        }


class JSONer(Simpler):
    def to_json(self) -> Dict[str, Any]:
        d = self.to_simplified()
        d["_json"] = True
        return d

    @classmethod
    def from_json(cls: Type[T], data) -> T:
        o = cls(data.get("name", ""), data.get("value"))
        o.version = data.get("version", 1) + 1
        return o


class YAMLer(Simpler):
    def to_yaml(self) -> Dict[str, Any]:
        d = self.to_simplified()
        d["_yaml"] = True
        return d


@pytest.mark.parametrize(
    "obj, compatible, json_val",
    [
        ({}, False, "{}"),
        ({"name": "foobar"}, False, '{"name": "foobar"}'),
        ([1, 2, 3], False, "[1, 2, 3]"),
        (JSONer("bob"), False, '{"name": "bob", "value": {}, "version": 1}'),
        (
            JSONer("betty", 77),
            False,
            '{"name": "betty", "value": 77, "version": 1}',
        ),
        ({}, True, "{}"),
        ({"name": "foobar"}, True, '{"name": "foobar"}'),
        (
            JSONer("bob"),
            True,
            '{"_json": true, "name": "bob", "value": {}, "version": 1}',
        ),
    ],
)
def test_format_json(obj: Any, compatible: bool, json_val: str):
    assert (
        object_format.ObjectFormatAdapter(
            obj, compatible=compatible, json_indent=None
        ).format_json()
        == json_val
    )


@pytest.mark.parametrize(
    "obj, compatible, yaml_val",
    [
        ({}, False, "{}\n"),
        ({"name": "foobar"}, False, "name: foobar\n"),
        (
            {"stuff": [1, 88, 909, 32]},
            False,
            "stuff:\n- 1\n- 88\n- 909\n- 32\n",
        ),
        (
            JSONer("zebulon", "999"),
            False,
            "name: zebulon\nvalue: '999'\nversion: 1\n",
        ),
        ({}, True, "{}\n"),
        ({"name": "foobar"}, True, "name: foobar\n"),
        (
            YAMLer("thingy", "404"),
            True,
            "_yaml: true\nname: thingy\nvalue: '404'\nversion: 1\n",
        ),
    ],
)
def test_format_yaml(obj: Any, compatible: bool, yaml_val: str):
    assert (
        object_format.ObjectFormatAdapter(
            obj, compatible=compatible
        ).format_yaml()
        == yaml_val
    )


class Retty:
    def __init__(self, v) -> None:
        self.value = v

    def mgr_return_value(self) -> int:
        return self.value


@pytest.mark.parametrize(
    "obj, ret",
    [
        ({}, 0),
        ({"fish": "sticks"}, 0),
        (-55, 0),
        (Retty(0), 0),
        (Retty(-55), -55),
    ],
)
def test_return_value(obj: Any, ret: int):
    rva = object_format.ReturnValueAdapter(obj)
    # a ReturnValueAdapter instance meets the ReturnValueProvider protocol.
    assert object_format._is_return_value_provider(rva)
    assert rva.mgr_return_value() == ret


def test_valid_formats():
    ofa = object_format.ObjectFormatAdapter({"fred": "wilma"})
    vf = ofa.valid_formats()
    assert "json" in vf
    assert "yaml" in vf
    assert "xml" in vf
    assert "plain" in vf


def test_error_response_exceptions():
    err = object_format.ErrorResponseBase()
    with pytest.raises(NotImplementedError):
        err.format_response()

    err = object_format.UnsupportedFormat("cheese")
    assert err.format_response() == (-22, "", "Unsupported format: cheese")

    err = object_format.UnknownFormat("chocolate")
    assert err.format_response() == (-22, "", "Unknown format name: chocolate")


@pytest.mark.parametrize(
    "value, format, result",
    [
        ({}, None, (0, "{}", "")),
        ({"blat": True}, "json", (0, '{\n  "blat": true\n}', "")),
        ({"blat": True}, "yaml", (0, "blat: true\n", "")),
        ({"blat": True}, "toml", (-22, "", "Unknown format name: toml")),
        ({"blat": True}, "xml", (-22, "", "Unsupported format: xml")),
        (
            JSONer("hoop", "303"),
            "yaml",
            (0, "name: hoop\nvalue: '303'\nversion: 1\n", ""),
        ),
    ],
)
def test_responder_decorator_default(
    value: Any, format: Optional[str], result: Tuple[int, str, str]
) -> None:
    @object_format.Responder()
    def orf_value(format: Optional[str] = None):
        return value

    assert orf_value(format=format) == result


class PhonyMultiYAMLFormatAdapter(object_format.ObjectFormatAdapter):
    """This adapter puts a yaml document/directive separator line
    before all output. It doesn't actully support multiple documents.
    """
    def format_yaml(self):
        yml = super().format_yaml()
        return "---\n{}".format(yml)


@pytest.mark.parametrize(
    "value, format, result",
    [
        ({}, None, (0, "{}", "")),
        ({"blat": True}, "json", (0, '{\n  "blat": true\n}', "")),
        ({"blat": True}, "yaml", (0, "---\nblat: true\n", "")),
        ({"blat": True}, "toml", (-22, "", "Unknown format name: toml")),
        ({"blat": True}, "xml", (-22, "", "Unsupported format: xml")),
        (
            JSONer("hoop", "303"),
            "yaml",
            (0, "---\nname: hoop\nvalue: '303'\nversion: 1\n", ""),
        ),
    ],
)
def test_responder_decorator_custom(
    value: Any, format: Optional[str], result: Tuple[int, str, str]
) -> None:
    @object_format.Responder(PhonyMultiYAMLFormatAdapter)
    def orf_value(format: Optional[str] = None):
        return value

    assert orf_value(format=format) == result
