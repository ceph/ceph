from typing import Any, Dict, Type, TypeVar

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
