import errno
from typing import (
    Any,
    Dict,
    Optional,
    Tuple,
    Type,
    TypeVar,
)

import pytest

from mgr_module import CLICommand
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
    def __init__(self, v, status="") -> None:
        self.value = v
        self.status = status

    def mgr_return_value(self) -> int:
        return self.value

    def mgr_status_value(self) -> str:
        if self.status:
            return self.status
        return "NOPE"


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


@pytest.mark.parametrize(
    "obj, ret",
    [
        ({}, ""),
        ({"fish": "sticks"}, ""),
        (-55, ""),
        (Retty(0), "NOPE"),
        (Retty(-55, "cake"), "cake"),
        (Retty(-50, "pie"), "pie"),
    ],
)
def test_return_status(obj: Any, ret: str):
    rva = object_format.StatusValueAdapter(obj)
    # a StatusValueAdapter instance meets the StatusValueProvider protocol.
    assert object_format._is_status_value_provider(rva)
    assert rva.mgr_status_value() == ret


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


class FancyDemoAdapter(PhonyMultiYAMLFormatAdapter):
    """This adapter demonstrates adding formatting for other formats
    like xml and plain text.
    """
    def format_xml(self) -> str:
        name = self.obj.get("name")
        size = self.obj.get("size")
        return f'<object name="{name}" size="{size}" />'

    def format_plain(self) -> str:
        name = self.obj.get("name")
        size = self.obj.get("size")
        es = 'es' if size != 1 else ''
        return f"{size} box{es} of {name}"


class DecoDemo:
    """Class to stand in for a mgr module, used to test CLICommand integration."""

    @CLICommand("alpha one", perm="rw")
    @object_format.Responder()
    def alpha_one(self, name: str = "default") -> Dict[str, str]:
        return {
            "alpha": "one",
            "name": name,
            "weight": 300,
        }

    @CLICommand("beta two", perm="r")
    @object_format.Responder()
    def beta_two(
        self, name: str = "default", format: Optional[str] = None
    ) -> Dict[str, str]:
        return {
            "beta": "two",
            "name": name,
            "weight": 72,
        }

    @CLICommand("gamma three", perm="rw")
    @object_format.Responder(FancyDemoAdapter)
    def gamma_three(self, size: int = 0) -> Dict[str, Any]:
        return {"name": "funnystuff", "size": size}

    @CLICommand("z_err", perm="rw")
    @object_format.ErrorResponseHandler()
    def z_err(self, name: str = "default") -> Tuple[int, str, str]:
        if "z" in name:
            raise object_format.ErrorResponse(f"{name} bad")
        return 0, name, ""

    @CLICommand("empty one", perm="rw")
    @object_format.EmptyResponder()
    def empty_one(self, name: str = "default", retval: Optional[int] = None) -> None:
        # in real code, this would be making some sort of state change
        # but we need to handle erors still
        if retval is None:
            retval = -5
        if name in ["pow"]:
            raise object_format.ErrorResponse(name, return_value=retval)
        return

    @CLICommand("empty bad", perm="rw")
    @object_format.EmptyResponder()
    def empty_bad(self, name: str = "default") -> int:
        # in real code, this would be making some sort of state change
        return 5


@pytest.mark.parametrize(
    "prefix, can_format, args, response",
    [
        (
            "alpha one",
            True,
            {"name": "moonbase"},
            (
                0,
                '{\n  "alpha": "one",\n  "name": "moonbase",\n  "weight": 300\n}',
                "",
            ),
        ),
        # ---
        (
            "alpha one",
            True,
            {"name": "moonbase2", "format": "yaml"},
            (
                0,
                "alpha: one\nname: moonbase2\nweight: 300\n",
                "",
            ),
        ),
        # ---
        (
            "alpha one",
            True,
            {"name": "moonbase2", "format": "chocolate"},
            (
                -22,
                "",
                "Unknown format name: chocolate",
            ),
        ),
        # ---
        (
            "beta two",
            True,
            {"name": "blocker"},
            (
                0,
                '{\n  "beta": "two",\n  "name": "blocker",\n  "weight": 72\n}',
                "",
            ),
        ),
        # ---
        (
            "beta two",
            True,
            {"name": "test", "format": "yaml"},
            (
                0,
                "beta: two\nname: test\nweight: 72\n",
                "",
            ),
        ),
        # ---
        (
            "beta two",
            True,
            {"name": "test", "format": "plain"},
            (
                -22,
                "",
                "Unsupported format: plain",
            ),
        ),
        # ---
        (
            "gamma three",
            True,
            {},
            (
                0,
                '{\n  "name": "funnystuff",\n  "size": 0\n}',
                "",
            ),
        ),
        # ---
        (
            "gamma three",
            True,
            {"size": 1, "format": "json"},
            (
                0,
                '{\n  "name": "funnystuff",\n  "size": 1\n}',
                "",
            ),
        ),
        # ---
        (
            "gamma three",
            True,
            {"size": 1, "format": "plain"},
            (
                0,
                "1 box of funnystuff",
                "",
            ),
        ),
        # ---
        (
            "gamma three",
            True,
            {"size": 2, "format": "plain"},
            (
                0,
                "2 boxes of funnystuff",
                "",
            ),
        ),
        # ---
        (
            "gamma three",
            True,
            {"size": 2, "format": "xml"},
            (
                0,
                '<object name="funnystuff" size="2" />',
                "",
            ),
        ),
        # ---
        (
            "gamma three",
            True,
            {"size": 2, "format": "toml"},
            (
                -22,
                "",
                "Unknown format name: toml",
            ),
        ),
        # ---
        (
            "z_err",
            False,
            {"name": "foobar"},
            (
                0,
                "foobar",
                "",
            ),
        ),
        # ---
        (
            "z_err",
            False,
            {"name": "zamboni"},
            (
                -22,
                "",
                "zamboni bad",
            ),
        ),
        # ---
        (
            "empty one",
            False,
            {"name": "zucchini"},
            (
                0,
                "",
                "",
            ),
        ),
        # ---
        (
            "empty one",
            False,
            {"name": "pow"},
            (
                -5,
                "",
                "pow",
            ),
        ),
        # Ensure setting return_value to zero even on an exception is honored
        (
            "empty one",
            False,
            {"name": "pow", "retval": 0},
            (
                0,
                "",
                "pow",
            ),
        ),
    ],
)
def test_cli_with_decorators(prefix, can_format, args, response):
    dd = DecoDemo()
    cmd = CLICommand.COMMANDS[prefix]
    assert cmd.call(dd, args, None) == response
    # slighly hacky way to check that the CLI "knows" about a --format option
    # checking the extra_args feature of the Decorators that provide them (Responder)
    if can_format:
        assert 'name=format,' in cmd.args


def test_error_response():
    e1 = object_format.ErrorResponse("nope")
    assert e1.format_response() == (-22, "", "nope")
    assert e1.return_value == -22
    assert e1.errno == 22
    assert "ErrorResponse" in repr(e1)
    assert "nope" in repr(e1)
    assert e1.mgr_return_value() == -22

    try:
        open("/this/is_/extremely_/unlikely/_to/exist.txt")
    except Exception as e:
        e2 = object_format.ErrorResponse.wrap(e)
    r = e2.format_response()
    assert r[0] == -errno.ENOENT
    assert r[1] == ""
    assert "No such file or directory" in r[2]
    assert "ErrorResponse" in repr(e2)
    assert "No such file or directory" in repr(e2)
    assert r[0] == e2.mgr_return_value()

    e3 = object_format.ErrorResponse.wrap(RuntimeError("blat"))
    r = e3.format_response()
    assert r[0] == -errno.EINVAL
    assert r[1] == ""
    assert "blat" in r[2]
    assert r[0] == e3.mgr_return_value()

    # A custom exception type with an errno property

    class MyCoolException(Exception):
        def __init__(self, err_msg: str, errno: int = 0) -> None:
            super().__init__(errno, err_msg)
            self.errno = errno
            self.err_msg = err_msg

        def __str__(self) -> str:
            return self.err_msg

    e4 = object_format.ErrorResponse.wrap(MyCoolException("beep", -17))
    r = e4.format_response()
    assert r[0] == -17
    assert r[1] == ""
    assert r[2] == "beep"
    assert e4.mgr_return_value() == -17

    e5 = object_format.ErrorResponse.wrap(MyCoolException("ok, fine", 0))
    r = e5.format_response()
    assert r[0] == 0
    assert r[1] == ""
    assert r[2] == "ok, fine"

    e5 = object_format.ErrorResponse.wrap(MyCoolException("no can do", 8))
    r = e5.format_response()
    assert r[0] == -8
    assert r[1] == ""
    assert r[2] == "no can do"

    # A custom exception type that inherits from ErrorResponseBase

    class MyErrorResponse(object_format.ErrorResponseBase):
        def __init__(self, err_msg: str, return_value: int):
            super().__init__(self, err_msg)
            self.msg = err_msg
            self.return_value = return_value

        def format_response(self):
            return self.return_value, "", self.msg


    e6 = object_format.ErrorResponse.wrap(MyErrorResponse("yeah, sure", 0))
    r = e6.format_response()
    assert r[0] == 0
    assert r[1] == ""
    assert r[2] == "yeah, sure"
    assert isinstance(e5, object_format.ErrorResponseBase)
    assert isinstance(e6, MyErrorResponse)

    e7 = object_format.ErrorResponse.wrap(MyErrorResponse("no can do", -8))
    r = e7.format_response()
    assert r[0] == -8
    assert r[1] == ""
    assert r[2] == "no can do"
    assert isinstance(e7, object_format.ErrorResponseBase)
    assert isinstance(e7, MyErrorResponse)


def test_empty_responder_return_check():
    dd = DecoDemo()
    with pytest.raises(ValueError):
        CLICommand.COMMANDS["empty bad"].call(dd, {}, None)
