# pylint: disable=C0102

import json
from typing import NamedTuple

import pytest

from ..controllers._crud import SecretStr, serialize


def assertObjectEquals(a, b):
    assert json.dumps(a) == json.dumps(b)


class NamedTupleMock(NamedTuple):
    foo: int
    var: str


class NamedTupleSecretMock(NamedTuple):
    foo: int
    var: str
    key: SecretStr


@pytest.mark.parametrize("inp,out", [
    (["foo", "var"], ["foo", "var"]),
    (NamedTupleMock(1, "test"), {"foo": 1, "var": "test"}),
    (NamedTupleSecretMock(1, "test", "supposethisisakey"), {"foo": 1, "var": "test",
                                                            "key": "***********"}),
    ((1, 2, 3), [1, 2, 3]),
    (set((1, 2, 3)), [1, 2, 3]),
])
def test_serialize(inp, out):
    assertObjectEquals(serialize(inp), out)
