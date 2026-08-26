# pylint: disable=C0102

import json
from typing import NamedTuple

import pytest
from jsonschema import validate

from ..controllers._crud import ArrayHorizontalContainer, \
    ArrayVerticalContainer, Form, FormField, HorizontalContainer, SecretStr, \
    VerticalContainer, serialize


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
    (NamedTupleSecretMock(1, "test", "imaginethisisakey"), {"foo": 1, "var": "test",
                                                            "key": "***********"}),
    ((1, 2, 3), [1, 2, 3]),
    (set((1, 2, 3)), [1, 2, 3]),
])
def test_serialize(inp, out):
    assertObjectEquals(serialize(inp), out)


def test_schema():
    form = Form(path='/cluster/user/create',
                root_container=VerticalContainer('Create user', key='create_user', fields=[
                    FormField('User entity', key='user_entity', field_type=str),
                    ArrayHorizontalContainer('Capabilities', key='caps', fields=[
                        FormField('left', field_type=str, key='left'),
                        FormField('right', key='right', field_type=str)
                    ]),
                    ArrayVerticalContainer('ah', key='ah', fields=[
                        FormField('top', key='top', field_type=str),
                        FormField('bottom', key='bottom', field_type=str)
                    ]),
                    HorizontalContainer('oh', key='oh', fields=[
                        FormField('left', key='left', field_type=str),
                        FormField('right', key='right', field_type=str)
                    ]),
                    VerticalContainer('ov', key='ov', fields=[
                        FormField('top', key='top', field_type=str),
                        FormField('bottom', key='bottom', field_type=bool)
                    ]),
                ]))
    form_dict = form.to_dict()
    schema = {'schema': form_dict['control_schema'], 'layout': form_dict['ui_schema']}
    validate(instance={'user_entity': 'foo',
                       'caps': [{'left': 'foo', 'right': 'foo2'}],
                       'ah': [{'top': 'foo', 'bottom': 'foo2'}],
                       'oh': {'left': 'foo', 'right': 'foo2'},
                       'ov': {'top': 'foo', 'bottom': True}}, schema=schema['schema'])


def test_enum_field_schema():
    form = Form(path='/cluster/user/create',
                root_container=VerticalContainer('Create user', key='create_user', fields=[
                    FormField('Key type', key='key_type', field_type=str,
                              default_value='aes', enum_values=['aes', 'aes256k']),
                ]))
    schema = form.to_dict()['control_schema']
    key_type = schema['properties']['key_type']
    assert key_type['enum'] == ['aes', 'aes256k']
    assert key_type['default'] == 'aes'
    validate(instance={'key_type': 'aes'}, schema=schema)
    validate(instance={'key_type': 'aes256k'}, schema=schema)
