from typing import Dict, List, Optional, Tuple

import dataclasses
import enum

import pytest

import smb.resourcelib


class Thingy(str, enum.Enum):
    STUFF = 'stuff'
    JUNK = 'junk'
    DEBRIS = 'debris'


@dataclasses.dataclass
class ReadMe:
    foo: str
    bar: int = 0
    baz: float = 0.1
    bingo: Optional[str] = None
    quux: Optional[List[int]] = None
    womble: Optional[Dict[str, str]] = None
    waver: Optional[Tuple[int, str]] = None


def test_resource_config_field_metadata():
    rmc = smb.resourcelib.Resource.create(ReadMe)
    assert not rmc.conditional
    assert rmc.type_name() == 'ReadMe'

    # did it load the fields
    assert 'foo' in rmc.fields
    assert 'womble' in rmc.fields
    assert len(rmc.fields) == 7

    # magic getattr fields (quiet is always unset by default)
    assert not rmc.foo.quiet
    # optional
    assert not rmc.foo.optional()
    assert not rmc.bar.optional()
    assert rmc.bingo.optional()
    assert rmc.womble.optional()
    # inner type
    assert rmc.foo.inner_type() == str
    assert rmc.bingo.inner_type() == str
    # takes
    assert not rmc.foo.takes(str)  # takes only useful for container types
    assert rmc.quux.takes(list)
    assert rmc.womble.takes(dict)
    assert rmc.waver.takes(tuple)
    # list element type
    assert rmc.quux.list_element_type() == int
    # dict element types
    assert rmc.womble.dict_element_types() == (str, str)

    assert 'ReadMe' in repr(rmc)


@pytest.mark.parametrize(
    "params",
    [
        # very basic
        {
            'kwargs': {'foo': 'smile'},
            'expected': {
                'foo': 'smile',
                'bar': 0,
                'baz': 0.1,
            },
        },
        # set some other scalar values
        {
            'kwargs': {'foo': 'smile', 'bar': 12, 'bingo': 'b18'},
            'expected': {
                'foo': 'smile',
                'bar': 12,
                'baz': 0.1,
                'bingo': 'b18',
            },
        },
        # a list value
        {
            'kwargs': {'foo': 'smile', 'bar': 12, 'quux': [3, 11]},
            'expected': {
                'foo': 'smile',
                'bar': 12,
                'baz': 0.1,
                'quux': [3, 11],
            },
        },
        # a dict value
        {
            'kwargs': {
                'foo': 'smile',
                'bar': 12,
                'womble': {"test": "one", "be": "good"},
            },
            'expected': {
                'foo': 'smile',
                'bar': 12,
                'baz': 0.1,
                'womble': {"test": "one", "be": "good"},
            },
        },
    ],
)
def test_basic_resource_config_to_simplified(params):
    rmc = smb.resourcelib.Resource.create(ReadMe)
    obj = ReadMe(*params.get('args', []), **params.get('kwargs', {}))
    result = rmc.object_to_simplified(obj)
    assert result == params['expected']


@pytest.mark.parametrize(
    "params",
    [
        # very basic
        {
            "data": {
                "foo": "hello",
            },
            "expected": ReadMe("hello"),
        },
        # two params
        {
            "data": {
                "foo": "greetings",
                "bar": 99,
            },
            "expected": ReadMe("greetings", bar=99),
        },
        # all scalars
        {
            "data": {
                "foo": "aloha",
                "bar": 101,
                "baz": 3.14,
                "bingo": "nameo",
            },
            "expected": ReadMe("aloha", bar=101, baz=3.14, bingo='nameo'),
        },
        # list and dict
        {
            "data": {
                "foo": "icu",
                "bar": 16,
                "baz": 2.2,
                "bingo": "yep",
                "quux": [1, 5, 9],
                "womble": {"something": "for everyone", "blank": ""},
            },
            "expected": ReadMe(
                "icu",
                bar=16,
                baz=2.2,
                bingo='yep',
                quux=[1, 5, 9],
                womble={"something": "for everyone", "blank": ""},
            ),
        },
    ],
)
def test_basic_resource_config_from_simplified(params):
    data = params['data']
    rmc = smb.resourcelib.Resource.create(ReadMe)
    result = rmc.object_from_simplified(data)
    assert result == params['expected']


def test_registry():
    r = smb.resourcelib.Registry()
    assert not r.resources
    assert not r.types

    @dataclasses.dataclass
    class Foo:
        name: str

    resource = r.enable(Foo)
    assert not r.resources
    assert r.types

    r.track('foo', resource)
    assert r.resources
    assert r.types

    config2 = r.select({'resource_type': 'foo'})
    assert config2 is resource

    with pytest.raises(smb.resourcelib.MissingResourceTypeError):
        r.select({})

    with pytest.raises(smb.resourcelib.InvalidResourceTypeError):
        r.select({'resource_type': 'oopsie'})

    with pytest.raises(smb.resourcelib.ResourceTypeError):
        cx = r.enable(ReadMe)
        r.track('foo', cx)


def test_registry_select_on_condition():
    r = smb.resourcelib.Registry()

    @dataclasses.dataclass
    class A:
        name: str

        @staticmethod
        def _condition(d):
            return 'flavor' not in d and 'name' in d

    @dataclasses.dataclass
    class B:
        name: str
        flavor: str

        @staticmethod
        def _condition(d):
            return 'flavor' in d

    configa = r.enable(A)
    configa.on_condition(A._condition)
    configb = r.enable(B)
    configb.on_condition(B._condition)

    r.track('x', configa)
    r.track('x', configb)

    c = r.select({'resource_type': 'x', 'name': "joe", "flavor": "coffee"})
    assert c is configb

    c = r.select({'resource_type': 'x', 'name': "joe"})
    assert c is configa

    with pytest.raises(smb.resourcelib.ResourceTypeError):
        r.select({'resource_type': 'x'})

    # this should normally be impossible
    configa._on_condition = None
    configb._on_condition = None
    with pytest.raises(smb.resourcelib.ResourceTypeError):
        r.select({'resource_type': 'x'})


@smb.resourcelib.component()
class Worker:
    name: str
    age: int
    role: Optional[str] = None

    def validate(self):
        if not self.name:
            raise ValueError('name missing')
        if self.age <= 0:
            raise ValueError('invalid age')


@smb.resourcelib.component()
class Unit:
    label: str
    manager: Worker
    full_timers: List[Worker]
    interns: Optional[List[Worker]] = None


@smb.resourcelib.resource('bigbiz')
class BigBiz:
    name: str
    address: List[str]
    ceo: Worker
    units: Optional[List[Unit]] = None


@smb.resourcelib.resource('smallbiz')
class SmallBiz:
    name: str
    address: List[str]
    people: List[Worker]


def test_resource_round_trip():
    r1 = BigBiz(
        name='Mega Co',
        address=['1010 Bigness Way', 'Metropolis', 'IL', '012345'],
        ceo=Worker('F. Smith', 55),
        units=[
            Unit(
                label='Sales',
                manager=Worker('Al Pha', 42),
                full_timers=[Worker('P. Rep', 33)],
            ),
            Unit(
                label='Engineering',
                manager=Worker('O. Mega', 42),
                full_timers=[
                    Worker('I. Contrib', 28),
                    Worker('U. Needme', 29, role='QA'),
                ],
                interns=[Worker('J. Younya', 22)],
            ),
        ],
    )

    data = r1.to_simplified()
    assert 'resource_type' in data

    r2 = BigBiz._resource_config.object_from_simplified(data)
    assert r1 == r2


def test_resource_round_trip2():
    r1 = SmallBiz(
        name='Joes Diner',
        address=['123 Main St', 'Smallville', 'IA', '048394'],
        people=[
            Worker('Joe', 44),
            Worker('Lisa', 43),
            Worker('Tina', 23),
        ],
    )

    data = r1.to_simplified()
    assert 'resource_type' in data

    r2 = SmallBiz._resource_config.object_from_simplified(data)
    assert r1 == r2


@pytest.mark.parametrize(
    "params",
    [
        # small biz 1
        {
            'data': {
                'resource_type': 'smallbiz',
                'name': 'Le Shoppe',
                'address': ['12 Fashion Way', 'Urbia', 'WA', '01209'],
                'people': [
                    {'name': 'Madelyn', 'age': 39},
                    {'name': 'Mark', 'age': 39},
                ],
            },
            'expect_types': [SmallBiz],
        },
        # big biz 1
        {
            'data': {
                'resource_type': 'bigbiz',
                'name': 'MegaLoMart',
                'address': ['1 MegaLo Drive', 'Mango', 'TX', '22020'],
                'ceo': {'name': 'D. B. Bawes', 'age': 61},
            },
            'expect_types': [BigBiz],
        },
        # raw list
        {
            'data': [
                {
                    'resource_type': 'smallbiz',
                    'name': 'Le Shoppe',
                    'address': ['12 Fashion Way', 'Urbia', 'WA', '01209'],
                    'people': [
                        {'name': 'Madelyn', 'age': 39},
                        {'name': 'Mark', 'age': 39},
                    ],
                },
                {
                    'resource_type': 'bigbiz',
                    'name': 'MegaLoMart',
                    'address': ['1 MegaLo Drive', 'Mango', 'TX', '22020'],
                    'ceo': {'name': 'D. B. Bawes', 'age': 61},
                },
            ],
            'expect_types': [SmallBiz, BigBiz],
        },
        # list object
        {
            'data': {
                'resources': [
                    {
                        'resource_type': 'smallbiz',
                        'name': 'Le Shoppe',
                        'address': ['12 Fashion Way', 'Urbia', 'WA', '01209'],
                        'people': [
                            {'name': 'Madelyn', 'age': 39},
                            {'name': 'Mark', 'age': 39},
                        ],
                    },
                    {
                        'resource_type': 'bigbiz',
                        'name': 'MegaLoMart',
                        'address': ['1 MegaLo Drive', 'Mango', 'TX', '22020'],
                        'ceo': {'name': 'D. B. Bawes', 'age': 61},
                    },
                ]
            },
            'expect_types': [SmallBiz, BigBiz],
        },
    ],
)
def test_load(params):
    data = params['data']
    objs = smb.resourcelib.load(data)
    assert len(objs) == len(params['expect_types'])
    for obj, expect_type in zip(objs, params['expect_types']):
        assert isinstance(obj, expect_type)


def test_load_validation_error():
    data = {
        'resource_type': 'smallbiz',
        'name': 'Le Shoppe',
        'address': ['12 Fashion Way', 'Urbia', 'WA', '01209'],
        'people': [
            {'name': 'Madelyn', 'age': 39},
            {'name': '', 'age': 39},
        ],
    }
    with pytest.raises(ValueError):
        smb.resourcelib.load(data)


def test_missing_field_error():
    data = {
        'resource_type': 'smallbiz',
        'name': 'Le Shoppe',
        'address': ['12 Fashion Way', 'Urbia', 'WA', '01209'],
        'people': [
            {'name': 'Madelyn', 'age': 39},
            {'age': 39},
        ],
    }
    with pytest.raises(smb.resourcelib.MissingRequiredFieldError):
        smb.resourcelib.load(data)


def test_load_invalid_resources_type():
    data = {'resources': 55}
    with pytest.raises(TypeError):
        smb.resourcelib.load(data)


def test_explicit_none_in_data():
    data = {
        'resource_type': 'bigbiz',
        'name': 'MegaLoMart',
        'address': ['1 MegaLo Drive', 'Mango', 'TX', '22020'],
        'ceo': {'name': 'D. B. Bawes', 'age': 61},
        'units': None,
    }
    obj = BigBiz._resource_config.object_from_simplified(data)
    assert obj.units is None
