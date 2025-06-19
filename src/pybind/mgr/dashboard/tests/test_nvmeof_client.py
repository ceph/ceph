from typing import Dict, List, NamedTuple, Optional
from unittest.mock import MagicMock

import pytest

from ..services import nvmeof_client
from ..services.nvmeof_client import MaxRecursionDepthError, convert_to_model, \
    obj_to_namedtuple, pick


class TestObjToNamedTuple:
    def test_basic(self):
        class Person(NamedTuple):
            name: str
            age: int

        class P:
            def __init__(self, name, age):
                self._name = name
                self._age = age

            @property
            def name(self):
                return self._name

            @property
            def age(self):
                return self._age

        obj = P("Alice", 25)

        person = obj_to_namedtuple(obj, Person)
        assert person.name == "Alice"
        assert person.age == 25

    def test_nested(self):
        class Address(NamedTuple):
            street: str
            city: str

        class Person(NamedTuple):
            name: str
            age: int
            address: Address

        obj = MagicMock()
        obj.name = "Bob"
        obj.age = 30
        obj.address.street = "456 Oak St"
        obj.address.city = "Springfield"

        person = obj_to_namedtuple(obj, Person)
        assert person.name == "Bob"
        assert person.age == 30
        assert person.address.street == "456 Oak St"
        assert person.address.city == "Springfield"

    def test_empty_obj(self):
        class Person(NamedTuple):
            name: str
            age: int

        obj = object()

        person = obj_to_namedtuple(obj, Person)
        assert person.name is None
        assert person.age is None

    def test_empty_list_or_dict(self):
        class Person(NamedTuple):
            name: str
            hobbies: List[str]
            address: Dict[str, str]

        class P:
            def __init__(self, name, hobbies, address):
                self._name = name
                self._hobbies = hobbies
                self._address = address

            @property
            def name(self):
                return self._name

            @property
            def hobbies(self):
                return self._hobbies

            @property
            def address(self):
                return self._address
        name = "George"
        obj = P(name, [], {})

        person = obj_to_namedtuple(obj, Person)
        assert person.name == "George"
        assert person.hobbies == []
        assert person.address == {}


class TestJsonToNamedTuple:

    def test_basic(self):
        class Person(NamedTuple):
            name: str
            age: int

        json_data = {
            "name": "Alice",
            "age": 25
        }

        person = obj_to_namedtuple(json_data, Person)
        assert person.name == "Alice"
        assert person.age == 25

    def test_nested(self):
        class Address(NamedTuple):
            street: str
            city: str

        class Person(NamedTuple):
            name: str
            age: int
            address: Address

        json_data = {
            "name": "Bob",
            "age": 30,
            "address": {
                "street": "456 Oak St",
                "city": "Springfield"
            }
        }

        person = obj_to_namedtuple(json_data, Person)
        assert person.name == "Bob"
        assert person.age == 30
        assert person.address.street == "456 Oak St"
        assert person.address.city == "Springfield"

    def test_list(self):
        class Person(NamedTuple):
            name: str
            hobbies: List[str]

        json_data = {
            "name": "Charlie",
            "hobbies": ["reading", "cycling", "swimming"]
        }

        person = obj_to_namedtuple(json_data, Person)
        assert person.name == "Charlie"
        assert person.hobbies == ["reading", "cycling", "swimming"]

    def test_nested_list(self):
        class Address(NamedTuple):
            street: str
            city: str

        class Person(NamedTuple):
            name: str
            addresses: List[Address]

        json_data = {
            "name": "Diana",
            "addresses": [
                {"street": "789 Pine St", "city": "Oakville"},
                {"street": "101 Maple Ave", "city": "Mapleton"}
            ]
        }

        person = obj_to_namedtuple(json_data, Person)
        assert person.name == "Diana"
        assert len(person.addresses) == 2
        assert person.addresses[0].street == "789 Pine St"
        assert person.addresses[1].street == "101 Maple Ave"

    def test_missing_fields(self):
        class Person(NamedTuple):
            name: str
            age: int
            address: str

        json_data = {
            "name": "Eva",
            "age": 40
        }

        person = obj_to_namedtuple(json_data, Person)
        assert person.name == "Eva"
        assert person.age == 40
        assert person.address is None

    def test_redundant_fields(self):
        class Person(NamedTuple):
            name: str
            age: int

        json_data = {
            "name": "Eva",
            "age": 40,
            "last_name": "Cohen"
        }

        person = obj_to_namedtuple(json_data, Person)
        assert person.name == "Eva"
        assert person.age == 40

    def test_max_depth_exceeded(self):
        class Bla(NamedTuple):
            a: str

        class Address(NamedTuple):
            street: str
            city: str
            bla: Bla

        class Person(NamedTuple):
            name: str
            address: Address

        json_data = {
            "name": "Frank",
            "address": {
                "street": "123 Elm St",
                "city": "Somewhere",
                "bla": {
                    "a": "blabla",
                }
            }
        }

        with pytest.raises(MaxRecursionDepthError):
            obj_to_namedtuple(json_data, Person, max_depth=2)

    def test_empty_json(self):
        class Person(NamedTuple):
            name: str
            age: int

        json_data = {}

        person = obj_to_namedtuple(json_data, Person)
        assert person.name is None
        assert person.age is None

    def test_empty_list_or_dict(self):
        class Person(NamedTuple):
            name: str
            hobbies: List[str]
            address: Dict[str, str]

        json_data = {
            "name": "George",
            "hobbies": [],
            "address": {}
        }

        person = obj_to_namedtuple(json_data, Person)
        assert person.name == "George"
        assert person.hobbies == []
        assert person.address == {}

    def test_depth_within_limit(self):
        class Address(NamedTuple):
            street: str
            city: str

        class Person(NamedTuple):
            name: str
            address: Address

        json_data = {
            "name": "Helen",
            "address": {
                "street": "123 Main St",
                "city": "Metropolis"
            }
        }

        person = obj_to_namedtuple(json_data, Person, max_depth=4)
        assert person.name == "Helen"
        assert person.address.street == "123 Main St"
        assert person.address.city == "Metropolis"


class Boy(NamedTuple):
    name: str
    age: int


class Adult(NamedTuple):
    name: str
    age: int
    children: List[str]
    hobby: Optional[str]


class EmptyModel(NamedTuple):
    pass


class ModelWithDefaultParam(NamedTuple):
    a: str
    b: str = 'iamdefault'


@pytest.fixture(name="person_func")
def fixture_person_func():
    @convert_to_model(Boy)
    def get_person() -> dict:
        return {"name": "Alice", "age": 30}
    return get_person


@pytest.fixture(name="empty_func")
def fixture_empty_func():
    @convert_to_model(EmptyModel)
    def get_empty_data() -> dict:
        return {}
    return get_empty_data


@pytest.fixture(name="disable_message_to_dict")
def fixture_disable_message_to_dict(monkeypatch):
    monkeypatch.setattr(nvmeof_client, 'MessageToDict', lambda x, **kwargs: x)


class TestConvertToModel:
    def test_basic_functionality(self, person_func, disable_message_to_dict):
        # pylint: disable=unused-argument
        result = person_func()
        assert result == {'name': 'Alice', 'age': 30}

    def test_empty_output(self, disable_message_to_dict):
        # pylint: disable=unused-argument
        @convert_to_model(Boy)
        def get_empty_person() -> dict:
            return {}

        result = get_empty_person()
        assert result == {'name': None, 'age': None}  # Assuming default values for empty fields

    def test_non_dict_return_value(self, disable_message_to_dict):
        # pylint: disable=unused-argument
        @convert_to_model(Boy)
        def get_person_list() -> list:
            return ["Alice", 30]  # This is an invalid return type

        with pytest.raises(TypeError):
            get_person_list()

    def test_optional_fields(self, disable_message_to_dict):
        # pylint: disable=unused-argument
        @convert_to_model(Adult)
        def get_adult() -> dict:
            return {"name": "Charlie", "age": 40, "children": []}

        result = get_adult()
        assert result == {'name': 'Charlie', 'age': 40, "children": [], 'hobby': None}

    def test_fields_default_value(self, disable_message_to_dict):
        # pylint: disable=unused-argument
        @convert_to_model(ModelWithDefaultParam)
        def get() -> dict:
            return {"a": "bla"}

        result = get()
        assert result == {'a': 'bla', 'b': "iamdefault"}

        # pylint: disable=unused-argument
        @convert_to_model(ModelWithDefaultParam)
        def get2() -> dict:
            return {"a": "bla", "b": 'notdefault'}

        result = get2()
        assert result == {'a': 'bla', 'b': "notdefault"}

    def test_nested_fields(self, disable_message_to_dict):
        # pylint: disable=unused-argument
        @convert_to_model(Adult)
        def get_adult() -> dict:
            return {"name": "Charlie", "age": 40, "children": [{"name": "Alice", "age": 30}]}

        result = get_adult()
        assert result == {'name': 'Charlie', 'age': 40,
                          "children": [{"name": "Alice", "age": 30}], 'hobby': None}

    def test_none_as_input(self, disable_message_to_dict):
        # pylint: disable=unused-argument
        @convert_to_model(Boy)
        def get_none_person() -> dict:
            return None

        with pytest.raises(TypeError):
            get_none_person()

    def test_multiple_function_calls(self, person_func, disable_message_to_dict):
        # pylint: disable=unused-argument
        result1 = person_func()
        result2 = person_func()
        assert result1 == result2

    def test_empty_model(self, empty_func, disable_message_to_dict):
        # pylint: disable=unused-argument
        result = empty_func()
        assert result == {}

    def test_finalize(self, disable_message_to_dict):
        # pylint: disable=unused-argument
        def finalizer(output):
            output['name'] = output['name'].upper()
            return output

        @convert_to_model(Boy, finalize=finalizer)
        def get_person() -> dict:
            return {"name": "Alice", "age": 30}

        assert get_person()['name'] == "ALICE"


class TestPick:
    def test_basic_field_access(self):
        @pick("name")
        def get_person():
            return {"name": "Alice", "height": 170}

        assert get_person() == "Alice"

    def test_first_true_on_string_field(self):
        @pick("name", first=True)
        def get_person():
            return {"name": "Alice"}

        assert get_person() == "A"

    def test_first_true_on_list_field(self):
        @pick("tags", first=True)
        def get_item():
            return {"item": "Shirt", "tags": ["red", "cotton", "medium"]}

        assert get_item() == "red"

    def test_default_field_access_on_list_field(self):
        @pick("tags")
        def get_item():
            return {"item": "Shirt", "tags": ["red", "cotton", "medium"]}

        assert get_item() == ["red", "cotton", "medium"]

    def test_nested_models(self):
        @pick("address")
        def get_person():
            return {"name": "Alice", "address": {"state": "New York", "country": "USA"}}

        assert get_person() == {"state": "New York", "country": "USA"}

    def test_field_not_present(self):
        @pick("email")
        def get_person():
            return {"name": "Alice", "address": {"state": "New York", "country": "USA"}}

        with pytest.raises(KeyError):
            get_person()

    def test_first_true_on_empty_collection(self):
        @pick("tags", first=True)
        def get_item():
            return {"item": "Shirt", "tags": []}
        with pytest.raises(IndexError):
            get_item()

    def test_first_true_on_empty_string(self):
        @pick("name", first=True)
        def get_person():
            return {"name": ""}
        with pytest.raises(IndexError):
            get_person()

    def test_none_type_field(self):
        @pick("job")
        def get_person():
            return {"name": ""}
        with pytest.raises(KeyError):
            get_person()

    def test_none_model(self):
        @pick("name")
        def get_person():
            return None
        with pytest.raises(TypeError):
            get_person()
