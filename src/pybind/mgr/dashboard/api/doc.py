from enum import Enum
from typing import Any, Dict, List, Optional


class SchemaType(Enum):
    """
    Representation of the type property of a schema object:
    http://spec.openapis.org/oas/v3.0.3.html#schema-object
    """
    ARRAY = 'array'
    BOOLEAN = 'boolean'
    INTEGER = 'integer'
    NUMBER = 'number'
    OBJECT = 'object'
    STRING = 'string'

    def __str__(self):
        return str(self.value)


class Schema:
    """
    Representation of a schema object:
    http://spec.openapis.org/oas/v3.0.3.html#schema-object
    """

    def __init__(self, schema_type: SchemaType = SchemaType.OBJECT,
                 properties: Optional[Dict] = None, required: Optional[List] = None):
        self._type = schema_type
        self._properties = properties if properties else {}
        self._required = required if required else []

    def as_dict(self) -> Dict[str, Any]:
        schema: Dict[str, Any] = {'type': str(self._type)}

        if self._type == SchemaType.ARRAY:
            items = Schema(properties=self._properties)
            schema['items'] = items.as_dict()
        else:
            schema['properties'] = self._properties

        if self._required:
            schema['required'] = self._required

        return schema


class SchemaInput:
    """
    Simple DTO to transfer data in a structured manner for creating a schema object.
    """
    type: SchemaType
    params: List[Any]
