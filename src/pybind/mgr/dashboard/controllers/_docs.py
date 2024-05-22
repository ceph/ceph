from typing import Any, Dict, List, NamedTuple, Optional, Tuple, Type, Union

from ..api.doc import SchemaInput, SchemaType


class EndpointDoc:  # noqa: N802
    DICT_TYPE = Union[Dict[str, Any], Dict[int, Any]]

    def __init__(self, description: str = "", group: str = "",
                 parameters: Optional[Union[DICT_TYPE, List[Any], Tuple[Any, ...]]] = None,
                 responses: Optional[DICT_TYPE] = None) -> None:
        self.description = description
        self.group = group
        self.parameters = parameters
        self.responses = responses

        self.validate_args()

        if not self.parameters:
            self.parameters = {}  # type: ignore

        self.resp = {}
        if self.responses:
            for status_code, response_body in self.responses.items():
                schema_input = SchemaInput()
                schema_input.type = SchemaType.ARRAY if \
                    isinstance(response_body, list) else SchemaType.OBJECT
                schema_input.params = self._split_parameters(response_body)

                self.resp[str(status_code)] = schema_input

    def validate_args(self) -> None:
        if not isinstance(self.description, str):
            raise Exception("%s has been called with a description that is not a string: %s"
                            % (EndpointDoc.__name__, self.description))
        if not isinstance(self.group, str):
            raise Exception("%s has been called with a groupname that is not a string: %s"
                            % (EndpointDoc.__name__, self.group))
        if self.parameters and not isinstance(self.parameters, dict):
            raise Exception("%s has been called with parameters that is not a dict: %s"
                            % (EndpointDoc.__name__, self.parameters))
        if self.responses and not isinstance(self.responses, dict):
            raise Exception("%s has been called with responses that is not a dict: %s"
                            % (EndpointDoc.__name__, self.responses))

    def _split_param(self, name: str, p_type: Union[type, DICT_TYPE, List[Any], Tuple[Any, ...]],
                     description: str, optional: bool = False, default_value: Any = None,
                     nested: bool = False) -> Dict[str, Any]:
        param = {
            'name': name,
            'description': description,
            'required': not optional,
            'nested': nested,
        }
        if default_value:
            param['default'] = default_value
        if isinstance(p_type, type):
            param['type'] = p_type
        else:
            nested_params = self._split_parameters(p_type, nested=True)
            if nested_params:
                param['type'] = type(p_type)
                param['nested_params'] = nested_params
            else:
                param['type'] = p_type
        return param

    #  Optional must be set to True in order to set default value and parameters format must be:
    # 'name: (type or nested parameters, description, [optional], [default value])'
    def _split_dict(self, data: DICT_TYPE, nested: bool) -> List[Any]:
        splitted = []
        for name, props in data.items():
            if isinstance(name, str) and isinstance(props, tuple):
                if len(props) == 2:
                    param = self._split_param(name, props[0], props[1], nested=nested)
                elif len(props) == 3:
                    param = self._split_param(
                        name, props[0], props[1], optional=props[2], nested=nested)
                if len(props) == 4:
                    param = self._split_param(name, props[0], props[1], props[2], props[3], nested)
                splitted.append(param)
            else:
                raise Exception(
                    """Parameter %s in %s has not correct format. Valid formats are:
                    <name>: (<type>, <description>, [optional], [default value])
                    <name>: (<[type]>, <description>, [optional], [default value])
                    <name>: (<[nested parameters]>, <description>, [optional], [default value])
                    <name>: (<{nested parameters}>, <description>, [optional], [default value])"""
                    % (name, EndpointDoc.__name__))
        return splitted

    def _split_list(self, data: Union[List[Any], Tuple[Any, ...]], nested: bool) -> List[Any]:
        splitted = []  # type: List[Any]
        for item in data:
            splitted.extend(self._split_parameters(item, nested))
        return splitted

    # nested = True means parameters are inside a dict or array
    def _split_parameters(self, data: Optional[Union[DICT_TYPE, List[Any], Tuple[Any, ...]]],
                          nested: bool = False) -> List[Any]:
        param_list = []  # type: List[Any]
        if isinstance(data, dict):
            param_list.extend(self._split_dict(data, nested))
        elif isinstance(data, (list, tuple)):
            param_list.extend(self._split_list(data, True))
        return param_list

    def __call__(self, func: Any) -> Any:
        func.doc_info = {
            'summary': self.description,
            'tag': self.group,
            'parameters': self._split_parameters(self.parameters),
            'response': self.resp
        }
        return func


class Param(NamedTuple):
    type: Union[Type, List[Type]]
    description: str
    optional: bool = False
    default: Optional[Any] = None


class APIDoc(object):
    def __init__(self, description="", group=""):
        self.tag = group
        self.tag_descr = description

    def __call__(self, cls):
        cls.doc_info = {
            'tag': self.tag,
            'tag_descr': self.tag_descr
        }
        return cls
