# -*- coding: utf-8 -*-
import logging
from typing import Any, Dict, List, Optional, Union

import cherrypy

from .. import mgr
from ..api.doc import Schema, SchemaInput, SchemaType
from . import ENDPOINT_MAP, BaseController, Endpoint, Router
from ._version import APIVersion

NO_DESCRIPTION_AVAILABLE = "*No description available*"

logger = logging.getLogger('controllers.docs')


@Router('/docs', secure=False)
class Docs(BaseController):

    @classmethod
    def _gen_tags(cls, all_endpoints):
        """ Generates a list of all tags and corresponding descriptions. """
        # Scenarios to consider:
        #     * Intentionally make up a new tag name at controller => New tag name displayed.
        #     * Misspell or make up a new tag name at endpoint => Neither tag or endpoint displayed.
        #     * Misspell tag name at controller (when referring to another controller) =>
        #       Tag displayed but no endpoints assigned
        #     * Description for a tag added at multiple locations => Only one description displayed.
        list_of_ctrl = set()
        for endpoints in ENDPOINT_MAP.values():
            for endpoint in endpoints:
                if endpoint.is_api or all_endpoints:
                    list_of_ctrl.add(endpoint.ctrl)

        tag_map: Dict[str, str] = {}
        for ctrl in sorted(list_of_ctrl, key=lambda ctrl: ctrl.__name__):
            tag_name = ctrl.__name__
            tag_descr = ""
            if hasattr(ctrl, 'doc_info'):
                if ctrl.doc_info['tag']:
                    tag_name = ctrl.doc_info['tag']
                tag_descr = ctrl.doc_info['tag_descr']
            if tag_name not in tag_map or not tag_map[tag_name]:
                tag_map[tag_name] = tag_descr

        tags = [{'name': k, 'description': v if v else NO_DESCRIPTION_AVAILABLE}
                for k, v in tag_map.items()]
        tags.sort(key=lambda e: e['name'])
        return tags

    @classmethod
    def _get_tag(cls, endpoint):
        """ Returns the name of a tag to assign to a path. """
        ctrl = endpoint.ctrl
        func = endpoint.func
        tag = ctrl.__name__
        if hasattr(func, 'doc_info') and func.doc_info['tag']:
            tag = func.doc_info['tag']
        elif hasattr(ctrl, 'doc_info') and ctrl.doc_info['tag']:
            tag = ctrl.doc_info['tag']
        return tag

    @classmethod
    def _gen_type(cls, param):
        # pylint: disable=too-many-return-statements
        """
        Generates the type of parameter based on its name and default value,
        using very simple heuristics.
        Used if type is not explicitly defined.
        """
        param_name = param['name']
        def_value = param['default'] if 'default' in param else None
        if param_name.startswith("is_"):
            return str(SchemaType.BOOLEAN)
        if "size" in param_name:
            return str(SchemaType.INTEGER)
        if "count" in param_name:
            return str(SchemaType.INTEGER)
        if "num" in param_name:
            return str(SchemaType.INTEGER)
        if isinstance(def_value, bool):
            return str(SchemaType.BOOLEAN)
        if isinstance(def_value, int):
            return str(SchemaType.INTEGER)
        return str(SchemaType.STRING)

    @classmethod
    # isinstance doesn't work: input is always <type 'type'>.
    def _type_to_str(cls, type_as_type):
        """ Used if type is explicitly defined. """
        if type_as_type is str:
            type_as_str = str(SchemaType.STRING)
        elif type_as_type is int:
            type_as_str = str(SchemaType.INTEGER)
        elif type_as_type is bool:
            type_as_str = str(SchemaType.BOOLEAN)
        elif type_as_type is list or type_as_type is tuple:
            type_as_str = str(SchemaType.ARRAY)
        elif type_as_type is float:
            type_as_str = str(SchemaType.NUMBER)
        else:
            type_as_str = str(SchemaType.OBJECT)
        return type_as_str

    @classmethod
    def _add_param_info(cls, parameters, p_info):
        # Cases to consider:
        #     * Parameter name (if not nested) misspelt in decorator => parameter not displayed
        #     * Sometimes a parameter is used for several endpoints (e.g. fs_id in CephFS).
        #       Currently, there is no possibility of reuse. Should there be?
        #       But what if there are two parameters with same name but different functionality?
        """
        Adds explicitly described information for parameters of an endpoint.

        There are two cases:
        * Either the parameter in p_info corresponds to an endpoint parameter. Implicit information
        has higher priority, so only information that doesn't already exist is added.
        * Or the parameter in p_info describes a nested parameter inside an endpoint parameter.
        In that case there is no implicit information at all so all explicitly described info needs
        to be added.
        """
        for p in p_info:
            if not p['nested']:
                for parameter in parameters:
                    if p['name'] == parameter['name']:
                        parameter['type'] = p['type']
                        parameter['description'] = p['description']
                        if 'nested_params' in p:
                            parameter['nested_params'] = cls._add_param_info([], p['nested_params'])
            else:
                nested_p = {
                    'name': p['name'],
                    'type': p['type'],
                    'description': p['description'],
                    'required': p['required'],
                }
                if 'default' in p:
                    nested_p['default'] = p['default']
                if 'nested_params' in p:
                    nested_p['nested_params'] = cls._add_param_info([], p['nested_params'])
                parameters.append(nested_p)

        return parameters

    @classmethod
    def _gen_schema_for_content(cls, params: List[Any]) -> Dict[str, Any]:
        """
        Generates information to the content-object in OpenAPI Spec.
        Used to for request body and responses.
        """
        required_params = []
        properties = {}
        schema_type = SchemaType.OBJECT
        if isinstance(params, SchemaInput):
            schema_type = params.type
            params = params.params

        for param in params:
            if param['required']:
                required_params.append(param['name'])

            props = {}
            if 'type' in param:
                props['type'] = cls._type_to_str(param['type'])
                if 'nested_params' in param:
                    if props['type'] == str(SchemaType.ARRAY):  # dict in array
                        props['items'] = cls._gen_schema_for_content(param['nested_params'])
                    else:  # dict in dict
                        props = cls._gen_schema_for_content(param['nested_params'])
                elif props['type'] == str(SchemaType.OBJECT):  # e.g. [int]
                    props['type'] = str(SchemaType.ARRAY)
                    props['items'] = {'type': cls._type_to_str(param['type'][0])}
            else:
                props['type'] = cls._gen_type(param)
            if 'description' in param:
                props['description'] = param['description']
            if 'default' in param:
                props['default'] = param['default']
            properties[param['name']] = props

        schema = Schema(schema_type=schema_type, properties=properties,
                        required=required_params)

        return schema.as_dict()

    @classmethod
    def _gen_responses(cls, method, resp_object=None,
                       version: Optional[APIVersion] = None):
        resp: Dict[str, Dict[str, Union[str, Any]]] = {
            '400': {
                "description": "Operation exception. Please check the "
                               "response body for details."
            },
            '401': {
                "description": "Unauthenticated access. Please login first."
            },
            '403': {
                "description": "Unauthorized access. Please check your "
                               "permissions."
            },
            '500': {
                "description": "Unexpected error. Please check the "
                               "response body for the stack trace."
            }
        }

        if not version:
            version = APIVersion.DEFAULT

        if method.lower() == 'get':
            resp['200'] = {'description': "OK",
                           'content': {version.to_mime_type():
                                       {'type': 'object'}}}
        if method.lower() == 'post':
            resp['201'] = {'description': "Resource created.",
                           'content': {version.to_mime_type():
                                       {'type': 'object'}}}
        if method.lower() in ['put', 'patch']:
            resp['200'] = {'description': "Resource updated.",
                           'content': {version.to_mime_type():
                                       {'type': 'object'}}}
        if method.lower() == 'delete':
            resp['204'] = {'description': "Resource deleted.",
                           'content': {version.to_mime_type():
                                       {'type': 'object'}}}
        if method.lower() in ['post', 'put', 'delete']:
            resp['202'] = {'description': "Operation is still executing."
                                          " Please check the task queue.",
                           'content': {version.to_mime_type():
                                       {'type': 'object'}}}

        if resp_object:
            for status_code, response_body in resp_object.items():
                if status_code in resp:
                    resp[status_code].update(
                        {'content':
                         {version.to_mime_type():
                          {'schema': cls._gen_schema_for_content(response_body)}
                          }})

        return resp

    @classmethod
    def _gen_params(cls, params, location):
        parameters = []
        for param in params:
            if 'type' in param:
                _type = cls._type_to_str(param['type'])
            else:
                _type = cls._gen_type(param)
            res = {
                'name': param['name'],
                'in': location,
                'schema': {
                    'type': _type
                },
            }
            if param.get('description'):
                res['description'] = param['description']
            if param['required']:
                res['required'] = True
            elif param['default'] is None:
                res['allowEmptyValue'] = True
            else:
                res['default'] = param['default']
            parameters.append(res)

        return parameters

    @staticmethod
    def _process_func_attr(func):
        summary = ''
        version = None
        response = {}
        p_info = []

        if hasattr(func, '__method_map_method__'):
            version = func.__method_map_method__['version']
        elif hasattr(func, '__resource_method__'):
            version = func.__resource_method__['version']
        elif hasattr(func, '__collection_method__'):
            version = func.__collection_method__['version']

        if hasattr(func, 'doc_info'):
            if func.doc_info['summary']:
                summary = func.doc_info['summary']
            response = func.doc_info['response']
            p_info = func.doc_info['parameters']

        return summary, version, response, p_info

    @classmethod
    def _get_params(cls, endpoint, para_info):
        params = []

        def extend_params(endpoint_params, param_name):
            if endpoint_params:
                params.extend(
                    cls._gen_params(
                        cls._add_param_info(endpoint_params, para_info), param_name))

        extend_params(endpoint.path_params, 'path')
        extend_params(endpoint.query_params, 'query')
        return params

    @classmethod
    def set_request_body_param(cls, endpoint_param, method, methods, p_info):
        if endpoint_param:
            params_info = cls._add_param_info(endpoint_param, p_info)
            methods[method.lower()]['requestBody'] = {
                'content': {
                    'application/json': {
                        'schema': cls._gen_schema_for_content(params_info)}}}

    @classmethod
    def gen_paths(cls, all_endpoints):
        # pylint: disable=R0912
        method_order = ['get', 'post', 'put', 'patch', 'delete']
        paths = {}
        for path, endpoints in sorted(list(ENDPOINT_MAP.items()),
                                      key=lambda p: p[0]):
            methods = {}
            skip = False

            endpoint_list = sorted(endpoints, key=lambda e:
                                   method_order.index(e.method.lower()))
            for endpoint in endpoint_list:
                if not endpoint.is_api and not all_endpoints:
                    skip = True
                    break

                method = endpoint.method
                func = endpoint.func

                summary, version, resp, p_info = cls._process_func_attr(func)
                params = cls._get_params(endpoint, p_info)

                methods[method.lower()] = {
                    'tags': [cls._get_tag(endpoint)],
                    'description': func.__doc__,
                    'parameters': params,
                    'responses': cls._gen_responses(method, resp, version)
                }
                if summary:
                    methods[method.lower()]['summary'] = summary

                if method.lower() in ['post', 'put', 'patch']:
                    cls.set_request_body_param(endpoint.body_params, method, methods, p_info)
                    cls.set_request_body_param(endpoint.query_params, method, methods, p_info)

                if endpoint.is_secure:
                    methods[method.lower()]['security'] = [{'jwt': []}]

            if not skip:
                paths[path] = methods

        return paths

    @classmethod
    def _gen_spec(cls, all_endpoints=False, base_url="", offline=False):
        if all_endpoints:
            base_url = ""

        host = cherrypy.request.base.split('://', 1)[1] if not offline else 'example.com'
        logger.debug("Host: %s", host)

        paths = cls.gen_paths(all_endpoints)

        if not base_url:
            base_url = "/"

        scheme = 'https' if offline or mgr.get_localized_module_option('ssl') else 'http'

        spec = {
            'openapi': "3.0.0",
            'info': {
                'description': "This is the official Ceph REST API",
                'version': "v1",
                'title': "Ceph REST API"
            },
            'host': host,
            'basePath': base_url,
            'servers': [{'url': "{}{}".format(
                cherrypy.request.base if not offline else '',
                base_url)}],
            'tags': cls._gen_tags(all_endpoints),
            'schemes': [scheme],
            'paths': paths,
            'components': {
                'securitySchemes': {
                    'jwt': {
                        'type': 'http',
                        'scheme': 'bearer',
                        'bearerFormat': 'JWT'
                    }
                }
            }
        }

        return spec

    @Endpoint(path="openapi.json", version=None)
    def open_api_json(self):
        return self._gen_spec(False, "/")

    @Endpoint(path="api-all.json", version=None)
    def api_all_json(self):
        return self._gen_spec(True, "/")


if __name__ == "__main__":
    import sys

    import yaml

    def fix_null_descr(obj):
        """
        A hot fix for errors caused by null description values when generating
        static documentation: better fix would be default values in source
        to be 'None' strings: however, decorator changes didn't resolve
        """
        return {k: fix_null_descr(v) for k, v in obj.items() if v is not None} \
            if isinstance(obj, dict) else obj

    Router.generate_routes("/api")
    try:
        with open(sys.argv[1], 'w') as f:
            # pylint: disable=protected-access
            yaml.dump(
                fix_null_descr(Docs._gen_spec(all_endpoints=False, base_url="/", offline=True)),
                f)
    except IndexError:
        sys.exit("Output file name missing; correct syntax is: `cmd <file.yml>`")
    except IsADirectoryError:
        sys.exit("Specified output is a directory; correct syntax is: `cmd <file.yml>`")
