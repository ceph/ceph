# -*- coding: utf-8 -*-
from __future__ import absolute_import

import cherrypy

from . import Controller, BaseController, Endpoint, ENDPOINT_MAP
from .. import logger, mgr

from ..tools import str_to_bool


@Controller('/docs', secure=False)
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

        TAG_MAP = {}
        for ctrl in list_of_ctrl:
            tag_name = ctrl.__name__
            tag_descr = ""
            if hasattr(ctrl, 'doc_info'):
                if ctrl.doc_info['tag']:
                    tag_name = ctrl.doc_info['tag']
                tag_descr = ctrl.doc_info['tag_descr']
            if tag_name not in TAG_MAP or not TAG_MAP[tag_name]:
                TAG_MAP[tag_name] = tag_descr

        tags = [{'name': k, 'description': v if v else "*No description available*"}
                for k, v in TAG_MAP.items()]
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
            return "boolean"
        if "size" in param_name:
            return "integer"
        if "count" in param_name:
            return "integer"
        if "num" in param_name:
            return "integer"
        if isinstance(def_value, bool):
            return "boolean"
        if isinstance(def_value, int):
            return "integer"
        return "string"

    @classmethod
    # isinstance doesn't work: input is always <type 'type'>.
    def _type_to_str(cls, type_as_type):
        """ Used if type is explicitly defined. """
        if type_as_type is str:
            type_as_str = 'string'
        elif type_as_type is int:
            type_as_str = 'integer'
        elif type_as_type is bool:
            type_as_str = 'boolean'
        elif type_as_type is list or type_as_type is tuple:
            type_as_str = 'array'
        elif type_as_type is float:
            type_as_str = 'number'
        else:
            type_as_str = 'object'
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
    def _gen_schema_for_content(cls, params):
        """
        Generates information to the content-object in OpenAPI Spec.
        Used to for request body and responses.
        """
        required_params = []
        properties = {}

        for param in params:
            if param['required']:
                required_params.append(param['name'])

            props = {}
            if 'type' in param:
                props['type'] = cls._type_to_str(param['type'])
                if 'nested_params' in param:
                    if props['type'] == 'array':  # dict in array
                        props['items'] = cls._gen_schema_for_content(param['nested_params'])
                    else:  # dict in dict
                        props = cls._gen_schema_for_content(param['nested_params'])
                elif props['type'] == 'object':  # e.g. [int]
                    props['type'] = 'array'
                    props['items'] = {'type': cls._type_to_str(param['type'][0])}
            else:
                props['type'] = cls._gen_type(param)
            if 'description' in param:
                props['description'] = param['description']
            if 'default' in param:
                props['default'] = param['default']
            properties[param['name']] = props

        schema = {
            'type': 'object',
            'properties': properties,
        }
        if required_params:
            schema['required'] = required_params
        return schema

    @classmethod
    def _gen_responses(cls, method, resp_object=None):
        resp = {
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
        if method.lower() == 'get':
            resp['200'] = {'description': "OK"}
        if method.lower() == 'post':
            resp['201'] = {'description': "Resource created."}
        if method.lower() == 'put':
            resp['200'] = {'description': "Resource updated."}
        if method.lower() == 'delete':
            resp['204'] = {'description': "Resource deleted."}
        if method.lower() in ['post', 'put', 'delete']:
            resp['202'] = {'description': "Operation is still executing."
                                          " Please check the task queue."}

        if resp_object:
            for status_code, response_body in resp_object.items():
                resp[status_code].update({
                    'content': {
                        'application/json': {
                            'schema': cls._gen_schema_for_content(response_body)}}})

        return resp

    @classmethod
    def _gen_params(cls, params, location):
        parameters = []
        for param in params:
            if 'type' in param:
                _type = cls._type_to_str(param['type'])
            else:
                _type = cls._gen_type(param)
            if 'description' in param:
                descr = param['description']
            else:
                descr = "*No description available*"
            res = {
                'name': param['name'],
                'in': location,
                'schema': {
                    'type': _type
                },
                'description': descr
            }
            if param['required']:
                res['required'] = True
            elif param['default'] is None:
                res['allowEmptyValue'] = True
            else:
                res['default'] = param['default']
            parameters.append(res)

        return parameters

    @classmethod
    def _gen_paths(cls, all_endpoints, baseUrl):
        METHOD_ORDER = ['get', 'post', 'put', 'delete']
        paths = {}
        for path, endpoints in sorted(list(ENDPOINT_MAP.items()),
                                      key=lambda p: p[0]):
            methods = {}
            skip = False

            endpoint_list = sorted(endpoints, key=lambda e:
                                   METHOD_ORDER.index(e.method.lower()))
            for endpoint in endpoint_list:
                if not endpoint.is_api and not all_endpoints:
                    skip = True
                    break

                method = endpoint.method
                func = endpoint.func

                summary = "No description available"
                resp = {}
                p_info = []
                if hasattr(func, 'doc_info'):
                    if func.doc_info['summary']:
                        summary = func.doc_info['summary']
                    resp = func.doc_info['response']
                    p_info = func.doc_info['parameters']
                params = []
                if endpoint.path_params:
                    params.extend(
                        cls._gen_params(
                            cls._add_param_info(endpoint.path_params, p_info), 'path'))
                if endpoint.query_params:
                    params.extend(
                        cls._gen_params(
                            cls._add_param_info(endpoint.query_params, p_info), 'query'))

                methods[method.lower()] = {
                    'tags': [cls._get_tag(endpoint)],
                    'summary': summary,
                    'description': func.__doc__,
                    'parameters': params,
                    'responses': cls._gen_responses(method, resp)
                }

                if method.lower() in ['post', 'put']:
                    if endpoint.body_params:
                        body_params = cls._add_param_info(endpoint.body_params, p_info)
                        methods[method.lower()]['requestBody'] = {
                            'content': {
                                'application/json': {
                                    'schema': cls._gen_schema_for_content(body_params)}}}

                if endpoint.is_secure:
                    methods[method.lower()]['security'] = [{'jwt': []}]

            if not skip:
                paths[path[len(baseUrl):]] = methods

        return paths

    def _gen_spec(self, all_endpoints=False, base_url=""):
        if all_endpoints:
            base_url = ""

        host = cherrypy.request.base
        host = host[host.index(':')+3:]
        logger.debug("DOCS: Host: %s", host)

        paths = self._gen_paths(all_endpoints, base_url)

        if not base_url:
            base_url = "/"

        scheme = 'https'
        ssl = str_to_bool(mgr.get_localized_module_option('ssl', True))
        if not ssl:
            scheme = 'http'

        spec = {
            'openapi': "3.0.0",
            'info': {
                'description': "Please note that this API is not an official "
                               "Ceph REST API to be used by third-party "
                               "applications. It's primary purpose is to serve"
                               " the requirements of the Ceph Dashboard and is"
                               " subject to change at any time. Use at your "
                               "own risk.",
                'version': "v1",
                'title': "Ceph-Dashboard REST API"
            },
            'host': host,
            'basePath': base_url,
            'servers': [{'url': "{}{}".format(cherrypy.request.base, base_url)}],
            'tags': self._gen_tags(all_endpoints),
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

    @Endpoint(path="api.json")
    def api_json(self):
        return self._gen_spec(False, "/api")

    @Endpoint(path="api-all.json")
    def api_all_json(self):
        return self._gen_spec(True, "/api")

    def _swagger_ui_page(self, all_endpoints=False, token=None):
        base = cherrypy.request.base
        if all_endpoints:
            spec_url = "{}/docs/api-all.json".format(base)
        else:
            spec_url = "{}/docs/api.json".format(base)

        auth_header = cherrypy.request.headers.get('authorization')
        jwt_token = ""
        if auth_header is not None:
            scheme, params = auth_header.split(' ', 1)
            if scheme.lower() == 'bearer':
                jwt_token = params
        else:
            if token is not None:
                jwt_token = token

        api_key_callback = """, onComplete: () => {{
                        ui.preauthorizeApiKey('jwt', '{}');
                    }}
        """.format(jwt_token)

        page = """
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="referrer" content="no-referrer" />
            <link href="https://fonts.googleapis.com/css?family=Open+Sans:400, \
                        700|Source+Code+Pro:300,600|Titillium+Web:400,600,700"
                  rel="stylesheet">
            <link rel="stylesheet" type="text/css"
                  href="//unpkg.com/swagger-ui-dist@3/swagger-ui.css" >
            <style>
                html
                {{
                    box-sizing: border-box;
                    overflow: -moz-scrollbars-vertical;
                    overflow-y: scroll;
                }}
                *,
                *:before,
                *:after
                {{
                    box-sizing: inherit;
                }}
                body {{
                    margin:0;
                    background: #fafafa;
                }}
            </style>
        </head>
        <body>
        <div id="swagger-ui"></div>
        <script src="//unpkg.com/swagger-ui-dist@3/swagger-ui-bundle.js">
        </script>
        <script>
            window.onload = function() {{
                const ui = SwaggerUIBundle({{
                    url: '{}',
                    dom_id: '#swagger-ui',
                    presets: [
                        SwaggerUIBundle.presets.apis
                    ],
                    layout: "BaseLayout"
                    {}
                }})
                window.ui = ui
            }}
        </script>
        </body>
        </html>
        """.format(spec_url, api_key_callback)

        return page

    @Endpoint(json_response=False)
    def __call__(self, all_endpoints=False):
        return self._swagger_ui_page(all_endpoints)

    @Endpoint('POST', path="/", json_response=False,
              query_params="{all_endpoints}")
    def _with_token(self, token, all_endpoints=False):
        return self._swagger_ui_page(all_endpoints, token)
