# -*- coding: utf-8 -*-
from __future__ import absolute_import

import cherrypy

from . import Controller, BaseController, Endpoint, ENDPOINT_MAP
from .. import logger


@Controller('/docs')
class Docs(BaseController):

    @classmethod
    def _gen_tags(cls, all_endpoints):
        ctrl_names = set()
        for endpoints in ENDPOINT_MAP.values():
            for endpoint in endpoints:
                if endpoint.is_api or all_endpoints:
                    ctrl_names.add(endpoint.group)

        return [{'name': name, 'description': ""}
                for name in sorted(ctrl_names)]

    @classmethod
    def _gen_type(cls, param):
        # pylint: disable=too-many-return-statements
        """
        Generates the type of parameter based on its name and default value,
        using very simple heuristics.
        """
        param_name = param['name']
        def_value = param['default'] if 'default' in param else None
        if param_name.startswith("is_"):
            return "boolean"
        elif "size" in param_name:
            return "integer"
        elif "count" in param_name:
            return "integer"
        elif "num" in param_name:
            return "integer"
        elif isinstance(def_value, bool):
            return "boolean"
        elif isinstance(def_value, int):
            return "integer"
        return "string"

    @classmethod
    def _gen_body_param(cls, body_params):
        required = [p['name'] for p in body_params if p['required']]

        props = {}
        for p in body_params:
            props[p['name']] = {
                'type': cls._gen_type(p)
            }
            if 'default' in p:
                props[p['name']]['default'] = p['default']

        if not props:
            return None

        return {
            'in': "body",
            'name': "body",
            'description': "",
            'required': True,
            'schema': {
                'type': "object",
                'required': required,
                'properties': props
            }
        }

    @classmethod
    def _gen_responses_descriptions(cls, method):
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

        return resp

    @classmethod
    def _gen_param(cls, param, ptype):
        res = {
            'name': param['name'],
            'in': ptype,
            'type': cls._gen_type(param)
        }
        if param['required']:
            res['required'] = True
        elif param['default'] is None:
            res['allowEmptyValue'] = True
        else:
            res['default'] = param['default']
        return res

    def _gen_spec(self, all_endpoints=False, baseUrl=""):
        if all_endpoints:
            baseUrl = ""
        METHOD_ORDER = ['get', 'post', 'put', 'delete']
        host = cherrypy.request.base
        host = host[host.index(':')+3:]
        logger.debug("DOCS: Host: %s", host)

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
                params = []
                params.extend([self._gen_param(p, 'path')
                               for p in endpoint.path_params])
                params.extend([self._gen_param(p, 'query')
                               for p in endpoint.query_params])

                if method.lower() in ['post', 'put']:
                    body_params = self._gen_body_param(endpoint.body_params)
                    if body_params:
                        params.append(body_params)

                methods[method.lower()] = {
                    'tags': [endpoint.group],
                    'summary': "",
                    'consumes': [
                        "application/json"
                    ],
                    'produces': [
                        "application/json"
                    ],
                    'parameters': params,
                    'responses': self._gen_responses_descriptions(method),
                    "security": [""]
                }

            if not skip:
                paths[path[len(baseUrl):]] = methods

        if not baseUrl:
            baseUrl = "/"
        spec = {
            'swagger': "2.0",
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
            'basePath': baseUrl,
            'tags': self._gen_tags(all_endpoints),
            'schemes': ["https"],
            'paths': paths
        }

        return spec

    @Endpoint(path="api.json")
    def api_json(self):
        return self._gen_spec(False, "/api")

    @Endpoint(path="api-all.json")
    def api_all_json(self):
        return self._gen_spec(True, "/api")

    @Endpoint(json_response=False)
    def __call__(self, all_endpoints=False):
        base = cherrypy.request.base
        if all_endpoints:
            spec_url = "{}/docs/api-all.json".format(base)
        else:
            spec_url = "{}/docs/api.json".format(base)
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
                }})
                window.ui = ui
            }}
        </script>
        </body>
        </html>
        """.format(spec_url)

        return page
