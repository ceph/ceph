# # -*- coding: utf-8 -*-

import unittest

from ..api.doc import SchemaType
from ..controllers import ENDPOINT_MAP, APIDoc, APIRouter, Endpoint, EndpointDoc, RESTController
from ..controllers._version import APIVersion
from ..controllers.docs import Docs
from ..tests import ControllerTestCase


# Dummy controller and endpoint that can be assigned with @EndpointDoc and @GroupDoc
@APIDoc("Group description", group="FooGroup")
@APIRouter("/doctest/", secure=False)
class DecoratedController(RESTController):
    RESOURCE_ID = 'doctest'

    @EndpointDoc(
        description="Endpoint description",
        group="BarGroup",
        parameters={
            'parameter': (int, "Description of parameter"),
        },
        responses={
            200: [{
                'my_prop': (str, '200 property desc.')
            }],
            202: {
                'my_prop': (str, '202 property desc.')
            },
        },
    )
    @Endpoint(json_response=False)
    @RESTController.Resource('PUT', version=APIVersion(0, 1))
    def decorated_func(self, parameter):
        pass

    @RESTController.MethodMap(version=APIVersion(0, 1))
    def list(self):
        pass


# To assure functionality of @EndpointDoc, @GroupDoc
class DocDecoratorsTest(ControllerTestCase):
    @classmethod
    def setup_server(cls):
        cls.setup_controllers([DecoratedController, Docs], "/test")

    def test_group_info_attr(self):
        test_ctrl = DecoratedController()
        self.assertTrue(hasattr(test_ctrl, 'doc_info'))
        self.assertIn('tag_descr', test_ctrl.doc_info)
        self.assertIn('tag', test_ctrl.doc_info)

    def test_endpoint_info_attr(self):
        test_ctrl = DecoratedController()
        test_endpoint = test_ctrl.decorated_func
        self.assertTrue(hasattr(test_endpoint, 'doc_info'))
        self.assertIn('summary', test_endpoint.doc_info)
        self.assertIn('tag', test_endpoint.doc_info)
        self.assertIn('parameters', test_endpoint.doc_info)
        self.assertIn('response', test_endpoint.doc_info)


# To assure functionality of Docs.py
# pylint: disable=protected-access
class DocsTest(ControllerTestCase):
    @classmethod
    def setup_server(cls):
        ENDPOINT_MAP.clear()
        cls.setup_controllers([DecoratedController, Docs], "/test")

    def test_type_to_str(self):
        self.assertEqual(Docs()._type_to_str(str), str(SchemaType.STRING))
        self.assertEqual(Docs()._type_to_str(int), str(SchemaType.INTEGER))
        self.assertEqual(Docs()._type_to_str(bool), str(SchemaType.BOOLEAN))
        self.assertEqual(Docs()._type_to_str(list), str(SchemaType.ARRAY))
        self.assertEqual(Docs()._type_to_str(tuple), str(SchemaType.ARRAY))
        self.assertEqual(Docs()._type_to_str(float), str(SchemaType.NUMBER))
        self.assertEqual(Docs()._type_to_str(object), str(SchemaType.OBJECT))
        self.assertEqual(Docs()._type_to_str(None), str(SchemaType.OBJECT))

    def test_gen_paths(self):
        outcome = Docs().gen_paths(False)['/api/doctest//{doctest}/decorated_func']['put']
        self.assertIn('tags', outcome)
        self.assertIn('summary', outcome)
        self.assertIn('parameters', outcome)
        self.assertIn('responses', outcome)

        expected_response_content = {
            '200': {
                APIVersion(0, 1).to_mime_type(): {
                    'schema': {'type': 'array',
                               'items': {'type': 'object', 'properties': {
                                   'my_prop': {
                                       'type': 'string',
                                       'description': '200 property desc.'}}},
                               'required': ['my_prop']}}},
            '202': {
                APIVersion(0, 1).to_mime_type(): {
                    'schema': {'type': 'object',
                               'properties': {'my_prop': {
                                   'type': 'string',
                                   'description': '202 property desc.'}},
                               'required': ['my_prop']}}
            }
        }
        # Check that a schema of type 'array' is received in the response.
        self.assertEqual(expected_response_content['200'], outcome['responses']['200']['content'])
        # Check that a schema of type 'object' is received in the response.
        self.assertEqual(expected_response_content['202'], outcome['responses']['202']['content'])

    def test_gen_method_paths(self):
        outcome = Docs().gen_paths(False)['/api/doctest/']['get']

        self.assertEqual({APIVersion(0, 1).to_mime_type(): {'type': 'object'}},
                         outcome['responses']['200']['content'])

    def test_gen_paths_all(self):
        paths = Docs().gen_paths(False)
        for key in paths:
            self.assertTrue(any(base in key.split('/')[1] for base in ['api', 'ui-api']))

    def test_gen_tags(self):
        outcome = Docs._gen_tags(False)
        self.assertEqual([{'description': 'Group description', 'name': 'FooGroup'}], outcome)


class TestEndpointDocWrapper(unittest.TestCase):
    def test_wrong_param_types(self):
        with self.assertRaises(Exception):
            EndpointDoc(description=False)
        with self.assertRaises(Exception):
            EndpointDoc(group=False)
        with self.assertRaises(Exception):
            EndpointDoc(parameters='wrong parameters')
        with self.assertRaises(Exception):
            EndpointDoc(responses='wrong response')

        def dummy_func():
            pass
        with self.assertRaises(Exception):
            EndpointDoc(parameters={'parameter': 'wrong parameter'})(dummy_func)

    def test_split_dict(self):
        edoc = EndpointDoc()
        data = {
            'name1': (int, 'description1'),
            'dict_param': ({'name2': (int, 'description2')}, 'description_dict'),
            'list_param': ([int, float], 'description_list')
        }
        expected = [
            {
                'name': 'name1',
                'description': 'description1',
                'required': True,
                'nested': False,
                'type': int
            },
            {
                'name': 'dict_param',
                'description': 'description_dict',
                'required': True,
                'nested': False,
                'type': dict,
                'nested_params': [
                    {
                        'name': 'name2',
                        'description': 'description2',
                        'required': True,
                        'nested': True,
                        'type': int
                    }
                ]
            },
            {
                'name': 'list_param',
                'description':
                'description_list',
                'required': True,
                'nested': False,
                'type': [int, float]
            }
        ]

        res = edoc._split_dict(data, False)
        self.assertEqual(res, expected)

    def test_split_param(self):
        edoc = EndpointDoc()
        name = 'foo'
        p_type = int
        description = 'description'
        default_value = 1
        expected = {
            'name': name,
            'description': description,
            'required': True,
            'nested': False,
            'default': default_value,
            'type': p_type,
        }
        res = edoc._split_param(name, p_type, description, default_value=default_value)
        self.assertEqual(res, expected)

    def test_split_param_nested(self):
        edoc = EndpointDoc()
        name = 'foo'
        p_type = {'name2': (int, 'description2')}, 'description_dict'
        description = 'description'
        default_value = 1
        expected = {
            'name': name,
            'description': description,
            'required': True,
            'nested': True,
            'default': default_value,
            'type': type(p_type),
            'nested_params': [
                {
                    'name': 'name2',
                    'description': 'description2',
                    'required': True,
                    'nested': True,
                    'type': int
                }
            ]
        }
        res = edoc._split_param(name, p_type, description, default_value=default_value,
                                nested=True)
        self.assertEqual(res, expected)

    def test_split_list(self):
        edoc = EndpointDoc()
        data = [('foo', int), ('foo', float)]
        expected = []

        res = edoc._split_list(data, True)

        self.assertEqual(res, expected)
