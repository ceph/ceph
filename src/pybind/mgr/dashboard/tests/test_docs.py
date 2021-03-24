# # -*- coding: utf-8 -*-
from __future__ import absolute_import

from .. import DEFAULT_VERSION
from ..api.doc import SchemaType
from ..controllers import ApiController, ControllerDoc, Endpoint, EndpointDoc, RESTController
from ..controllers.docs import Docs
from . import ControllerTestCase  # pylint: disable=no-name-in-module


# Dummy controller and endpoint that can be assigned with @EndpointDoc and @GroupDoc
@ControllerDoc("Group description", group="FooGroup")
@ApiController("/doctest/", secure=False)
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
    @RESTController.Resource('PUT')
    def decorated_func(self, parameter):
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
        outcome = Docs()._gen_paths(False)['/api/doctest//{doctest}/decorated_func']['put']
        self.assertIn('tags', outcome)
        self.assertIn('summary', outcome)
        self.assertIn('parameters', outcome)
        self.assertIn('responses', outcome)

        expected_response_content = {
            '200': {
                'application/vnd.ceph.api.v{}+json'.format(DEFAULT_VERSION): {
                    'schema': {'type': 'array',
                               'items': {'type': 'object', 'properties': {
                                   'my_prop': {
                                       'type': 'string',
                                       'description': '200 property desc.'}}},
                               'required': ['my_prop']}}},
            '202': {
                'application/vnd.ceph.api.v{}+json'.format(DEFAULT_VERSION): {
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

    def test_gen_paths_all(self):
        paths = Docs()._gen_paths(False)
        for key in paths:
            self.assertTrue(any(base in key.split('/')[1] for base in ['api', 'ui-api']))

    def test_gen_tags(self):
        outcome = Docs()._gen_tags(False)[0]
        self.assertEqual({'description': 'Group description', 'name': 'FooGroup'}, outcome)
