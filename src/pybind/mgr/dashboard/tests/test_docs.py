# # -*- coding: utf-8 -*-
from __future__ import absolute_import

from . import ControllerTestCase
from ..controllers import RESTController, ApiController, Endpoint, EndpointDoc, ControllerDoc
from ..controllers.docs import Docs


# Dummy controller and endpoint that can be assigned with @EndpointDoc and @GroupDoc
@ControllerDoc("Group description", group="FooGroup")
@ApiController("/doctest/", secure=False)
class DecoratedController(RESTController):
    @EndpointDoc(
        description="Endpoint description",
        group="BarGroup",
        parameters={
            'parameter': (int, "Description of parameter"),
        },
        responses={
            200: {
                'resp': (str, 'Description of response')
            },
        },
    )
    @Endpoint(json_response=False)
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
        cls.setup_controllers([Docs], "/test")

    def test_type_to_str(self):
        self.assertEqual(Docs()._type_to_str(str), "string")

    def test_gen_paths(self):
        outcome = Docs()._gen_paths(False, "")['/api/doctest//decorated_func/{parameter}']['get']
        self.assertIn('tags', outcome)
        self.assertIn('summary', outcome)
        self.assertIn('parameters', outcome)
        self.assertIn('responses', outcome)

    def test_gen_tags(self):
        outcome = Docs()._gen_tags(False)[0]
        self.assertEqual({'description': 'Group description', 'name': 'FooGroup'}, outcome)
