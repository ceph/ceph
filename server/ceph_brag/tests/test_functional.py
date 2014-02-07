from unittest import TestCase
from webtest import TestApp
from ceph_brag.tests import FunctionalTest
import sys
from pecan import request

class TestRootController(FunctionalTest):

    def test_get(self):
        response = self.app.get('/?k1=v1&k2=v2')
        assert response.status_int == 200

    def test_put(self):
        #response = self.app.put('/', upload_files=[("uploadfield", "/tmp/sample.json")])
        with open ("/tmp/sample.json", "r") as myfile:
            data=myfile.read().replace('\n', '')
        response = self.app.request('/', method='PUT', body=data)
        assert response.status_int == 302

    def test_get_not_found(self):
        response = self.app.get('/a/bogus/url', expect_errors=True)
        assert response.status_int == 404
