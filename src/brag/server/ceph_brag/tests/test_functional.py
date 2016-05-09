from unittest import TestCase
from webtest import TestApp
from ceph_brag.tests import FunctionalTest
import json, sys
from pecan import request

class TestRootController(FunctionalTest):
    def test_1_get_invalid_url_format(self):
        response = self.app.get('/1/2/3', expect_errors=True)
        assert response.status_int == 400

    def test_2_put(self):
        with open ("sample.json", "r") as myfile:
            data=myfile.read().replace('\n', '')
        response = self.app.request('/', method='PUT', body=data)
        assert response.status_int == 201

    def test_3_put_invalid_json(self):
        response = self.app.request('/', method='PUT', body='{asdfg', expect_errors=True)
        assert response.status_int == 422

    def test_4_put_invalid_entries_1(self):
        response = self.app.request('/', method='PUT', body='{}', expect_errors=True)
        assert response.status_int == 422

    def test_5_put_incomplete_json(self):
        response = self.app.request('/', method='PUT', body='{\"uuid\":\"adfs-12312ad\"}',
                                    expect_errors=True)
        assert response.status_int == 422

    def test_6_get(self):
        response = self.app.get('/')
        js = json.loads(response.body)
        for entry in js:
            ci = entry
            break

        response = self.app.get('/'+ci['uuid']+'/'+str(ci['num_versions']))
        assert response.status_int == 200

    def test_7_get_invalid_uuid(self):
        response = self.app.get('/xxxxxx', expect_errors=True)
        assert response.status_int == 400

    def test_8_get_invalid_version(self):
        response = self.app.get('/')
        js = json.loads(response.body)
        for entry in js:
            ci = entry
            break

        response = self.app.get('/'+ci['uuid']+'/'+str(0), expect_errors=True)
        assert response.status_int == 400

    def test_9_delete_invalid_parameters(self):
        response = self.app.delete('/', expect_errors=True)
        assert response.status_int == 400

    def test_91_delete_wrong_uuid(self):
        response = self.app.delete('/?uuid=xxxx', expect_errors=True)
        assert response.status_int == 400

    def test_92_delete(self):
        response = self.app.get('/')
        js = json.loads(response.body)
        for entry in js:
            response = self.app.delete('/?uuid='+entry['uuid'])
            assert response.status_int == 200
