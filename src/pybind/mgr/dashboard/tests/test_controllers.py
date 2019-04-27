# -*- coding: utf-8 -*-
from __future__ import absolute_import

from . import ControllerTestCase
from ..controllers import BaseController, RESTController, Controller, \
                          ApiController, Endpoint


@Controller("/btest/{key}", base_url="/ui", secure=False)
class BTest(BaseController):
    @Endpoint()
    def test1(self, key, opt=1):
        return {'key': key, 'opt': opt}

    @Endpoint()
    def test2(self, key, skey, opt=1):
        return {'key': key, 'skey': skey, 'opt': opt}

    @Endpoint(path="/foo/{skey}/test-3")
    def test3(self, key, skey, opt=1):
        return {'key': key, 'skey': skey, 'opt': opt}

    @Endpoint('POST', path="/foo/{skey}/test-3", query_params=['opt'])
    def test4(self, key, skey, data, opt=1):
        return {'key': key, 'skey': skey, 'data': data, 'opt': opt}

    @Endpoint('PUT', path_params=['skey'], query_params=['opt'])
    def test5(self, key, skey, data1, data2=None, opt=1):
        return {'key': key, 'skey': skey, 'data1': data1, 'data2': data2,
                'opt': opt}

    @Endpoint('GET', json_response=False)
    def test6(self, key, opt=1):
        return "My Formatted string key={} opt={}".format(key, opt)

    @Endpoint()
    def __call__(self, key, opt=1):
        return {'key': key, 'opt': opt}


@ApiController("/rtest/{key}", secure=False)
class RTest(RESTController):
    RESOURCE_ID = 'skey/ekey'

    def list(self, key, opt=1):
        return {'key': key, 'opt': opt}

    def create(self, key, data1, data2=None):
        return {'key': key, 'data1': data1, 'data2': data2}

    def get(self, key, skey, ekey, opt=1):
        return {'key': key, 'skey': skey, 'ekey': ekey, 'opt': opt}

    def set(self, key, skey, ekey, data):
        return {'key': key, 'skey': skey, 'ekey': ekey, 'data': data}

    def delete(self, key, skey, ekey, opt=1):
        pass

    def bulk_set(self, key, data1, data2=None):
        return {'key': key, 'data1': data1, 'data2': data2}

    def bulk_delete(self, key, opt=1):
        pass

    @RESTController.Collection('POST')
    def cmethod(self, key, data):
        return {'key': key, 'data': data}

    @RESTController.Resource('GET')
    def rmethod(self, key, skey, ekey, opt=1):
        return {'key': key, 'skey': skey, 'ekey': ekey, 'opt': opt}


@Controller("/", secure=False)
class Root(BaseController):
    @Endpoint(json_response=False)
    def __call__(self):
        return "<html></html>"


class ControllersTest(ControllerTestCase):
    @classmethod
    def setup_server(cls):
        cls.setup_controllers([BTest, RTest], "/test")

    def test_1(self):
        self._get('/test/ui/btest/{}/test1?opt=3'.format(100))
        self.assertStatus(200)
        self.assertJsonBody({'key': '100', 'opt': '3'})

    def test_2(self):
        self._get('/test/ui/btest/{}/test2/{}?opt=3'.format(100, 200))
        self.assertStatus(200)
        self.assertJsonBody({'key': '100', 'skey': '200', 'opt': '3'})

    def test_3(self):
        self._get('/test/ui/btest/{}/foo/{}/test-3?opt=3'.format(100, 200))
        self.assertStatus(200)
        self.assertJsonBody({'key': '100', 'skey': '200', 'opt': '3'})

    def test_4(self):
        self._post('/test/ui/btest/{}/foo/{}/test-3?opt=3'.format(100, 200),
                   {'data': 30})
        self.assertStatus(200)
        self.assertJsonBody({'key': '100', 'skey': '200', 'data': 30,
                             'opt': '3'})

    def test_5(self):
        self._put('/test/ui/btest/{}/test5/{}?opt=3'.format(100, 200),
                  {'data1': 40, 'data2': "hello"})
        self.assertStatus(200)
        self.assertJsonBody({'key': '100', 'skey': '200', 'data1': 40,
                             'data2': "hello", 'opt': '3'})

    def test_6(self):
        self._get('/test/ui/btest/{}/test6'.format(100))
        self.assertStatus(200)
        self.assertBody("My Formatted string key=100 opt=1")

    def test_7(self):
        self._get('/test/ui/btest/{}?opt=3'.format(100))
        self.assertStatus(200)
        self.assertJsonBody({'key': '100', 'opt': '3'})

    def test_rest_list(self):
        self._get('/test/api/rtest/{}?opt=2'.format(300))
        self.assertStatus(200)
        self.assertJsonBody({'key': '300', 'opt': '2'})

    def test_rest_create(self):
        self._post('/test/api/rtest/{}'.format(300),
                   {'data1': 20, 'data2': True})
        self.assertStatus(201)
        self.assertJsonBody({'key': '300', 'data1': 20, 'data2': True})

    def test_rest_get(self):
        self._get('/test/api/rtest/{}/{}/{}?opt=3'.format(300, 1, 2))
        self.assertStatus(200)
        self.assertJsonBody({'key': '300', 'skey': '1', 'ekey': '2',
                             'opt': '3'})

    def test_rest_set(self):
        self._put('/test/api/rtest/{}/{}/{}'.format(300, 1, 2),
                  {'data': 40})
        self.assertStatus(200)
        self.assertJsonBody({'key': '300', 'skey': '1', 'ekey': '2',
                             'data': 40})

    def test_rest_delete(self):
        self._delete('/test/api/rtest/{}/{}/{}?opt=3'.format(300, 1, 2))
        self.assertStatus(204)

    def test_rest_bulk_set(self):
        self._put('/test/api/rtest/{}'.format(300),
                  {'data1': 20, 'data2': True})
        self.assertStatus(200)
        self.assertJsonBody({'key': '300', 'data1': 20, 'data2': True})

        self._put('/test/api/rtest/{}'.format(400),
                  {'data1': 20, 'data2': ['one', 'two', 'three']})
        self.assertStatus(200)
        self.assertJsonBody({
            'key': '400',
            'data1': 20,
            'data2': ['one', 'two', 'three'],
        })

    def test_rest_bulk_delete(self):
        self._delete('/test/api/rtest/{}?opt=2'.format(300))
        self.assertStatus(204)

    def test_rest_collection(self):
        self._post('/test/api/rtest/{}/cmethod'.format(300), {'data': 30})
        self.assertStatus(200)
        self.assertJsonBody({'key': '300', 'data': 30})

    def test_rest_resource(self):
        self._get('/test/api/rtest/{}/{}/{}/rmethod?opt=4'.format(300, 2, 3))
        self.assertStatus(200)
        self.assertJsonBody({'key': '300', 'skey': '2', 'ekey': '3',
                             'opt': '4'})


class RootControllerTest(ControllerTestCase):
    @classmethod
    def setup_server(cls):
        cls.setup_controllers([Root])

    def test_index(self):
        self._get("/")
        self.assertBody("<html></html>")
