from unittest import TestCase
from webtest import TestApp
from ceph_brag.tests import FunctionalTest
import sys

class TestRootController(FunctionalTest):

    def test_get(self):
        response = self.app.get('/?k1=v1&k2=v2')
        print >> sys.stderr, "Vars of response = " + str(vars(response))
        assert response.status_int == 200

    def test_search(self):
        response = self.app.post('/', params={'q': 'RestController'})
        assert response.status_int == 302
        assert response.headers['Location'] == (
            'http://pecan.readthedocs.org/en/latest/search.html'
            '?q=RestController'
        )

    def test_get_not_found(self):
        response = self.app.get('/a/bogus/url', expect_errors=True)
        assert response.status_int == 404
