# -*- coding: utf-8 -*-
# pylint: disable=dangerous-default-value,too-many-public-methods
from __future__ import absolute_import

import unittest
try:
    import mock
except ImportError:
    import unittest.mock as mock

from ..services.ceph_service import CephService


class CephServiceTest(unittest.TestCase):
    pools = [{
        'pool_name': 'good_pool',
        'pool': 1,
    }, {
        'pool_name': 'bad_pool',
        'pool': 2,
        'flaky': 'option_x'
    }]

    def setUp(self):
        #  Mock get_pool_list
        self.list_patch = mock.patch('dashboard.services.ceph_service.CephService.get_pool_list')
        self.list = self.list_patch.start()
        self.list.return_value = self.pools
        #  Mock mgr.get
        self.mgr_patch = mock.patch('dashboard.mgr.get')
        self.mgr = self.mgr_patch.start()
        self.mgr.return_value = {
            'by_pool': {
                '1': {'active+clean': 16},
                '2': {'creating+incomplete': 16},
            }
        }
        self.service = CephService()

    def tearDown(self):
        self.list_patch.stop()
        self.mgr_patch.stop()

    def test_get_pool_by_attribute_with_match(self):
        self.assertEqual(self.service.get_pool_by_attribute('pool', 1), self.pools[0])
        self.assertEqual(self.service.get_pool_by_attribute('pool_name', 'bad_pool'), self.pools[1])

    def test_get_pool_by_attribute_without_a_match(self):
        self.assertEqual(self.service.get_pool_by_attribute('pool', 3), None)
        self.assertEqual(self.service.get_pool_by_attribute('not_there', 'sth'), None)

    def test_get_pool_by_attribute_matching_a_not_always_set_attribute(self):
        self.assertEqual(self.service.get_pool_by_attribute('flaky', 'option_x'), self.pools[1])

    @mock.patch('dashboard.mgr.rados.pool_reverse_lookup', return_value='good_pool')
    def test_get_pool_name_from_id_with_match(self, _mock):
        self.assertEqual(self.service.get_pool_name_from_id(1), 'good_pool')

    @mock.patch('dashboard.mgr.rados.pool_reverse_lookup', return_value=None)
    def test_get_pool_name_from_id_without_match(self, _mock):
        self.assertEqual(self.service.get_pool_name_from_id(3), None)

    def test_get_pool_pg_status(self):
        self.assertEqual(self.service.get_pool_pg_status('good_pool'), {'active+clean': 16})

    def test_get_pg_status_without_match(self):
        self.assertEqual(self.service.get_pool_pg_status('no-pool'), {})
