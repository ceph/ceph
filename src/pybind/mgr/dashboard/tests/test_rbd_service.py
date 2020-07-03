# -*- coding: utf-8 -*-
# pylint: disable=dangerous-default-value,too-many-public-methods
from __future__ import absolute_import

import unittest
try:
    import mock
except ImportError:
    import unittest.mock as mock

from ..services.rbd import RbdConfiguration


class RbdServiceTest(unittest.TestCase):

    @mock.patch('dashboard.services.rbd.RbdConfiguration._rbd.config_list')
    @mock.patch('dashboard.mgr.get')
    @mock.patch('dashboard.services.ceph_service.CephService.get_pool_list')
    def test_pool_rbd_configuration_with_different_pg_states(self, get_pool_list, get, config_list):
        get_pool_list.return_value = [{
            'pool_name': 'good-pool',
            'pool': 1,
        }, {
            'pool_name': 'bad-pool',
            'pool': 2,
        }]
        get.return_value = {
            'by_pool': {
                '1': {'active+clean': 32},
                '2': {'unknown': 32},
            }
        }
        config_list.return_value = [1, 2, 3]
        config = RbdConfiguration('bad-pool')
        self.assertEqual(config.list(), [])
        config = RbdConfiguration('good-pool')
        self.assertEqual(config.list(), [1, 2, 3])
