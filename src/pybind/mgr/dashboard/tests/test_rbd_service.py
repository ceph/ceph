# -*- coding: utf-8 -*-
# pylint: disable=dangerous-default-value,too-many-public-methods
from __future__ import absolute_import

import unittest
from datetime import datetime
from unittest.mock import MagicMock

try:
    import mock
except ImportError:
    import unittest.mock as mock

from .. import mgr
from ..services.rbd import RbdConfiguration, RBDSchedulerInterval, RbdService, \
    get_image_spec, parse_image_spec


class ImageNotFoundStub(Exception):
    def __init__(self, message, errno=None):
        super(ImageNotFoundStub, self).__init__(
            'RBD image not found (%s)' % message, errno)


class RbdServiceTest(unittest.TestCase):

    def test_compose_image_spec(self):
        self.assertEqual(get_image_spec('mypool', 'myns', 'myimage'), 'mypool/myns/myimage')
        self.assertEqual(get_image_spec('mypool', None, 'myimage'), 'mypool/myimage')

    def test_parse_image_spec(self):
        self.assertEqual(parse_image_spec('mypool/myns/myimage'), ('mypool', 'myns', 'myimage'))
        self.assertEqual(parse_image_spec('mypool/myimage'), ('mypool', None, 'myimage'))

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
                '2': {'creating+incomplete': 32},
            }
        }
        config_list.return_value = [1, 2, 3]
        config = RbdConfiguration('bad-pool')
        self.assertEqual(config.list(), [])
        config = RbdConfiguration('good-pool')
        self.assertEqual(config.list(), [1, 2, 3])

    @mock.patch('dashboard.services.rbd.rbd.RBD')
    def test_rbd_image_stat_removing(self, rbd_mock):
        time = datetime.utcnow()
        rbd_inst_mock = rbd_mock.return_value
        rbd_inst_mock.trash_get.return_value = {
            'id': '3c1a5ee60a88',
            'name': 'test_rbd',
            'source': 'REMOVING',
            'deletion_time': time,
            'deferment_end_time': time
        }

        ioctx_mock = MagicMock()

        # pylint: disable=protected-access
        rbd = RbdService._rbd_image_stat_removing(ioctx_mock, 'test_pool', '', '3c1a5ee60a88')
        self.assertEqual(rbd, {
            'id': '3c1a5ee60a88',
            'unique_id': 'test_pool/3c1a5ee60a88',
            'name': 'test_rbd',
            'source': 'REMOVING',
            'deletion_time': '{}Z'.format(time.isoformat()),
            'deferment_end_time': '{}Z'.format(time.isoformat()),
            'pool_name': 'test_pool',
            'namespace': ''
        })

    @mock.patch('dashboard.services.rbd.rbd.ImageNotFound', new_callable=lambda: ImageNotFoundStub)
    @mock.patch('dashboard.services.rbd.rbd.RBD')
    def test_rbd_image_stat_filter_source_user(self, rbd_mock, _):
        rbd_inst_mock = rbd_mock.return_value
        rbd_inst_mock.trash_get.return_value = {
            'id': '3c1a5ee60a88',
            'name': 'test_rbd',
            'source': 'USER'
        }

        ioctx_mock = MagicMock()
        with self.assertRaises(ImageNotFoundStub) as ctx:
            # pylint: disable=protected-access
            RbdService._rbd_image_stat_removing(ioctx_mock, 'test_pool', '', '3c1a5ee60a88')
        self.assertIn('No image test_pool/3c1a5ee60a88 in status `REMOVING` found.',
                      str(ctx.exception))

    @mock.patch('dashboard.services.rbd.rbd.ImageNotFound', new_callable=lambda: ImageNotFoundStub)
    @mock.patch('dashboard.services.rbd.RbdService._rbd_image_stat_removing')
    @mock.patch('dashboard.services.rbd.RbdService._rbd_image_stat')
    @mock.patch('dashboard.services.rbd.RbdService._rbd_image_refs')
    @mock.patch('dashboard.services.rbd.rbd.RBD')
    def test_rbd_pool_list(self, rbd_mock, rbd_image_ref_mock, rbd_image_stat_mock,
                           rbd_image_stat_removing_mock, _):
        time = datetime.utcnow()

        ioctx_mock = MagicMock()
        mgr.rados = MagicMock()
        mgr.rados.open_ioctx.return_value = ioctx_mock

        rbd_inst_mock = rbd_mock.return_value
        rbd_inst_mock.namespace_list.return_value = []
        rbd_image_ref_mock.return_value = [{'name': 'test_rbd', 'id': '3c1a5ee60a88'}]

        rbd_image_stat_mock.side_effect = mock.Mock(side_effect=ImageNotFoundStub(
            'RBD image not found test_pool/3c1a5ee60a88'))

        rbd_image_stat_removing_mock.return_value = {
            'id': '3c1a5ee60a88',
            'unique_id': 'test_pool/3c1a5ee60a88',
            'name': 'test_rbd',
            'source': 'REMOVING',
            'deletion_time': '{}Z'.format(time.isoformat()),
            'deferment_end_time': '{}Z'.format(time.isoformat()),
            'pool_name': 'test_pool',
            'namespace': ''
        }

        rbd_pool_list = RbdService.rbd_pool_list(['test_pool'], offset=0, limit=5)
        self.assertEqual(rbd_pool_list, ([{
            'id': '3c1a5ee60a88',
            'unique_id': 'test_pool/3c1a5ee60a88',
            'name': 'test_rbd',
            'source': 'REMOVING',
            'deletion_time': '{}Z'.format(time.isoformat()),
            'deferment_end_time': '{}Z'.format(time.isoformat()),
            'pool_name': 'test_pool',
            'namespace': ''
        }], 1))

    def test_valid_interval(self):
        test_cases = [
            ('15m', False),
            ('1h', False),
            ('5d', False),
            ('m', True),
            ('d', True),
            ('1s', True),
            ('11', True),
            ('1m1', True),
        ]
        for interval, error in test_cases:
            if error:
                with self.assertRaises(ValueError):
                    RBDSchedulerInterval(interval)
            else:
                self.assertEqual(str(RBDSchedulerInterval(interval)), interval)
