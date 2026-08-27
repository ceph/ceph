# -*- coding: utf-8 -*-
# pylint: disable=dangerous-default-value,too-many-public-methods

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

    def setUp(self):
        # pylint: disable=protected-access
        RbdService._rbd_inst = mock.Mock()
        self.rbd_inst_mock = RbdService._rbd_inst

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

    def test_rbd_image_stat_removing(self):
        time = datetime.utcnow()
        self.rbd_inst_mock.trash_get.return_value = {
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
            'deletion_time': time.isoformat(),
            'deferment_end_time': time.isoformat(),
            'pool_name': 'test_pool',
            'namespace': ''
        })

    @mock.patch('dashboard.services.rbd.rbd.ImageNotFound', new_callable=lambda: ImageNotFoundStub)
    def test_rbd_image_stat_filter_source_user(self, _):
        self.rbd_inst_mock.trash_get.return_value = {
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
    @mock.patch('dashboard.services.rbd.RbdService._pool_namespaces')
    @mock.patch('dashboard.services.rbd.RbdService._rbd_image_stat_removing')
    @mock.patch('dashboard.services.rbd.RbdService._rbd_image_stat')
    @mock.patch('dashboard.services.rbd.RbdService._rbd_image_refs')
    def test_rbd_pool_list(self, rbd_image_ref_mock, rbd_image_stat_mock,
                           rbd_image_stat_removing_mock, pool_namespaces, _):
        time = datetime.utcnow()

        ioctx_mock = MagicMock()
        mgr.rados = MagicMock()
        mgr.rados.open_ioctx.return_value = ioctx_mock

        self.rbd_inst_mock.namespace_list.return_value = []
        rbd_image_ref_mock.return_value = [{'name': 'test_rbd', 'id': '3c1a5ee60a88'}]
        pool_namespaces.return_value = ['']

        rbd_image_stat_mock.side_effect = mock.Mock(side_effect=ImageNotFoundStub(
            'RBD image not found test_pool/3c1a5ee60a88'))

        rbd_image_stat_removing_mock.return_value = {
            'id': '3c1a5ee60a88',
            'unique_id': 'test_pool/3c1a5ee60a88',
            'name': 'test_rbd',
            'source': 'REMOVING',
            'deletion_time': time.isoformat(),
            'deferment_end_time': time.isoformat(),
            'pool_name': 'test_pool',
            'namespace': ''
        }

        # test with limit 0, it should return a list of pools with an empty list, but
        rbd_pool_list = RbdService.rbd_pool_list(['test_pool'], offset=0, limit=0)
        self.assertEqual(rbd_pool_list, ([], 1))

        self.rbd_inst_mock.namespace_list.return_value = []

        rbd_pool_list = RbdService.rbd_pool_list(['test_pool'], offset=0, limit=5)
        self.assertEqual(rbd_pool_list, ([{
            'id': '3c1a5ee60a88',
            'unique_id': 'test_pool/3c1a5ee60a88',
            'name': 'test_rbd',
            'source': 'REMOVING',
            'deletion_time': time.isoformat(),
            'deferment_end_time': time.isoformat(),
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

    def test_rbd_image_refs_cache(self):
        ioctx_mock = MagicMock()
        mgr.rados = MagicMock()
        mgr.rados.open_ioctx.return_value = ioctx_mock
        images = [{'image': str(i), 'id': str(i)} for i in range(10)]
        for i in range(5):
            self.rbd_inst_mock.list2.return_value = images[i*2:(i*2)+2]
            ioctx_mock = MagicMock()
            # pylint: disable=protected-access
            res = RbdService._rbd_image_refs(ioctx_mock, str(i))
            self.assertEqual(res, images[i*2:(i*2)+2])

    @mock.patch('dashboard.services.rbd.rbd.Image')
    def test_image_diff(self, rbd_image_mock):
        mgr.rados = MagicMock()
        img = MagicMock()
        img.size.return_value = 64 * 1024 * 1024

        def _diff_iterate(offset, length, from_snap, callback, whole_object=False):
            callback(0, 4194304, True)
            callback(33554432, 4194304, True)
        img.diff_iterate.side_effect = _diff_iterate
        rbd_image_mock.return_value.__enter__.return_value = img

        result = RbdService.image_diff('rbd/img', from_snapshot='snap1',
                                       snapshot_name='snap2', whole_object=True)

        # the target snapshot is opened read-only
        self.assertEqual(rbd_image_mock.call_args[0][1], 'img')
        self.assertEqual(rbd_image_mock.call_args[1],
                         {'snapshot': 'snap2', 'read_only': True})
        # the whole image is walked from the source snapshot at object granularity
        diff_args = img.diff_iterate.call_args
        self.assertEqual(diff_args[0][0], 0)
        self.assertEqual(diff_args[0][1], 64 * 1024 * 1024)
        self.assertEqual(diff_args[0][2], 'snap1')
        self.assertTrue(diff_args[1]['whole_object'])
        self.assertEqual(result, {
            'image_size': 64 * 1024 * 1024,
            'offset': 0,
            'length': 64 * 1024 * 1024,
            'from_snapshot': 'snap1',
            'snapshot_name': 'snap2',
            'whole_object': True,
            'count': 2,
            'diffs': [
                {'offset': 0, 'length': 4194304, 'exists': True},
                {'offset': 33554432, 'length': 4194304, 'exists': True},
            ],
        })

    @mock.patch('dashboard.services.rbd.rbd.Image')
    def test_image_diff_length_is_clamped_to_image_size(self, rbd_image_mock):
        mgr.rados = MagicMock()
        img = MagicMock()
        img.size.return_value = 16 * 1024 * 1024
        img.diff_iterate.side_effect = lambda *a, **k: None
        rbd_image_mock.return_value.__enter__.return_value = img

        # ask for more than the image holds, starting at a non-zero offset
        result = RbdService.image_diff('rbd/img', offset=8 * 1024 * 1024,
                                       length=999 * 1024 * 1024)

        self.assertEqual(result['offset'], 8 * 1024 * 1024)
        self.assertEqual(result['length'], 8 * 1024 * 1024)  # clamped to size - offset
        self.assertEqual(img.diff_iterate.call_args[0][1], 8 * 1024 * 1024)
        self.assertIsNone(result['from_snapshot'])
        self.assertIsNone(result['snapshot_name'])
