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
      
    @mock.patch.object(RbdMirroringService, 'get_schedule_interval')
    @mock.patch.object(RbdMirroringService, 'get_schedule_time_for_image')
    @mock.patch.object(RbdMirroringService, 'get_snapshot_schedule_info')
    def test_rbd_image_inherited_schedule(self, mock_get_info, mock_get_time, mock_get_interval):
        mock_get_info.return_value = None
        mock_get_time.return_value = '2026-02-26T12:00:00Z'
        mock_get_interval.side_effect = lambda spec: [{'schedule': '1d'}] if spec == 'pool1/' else None

        with mock.patch('dashboard.services.rbd.rbd.Image') as mock_rbd_image, \
             mock.patch('dashboard.services.rbd.CephService.get_pool_name_from_id') as mock_pool_id, \
             mock.patch('dashboard.services.rbd.RbdConfiguration') as mock_config_cls, \
             mock.patch('dashboard.services.rbd.RbdImageMetadataService') as mock_meta_cls:
            
            mock_pool_id.return_value = 'pool1'
            mock_config_cls.return_value.list.return_value = []
            mock_meta_cls.return_value.list.return_value = {}

            img_instance = mock_rbd_image.return_value.__enter__.return_value
            img_instance.stat.return_value = {'block_name_prefix': 'rbd_id.123'}
            img_instance.mirror_image_get_info.return_value = {'state': rbd.RBD_MIRROR_IMAGE_ENABLED, 'primary': True}
            img_instance.mirror_image_get_mode.return_value = 2
            img_instance.create_timestamp.return_value = datetime.now()
            img_instance.stripe_count.return_value = 1
            img_instance.stripe_unit.return_value = 4194304
            img_instance.old_format.return_value = False
            img_instance.id.return_value = '123'
            img_instance.features.return_value = 0
            img_instance.list_snaps.return_value = []
            img_instance.data_pool_id.return_value = 1

            ioctx_mock = mock.Mock()
            result = RbdService._rbd_image(ioctx_mock, 'pool1', None, 'image1')

            schedule_info = result.get('schedule_info')
            self.assertEqual(schedule_info['name'], 'pool1/image1')
            self.assertEqual(schedule_info['schedule_source'], 'pool1/')
            self.assertEqual(schedule_info['inherited'], 'pool')
