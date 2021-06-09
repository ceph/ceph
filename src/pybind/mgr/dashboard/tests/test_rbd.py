# -*- coding: utf-8 -*-
from __future__ import absolute_import

from datetime import datetime

import unittest
try:
    import mock
except ImportError:
    import unittest.mock as mock
from mock import MagicMock

from .. import mgr
from ..controllers.rbd import Rbd


class ImageNotFoundStub(Exception):
    def __init__(self, message, errno=None):
        super(ImageNotFoundStub, self).__init__(
            'RBD image not found (%s)' % message, errno)


class RbdTest(unittest.TestCase):

    @mock.patch('dashboard.controllers.rbd.rbd.RBD')
    def test_rbd_image_removing(self, rbd_mock):
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
        rbd = Rbd._rbd_image_removing(ioctx_mock, 'test_pool', '3c1a5ee60a88')
        self.assertEqual(rbd, {
            'id': '3c1a5ee60a88',
            'unique_id': 'test_pool/3c1a5ee60a88',
            'name': 'test_rbd',
            'source': 'REMOVING',
            'deletion_time': '{}Z'.format(time.isoformat()),
            'deferment_end_time': '{}Z'.format(time.isoformat()),
            'pool_name': 'test_pool'
        })

    @mock.patch('dashboard.controllers.rbd.rbd.ImageNotFound',
                new_callable=lambda: ImageNotFoundStub)
    @mock.patch('dashboard.controllers.rbd.rbd.RBD')
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
            Rbd._rbd_image_removing(ioctx_mock, 'test_pool', '3c1a5ee60a88')
        self.assertIn('No image test_pool/3c1a5ee60a88 in status `REMOVING` found.',
                      str(ctx.exception))

    @mock.patch('dashboard.controllers.rbd.rbd.ImageNotFound',
                new_callable=lambda: ImageNotFoundStub)
    @mock.patch('dashboard.controllers.rbd.Rbd._rbd_image_removing')
    @mock.patch('dashboard.controllers.rbd.Rbd._rbd_image')
    @mock.patch('dashboard.controllers.rbd.rbd.RBD')
    def test_rbd_pool_list(self, rbd_mock, rbd_image_mock, rbd_image_removing_mock, _):
        time = datetime.utcnow()

        ioctx_mock = MagicMock()
        mgr.rados = MagicMock()
        mgr.rados.open_ioctx.return_value = ioctx_mock

        rbd_inst_mock = rbd_mock.return_value
        rbd_inst_mock.list2.return_value = [{'name': 'test_rbd', 'id': '3c1a5ee60a88'}]

        rbd_image_mock.side_effect = mock.Mock(side_effect=ImageNotFoundStub(
            'RBD image not found test_pool/3c1a5ee60a88'))

        rbd_image_removing_mock.return_value = {
            'id': '3c1a5ee60a88',
            'unique_id': 'test_pool/3c1a5ee60a88',
            'name': 'test_rbd',
            'source': 'REMOVING',
            'deletion_time': '{}Z'.format(time.isoformat()),
            'deferment_end_time': '{}Z'.format(time.isoformat()),
            'pool_name': 'test_pool'
        }

        # pylint: disable=protected-access
        rbd_pool_list = Rbd._rbd_pool_list('test_pool')
        self.assertEqual(rbd_pool_list, (0, [{
            'id': '3c1a5ee60a88',
            'unique_id': 'test_pool/3c1a5ee60a88',
            'name': 'test_rbd',
            'source': 'REMOVING',
            'deletion_time': '{}Z'.format(time.isoformat()),
            'deferment_end_time': '{}Z'.format(time.isoformat()),
            'pool_name': 'test_pool'
        }]))
