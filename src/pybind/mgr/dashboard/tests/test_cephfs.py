# -*- coding: utf-8 -*-
from collections import defaultdict
try:
    from mock import Mock
except ImportError:
    from unittest.mock import Mock

from .. import mgr
from . import ControllerTestCase
from ..controllers.cephfs import CephFS


class MetaDataMock(object):
    def get(self, _x, _y):
        return 'bar'


def get_metadata_mock(key, meta_key):
    return {
        'mds': {
            None: None,  # Unknown key
            'foo': MetaDataMock()
        }[meta_key]
    }[key]


class CephFsTest(ControllerTestCase):
    cephFs = CephFS()

    @classmethod
    def setup_server(cls):
        mgr.get_metadata = Mock(side_effect=get_metadata_mock)

    def tearDown(self):
        mgr.get_metadata.stop()

    def test_append_of_mds_metadata_if_key_is_not_found(self):
        mds_versions = defaultdict(list)
        # pylint: disable=protected-access
        self.cephFs._append_mds_metadata(mds_versions, None)
        self.assertEqual(len(mds_versions), 0)

    def test_append_of_mds_metadata_with_existing_metadata(self):
        mds_versions = defaultdict(list)
        # pylint: disable=protected-access
        self.cephFs._append_mds_metadata(mds_versions, 'foo')
        self.assertEqual(len(mds_versions), 1)
        self.assertEqual(mds_versions['bar'], ['foo'])
