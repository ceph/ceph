from mock import patch, Mock
from ceph_volume.objectstore.bluestore import BlueStore


class TestBlueStore:
    @patch('ceph_volume.objectstore.baseobjectstore.prepare_utils.create_key', Mock(return_value=['AQCee6ZkzhOrJRAAZWSvNC3KdXOpC2w8ly4AZQ==']))
    def setup_method(self, m_create_key):
        self.b = BlueStore([])
        self.b.osd_mkfs_cmd = ['binary', 'arg1']

    def test_add_objectstore_opts_wal_device_path(self, monkeypatch):
        monkeypatch.setattr('ceph_volume.util.system.chown', lambda path: 0)
        self.b.wal_device_path = '/dev/nvme0n1'
        self.b.add_objectstore_opts()
        assert self.b.osd_mkfs_cmd == ['binary', 'arg1', '--bluestore-block-wal-path', '/dev/nvme0n1']

    def test_add_objectstore_opts_db_device_path(self, monkeypatch):
        monkeypatch.setattr('ceph_volume.util.system.chown', lambda path: 0)
        self.b.db_device_path = '/dev/ssd1'
        self.b.add_objectstore_opts()
        assert self.b.osd_mkfs_cmd == ['binary', 'arg1', '--bluestore-block-db-path', '/dev/ssd1']

    def test_add_objectstore_opts_osdspec_affinity(self, monkeypatch):
        monkeypatch.setattr('ceph_volume.util.system.chown', lambda path: 0)
        self.b.get_osdspec_affinity = lambda: 'foo'
        self.b.add_objectstore_opts()
        assert self.b.osd_mkfs_cmd == ['binary', 'arg1', '--osdspec-affinity', 'foo']