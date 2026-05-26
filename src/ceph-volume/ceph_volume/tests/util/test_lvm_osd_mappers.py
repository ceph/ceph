from unittest.mock import MagicMock, patch

from ceph_volume.util.lvm_osd_mappers import OsdLvmMappers


@patch('ceph_volume.util.lvm_osd_mappers.get_lvs', return_value=[])
class TestOsdLvmMappersClusterContext:
    @patch(
        'ceph_volume.util.lvm_osd_mappers.OsdLuksCredentials.apply_cluster_context'
    )
    def test_apply_cluster_context_can_be_skipped(
        self,
        m_apply: MagicMock,
        m_get_lvs: MagicMock,
    ) -> None:
        OsdLvmMappers('0', 'fsid', apply_cluster_context=False)
        m_apply.assert_not_called()


@patch('ceph_volume.util.lvm_osd_mappers.get_lvs', return_value=[])
@patch('ceph_volume.util.lvm_osd_mappers.process.call')
@patch('ceph_volume.util.lvm_osd_mappers.OsdLvmMappers._rescan_physical_volumes')
@patch('ceph_volume.util.lvm_osd_mappers.OsdLvmMappers._refresh_osd_volumes_from_lvm')
class TestOsdLvmMappersEnsureOpen:
    @patch(
        'ceph_volume.util.lvm_osd_mappers.OsdLuksCredentials.apply_cluster_context'
    )
    def test_ensure_open_without_encryption_skips_credentials(
        self,
        m_cluster: MagicMock,
        m_refresh_lvs: MagicMock,
        m_rescan: MagicMock,
        m_call: MagicMock,
        m_get_lvs: MagicMock,
    ) -> None:
        mappers = OsdLvmMappers('1', 'fsid-1')
        mappers.encrypted = False
        mappers.ensure_open()
        m_rescan.assert_called_once()
        m_refresh_lvs.assert_called_once()
