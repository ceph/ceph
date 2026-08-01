from unittest.mock import MagicMock, patch

from ceph_volume.util.raw_osd_crypt_mappers import RawOsdCryptMappers


class TestBackingDevicePath:
    def test_empty(self) -> None:
        assert RawOsdCryptMappers.backing_device_path('') == ''

    def test_plain_block_device(self) -> None:
        assert RawOsdCryptMappers.backing_device_path('/dev/sda1') == '/dev/sda1'

    @patch('ceph_volume.util.raw_osd_crypt_mappers.disk.get_parent_device_from_mapper')
    def test_mapper_resolves_parent(self, m_parent: MagicMock) -> None:
        m_parent.return_value = '/dev/sda1'
        assert RawOsdCryptMappers.backing_device_path(
            '/dev/mapper/ceph-fsid-sda1-block-dmcrypt',
        ) == '/dev/sda1'

    @patch('ceph_volume.util.raw_osd_crypt_mappers.disk.get_parent_device_from_mapper')
    def test_mapper_without_parent_returns_empty(self, m_parent: MagicMock) -> None:
        m_parent.return_value = ''
        assert RawOsdCryptMappers.backing_device_path(
            '/dev/mapper/ceph-fsid-sda1-block-dmcrypt',
        ) == ''


@patch(
    'ceph_volume.util.raw_osd_crypt_mappers.OsdLuksCredentials.apply_cluster_context'
)
class TestMapperName:
    def test_mapper_name_from_backing_path(
        self, m_cluster: MagicMock,
    ) -> None:
        mappers = RawOsdCryptMappers(
            '0', 'fsid-uuid', '/dev/nvme2n2', cluster_name='ceph',
        )
        assert mappers._mapper_name_for_role('block') == (
            'ceph-fsid-uuid-nvme2n2-block-dmcrypt'
        )
        assert mappers._mapper_path_for_backing('/dev/nvme2n2', 'block') == (
            '/dev/mapper/ceph-fsid-uuid-nvme2n2-block-dmcrypt'
        )

    @patch(
        'ceph_volume.util.raw_osd_crypt_mappers.disk.get_parent_device_from_mapper'
    )
    def test_mapper_name_from_activate_mapper_path(
        self,
        m_parent: MagicMock,
        m_cluster: MagicMock,
    ) -> None:
        m_parent.return_value = '/dev/nvme2n2'
        mappers = RawOsdCryptMappers(
            '0',
            'fsid-uuid',
            '/dev/mapper/ceph-fsid-uuid-nvme2n2-block-dmcrypt',
            cluster_name='ceph',
        )
        assert mappers._mapper_name_for_role('block') == (
            'ceph-fsid-uuid-nvme2n2-block-dmcrypt'
        )


@patch(
    'ceph_volume.util.raw_osd_crypt_mappers.OsdLuksCredentials.apply_cluster_context'
)
class TestApplies:
    def test_tpm(self, m_cluster: MagicMock) -> None:
        mappers = RawOsdCryptMappers(
            '0', 'fsid', '/dev/sda', cluster_name='ceph', with_tpm=True,
        )
        assert mappers.applies()

    @patch('ceph_volume.util.raw_osd_crypt_mappers.disk.get_parent_device_from_mapper')
    def test_existing_dmcrypt_mapper_path(
        self, m_parent: MagicMock, m_cluster: MagicMock,
    ) -> None:
        m_parent.return_value = '/dev/sda1'
        mappers = RawOsdCryptMappers(
            '0',
            'fsid',
            '/dev/mapper/ceph-fsid-sda1-block-dmcrypt',
            cluster_name='ceph',
        )
        assert mappers.applies()

    @patch('ceph_volume.util.raw_osd_crypt_mappers.encryption_utils.CephLuks2')
    def test_luks2_ceph_encrypted_backing(
        self, m_luks_cls: MagicMock, m_cluster: MagicMock,
    ) -> None:
        m_luks_cls.return_value.is_ceph_encrypted = True
        mappers = RawOsdCryptMappers(
            '0', 'fsid', '/dev/sda1', cluster_name='ceph',
        )
        assert mappers.applies()

    @patch('ceph_volume.util.raw_osd_crypt_mappers.encryption_utils.CephLuks2')
    def test_clear_device(self, m_luks_cls: MagicMock, m_cluster: MagicMock) -> None:
        m_luks_cls.return_value.is_ceph_encrypted = False
        mappers = RawOsdCryptMappers(
            '0', 'fsid', '/dev/sda1', cluster_name='ceph',
        )
        assert not mappers.applies()


class TestRefresh:
    @patch(
        'ceph_volume.util.raw_osd_crypt_mappers.OsdLuksCredentials.apply_cluster_context'
    )
    @patch(
        'ceph_volume.util.raw_osd_crypt_mappers.OsdLuksCredentials.resolve_secret',
        return_value='test-key',
    )
    @patch('ceph_volume.util.raw_osd_crypt_mappers.encryption_utils.dmsetup_remove')
    @patch('ceph_volume.util.raw_osd_crypt_mappers.encryption_utils.luks_open')
    def test_refresh_closes_then_opens(
        self,
        m_luks_open: MagicMock,
        m_dmsetup_remove: MagicMock,
        m_resolve: MagicMock,
        m_cluster: MagicMock,
    ) -> None:
        mappers = RawOsdCryptMappers(
            '0', 'fsid', '/dev/sda1', cluster_name='ceph',
        )
        mappers.refresh()
        m_dmsetup_remove.assert_called_once_with(
            'ceph-fsid-sda1-block-dmcrypt',
            terminal_logging=False,
        )
        assert m_luks_open.called
