from unittest.mock import patch

from ceph_volume import conf
from ceph_volume.util.osd_luks_credentials import OsdLuksCredentials


class TestOsdLuksCredentialsApplyClusterContext:
    @patch('ceph_volume.util.osd_luks_credentials.configuration.load')
    @patch('ceph_volume.util.osd_luks_credentials.configuration.load_ceph_conf_path')
    def test_sets_cluster_and_loads_conf(self, m_load_path, m_load):
        OsdLuksCredentials(42, 'osd-fsid').apply_cluster_context('mycluster')
        assert conf.cluster == 'mycluster'
        m_load_path.assert_called_once_with('mycluster')
        m_load.assert_called_once_with()


class TestOsdLuksCredentialsResolveSecret:
    def test_tpm_returns_empty_string(self):
        c = OsdLuksCredentials(1, 'fsid', luks_secret='should-ignore', with_tpm=True)
        assert c.resolve_secret('lockbox') == ''

    def test_preconfigured_secret_short_circuits(self):
        c = OsdLuksCredentials(1, 'fsid', luks_secret='cached-key', with_tpm=False)
        with patch(
            'ceph_volume.util.osd_luks_credentials.encryption_utils.get_dmcrypt_key'
        ) as m_get:
            assert c.resolve_secret('lockbox') == 'cached-key'
        m_get.assert_not_called()

    @patch('ceph_volume.util.osd_luks_credentials.encryption_utils.get_dmcrypt_key')
    @patch('ceph_volume.util.osd_luks_credentials.encryption_utils.write_lockbox_keyring')
    @patch('ceph_volume.util.osd_luks_credentials.system.path_is_mounted', return_value=True)
    def test_lockbox_then_get_dmcrypt_key(
        self, m_mounted, m_write_lb, m_get_key,
    ):
        m_get_key.return_value = 'from-mon'
        c = OsdLuksCredentials('0', 'osd-fsid-1', luks_secret=None, with_tpm=False)
        out = c.resolve_secret('lockbox-secret-b64')
        assert out == 'from-mon'
        assert c.luks_secret == 'from-mon'
        m_write_lb.assert_called_once_with('0', 'osd-fsid-1', 'lockbox-secret-b64')
        m_get_key.assert_called_once_with('0', 'osd-fsid-1')

    @patch('ceph_volume.util.osd_luks_credentials.encryption_utils.get_dmcrypt_key')
    @patch('ceph_volume.util.osd_luks_credentials.encryption_utils.write_lockbox_keyring')
    @patch('ceph_volume.util.osd_luks_credentials.prepare_utils.create_osd_path')
    @patch('ceph_volume.util.osd_luks_credentials.system.path_is_mounted', return_value=False)
    def test_lockbox_creates_tmpfs_osd_path_when_unmounted(
        self, m_mounted, m_create, m_write_lb, m_get_key,
    ):
        conf.cluster = 'ceph'
        m_get_key.return_value = 'k'
        c = OsdLuksCredentials(3, 'fsid-2', with_tpm=False)
        assert c.resolve_secret('lb') == 'k'
        m_create.assert_called_once_with('3', tmpfs=True)
        m_write_lb.assert_called_once()

    @patch('ceph_volume.util.osd_luks_credentials.encryption_utils.get_dmcrypt_key')
    @patch('ceph_volume.util.osd_luks_credentials.encryption_utils.write_lockbox_keyring')
    def test_missing_lockbox_skips_write_still_fetches_key(
        self, m_write_lb, m_get_key,
    ):
        m_get_key.return_value = 'nomon'
        c = OsdLuksCredentials(9, 'fsid-3', with_tpm=False)
        assert c.resolve_secret(None) == 'nomon'
        m_write_lb.assert_not_called()
        m_get_key.assert_called_once_with('9', 'fsid-3')

    def test_osd_id_normalized_to_str(self):
        c = OsdLuksCredentials(7, 'x')
        assert c.osd_id == '7'
