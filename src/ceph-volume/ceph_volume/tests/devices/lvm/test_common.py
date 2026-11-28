from unittest.mock import MagicMock, patch

from ceph_volume.devices.lvm import common


class TestCommon(object):

    def test_get_default_args_smoke(self):
        default_args = common.get_default_args()
        assert default_args

    @patch('ceph_volume.util.arg_validators.disk.has_bluestore_label', return_value=False)
    @patch('ceph_volume.util.arg_validators.Device')
    def test_prepare_parser_dmcrypt_open_opts_string_with_leading_dashes(
        self, mocked_device, _mock_bs_label
    ):
        mocked_device.return_value = MagicMock(
            exists=True,
            has_gpt_headers=False,
            has_partitions=False,
            used_by_ceph=False,
            has_fs=False,
            is_lv=False,
            path='/dev/nvme8n1',
            vg_name='vg',
            lv_name='lv',
        )
        parser = common.prepare_parser('ceph-volume lvm prepare', 'test')
        ns = parser.parse_args([
            '--data', '/dev/nvme8n1',
            '--dmcrypt-open-opts', '--persistent --debug-json',
        ])
        assert ns.dmcrypt_open_opts == '--persistent --debug-json'

    @patch('ceph_volume.util.arg_validators.disk.has_bluestore_label', return_value=False)
    @patch('ceph_volume.util.arg_validators.Device')
    def test_prepare_parser_dmcrypt_format_opts_same_shape(
        self, mocked_device, _mock_bs_label
    ):
        mocked_device.return_value = MagicMock(
            exists=True,
            has_gpt_headers=False,
            has_partitions=False,
            used_by_ceph=False,
            has_fs=False,
            is_lv=False,
            path='/dev/nvme0n1',
            vg_name='vg',
            lv_name='lv',
        )
        parser = common.prepare_parser('ceph-volume lvm prepare', 'test')
        ns = parser.parse_args([
            '--data', '/dev/nvme0n1',
            '--dmcrypt-format-opts', '--foo bar',
        ])
        assert ns.dmcrypt_format_opts == '--foo bar'

    @patch('ceph_volume.util.arg_validators.disk.has_bluestore_label', return_value=False)
    @patch('ceph_volume.util.arg_validators.Device')
    def test_prepare_parser_dmcrypt_opts_default_none(self, mocked_device, _mock_bs_label):
        mocked_device.return_value = MagicMock(
            exists=True,
            has_gpt_headers=False,
            has_partitions=False,
            used_by_ceph=False,
            has_fs=False,
            is_lv=False,
            path='/dev/sdz',
            vg_name='vg',
            lv_name='lv',
        )
        parser = common.prepare_parser('ceph-volume lvm prepare', 'test')
        ns = parser.parse_args(['--data', '/dev/sdz'])
        assert ns.dmcrypt_open_opts is None
        assert ns.dmcrypt_format_opts is None


class TestRollbackOsd(object):

    @patch('ceph_volume.devices.lvm.common.Zap')
    @patch('ceph_volume.devices.lvm.common.process.run')
    def test_purges_a_new_id(self, m_run, m_zap):
        common.rollback_osd('3')
        cmd = m_run.call_args[0][0]
        assert cmd[-3:] == ['purge-new', 'osd.3', '--yes-i-really-mean-it']
        m_zap.assert_called_once_with(['--destroy', '--osd-id', '3'])

    @patch('ceph_volume.devices.lvm.common.Zap')
    @patch('ceph_volume.devices.lvm.common.process.run')
    def test_keeps_a_reused_destroyed_id(self, m_run, m_zap):
        common.rollback_osd('3', reused_destroyed_id=True)
        m_run.assert_not_called()
        m_zap.assert_called_once_with(['--destroy', '--osd-id', '3'])

    @patch('ceph_volume.devices.lvm.common.Zap')
    @patch('ceph_volume.devices.lvm.common.process.run')
    def test_nothing_to_rollback_without_an_id(self, m_run, m_zap):
        common.rollback_osd(None)
        m_run.assert_not_called()
        m_zap.assert_not_called()
