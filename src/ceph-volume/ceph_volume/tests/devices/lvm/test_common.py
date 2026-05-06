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
