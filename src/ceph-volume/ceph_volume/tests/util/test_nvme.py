from unittest.mock import patch

from ceph_volume.util import nvme


class TestNvmePreformat:
    @patch('ceph_volume.util.nvme.process.call')
    def test_non_nvme_device_skips_preformat(self, m_call, fake_filesystem):
        assert nvme.preformat('/dev/sda') is False
        m_call.assert_not_called()

    @patch('ceph_volume.util.nvme.disk.is_device', return_value=True)
    @patch('ceph_volume.util.nvme.process.call', return_value=([], [], 0))
    def test_preformat_invokes_nvme_cli(self, m_call, m_is_device, fake_filesystem):
        fake_filesystem.create_dir('/sys/block/nvme0n1/device/nvme0')
        assert nvme.preformat('/dev/nvme0n1') is True
        m_call.assert_called_once_with(
            ['nvme', 'format', '/dev/nvme0n1', '--force'],
            run_on_host=False,
            show_command=True,
            terminal_verbose=True,
            verbose_on_failure=True
        )

    @patch('ceph_volume.util.nvme.disk.is_device', return_value=True)
    @patch('ceph_volume.util.nvme.process.call', return_value=([], [], 1))
    def test_preformat_handles_non_zero_rc(self, m_call, m_is_device, fake_filesystem):
        fake_filesystem.create_dir('/sys/block/nvme0n1/device/nvme0')
        assert nvme.preformat('/dev/nvme0n1') is False
        assert m_call.called

    @patch('ceph_volume.util.nvme.disk.is_device', return_value=True)
    @patch('ceph_volume.util.nvme.process.call', side_effect=FileNotFoundError('missing nvme'))
    def test_preformat_handles_missing_cli(self, m_call, m_is_device, fake_filesystem):
        fake_filesystem.create_dir('/sys/block/nvme0n1/device/nvme0')
        assert nvme.preformat('/dev/nvme0n1') is False
        assert m_call.called

    def test_partition_is_not_formatted(self, fake_filesystem):
        fake_filesystem.create_file('/sys/block/nvme0n1p1/partition', contents='1')
        assert nvme.preformat('/dev/nvme0n1p1') is False
