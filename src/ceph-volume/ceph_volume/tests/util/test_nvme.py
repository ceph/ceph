import ctypes
import struct
import pytest
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


def _make_ctrl_buf(ds: int = 0, dss: int = 14, dvs: int = 0) -> bytes:
    """Build a fake 4096-byte Identify Controller response with given FCM fields."""
    buf = bytearray(4096)
    struct.pack_into('<H', buf, 3408, ds)
    buf[3410] = dss
    struct.pack_into('<H', buf, 3412, dvs)
    return bytes(buf)


def _make_ns_buf(lba_format_index: int = 0, lbads: int = 9) -> bytes:
    """Build a fake 4096-byte Identify Namespace response with given LBA format."""
    buf = bytearray(4096)
    buf[26] = lba_format_index & 0x0F  # FLBAS bits 3:0
    buf[128 + lba_format_index * 4 + 2] = lbads
    return bytes(buf)


class TestFcmDedupInfo:
    def test_non_nvme_device_returns_not_supported(self):
        """Non-NVMe devices return not-supported without issuing any ioctl."""
        result = nvme.fcm_dedup_info('/dev/sda')
        assert result.supported is False
        assert result.slot_size_bytes == 0
        assert result.dvs == 0
        assert result.lba_size_bytes == 0

    def test_empty_device_raises_error(self):
        """Empty device path raises ValueError."""
        with pytest.raises(ValueError, match='device path is required'):
            nvme.fcm_dedup_info('')

    @patch('ceph_volume.util.nvme.os.open', side_effect=OSError('Permission denied'))
    def test_ioctl_open_error_returns_not_supported(self, m_open):
        """OSError on os.open() is caught and returns not-supported."""
        result = nvme.fcm_dedup_info('/dev/nvme0n1')
        assert result.supported is False

    @patch('ceph_volume.util.nvme.fcntl.ioctl', side_effect=OSError('ioctl failed'))
    @patch('ceph_volume.util.nvme.os.open', return_value=3)
    @patch('ceph_volume.util.nvme.os.close')
    def test_ioctl_error_returns_not_supported(self, m_close, m_open, m_ioctl):
        """OSError from fcntl.ioctl() is caught and returns not-supported."""
        result = nvme.fcm_dedup_info('/dev/nvme0n1')
        assert result.supported is False

    @patch('ceph_volume.util.nvme.fcntl.ioctl')
    @patch('ceph_volume.util.nvme.os.open', return_value=3)
    @patch('ceph_volume.util.nvme.os.close')
    def test_ds_zero_dvs_zero_returns_not_supported(self, m_close, m_open, m_ioctl):
        """DS=0 and DVS=0 means not FCM-capable."""
        ctrl_buf = _make_ctrl_buf(ds=0, dss=14, dvs=0)

        def fake_ioctl(fd, request, cmd):
            # Write the fake identify response into the resp buffer via the cmd's addr
            ctypes.memmove(cmd.addr, ctrl_buf, 4096)

        m_ioctl.side_effect = fake_ioctl
        result = nvme.fcm_dedup_info('/dev/nvme0n1')
        assert result.supported is False

    @patch('ceph_volume.util.nvme.fcntl.ioctl')
    @patch('ceph_volume.util.nvme.os.open', return_value=3)
    @patch('ceph_volume.util.nvme.os.close')
    def test_dvs_below_minimum_returns_not_supported(self, m_close, m_open, m_ioctl):
        """DS=1 but DVS < 4 is too old; must return not-supported and log an error."""
        ctrl_buf = _make_ctrl_buf(ds=1, dss=14, dvs=3)

        def fake_ioctl(fd, request, cmd):
            ctypes.memmove(cmd.addr, ctrl_buf, 4096)

        m_ioctl.side_effect = fake_ioctl
        with patch('ceph_volume.util.nvme.logger') as m_logger:
            result = nvme.fcm_dedup_info('/dev/nvme0n1')

        assert result.supported is False
        # Confirm an error was logged mentioning the DVS value
        m_logger.error.assert_called_once()
        args = m_logger.error.call_args[0]
        # args = (fmt, device, dvs) — interpolate and check for DVS value
        assert 'DVS=3' in (args[0] % args[1:])

    @patch('ceph_volume.util.nvme.fcntl.ioctl')
    @patch('ceph_volume.util.nvme.os.open', return_value=3)
    @patch('ceph_volume.util.nvme.os.close')
    def test_fcm_device_returns_supported(self, m_close, m_open, m_ioctl):
        """DS=1, DVS=4, DSS=14 with LBADS=9 returns supported with correct values."""
        ctrl_buf = _make_ctrl_buf(ds=1, dss=14, dvs=4)
        ns_buf = _make_ns_buf(lba_format_index=0, lbads=9)
        call_count = [0]

        def fake_ioctl(fd, request, cmd):
            call_count[0] += 1
            if call_count[0] == 1:
                ctypes.memmove(cmd.addr, ctrl_buf, 4096)  # Identify Controller
            else:
                ctypes.memmove(cmd.addr, ns_buf, 4096)    # Identify Namespace

        m_ioctl.side_effect = fake_ioctl
        result = nvme.fcm_dedup_info('/dev/nvme0n1')

        assert result.supported is True
        assert result.slot_size_bytes == 16384  # 2**14
        assert result.dvs == 4
        assert result.lba_size_bytes == 512     # 2**9


class TestFcmComputeReservation:
    def test_reservation_calculation_1tb(self):
        """Test reservation calculation for 1 TB drive with 16 KiB slots."""
        size = 1024 * 1024 * 1024 * 1024  # 1 TiB
        slot_size = 16 * 1024  # 16 KiB

        result = nvme.fcm_compute_reservation(size, slot_size)

        # Check that we got the expected structure
        assert 'total_bytes' in result
        assert 'regions' in result
        assert 'S' in result['regions']
        assert 'M' in result['regions']
        assert 'FP' in result['regions']
        assert 'E' in result['regions']

        # Verify alignment: S and M should be slot-size aligned
        assert result['regions']['S'] % slot_size == 0
        assert result['regions']['M'] % slot_size == 0

        # FP and M should be 512 KiB aligned
        assert result['regions']['FP'] % (512 * 1024) == 0
        assert result['regions']['M'] % (512 * 1024) == 0

        # E should be 16 KiB aligned
        assert result['regions']['E'] % (16 * 1024) == 0

        # Verify formula values for 1 TiB:
        # C = 1099511627776
        # R = int((C - 2*GiB) / 1.1015) = 996063640248
        # total_bytes = C - R
        r_expected = int((size - 2 * 1024 * 1024 * 1024) / 1.1015)
        assert result['total_bytes'] == size - r_expected

    def test_reservation_calculation_4tb(self):
        """Test reservation calculation for 4 TB drive with 32 KiB slots."""
        size = 4 * 1024 * 1024 * 1024 * 1024  # 4 TiB
        slot_size = 32 * 1024  # 32 KiB

        result = nvme.fcm_compute_reservation(size, slot_size)

        # Verify alignment with different slot size
        assert result['regions']['S'] % slot_size == 0
        assert result['regions']['M'] % (512 * 1024) == 0
        assert result['regions']['FP'] % (512 * 1024) == 0
        assert result['regions']['E'] % (16 * 1024) == 0

        r_expected = int((size - 2 * 1024 * 1024 * 1024) / 1.1015)
        assert result['total_bytes'] == size - r_expected


class TestFcmReservationLbas:
    def test_lba_calculation_512b_sectors(self):
        """Test LBA calculation with 512-byte sectors."""
        lv_size = 1024 * 1024 * 1024 * 1024  # 1 TiB (TOTAL/original size)
        reservation_total = 100 * 1024 * 1024 * 1024  # 100 GiB
        lba_size = 512

        base_lba, size_lba = nvme.fcm_reservation_lbas(lv_size, reservation_total, lba_size)

        # Verify calculations: base_lba points to start of reserved region
        assert base_lba == (lv_size - reservation_total) // lba_size
        assert size_lba == reservation_total // lba_size

        # Verify round-trip: reserved region extends to end of device
        assert base_lba * lba_size + size_lba * lba_size == lv_size

    def test_lba_calculation_4k_sectors(self):
        """Test LBA calculation with 4096-byte sectors."""
        lv_size = 1024 * 1024 * 1024 * 1024  # 1 TiB (TOTAL/original size)
        reservation_total = 100 * 1024 * 1024 * 1024  # 100 GiB
        lba_size = 4096

        base_lba, size_lba = nvme.fcm_reservation_lbas(lv_size, reservation_total, lba_size)

        # Verify calculations: base_lba points to start of reserved region
        assert base_lba == (lv_size - reservation_total) // lba_size
        assert size_lba == reservation_total // lba_size
