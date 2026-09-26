import pytest

from unittest import mock
from tests.fixtures import host_sysfs, import_cephadm, cephadm_fs

from cephadmlib.host_facts import Enclosure

_cephadm = import_cephadm()


@pytest.fixture
def enclosure(host_sysfs):
    e = Enclosure(
        enc_id='1',
        enc_path='/sys/class/scsi_generic/sg2/device/enclosure/0:0:1:0',
        dev_path='/sys/class/scsi_generic/sg2',
    )
    yield e


class TestEnclosure:

    def test_enc_metadata(self, enclosure):
        """Check metadata for the enclosure e.g. vendor and model"""

        assert enclosure.vendor == "EnclosuresInc"
        assert enclosure.components == '12'
        assert enclosure.model == "D12"
        assert enclosure.enc_id == '1'

        assert enclosure.ses_paths == ['sg2']
        assert enclosure.path_count == 1

    def test_enc_slots(self, enclosure):
        """Check slot count"""

        assert len(enclosure.slot_map) == 12

    def test_enc_slot_format(self, enclosure):
        """Check the attributes of a slot are as expected"""

        assert all(
            k in ['fault', 'locate', 'serial', 'status']
            for k, _v in enclosure.slot_map['0'].items()
        )

    def test_enc_slot_status(self, enclosure):
        """Check the number of occupied slots is correct"""

        occupied_slots = [
            slot_id
            for slot_id in enclosure.slot_map
            if enclosure.slot_map[slot_id].get('status').upper() == 'OK'
        ]

        assert len(occupied_slots) == 6

    def test_enc_disk_count(self, enclosure):
        """Check the disks found matches the slot info"""

        assert len(enclosure.device_lookup) == 6
        assert enclosure.device_count == 6

    def test_enc_device_serial(self, enclosure):
        """Check the device serial numbers are as expected"""

        assert all(
            fake_serial in enclosure.device_lookup.keys()
            for fake_serial in [
                'fake000',
                'fake001',
                'fake002',
                'fake003',
                'fake004',
                'fake005',
            ]
        )

    def test_enc_slot_to_serial(self, enclosure):
        """Check serial number to slot matches across slot_map and device_lookup"""

        for serial, slot in enclosure.device_lookup.items():
            assert enclosure.slot_map[slot].get('serial') == serial


def test_host_facts_security(cephadm_fs):
    cephadm_fs.create_file('/sys/kernel/security/lsm', contents='apparmor\n')
    cephadm_fs.create_file('/etc/apparmor', contents='foo\n')
    # List from https://tracker.ceph.com/issues/66389
    profiles_lines = [
        'foo (complain)',
        '/usr/bin/man (enforce)',
        '1password (unconfined)',
        'Discord (unconfined)',
        'MongoDB Compass (unconfined)',
        'profile name with spaces (enforce)',
    ]
    cephadm_fs.create_file(
        '/sys/kernel/security/apparmor/profiles',
        contents='\n'.join(profiles_lines),
    )

    from cephadmlib.host_facts import HostFacts

    class TestHostFacts(HostFacts):
        def _populate_sysctl_options(self):
            return {}

    ctx = mock.MagicMock()
    hfacts = TestHostFacts(ctx)
    ksec = hfacts.kernel_security
    assert ksec
    assert ksec['type'] == 'AppArmor'
    assert ksec['type'] == 'AppArmor'
    assert ksec['complain'] == 0
    assert ksec['enforce'] == 1
    assert ksec['unconfined'] == 2


def test_host_facts_skips_sysfs_only_nvme_alias_entries(cephadm_fs):
    """
    Some platforms expose extra /sys/block nvme entries that don't have a real
    /dev node (ex:nvme2c2n1 alongside nvme2n1),duplicates should not be counted.
    """
    from cephadmlib.host_facts import HostFacts

    # nvme2n1 is the "real" block device (has /sys/block/../dev and /dev/..).
    cephadm_fs.create_dir('/dev')
    cephadm_fs.create_dir('/sys/block/nvme2n1')
    cephadm_fs.create_file('/sys/block/nvme2n1/dev', contents='259:7')
    cephadm_fs.create_file('/dev/nvme2n1')

    # nvme2c2n1 is a sysfs-only alias (no /dev node), and it should be ignored.
    cephadm_fs.create_dir('/sys/block/nvme2c2n1')

    hfacts = object.__new__(HostFacts)
    assert hfacts._get_block_devs() == ['nvme2n1']


class TestX86_64IsaLevel:
    """Verify x86-64 microarchitecture level detection from cpu flags"""

    # flag sets modeled on real /proc/cpuinfo output (abbreviated to the
    # flags relevant for level detection plus a few extras)
    V1_FLAGS = {'fpu', 'mmx', 'sse', 'sse2', 'ht', 'syscall', 'nx', 'lm'}
    V2_FLAGS = V1_FLAGS | {
        'cx16', 'lahf_lm', 'popcnt', 'sse4_1', 'sse4_2', 'ssse3',
    }
    V3_FLAGS = V2_FLAGS | {
        'abm', 'avx', 'avx2', 'bmi1', 'bmi2', 'f16c', 'fma', 'movbe',
        'xsave',
    }
    V4_FLAGS = V3_FLAGS | {
        'avx512f', 'avx512bw', 'avx512cd', 'avx512dq', 'avx512vl',
    }

    def test_v1(self):
        from cephadmlib.host_facts import get_x86_64_isa_level

        assert get_x86_64_isa_level(self.V1_FLAGS) == 'x86-64-v1'

    def test_v2(self):
        from cephadmlib.host_facts import get_x86_64_isa_level

        assert get_x86_64_isa_level(self.V2_FLAGS) == 'x86-64-v2'

    def test_v2_with_partial_v3_flags(self):
        from cephadmlib.host_facts import get_x86_64_isa_level

        # e.g. Ivy Bridge: has avx/f16c/xsave but not avx2/bmi/fma/movbe
        flags = self.V2_FLAGS | {'avx', 'f16c', 'xsave'}
        assert get_x86_64_isa_level(flags) == 'x86-64-v2'

    def test_v3(self):
        from cephadmlib.host_facts import get_x86_64_isa_level

        assert get_x86_64_isa_level(self.V3_FLAGS) == 'x86-64-v3'

    def test_v4(self):
        from cephadmlib.host_facts import get_x86_64_isa_level

        assert get_x86_64_isa_level(self.V4_FLAGS) == 'x86-64-v4'

    def test_v4_flags_without_v3_does_not_skip_levels(self):
        from cephadmlib.host_facts import get_x86_64_isa_level

        # a (hypothetical) cpu missing a v3 flag caps at v2 even with
        # avx512 support present
        flags = (self.V4_FLAGS - {'bmi2'})
        assert get_x86_64_isa_level(flags) == 'x86-64-v2'
