import ctypes
import json
import struct

import mock
import pytest

from tests.fixtures import cephadm_fs, import_cephadm, with_cephadm_ctx

from cephadmlib import nvme
from cephadmlib.exceptions import Error

_cephadm = import_cephadm()

DEVICE = '/dev/nvme0n1'
CONTROLLER_BLOB = bytes(range(256)) * 16  # 4096 distinctive bytes
NAMESPACE_BLOB = bytes(reversed(range(256))) * 16


def fake_sysfs(fs, namespace='nvme0n1', controller='nvme0', nsid=1,
               vendor='0x1014', device='0x0634', multipath=False):
    """Lay out the sysfs entries nvme-info reads for one namespace."""
    block = f'/sys/class/block/{namespace}'
    fs.create_file(f'{block}/nsid', contents=f'{nsid}\n')
    if multipath:
        # With NVMe multipath the namespace hangs off the subsystem, and the
        # controllers sit below it.
        pci = f'{block}/device/{controller}/device'
    else:
        pci = f'{block}/device/device'
    fs.create_file(f'{pci}/vendor', contents=f'{vendor}\n')
    fs.create_file(f'{pci}/device', contents=f'{device}\n')


class FakeIoctl:
    """An NVMe admin passthrough that answers Identify from a canned map."""

    def __init__(self, responses=None, error=None, status=0):
        self.responses = responses if responses is not None else {
            (nvme.CNS_CONTROLLER, 0): CONTROLLER_BLOB,
            (nvme.CNS_NAMESPACE, 1): NAMESPACE_BLOB,
        }
        self.error = error
        # A controller that rejects the command reports it as the ioctl's
        # return value, not as an exception, and leaves the buffer alone.
        self.status = status
        self.commands = []

    def __call__(self, fd, op, arg):
        if self.error is not None:
            raise self.error
        fields = nvme.unpack_passthru(bytes(arg))
        self.commands.append((op, fields))
        if self.status:
            return self.status
        payload = self.responses[(fields['cdw10'] & 0xFF, fields['nsid'])]
        # Writing through the address in the command is the whole point: a
        # struct with no data pointer would silently return zeroes.
        ctypes.memmove(fields['addr'], payload, fields['data_len'])
        return 0


@pytest.fixture
def drive(cephadm_fs):
    fake_sysfs(cephadm_fs)
    cephadm_fs.create_file(DEVICE, contents='')
    return cephadm_fs


def describe(ioctl, device=DEVICE):
    with mock.patch('cephadmlib.nvme.fcntl.ioctl', ioctl):
        return nvme.describe_device(device)


class TestDevicePathValidation:
    @pytest.mark.parametrize('path', [
        '/dev/sda',
        '/dev/nvme0',
        '/dev/nvme0n1p1',
        '/dev/dm-0',
        '/dev/mapper/vg-lv',
        'nvme0n1',
        '/dev/nvme0n1 ',
        '/dev/nvme0n1;reboot',
        '/dev/../dev/nvme0n1',
        '',
    ])
    def test_only_a_namespace_node_is_accepted(self, path):
        with pytest.raises(Error):
            nvme.describe_device(path)

    def test_a_rejected_path_is_never_opened(self, drive):
        ioctl = FakeIoctl()
        with mock.patch('cephadmlib.nvme.fcntl.ioctl', ioctl):
            with pytest.raises(Error):
                nvme.describe_device('/dev/nvme0n1p1')
        assert ioctl.commands == []


class TestAbsentNamespace:
    """A namespace the host does not have is a fact, not a failed probe.

    The caller reads an error as "something is broken" and aborts OSD
    creation; it must be able to tell that apart from a device that simply
    is not there, which no OSD can be created on anyway.
    """

    def test_an_absent_namespace_reports_not_present(self, cephadm_fs):
        info = nvme.describe_device('/dev/nvme9n9')
        assert info['present'] is False
        assert info['device'] == '/dev/nvme9n9'

    def test_an_absent_namespace_is_not_an_error(self, cephadm_fs):
        # No raise: the distinction is the point.
        nvme.describe_device('/dev/nvme9n9')

    def test_an_absent_namespace_issues_no_commands(self, cephadm_fs):
        ioctl = FakeIoctl()
        with mock.patch('cephadmlib.nvme.fcntl.ioctl', ioctl):
            nvme.describe_device('/dev/nvme9n9')
        assert ioctl.commands == []

    def test_a_present_namespace_says_so(self, drive):
        assert describe(FakeIoctl())['present'] is True

    def test_a_present_namespace_with_unreadable_sysfs_still_errors(self, cephadm_fs):
        # The block directory exists, so the device is there; failing to read
        # its ids is a real fault and must not be reported as absent.
        cephadm_fs.create_file('/sys/class/block/nvme0n1/uevent', contents='x')
        cephadm_fs.create_file(DEVICE, contents='')
        with pytest.raises(Error):
            describe(FakeIoctl())


class TestIdentifyCommands:
    def test_the_passthru_struct_is_the_kernel_size(self):
        assert struct.calcsize(nvme._PASSTHRU_FORMAT) == 72

    def test_the_admin_ioctl_is_used_not_the_io_one(self, drive):
        ioctl = FakeIoctl()
        describe(ioctl)
        # 0xC0484E43 is NVME_IOCTL_IO_CMD and does not serve Identify.
        assert {op for op, _ in ioctl.commands} == {0xC0484E41}

    def test_identify_controller_is_cns_1_on_nsid_0(self, drive):
        ioctl = FakeIoctl()
        describe(ioctl)
        ctrl = [f for _, f in ioctl.commands if f['cdw10'] & 0xFF == nvme.CNS_CONTROLLER]
        assert len(ctrl) == 1
        assert ctrl[0]['nsid'] == 0
        assert ctrl[0]['opcode'] == nvme.NVME_ADMIN_IDENTIFY

    def test_identify_namespace_is_cns_0_on_the_namespace_id(self, drive):
        ioctl = FakeIoctl()
        describe(ioctl)
        ns = [f for _, f in ioctl.commands if f['cdw10'] & 0xFF == nvme.CNS_NAMESPACE]
        assert len(ns) == 1
        assert ns[0]['nsid'] == 1
        assert ns[0]['opcode'] == nvme.NVME_ADMIN_IDENTIFY

    def test_every_command_carries_a_data_pointer_and_length(self, drive):
        # The defect this subcommand replaces packed a struct with neither,
        # so it returned 4096 zero bytes and looked like a non-FCM drive.
        ioctl = FakeIoctl()
        describe(ioctl)
        assert ioctl.commands
        for _, fields in ioctl.commands:
            assert fields['addr'] != 0
            assert fields['data_len'] == 4096

    def test_the_namespace_id_comes_from_sysfs(self, cephadm_fs):
        fake_sysfs(cephadm_fs, nsid=7)
        cephadm_fs.create_file(DEVICE, contents='')
        ioctl = FakeIoctl(responses={
            (nvme.CNS_CONTROLLER, 0): CONTROLLER_BLOB,
            (nvme.CNS_NAMESPACE, 7): NAMESPACE_BLOB,
        })
        assert describe(ioctl)['nsid'] == 7

    def test_an_ioctl_failure_is_reported_not_swallowed(self, drive):
        with pytest.raises(Error):
            describe(FakeIoctl(error=OSError(5, 'I/O error')))


class TestCompletionStatus:
    """A command the controller rejected must not look like a good answer.

    The kernel returns -1 only for host-side failures. A non-zero NVMe
    completion status comes back as the ioctl's return value with nothing
    raised, and the data buffer is never written, so an unchecked return
    reports 4096 zero bytes as a valid Identify structure.
    """

    def test_a_non_zero_status_raises(self, drive):
        with pytest.raises(Error):
            describe(FakeIoctl(status=0x2))

    def test_a_non_zero_status_never_returns_a_zeroed_blob(self, drive):
        try:
            info = describe(FakeIoctl(status=0x2))
        except Error:
            return
        assert False, f'expected Error, got {info["identify_controller"][:32]}'

    def test_the_status_is_named_in_the_message(self, drive):
        with pytest.raises(Error) as caught:
            describe(FakeIoctl(status=0x4002))
        assert '0x4002' in str(caught.value)

    def test_a_zero_status_still_succeeds(self, drive):
        assert describe(FakeIoctl(status=0))['nsid'] == 1


class TestOutput:
    def test_both_identify_structures_are_returned_raw(self, drive):
        info = describe(FakeIoctl())
        assert info['identify_controller'] == CONTROLLER_BLOB.hex()
        assert info['identify_namespace'] == NAMESPACE_BLOB.hex()

    def test_the_blobs_are_full_length(self, drive):
        info = describe(FakeIoctl())
        assert len(info['identify_controller']) == 4096 * 2
        assert len(info['identify_namespace']) == 4096 * 2

    def test_no_field_is_parsed_out_of_the_blobs(self, drive):
        # Every interpretation belongs to the caller; a new requirement must
        # not mean another change here.
        info = describe(FakeIoctl())
        assert set(info) == {
            'device', 'present', 'pci_vendor', 'pci_device', 'nsid',
            'identify_controller', 'identify_namespace',
        }

    def test_the_device_is_echoed_back(self, drive):
        assert describe(FakeIoctl())['device'] == DEVICE


class TestPciIds:
    def test_ids_are_read_from_sysfs(self, drive):
        info = describe(FakeIoctl())
        assert (info['pci_vendor'], info['pci_device']) == ('0x1014', '0x0634')

    def test_a_multipath_namespace_finds_its_controller(self, cephadm_fs):
        fake_sysfs(cephadm_fs, multipath=True, vendor='0x144d', device='0xa80a')
        cephadm_fs.create_file(DEVICE, contents='')
        info = describe(FakeIoctl())
        assert (info['pci_vendor'], info['pci_device']) == ('0x144d', '0xa80a')

    def test_a_namespace_with_no_pci_ids_is_an_error(self, cephadm_fs):
        cephadm_fs.create_file('/sys/class/block/nvme0n1/nsid', contents='1\n')
        cephadm_fs.create_file(DEVICE, contents='')
        with pytest.raises(Error):
            describe(FakeIoctl())


class TestCommandNvmeInfo:
    def test_it_prints_one_json_object(self, drive, capsys):
        with with_cephadm_ctx([]) as ctx:
            ctx.nvme_info_device = DEVICE
            with mock.patch('cephadmlib.nvme.fcntl.ioctl', FakeIoctl()):
                _cephadm.command_nvme_info(ctx)
        info = json.loads(capsys.readouterr().out)
        assert info['device'] == DEVICE
        assert info['identify_controller'] == CONTROLLER_BLOB.hex()

    def test_a_bad_device_raises_rather_than_printing(self, drive, capsys):
        with with_cephadm_ctx([]) as ctx:
            ctx.nvme_info_device = '/dev/sda'
            with pytest.raises(Error):
                _cephadm.command_nvme_info(ctx)
        assert capsys.readouterr().out == ''

    def test_the_subcommand_is_registered_and_needs_a_device(self):
        parser = _cephadm._get_parser()
        args = parser.parse_args(['nvme-info', '--device', DEVICE])
        assert args.func is _cephadm.command_nvme_info
        assert args.nvme_info_device == DEVICE
        with pytest.raises(SystemExit):
            parser.parse_args(['nvme-info'])
