import pytest
from unittest.mock import patch, Mock, call
from ceph_volume.activate import main as activate


OSD_FSID = '0d8d61dc-2a3c-4b1c-8f3e-7b3a2a9b6c11'


def _argv(osd_id='1', osd_fsid=OSD_FSID):
    argv = ['--no-systemd', '--no-tmpfs']
    if osd_id is not None:
        argv += ['--osd-id', osd_id]
    if osd_fsid is not None:
        argv += ['--osd-uuid', osd_fsid]
    return argv


class TestActivate(object):

    @pytest.fixture(autouse=True)
    def mocks(self):
        # Record the order in which the activation methods are tried.
        self.calls = Mock()
        with patch('ceph_volume.activate.main.OsdLvmMappers'), \
             patch('ceph_volume.api.lvm.get_lvs') as get_lvs, \
             patch('ceph_volume.activate.main.RAWActivate') as raw, \
             patch('ceph_volume.activate.main.LVMActivate') as lvm, \
             patch('ceph_volume.activate.main.SimpleActivate') as simple:
            self.get_lvs = get_lvs
            self.calls.attach_mock(raw.return_value.activate, 'raw')
            self.calls.attach_mock(lvm.return_value.activate, 'lvm')
            self.calls.attach_mock(simple.return_value.activate, 'simple')
            yield

    def tried(self):
        return [c[0] for c in self.calls.mock_calls]

    def test_lvm_osd_does_not_try_raw(self):
        self.get_lvs.return_value = [Mock(), Mock()]  # block and db LV
        activate.Activate(_argv()).main()
        self.get_lvs.assert_called_once_with(
            tags={'ceph.osd_fsid': OSD_FSID, 'ceph.osd_id': '1'})
        assert self.calls.mock_calls == [call.lvm(None, '1', OSD_FSID)]

    def test_lvm_osd_lookup_by_osd_fsid_only(self):
        self.get_lvs.return_value = [Mock()]
        activate.Activate(_argv(osd_id=None)).main()
        self.get_lvs.assert_called_once_with(tags={'ceph.osd_fsid': OSD_FSID})
        assert self.calls.mock_calls == [call.lvm(None, None, OSD_FSID)]

    def test_lvm_osd_lvm_fails_tries_simple_not_raw(self):
        self.get_lvs.return_value = [Mock()]
        self.calls.lvm.side_effect = RuntimeError('lvm failed')
        activate.Activate(_argv()).main()
        assert self.tried() == ['lvm', 'simple']

    def test_no_lv_tries_raw_first(self):
        self.get_lvs.return_value = []
        activate.Activate(_argv()).main()
        assert self.tried() == ['raw']

    def test_no_lv_raw_fails_then_lvm(self):
        self.get_lvs.return_value = []
        self.calls.raw.side_effect = RuntimeError(
            'did not find any matching OSD to activate')
        activate.Activate(_argv()).main()
        assert self.calls.mock_calls == [call.raw(),
                                         call.lvm(None, '1', OSD_FSID)]

    def test_no_lv_raw_and_lvm_fail_then_simple(self, capsys):
        self.get_lvs.return_value = []
        self.calls.raw.side_effect = RuntimeError('raw failed')
        self.calls.lvm.side_effect = RuntimeError('lvm failed')
        self.calls.simple.side_effect = RuntimeError('simple failed')
        activate.Activate(_argv()).main()
        assert self.tried() == ['raw', 'lvm', 'simple']
        assert 'Failed to activate any OSD(s)' in capsys.readouterr().err

    def test_lv_lookup_fails_tries_raw_first(self):
        self.get_lvs.side_effect = RuntimeError('lvs failed')
        self.calls.raw.side_effect = RuntimeError('raw failed')
        activate.Activate(_argv()).main()
        assert self.tried() == ['raw', 'lvm']

    def test_no_osd_fsid_skips_lookup(self):
        activate.Activate(_argv(osd_fsid=None)).main()
        self.get_lvs.assert_not_called()
        assert self.tried() == ['raw']
