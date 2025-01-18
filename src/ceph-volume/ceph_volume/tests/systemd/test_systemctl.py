import pytest
from ceph_volume.systemd import systemctl

class TestSystemctl(object):

    @pytest.mark.parametrize("stdout,expected", [
        (['Id=ceph-osd@1.service', '', 'Id=ceph-osd@2.service'], ['1','2']),
        (['Id=ceph-osd1.service',], []),
        (['Id=ceph-osd@1'], ['1']),
        ([], []),
    ])
    def test_get_running_osd_ids(self, stub_call, stdout, expected):
        stub_call((stdout, [], 0))
        osd_ids = systemctl.get_running_osd_ids()
        assert osd_ids == expected

    def test_returns_empty_list_on_nonzero_return_code(self, stub_call):
        stdout = ['Id=ceph-osd@1.service', '', 'Id=ceph-osd@2.service']
        stub_call((stdout, [], 1))
        osd_ids = systemctl.get_running_osd_ids()
        assert osd_ids == []
