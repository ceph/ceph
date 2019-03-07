from ceph_volume.systemd import systemctl

class TestSystemctl(object):

    def test_get_running_osd_ids(self, stub_call):
        stdout = ['Id=ceph-osd@1.service', '', 'Id=ceph-osd@2.service']
        stub_call((stdout, [], 0))
        osd_ids = systemctl.get_running_osd_ids()
        assert osd_ids == ['1', '2']
