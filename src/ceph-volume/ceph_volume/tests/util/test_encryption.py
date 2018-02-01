from ceph_volume.util import encryption


class TestStatus(object):

    def test_skips_unuseful_lines(self, stub_call):
        out = ['some line here', '  device: /dev/sdc1']
        stub_call((out, '', 0))
        assert encryption.status('/dev/sdc1') == {'device': '/dev/sdc1'}

    def test_removes_extra_quotes(self, stub_call):
        out = ['some line here', '  device: "/dev/sdc1"']
        stub_call((out, '', 0))
        assert encryption.status('/dev/sdc1') == {'device': '/dev/sdc1'}

    def test_ignores_bogus_lines(self, stub_call):
        out = ['some line here', '  ']
        stub_call((out, '', 0))
        assert encryption.status('/dev/sdc1') == {}
