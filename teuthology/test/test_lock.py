from .. import lock


class TestLock(object):

    def test_canonicalize_hostname(self):
        host_base = 'box1'
        result = lock.canonicalize_hostname(host_base)
        assert result == 'ubuntu@box1.front.sepia.ceph.com'

    def test_decanonicalize_hostname(self):
        host = 'ubuntu@box1.front.sepia.ceph.com'
        result = lock.decanonicalize_hostname(host)
        assert result == 'box1'

    def test_canonicalize_hostname_nouser(self):
        host_base = 'box1'
        result = lock.canonicalize_hostname(host_base, user=None)
        assert result == 'box1.front.sepia.ceph.com'

    def test_decanonicalize_hostname_nouser(self):
        host = 'box1.front.sepia.ceph.com'
        result = lock.decanonicalize_hostname(host)
        assert result == 'box1'
