from ceph.deployment.utils import is_ipv6, unwrap_ipv6, wrap_ipv6


def test_is_ipv6():
    for good in ("[::1]", "::1",
                 "fff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"):
        assert is_ipv6(good)
    for bad in ("127.0.0.1",
                "ffff:ffff:ffff:ffff:ffff:ffff:ffff:fffg",
                "1:2:3:4:5:6:7:8:9", "fd00::1::1", "[fg::1]"):
        assert not is_ipv6(bad)


def test_unwrap_ipv6():
    def unwrap_test(address, expected):
        assert unwrap_ipv6(address) == expected

    tests = [
        ('::1', '::1'), ('[::1]', '::1'),
        ('[fde4:8dba:82e1:0:5054:ff:fe6a:357]', 'fde4:8dba:82e1:0:5054:ff:fe6a:357'),
        ('can actually be any string', 'can actually be any string'),
        ('[but needs to be stripped] ', '[but needs to be stripped] ')]
    for address, expected in tests:
        unwrap_test(address, expected)


def test_wrap_ipv6():
    def wrap_test(address, expected):
        assert wrap_ipv6(address) == expected

    tests = [
        ('::1', '[::1]'), ('[::1]', '[::1]'),
        ('fde4:8dba:82e1:0:5054:ff:fe6a:357', '[fde4:8dba:82e1:0:5054:ff:fe6a:357]'),
        ('myhost.example.com', 'myhost.example.com'), ('192.168.0.1', '192.168.0.1'),
        ('', ''), ('fd00::1::1', 'fd00::1::1')]
    for address, expected in tests:
        wrap_test(address, expected)
