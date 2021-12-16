import pytest

from ceph.deployment.utils import is_ipv6, unwrap_ipv6, wrap_ipv6, valid_addr
from typing import NamedTuple


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


class Address(NamedTuple):
    addr: str
    status: bool
    description: str


@pytest.mark.parametrize('addr_object', [
    Address('www.ibm.com', True, 'Name'),
    Address('www.google.com:162', True, 'Name:Port'),
    Address('my.big.domain.name.for.big.people', False, 'DNS lookup failed'),
    Address('192.168.122.1', True, 'IPv4'),
    Address('[192.168.122.1]', False, 'IPv4 address wrapped in brackets is invalid'),
    Address('10.40003.200', False, 'Invalid partial IPv4 address'),
    Address('10.7.5', False, 'Invalid partial IPv4 address'),
    Address('10.7', False, 'Invalid partial IPv4 address'),
    Address('192.168.122.5:7090', True, 'IPv4:Port'),
    Address('fe80::7561:c8fb:d3d7:1fa4', True, 'IPv6'),
    Address('[fe80::7561:c8fb:d3d7:1fa4]:9464', True, 'IPv6:Port'),
    Address('[fe80::7561:c8fb:d3d7:1fa4]', True, 'IPv6'),
    Address('[fe80::7561:c8fb:d3d7:1fa4', False,
            'Address has incorrect/incomplete use of enclosing brackets'),
    Address('fe80::7561:c8fb:d3d7:1fa4]', False,
            'Address has incorrect/incomplete use of enclosing brackets'),
    Address('fred.knockinson.org', False, 'DNS lookup failed'),
    Address('tumbleweed.pickles.gov.au', False, 'DNS lookup failed'),
    Address('192.168.122.5:00PS', False, 'Port must be numeric'),
    Address('[fe80::7561:c8fb:d3d7:1fa4]:DOH', False, 'Port must be numeric')
])
def test_valid_addr(addr_object: Address):

    valid, description = valid_addr(addr_object.addr)
    assert valid == addr_object.status
    assert description == addr_object.description
