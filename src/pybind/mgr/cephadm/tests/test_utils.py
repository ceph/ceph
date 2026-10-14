import socket
from unittest import mock

import pytest

from cephadm.utils import resolve_ip
from orchestrator import OrchestratorError


V6 = (socket.AF_INET6, socket.SOCK_STREAM, 6, 'host', ('2001:db8::5', 0, 0, 0))
V4 = (socket.AF_INET, socket.SOCK_STREAM, 6, 'host', ('192.0.2.5', 0))


@pytest.mark.parametrize('addrs, prefer_ipv6, expected', [
    ([V6, V4], False, '192.0.2.5'),
    ([V4, V6], False, '192.0.2.5'),
    ([V6, V4], True, '2001:db8::5'),
    ([V4, V6], True, '2001:db8::5'),
    ([V4], True, '192.0.2.5'),
    ([V6], False, '2001:db8::5'),
])
@mock.patch('cephadm.utils.socket.getaddrinfo')
def test_resolve_ip(getaddrinfo, addrs, prefer_ipv6, expected):
    getaddrinfo.return_value = addrs
    assert resolve_ip('host', prefer_ipv6=prefer_ipv6) == expected


@mock.patch('cephadm.utils.socket.getaddrinfo')
def test_resolve_ip_unresolvable(getaddrinfo):
    getaddrinfo.side_effect = socket.gaierror(-2, 'Name or service not known')
    with pytest.raises(OrchestratorError, match='Cannot resolve ip for host nosuchhost'):
        resolve_ip('nosuchhost')
