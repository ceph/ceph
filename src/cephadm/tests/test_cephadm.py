# type: ignore
import argparse
import mock
import os
import sys
import unittest

import pytest

if sys.version_info >= (3, 3):
    from importlib.machinery import SourceFileLoader
    cd = SourceFileLoader('cephadm', 'cephadm').load_module()
else:
    import imp
    cd = imp.load_source('cephadm', 'cephadm')

class TestCephAdm(object):
    def test_is_fsid(self):
        assert not cd.is_fsid('no-uuid')

    def test__get_parser_image(self):
        args = cd._parse_args(['--image', 'foo', 'version'])
        assert args.image == 'foo'

    def test_CustomValidation(self):
        assert cd._parse_args(['deploy', '--name', 'mon.a', '--fsid', 'fsid'])

        with pytest.raises(SystemExit):
            cd._parse_args(['deploy', '--name', 'wrong', '--fsid', 'fsid'])

    @pytest.mark.parametrize("test_input, expected", [
        ("podman version 1.6.2", (1,6,2)),
        ("podman version 1.6.2-stable2", (1,6,2)),
    ])
    def test_parse_podman_version(self, test_input, expected):
        assert cd._parse_podman_version(test_input) == expected

    def test_parse_podman_version_invalid(self):
        with pytest.raises(ValueError) as res:
            cd._parse_podman_version('podman version inval.id')
        assert 'inval' in str(res.value)

    @pytest.mark.parametrize("test_input, expected", [
        (
"""
default via 192.168.178.1 dev enxd89ef3f34260 proto dhcp metric 100
10.0.0.0/8 via 10.4.0.1 dev tun0 proto static metric 50
10.3.0.0/21 via 10.4.0.1 dev tun0 proto static metric 50
10.4.0.1 dev tun0 proto kernel scope link src 10.4.0.2 metric 50
137.1.0.0/16 via 10.4.0.1 dev tun0 proto static metric 50
138.1.0.0/16 via 10.4.0.1 dev tun0 proto static metric 50
139.1.0.0/16 via 10.4.0.1 dev tun0 proto static metric 50
140.1.0.0/17 via 10.4.0.1 dev tun0 proto static metric 50
141.1.0.0/16 via 10.4.0.1 dev tun0 proto static metric 50
169.254.0.0/16 dev docker0 scope link metric 1000
172.17.0.0/16 dev docker0 proto kernel scope link src 172.17.0.1
192.168.39.0/24 dev virbr1 proto kernel scope link src 192.168.39.1 linkdown
192.168.122.0/24 dev virbr0 proto kernel scope link src 192.168.122.1 linkdown
192.168.178.0/24 dev enxd89ef3f34260 proto kernel scope link src 192.168.178.28 metric 100
192.168.178.1 dev enxd89ef3f34260 proto static scope link metric 100
195.135.221.12 via 192.168.178.1 dev enxd89ef3f34260 proto static metric 100
""",
            {
                '10.4.0.1': ['10.4.0.2'],
                '172.17.0.0/16': ['172.17.0.1'],
                '192.168.39.0/24': ['192.168.39.1'],
                '192.168.122.0/24': ['192.168.122.1'],
                '192.168.178.0/24': ['192.168.178.28']
            }
        ),        (
"""
default via 10.3.64.1 dev eno1 proto static metric 100
10.3.64.0/24 dev eno1 proto kernel scope link src 10.3.64.23 metric 100
10.3.64.0/24 dev eno1 proto kernel scope link src 10.3.64.27 metric 100
10.88.0.0/16 dev cni-podman0 proto kernel scope link src 10.88.0.1 linkdown
172.21.0.0/20 via 172.21.3.189 dev tun0
172.21.1.0/20 via 172.21.3.189 dev tun0
172.21.2.1 via 172.21.3.189 dev tun0
172.21.3.1 dev tun0 proto kernel scope link src 172.21.3.2
172.21.4.0/24 via 172.21.3.1 dev tun0
172.21.5.0/24 via 172.21.3.1 dev tun0
172.21.6.0/24 via 172.21.3.1 dev tun0
172.21.7.0/24 via 172.21.3.1 dev tun0
192.168.122.0/24 dev virbr0 proto kernel scope link src 192.168.122.1 linkdown
""",
            {
                '10.3.64.0/24': ['10.3.64.23', '10.3.64.27'],
                '10.88.0.0/16': ['10.88.0.1'],
                '172.21.3.1': ['172.21.3.2'],
                '192.168.122.0/24': ['192.168.122.1']}
        ),
    ])
    def test_parse_ip_route(self, test_input, expected):
        assert cd._parse_ip_route(test_input) == expected
