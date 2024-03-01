import json
from textwrap import dedent
from unittest import mock

import pytest

from tests.fixtures import with_cephadm_ctx, cephadm_fs, import_cephadm

from cephadmlib.host_facts import _parse_ipv4_route, _parse_ipv6_route
from cephadmlib.net_utils import get_ipv6_address

_cephadm = import_cephadm()


class TestCommandListNetworks:
    @pytest.mark.parametrize("test_input, expected", [
        (
            dedent("""
            default via 192.168.178.1 dev enxd89ef3f34260 proto dhcp metric 100
            10.0.0.0/8 via 10.4.0.1 dev tun0 proto static metric 50
            10.3.0.0/21 via 10.4.0.1 dev tun0 proto static metric 50
            10.4.0.1 dev tun0 proto kernel scope link src 10.4.0.2 metric 50
            137.1.0.0/16 via 10.4.0.1 dev tun0 proto static metric 50
            138.1.0.0/16 via 10.4.0.1 dev tun0 proto static metric 50
            139.1.0.0/16 via 10.4.0.1 dev tun0 proto static metric 50
            140.1.0.0/17 via 10.4.0.1 dev tun0 proto static metric 50
            141.1.0.0/16 via 10.4.0.1 dev tun0 proto static metric 50
            172.16.100.34 via 172.16.100.34 dev eth1 proto kernel scope link src 172.16.100.34
            192.168.122.1 dev ens3 proto dhcp scope link src 192.168.122.236 metric 100
            169.254.0.0/16 dev docker0 scope link metric 1000
            172.17.0.0/16 dev docker0 proto kernel scope link src 172.17.0.1
            192.168.39.0/24 dev virbr1 proto kernel scope link src 192.168.39.1 linkdown
            192.168.122.0/24 dev virbr0 proto kernel scope link src 192.168.122.1 linkdown
            192.168.178.0/24 dev enxd89ef3f34260 proto kernel scope link src 192.168.178.28 metric 100
            192.168.178.1 dev enxd89ef3f34260 proto static scope link metric 100
            195.135.221.12 via 192.168.178.1 dev enxd89ef3f34260 proto static metric 100
            """),
            {
                '172.16.100.34/32': {'eth1': {'172.16.100.34'}},
                '192.168.122.1/32': {'ens3': {'192.168.122.236'}},
                '10.4.0.1/32': {'tun0': {'10.4.0.2'}},
                '172.17.0.0/16': {'docker0': {'172.17.0.1'}},
                '192.168.39.0/24': {'virbr1': {'192.168.39.1'}},
                '192.168.122.0/24': {'virbr0': {'192.168.122.1'}},
                '192.168.178.0/24': {'enxd89ef3f34260': {'192.168.178.28'}}
            }
        ), (
            dedent("""
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
            192.168.122.0/24 dev virbr0 proto kernel scope link src 192.168.122.1 linkdown
            192.168.122.0/24 dev virbr0 proto kernel scope link src 192.168.122.1 linkdown
            192.168.122.0/24 dev virbr0 proto kernel scope link src 192.168.122.1 linkdown
            """),
            {
                '10.3.64.0/24': {'eno1': {'10.3.64.23', '10.3.64.27'}},
                '10.88.0.0/16': {'cni-podman0': {'10.88.0.1'}},
                '172.21.3.1/32': {'tun0': {'172.21.3.2'}},
                '192.168.122.0/24': {'virbr0': {'192.168.122.1'}}
            }
        ),
    ])
    def test_parse_ipv4_route(self, test_input, expected):
        assert _parse_ipv4_route(test_input) == expected

    @pytest.mark.parametrize("test_routes, test_ips, expected", [
        (
            dedent("""
            ::1 dev lo proto kernel metric 256 pref medium
            fe80::/64 dev eno1 proto kernel metric 100 pref medium
            fe80::/64 dev br-3d443496454c proto kernel metric 256 linkdown pref medium
            fe80::/64 dev tun0 proto kernel metric 256 pref medium
            fe80::/64 dev br-4355f5dbb528 proto kernel metric 256 pref medium
            fe80::/64 dev docker0 proto kernel metric 256 linkdown pref medium
            fe80::/64 dev cni-podman0 proto kernel metric 256 linkdown pref medium
            fe80::/64 dev veth88ba1e8 proto kernel metric 256 pref medium
            fe80::/64 dev vethb6e5fc7 proto kernel metric 256 pref medium
            fe80::/64 dev vethaddb245 proto kernel metric 256 pref medium
            fe80::/64 dev vethbd14d6b proto kernel metric 256 pref medium
            fe80::/64 dev veth13e8fd2 proto kernel metric 256 pref medium
            fe80::/64 dev veth1d3aa9e proto kernel metric 256 pref medium
            fe80::/64 dev vethe485ca9 proto kernel metric 256 pref medium
            """),
            dedent("""
            1: lo: <LOOPBACK,UP,LOWER_UP> mtu 65536 state UNKNOWN qlen 1000
                inet6 ::1/128 scope host 
                   valid_lft forever preferred_lft forever
            2: eno1: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 state UP qlen 1000
                inet6 fe80::225:90ff:fee5:26e8/64 scope link noprefixroute 
                   valid_lft forever preferred_lft forever
            6: br-3d443496454c: <NO-CARRIER,BROADCAST,MULTICAST,UP> mtu 1500 state DOWN 
                inet6 fe80::42:23ff:fe9d:ee4/64 scope link 
                   valid_lft forever preferred_lft forever
            7: br-4355f5dbb528: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 state UP 
                inet6 fe80::42:6eff:fe35:41fe/64 scope link 
                   valid_lft forever preferred_lft forever
            8: docker0: <NO-CARRIER,BROADCAST,MULTICAST,UP> mtu 1500 state DOWN 
                inet6 fe80::42:faff:fee6:40a0/64 scope link 
                   valid_lft forever preferred_lft forever
            11: tun0: <POINTOPOINT,MULTICAST,NOARP,UP,LOWER_UP> mtu 1500 state UNKNOWN qlen 100
                inet6 fe80::98a6:733e:dafd:350/64 scope link stable-privacy 
                   valid_lft forever preferred_lft forever
            28: cni-podman0: <NO-CARRIER,BROADCAST,MULTICAST,UP> mtu 1500 state DOWN qlen 1000
                inet6 fe80::3449:cbff:fe89:b87e/64 scope link 
                   valid_lft forever preferred_lft forever
            31: vethaddb245@if30: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 state UP 
                inet6 fe80::90f7:3eff:feed:a6bb/64 scope link 
                   valid_lft forever preferred_lft forever
            33: veth88ba1e8@if32: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 state UP 
                inet6 fe80::d:f5ff:fe73:8c82/64 scope link 
                   valid_lft forever preferred_lft forever
            35: vethbd14d6b@if34: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 state UP 
                inet6 fe80::b44f:8ff:fe6f:813d/64 scope link 
                   valid_lft forever preferred_lft forever
            37: vethb6e5fc7@if36: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 state UP 
                inet6 fe80::4869:c6ff:feaa:8afe/64 scope link 
                   valid_lft forever preferred_lft forever
            39: veth13e8fd2@if38: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 state UP 
                inet6 fe80::78f4:71ff:fefe:eb40/64 scope link 
                   valid_lft forever preferred_lft forever
            41: veth1d3aa9e@if40: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 state UP 
                inet6 fe80::24bd:88ff:fe28:5b18/64 scope link 
                   valid_lft forever preferred_lft forever
            43: vethe485ca9@if42: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 state UP 
                inet6 fe80::6425:87ff:fe42:b9f0/64 scope link 
                   valid_lft forever preferred_lft forever
            """),
            {
                "fe80::/64": {
                    "eno1": {"fe80::225:90ff:fee5:26e8"},
                    "br-3d443496454c": {"fe80::42:23ff:fe9d:ee4"},
                    "tun0": {"fe80::98a6:733e:dafd:350"},
                    "br-4355f5dbb528": {"fe80::42:6eff:fe35:41fe"},
                    "docker0": {"fe80::42:faff:fee6:40a0"},
                    "cni-podman0": {"fe80::3449:cbff:fe89:b87e"},
                    "veth88ba1e8": {"fe80::d:f5ff:fe73:8c82"},
                    "vethb6e5fc7": {"fe80::4869:c6ff:feaa:8afe"},
                    "vethaddb245": {"fe80::90f7:3eff:feed:a6bb"},
                    "vethbd14d6b": {"fe80::b44f:8ff:fe6f:813d"},
                    "veth13e8fd2": {"fe80::78f4:71ff:fefe:eb40"},
                    "veth1d3aa9e": {"fe80::24bd:88ff:fe28:5b18"},
                    "vethe485ca9": {"fe80::6425:87ff:fe42:b9f0"},
                }
            }
        ),
        (
            dedent("""
            ::1 dev lo proto kernel metric 256 pref medium
            2001:1458:301:eb::100:1a dev ens20f0 proto kernel metric 100 pref medium
            2001:1458:301:eb::/64 dev ens20f0 proto ra metric 100 pref medium
            fd01:1458:304:5e::/64 dev ens20f0 proto ra metric 100 pref medium
            fe80::/64 dev ens20f0 proto kernel metric 100 pref medium
            default proto ra metric 100
                    nexthop via fe80::46ec:ce00:b8a0:d3c8 dev ens20f0 weight 1
                    nexthop via fe80::46ec:ce00:b8a2:33c8 dev ens20f0 weight 1 pref medium
            """),
            dedent("""
            1: lo: <LOOPBACK,UP,LOWER_UP> mtu 65536 state UNKNOWN qlen 1000
                inet6 ::1/128 scope host
                   valid_lft forever preferred_lft forever
            2: ens20f0: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 state UP qlen 1000
                inet6 2001:1458:301:eb::100:1a/128 scope global dynamic noprefixroute
                   valid_lft 590879sec preferred_lft 590879sec
                inet6 fe80::2e60:cff:fef8:da41/64 scope link noprefixroute
                   valid_lft forever preferred_lft forever
                inet6 fe80::2e60:cff:fef8:da41/64 scope link noprefixroute
                   valid_lft forever preferred_lft forever
                inet6 fe80::2e60:cff:fef8:da41/64 scope link noprefixroute
                   valid_lft forever preferred_lft forever
            """),
            {
                '2001:1458:301:eb::100:1a/128': {
                    'ens20f0': {
                        '2001:1458:301:eb::100:1a'
                    },
                },
                '2001:1458:301:eb::/64': {
                    'ens20f0': set(),
                },
                'fe80::/64': {
                    'ens20f0': {'fe80::2e60:cff:fef8:da41'},
                },
                'fd01:1458:304:5e::/64': {
                    'ens20f0': set()
                },
            }
        ),
        (
            dedent("""
            ::1 dev lo proto kernel metric 256 pref medium
            fe80::/64 dev ceph-brx proto kernel metric 256 pref medium
            fe80::/64 dev brx.0 proto kernel metric 256 pref medium
            default via fe80::327c:5e00:6487:71e0 dev enp3s0f1 proto ra metric 1024 expires 1790sec hoplimit 64 pref medium            """),
            dedent("""
            1: lo: <LOOPBACK,UP,LOWER_UP> mtu 65536 state UNKNOWN qlen 1000
                inet6 ::1/128 scope host
                   valid_lft forever preferred_lft forever
            5: enp3s0f1: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 state UP qlen 1000
                inet6 fe80::ec4:7aff:fe8f:cb83/64 scope link noprefixroute
                   valid_lft forever preferred_lft forever
            6: ceph-brx: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 state UP qlen 1000
                inet6 fe80::d8a1:69ff:fede:8f58/64 scope link
                   valid_lft forever preferred_lft forever
            7: brx.0@eno1: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 state UP qlen 1000
                inet6 fe80::a4cb:54ff:fecc:f2a2/64 scope link
                   valid_lft forever preferred_lft forever
            """),
            {
                'fe80::/64': {
                    'brx.0': {'fe80::a4cb:54ff:fecc:f2a2'},
                    'ceph-brx': {'fe80::d8a1:69ff:fede:8f58'}
                }
            }
        ),
    ])
    def test_parse_ipv6_route(self, test_routes, test_ips, expected):
        assert _parse_ipv6_route(test_routes, test_ips) == expected

    @mock.patch('cephadmlib.net_utils.read_file')
    def test_get_ipv6_addr(self, _read_file):
        proc_net_if_net6 = """fe80000000000000505400fffe347999 02 40 20 80     eth0
fe80000000000000505400fffe04c154 03 40 20 80     eth1
00000000000000000000000000000001 01 80 10 80       lo"""
        _read_file.return_value = proc_net_if_net6

        ipv6_addr = get_ipv6_address('eth0')
        assert ipv6_addr == 'fe80::5054:ff:fe34:7999/64'

        ipv6_addr = get_ipv6_address('eth1')
        assert ipv6_addr == 'fe80::5054:ff:fe04:c154/64'

    @mock.patch('cephadmlib.host_facts.call_throws')
    @mock.patch('cephadmlib.host_facts.find_executable')
    def test_command_list_networks(self, _find_exe, _call_throws, cephadm_fs, capsys):
        _call_throws.return_value = ('10.4.0.1 dev tun0 proto kernel scope link src 10.4.0.2 metric 50\n', '', '')
        _find_exe.return_value = 'ip'
        with with_cephadm_ctx([]) as ctx:
            _cephadm.command_list_networks(ctx)
            assert json.loads(capsys.readouterr().out) == {
                '10.4.0.1/32': {'tun0': ['10.4.0.2']}
            }
