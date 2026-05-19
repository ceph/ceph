import json
from textwrap import dedent

from cephadmlib.host_facts import (
    _merge_ipv4_network_dicts,
    _parse_ipv4_bgp_route,
    _parse_ipv4_lo_route,
    _parse_ipv4_route,
    _parse_ipv6_bgp_route,
)


def _merged_ipv4(out: str, *, allow_lo: bool = False):
    r = _parse_ipv4_route(out, allow_lo)
    if allow_lo:
        _merge_ipv4_network_dicts(r, _parse_ipv4_lo_route(out))
    return r


class TestLoopbackRouteParsing:
    def test_parse_ipv4_route_omits_127_on_lo(self):
        test_input = dedent(
            """
            192.168.1.0/24 dev eth0 proto kernel scope link src 192.168.1.1
            127.0.0.0/8 dev lo proto kernel scope link src 127.0.0.1
            """
        )
        expected = {'192.168.1.0/24': {'eth0': {'192.168.1.1'}}}
        assert _parse_ipv4_route(test_input) == expected

    def test_parse_ipv4_route_includes_non_loopback_on_lo(self):
        test_input = dedent(
            """
            192.168.1.0/24 dev eth0 proto kernel scope link src 192.168.1.1
            10.168.100.0/24 dev lo proto kernel scope link src 10.168.100.10
            """
        )
        expected_with_lo = {
            '192.168.1.0/24': {'eth0': {'192.168.1.1'}},
            '10.168.100.0/24': {'lo': {'10.168.100.10'}},
        }
        assert _parse_ipv4_route(test_input) == {'192.168.1.0/24': {'eth0': {'192.168.1.1'}}}
        assert _parse_ipv4_route(test_input, allow_lo_routes=True) == expected_with_lo

    def test_parse_ipv4_lo_route_adds_lo_host_route_without_src(self):
        # ``ip route add …/32 dev lo proto bgp`` prints no ``src`` line.
        test_input = dedent(
            """
            192.168.100.0/24 dev ens3 proto kernel scope link src 192.168.100.100
            10.168.100.10 dev lo proto bgp scope link
            """
        )
        expected = {
            '192.168.100.0/24': {'ens3': {'192.168.100.100'}},
            '10.168.100.10/32': {'lo': {'10.168.100.10'}},
        }
        assert _merged_ipv4(test_input, allow_lo=True) == expected

    def test_parse_ipv4_lo_route_scope_host_on_lo(self):
        test_input = dedent(
            """
            192.168.100.0/24 dev ens3 proto kernel scope link src 192.168.100.100
            10.10.10.90 dev lo proto kernel scope host
            """
        )
        assert _parse_ipv4_route(test_input) == {
            '192.168.100.0/24': {'ens3': {'192.168.100.100'}},
        }
        assert _merged_ipv4(test_input, allow_lo=True) == {
            '192.168.100.0/24': {'ens3': {'192.168.100.100'}},
            '10.10.10.90/32': {'lo': {'10.10.10.90'}},
        }

    def test_parse_ipv4_lo_route_bgp_scope_host_on_lo(self):
        test_input = dedent(
            """
            10.10.10.10 dev lo proto bgp scope host
            """
        )
        assert _parse_ipv4_route(test_input) == {}
        assert _parse_ipv4_lo_route(test_input) == {
            '10.10.10.10/32': {'lo': {'10.10.10.10'}},
        }

    def test_parse_ipv4_lo_route_dummy_scope_host(self):
        # Host-scoped /32 on dummy (tracker.ceph.com/issues/76229).
        test_input = dedent(
            """
            192.168.100.0/24 dev ens3 proto kernel scope link src 192.168.100.100
            10.1.0.40 dev dummy0 proto kernel scope host src 10.1.0.40
            """
        )
        expected = {
            '192.168.100.0/24': {'ens3': {'192.168.100.100'}},
            '10.1.0.40/32': {'dummy0': {'10.1.0.40'}},
        }
        assert _merged_ipv4(test_input, allow_lo=True) == expected

    def test_parse_ipv4_bgp_route_ecmp_nexthops(self):
        # From ``ip -j route ls proto bgp`` on a BGP host (ceph-node-0).
        bgp_input = json.dumps(
            [
                {
                    'dst': '192.168.100.5',
                    'protocol': 'bgp',
                    'prefsrc': '192.168.100.11',
                    'flags': [],
                    'nexthops': [
                        {
                            'gateway': '169.254.0.1',
                            'dev': 'ens1f0np0',
                            'weight': 1,
                            'flags': ['onlink'],
                        },
                        {
                            'gateway': '169.254.0.1',
                            'dev': 'ens1f1np1',
                            'weight': 1,
                            'flags': ['onlink'],
                        },
                    ],
                },
                {
                    'dst': '192.168.100.6',
                    'protocol': 'bgp',
                    'prefsrc': '192.168.100.11',
                    'flags': [],
                    'nexthops': [
                        {
                            'gateway': '169.254.0.1',
                            'dev': 'ens1f0np0',
                            'weight': 1,
                            'flags': ['onlink'],
                        },
                        {
                            'gateway': '169.254.0.1',
                            'dev': 'ens1f1np1',
                            'weight': 1,
                            'flags': ['onlink'],
                        },
                    ],
                },
                {
                    'dst': '10.0.1.11',
                    'protocol': 'bgp',
                    'prefsrc': '10.0.1.11',
                    'flags': [],
                    'nexthops': [
                        {
                            'gateway': '169.254.0.1',
                            'dev': 'ens1f0np0',
                            'weight': 1,
                            'flags': ['onlink'],
                        },
                        {
                            'gateway': '169.254.0.1',
                            'dev': 'ens1f1np1',
                            'weight': 1,
                            'flags': ['onlink'],
                        },
                    ],
                },
            ]
        )
        expected = {
            '192.168.100.5/32': {
                'ens1f0np0': {'192.168.100.11'},
                'ens1f1np1': {'192.168.100.11'},
            },
            '192.168.100.6/32': {
                'ens1f0np0': {'192.168.100.11'},
                'ens1f1np1': {'192.168.100.11'},
            },
            '10.0.1.11/32': {
                'ens1f0np0': {'10.0.1.11'},
                'ens1f1np1': {'10.0.1.11'},
            },
        }
        assert _parse_ipv4_bgp_route(bgp_input) == expected

    def test_parse_ipv4_bgp_route_ecmp_without_protocol_field(self):
        # ``ip -j route ls proto bgp`` may omit ``protocol`` on filtered output.
        bgp_input = json.dumps(
            [
                {
                    'dst': '192.168.100.5',
                    'prefsrc': '192.168.100.11',
                    'metric': 20,
                    'flags': [],
                    'nexthops': [
                        {
                            'gateway': '169.254.0.1',
                            'dev': 'ens1f0np0',
                            'weight': 1,
                            'flags': ['onlink'],
                        },
                        {
                            'gateway': '169.254.0.1',
                            'dev': 'ens1f1np1',
                            'weight': 1,
                            'flags': ['onlink'],
                        },
                    ],
                },
                {
                    'dst': '192.168.100.6',
                    'prefsrc': '192.168.100.11',
                    'metric': 20,
                    'flags': [],
                    'nexthops': [
                        {
                            'gateway': '169.254.0.1',
                            'dev': 'ens1f0np0',
                            'weight': 1,
                            'flags': ['onlink'],
                        },
                        {
                            'gateway': '169.254.0.1',
                            'dev': 'ens1f1np1',
                            'weight': 1,
                            'flags': ['onlink'],
                        },
                    ],
                },
            ]
        )
        expected = {
            '192.168.100.5/32': {
                'ens1f0np0': {'192.168.100.11'},
                'ens1f1np1': {'192.168.100.11'},
            },
            '192.168.100.6/32': {
                'ens1f0np0': {'192.168.100.11'},
                'ens1f1np1': {'192.168.100.11'},
            },
        }
        assert _parse_ipv4_bgp_route(bgp_input) == expected

    def test_parse_ipv4_bgp_route_lo_without_src(self):
        bgp_input = json.dumps(
            [
                {
                    'dst': '10.168.100.10',
                    'dev': 'lo',
                    'protocol': 'bgp',
                    'scope': 'link',
                    'flags': [],
                }
            ]
        )
        assert _parse_ipv4_bgp_route(bgp_input) == {}
        assert _parse_ipv4_bgp_route(bgp_input, allow_lo_routes=True) == {
            '10.168.100.10/32': {'lo': {'10.168.100.10'}},
        }

    def test_parse_ipv4_bgp_route_skips_default(self):
        bgp_input = json.dumps(
            [
                {
                    'dst': 'default',
                    'protocol': 'bgp',
                    'prefsrc': '192.168.100.11',
                    'flags': [],
                    'nexthops': [
                        {
                            'gateway': '169.254.0.1',
                            'dev': 'ens1f0np0',
                            'weight': 1,
                            'flags': ['onlink'],
                        },
                    ],
                },
                {
                    'dst': '192.168.100.5',
                    'protocol': 'bgp',
                    'prefsrc': '192.168.100.11',
                    'flags': [],
                    'nexthops': [
                        {
                            'gateway': '169.254.0.1',
                            'dev': 'ens1f0np0',
                            'weight': 1,
                            'flags': ['onlink'],
                        },
                    ],
                },
            ]
        )
        assert _parse_ipv4_bgp_route(bgp_input) == {
            '192.168.100.5/32': {'ens1f0np0': {'192.168.100.11'}},
        }

    def test_parse_ipv4_bgp_route_lo_from_full_table(self):
        # ``ip -j route ls`` entry for ``10.10.10.10 dev lo proto bgp``.
        bgp_input = json.dumps(
            [
                {
                    'dst': '10.10.10.10',
                    'dev': 'lo',
                    'protocol': 'bgp',
                    'scope': 'link',
                    'flags': [],
                }
            ]
        )
        assert _parse_ipv4_bgp_route(bgp_input, allow_lo_routes=True) == {
            '10.10.10.10/32': {'lo': {'10.10.10.10'}},
        }

    def test_parse_ipv6_bgp_route_ecmp_nexthops(self):
        bgp_input = json.dumps(
            [
                {
                    'dst': '2001:db8:100::5',
                    'protocol': 'bgp',
                    'prefsrc': '2001:db8:100::11',
                    'metric': 20,
                    'pref': 'medium',
                    'flags': [],
                    'nexthops': [
                        {
                            'gateway': 'fe80::1',
                            'dev': 'ens1f0np0',
                            'weight': 1,
                            'flags': [],
                        },
                        {
                            'gateway': 'fe80::2',
                            'dev': 'ens1f1np1',
                            'weight': 1,
                            'flags': [],
                        },
                    ],
                },
                {
                    'dst': '2001:db8:200::1:11',
                    'protocol': 'bgp',
                    'prefsrc': '2001:db8:200::1:11',
                    'metric': 20,
                    'pref': 'medium',
                    'flags': [],
                    'nexthops': [
                        {
                            'gateway': 'fe80::a',
                            'dev': 'ens1f0np0',
                            'weight': 1,
                            'flags': [],
                        },
                        {
                            'gateway': 'fe80::b',
                            'dev': 'ens1f1np1',
                            'weight': 1,
                            'flags': [],
                        },
                    ],
                },
            ]
        )
        expected = {
            '2001:db8:100::5/128': {
                'ens1f0np0': {'2001:db8:100::11'},
                'ens1f1np1': {'2001:db8:100::11'},
            },
            '2001:db8:200::1:11/128': {
                'ens1f0np0': {'2001:db8:200::1:11'},
                'ens1f1np1': {'2001:db8:200::1:11'},
            },
        }
        assert _parse_ipv6_bgp_route(bgp_input) == expected

    def test_parse_ipv6_bgp_route_lo_without_src(self):
        bgp_input = json.dumps(
            [
                {
                    'dst': '2001:db8:bad:10::10',
                    'dev': 'lo',
                    'protocol': 'bgp',
                    'scope': 'link',
                    'flags': [],
                }
            ]
        )
        assert _parse_ipv6_bgp_route(bgp_input) == {}
        assert _parse_ipv6_bgp_route(bgp_input, allow_lo_routes=True) == {
            '2001:db8:bad:10::10/128': {'lo': {'2001:db8:bad:10::10'}},
        }
