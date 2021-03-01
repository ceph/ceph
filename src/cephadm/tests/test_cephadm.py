# type: ignore
import mock
from mock import patch
import os
import sys
import unittest

import pytest

with patch('builtins.open', create=True):
    from importlib.machinery import SourceFileLoader
    cd = SourceFileLoader('cephadm', 'cephadm').load_module()

class TestCephAdm(object):

    def test_docker_unit_file(self):
        cd.args = mock.Mock()
        cd.container_path = '/usr/bin/docker'
        r = cd.get_unit_file('9b9d7609-f4d5-4aba-94c8-effa764d96c9')
        assert 'Requires=docker.service' in r
        cd.container_path = '/usr/sbin/podman'
        r = cd.get_unit_file('9b9d7609-f4d5-4aba-94c8-effa764d96c9')
        assert 'Requires=docker.service' not in r

    def test_is_not_fsid(self):
        assert not cd.is_fsid('no-uuid')

    def test_is_fsid(self):
        assert cd.is_fsid('e863154d-33c7-4350-bca5-921e0467e55b')

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
    def test_parse_ipv4_route(self, test_input, expected):
        assert cd._parse_ipv4_route(test_input) == expected

    @pytest.mark.parametrize("test_routes, test_ips, expected", [
        (
"""
::1 dev lo proto kernel metric 256 pref medium
fdbc:7574:21fe:9200::/64 dev wlp2s0 proto ra metric 600 pref medium
fdd8:591e:4969:6363::/64 dev wlp2s0 proto ra metric 600 pref medium
fde4:8dba:82e1::/64 dev eth1 proto kernel metric 256 expires 1844sec pref medium
fe80::/64 dev tun0 proto kernel metric 256 pref medium
fe80::/64 dev wlp2s0 proto kernel metric 600 pref medium
default dev tun0 proto static metric 50 pref medium
default via fe80::2480:28ec:5097:3fe2 dev wlp2s0 proto ra metric 20600 pref medium
""",
"""
1: lo: <LOOPBACK,UP,LOWER_UP> mtu 65536 state UNKNOWN qlen 1000
    inet6 ::1/128 scope host
       valid_lft forever preferred_lft forever
2: eth0: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 state UP qlen 1000
    inet6 fdd8:591e:4969:6363:4c52:cafe:8dd4:dc4/64 scope global temporary dynamic
       valid_lft 86394sec preferred_lft 14394sec
    inet6 fdbc:7574:21fe:9200:4c52:cafe:8dd4:dc4/64 scope global temporary dynamic
       valid_lft 6745sec preferred_lft 3145sec
    inet6 fdd8:591e:4969:6363:103a:abcd:af1f:57f3/64 scope global temporary deprecated dynamic
       valid_lft 86394sec preferred_lft 0sec
    inet6 fdbc:7574:21fe:9200:103a:abcd:af1f:57f3/64 scope global temporary deprecated dynamic
       valid_lft 6745sec preferred_lft 0sec
    inet6 fdd8:591e:4969:6363:a128:1234:2bdd:1b6f/64 scope global temporary deprecated dynamic
       valid_lft 86394sec preferred_lft 0sec
    inet6 fdbc:7574:21fe:9200:a128:1234:2bdd:1b6f/64 scope global temporary deprecated dynamic
       valid_lft 6745sec preferred_lft 0sec
    inet6 fdd8:591e:4969:6363:d581:4321:380b:3905/64 scope global temporary deprecated dynamic
       valid_lft 86394sec preferred_lft 0sec
    inet6 fdbc:7574:21fe:9200:d581:4321:380b:3905/64 scope global temporary deprecated dynamic
       valid_lft 6745sec preferred_lft 0sec
    inet6 fe80::1111:2222:3333:4444/64 scope link noprefixroute
       valid_lft forever preferred_lft forever
    inet6 fde4:8dba:82e1:0:ec4a:e402:e9df:b357/64 scope global temporary dynamic
       valid_lft 1074sec preferred_lft 1074sec
    inet6 fde4:8dba:82e1:0:5054:ff:fe72:61af/64 scope global dynamic mngtmpaddr
       valid_lft 1074sec preferred_lft 1074sec
12: tun0: <POINTOPOINT,MULTICAST,NOARP,UP,LOWER_UP> mtu 1500 state UNKNOWN qlen 100
    inet6 fe80::cafe:cafe:cafe:cafe/64 scope link stable-privacy
       valid_lft forever preferred_lft forever
""",
            {
                "::1": ["::1"],
                "fdbc:7574:21fe:9200::/64": ["fdbc:7574:21fe:9200:4c52:cafe:8dd4:dc4",
                                             "fdbc:7574:21fe:9200:103a:abcd:af1f:57f3",
                                             "fdbc:7574:21fe:9200:a128:1234:2bdd:1b6f",
                                             "fdbc:7574:21fe:9200:d581:4321:380b:3905"],
                "fdd8:591e:4969:6363::/64": ["fdd8:591e:4969:6363:4c52:cafe:8dd4:dc4",
                                             "fdd8:591e:4969:6363:103a:abcd:af1f:57f3",
                                             "fdd8:591e:4969:6363:a128:1234:2bdd:1b6f",
                                             "fdd8:591e:4969:6363:d581:4321:380b:3905"],
                "fde4:8dba:82e1::/64": ["fde4:8dba:82e1:0:ec4a:e402:e9df:b357",
                                        "fde4:8dba:82e1:0:5054:ff:fe72:61af"],
                "fe80::/64": ["fe80::1111:2222:3333:4444",
                              "fe80::cafe:cafe:cafe:cafe"]
            }
        )])
    def test_parse_ipv6_route(self, test_routes, test_ips, expected):
        assert cd._parse_ipv6_route(test_routes, test_ips) == expected

    def test_is_ipv6(self):
        cd.logger = mock.Mock()
        for good in ("[::1]", "::1",
                     "fff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"):
            assert cd.is_ipv6(good)
        for bad in ("127.0.0.1",
                    "ffff:ffff:ffff:ffff:ffff:ffff:ffff:fffg",
                    "1:2:3:4:5:6:7:8:9", "fd00::1::1", "[fg::1]"):
            assert not cd.is_ipv6(bad)

    def test_unwrap_ipv6(self):
        def unwrap_test(address, expected):
            assert cd.unwrap_ipv6(address) == expected

        tests = [
            ('::1', '::1'), ('[::1]', '::1'),
            ('[fde4:8dba:82e1:0:5054:ff:fe6a:357]', 'fde4:8dba:82e1:0:5054:ff:fe6a:357'),
            ('can actually be any string', 'can actually be any string'),
            ('[but needs to be stripped] ', '[but needs to be stripped] ')]
        for address, expected in tests:
            unwrap_test(address, expected)

    def test_wrap_ipv6(self):
        def wrap_test(address, expected):
            assert cd.wrap_ipv6(address) == expected

        tests = [
            ('::1', '[::1]'), ('[::1]', '[::1]'),
            ('fde4:8dba:82e1:0:5054:ff:fe6a:357',
             '[fde4:8dba:82e1:0:5054:ff:fe6a:357]'),
            ('myhost.example.com', 'myhost.example.com'),
            ('192.168.0.1', '192.168.0.1'),
            ('', ''), ('fd00::1::1', 'fd00::1::1')]
        for address, expected in tests:
            wrap_test(address, expected)

    @mock.patch('cephadm.call_throws')
    @mock.patch('cephadm.get_parm')
    def test_registry_login(self, get_parm, call_throws):

        # test normal valid login with url, username and password specified
        call_throws.return_value = '', '', 0
        args = cd._parse_args(['registry-login', '--registry-url', 'sample-url', '--registry-username', 'sample-user', '--registry-password', 'sample-pass'])
        cd.args = args
        retval = cd.command_registry_login()
        assert retval == 0

        # test bad login attempt with invalid arguments given
        args = cd._parse_args(['registry-login', '--registry-url', 'bad-args-url'])
        cd.args = args
        with pytest.raises(Exception) as e:
            assert cd.command_registry_login()
        assert str(e.value) == ('Invalid custom registry arguments received. To login to a custom registry include '
                                '--registry-url, --registry-username and --registry-password options or --registry-json option')

        # test normal valid login with json file
        get_parm.return_value = {"url": "sample-url", "username": "sample-username", "password": "sample-password"}
        args = cd._parse_args(['registry-login', '--registry-json', 'sample-json'])
        cd.args = args
        retval = cd.command_registry_login()
        assert retval == 0

        # test bad login attempt with bad json file
        get_parm.return_value = {"bad-json": "bad-json"}
        args = cd._parse_args(['registry-login', '--registry-json', 'sample-json'])
        cd.args = args
        with pytest.raises(Exception) as e:
            assert cd.command_registry_login()
        assert str(e.value) == ("json provided for custom registry login did not include all necessary fields. "
                        "Please setup json file as\n"
                        "{\n"
                          " \"url\": \"REGISTRY_URL\",\n"
                          " \"username\": \"REGISTRY_USERNAME\",\n"
                          " \"password\": \"REGISTRY_PASSWORD\"\n"
                        "}\n")

        # test login attempt with valid arguments where login command fails
        call_throws.side_effect = Exception
        args = cd._parse_args(['registry-login', '--registry-url', 'sample-url', '--registry-username', 'sample-user', '--registry-password', 'sample-pass'])
        cd.args = args
        with pytest.raises(Exception) as e:
            cd.command_registry_login()
        assert str(e.value) == "Failed to login to custom registry @ sample-url as sample-user with given password"

    def test_get_image_info_from_inspect(self):
        # podman
        out = """204a01f9b0b6710dd0c0af7f37ce7139c47ff0f0105d778d7104c69282dfbbf1,["docker.io/ceph/ceph@sha256:1cc9b824e1b076cdff52a9aa3f0cc8557d879fb2fbbba0cafed970aca59a3992"]"""
        r = cd.get_image_info_from_inspect(out, 'registry/ceph/ceph:latest')
        assert r == {
            'image_id': '204a01f9b0b6710dd0c0af7f37ce7139c47ff0f0105d778d7104c69282dfbbf1',
            'repo_digest': 'docker.io/ceph/ceph@sha256:1cc9b824e1b076cdff52a9aa3f0cc8557d879fb2fbbba0cafed970aca59a3992'
        }

        # docker
        out = """sha256:16f4549cf7a8f112bbebf7946749e961fbbd1b0838627fe619aab16bc17ce552,["quay.ceph.io/ceph-ci/ceph@sha256:4e13da36c1bd6780b312a985410ae678984c37e6a9493a74c87e4a50b9bda41f"]"""
        r = cd.get_image_info_from_inspect(out, 'registry/ceph/ceph:latest')
        assert r == {
            'image_id': '16f4549cf7a8f112bbebf7946749e961fbbd1b0838627fe619aab16bc17ce552',
            'repo_digest': 'quay.ceph.io/ceph-ci/ceph@sha256:4e13da36c1bd6780b312a985410ae678984c37e6a9493a74c87e4a50b9bda41f'
        }

    def test_dict_get(self):
        result = cd.dict_get({'a': 1}, 'a', require=True)
        assert result == 1
        result = cd.dict_get({'a': 1}, 'b')
        assert result is None
        result = cd.dict_get({'a': 1}, 'b', default=2)
        assert result == 2

    def test_dict_get_error(self):
        with pytest.raises(cd.Error):
            cd.dict_get({'a': 1}, 'b', require=True)

    def test_dict_get_join(self):
        result = cd.dict_get_join({'foo': ['a', 'b']}, 'foo')
        assert result == 'a\nb'
        result = cd.dict_get_join({'foo': [1, 2]}, 'foo')
        assert result == '1\n2'
        result = cd.dict_get_join({'bar': 'a'}, 'bar')
        assert result == 'a'
        result = cd.dict_get_join({'a': 1}, 'a')
        assert result == 1

    def test_last_local_images(self):
        out = '''
docker.io/ceph/daemon-base@
docker.io/ceph/ceph:v15.2.5
docker.io/ceph/daemon-base:octopus
        '''
        image = cd._filter_last_local_ceph_image(out)
        assert image == 'docker.io/ceph/ceph:v15.2.5'


class TestCustomContainer(unittest.TestCase):
    cc: cd.CustomContainer

    def setUp(self):
        self.cc = cd.CustomContainer(
            'e863154d-33c7-4350-bca5-921e0467e55b',
            'container',
            config_json={
                'entrypoint': 'bash',
                'gid': 1000,
                'args': [
                    '--no-healthcheck',
                    '-p 6800:6800'
                ],
                'envs': ['SECRET=password'],
                'ports': [8080, 8443],
                'volume_mounts': {
                    '/CONFIG_DIR': '/foo/conf',
                    'bar/config': '/bar:ro'
                },
                'bind_mounts': [
                    [
                        'type=bind',
                        'source=/CONFIG_DIR',
                        'destination=/foo/conf',
                        ''
                    ],
                    [
                        'type=bind',
                        'source=bar/config',
                        'destination=/bar:ro',
                        'ro=true'
                    ]
                ]
            },
            image='docker.io/library/hello-world:latest'
        )

    def test_entrypoint(self):
        self.assertEqual(self.cc.entrypoint, 'bash')

    def test_uid_gid(self):
        self.assertEqual(self.cc.uid, 65534)
        self.assertEqual(self.cc.gid, 1000)

    def test_ports(self):
        self.assertEqual(self.cc.ports, [8080, 8443])

    def test_get_container_args(self):
        result = self.cc.get_container_args()
        self.assertEqual(result, [
            '--no-healthcheck',
            '-p 6800:6800'
        ])

    def test_get_container_envs(self):
        result = self.cc.get_container_envs()
        self.assertEqual(result, ['SECRET=password'])

    def test_get_container_mounts(self):
        result = self.cc.get_container_mounts('/xyz')
        self.assertDictEqual(result, {
            '/CONFIG_DIR': '/foo/conf',
            '/xyz/bar/config': '/bar:ro'
        })

    def test_get_container_binds(self):
        result = self.cc.get_container_binds('/xyz')
        self.assertEqual(result, [
            [
                'type=bind',
                'source=/CONFIG_DIR',
                'destination=/foo/conf',
                ''
            ],
            [
                'type=bind',
                'source=/xyz/bar/config',
                'destination=/bar:ro',
                'ro=true'
            ]
        ])
