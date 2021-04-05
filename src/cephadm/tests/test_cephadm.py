# type: ignore
from typing import List, Optional
import mock
from mock import patch, call
import os
import sys
import unittest
import threading
import time
import errno
import socket
from http.server import HTTPServer
from urllib.request import Request, urlopen
from urllib.error import HTTPError

import pytest

from .fixtures import exporter

with patch('builtins.open', create=True):
    from importlib.machinery import SourceFileLoader
    cd = SourceFileLoader('cephadm', 'cephadm').load_module()

class TestCephAdm(object):

    @staticmethod
    def mock_docker():
        docker = mock.Mock(cd.Docker)
        docker.path = '/usr/bin/docker'
        return docker

    @staticmethod
    def mock_podman():
        podman = mock.Mock(cd.Podman)
        podman.path = '/usr/bin/podman'
        podman.version = (2, 1, 0)
        return podman

    def test_docker_unit_file(self):
        ctx = mock.Mock()
        ctx.container_engine = self.mock_docker()
        r = cd.get_unit_file(ctx, '9b9d7609-f4d5-4aba-94c8-effa764d96c9')
        assert 'Requires=docker.service' in r
        ctx.container_engine = self.mock_podman()
        r = cd.get_unit_file(ctx, '9b9d7609-f4d5-4aba-94c8-effa764d96c9')
        assert 'Requires=docker.service' not in r

    @mock.patch('cephadm.logger')
    def test_attempt_bind(self, logger):
        ctx = None
        address = None
        port = 0

        def os_error(errno):
            _os_error = OSError()
            _os_error.errno = errno
            return _os_error

        for side_effect, expected_exception in (
            (os_error(errno.EADDRINUSE), cd.PortOccupiedError),
            (os_error(errno.EAFNOSUPPORT), OSError),
            (os_error(errno.EADDRNOTAVAIL), OSError),
            (None, None),
        ):
            _socket = mock.Mock()
            _socket.bind.side_effect = side_effect
            try:
                cd.attempt_bind(ctx, _socket, address, port)
            except Exception as e:
                assert isinstance(e, expected_exception)
            else:
                if expected_exception is not None:
                    assert False

    @mock.patch('cephadm.attempt_bind')
    @mock.patch('cephadm.logger')
    def test_port_in_use(self, logger, attempt_bind):
        empty_ctx = None

        assert cd.port_in_use(empty_ctx, 9100) == False

        attempt_bind.side_effect = cd.PortOccupiedError('msg')
        assert cd.port_in_use(empty_ctx, 9100) == True

        os_error = OSError()
        os_error.errno = errno.EADDRNOTAVAIL
        attempt_bind.side_effect = os_error
        assert cd.port_in_use(empty_ctx, 9100) == False

        os_error = OSError()
        os_error.errno = errno.EAFNOSUPPORT
        attempt_bind.side_effect = os_error
        assert cd.port_in_use(empty_ctx, 9100) == False

    @mock.patch('socket.socket')
    @mock.patch('cephadm.logger')
    def test_check_ip_port_success(self, logger, _socket):
        ctx = mock.Mock()
        ctx.skip_ping_check = False  # enables executing port check with `check_ip_port`

        for address, address_family in (
            ('0.0.0.0', socket.AF_INET),
            ('::', socket.AF_INET6),
        ):
            try:
                cd.check_ip_port(ctx, address, 9100)
            except:
                assert False
            else:
                assert _socket.call_args == call(address_family, socket.SOCK_STREAM)

    @mock.patch('socket.socket')
    @mock.patch('cephadm.logger')
    def test_check_ip_port_failure(self, logger, _socket):
        ctx = mock.Mock()
        ctx.skip_ping_check = False  # enables executing port check with `check_ip_port`

        def os_error(errno):
            _os_error = OSError()
            _os_error.errno = errno
            return _os_error

        for address, address_family in (
            ('0.0.0.0', socket.AF_INET),
            ('::', socket.AF_INET6),
        ):
            for side_effect, expected_exception in (
                (os_error(errno.EADDRINUSE), cd.PortOccupiedError),
                (os_error(errno.EADDRNOTAVAIL), OSError),
                (os_error(errno.EAFNOSUPPORT), OSError),
                (None, None),
            ):
                mock_socket_obj = mock.Mock()
                mock_socket_obj.bind.side_effect = side_effect
                _socket.return_value = mock_socket_obj
                try:
                    cd.check_ip_port(ctx, address, 9100)
                except Exception as e:
                    assert isinstance(e, expected_exception)
                else:
                    if side_effect is not None:
                        assert False


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
        ("1.6.2", (1,6,2)),
        ("1.6.2-stable2", (1,6,2)),
    ])
    def test_parse_podman_version(self, test_input, expected):
        assert cd._parse_podman_version(test_input) == expected

    def test_parse_podman_version_invalid(self):
        with pytest.raises(ValueError) as res:
            cd._parse_podman_version('inval.id')
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
        ctx: Optional[cd.CephadmContext] = cd.cephadm_init_ctx(
            ['registry-login', '--registry-url', 'sample-url',
            '--registry-username', 'sample-user', '--registry-password',
            'sample-pass'])
        ctx.container_engine = self.mock_docker()
        assert ctx
        retval = cd.command_registry_login(ctx)
        assert retval == 0

        # test bad login attempt with invalid arguments given
        ctx: Optional[cd.CephadmContext] = cd.cephadm_init_ctx(
            ['registry-login', '--registry-url', 'bad-args-url'])
        assert ctx
        with pytest.raises(Exception) as e:
            assert cd.command_registry_login(ctx)
        assert str(e.value) == ('Invalid custom registry arguments received. To login to a custom registry include '
                                '--registry-url, --registry-username and --registry-password options or --registry-json option')

        # test normal valid login with json file
        get_parm.return_value = {"url": "sample-url", "username": "sample-username", "password": "sample-password"}
        ctx: Optional[cd.CephadmContext] = cd.cephadm_init_ctx(
            ['registry-login', '--registry-json', 'sample-json'])
        ctx.container_engine = self.mock_docker()
        assert ctx
        retval = cd.command_registry_login(ctx)
        assert retval == 0

        # test bad login attempt with bad json file
        get_parm.return_value = {"bad-json": "bad-json"}
        ctx: Optional[cd.CephadmContext] =  cd.cephadm_init_ctx(
            ['registry-login', '--registry-json', 'sample-json'])
        assert ctx
        with pytest.raises(Exception) as e:
            assert cd.command_registry_login(ctx)
        assert str(e.value) == ("json provided for custom registry login did not include all necessary fields. "
                        "Please setup json file as\n"
                        "{\n"
                          " \"url\": \"REGISTRY_URL\",\n"
                          " \"username\": \"REGISTRY_USERNAME\",\n"
                          " \"password\": \"REGISTRY_PASSWORD\"\n"
                        "}\n")

        # test login attempt with valid arguments where login command fails
        call_throws.side_effect = Exception
        ctx: Optional[cd.CephadmContext] = cd.cephadm_init_ctx(
            ['registry-login', '--registry-url', 'sample-url',
            '--registry-username', 'sample-user', '--registry-password',
            'sample-pass'])
        assert ctx
        with pytest.raises(Exception) as e:
            cd.command_registry_login(ctx)
        assert str(e.value) == "Failed to login to custom registry @ sample-url as sample-user with given password"

    def test_get_image_info_from_inspect(self):
        # podman
        out = """204a01f9b0b6710dd0c0af7f37ce7139c47ff0f0105d778d7104c69282dfbbf1,[docker.io/ceph/ceph@sha256:1cc9b824e1b076cdff52a9aa3f0cc8557d879fb2fbbba0cafed970aca59a3992]"""
        r = cd.get_image_info_from_inspect(out, 'registry/ceph/ceph:latest')
        print(r)
        assert r == {
            'image_id': '204a01f9b0b6710dd0c0af7f37ce7139c47ff0f0105d778d7104c69282dfbbf1',
            'repo_digests': ['docker.io/ceph/ceph@sha256:1cc9b824e1b076cdff52a9aa3f0cc8557d879fb2fbbba0cafed970aca59a3992']
        }

        # docker
        out = """sha256:16f4549cf7a8f112bbebf7946749e961fbbd1b0838627fe619aab16bc17ce552,[quay.ceph.io/ceph-ci/ceph@sha256:4e13da36c1bd6780b312a985410ae678984c37e6a9493a74c87e4a50b9bda41f]"""
        r = cd.get_image_info_from_inspect(out, 'registry/ceph/ceph:latest')
        assert r == {
            'image_id': '16f4549cf7a8f112bbebf7946749e961fbbd1b0838627fe619aab16bc17ce552',
            'repo_digests': ['quay.ceph.io/ceph-ci/ceph@sha256:4e13da36c1bd6780b312a985410ae678984c37e6a9493a74c87e4a50b9bda41f']
        }

        # multiple digests (podman)
        out = """e935122ab143a64d92ed1fbb27d030cf6e2f0258207be1baf1b509c466aeeb42,[docker.io/prom/prometheus@sha256:e4ca62c0d62f3e886e684806dfe9d4e0cda60d54986898173c1083856cfda0f4 docker.io/prom/prometheus@sha256:efd99a6be65885c07c559679a0df4ec709604bcdd8cd83f0d00a1a683b28fb6a]"""
        r = cd.get_image_info_from_inspect(out, 'registry/prom/prometheus:latest')
        assert r == {
            'image_id': 'e935122ab143a64d92ed1fbb27d030cf6e2f0258207be1baf1b509c466aeeb42',
            'repo_digests': [
                'docker.io/prom/prometheus@sha256:e4ca62c0d62f3e886e684806dfe9d4e0cda60d54986898173c1083856cfda0f4',
                'docker.io/prom/prometheus@sha256:efd99a6be65885c07c559679a0df4ec709604bcdd8cd83f0d00a1a683b28fb6a',
            ]
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


class TestCephadmExporter(object):
    exporter: cd.CephadmDaemon
    files_created: List[str] = []
    crt = """-----BEGIN CERTIFICATE-----
MIIC1zCCAb8CEFHoZE2MfUVzo53fzzBKAT0wDQYJKoZIhvcNAQENBQAwKjENMAsG
A1UECgwEQ2VwaDEZMBcGA1UECwwQY2VwaGFkbS1leHBvcnRlcjAeFw0yMDExMjUy
MzEwNTVaFw0zMDExMjMyMzEwNTVaMCoxDTALBgNVBAoMBENlcGgxGTAXBgNVBAsM
EGNlcGhhZG0tZXhwb3J0ZXIwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIB
AQCsTfcJcXbREqfx1zTUuEmK+lJn9WWjk0URRF1Z+QgPkascNdkX16PnvhbGwXmF
BTdAcNl7V0U+z4EsGJ7hJsB7qTq6Rb6wNl7r0OxjeWOmB9xbF4Q/KR5yrbM1DA9A
B5fNswrUXViku5Y2jlOAz+ZMBhYxMx0edqhxSn297j04Z6RF4Mvkc43v0FH7Ju7k
O5+0VbdzcOdu37DFpoE4Ll2MZ/GuAHcJ8SD06sEdzFEjRCraav976743XcUlhZGX
ZTTG/Zf/a+wuCjtMG3od7vRFfuRrM5oTE133DuQ5deR7ybcZNDyopDjHF8xB1bAk
IOz4SbP6Q25K99Czm1K+3kMLAgMBAAEwDQYJKoZIhvcNAQENBQADggEBACmtvZb8
dJGHx/WC0/JHxnEJCJM2qnn87ELzbbIQL1w1Yb/I6JQYPgq+WiQPaHaLL9eYsm0l
dFwvrh+WC0JpXDfADnUnkTSB/WpZ2nC+2JxBptrQEuIcqNXpcJd0bKDiHunv04JI
uEVpTAK05dBV38qNmIlu4HyB4OEnuQpyOr9xpIhdxuJ95O9K0j5BIw98ZaEwYNUP
Rm3YlQwfS6R5xaBvL9kyfxyAD2joNj44q6w/5zj4egXVIA5VpkQm8DmMtu0Pd2NG
dzfYRmqrDolh+rty8HiyIxzeDJQ5bj6LKbUkmABvX50nDySVyMfHmt461/n7W65R
CHFLoOmfJJik+Uc=\n-----END CERTIFICATE-----
"""
    key = """-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCsTfcJcXbREqfx
1zTUuEmK+lJn9WWjk0URRF1Z+QgPkascNdkX16PnvhbGwXmFBTdAcNl7V0U+z4Es
GJ7hJsB7qTq6Rb6wNl7r0OxjeWOmB9xbF4Q/KR5yrbM1DA9AB5fNswrUXViku5Y2
jlOAz+ZMBhYxMx0edqhxSn297j04Z6RF4Mvkc43v0FH7Ju7kO5+0VbdzcOdu37DF
poE4Ll2MZ/GuAHcJ8SD06sEdzFEjRCraav976743XcUlhZGXZTTG/Zf/a+wuCjtM
G3od7vRFfuRrM5oTE133DuQ5deR7ybcZNDyopDjHF8xB1bAkIOz4SbP6Q25K99Cz
m1K+3kMLAgMBAAECggEASnAwToMXWsGdjqxzpYasNv9oBIOO0nk4OHp5ffpJUjiT
XM+ip1tA80g7HMjPD/mt4gge3NtaDgWlf4Bve0O7mnEE7x5cgFIs9eG/jkYOF9eD
ilMBjivcfJywNDWujPH60iIMhqyBNEHaZl1ck+S9UJC8m6rCZLvMj40n/5riFfBy
1sjf2uOwcfWrjSj9Ju4wlMI6khSSz2aYC7glQQ/fo2+YArbEUcy60iloPQ6wEgZK
okoVWZA9AehwLcnRjkwd9EVmMMtRGPE/AcP4s/kKA0tRDRicPLN727Ke/yxv+Ppo
hbIZIcOn7soOFAENcodJ4YRSCd++QfCNaVAi7vwWWQKBgQDeBY4vvr+H0brbSjQg
O7Fpqub/fxZY3UoHWDqWs2X4o3qhDqaTQODpuYtCm8YQE//55JoLWKAD0evq5dLS
YLrtC1Vyxf+TA7opCUjWBe+liyndbJdB5q0zF7qdWUtQKGVSWyUWhK8gHa6M64fP
oi83DD7F0OGusTWGtfbceErk/wKBgQDGrJLRo/5xnAH5VmPfNu+S6h0M2qM6CYwe
Y5wHFG2uQQct73adf53SkhvZVmOzJsWQbVnlDOKMhqazcs+7VWRgO5X3naWVcctE
Hggw9MgpbXAWFOI5sNYsCYE58E+fTHjE6O4A3MhMCsze+CIC3sKuPQBBiL9bWSOX
8POswqfl9QKBgDe/nVxPwTgRaaH2l/AgDQRDbY1qE+psZlJBzTRaB5jPM9ONIjaH
a/JELLuk8a7H1tagmC2RK1zKMTriSnWY5FbxKZuQLAR2QyBavHdBNlOTBggbZD+f
9I2Hv8wSx95wxkBPsphc6Lxft5ya55czWjewU3LIaGK9DHuu5TWm3udxAoGBAJGP
PsJ59KIoOwoDUYjpJv3sqPwR9CVBeXeKY3aMcQ+KdUgiejVKmsb8ZYsG0GUhsv3u
ID7BAfsTbG9tXuVR2wjmnymcRwUHKnXtyvKTZVN06vpCsryx4zjAff2FI9ECpjke
r8HSAK41+4QhKEoSC3C9IMLi/dBfrsRTtTSOKZVBAoGBAI2dl5HEIFpufaI4toWM
LO5HFrlXgRDGoc/+Byr5/8ZZpYpU115Ol/q6M+l0koV2ygJ9jeJJEllFWykIDS6F
XxazFI74swAqobHb2ZS/SLhoVxE82DdSeXrjkTvUjNtrW5zs1gIMKBR4nD6H8AqL
iMN28C2bKGao5UHvdER1rGy7
-----END PRIVATE KEY-----
"""
    token = "MyAccessToken"

    @classmethod
    def setup_class(cls):
        # create the ssl files
        fname = os.path.join(os.getcwd(), 'crt')
        with open(fname, 'w') as crt:
            crt.write(cls.crt)
            cls.files_created.append(fname)
        fname = os.path.join(os.getcwd(), 'key')
        with open(fname, 'w') as crt:
            crt.write(cls.key)
            cls.files_created.append(fname)
        fname = os.path.join(os.getcwd(), 'token')
        with open(fname, 'w') as crt:
            crt.write(cls.token)
            cls.files_created.append(fname)
         # start a simple http instance to test the requesthandler
        cls.server = HTTPServer(('0.0.0.0', 9443), cd.CephadmDaemonHandler)
        cls.server.cephadm_cache = cd.CephadmCache()
        cls.server.token = cls.token
        t = threading.Thread(target=cls.server.serve_forever)
        t.daemon = True
        t.start() 

    @classmethod
    def teardown_class(cls):
        cls.server.shutdown()
        assert len(cls.files_created) > 0
        for f in cls.files_created:
            os.remove(f)      
    
    def setup_method(self):
        # re-init the cache for every test
        TestCephadmExporter.server.cephadm_cache = cd.CephadmCache()
    
    def teardown_method(self):
        pass

    def test_files_ready(self):
        assert os.path.exists(os.path.join(os.getcwd(), 'crt'))
        assert os.path.exists(os.path.join(os.getcwd(), 'key'))
        assert os.path.exists(os.path.join(os.getcwd(), 'token'))

    def test_can_run(self, exporter):
        assert exporter.can_run
    
    def test_token_valid(self, exporter):
        assert exporter.token == self.token

    def test_unit_name(self,exporter):
        assert exporter.unit_name
        assert exporter.unit_name == "ceph-foobar-cephadm-exporter.test.service"

    def test_unit_run(self,exporter):
        assert exporter.unit_run
        lines = exporter.unit_run.split('\n')
        assert len(lines) == 2
        assert "cephadm exporter --fsid foobar --id test --port 9443 &" in lines[1]

    def test_binary_path(self, exporter):
        assert os.path.isfile(exporter.binary_path)

    def test_systemd_unit(self, exporter):
        assert exporter.unit_file

    def test_validate_passes(self, exporter):
        config = {
            "crt": self.crt,
            "key": self.key,
            "token": self.token,
        }
        cd.CephadmDaemon.validate_config(config)

    def test_validate_fails(self, exporter):
        config = {
            "key": self.key,
            "token": self.token,
        }
        with pytest.raises(cd.Error):
            cd.CephadmDaemon.validate_config(config)

    def test_port_active(self, exporter):
        assert exporter.port_active == True

    def test_rqst_health_200(self):
        hdrs={"Authorization":f"Bearer {TestCephadmExporter.token}"}
        req=Request("http://localhost:9443/v1/metadata/health",headers=hdrs)
        r = urlopen(req)
        assert r.status == 200

    def test_rqst_all_inactive_500(self):
        hdrs={"Authorization":f"Bearer {TestCephadmExporter.token}"}
        req=Request("http://localhost:9443/v1/metadata",headers=hdrs)
        try:
            r = urlopen(req)
        except HTTPError as e:
            assert e.code == 500

    def test_rqst_no_auth_401(self):
        req=Request("http://localhost:9443/v1/metadata")
        try:
            urlopen(req)
        except HTTPError as e:
            assert e.code == 401
 
    def test_rqst_bad_auth_401(self):
        hdrs={"Authorization":f"Bearer BogusAuthToken"}
        req=Request("http://localhost:9443/v1/metadata",headers=hdrs)
        try:
            urlopen(req)
        except HTTPError as e:
            assert e.code == 401

    def test_rqst_badURL_404(self):
        hdrs={"Authorization":f"Bearer {TestCephadmExporter.token}"}
        req=Request("http://localhost:9443/v1/metazoic",headers=hdrs)
        try:
            urlopen(req)
        except HTTPError as e:
            assert e.code == 404

    def test_rqst_inactive_task_204(self):
        # all tasks initialise as inactive, and then 'go' active as their thread starts
        # so we can pick any task to check for an inactive response (no content)
        hdrs={"Authorization":f"Bearer {TestCephadmExporter.token}"}
        req=Request("http://localhost:9443/v1/metadata/disks",headers=hdrs)
        r = urlopen(req)
        assert r.status == 204

    def test_rqst_active_task_200(self):
        TestCephadmExporter.server.cephadm_cache.tasks['host'] = 'active'
        hdrs={"Authorization":f"Bearer {TestCephadmExporter.token}"}
        req=Request("http://localhost:9443/v1/metadata/host",headers=hdrs)
        r = urlopen(req)
        assert r.status == 200

    def test_rqst_all_206(self):
        TestCephadmExporter.server.cephadm_cache.tasks['disks'] = 'active'
        hdrs={"Authorization":f"Bearer {TestCephadmExporter.token}"}
        req=Request("http://localhost:9443/v1/metadata",headers=hdrs)
        r = urlopen(req)
        assert r.status == 206

    def test_rqst_disks_200(self):
        TestCephadmExporter.server.cephadm_cache.tasks['disks'] = 'active'
        hdrs={"Authorization":f"Bearer {TestCephadmExporter.token}"}
        req=Request("http://localhost:9443/v1/metadata/disks",headers=hdrs)
        r = urlopen(req)
        assert r.status == 200

    def test_thread_exception(self, exporter):
        # run is patched to invoke a mocked scrape_host thread that will raise so 
        # we check here that the exception handler updates the cache object as we'd
        # expect with the error
        exporter.run()
        assert exporter.cephadm_cache.host['scrape_errors']
        assert exporter.cephadm_cache.host['scrape_errors'] == ['ValueError exception: wah']
        assert exporter.cephadm_cache.errors == ['host thread stopped']

    # Test the requesthandler does the right thing with invalid methods...
    # ie. return a "501" - Not Implemented / Unsupported Method
    def test_invalid_method_HEAD(self):
        hdrs={"Authorization":f"Bearer {TestCephadmExporter.token}"}
        req=Request("http://localhost:9443/v1/metadata/health",headers=hdrs, method="HEAD")
        with pytest.raises(HTTPError, match=r"HTTP Error 501: .*") as e:
            urlopen(req)

    def test_invalid_method_DELETE(self):
        hdrs={"Authorization":f"Bearer {TestCephadmExporter.token}"}
        req=Request("http://localhost:9443/v1/metadata/health",headers=hdrs, method="DELETE")
        with pytest.raises(HTTPError, match=r"HTTP Error 501: .*") as e:
            urlopen(req)

    def test_invalid_method_POST(self):
        hdrs={"Authorization":f"Bearer {TestCephadmExporter.token}"}
        req=Request("http://localhost:9443/v1/metadata/health",headers=hdrs, method="POST")
        with pytest.raises(HTTPError, match=r"HTTP Error 501: .*") as e:
            urlopen(req)

    def test_invalid_method_PUT(self):
        hdrs={"Authorization":f"Bearer {TestCephadmExporter.token}"}
        req=Request("http://localhost:9443/v1/metadata/health",headers=hdrs, method="PUT")
        with pytest.raises(HTTPError, match=r"HTTP Error 501: .*") as e:
            urlopen(req)

    def test_invalid_method_CONNECT(self):
        hdrs={"Authorization":f"Bearer {TestCephadmExporter.token}"}
        req=Request("http://localhost:9443/v1/metadata/health",headers=hdrs, method="CONNECT")
        with pytest.raises(HTTPError, match=r"HTTP Error 501: .*") as e:
            urlopen(req)

    def test_invalid_method_TRACE(self):
        hdrs={"Authorization":f"Bearer {TestCephadmExporter.token}"}
        req=Request("http://localhost:9443/v1/metadata/health",headers=hdrs, method="TRACE")
        with pytest.raises(HTTPError, match=r"HTTP Error 501: .*") as e:
            urlopen(req)

    def test_invalid_method_OPTIONS(self):
        hdrs={"Authorization":f"Bearer {TestCephadmExporter.token}"}
        req=Request("http://localhost:9443/v1/metadata/health",headers=hdrs, method="OPTIONS")
        with pytest.raises(HTTPError, match=r"HTTP Error 501: .*") as e:
            urlopen(req)

    def test_invalid_method_PATCH(self):
        hdrs={"Authorization":f"Bearer {TestCephadmExporter.token}"}
        req=Request("http://localhost:9443/v1/metadata/health",headers=hdrs, method="PATCH")
        with pytest.raises(HTTPError, match=r"HTTP Error 501: .*") as e:
            urlopen(req)

    def test_ipv4_subnet(self):
        rc, v, msg = cd.check_subnet('192.168.1.0/24')
        assert rc == 0 and v[0] == 4
    
    def test_ipv4_subnet_list(self):
        rc, v, msg = cd.check_subnet('192.168.1.0/24,10.90.90.0/24')
        assert rc == 0 and not msg
    
    def test_ipv4_subnet_badlist(self):
        rc, v, msg = cd.check_subnet('192.168.1.0/24,192.168.1.1')
        assert rc == 1 and msg

    def test_ipv4_subnet_mixed(self):
        rc, v, msg = cd.check_subnet('192.168.100.0/24,fe80::/64')
        assert rc == 0 and v == [4,6]

    def test_ipv6_subnet(self):
        rc, v, msg = cd.check_subnet('fe80::/64')
        assert rc == 0 and v[0] == 6
    
    def test_subnet_mask_missing(self):
        rc, v, msg = cd.check_subnet('192.168.1.58')
        assert rc == 1 and msg
    
    def test_subnet_mask_junk(self):
        rc, v, msg = cd.check_subnet('wah')
        assert rc == 1 and msg


class TestMaintenance:
    systemd_target = "ceph.00000000-0000-0000-0000-000000c0ffee.target"

    def test_systemd_target_OK(self, tmp_path):
        base = tmp_path 
        wants = base / "ceph.target.wants"
        wants.mkdir()
        target = wants / TestMaintenance.systemd_target
        target.touch()
        cd.UNIT_DIR = str(base)

        assert cd.systemd_target_state(target.name)

    def test_systemd_target_NOTOK(self, tmp_path):
        base = tmp_path 
        cd.UNIT_DIR = str(base)
        assert not cd.systemd_target_state(TestMaintenance.systemd_target)

    def test_parser_OK(self):
        args = cd._parse_args(['host-maintenance', 'enter'])
        assert args.maintenance_action == 'enter'

    def test_parser_BAD(self):
        with pytest.raises(SystemExit):
            cd._parse_args(['host-maintenance', 'wah'])


class TestMonitoring(object):
    @mock.patch('cephadm.call')
    def test_get_version_alertmanager(self, _call):
        ctx = mock.Mock()
        daemon_type = 'alertmanager'

        # binary `prometheus`
        _call.return_value = '', '{}, version 0.16.1'.format(daemon_type), 0
        version = cd.Monitoring.get_version(ctx, 'container_id', daemon_type)
        assert version == '0.16.1'

        # binary `prometheus-alertmanager`
        _call.side_effect = (
            ('', '', 1),
            ('', '{}, version 0.16.1'.format(daemon_type), 0),
        )
        version = cd.Monitoring.get_version(ctx, 'container_id', daemon_type)
        assert version == '0.16.1'

    @mock.patch('cephadm.call')
    def test_get_version_prometheus(self, _call):
        ctx = mock.Mock()
        daemon_type = 'prometheus'
        _call.return_value = '', '{}, version 0.16.1'.format(daemon_type), 0
        version = cd.Monitoring.get_version(ctx, 'container_id', daemon_type)
        assert version == '0.16.1'

    @mock.patch('cephadm.call')
    def test_get_version_node_exporter(self, _call):
        ctx = mock.Mock()
        daemon_type = 'node-exporter'
        _call.return_value = '', '{}, version 0.16.1'.format(daemon_type.replace('-', '_')), 0
        version = cd.Monitoring.get_version(ctx, 'container_id', daemon_type)
        assert version == '0.16.1'
