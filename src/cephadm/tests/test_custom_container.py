import unittest
from unittest import mock

from .fixtures import (
    cephadm_fs,
    import_cephadm,
    mock_podman,
    with_cephadm_ctx,
)


_cephadm = import_cephadm()


class TestCustomContainer(unittest.TestCase):
    cc: _cephadm.CustomContainer

    def setUp(self):
        self.cc = _cephadm.CustomContainer(
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
        # TODO: get_container_mounts was made private. test the private func for
        # now. in the future update to test base class func
        # customize_container_mounts
        result = self.cc._get_container_mounts('/xyz')
        self.assertDictEqual(result, {
            '/CONFIG_DIR': '/foo/conf',
            '/xyz/bar/config': '/bar:ro'
        })

    def test_get_container_binds(self):
        # TODO: get_container_binds was made private. test the private func for
        # now. in the future update to test base class fune
        # customize_container_binds
        result = self.cc._get_container_binds('/xyz')
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


def test_deploy_custom_container(cephadm_fs):
    m1 = mock.patch(
        'cephadmlib.container_types.call', return_value=('', '', 0)
    )
    m2 = mock.patch('cephadmlib.container_types.call_throws', return_value=0)
    fsid = 'b01dbeef-701d-9abe-0000-e1e5a47004a7'
    with with_cephadm_ctx([]) as ctx, m1, m2:
        ctx.container_engine = mock_podman()
        ctx.fsid = fsid
        ctx.name = 'container.tdcc'
        ctx.reconfig = False
        ctx.allow_ptrace = False
        ctx.image = 'quay.io/foobar/quux:latest'
        ctx.extra_entrypoint_args = [
            '--label',
            'frobnicationist',
            '--servers',
            '192.168.8.42,192.168.8.43,192.168.12.11',
        ]
        ctx.config_blobs = {
            'envs': ['FOO=1', 'BAR=77'],
        }

        _cephadm._common_deploy(ctx)

        with open(f'/var/lib/ceph/{fsid}/container.tdcc/unit.run') as f:
            runfile_lines = f.read().splitlines()
        assert 'set -e' in runfile_lines
        assert len(runfile_lines) > 2
        assert runfile_lines[-1] == (
            '/usr/bin/podman run'
            ' --rm --ipc=host --stop-signal=SIGTERM --init'
            ' --name ceph-b01dbeef-701d-9abe-0000-e1e5a47004a7-container-tdcc'
            ' -d --log-driver journald'
            ' --conmon-pidfile /run/ceph-b01dbeef-701d-9abe-0000-e1e5a47004a7@container.tdcc.service-pid'
            ' --cidfile /run/ceph-b01dbeef-701d-9abe-0000-e1e5a47004a7@container.tdcc.service-cid'
            ' --cgroups=split --no-hosts'
            ' -e CONTAINER_IMAGE=quay.io/foobar/quux:latest'
            ' -e NODE_NAME=host1'
            ' -e FOO=1'
            ' -e BAR=77'
            ' quay.io/foobar/quux:latest'
            ' --label frobnicationist --servers 192.168.8.42,192.168.8.43,192.168.12.11'
        )
        assert all([
            l.startswith('! /usr/bin/podman rm')
            for l in runfile_lines
            if not l.startswith(('#', 'set', '/usr/bin/podman run'))
        ]), 'remaining commands should be "rms"'


def test_deploy_custom_container_and_inits(cephadm_fs):
    m1 = mock.patch(
        'cephadmlib.container_types.call', return_value=('', '', 0)
    )
    m2 = mock.patch('cephadmlib.container_types.call_throws', return_value=0)
    fsid = 'b01dbeef-701d-9abe-0000-e1e5a47004a7'
    with with_cephadm_ctx([]) as ctx, m1, m2:
        ctx.container_engine = mock_podman()
        ctx.fsid = fsid
        ctx.name = 'container.tdccai'
        ctx.reconfig = False
        ctx.allow_ptrace = False
        ctx.image = 'quay.io/foobar/quux:latest'
        ctx.extra_entrypoint_args = [
            '--label',
            'treepollenparty',
            '--servers',
            '192.168.8.42,192.168.8.43,192.168.12.11',
        ]
        ctx.init_containers = [
            {
                'entrypoint': '/usr/local/bin/prepare.sh',
                'volume_mounts': {
                    'data1': '/var/lib/myapp',
                },
            },
            {
                'entrypoint': '/usr/local/bin/populate.sh',
                'entrypoint_args': [
                    '--source=https://my.cool.example.com/samples/geo.1.txt',
                ],
            },
        ]
        ctx.config_blobs = {
            'dirs': ['data1', 'data2'],
            'volume_mounts': {
                'data1': '/var/lib/myapp',
                'data2': '/srv',
            },
        }

        _cephadm._common_deploy(ctx)

        with open(f'/var/lib/ceph/{fsid}/container.tdccai/unit.run') as f:
            runfile_lines = f.read().splitlines()
        assert 'set -e' in runfile_lines
        assert len(runfile_lines) > 2
        assert runfile_lines[-1] == (
            '/usr/bin/podman run'
            ' --rm --ipc=host --stop-signal=SIGTERM --init'
            ' --name ceph-b01dbeef-701d-9abe-0000-e1e5a47004a7-container-tdccai'
            ' -d --log-driver journald'
            ' --conmon-pidfile /run/ceph-b01dbeef-701d-9abe-0000-e1e5a47004a7@container.tdccai.service-pid'
            ' --cidfile /run/ceph-b01dbeef-701d-9abe-0000-e1e5a47004a7@container.tdccai.service-cid'
            ' --cgroups=split --no-hosts'
            ' -e CONTAINER_IMAGE=quay.io/foobar/quux:latest'
            ' -e NODE_NAME=host1'
            ' -v /var/lib/ceph/b01dbeef-701d-9abe-0000-e1e5a47004a7/container.tdccai/data1:/var/lib/myapp'
            ' -v /var/lib/ceph/b01dbeef-701d-9abe-0000-e1e5a47004a7/container.tdccai/data2:/srv'
            ' quay.io/foobar/quux:latest'
            ' --label treepollenparty --servers 192.168.8.42,192.168.8.43,192.168.12.11'
        )
        assert all([
            l.startswith('! /usr/bin/podman rm')
            for l in runfile_lines
            if not l.startswith(('#', 'set', '/usr/bin/podman run'))
        ]), 'remaining commands should be "rms"'

        with open(f'/var/lib/ceph/{fsid}/container.tdccai/init_containers.run') as f:
            icfile_lines = f.read().splitlines()

        idx = icfile_lines.index('# init container cleanup')
        assert idx >= 0
        assert any(
            l.strip().startswith('! /usr/bin/podman rm')
            for l in icfile_lines
        )

        slines = [l.strip() for l in icfile_lines]
        idx = slines.index('# run init container 0: ceph-b01dbeef-701d-9abe-0000-e1e5a47004a7-container-tdccai-init')
        assert idx > 0
        assert slines[idx + 1] == (
            '/usr/bin/podman run'
            ' --stop-signal=SIGTERM'
            ' --entrypoint /usr/local/bin/prepare.sh'
            ' --name ceph-b01dbeef-701d-9abe-0000-e1e5a47004a7-container-tdccai-init'
            ' -e CONTAINER_IMAGE=quay.io/foobar/quux:latest'
            ' -v /var/lib/ceph/b01dbeef-701d-9abe-0000-e1e5a47004a7/container.tdccai/data1:/var/lib/myapp'
            ' quay.io/foobar/quux:latest'
        )
        assert slines[idx + 3].startswith('! /usr/bin/podman rm')
        assert slines[idx + 4].startswith('! /usr/bin/podman rm')

        idx = slines.index('# run init container 1: ceph-b01dbeef-701d-9abe-0000-e1e5a47004a7-container-tdccai-init')
        assert idx > 0
        assert slines[idx + 1] == (
            '/usr/bin/podman run'
            ' --stop-signal=SIGTERM'
            ' --entrypoint /usr/local/bin/populate.sh'
            ' --name ceph-b01dbeef-701d-9abe-0000-e1e5a47004a7-container-tdccai-init'
            ' -e CONTAINER_IMAGE=quay.io/foobar/quux:latest'
            ' -v /var/lib/ceph/b01dbeef-701d-9abe-0000-e1e5a47004a7/container.tdccai/data1:/var/lib/myapp'
            ' -v /var/lib/ceph/b01dbeef-701d-9abe-0000-e1e5a47004a7/container.tdccai/data2:/srv'
            ' quay.io/foobar/quux:latest'
            ' --source=https://my.cool.example.com/samples/geo.1.txt'
        )
        assert slines[idx + 3].startswith('! /usr/bin/podman rm')
        assert slines[idx + 4].startswith('! /usr/bin/podman rm')
