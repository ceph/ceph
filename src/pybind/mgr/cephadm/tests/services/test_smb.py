import json
from unittest.mock import patch

from cephadm.services.smb import SMBSpec, SMBExternalCephCluster
from cephadm.module import CephadmOrchestrator
from cephadm.tests.fixtures import with_host, with_service, async_side_effect


_SAMBA_METRICS_IMAGE = 'quay.io/samba.org/samba-metrics:devbuilds-centos-amd64'


class TestSMB:
    @patch("cephadm.module.CephadmOrchestrator.get_unique_name")
    @patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_deploy_smb(
        self, _run_cephadm, _get_uname, cephadm_module: CephadmOrchestrator
    ):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        _get_uname.return_value = 'tango.briskly'

        spec = SMBSpec(
            cluster_id='foxtrot',
            config_uri='rados://.smb/foxtrot/config.json',
        )

        expected = {
            'fsid': 'fsid',
            'name': 'smb.tango.briskly',
            'image': '',
            'deploy_arguments': [],
            'params': {
                "tcp_ports": [445, 9922]
            },
            'meta': {
                'service_name': 'smb',
                'ports': [445, 9922],
                'ip': None,
                'deployed_by': [],
                'rank': None,
                'rank_generation': None,
                'extra_container_args': None,
                'extra_entrypoint_args': None,
            },
            'config_blobs': {
                'cluster_id': 'foxtrot',
                'features': [],
                'config_uri': 'rados://.smb/foxtrot/config.json',
                'config': '',
                'keyring': '[client.smb.config.tango.briskly]\nkey = None\n',
                'config_auth_entity': 'client.smb.config.tango.briskly',
                'metrics_image': _SAMBA_METRICS_IMAGE,
                'service_ports': {
                    'smb': 445,
                    'smbmetrics': 9922,
                    'ctdb': 4379,
                    'remote-control': 54445,
                },
            },
        }
        with with_host(cephadm_module, 'hostx'):
            with with_service(cephadm_module, spec):
                _run_cephadm.assert_called_with(
                    'hostx',
                    'smb.tango.briskly',
                    ['_orch', 'deploy'],
                    [],
                    stdin=json.dumps(expected),
                    error_ok=True,
                    use_current_daemon_image=False,
                )

    @patch("cephadm.module.CephadmOrchestrator.get_unique_name")
    @patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_deploy_smb_join_dns(
        self, _run_cephadm, _get_uname, cephadm_module: CephadmOrchestrator
    ):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        _get_uname.return_value = 'tango.briskly'

        spec = SMBSpec(
            cluster_id='foxtrot',
            features=['domain'],
            config_uri='rados://.smb/foxtrot/config2.json',
            join_sources=[
                'rados://.smb/foxtrot/join1.json',
                'rados:mon-config-key:smb/config/foxtrot/join2.json',
            ],
            custom_dns=['10.8.88.103'],
            include_ceph_users=[
                'client.smb.fs.cephfs.share1',
                'client.smb.fs.cephfs.share2',
                'client.smb.fs.fs2.share3',
            ],
        )

        expected = {
            'fsid': 'fsid',
            'name': 'smb.tango.briskly',
            'image': '',
            'deploy_arguments': [],
            'params': {
                'tcp_ports': [445, 9922]
            },
            'meta': {
                'service_name': 'smb',
                'ports': [445, 9922],
                'ip': None,
                'deployed_by': [],
                'rank': None,
                'rank_generation': None,
                'extra_container_args': None,
                'extra_entrypoint_args': None,
            },
            'config_blobs': {
                'cluster_id': 'foxtrot',
                'features': ['domain'],
                'config_uri': 'rados://.smb/foxtrot/config2.json',
                'join_sources': [
                    'rados://.smb/foxtrot/join1.json',
                    'rados:mon-config-key:smb/config/foxtrot/join2.json',
                ],
                'custom_dns': ['10.8.88.103'],
                'config': '',
                'keyring': (
                    '[client.smb.config.tango.briskly]\nkey = None\n\n'
                    '[client.smb.fs.cephfs.share1]\nkey = None\n\n'
                    '[client.smb.fs.cephfs.share2]\nkey = None\n\n'
                    '[client.smb.fs.fs2.share3]\nkey = None\n'
                ),
                'config_auth_entity': 'client.smb.config.tango.briskly',
                'metrics_image': _SAMBA_METRICS_IMAGE,
                'service_ports': {
                    'smb': 445,
                    'smbmetrics': 9922,
                    'ctdb': 4379,
                    'remote-control': 54445,
                },
            },
        }
        with with_host(cephadm_module, 'hostx'):
            with with_service(cephadm_module, spec):
                _run_cephadm.assert_called_with(
                    'hostx',
                    'smb.tango.briskly',
                    ['_orch', 'deploy'],
                    [],
                    stdin=json.dumps(expected),
                    error_ok=True,
                    use_current_daemon_image=False,
                )


def test_smb_get_dependencies(cephadm_module):
    from cephadm.services.smb import SMBService

    spec = SMBSpec(
        cluster_id='foxtrot',
        features=['domain'],
        config_uri='rados://.smb/foxtrot/config2.json',
        join_sources=[
            'rados:mon-config-key:smb/config/foxtrot/join2.json',
        ],
        include_ceph_users=[
            'client.smb.fs.cephfs.share1',
            'client.smb.fs.cephfs.share2',
            'client.smb.fs.fs2.share3',
        ],
        ceph_cluster_configs=[
            SMBExternalCephCluster(
                alias="exo",
                fsid='cf05db31-3d4e-4ffd-85ad-753434d5add1',
                mon_host='[v2:192.168.76.200:3300/0,v1:192.168.76.200:6789/0]',
                user='client.smb.remote1',
                key='AQBAAK9ptYayIRAAQ1Bcpti9yMvX2Gl0KMmc4Q=='
            )
        ]
    )

    deps = SMBService.get_dependencies(cephadm_module, spec, spec.service_type)
    assert deps == [
        'smb+meta:ceph_cluster_config.exo=sha256:859b001f76df4d184b858b9c3e323ca8ff85a311414d0405f4484d17aa481ef3'
    ]
