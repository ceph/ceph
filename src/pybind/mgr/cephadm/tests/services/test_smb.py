import json
from unittest.mock import patch

from ceph.smb.constants import REMOTE_CONTROL
from cephadm.services.smb import SMBSpec, SMBExternalCephCluster
from cephadm.module import CephadmOrchestrator
from cephadm.tests.fixtures import with_host, with_service, async_side_effect

from cephadm.services.service_registry import service_registry
from cephadm.services.cephadmservice import CephadmDaemonDeploySpec
from ceph.deployment.service_spec import PlacementSpec

_SAMBA_METRICS_IMAGE = 'quay.io/samba.org/samba-metrics:devbuilds-centos-any'

cephadm_root_ca = """-----BEGIN CERTIFICATE-----\nMIIE7DCCAtSgAwIBAgIUE8b2zZ64geu2ns3Zfn3/4L+Cf6MwDQYJKoZIhvcNAQEL\nBQAwFzEVMBMGA1UEAwwMY2VwaGFkbS1yb290MB4XDTI0MDYyNjE0NDA1M1oXDTM0\nMDYyNzE0NDA1M1owFzEVMBMGA1UEAwwMY2VwaGFkbS1yb290MIICIjANBgkqhkiG\n9w0BAQEFAAOCAg8AMIICCgKCAgEAsZRJsdtTr9GLG1lWFql5SGc46ldFanNJd1Gl\nqXq5vgZVKRDTmNgAb/XFuNEEmbDAXYIRZolZeYKMHfn0pouPRSel0OsC6/02ZUOW\nIuN89Wgo3IYleCFpkVIumD8URP3hwdu85plRxYZTtlruBaTRH38lssyCqxaOdEt7\nAUhvYhcMPJThB17eOSQ73mb8JEC83vB47fosI7IhZuvXvRSuZwUW30rJanWNhyZq\neS2B8qw2RSO0+77H6gA4ftBnitfsE1Y8/F9Z/f92JOZuSMQXUB07msznPbRJia3f\nueO8gOc32vxd1A1/Qzp14uX34yEGY9ko2lW226cZO29IVUtXOX+LueQttwtdlpz8\ne6Npm09pXhXAHxV/OW3M28MdXmobIqT/m9MfkeAErt5guUeC5y8doz6/3VQRjFEn\nRpN0WkblgnNAQ3DONPc+Qd9Fi/wZV2X7bXoYpNdoWDsEOiE/eLmhG1A2GqU/mneP\nzQ6u79nbdwTYpwqHpa+PvusXeLfKauzI8lLUJotdXy9EK8iHUofibB61OljYye6B\nG3b8C4QfGsw8cDb4APZd/6AZYyMx/V3cGZ+GcOV7WvsC8k7yx5Uqasm/kiGQ3EZo\nuNenNEYoGYrjb8D/8QzqNUTwlEh27/ps80tO7l2GGTvWVZL0PRZbmLDvO77amtOf\nOiRXMoUCAwEAAaMwMC4wGwYDVR0RBBQwEocQAAAAAAAAAAAAAAAAAAAAATAPBgNV\nHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4ICAQAxwzX5AhYEWhTV4VUwUj5+\nqPdl4Q2tIxRokqyE+cDxoSd+6JfGUefUbNyBxDt0HaBq8obDqqrbcytxnn7mpnDu\nhtiauY+I4Amt7hqFOiFA4cCLi2mfok6g2vL53tvhd9IrsfflAU2wy7hL76Ejm5El\nA+nXlkJwps01Whl9pBkUvIbOn3pXX50LT4hb5zN0PSu957rjd2xb4HdfuySm6nW4\n4GxtVWfmGA6zbC4XMEwvkuhZ7kD2qjkAguGDF01uMglkrkCJT3OROlNBuSTSBGqt\ntntp5VytHvb7KTF7GttM3ha8/EU2KYaHM6WImQQTrOfiImAktOk4B3lzUZX3HYIx\n+sByO4P4dCvAoGz1nlWYB2AvCOGbKf0Tgrh4t4jkiF8FHTXGdfvWmjgi1pddCNAy\nn65WOCmVmLZPERAHOk1oBwqyReSvgoCFo8FxbZcNxJdlhM0Z6hzKggm3O3Dl88Xl\n5euqJjh2STkBW8Xuowkg1TOs5XyWvKoDFAUzyzeLOL8YSG+gXV22gPTUaPSVAqdb\nwd0Fx2kjConuC5bgTzQHs8XWA930U3XWZraj21Vaa8UxlBLH4fUro8H5lMSYlZNE\nJHRNW8BkznAClaFSDG3dybLsrzrBFAu/Qb5zVkT1xyq0YkepGB7leXwq6vjWA5Pw\nmZbKSphWfh0qipoqxqhfkw==\n-----END CERTIFICATE-----\n"""

ceph_generated_cert = """-----BEGIN CERTIFICATE-----\nMIICxjCCAa4CEQDIZSujNBlKaLJzmvntjukjMA0GCSqGSIb3DQEBDQUAMCExDTAL\nBgNVBAoMBENlcGgxEDAOBgNVBAMMB2NlcGhhZG0wHhcNMjIwNzEzMTE0NzA3WhcN\nMzIwNzEwMTE0NzA3WjAhMQ0wCwYDVQQKDARDZXBoMRAwDgYDVQQDDAdjZXBoYWRt\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAyyMe4DMA+MeYK7BHZMHB\nq7zjliEOcNgxomjU8qbf5USF7Mqrf6+/87XWqj4pCyAW8x0WXEr6A56a+cmBVmt+\nqtWDzl020aoId6lL5EgLLn6/kMDCCJLq++Lg9cEofMSvcZh+lY2f+1p+C+00xent\nrLXvXGOilAZWaQfojT2BpRnNWWIFbpFwlcKrlg2G0cFjV5c1m6a0wpsQ9JHOieq0\nSvwCixajwq3CwAYuuiU1wjI4oJO4Io1+g8yB3nH2Mo/25SApCxMXuXh4kHLQr/T4\n4hqisvG4uJYgKMcSIrWj5o25mclByGi1UI/kZkCUES94i7Z/3ihx4Bad0AMs/9tw\nFwIDAQABMA0GCSqGSIb3DQEBDQUAA4IBAQAf+pwz7Gd7mDwU2LY0TQXsK6/8KGzh\nHuX+ErOb8h5cOAbvCnHjyJFWf6gCITG98k9nxU9NToG0WYuNm/max1y/54f0dtxZ\npUo6KSNl3w6iYCfGOeUIj8isi06xMmeTgMNzv8DYhDt+P2igN6LenqWTVztogkiV\nxQ5ZJFFLEw4sN0CXnrZX3t5ruakxLXLTLKeE0I91YJvjClSBGkVJq26wOKQNHMhx\npWxeydQ5EgPZY+Aviz5Dnxe8aB7oSSovpXByzxURSabOuCK21awW5WJCGNpmqhWK\nZzACBDEstccj57c4OGV0eayHJRsluVr2e9NHRINZA3qdB37e6gsI1xHo\n-----END CERTIFICATE-----\n"""

ceph_generated_key = """-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDLIx7gMwD4x5gr\nsEdkwcGrvOOWIQ5w2DGiaNTypt/lRIXsyqt/r7/ztdaqPikLIBbzHRZcSvoDnpr5\nyYFWa36q1YPOXTbRqgh3qUvkSAsufr+QwMIIkur74uD1wSh8xK9xmH6VjZ/7Wn4L\n7TTF6e2ste9cY6KUBlZpB+iNPYGlGc1ZYgVukXCVwquWDYbRwWNXlzWbprTCmxD0\nkc6J6rRK/AKLFqPCrcLABi66JTXCMjigk7gijX6DzIHecfYyj/blICkLExe5eHiQ\nctCv9PjiGqKy8bi4liAoxxIitaPmjbmZyUHIaLVQj+RmQJQRL3iLtn/eKHHgFp3Q\nAyz/23AXAgMBAAECggEAVoTB3Mm8azlPlaQB9GcV3tiXslSn+uYJ1duCf0sV52dV\nBzKW8s5fGiTjpiTNhGCJhchowqxoaew+o47wmGc2TvqbpeRLuecKrjScD0GkCYyQ\neM2wlshEbz4FhIZdgS6gbuh9WaM1dW/oaZoBNR5aTYo7xYTmNNeyLA/jO2zr7+4W\n5yES1lMSBXpKk7bDGKYY4bsX2b5RLr2Grh2u2bp7hoLABCEvuu8tSQdWXLEXWpXo\njwmV3hc6tabypIa0mj2Dmn2Dmt1ppSO0AZWG/WAizN3f4Z0r/u9HnbVrVmh0IEDw\n3uf2LP5o3msG9qKCbzv3lMgt9mMr70HOKnJ8ohMSKQKBgQDLkNb+0nr152HU9AeJ\nvdz8BeMxcwxCG77iwZphZ1HprmYKvvXgedqWtS6FRU+nV6UuQoPUbQxJBQzrN1Qv\nwKSlOAPCrTJgNgF/RbfxZTrIgCPuK2KM8I89VZv92TSGi362oQA4MazXC8RAWjoJ\nSu1/PHzK3aXOfVNSLrOWvIYeZQKBgQD/dgT6RUXKg0UhmXj7ExevV+c7oOJTDlMl\nvLngrmbjRgPO9VxLnZQGdyaBJeRngU/UXfNgajT/MU8B5fSKInnTMawv/tW7634B\nw3v6n5kNIMIjJmENRsXBVMllDTkT9S7ApV+VoGnXRccbTiDapBThSGd0wri/CuwK\nNWK1YFOeywKBgEDyI/XG114PBUJ43NLQVWm+wx5qszWAPqV/2S5MVXD1qC6zgCSv\nG9NLWN1CIMimCNg6dm7Wn73IM7fzvhNCJgVkWqbItTLG6DFf3/DPODLx1wTMqLOI\nqFqMLqmNm9l1Nec0dKp5BsjRQzq4zp1aX21hsfrTPmwjxeqJZdioqy2VAoGAXR5X\nCCdSHlSlUW8RE2xNOOQw7KJjfWT+WAYoN0c7R+MQplL31rRU7dpm1bLLRBN11vJ8\nMYvlT5RYuVdqQSP6BkrX+hLJNBvOLbRlL+EXOBrVyVxHCkDe+u7+DnC4epbn+N8P\nLYpwqkDMKB7diPVAizIKTBxinXjMu5fkKDs5n+sCgYBbZheYKk5M0sIxiDfZuXGB\nkf4mJdEkTI1KUGRdCwO/O7hXbroGoUVJTwqBLi1tKqLLarwCITje2T200BYOzj82\nqwRkCXGtXPKnxYEEUOiFx9OeDrzsZV00cxsEnX0Zdj+PucQ/J3Cvd0dWUspJfLHJ\n39gnaegswnz9KMQAvzKFdg==\n-----END PRIVATE KEY-----\n"""


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
                'extra_config_uris': ['rados:mon-config-key:smb/config/foxtrot/config.smb.rgw'],
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
                'extra_config_uris': ['rados:mon-config-key:smb/config/foxtrot/config.smb.rgw'],
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

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_smb_tls_feature_cert_in_config_blobs(
        self, _run_cephadm, cephadm_module: CephadmOrchestrator
    ):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        with with_host(cephadm_module, 'test', addr='1.2.3.7'):
            cephadm_module.cache.update_host_networks(
                'test',
                {'1.2.3.0/24': {'if0': ['1.2.3.7']}}
            )

            smb_spec = SMBSpec(
                cluster_id='foxtrot',
                service_id='foo',
                config_uri='rados://.smb/foxtrot/config2.json',
                placement=PlacementSpec(hosts=['test']),
                features=[REMOTE_CONTROL],
                ssl_certificates={
                    'remote_control': {
                        'enabled': True,
                        'ssl_cert': ceph_generated_cert,
                        'ssl_key': ceph_generated_key,
                        'ssl_ca_cert': cephadm_root_ca,
                        'certificate_source': 'inline',
                    },
                },
            )
            service_name = smb_spec.service_name()

            with with_service(cephadm_module, smb_spec):
                smb_conf, _ = service_registry.get_service('smb').generate_config(
                    CephadmDaemonDeploySpec(
                        host='test',
                        daemon_id='foo.test.0',
                        service_name=service_name,
                    )
                )
                files = smb_conf.get('files', {})
                assert files.get('remote_control.ssl.crt') == ceph_generated_cert
                assert files.get('remote_control.ssl.key') == ceph_generated_key
                assert files.get('remote_control.ca.crt') == cephadm_root_ca


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
        'smb+meta:ceph_cluster_config.exo=sha256:859b001f76df4d184b858b9c3e323ca8ff85a311414d0405f4484d17aa481ef3',
        'smb+field:features=domain',
        'smb+field:rgw_creds_uri=rados:mon-config-key:smb/config/foxtrot/config.smb.rgw',
    ]


def test_pool_caps_from_uri(cephadm_module):
    from cephadm.services.smb import SMBService

    smb_service = SMBService(cephadm_module)

    # objects living in the shared .smb pool must be scoped to the
    # cluster's own namespace.
    assert smb_service._pool_caps_from_uri(
        'rados://.smb/foxtrot/config.json'
    ) == [
        'allow r pool=.smb namespace=foxtrot',
        'allow rwx pool=.smb namespace=foxtrot object_prefix cluster.meta.',
    ]

    # the empty-namespace uri shape that RADOSConfigEntry.uri can produce
    # (rados://<pool>//<key>) must still scope the read cap consistently
    # with the existing rwx cap.
    assert smb_service._pool_caps_from_uri(
        'rados://.smb//config.smb'
    ) == [
        'allow r pool=.smb namespace=',
        'allow rwx pool=.smb namespace= object_prefix cluster.meta.',
    ]
