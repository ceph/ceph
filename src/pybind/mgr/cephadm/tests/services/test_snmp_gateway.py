
import json
from unittest.mock import patch

from cephadm.module import CephadmOrchestrator
from ceph.deployment.service_spec import SNMPGatewaySpec
from cephadm.tests.fixtures import with_host, with_service, async_side_effect


class TestSNMPGateway:

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_snmp_v2c_deployment(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        spec = SNMPGatewaySpec(
            snmp_version='V2c',
            snmp_destination='192.168.1.1:162',
            credentials={
                'snmp_community': 'public'
            })

        config = {
            "destination": spec.snmp_destination,
            "snmp_version": spec.snmp_version,
            "snmp_community": spec.credentials.get('snmp_community')
        }

        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, spec):
                _run_cephadm.assert_called_with(
                    'test',
                    "snmp-gateway.test",
                    ['_orch', 'deploy'],
                    [],
                    stdin=json.dumps({
                        "fsid": "fsid",
                        "name": 'snmp-gateway.test',
                        "image": '',
                        "deploy_arguments": [],
                        "params": {
                            'tcp_ports': [9464],
                        },
                        "meta": {
                            'service_name': 'snmp-gateway',
                            'ports': [9464],
                            'ip': None,
                            'deployed_by': [],
                            'rank': None,
                            'rank_generation': None,
                            'extra_container_args': None,
                            'extra_entrypoint_args': None,
                        },
                        "config_blobs": config,
                    }),
                    error_ok=True,
                    use_current_daemon_image=False,
                )

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_snmp_v2c_with_port(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        spec = SNMPGatewaySpec(
            snmp_version='V2c',
            snmp_destination='192.168.1.1:162',
            credentials={
                'snmp_community': 'public'
            },
            port=9465)

        config = {
            "destination": spec.snmp_destination,
            "snmp_version": spec.snmp_version,
            "snmp_community": spec.credentials.get('snmp_community')
        }

        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, spec):
                _run_cephadm.assert_called_with(
                    'test',
                    "snmp-gateway.test",
                    ['_orch', 'deploy'],
                    [],
                    stdin=json.dumps({
                        "fsid": "fsid",
                        "name": 'snmp-gateway.test',
                        "image": '',
                        "deploy_arguments": [],
                        "params": {
                            'tcp_ports': [9465],
                        },
                        "meta": {
                            'service_name': 'snmp-gateway',
                            'ports': [9465],
                            'ip': None,
                            'deployed_by': [],
                            'rank': None,
                            'rank_generation': None,
                            'extra_container_args': None,
                            'extra_entrypoint_args': None,
                        },
                        "config_blobs": config,
                    }),
                    error_ok=True,
                    use_current_daemon_image=False,
                )

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_snmp_v3nopriv_deployment(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        spec = SNMPGatewaySpec(
            snmp_version='V3',
            snmp_destination='192.168.1.1:162',
            engine_id='8000C53F00000000',
            credentials={
                'snmp_v3_auth_username': 'myuser',
                'snmp_v3_auth_password': 'mypassword'
            })

        config = {
            'destination': spec.snmp_destination,
            'snmp_version': spec.snmp_version,
            'snmp_v3_auth_protocol': 'SHA',
            'snmp_v3_auth_username': 'myuser',
            'snmp_v3_auth_password': 'mypassword',
            'snmp_v3_engine_id': '8000C53F00000000'
        }

        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, spec):
                _run_cephadm.assert_called_with(
                    'test',
                    "snmp-gateway.test",
                    ['_orch', 'deploy'],
                    [],
                    stdin=json.dumps({
                        "fsid": "fsid",
                        "name": 'snmp-gateway.test',
                        "image": '',
                        "deploy_arguments": [],
                        "params": {
                            'tcp_ports': [9464],
                        },
                        "meta": {
                            'service_name': 'snmp-gateway',
                            'ports': [9464],
                            'ip': None,
                            'deployed_by': [],
                            'rank': None,
                            'rank_generation': None,
                            'extra_container_args': None,
                            'extra_entrypoint_args': None,
                        },
                        "config_blobs": config,
                    }),
                    error_ok=True,
                    use_current_daemon_image=False,
                )

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_snmp_v3priv_deployment(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        spec = SNMPGatewaySpec(
            snmp_version='V3',
            snmp_destination='192.168.1.1:162',
            engine_id='8000C53F00000000',
            auth_protocol='MD5',
            privacy_protocol='AES',
            credentials={
                'snmp_v3_auth_username': 'myuser',
                'snmp_v3_auth_password': 'mypassword',
                'snmp_v3_priv_password': 'mysecret',
            })

        config = {
            'destination': spec.snmp_destination,
            'snmp_version': spec.snmp_version,
            'snmp_v3_auth_protocol': 'MD5',
            'snmp_v3_auth_username': spec.credentials.get('snmp_v3_auth_username'),
            'snmp_v3_auth_password': spec.credentials.get('snmp_v3_auth_password'),
            'snmp_v3_engine_id': '8000C53F00000000',
            'snmp_v3_priv_protocol': spec.privacy_protocol,
            'snmp_v3_priv_password': spec.credentials.get('snmp_v3_priv_password'),
        }

        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, spec):
                _run_cephadm.assert_called_with(
                    'test',
                    "snmp-gateway.test",
                    ['_orch', 'deploy'],
                    [],
                    stdin=json.dumps({
                        "fsid": "fsid",
                        "name": 'snmp-gateway.test',
                        "image": '',
                        "deploy_arguments": [],
                        "params": {
                            'tcp_ports': [9464],
                        },
                        "meta": {
                            'service_name': 'snmp-gateway',
                            'ports': [9464],
                            'ip': None,
                            'deployed_by': [],
                            'rank': None,
                            'rank_generation': None,
                            'extra_container_args': None,
                            'extra_entrypoint_args': None,
                        },
                        "config_blobs": config,
                    }),
                    error_ok=True,
                    use_current_daemon_image=False,
                )
