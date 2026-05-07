import contextlib
import json
from unittest.mock import patch, ANY

from cephadm.module import CephadmOrchestrator
from ceph.deployment.service_spec import TracingSpec
from cephadm.tests.fixtures import with_host, with_service, async_side_effect
from mgr_util import build_url


class TestJaeger:
    @patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_jaeger_query(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        spec = TracingSpec(es_nodes="192.168.0.1:9200", service_type="jaeger-query")
        config = {"elasticsearch_nodes": "http://192.168.0.1:9200"}

        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, spec):
                _run_cephadm.assert_called_with(
                    'test',
                    "jaeger-query.test",
                    ['_orch', 'deploy'],
                    [],
                    stdin=json.dumps({
                        "fsid": "fsid",
                        "name": 'jaeger-query.test',
                        "image": '',
                        "deploy_arguments": [],
                        "params": {
                            'tcp_ports': [16686],
                        },
                        "meta": {
                            'service_name': 'jaeger-query',
                            'ports': [16686],
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
    def test_jaeger_collector_es_deploy(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        collector_spec = TracingSpec(service_type="jaeger-collector")
        es_spec = TracingSpec(service_type="elasticsearch")
        es_config = {}

        with with_host(cephadm_module, 'test'):
            collector_config = {
                "elasticsearch_nodes": f'http://{build_url(host=cephadm_module.inventory.get_addr("test"), port=9200).lstrip("/")}'}
            with with_service(cephadm_module, es_spec):
                _run_cephadm.assert_called_with(
                    "test",
                    "elasticsearch.test",
                    ['_orch', 'deploy'],
                    [],
                    stdin=json.dumps({
                        "fsid": "fsid",
                        "name": 'elasticsearch.test',
                        "image": '',
                        "deploy_arguments": [],
                        "params": {
                            'tcp_ports': [9200],
                        },
                        "meta": {
                            'service_name': 'elasticsearch',
                            'ports': [9200],
                            'ip': None,
                            'deployed_by': [],
                            'rank': None,
                            'rank_generation': None,
                            'extra_container_args': None,
                            'extra_entrypoint_args': None,
                        },
                        "config_blobs": es_config,
                    }),
                    error_ok=True,
                    use_current_daemon_image=False,
                )
                with with_service(cephadm_module, collector_spec):
                    _run_cephadm.assert_called_with(
                        "test",
                        "jaeger-collector.test",
                        ['_orch', 'deploy'],
                        [],
                        stdin=json.dumps({
                            "fsid": "fsid",
                            "name": 'jaeger-collector.test',
                            "image": '',
                            "deploy_arguments": [],
                            "params": {
                                'tcp_ports': [14250],
                            },
                            "meta": {
                                'service_name': 'jaeger-collector',
                                'ports': [14250],
                                'ip': None,
                                'deployed_by': [],
                                'rank': None,
                                'rank_generation': None,
                                'extra_container_args': None,
                                'extra_entrypoint_args': None,
                            },
                            "config_blobs": collector_config,
                        }),
                        error_ok=True,
                        use_current_daemon_image=False,
                    )

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_jaeger_agent(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        collector_spec = TracingSpec(service_type="jaeger-collector", es_nodes="192.168.0.1:9200")
        collector_config = {"elasticsearch_nodes": "http://192.168.0.1:9200"}

        agent_spec = TracingSpec(service_type="jaeger-agent")
        agent_config = {"collector_nodes": "test:14250"}

        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, collector_spec):
                _run_cephadm.assert_called_with(
                    "test",
                    "jaeger-collector.test",
                    ['_orch', 'deploy'],
                    [],
                    stdin=json.dumps({
                        "fsid": "fsid",
                        "name": 'jaeger-collector.test',
                        "image": '',
                        "deploy_arguments": [],
                        "params": {
                            'tcp_ports': [14250],
                        },
                        "meta": {
                            'service_name': 'jaeger-collector',
                            'ports': [14250],
                            'ip': None,
                            'deployed_by': [],
                            'rank': None,
                            'rank_generation': None,
                            'extra_container_args': None,
                            'extra_entrypoint_args': None,
                        },
                        "config_blobs": collector_config,
                    }),
                    error_ok=True,
                    use_current_daemon_image=False,
                )
                with with_service(cephadm_module, agent_spec):
                    _run_cephadm.assert_called_with(
                        "test",
                        "jaeger-agent.test",
                        ['_orch', 'deploy'],
                        [],
                        stdin=json.dumps({
                            "fsid": "fsid",
                            "name": 'jaeger-agent.test',
                            "image": '',
                            "deploy_arguments": [],
                            "params": {
                                'tcp_ports': [6799],
                            },
                            "meta": {
                                'service_name': 'jaeger-agent',
                                'ports': [6799],
                                'ip': None,
                                'deployed_by': [],
                                'rank': None,
                                'rank_generation': None,
                                'extra_container_args': None,
                                'extra_entrypoint_args': None,
                            },
                            "config_blobs": agent_config,
                        }),
                        error_ok=True,
                        use_current_daemon_image=False,
                    )


def test_jaeger_agent_choose_next_action(cephadm_module, mock_cephadm):
    jaeger_spec = TracingSpec(service_type="jaeger-agent")
    coll_spec = TracingSpec(service_type="jaeger-collector")
    es_spec = TracingSpec(service_type="elasticsearch")
    with contextlib.ExitStack() as stack:
        stack.enter_context(with_host(cephadm_module, "test"))
        stack.enter_context(with_service(cephadm_module, jaeger_spec))
        stack.enter_context(with_service(cephadm_module, es_spec))
        stack.enter_context(with_service(cephadm_module, coll_spec))
        # manually invoke _check_daemons to trigger a call to
        # _daemon_action so we can check what action was chosen
        mock_cephadm.serve(cephadm_module)._check_daemons()
        mock_cephadm._daemon_action.assert_called_with(ANY, action="redeploy")
