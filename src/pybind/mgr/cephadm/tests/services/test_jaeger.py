import contextlib
import json
from unittest.mock import patch, ANY

from cephadm.module import CephadmOrchestrator
from ceph.deployment.service_spec import TracingSpec
from cephadm.tests.fixtures import with_host, with_service, async_side_effect


class TestJaeger:
    @patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_jaeger(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        spec = TracingSpec(service_type="jaeger", es_nodes="192.168.0.1:9200")
        config_yaml = (
            'service:\n'
            '  extensions: [jaeger_storage, jaeger_query]\n'
            '  pipelines:\n'
            '    traces:\n'
            '      receivers: [otlp]\n'
            '      exporters: [jaeger_storage_exporter]\n'
            'extensions:\n'
            '  jaeger_storage:\n'
            '    backends:\n'
            '      some_storage:\n'
            '        elasticsearch:\n'
            '          server_urls: [http://192.168.0.1:9200]\n'
            '  jaeger_query:\n'
            '    storage:\n'
            '      traces: some_storage\n'
            'receivers:\n'
            '  otlp:\n'
            '    protocols:\n'
            '      grpc:\n'
            '        endpoint: "0.0.0.0:4317"\n'
            'exporters:\n'
            '  jaeger_storage_exporter:\n'
            '    trace_storage: some_storage\n'
        )
        config = {'files': {'config.yaml': config_yaml}}

        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, spec):
                _run_cephadm.assert_called_with(
                    "test",
                    "jaeger.test",
                    ['_orch', 'deploy'],
                    [],
                    stdin=json.dumps(
                        {
                            "fsid": "fsid",
                            "name": 'jaeger.test',
                            "image": '',
                            "deploy_arguments": [],
                            "params": {
                                'tcp_ports': [4317],
                            },
                            "meta": {
                                'service_name': 'jaeger',
                                'ports': [4317],
                                'ip': None,
                                'deployed_by': [],
                                'rank': None,
                                'rank_generation': None,
                                'extra_container_args': None,
                                'extra_entrypoint_args': None,
                            },
                            "config_blobs": config,
                        }
                    ),
                    error_ok=True,
                    use_current_daemon_image=False,
                )


def test_jaeger_choose_next_action(cephadm_module, mock_cephadm):
    jaeger_spec = TracingSpec(service_type="jaeger")
    es_spec = TracingSpec(service_type="elasticsearch")
    with contextlib.ExitStack() as stack:
        stack.enter_context(with_host(cephadm_module, "test"))
        stack.enter_context(with_service(cephadm_module, jaeger_spec))
        stack.enter_context(with_service(cephadm_module, es_spec))
        # manually invoke _check_daemons to trigger a call to
        # _daemon_action so we can check what action was chosen
        mock_cephadm.serve(cephadm_module)._check_daemons()
        mock_cephadm._daemon_action.assert_called_with(ANY, action="redeploy")
