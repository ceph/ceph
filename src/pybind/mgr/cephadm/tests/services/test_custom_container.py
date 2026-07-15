import contextlib
import json
from unittest.mock import patch, ANY

import pytest

from cephadm.module import CephadmOrchestrator
from ceph.deployment.service_spec import CustomContainerSpec
from cephadm.tests.fixtures import with_host, with_service, async_side_effect


class TestCustomContainer:
    @patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_deploy_custom_container(
        self, _run_cephadm, cephadm_module: CephadmOrchestrator
    ):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        spec = CustomContainerSpec(
            service_id='tsettinu',
            image='quay.io/foobar/barbaz:latest',
            entrypoint='/usr/local/bin/blat.sh',
            ports=[9090],
        )

        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, spec):
                _run_cephadm.assert_called_with(
                    'test',
                    "container.tsettinu.test",
                    ['_orch', 'deploy'],
                    [],
                    stdin=json.dumps(
                        {
                            "fsid": "fsid",
                            "name": 'container.tsettinu.test',
                            "image": 'quay.io/foobar/barbaz:latest',
                            "deploy_arguments": [],
                            "params": {
                                'tcp_ports': [9090],
                            },
                            "meta": {
                                'service_name': 'container.tsettinu',
                                'ports': [],
                                'ip': None,
                                'deployed_by': [],
                                'rank': None,
                                'rank_generation': None,
                                'extra_container_args': None,
                                'extra_entrypoint_args': None,
                            },
                            "config_blobs": {
                                "image": "quay.io/foobar/barbaz:latest",
                                "entrypoint": "/usr/local/bin/blat.sh",
                                "args": [],
                                "envs": [],
                                "volume_mounts": {},
                                "privileged": False,
                                "ports": [9090],
                                "dirs": [],
                                "files": {},
                            },
                        }
                    ),
                    error_ok=True,
                    use_current_daemon_image=False,
                )

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_deploy_custom_container_with_init_ctrs(
        self, _run_cephadm, cephadm_module: CephadmOrchestrator
    ):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        spec = CustomContainerSpec(
            service_id='tsettinu',
            image='quay.io/foobar/barbaz:latest',
            entrypoint='/usr/local/bin/blat.sh',
            ports=[9090],
            init_containers=[
                {'entrypoint': '/usr/local/bin/prepare.sh'},
                {
                    'entrypoint': '/usr/local/bin/optimize.sh',
                    'entrypoint_args': [
                        '--timeout=5m',
                        '--cores=8',
                        {'argument': '--title=Alpha One'},
                    ],
                },
            ],
        )

        expected = {
            'fsid': 'fsid',
            'name': 'container.tsettinu.test',
            'image': 'quay.io/foobar/barbaz:latest',
            'deploy_arguments': [],
            'params': {
                'tcp_ports': [9090],
                'init_containers': [
                    {'entrypoint': '/usr/local/bin/prepare.sh'},
                    {
                        'entrypoint': '/usr/local/bin/optimize.sh',
                        'entrypoint_args': [
                            '--timeout=5m',
                            '--cores=8',
                            '--title=Alpha One',
                        ],
                    },
                ],
            },
            'meta': {
                'service_name': 'container.tsettinu',
                'ports': [],
                'ip': None,
                'deployed_by': [],
                'rank': None,
                'rank_generation': None,
                'extra_container_args': None,
                'extra_entrypoint_args': None,
                'init_containers': [
                    {'entrypoint': '/usr/local/bin/prepare.sh'},
                    {
                        'entrypoint': '/usr/local/bin/optimize.sh',
                        'entrypoint_args': [
                            '--timeout=5m',
                            '--cores=8',
                            {'argument': '--title=Alpha One', 'split': False},
                        ],
                    },
                ],
            },
            'config_blobs': {
                'image': 'quay.io/foobar/barbaz:latest',
                'entrypoint': '/usr/local/bin/blat.sh',
                'args': [],
                'envs': [],
                'volume_mounts': {},
                'privileged': False,
                'ports': [9090],
                'dirs': [],
                'files': {},
            },
        }
        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, spec):
                _run_cephadm.assert_called_with(
                    'test',
                    'container.tsettinu.test',
                    ['_orch', 'deploy'],
                    [],
                    stdin=json.dumps(expected),
                    error_ok=True,
                    use_current_daemon_image=False,
                )


@pytest.mark.parametrize(
    'newargs',
    [
        {'cargs': ['--secret=foo,target=/etc/foo.x']},
        {'eargs': ['flip flop']},
        {'eargs': ['flip flop'], "cargs": None},
    ],
)
def test_custom_cont_choose_next_action(
    cephadm_module, mock_cephadm, newargs
):
    cargs1 = cargs2 = ['--secret=foo,type=env']
    eargs1 = eargs2 = ['flim flam']
    if 'cargs' in newargs:
        cargs2 = newargs['cargs']
    if 'eargs' in newargs:
        eargs2 = newargs['eargs']
    spec = CustomContainerSpec(
        service_id='tsettinu',
        image='quay.io/foobar/barbaz:latest',
        entrypoint='/usr/local/bin/blat.sh',
        ports=[9090],
        extra_container_args=cargs1,
        extra_entrypoint_args=eargs1,
    )
    # use a second spec to avoid messing around with ArgumentSpec types
    spec2 = CustomContainerSpec(
        service_id='tsettinu',
        image='quay.io/foobar/barbaz:latest',
        entrypoint='/usr/local/bin/blat.sh',
        ports=[9090],
        extra_container_args=cargs2,
        extra_entrypoint_args=eargs2,
    )
    with contextlib.ExitStack() as stack:
        stack.enter_context(with_host(cephadm_module, 'test'))
        stack.enter_context(with_service(cephadm_module, spec))
        cephadm_module.apply([spec2])
        # manually invoke _check_daemons to trigger a call to
        # _daemon_action so we can check what action was chosen
        mock_cephadm.serve(cephadm_module)._check_daemons()
        mock_cephadm._daemon_action.assert_called_with(ANY, action='redeploy')
