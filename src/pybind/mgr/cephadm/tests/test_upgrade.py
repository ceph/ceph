import json
from unittest import mock

import pytest

from ceph.deployment.service_spec import ServiceSpec
from cephadm import CephadmOrchestrator
from cephadm.upgrade import CephadmUpgrade
from cephadm.serve import CephadmServe
from .fixtures import _run_cephadm, wait, cephadm_module, with_host, with_service


@mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
def test_upgrade_start(cephadm_module: CephadmOrchestrator):
    with with_host(cephadm_module, 'test'):
        assert wait(cephadm_module, cephadm_module.upgrade_start(
            'image_id', None)) == 'Initiating upgrade to image_id'

        assert wait(cephadm_module, cephadm_module.upgrade_status()).target_image == 'image_id'

        assert wait(cephadm_module, cephadm_module.upgrade_pause()) == 'Paused upgrade to image_id'

        assert wait(cephadm_module, cephadm_module.upgrade_resume()
                    ) == 'Resumed upgrade to image_id'

        assert wait(cephadm_module, cephadm_module.upgrade_stop()) == 'Stopped upgrade to image_id'


@mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
@pytest.mark.parametrize("use_repo_digest",
                         [
                             False,
                             True
                         ])
def test_upgrade_run(use_repo_digest, cephadm_module: CephadmOrchestrator):
    with with_host(cephadm_module, 'test'):
        cephadm_module.set_container_image('global', 'from_image')
        if use_repo_digest:
            cephadm_module.use_repo_digest = True
        with with_service(cephadm_module, ServiceSpec('mgr'), CephadmOrchestrator.apply_mgr, 'test'):
            assert wait(cephadm_module, cephadm_module.upgrade_start(
                'to_image', None)) == 'Initiating upgrade to to_image'

            assert wait(cephadm_module, cephadm_module.upgrade_status()).target_image == 'to_image'

            def _versions_mock(cmd):
                return json.dumps({
                    'mgr': {
                        'myversion': 1
                    }
                })

            cephadm_module._mon_command_mock_versions = _versions_mock

            with mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm(json.dumps({
                'image_id': 'image_id',
                'repo_digest': 'to_image@repo_digest',
            }))):

                cephadm_module.upgrade._do_upgrade()

            assert cephadm_module.upgrade_status is not None

            with mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm(
                json.dumps([
                    dict(
                        name=list(cephadm_module.cache.daemons['test'].keys())[0],
                        style='cephadm',
                        fsid='fsid',
                        container_id='container_id',
                        container_image_id='image_id',
                        version='version',
                        state='running',
                    )
                ])
            )):
                CephadmServe(cephadm_module)._refresh_hosts_and_daemons()

            cephadm_module.upgrade._do_upgrade()

            _, image, _ = cephadm_module.check_mon_command({
                'prefix': 'config get',
                'who': 'global',
                'key': 'container_image',
            })
            if use_repo_digest:
                assert image == 'to_image@repo_digest'
            else:
                assert image == 'to_image'


def test_upgrade_state_null(cephadm_module: CephadmOrchestrator):
    # This test validates https://tracker.ceph.com/issues/47580
    cephadm_module.set_store('upgrade_state', 'null')
    CephadmUpgrade(cephadm_module)
    assert CephadmUpgrade(cephadm_module).upgrade_state is None
