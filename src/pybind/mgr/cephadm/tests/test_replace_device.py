import pytest
from mock import patch
from .fixtures import _run_cephadm, with_host, wait
from .ceph_volume_data import data
from cephadm.serve import CephadmServe
from cephadm import CephadmOrchestrator
from orchestrator import OrchestratorError


class TestReplaceDevice:
    def test_invalid_device(self, cephadm_module: CephadmOrchestrator) -> None:
        with patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]')):
            with with_host(cephadm_module, 'test'):
                CephadmServe(cephadm_module)._refresh_host_daemons('test')
                with patch('cephadm.serve.CephadmServe._run_cephadm', _run_cephadm(data)):
                    with pytest.raises(OrchestratorError) as e:
                        cephadm_module.replace_device('test', '/dev/invalid-device')
                    assert "/dev/invalid-device doesn't appear to be used for an OSD, not a valid device in test." in str(e.value)

    def test_invalid_hostname(self, cephadm_module: CephadmOrchestrator) -> None:
        with patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]')):
            with with_host(cephadm_module, 'test'):
                CephadmServe(cephadm_module)._refresh_host_daemons('test')
                with patch('cephadm.serve.CephadmServe._run_cephadm', _run_cephadm(data)):
                    with pytest.raises(OrchestratorError):
                        cephadm_module.replace_device('invalid-hostname', '/dev/vdb')

    def test_block_device(self, cephadm_module: CephadmOrchestrator) -> None:
        with patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]')):
            with with_host(cephadm_module, 'test'):
                CephadmServe(cephadm_module)._refresh_host_daemons('test')
                with patch('cephadm.serve.CephadmServe._run_cephadm', _run_cephadm(data)):
                    c = cephadm_module.replace_device('test', '/dev/vdb')
                    result = wait(cephadm_module, c)
                    assert result == "Scheduled to destroy osds: ['0'] and mark /dev/vdb as being replaced."

    def test_shared_db_device_no_ireallymeanit_flag(self, cephadm_module: CephadmOrchestrator) -> None:
        with patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]')):
            with with_host(cephadm_module, 'test'):
                CephadmServe(cephadm_module)._refresh_host_daemons('test')
                with patch('cephadm.serve.CephadmServe._run_cephadm', _run_cephadm(data)):
                    with pytest.raises(OrchestratorError) as e:
                        cephadm_module.replace_device('test', '/dev/vdk')
                    assert "/dev/vdk is a shared device.\nReplacing /dev/vdk implies destroying OSD(s): ['0', '1'].\nPlease, *be very careful*, this can be a very dangerous operation.\nIf you know what you are doing, pass --yes-i-really-mean-it" in str(e.value)

    def test_shared_db_device(self, cephadm_module: CephadmOrchestrator) -> None:
        with patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('[]')):
            with with_host(cephadm_module, 'test'):
                CephadmServe(cephadm_module)._refresh_host_daemons('test')
                with patch('cephadm.serve.CephadmServe._run_cephadm', _run_cephadm(data)):
                    c = cephadm_module.replace_device('test', '/dev/vdk', yes_i_really_mean_it=True)
                    result = wait(cephadm_module, c)
                    assert result == "Scheduled to destroy osds: ['0', '1'] and mark /dev/vdk as being replaced."
