from unittest.mock import MagicMock, patch

import pytest

from ..controllers import nvmeof as nvmeof_controller
from ..services import nvmeof_client


@pytest.fixture(name='nvmeof_client_mock')
def fixture_nvmeof_client_mock(monkeypatch):
    """Stand in for NVMeoFClient, answering every RPC with a success.

    The keyword arguments each instance is built with decide the gateway
    the request goes to, and the mock records them.
    """
    monkeypatch.setattr(nvmeof_client, 'MessageToDict',
                        lambda msg, **kwargs: {'namespaces': [{}]})

    class SucceedingStub:
        def __getattr__(self, _name):
            return lambda *args, **kwargs: MagicMock(status=0, error_message='')

    with patch.object(nvmeof_controller, 'NVMeoFClient') as client:
        client.return_value.stub = SucceedingStub()
        yield client


@pytest.mark.parametrize('update', [
    {'rbd_image_size': 1024 ** 3},
    {'load_balancing_group': 3},
    {'rw_ios_per_second': 100},
    {'trash_image': True},
    {'location': 'dc1'},
])
def test_namespace_update_uses_requested_gateway(nvmeof_client_mock, update):
    nvmeof_controller.NVMeoFNamespace().update(
        nqn='nqn.2016-06.io.spdk:cnode1', nsid='1', gw_group='group-b', **update)

    assert nvmeof_client_mock.call_args_list
    for call in nvmeof_client_mock.call_args_list:
        assert call.kwargs.get('gw_group') == 'group-b'
