# -*- coding: utf-8 -*-
# pylint: disable=protected-access,unused-argument,redefined-outer-name

from typing import Dict, List
from unittest.mock import MagicMock, patch

import grpc
import pytest

from ..exceptions import DashboardException
from ..services.nvmeof_client import NVMeoFClient
from ..services.nvmeof_failover import FailoverNVMeoFClient

GW1 = 'gw1:5500'
GW2 = 'gw2:5500'
GATEWAYS_CONFIG = {
    'gateways': {
        'nvmeof': [
            {'service_url': GW1, 'daemon_name': 'gw1'},
            {'service_url': GW2, 'daemon_name': 'gw2'},
        ]
    }
}


class FakeRpcError(grpc.RpcError):
    def __init__(self, code: grpc.StatusCode, details: str = 'rpc failed'):
        super().__init__()
        self._code = code
        self._details = details

    def code(self) -> grpc.StatusCode:
        return self._code

    def details(self) -> str:
        return self._details


def fake_client_init(stubs: Dict[str, MagicMock]):
    """Replacement for NVMeoFClient.__init__ that wires a mock stub per address.
    Without an explicit address the first configured gateway is used,
    as the real base client does.
    """
    def init(self, gw_group=None, server_address=None):
        address = server_address or GW1
        self.gateway_addr = address
        self.daemon_name = address
        self.service_name = 'nvmeof'
        self.channel = address
        self.stub = stubs[address]
    return init


def make_client(stubs: Dict[str, MagicMock], idempotent: bool = True) -> FailoverNVMeoFClient:
    with patch.object(NVMeoFClient, '__init__', fake_client_init(stubs)), \
            patch('dashboard.services.nvmeof_failover.NvmeofGatewaysConfig.get_gateways_config',
                  return_value=GATEWAYS_CONFIG):
        return FailoverNVMeoFClient(gw_group='grp', idempotent=idempotent)


def stubs_for(gw1: MagicMock, gw2: MagicMock) -> Dict[str, MagicMock]:
    return {GW1: gw1, GW2: gw2}


def unavailable() -> FakeRpcError:
    return FakeRpcError(grpc.StatusCode.UNAVAILABLE)


class TestFailoverNVMeoFClient:
    def test_healthy_primary_answers_without_switching(self):
        gw1, gw2 = MagicMock(), MagicMock()
        gw1.get_gateway_info.return_value = 'info-gw1'
        client = make_client(stubs_for(gw1, gw2))

        with patch.object(FailoverNVMeoFClient, '_is_connected', return_value=True):
            assert client.stub.get_gateway_info('req') == 'info-gw1'

        assert client.gateway_addr == GW1
        gw2.get_gateway_info.assert_not_called()

    def test_unconnected_primary_is_skipped(self):
        gw1, gw2 = MagicMock(), MagicMock()
        gw2.get_gateway_info.return_value = 'info-gw2'
        stubs = stubs_for(gw1, gw2)
        client = make_client(stubs)

        with patch.object(NVMeoFClient, '__init__', fake_client_init(stubs)), \
                patch.object(FailoverNVMeoFClient, '_is_connected',
                             side_effect=lambda channel: channel == GW2):
            assert client.stub.get_gateway_info('req') == 'info-gw2'

        assert client.gateway_addr == GW2
        gw1.get_gateway_info.assert_not_called()

    def test_unavailable_primary_fails_over_when_idempotent(self):
        gw1, gw2 = MagicMock(), MagicMock()
        gw1.get_gateway_info.side_effect = unavailable()
        gw2.get_gateway_info.return_value = 'info-gw2'
        stubs = stubs_for(gw1, gw2)
        client = make_client(stubs, idempotent=True)

        with patch.object(NVMeoFClient, '__init__', fake_client_init(stubs)), \
                patch.object(FailoverNVMeoFClient, '_is_connected', return_value=True):
            assert client.stub.get_gateway_info('req') == 'info-gw2'

        assert client.gateway_addr == GW2

    def test_unavailable_is_not_retried_when_not_idempotent(self):
        gw1, gw2 = MagicMock(), MagicMock()
        gw1.create_subsystem.side_effect = unavailable()
        client = make_client(stubs_for(gw1, gw2), idempotent=False)

        with patch.object(FailoverNVMeoFClient, '_is_connected', return_value=True):
            with pytest.raises(grpc.RpcError):
                client.stub.create_subsystem('req')

        gw2.create_subsystem.assert_not_called()

    def test_other_rpc_errors_are_raised_without_failover(self):
        gw1, gw2 = MagicMock(), MagicMock()
        gw1.get_gateway_info.side_effect = FakeRpcError(grpc.StatusCode.INVALID_ARGUMENT)
        client = make_client(stubs_for(gw1, gw2), idempotent=True)

        with patch.object(FailoverNVMeoFClient, '_is_connected', return_value=True):
            with pytest.raises(grpc.RpcError):
                client.stub.get_gateway_info('req')

        gw2.get_gateway_info.assert_not_called()

    def test_all_gateways_unreachable_raises_504(self):
        gw1, gw2 = MagicMock(), MagicMock()
        stubs = stubs_for(gw1, gw2)
        client = make_client(stubs)

        with patch.object(NVMeoFClient, '__init__', fake_client_init(stubs)), \
                patch.object(FailoverNVMeoFClient, '_is_connected', return_value=False):
            with pytest.raises(DashboardException) as exc:
                client.stub.get_gateway_info('req')

        assert exc.value.status == 504
        assert exc.value.component == 'nvmeof'
        gw1.get_gateway_info.assert_not_called()
        gw2.get_gateway_info.assert_not_called()

    def test_pinned_server_address_does_not_fail_over(self):
        stub = MagicMock()
        stub.get_gateway_info.side_effect = unavailable()
        with patch.object(NVMeoFClient, '__init__', fake_client_init({GW1: stub})):
            client = FailoverNVMeoFClient(gw_group='grp', server_address=GW1)

        with pytest.raises(grpc.RpcError):
            client.stub.get_gateway_info('req')

        stub.get_gateway_info.assert_called_once()

    def test_endpoints_are_created_once_and_cached(self):
        gw1, gw2 = MagicMock(), MagicMock()
        client = make_client(stubs_for(gw1, gw2))
        created: List[str] = []

        def counting_init(self, gw_group=None, server_address=None):
            created.append(server_address)
            self.gateway_addr = server_address
            self.stub = gw2

        with patch.object(NVMeoFClient, '__init__', counting_init):
            first = client._endpoint(GW2)
            second = client._endpoint(GW2)

        assert first is second
        assert created == [GW2]


WRITE_METHODS = {
    'set_gateway_log_level', 'set_gateway_io_stats_mode', 'gw_refresh_network',
    'set_spdk_nvmf_logs', 'disable_spdk_nvmf_logs', 'create_subsystem', 'delete_subsystem',
    'namespace_add', 'namespace_delete', 'add_host', 'remove_host', 'add_kmip_server_endpoints',
}


def test_write_rpcs_are_never_retried_after_unavailable():
    # A write may already have executed when the gateway reports UNAVAILABLE, so retrying
    # it on another gateway could apply it twice.
    import ast
    from pathlib import Path

    path = Path(__file__).parent.parent / 'controllers' / 'nvmeof.py'
    tree = ast.parse(path.read_text())
    checked = 0
    for node in ast.walk(tree):
        # Matches <FailoverNVMeoFClient(...)>.stub.<method>(...)
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and isinstance(node.func.value, ast.Attribute) and node.func.value.attr == 'stub'
                and isinstance(node.func.value.value, ast.Call)):
            continue
        method = node.func.attr
        if method not in WRITE_METHODS:
            continue
        ctor = node.func.value.value
        flags = {k.arg: k.value.value for k in ctor.keywords if k.arg == 'idempotent'}
        assert flags.get('idempotent') is False, f'{method} at line {node.lineno} may be retried'
        checked += 1
    assert checked == 16


def test_refused_gateway_is_detected_quickly():
    import socket
    import time

    sock = socket.socket()
    sock.bind(('127.0.0.1', 0))
    port = sock.getsockname()[1]
    sock.close()
    channel = grpc.insecure_channel(f'127.0.0.1:{port}')
    try:
        start = time.monotonic()
        assert FailoverNVMeoFClient._is_connected(channel) is False
        assert time.monotonic() - start < 2.0
    finally:
        channel.close()


def test_unresponsive_channel_times_out_without_state_change():
    channel = MagicMock()
    with patch('dashboard.services.nvmeof_failover.CONNECT_TIMEOUT_SECONDS', 0.05):
        assert FailoverNVMeoFClient._is_connected(channel) is False
    channel.unsubscribe.assert_called_once()
