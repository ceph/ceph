# -*- coding: utf-8 -*-

import logging
import threading
from typing import Any, Dict, List, Optional

from ..exceptions import DashboardException
from .nvmeof_client import NVMeoFClient, grpc, pb2_grpc
from .nvmeof_conf import NvmeofGatewaysConfig

logger = logging.getLogger("nvmeof_failover")

# How long a gateway may take to connect before it is skipped. A stopped host
# drops packets silently, and without this bound gRPC retries it for ~20s.
CONNECT_TIMEOUT_SECONDS: float = 5.0


class _NoChannel:
    """Placeholder channel. GatewayStub only needs it to name its RPCs; no call is made on it."""

    def unary_unary(self, *_args, **_kwargs):
        return None


class _FailoverStub:
    """
    This is meant to provide the same interface as NVMeoFClient Stub
    `client.stub.set_gateway_log_level(req)`. calls run through the
    owning FailoverNVMeoFClient, which picks the gateway.
    """

    def __init__(self, owner: 'FailoverNVMeoFClient', idempotent: bool):
        self._owner = owner
        self._idempotent = idempotent
        names = [n for n in vars(pb2_grpc.GatewayStub(_NoChannel())) if not n.startswith('_')]
        for name in names:
            setattr(self, name, self._make(name))

    def _make(self, method_name: str):
        # pylint: disable=protected-access
        def rpc(request: Any) -> Any:
            return self._owner._call(method_name, request, self._idempotent)
        rpc.__name__ = method_name
        return rpc


class FailoverNVMeoFClient(NVMeoFClient):
    """
    NVMeoFClient that retries an RPC on the other gateways of the group.

    Gateways are tried in the order of the gateways config. A gateway is skipped
    if it does not connect within CONNECT_TIMEOUT_SECONDS, or if it answers
    UNAVAILABLE. A gateway that succeeds becomes the active one.

    `idempotent=False` is set on writes. Once the channel is connected the
    request may already have executed, so UNAVAILABLE is raised, not retried.

    A client pinned with `server_address` does not fail over, so use NVMeoFClient
    for those.
    """

    _rpc: Any

    def __init__(self, gw_group: Optional[str] = None, server_address: Optional[str] = None,
                 idempotent: bool = True):
        super().__init__(gw_group=gw_group, server_address=server_address)
        self._gw_group = gw_group
        self._pinned: Dict[str, NVMeoFClient] = {}
        self._urls: List[str] = []
        if server_address is None:
            gateways = NvmeofGatewaysConfig.get_gateways_config().get('gateways', {})
            self._urls = [gw['service_url'] for gw in gateways.get(self.service_name, [])]
        # The raw gRPC stub of the active gateway, kept apart from the failover stub.
        self._rpc = self.stub if hasattr(self, 'stub') else None
        self.stub = _FailoverStub(self, idempotent)

    def _endpoint(self, url: str) -> Optional[NVMeoFClient]:
        if url == self.gateway_addr:
            return self
        if url not in self._pinned:
            try:
                self._pinned[url] = NVMeoFClient(gw_group=self._gw_group, server_address=url)
            except DashboardException as e:
                logger.warning("Cannot create client for gateway %s: %s", url, e)
                return None
        return self._pinned[url]

    @staticmethod
    def _is_connected(channel: Any) -> bool:
        """Wait until the channel is READY or has failed, up to CONNECT_TIMEOUT_SECONDS.

        A refused connection reports TRANSIENT_FAILURE within milliseconds. Only a host
        that silently drops packets produces no state change, and then the timeout applies.
        """
        if channel is None:
            return False
        settled = threading.Event()
        result: List[bool] = []

        def on_state(state: Any) -> None:
            if state in (grpc.ChannelConnectivity.READY,
                         grpc.ChannelConnectivity.TRANSIENT_FAILURE):
                result.append(state == grpc.ChannelConnectivity.READY)
                settled.set()

        channel.subscribe(on_state, try_to_connect=True)
        try:
            settled.wait(CONNECT_TIMEOUT_SECONDS)
        finally:
            channel.unsubscribe(on_state)
        return bool(result and result[0])

    def _call(self, method_name: str, request: Any, idempotent: bool) -> Any:
        if not self._urls:
            return getattr(self._rpc, method_name)(request)

        last_error: Optional[grpc.RpcError] = None
        for url in self._urls:
            endpoint = self._endpoint(url)
            raw = self._rpc if endpoint is self else getattr(endpoint, 'stub', None)
            if endpoint is None or raw is None:
                continue
            if not self._is_connected(endpoint.channel):
                logger.warning("Gateway %s did not connect within %ss; trying next",
                               url, CONNECT_TIMEOUT_SECONDS)
                continue
            try:
                response = getattr(raw, method_name)(request)
            except grpc.RpcError as e:
                if e.code() != grpc.StatusCode.UNAVAILABLE or not idempotent:
                    raise
                last_error = e
                logger.warning("Gateway %s unavailable for %s; trying next",
                               url, method_name)
                continue
            if endpoint is not self:
                self._switch_to(endpoint)
            return response

        if last_error is not None:
            raise last_error
        raise DashboardException(
            msg=f"No reachable NVMe-oF gateway found for service {self.service_name}",
            code='gateway_unavailable',
            http_status_code=504,
            component='nvmeof',
        )

    def _switch_to(self, endpoint: NVMeoFClient) -> None:
        logger.warning("Failing over from gateway %s to %s",
                       self.gateway_addr, endpoint.gateway_addr)
        self.gateway_addr = endpoint.gateway_addr
        self.daemon_name = endpoint.daemon_name
        self.channel = endpoint.channel
        self._rpc = endpoint.stub
