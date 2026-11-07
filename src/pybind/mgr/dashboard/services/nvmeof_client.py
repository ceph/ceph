# pylint: disable=unexpected-keyword-arg

import functools
import logging
from typing import Annotated, Any, Callable, Dict, Generator, List, \
    NamedTuple, Optional, Type, get_args, get_origin

from ..exceptions import DashboardException
from ..services.ceph_service import CephService
from .nvmeof_conf import NvmeofGatewaysConfig, is_mtls_enabled

logger = logging.getLogger("nvmeof_client")

try:
    # if the protobuf version is newer than what we generated with
    # proto file import will fail (because of differences between what's
    # available in centos and ubuntu).
    # this "hack" should be removed once we update both the
    # distros; centos and ubuntu.
    import os
    os.environ["PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION"] = "python"

    import grpc  # type: ignore
    import grpc._channel  # type: ignore
    from google.protobuf.json_format import MessageToDict  # type: ignore
    from google.protobuf.message import Message  # type: ignore

    from .proto import gateway_pb2 as pb2  # type: ignore
    from .proto import gateway_pb2_grpc as pb2_grpc  # type: ignore
except ImportError:
    grpc = None
else:
    GATEWAY_CONNECT_TIMEOUT = 2

    class _FailoverStub(object):
        def __init__(self, client: 'NVMeoFClient'):
            self._client = client

        def __getattr__(self, method_name: str) -> Callable[..., Any]:
            def call(*args, **kwargs):
                return self._client._invoke(  # pylint: disable=protected-access
                    method_name, *args, **kwargs)
            return call

    class NVMeoFClient(object):
        pb2 = pb2

        def __init__(self, gw_group: Optional[str] = None, server_address: Optional[str] = None):

            logger.info("Initiating nvmeof gateway connection...")
            try:
                if not gw_group:
                    res = NvmeofGatewaysConfig.get_service_info()
                else:
                    res = NvmeofGatewaysConfig.get_service_info(gw_group)
                if res is None:
                    raise DashboardException("Gateway group does not exist")
                service_name, self.gateway_addr, self.daemon_name = res
            except TypeError as e:
                raise DashboardException(
                    f'Unable to retrieve the gateway info: {e}'
                )

            # While creating listener need to direct request to the gateway
            # address where listener is supposed to be added.
            if server_address:
                gateways_info = NvmeofGatewaysConfig.get_gateways_config()
                matched_gateway = next(
                    (
                        gateway
                        for gateways in gateways_info['gateways'].values()
                        for gateway in gateways
                        if server_address in gateway['service_url']
                    ),
                    None
                )
                if matched_gateway:
                    self.daemon_name = matched_gateway.get('daemon_name')
                    self.gateway_addr = matched_gateway.get('service_url')
                    logger.debug("Gateway address set to: %s", self.gateway_addr)
                else:
                    raise DashboardException(
                        msg=f"No gateway found matching server address: {server_address}",
                        code='server_address_not_found',
                        component='nvmeof',
                        http_status_code=400
                    )
            self.service_name = service_name

            self._fallbacks: List[Dict[str, Any]] = []
            if not server_address:
                gateways_info = NvmeofGatewaysConfig.get_gateways_config()
                self._fallbacks = [
                    gateway
                    for gateway in gateways_info.get('gateways', {}).get(service_name, [])
                    if gateway.get('service_url') != self.gateway_addr
                ]

            self._stubs: Dict[str, Any] = {}
            self._channels: Dict[str, Any] = {}
            self.channel = self._create_channel(self.gateway_addr, self.daemon_name)
            if self.channel is not None:
                self._channels[self.gateway_addr] = self.channel
                self._stubs[self.gateway_addr] = pb2_grpc.GatewayStub(self.channel)
                self.stub = _FailoverStub(self)

        @staticmethod
        def _encode_tls_bundle(bundle: Dict[str, str]) -> Dict[str, bytes]:
            """Encode TLS bundle string values to bytes for gRPC."""
            encoded: Dict[str, bytes] = {}
            for key, value in bundle.items():
                if isinstance(value, str):
                    encoded[key] = value.encode('utf-8')
                else:
                    encoded[key] = value
            return encoded

        def _create_channel(self, gateway_addr: str, daemon_name: Any):
            if is_mtls_enabled(self.service_name):
                tls_bundle = NvmeofGatewaysConfig.get_nvmeof_tls_bundle(self.service_name,
                                                                        daemon_name)
                if not tls_bundle:
                    logger.error("Cannot obtain nvmeof TLS bundle for the service %s (gw: %s)",
                                 self.service_name, gateway_addr)
                    return None
                logger.info('Securely connecting to: %s', gateway_addr)
                encoded_tls_bundle = self._encode_tls_bundle(tls_bundle)
                credentials = grpc.ssl_channel_credentials(
                    root_certificates=encoded_tls_bundle['server_cert'],
                    private_key=encoded_tls_bundle['client_key'],
                    certificate_chain=encoded_tls_bundle['client_cert'],
                )
                return grpc.secure_channel(gateway_addr, credentials)
            logger.info("Insecurely connecting to: %s", gateway_addr)
            return grpc.insecure_channel(gateway_addr)

        def _get_stub(self, gateway_addr: str, daemon_name: Any):
            if gateway_addr not in self._stubs:
                channel = self._create_channel(gateway_addr, daemon_name)
                if channel is None:
                    return None
                self._channels[gateway_addr] = channel
                self._stubs[gateway_addr] = pb2_grpc.GatewayStub(channel)
            return self._stubs[gateway_addr]

        def _is_reachable(self, gateway_addr: str) -> bool:
            """Return True if the channel connects within the timeout (instant if connected)."""
            ready = grpc.channel_ready_future(self._channels[gateway_addr])
            try:
                ready.result(timeout=GATEWAY_CONNECT_TIMEOUT)
                return True
            except grpc.FutureTimeoutError:
                ready.cancel()
                return False

        def _invoke(self, method_name: str, *args, **kwargs):
            targets = [(self.gateway_addr, self.daemon_name)] + [
                (gw['service_url'], gw.get('daemon_name')) for gw in self._fallbacks
            ]
            last_exc = None
            for gateway_addr, daemon_name in targets:
                stub = self._get_stub(gateway_addr, daemon_name)
                if stub is None:
                    continue
                # Only probe when there is somewhere to fail over to; a single
                # gateway keeps the original behaviour.
                if self._fallbacks and not self._is_reachable(gateway_addr):
                    logger.warning("Gateway %s is not reachable (timeout %ss) for %s",
                                   gateway_addr, GATEWAY_CONNECT_TIMEOUT, method_name)
                    continue
                try:
                    response = getattr(stub, method_name)(*args, **kwargs)
                except grpc.RpcError as e:
                    if e.code() != grpc.StatusCode.UNAVAILABLE:
                        raise
                    logger.warning("Gateway %s is unavailable for %s: %s",
                                   gateway_addr, method_name, e.details())
                    last_exc = e
                    continue
                if gateway_addr != self.gateway_addr:
                    logger.warning("Failing over from gateway %s to %s",
                                   self.gateway_addr, gateway_addr)
                    self.gateway_addr = gateway_addr
                    self.daemon_name = daemon_name
                    self.channel = self._channels[gateway_addr]
                return response
            if last_exc is not None:
                raise last_exc
            raise DashboardException(
                msg=f"No reachable NVMe-oF gateway found for service {self.service_name}",
                code='gateway_unavailable',
                http_status_code=504,
                component="nvmeof",
            )

    Model = Dict[str, Any]
    Collection = List[Model]

    import errno

    NVMeoFError2HTTP = {
        # errno errors
        errno.EPERM: 403,  # 1
        errno.ENOENT: 404,  # 2
        errno.EACCES: 403,  # 13
        errno.EEXIST: 409,  # 17
        errno.ENODEV: 404,  # 19
        # JSONRPC Spec: https://www.jsonrpc.org/specification#error_object
        -32602: 422,  # Invalid Params
        -32603: 500,  # Internal Error
    }

    def handle_nvmeof_error(func: Callable[..., Message]) -> Callable[..., Message]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Message:
            try:
                response = func(*args, **kwargs)
            except grpc._channel._InactiveRpcError as e:  # pylint: disable=protected-access
                raise DashboardException(
                    msg=e.details(),
                    code=e.code(),
                    http_status_code=504,
                    component="nvmeof",
                )

            status = getattr(response, "status", None)
            error_message = getattr(response, "error_message", None)

            # Normalize the response so callers do not see a non-zero status in
            # a successful HTTP response.
            if status == errno.EREMOTE:
                response.status = 0
                if hasattr(response, "error_message"):
                    response.error_message = ""
                return response
            if status not in (None, 0):
                raise DashboardException(
                    msg=error_message or "NVMeoF operation failed",
                    code=status,
                    http_status_code=NVMeoFError2HTTP.get(status, 400),  # type: ignore[arg-type]
                    component="nvmeof",
                )
            return response

        return wrapper

    def empty_response(func: Callable[..., Message]) -> Callable[..., None]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> None:
            func(*args, **kwargs)

        return wrapper

    class MaxRecursionDepthError(Exception):
        pass

    def _convert(value, field_type, depth, max_depth) -> Generator:
        if depth > max_depth:
            raise MaxRecursionDepthError(
                f"Maximum nesting depth of {max_depth} exceeded at depth {depth}.")

        if isinstance(value, dict) and hasattr(field_type, '_fields'):
            # Lazily create NamedTuple for nested dicts
            yield from _lazily_create_namedtuple(value, field_type, depth + 1, max_depth)
        elif isinstance(value, list):
            # Handle empty lists directly
            if not value:
                yield []
            else:
                # Lazily process each item in the list based on the expected item type
                item_type = field_type.__args__[0] if hasattr(field_type, '__args__') else None
                processed_items = []
                for v in value:
                    if item_type:
                        processed_items.append(next(_convert(v, item_type,
                                                             depth + 1, max_depth), None))
                    else:
                        processed_items.append(v)
                yield processed_items
        else:
            # Yield the value as is for simple types
            yield value

    def _lazily_create_namedtuple(data: Any, target_type: Type[NamedTuple],
                                  depth: int, max_depth: int) -> Generator:
        # pylint: disable=protected-access
        """ Lazily create NamedTuple from a dict """
        field_values = {}
        for field, field_type in zip(target_type._fields,
                                     target_type.__annotations__.values()):
            if get_origin(field_type) == Annotated:
                field_type = get_args(field_type)[0]
            # these conditions are complex since we need to navigate between dicts,
            # empty dicts and objects
            if isinstance(data, dict) and data.get(field) is not None:
                try:
                    field_values[field] = next(_convert(data.get(field), field_type,
                                                        depth, max_depth), None)
                except StopIteration:
                    return
            elif hasattr(data, field):
                try:
                    field_values[field] = next(_convert(getattr(data, field), field_type,
                                                        depth, max_depth), None)
                except StopIteration:
                    return
            else:
                field_values[field] = target_type._field_defaults.get(field)

        namedtuple_instance = target_type(**field_values)  # type: ignore
        yield namedtuple_instance

    def obj_to_namedtuple(data: Any, target_type: Type[NamedTuple],
                          max_depth: int = 7) -> NamedTuple:
        """
        Convert an object or dict to a NamedTuple, handling nesting and lists lazily.
        This will raise an error if nesting depth exceeds the max depth (default 4)
        to avoid bloating the memory in case of mutual references between objects.

        :param data: The input data - object or dictionary
        :param target_type: The target NamedTuple type
        :param max_depth: The maximum depth allowed for recursion
        :return: An instance of the target NamedTuple with fields populated from the JSON
        """

        if not isinstance(target_type, type) or not hasattr(target_type, '_fields'):
            raise TypeError("target_type must be a NamedTuple type.")
        if isinstance(data, list):
            raise TypeError("data can't be a list.")
        if data is None:
            raise TypeError("data can't be None.")
        namedtuple_values = next(_lazily_create_namedtuple(data, target_type, 1, max_depth))
        return namedtuple_values

    def namedtuple_to_dict(obj):
        if isinstance(obj, tuple) and hasattr(obj, '_asdict'):
            # If it's a namedtuple, convert it to a dictionary
            return {k: namedtuple_to_dict(v) for k, v in obj._asdict().items()}
        if isinstance(obj, list):
            # If it's a list, check each item and convert if it's a namedtuple
            return [
                namedtuple_to_dict(item)
                if isinstance(item, tuple) and hasattr(item, '_asdict')
                else item
                for item in obj
            ]
        return obj

    def convert_to_model(model: Type[NamedTuple],
                         finalize: Optional[Callable[[Dict], Dict]] = None
                         ) -> Callable[..., Callable[..., Model]]:
        def decorator(func: Callable[..., Message]) -> Callable[..., Model]:
            @functools.wraps(func)
            def wrapper(*args, **kwargs) -> Model:
                message = func(*args, **kwargs)
                msg_dict = MessageToDict(message, including_default_value_fields=True,
                                         preserving_proto_field_name=True)  # type: ignore

                result = namedtuple_to_dict(obj_to_namedtuple(msg_dict, model))
                if finalize:
                    return finalize(result)
                return result

            return wrapper

        return decorator

    # pylint: disable-next=redefined-outer-name
    def pick(field: str, first: bool = False,
             ) -> Callable[..., Callable[..., object]]:
        def decorator(func: Callable[..., Dict]) -> Callable[..., object]:
            @functools.wraps(func)
            def wrapper(*args, **kwargs) -> object:
                model = func(*args, **kwargs)
                field_to_ret = model[field]
                if first:
                    field_to_ret = field_to_ret[0]
                return field_to_ret
            return wrapper
        return decorator


def get_gateway_locations(pool: str, group: str, hosts: Optional[List[str]] = None):
    """
    Get locations for gateways in a service group using nvme-gw show command.

    Args:
        pool: The RBD pool name
        group: The NVMeoF gateway group name
        hosts: Optional list of hostnames to match locations to (in order)

    Returns:
        If hosts provided: List of location strings matching the order of hosts
        If hosts not provided: List of unique location strings (sorted)
    """
    try:  # pylint: disable=too-many-nested-blocks
        if not pool or not group:
            logger.warning('Pool or group not provided for location lookup: pool=%s, group=%s',
                           pool, group)
            return []

        result = CephService.send_command('mon', 'nvme-gw show',
                                          pool=pool, group=group)

        # Build a mapping of hostname to location
        host_to_location = {}
        if isinstance(result, dict):
            gateways = result.get('Created Gateways:', [])

            for gw in gateways:
                if isinstance(gw, dict):
                    gw_id = gw.get('gw-id', '')
                    location = gw.get('location', '')

                    # Extract hostname from gw-id (format: client.nvmeof.pool.group.hostname.xxx)
                    if gw_id:
                        parts = gw_id.split('.')
                        if len(parts) >= 5:
                            hostname = parts[4]
                            if location:
                                host_to_location[hostname] = location
                        else:
                            # If format is unexpected, log warning
                            logger.warning('Unexpected gateway ID format: %s', gw_id)

        # If hosts list provided, return locations in the same order
        if hosts:
            locations = []
            for host in hosts:
                # Get location for this host, empty string if not found
                location = host_to_location.get(host, '')
                if not location:
                    logger.debug('No location found for host: %s', host)
                locations.append(location)
            return locations

        # Otherwise return unique sorted locations
        unique_locations = set(host_to_location.values())
        return sorted(list(unique_locations))

    except Exception as e:  # pylint: disable=broad-except
        logger.error('Failed to get gateway locations for pool=%s, group=%s: %s',
                     pool, group, e)
        return []
