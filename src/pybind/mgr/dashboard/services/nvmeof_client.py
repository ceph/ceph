import functools
import logging
from collections.abc import Iterable
from typing import Any, Callable, Dict, List, NamedTuple, Optional, Type

from ..exceptions import DashboardException
from .nvmeof_conf import NvmeofGatewaysConfig

logger = logging.getLogger("nvmeof_client")

try:
    import grpc  # type: ignore
    import grpc._channel  # type: ignore
    from google.protobuf.message import Message  # type: ignore

    from .proto import gateway_pb2 as pb2
    from .proto import gateway_pb2_grpc as pb2_grpc
except ImportError:
    grpc = None
else:

    class NVMeoFClient(object):
        pb2 = pb2

        def __init__(self, gw_group: Optional[str] = None, traddr: Optional[str] = None):
            logger.info("Initiating nvmeof gateway connection...")
            try:
                if not gw_group:
                    service_name, self.gateway_addr, enable_auth = NvmeofGatewaysConfig.get_service_info()
                else:
                    service_name, self.gateway_addr, enable_auth = NvmeofGatewaysConfig.get_service_info(
                        gw_group
                    )
            except TypeError as e:
                raise DashboardException(
                    f'Unable to retrieve the gateway info: {e}'
                )

            # While creating listener need to direct request to the gateway
            # address where listener is supposed to be added.
            if traddr:
                gateways_info = NvmeofGatewaysConfig.get_gateways_config()
                matched_gateway = next(
                    (
                        gateway
                        for gateways in gateways_info['gateways'].values()
                        for gateway in gateways
                        if traddr in gateway['service_url']
                    ),
                    None
                )
                if matched_gateway:
                    self.gateway_addr = matched_gateway.get('service_url')
                    logger.debug("Gateway address set to: %s", self.gateway_addr)

            root_ca_cert = None
            if enable_auth:
                root_ca_cert = NvmeofGatewaysConfig.get_root_ca_cert(service_name)

            if root_ca_cert:
                client_key = NvmeofGatewaysConfig.get_client_key(service_name)
                client_cert = NvmeofGatewaysConfig.get_client_cert(service_name)

            if root_ca_cert and client_key and client_cert:
                logger.info('Securely connecting to: %s', self.gateway_addr)
                credentials = grpc.ssl_channel_credentials(
                    root_certificates=root_ca_cert,
                    private_key=client_key,
                    certificate_chain=client_cert,
                )
                self.channel = grpc.secure_channel(self.gateway_addr, credentials)
            else:
                logger.info("Insecurely connecting to: %s", self.gateway_addr)
                self.channel = grpc.insecure_channel(self.gateway_addr)
            self.stub = pb2_grpc.GatewayStub(self.channel)

    def make_namedtuple_from_object(cls: Type[NamedTuple], obj: Any) -> NamedTuple:
        return cls(
            **{
                field: getattr(obj, field)
                for field in cls._fields
                if hasattr(obj, field)
            }
        )  # type: ignore

    Model = Dict[str, Any]

    def map_model(
        model: Type[NamedTuple],
        first: Optional[str] = None,
    ) -> Callable[..., Callable[..., Model]]:
        def decorator(func: Callable[..., Message]) -> Callable[..., Model]:
            @functools.wraps(func)
            def wrapper(*args, **kwargs) -> Model:
                message = func(*args, **kwargs)
                if first:
                    try:
                        message = getattr(message, first)[0]
                    except IndexError:
                        raise DashboardException(
                            msg="Not Found", http_status_code=404, component="nvmeof"
                        )

                return make_namedtuple_from_object(model, message)._asdict()

            return wrapper

        return decorator

    Collection = List[Model]

    def map_collection(
        model: Type[NamedTuple],
        pick: str,
        finalize: Optional[Callable[[Message, Collection], Collection]] = None,
    ) -> Callable[..., Callable[..., Collection]]:
        def decorator(func: Callable[..., Message]) -> Callable[..., Collection]:
            @functools.wraps(func)
            def wrapper(*args, **kwargs) -> Collection:
                message = func(*args, **kwargs)
                collection: Iterable = getattr(message, pick)
                out = [
                    make_namedtuple_from_object(model, i)._asdict() for i in collection
                ]
                if finalize:
                    return finalize(message, out)
                return out

            return wrapper

        return decorator

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

            if response.status != 0:
                raise DashboardException(
                    msg=response.error_message,
                    code=response.status,
                    http_status_code=NVMeoFError2HTTP.get(response.status, 400),
                    component="nvmeof",
                )
            return response

        return wrapper

    def empty_response(func: Callable[..., Message]) -> Callable[..., None]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> None:
            func(*args, **kwargs)

        return wrapper
