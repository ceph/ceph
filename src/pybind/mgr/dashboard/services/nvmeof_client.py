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

        def __init__(self):
            logger.info("Initiating nvmeof gateway connection...")

            self.gateway_addr = list(
                NvmeofGatewaysConfig.get_gateways_config()["gateways"].values()
            )[0]["service_url"]
            self.channel = grpc.insecure_channel("{}".format(self.gateway_addr))
            logger.info("Found nvmeof gateway at %s", self.gateway_addr)
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
