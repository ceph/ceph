# pylint: disable=unexpected-keyword-arg

import functools
import logging
from typing import Annotated, Any, Callable, Dict, Generator, List, \
    NamedTuple, Optional, Type, get_args, get_origin

from ..exceptions import DashboardException
from .nvmeof_conf import NvmeofGatewaysConfig

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

    class NVMeoFClient(object):
        pb2 = pb2

        def __init__(self, gw_group: Optional[str] = None, traddr: Optional[str] = None):
            logger.info("Initiating nvmeof gateway connection...")
            try:
                if not gw_group:
                    service_name, self.gateway_addr = NvmeofGatewaysConfig.get_service_info()
                else:
                    service_name, self.gateway_addr = NvmeofGatewaysConfig.get_service_info(
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
                          max_depth: int = 4) -> NamedTuple:
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
