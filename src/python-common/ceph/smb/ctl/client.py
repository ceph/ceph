"""Ceph SMB client gRPC library"""

import typing

# the grpc/protobuf object mapping is highly dynamic so we lean pretty heavily
# on Any types in this module.
from typing import Any

import collections.abc
import contextlib
import functools
import warnings

import google.protobuf.descriptor_pool
import google.protobuf.internal as pbint
import grpc
import grpc._channel as gch  # type: ignore
import grpc_reflection.v1alpha.proto_reflection_descriptor_database
from google.protobuf.descriptor import (
    MethodDescriptor,
)

from ._typing import Self
from .config import ChannelType, Config


class ReflectionDescriptorDB(
    grpc_reflection.v1alpha.proto_reflection_descriptor_database.ProtoReflectionDescriptorDatabase
):
    def _AddSymbol(self, name: Any, proto: Any) -> None:
        # not very clean but it worked to fix my issue
        # see also: https://github.com/protocolbuffers/protobuf/issues/9867
        # & https://github.com/protocolbuffers/protobuf/commit/
        #   @ 610702ed18d4323e44b9741102ed90377243470e
        if name.startswith('.'):
            name = name[1:]
        super()._AddSymbol(name, proto)


def _reflection_ddb(channel: Any) -> Any:
    if pbint.api_implementation.Type() == 'python':
        return ReflectionDescriptorDB(channel)
    gra = grpc_reflection.v1alpha
    return gra.proto_reflection_descriptor_database.ProtoReflectionDescriptorDatabase(
        channel
    )


def _get_message_class(pool: Any, desc: Any) -> Any:
    try:
        from google.protobuf.message_factory import GetMessageClass

        return GetMessageClass(desc)
    except ImportError:
        pass
    try:
        from google.protobuf.message_factory import MessageFactory

        return MessageFactory(pool).GetPrototype(desc)
    except ImportError:
        pass
    raise RuntimeError("no suitable method for getting message class")


def _iscontainer(obj: Any) -> bool:
    """Return true if obj is a container type even for protobuf container types
    with private type implementations.
    """
    # of course protobuf gotta make this painful and use strange private types
    # so we will use abc isinstance checks to see what methods they implement
    # to probe if it's a worthy container type
    if isinstance(obj, (str, bytes)):
        return False
    return isinstance(obj, collections.abc.Iterable) and not isinstance(
        obj, collections.abc.Mapping
    )


def _extract(obj: Any) -> Any:
    """Convert gRPC object to a nested dict."""
    try:
        desc = obj.DESCRIPTOR
    except AttributeError:
        # not a grpc/protobuf object
        return obj
    out = {}
    for field in desc.fields:
        if NamedValue._is_enum_field(field):
            out[field.name] = NamedValue.from_field(obj, field)
            continue
        v = getattr(obj, field.name)
        if isinstance(v, collections.abc.Mapping):
            v = {_extract(k): _extract(vv) for k, vv in v.items()}
        if _iscontainer(v):
            v = [_extract(entry) for entry in v]
        if hasattr(v, 'DESCRIPTOR'):
            v = _extract(v)
        out[field.name] = v
    return out


class NamedValue:
    def __init__(self, name: str, value: int) -> None:
        self.name = name
        self.value = value

    def __repr__(self) -> str:
        return f'<NamedValue>({self.name}, {self.value})'

    def __str__(self) -> str:
        return self.name

    to_simplified = __str__

    @classmethod
    def from_field(cls, obj: Any, field: Any, *, desc: Any = None) -> Self:
        # this is convoluted. the grpc/protobuf docs are clear as mud. nothing
        # is simple.  maybe there's a better way to do this, but I didn't find
        # one.
        if isinstance(field, str):
            desc = desc if desc is not None else obj.DESCRIPTOR
            field_obj = desc.fields_by_name[field]
            value = getattr(obj, field)
        else:
            field_obj = field
            value = getattr(obj, field.name)
        ename = field_obj.enum_type.values_by_number[value].name
        return cls(ename, value)

    @classmethod
    def _is_enum_field(cls, field: Any) -> bool:
        return hasattr(getattr(field, 'enum_type', None), 'values_by_number')


class ValueResult:
    """Base result class that captures gRPC/protobuf results and
    converts them to a python dict.
    Can be serialized to JSON using the to_simplified method.
    """

    values: dict

    def __init__(self, values: dict) -> None:
        self.values = values

    @classmethod
    def convert(cls, obj: Any) -> Self:
        return cls(_extract(obj))

    def to_simplified(self) -> dict:
        """Return this object in a form that can be JSON/YAML serialized."""
        return self.values

    def __repr__(self) -> str:
        return f'<{self.__class__.__name__}>({self.values!r})'


class InfoResult(ValueResult):
    """Result value for Info API."""

    pass


class StatusResult(ValueResult):
    """Result value for StatusResult API."""

    pass


class ConfigSummaryResult(ValueResult):
    """Result value for ConfigSummary API."""


class CTDBStatusResult(ValueResult):
    """Result value for CTDBStatus API."""

    pass


class GetDebugLevelResult(ValueResult):
    """Result value for GetDebugLevel API."""

    pass


class EmptyResult:
    """Base result class for APIs that do not return any data."""

    @classmethod
    def convert(cls, obj: Any) -> Self:
        return cls()

    def to_simplified(self) -> dict:
        return {}


class CloseShareResult(EmptyResult):
    """Result value for CloseShare API."""


class SetDebugLevelResult(EmptyResult):
    """Result value for SetDebugLevel API."""


class CTDBMoveIPResult(EmptyResult):
    """Result value for CTDBMoveIPResult API."""


class KillClientConnectionResult(EmptyResult):
    """Result value for KillClientConnection API."""


class ConfigDumpResult:
    """Result value for ConfigDump API.
    ConfigDump is a streaming API. Pass a file-object to .dump to stream
    converted output to a file/stdio.
    """

    @classmethod
    def convert_stream(cls, obj: Any) -> Self:
        return cls(obj)

    def __init__(self, obj: Any) -> None:
        self._stream = obj

    def dump(self, fh: typing.IO) -> None:
        for item in self._stream:
            line = getattr(item, 'line', None)
            if line:
                fh.write(line.content)
            digest = getattr(item, 'digest', None)
            if digest and digest.hash != 0:
                content = self._hash_info(digest)
                fh.write(f'\n# digest = {content}\n')

    def _hash_info(self, digest: Any) -> str:
        hash_type = NamedValue.from_field(digest, 'hash')
        hash_name = str(hash_type).lower().split("_")[-1]
        return f'{hash_name}:{digest.config_digest}'


class ConfigSharesListResult:
    """Result value for ConfigSharesList API.
    ConfigSharesList is a streaming API. This class will buffer the
    streamed results in memory so as to allow output to JSON.
    """

    @classmethod
    def convert_stream(cls, obj: Any) -> Self:
        return cls([share.name for share in obj])

    def __init__(self, shares: list[str]) -> None:
        self._shares = shares

    def to_simplified(self) -> list[str]:
        return list(self._shares)


class APICallError(RuntimeError):
    def __init__(self, code: Any, details: str, msg: str) -> None:
        self.code = code
        self.details = details
        self.msg = msg

    def __repr__(self) -> str:
        return f'Error calling gRPC API: {self.msg}'

    __str__ = __repr__


class _Endpoint:
    """Helper class for constructing a virtual API endpoint."""

    def __init__(self, method: MethodDescriptor, pool: Any) -> None:
        self._method = method
        self._pool = pool
        self._input_type = _get_message_class(pool, method.input_type)
        self._output_type = _get_message_class(pool, method.output_type)
        self._expects_client_streaming: typing.Optional[bool] = None
        self._expects_server_streaming: typing.Optional[bool] = None

    def streaming(self, client: bool, server: bool) -> Self:
        """Set streaming direction hints."""
        # Streaming APIs should be hinted using .streaming(...) method because
        # protobuf < 3.20.0 doesn't have the {client,server_streaming attrs.
        # Unfortunately 3.19.6 is what ships with RHEL10 currently and we
        # expect to deploy there.
        # The {in_stream,out_stream} properties will produce a warning IFF
        # we have set a hint and it differs from the attr when it is available
        # on newer versions.
        self._expects_client_streaming = client
        self._expects_server_streaming = server
        return self

    @property
    def input_type(self) -> Any:
        """Returns python type for input message."""
        return self._input_type

    @property
    def output_type(self) -> Any:
        """Return python type for output message."""
        return self._output_type

    @property
    def in_stream(self) -> bool:
        """Return true if input messages should be streamed."""
        chint = self._expects_client_streaming
        try:
            cstream = self._method.client_streaming
        except AttributeError:
            cstream = chint
        if chint is not None and cstream != chint:
            warnings.warn(
                f'protobuf method {self._method.name} streaming'
                ' hint differs from expected value'
            )
        return bool(cstream)

    @property
    def out_stream(self) -> bool:
        """Return true if output messages should be streamed."""
        shint = self._expects_server_streaming
        try:
            sstream = self._method.server_streaming
        except AttributeError:
            sstream = shint
        if shint is not None and sstream != shint:
            warnings.warn(
                f'protobuf method {self._method.name} streaming'
                ' hint differs from expected value'
            )
        return bool(sstream)

    def _path(self) -> str:
        return "/" + self._method.full_name.replace('.', '/')

    def call(self, channel: Any, value: Any, *, metadata: Any = None) -> Any:
        """Execute an RPC call."""
        method_map = {
            # req, resp
            (False, False): channel.unary_unary,
            (False, True): channel.unary_stream,
            (True, False): channel.stream_unary,
            (True, True): channel.stream_stream,
        }
        rpc_method = method_map[(self.in_stream, self.out_stream)]
        if isinstance(metadata, dict):
            metadata = list(metadata.items())
        return rpc_method(
            self._path(),
            request_serializer=self.input_type.SerializeToString,
            response_deserializer=self.output_type.FromString,
        )(value, metadata=metadata)


class _API:
    """Helper class for mapping API names to endpoint objects."""

    _SERVICE_NAME = "SambaControl"

    def __init__(
        self, channel: Any, dpool: Any, *, service_name: str = ''
    ) -> None:
        service_name = service_name or self._SERVICE_NAME
        self._channel = channel
        self._dpool = dpool
        self._svc = dpool.FindServiceByName(service_name)

    @property
    def channel(self) -> Any:
        return self._channel

    def __getitem__(self, name: str) -> _Endpoint:
        method = self._svc.methods_by_name[name]
        return _Endpoint(method, pool=self._dpool)


class Client:
    """SambaControl gRPC API Client."""

    def __init__(self, config: Config) -> None:
        self._config = config

    @functools.cache
    def _credentials(self) -> Any:
        ca_cert = cert = key = None
        if cl := self._config.tls_ca_cert:
            ca_cert = cl.load()
        if cl := self._config.tls_cert:
            cert = cl.load()
        if cl := self._config.tls_key:
            key = cl.load()
        return grpc.ssl_channel_credentials(
            root_certificates=ca_cert,
            private_key=key,
            certificate_chain=cert,
        )

    def _channel(self) -> Any:
        """Return the grpc channel object."""
        if self._config.channel_type is ChannelType.SECURE:
            return grpc.secure_channel(
                self._config.address, self._credentials()
            )
        return grpc.insecure_channel(self._config.address)

    @contextlib.contextmanager
    def _api(self) -> typing.Iterator[_API]:
        """As a context manager, return a virtual API helper object."""
        with self._channel() as channel:
            try:
                refdb = _reflection_ddb(channel)
                dpool = google.protobuf.descriptor_pool.DescriptorPool(refdb)
                yield _API(channel, dpool)
            except (
                gch._MultiThreadedRendezvous,
                gch._InactiveRpcError,
            ) as e:
                raise APICallError(
                    code=e.code(),
                    details=e.details(),
                    msg=e.debug_error_string(),
                ) from e

    def info(self) -> InfoResult:
        """Call the SambaControl Info API."""
        with self._api() as api:
            info_api = api['Info']
            result = info_api.call(
                api.channel,
                info_api.input_type(),
                metadata=self._config.headers,
            )
        return InfoResult.convert(result)

    def status(self) -> StatusResult:
        """Call the SambaControl Status API."""
        with self._api() as api:
            status_api = api['Status']
            result = status_api.call(
                api.channel,
                status_api.input_type(),
                metadata=self._config.headers,
            )
        return StatusResult.convert(result)

    def close_share(
        self, share_name: str, denied_users: bool
    ) -> CloseShareResult:
        """Call the SambaControl CloseShare API."""
        with self._api() as api:
            close_share_api = api['CloseShare']
            result = close_share_api.call(
                api.channel,
                close_share_api.input_type(
                    share_name=share_name, denied_users=denied_users
                ),
                metadata=self._config.headers,
            )
        return CloseShareResult.convert(result)

    def kill_client_connection(
        self, ip_address: str
    ) -> KillClientConnectionResult:
        """Call the SambaControl KillClientConnection API."""
        with self._api() as api:
            kill_client_api = api['KillClientConnection']
            result = kill_client_api.call(
                api.channel,
                kill_client_api.input_type(ip_address=ip_address),
                metadata=self._config.headers,
            )
        return KillClientConnectionResult.convert(result)

    def config_dump(
        self, source: str, hash_alg: typing.Optional[str] = None
    ) -> ConfigDumpResult:
        """Call the SambaControl ConfigDump API."""

        # closure to wrap streaming results
        def later() -> Any:
            with self._api() as api:
                config_dump_api = api["ConfigDump"].streaming(False, True)
                result = config_dump_api.call(
                    api.channel,
                    config_dump_api.input_type(
                        source=source.upper(), hash=hash_alg
                    ),
                    metadata=self._config.headers,
                )
                yield from result

        return ConfigDumpResult.convert_stream(later())

    def config_summary(
        self, source: str, hash_alg: typing.Optional[str] = None
    ) -> ConfigSummaryResult:
        """Call the SambaControl ConfigSummary API."""
        with self._api() as api:
            config_summary_api = api["ConfigSummary"]
            result = config_summary_api.call(
                api.channel,
                config_summary_api.input_type(
                    source=source.upper(),
                    hash=hash_alg,
                ),
                metadata=self._config.headers,
            )
        return ConfigSummaryResult.convert(result)

    def config_shares_list(self, source: str) -> ConfigSharesListResult:
        """Call the SambaControl ConfigSharesList API."""

        # closure to wrap streaming results
        def later() -> Any:
            with self._api() as api:
                config_shares_list_api = api["ConfigSharesList"].streaming(
                    False, True
                )
                result = config_shares_list_api.call(
                    api.channel,
                    config_shares_list_api.input_type(source=source.upper()),
                    metadata=self._config.headers,
                )
                yield from result

        return ConfigSharesListResult.convert_stream(later())

    def set_debug_level(
        self, process: str, debug_level: str
    ) -> SetDebugLevelResult:
        """Call the SambaControl SetDebugLevel API."""
        with self._api() as api:
            set_debug_level_api = api["SetDebugLevel"]
            result = set_debug_level_api.call(
                api.channel,
                set_debug_level_api.input_type(
                    process=process.upper(),
                    debug_level=debug_level,
                ),
                metadata=self._config.headers,
            )
        return SetDebugLevelResult.convert(result)

    def get_debug_level(self, process: str) -> GetDebugLevelResult:
        """Call the SambaControl GetDebugLevel API."""
        with self._api() as api:
            get_debug_level_api = api["GetDebugLevel"]
            result = get_debug_level_api.call(
                api.channel,
                get_debug_level_api.input_type(process=process.upper()),
                metadata=self._config.headers,
            )
        return GetDebugLevelResult.convert(result)

    def ctdb_status(
        self,
    ) -> CTDBStatusResult:
        """Call the SambaControl CTDBStatus API."""
        with self._api() as api:
            ctdb_status_api = api["CTDBStatus"]
            result = ctdb_status_api.call(
                api.channel,
                ctdb_status_api.input_type(),
                metadata=self._config.headers,
            )
        return CTDBStatusResult.convert(result)

    def ctdb_move_ip(self, ip_address: str, node: str) -> CTDBMoveIPResult:
        """Call the SambaControl CTDBMoveIP API."""
        with self._api() as api:
            ctdb_move_ip_api = api["CTDBMoveIP"]
            result = ctdb_move_ip_api.call(
                api.channel,
                ctdb_move_ip_api.input_type(ip=ip_address, node=node),
                metadata=self._config.headers,
            )
        return CTDBMoveIPResult.convert(result)
