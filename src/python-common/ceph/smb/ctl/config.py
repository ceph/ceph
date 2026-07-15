"""Ceph SMB client config library"""

import typing

import abc
import dataclasses
import enum
import pathlib

from ._typing import Self


class ChannelType(str, enum.Enum):
    SECURE = 'secure'
    INSECURE = 'insecure'


class TLSLoader(abc.ABC):
    def load(self) -> bytes:
        raise NotImplementedError()


@dataclasses.dataclass
class TLSInline(TLSLoader):
    """Configure TLS with inline cert data. Mainly for testing."""

    data: typing.Union[str, bytes]

    def load(self) -> bytes:
        if isinstance(self.data, str):
            return self.data.encode()
        return self.data


@dataclasses.dataclass
class TLSPath(TLSLoader):
    """Configure TLS using a path to cert data."""

    path: pathlib.Path

    @classmethod
    def create(cls, path: typing.Union[str, pathlib.Path]) -> Self:
        return cls(pathlib.Path(path))

    def load(self) -> bytes:
        with open(self.path, 'rb') as fh:
            data = fh.read()
        return data


@dataclasses.dataclass
class Config:
    address: str
    channel_type: ChannelType
    tls_cert: typing.Optional[TLSLoader] = None
    tls_key: typing.Optional[TLSLoader] = None
    tls_ca_cert: typing.Optional[TLSLoader] = None
    headers: typing.Optional[typing.Dict[str, str]] = None
