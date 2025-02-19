import socket
from urllib.parse import ParseResult
from typing import Any, Dict, Optional, Tuple, Union


class BaseSocket(object):
    schemes = {
        'unixgram': (socket.AF_UNIX, socket.SOCK_DGRAM),
        'unix': (socket.AF_UNIX, socket.SOCK_STREAM),
        'tcp': (socket.AF_INET, socket.SOCK_STREAM),
        'tcp6': (socket.AF_INET6, socket.SOCK_STREAM),
        'udp': (socket.AF_INET, socket.SOCK_DGRAM),
        'udp6': (socket.AF_INET6, socket.SOCK_DGRAM),
    }

    def __init__(self, url: ParseResult) -> None:
        self.url = url

        try:
            socket_family, socket_type = self.schemes[self.url.scheme]
        except KeyError:
            raise RuntimeError('Unsupported socket type: %s', self.url.scheme)

        self.sock = socket.socket(family=socket_family, type=socket_type)
        if self.sock.family == socket.AF_UNIX:
            self.address: Union[str, Tuple[str, int]] = self.url.path
        else:
            assert self.url.hostname
            assert self.url.port
            self.address = (self.url.hostname, self.url.port)

    def connect(self) -> None:
        return self.sock.connect(self.address)

    def close(self) -> None:
        self.sock.close()

    def send(self, data: str, flags: int = 0) -> int:
        return self.sock.send(data.encode('utf-8') + b'\n', flags)

    def __enter__(self) -> 'BaseSocket':
        self.connect()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
