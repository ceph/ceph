import socket


class BaseSocket(object):
    schemes = {
        'unixgram': (socket.AF_UNIX, socket.SOCK_DGRAM),
        'unix': (socket.AF_UNIX, socket.SOCK_STREAM),
        'tcp': (socket.AF_INET, socket.SOCK_STREAM),
        'tcp6': (socket.AF_INET6, socket.SOCK_STREAM),
        'udp': (socket.AF_INET, socket.SOCK_DGRAM),
        'udp6': (socket.AF_INET6, socket.SOCK_DGRAM),
    }

    def __init__(self, url):
        self.url = url

        try:
            socket_family, socket_type = self.schemes[self.url.scheme]
        except KeyError:
            raise RuntimeError('Unsupported socket type: %s', self.url.scheme)

        self.sock = socket.socket(family=socket_family, type=socket_type)
        if self.sock.family == socket.AF_UNIX:
            self.address = self.url.path
        else:
            self.address = (self.url.hostname, self.url.port)

    def connect(self):
        return self.sock.connect(self.address)

    def close(self):
        self.sock.close()

    def send(self, data, flags=0):
        return self.sock.send(data.encode('utf-8') + b'\n', flags)

    def __del__(self):
        self.sock.close()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
