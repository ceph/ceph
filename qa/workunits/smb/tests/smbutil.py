import base64
import contextlib
import pathlib

import smbclient
from smbprotocol.header import NtStatus


class SMBTestServer:
    """Server configuration wrapper."""

    def __init__(self, data):
        self._server_data = data

    @property
    def ip_address(self):
        return self._server_data.get('ip_address', '')

    @property
    def name(self):
        return self._server_data.get('name', '')

    @property
    def port(self):
        return 445


class SMBTestConf:
    """Global test configuration wrapper."""

    def __init__(self, data):
        self._data = data

    @property
    def shares(self):
        return self._data['smb_shares']

    @property
    def username(self):
        users = self._data.get('smb_users', [])
        if users and users[0] and (un := users[0].get('username')):
            return un
        return r'domain1\bwayne'

    @property
    def password(self):
        users = self._data.get('smb_users', [])
        if users and users[0] and (pw := users[0].get('password')):
            return pw
        return base64.b64decode(b'MTExNVJvc2Uu').decode()

    @property
    def server(self):
        nodes = self._data.get('smb_nodes', [])
        return SMBTestServer(nodes[0])


@contextlib.contextmanager
def connection(conf, share):
    """Return a PathWrapper connecting to the given share."""
    server = conf.server.ip_address
    port = conf.server.port
    username = conf.username
    password = conf.password

    smbclient.register_session(
        server=server,
        port=port,
        username=username,
        password=password,
    )
    try:
        spath = pathlib.PureWindowsPath(f'//{server}/{share}')
        yield PathWrapper(spath)
    finally:
        smbclient.delete_session(server, port)


class PathWrapper:
    """Object that wraps the share connection and path within the share to act
    similarly to a pathlib.Path.
    """

    def __init__(self, share_path):
        self.share_path = share_path

    def __truediv__(self, other):
        return self.__class__(self.share_path / other)

    def listdir(self, **kwargs):
        """List directory contents."""
        return smbclient.listdir(str(self.share_path), **kwargs)

    def mkdir(self, exist_ok=False):
        """Create a new directory."""
        # TODO: parents=False
        if exist_ok:
            try:
                return smbclient.mkdir(str(self.share_path))
            except OSError as err:
                code = getattr(err, 'ntstatus', None)
                if code == NtStatus.STATUS_OBJECT_NAME_COLLISION:
                    return
                raise
        return smbclient.mkdir(str(self.share_path))

    def rmdir(self):
        """Remove a directory."""
        return smbclient.rmdir(str(self.share_path))

    def open(self, mode='r'):
        """Open a file."""
        return smbclient.open_file(str(self.share_path), mode=mode)

    def read_text(self):
        """Open the file in text mode, read it, and close the file."""
        with self.open() as fh:
            return fh.read()

    def write_text(self, txt):
        """Open the file in text mode, write to it, and close the file."""
        with self.open(mode='w') as fh:
            fh.write(txt)

    def unlink(self):
        """Unlink (remove) a file."""
        smbclient.remove(str(self.share_path))
