import base64
import contextlib
import pathlib
import time

import cephutil

import smbclient
from smbprotocol.header import NtStatus


class SMBTestHost:
    """Host configuration wrapper."""

    def __init__(self, data):
        self._server_data = data

    @property
    def ip_address(self):
        return self._server_data.get('ip_address', '')

    @property
    def name(self):
        return self._server_data.get('name', '')


class SMBTestServer(SMBTestHost):
    """Server configuration wrapper."""

    @property
    def port(self):
        return 445

    @property
    def ssh_user(self):
        return self._server_data.get('user', '')


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

    @property
    def admin_node(self):
        return SMBTestServer(self._data.get('admin_node', {}))

    @property
    def ssh_user(self):
        uname = self.admin_node.ssh_user
        assert uname, 'no ssh_user found'
        return uname

    @property
    def ssh_admin_host(self):
        return self.admin_node.ip_address

    def clients(self):
        clients = self._data.get('client_nodes') or []
        return [SMBTestHost(node_info) for node_info in clients]

    @property
    def default_client(self):
        # ideally we check that this is *our* ip or name, but we'll just wing
        # it for now until we really need to check
        return self.clients()[0]


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

    def write_bytes(self, data):
        """Open the file in binary mode, write bytes to it, and close the file."""
        with self.open(mode='wb') as fh:
            fh.write(data)

    def unlink(self):
        """Unlink (remove) a file."""
        smbclient.remove(str(self.share_path))


def get_shares(smb_cfg):
    """Get all SMB shares."""
    jres = cephutil.cephadm_shell_cmd(
        smb_cfg,
        ["ceph", "smb", "show", "ceph.smb.share"],
        load_json=True,
    )
    assert jres.obj
    resources = jres.obj['resources']
    assert len(resources) > 0
    assert all(r['resource_type'] == 'ceph.smb.share' for r in resources)
    return resources


def get_share_by_id(smb_cfg, cluster_id, share_id):
    """Get a specific share by cluster_id and share_id."""
    shares = get_shares(smb_cfg)
    for share in shares:
        if share['cluster_id'] == cluster_id and share['share_id'] == share_id:
            return share
    return None


def apply_share_config(smb_cfg, share):
    """Apply share configuration via the apply command."""
    jres = cephutil.cephadm_shell_cmd(
        smb_cfg,
        ['ceph', 'smb', 'apply', '-i-'],
        input_json={'resources': [share]},
        load_json=True,
    )
    assert jres.returncode == 0
    assert jres.obj and jres.obj.get('success')
    assert 'results' in jres.obj
    _results = jres.obj['results']
    assert len(_results) == 1, "more than one result found"
    _result = _results[0]
    assert 'resource' in _result
    resources_ret = _result['resource']
    assert resources_ret['resource_type'] == 'ceph.smb.share'
    # sleep to ensure the settings got applied in smbd
    # TODO: make this more dynamic somehow
    time.sleep(60)
    return resources_ret
