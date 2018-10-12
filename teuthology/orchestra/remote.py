"""
Support for paramiko remote objects.
"""
import teuthology.lock.query
import teuthology.lock.util
from . import run
from .opsys import OS
import connection
from teuthology import misc
import time
import re
import logging
from cStringIO import StringIO
import os
import pwd
import tempfile
import netaddr

import console

log = logging.getLogger(__name__)


class Remote(object):

    """
    A connection to a remote host.

    This is a higher-level wrapper around Paramiko's `SSHClient`.
    """

    # for unit tests to hook into
    _runner = staticmethod(run.run)

    def __init__(self, name, ssh=None, shortname=None, console=None,
                 host_key=None, keep_alive=True):
        self.name = name
        if '@' in name:
            (self.user, hostname) = name.split('@')
            # Temporary workaround for 'hostname --fqdn' not working on some
            # machines
            self._hostname = hostname
        else:
            # os.getlogin() doesn't work on non-login shells. The following
            # should work on any unix system
            self.user = pwd.getpwuid(os.getuid()).pw_name
            hostname = name
        self._shortname = shortname or hostname.split('.')[0]
        self._host_key = host_key
        self.keep_alive = keep_alive
        self._console = console
        self.ssh = ssh

    def connect(self, timeout=None):
        args = dict(user_at_host=self.name, host_key=self._host_key,
                    keep_alive=self.keep_alive)
        if timeout:
            args['timeout'] = timeout

        self.ssh = connection.connect(**args)
        return self.ssh

    def reconnect(self, timeout=None):
        """
        Attempts to re-establish connection. Returns True for success; False
        for failure.
        """
        if self.ssh is not None:
            self.ssh.close()
        if not timeout:
            return self._reconnect(timeout=timeout)
        start_time = time.time()
        elapsed_time = lambda: time.time() - start_time
        while elapsed_time() < timeout:
            success = self._reconnect()
            if success:
                break
            default_sleep_val = 30
            # Don't let time_remaining be < 0
            time_remaining = max(0, timeout - elapsed_time())
            sleep_val = min(time_remaining, default_sleep_val)
            time.sleep(sleep_val)
        return success

    def _reconnect(self, timeout=None):
        try:
            self.connect(timeout=timeout)
            return self.is_online
        except Exception as e:
            log.debug(e)
            return False

    @property
    def ip_address(self):
        return self.ssh.get_transport().getpeername()[0]

    @property
    def interface(self):
        """
        The interface used by the current SSH connection
        """
        if not hasattr(self, '_interface'):
            self._set_iface_and_cidr()
        return self._interface

    @property
    def cidr(self):
        """
        The network (in CIDR notation) used by the remote's SSH connection
        """
        if not hasattr(self, '_cidr'):
            self._set_iface_and_cidr()
        return self._cidr

    def _set_iface_and_cidr(self):
        proc = self.run(
            args=['PATH=/sbin:/usr/sbin', 'ip', 'addr', 'show'],
            stdout=StringIO(),
        )
        proc.wait()
        regexp = 'inet.? %s' % self.ip_address
        proc.stdout.seek(0)
        for line in proc.stdout.readlines():
            line = line.strip()
            if re.match(regexp, line):
                items = line.split()
                self._interface = items[-1]
                self._cidr = str(netaddr.IPNetwork(items[1]).cidr)
                return
        raise RuntimeError("Could not determine interface/CIDR!")

    @property
    def hostname(self):
        if not hasattr(self, '_hostname'):
            proc = self.run(args=['hostname', '--fqdn'], stdout=StringIO())
            proc.wait()
            self._hostname = proc.stdout.getvalue().strip()
        return self._hostname

    @property
    def machine_type(self):
        if not getattr(self, '_machine_type', None):
            remote_info = teuthology.lock.query.get_status(self.hostname)
            if not remote_info:
                return None
            self._machine_type = remote_info.get("machine_type", None)
        return self._machine_type

    @property
    def shortname(self):
        if self._shortname is None:
            self._shortname = self.hostname.split('.')[0]
        return self._shortname

    @property
    def is_online(self):
        if self.ssh is None:
            return False
        try:
            self.run(args="true")
        except Exception:
            return False
        return self.ssh.get_transport().is_active()

    def ensure_online(self):
        if not self.is_online:
            return self.connect()

    @property
    def system_type(self):
        """
        System type decorator
        """
        return misc.get_system_type(self)

    def __str__(self):
        return self.name

    def __repr__(self):
        return '{classname}(name={name!r})'.format(
            classname=self.__class__.__name__,
            name=self.name,
            )

    def run(self, **kwargs):
        """
        This calls `orchestra.run.run` with our SSH client.

        TODO refactor to move run.run here?
        """
        if self.ssh is None:
            self.reconnect()
        r = self._runner(client=self.ssh, name=self.shortname, **kwargs)
        r.remote = self
        return r

    def mktemp(self):
        """
        Make a remote temporary file

        Returns: the name of the temp file created using
                 tempfile.mkstemp
        """
        py_cmd = "import os; import tempfile; import sys;" + \
            "(fd,fname) = tempfile.mkstemp();" + \
            "os.close(fd);" + \
            "sys.stdout.write(fname.rstrip());" + \
            "sys.stdout.flush()"
        args = [
            'python',
            '-c',
            py_cmd,
            ]
        proc = self.run(
            args=args,
            stdout=StringIO(),
            )
        data = proc.stdout.getvalue()
        return data

    def chmod(self, file_path, permissions):
        """
        As super-user, set permissions on the remote file specified.
        """
        args = [
            'sudo',
            'chmod',
            permissions,
            file_path,
            ]
        self.run(
            args=args,
            )

    def chcon(self, file_path, context):
        """
        Set the SELinux context of a given file.

        VMs and non-RPM-based hosts will skip this operation because ours
        currently have SELinux disabled.

        :param file_path: The path to the file
        :param context:   The SELinux context to be used
        """
        if self.os.package_type != 'rpm':
            return
        if teuthology.lock.query.is_vm(self.shortname):
            return
        self.run(args="sudo chcon {con} {path}".format(
            con=context, path=file_path))

    def _sftp_put_file(self, local_path, remote_path):
        """
        Use the paramiko.SFTPClient to put a file. Returns the remote filename.
        """
        sftp = self.ssh.open_sftp()
        sftp.put(local_path, remote_path)
        return

    def _sftp_get_file(self, remote_path, local_path):
        """
        Use the paramiko.SFTPClient to get a file. Returns the local filename.
        """
        file_size = self._format_size(
            self._sftp_get_size(remote_path)
        ).strip()
        log.debug("{}:{} is {}".format(self.shortname, remote_path, file_size))
        sftp = self.ssh.open_sftp()
        sftp.get(remote_path, local_path)
        return local_path

    def _sftp_open_file(self, remote_path):
        """
        Use the paramiko.SFTPClient to open a file. Returns a
        paramiko.SFTPFile object.
        """
        sftp = self.ssh.open_sftp()
        return sftp.open(remote_path)

    def _sftp_get_size(self, remote_path):
        """
        Via _sftp_open_file, return the filesize in bytes
        """
        with self._sftp_open_file(remote_path) as f:
            return f.stat().st_size

    @staticmethod
    def _format_size(file_size):
        """
        Given a file_size in bytes, returns a human-readable representation.
        """
        for unit in ('B', 'KB', 'MB', 'GB', 'TB'):
            if abs(file_size) < 1024.0:
                break
            file_size = file_size / 1024.0
        return "{:3.0f}{}".format(file_size, unit)

    def remove(self, path):
        self.run(args=['rm', '-fr', path])

    def put_file(self, path, dest_path, sudo=False):
        """
        Copy a local filename to a remote file
        """
        if sudo:
            raise NotImplementedError("sudo not supported")

        self._sftp_put_file(path, dest_path)
        return

    def get_file(self, path, sudo=False, dest_dir='/tmp'):
        """
        Fetch a remote file, and return its local filename.

        :param sudo:     Use sudo on the remote end to read a file that
                         requires it. Defaults to False.
        :param dest_dir: Store the file in this directory. If it is /tmp,
                         generate a unique filename; if not, use the original
                         filename.
        :returns:        The path to the local file
        """
        if not os.path.isdir(dest_dir):
            raise IOError("{dir} is not a directory".format(dir=dest_dir))

        if sudo:
            orig_path = path
            path = self.mktemp()
            args = [
                'sudo',
                'cp',
                orig_path,
                path,
                ]
            self.run(args=args)
            self.chmod(path, '0666')

        if dest_dir == '/tmp':
            # If we're storing in /tmp, generate a unique filename
            (fd, local_path) = tempfile.mkstemp(dir=dest_dir)
            os.close(fd)
        else:
            # If we are storing somewhere other than /tmp, use the original
            # filename
            local_path = os.path.join(dest_dir, path.split(os.path.sep)[-1])

        self._sftp_get_file(path, local_path)
        if sudo:
            self.remove(path)
        return local_path

    def get_tar(self, path, to_path, sudo=False):
        """
        Tar a remote directory and copy it locally
        """
        remote_temp_path = self.mktemp()
        args = []
        if sudo:
            args.append('sudo')
        args.extend([
            'tar',
            'cz',
            '-f', remote_temp_path,
            '-C', path,
            '--',
            '.',
            ])
        self.run(args=args)
        if sudo:
            self.chmod(remote_temp_path, '0666')
        self._sftp_get_file(remote_temp_path, to_path)
        self.remove(remote_temp_path)

    def get_tar_stream(self, path, sudo=False):
        """
        Tar-compress a remote directory and return the RemoteProcess
        for streaming
        """
        args = []
        if sudo:
            args.append('sudo')
        args.extend([
            'tar',
            'cz',
            '-f', '-',
            '-C', path,
            '--',
            '.',
            ])
        return self.run(args=args, wait=False, stdout=run.PIPE)

    @property
    def os(self):
        if not hasattr(self, '_os'):
            proc = self.run(args=['cat', '/etc/os-release'], stdout=StringIO(),
                            stderr=StringIO(), check_status=False)
            if proc.exitstatus == 0:
                self._os = OS.from_os_release(proc.stdout.getvalue().strip())
                return self._os

            proc = self.run(args=['lsb_release', '-a'], stdout=StringIO(),
                            stderr=StringIO())
            self._os = OS.from_lsb_release(proc.stdout.getvalue().strip())
        return self._os

    @property
    def arch(self):
        if not hasattr(self, '_arch'):
            proc = self.run(args=['uname', '-m'], stdout=StringIO())
            proc.wait()
            self._arch = proc.stdout.getvalue().strip()
        return self._arch

    @property
    def host_key(self):
        if not self._host_key:
            trans = self.ssh.get_transport()
            key = trans.get_remote_server_key()
            self._host_key = ' '.join((key.get_name(), key.get_base64()))
        return self._host_key

    @property
    def inventory_info(self):
        node = dict()
        node['name'] = self.hostname
        node['user'] = self.user
        node['arch'] = self.arch
        node['os_type'] = self.os.name
        node['os_version'] = '.'.join(self.os.version.split('.')[:2])
        node['ssh_pub_key'] = self.host_key
        node['up'] = True
        return node

    @property
    def console(self):
        if not self._console:
            self._console = getRemoteConsole(self.name)
        return self._console

    @property
    def is_vm(self):
        if not hasattr(self, '_is_vm'):
            self._is_vm = teuthology.lock.query.is_vm(self.name)
        return self._is_vm

    @property
    def init_system(self):
        """
        Which init system does the remote use?

        :returns: 'systemd' or None
        """
        if not hasattr(self, '_init_system'):
            self._init_system = None
            proc = self.run(
                args=['which', 'systemctl'],
                check_status=False,
            )
            if proc.returncode == 0:
                self._init_system = 'systemd'
        return self._init_system

    def __del__(self):
        if self.ssh is not None:
            self.ssh.close()


def getShortName(name):
    """
    Extract the name portion from remote name strings.
    """
    hn = name.split('@')[-1]
    p = re.compile('([^.]+)\.?.*')
    return p.match(hn).groups()[0]


def getRemoteConsole(name, ipmiuser=None, ipmipass=None, ipmidomain=None,
                     logfile=None, timeout=20):
    """
    Return either VirtualConsole or PhysicalConsole depending on name.
    """
    if teuthology.lock.query.is_vm(name):
        try:
            return console.VirtualConsole(name)
        except Exception:
            return None
    return console.PhysicalConsole(
        name, ipmiuser, ipmipass, ipmidomain, logfile, timeout)
