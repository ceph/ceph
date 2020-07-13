"""
Support for paramiko remote objects.
"""

import teuthology.lock.query
import teuthology.lock.util
from teuthology.orchestra import run
from teuthology.orchestra import connection
from teuthology.orchestra import console
from teuthology.orchestra.opsys import OS
from teuthology import misc
from teuthology.exceptions import CommandFailedError
from teuthology.misc import host_shortname
import time
import re
import logging
from io import BytesIO
import os
import pwd
import tempfile
import netaddr

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
        self._shortname = shortname or host_shortname(hostname)
        self._host_key = host_key
        self.keep_alive = keep_alive
        self._console = console
        self.ssh = ssh

    def connect(self, timeout=None, create_key=None, context='connect'):
        args = dict(user_at_host=self.name, host_key=self._host_key,
                    keep_alive=self.keep_alive, _create_key=create_key)
        if context == 'reconnect':
            # The reason for the 'context' workaround is not very
            # clear from the technical side.
            # I'll get "[Errno 98] Address already in use" altough
            # there are no open tcp(ssh) connections.
            # When connecting without keepalive, host_key and _create_key 
            # set, it will proceed.
            args = dict(user_at_host=self.name, _create_key=False, host_key=None)
        if timeout:
            args['timeout'] = timeout

        self.ssh = connection.connect(**args)
        return self.ssh

    def reconnect(self, timeout=None, socket_timeout=None, sleep_time=30):
        """
        Attempts to re-establish connection. Returns True for success; False
        for failure.
        """
        if self.ssh is not None:
            self.ssh.close()
        if not timeout:
            return self._reconnect(timeout=socket_timeout)
        start_time = time.time()
        elapsed_time = lambda: time.time() - start_time
        while elapsed_time() < timeout:
            success = self._reconnect(timeout=socket_timeout)
            if success:
                log.info('Successfully reconnected to host')
                break
            # Don't let time_remaining be < 0
            time_remaining = max(0, timeout - elapsed_time())
            sleep_val = min(time_remaining, sleep_time)
            time.sleep(sleep_val)
        return success

    def _reconnect(self, timeout=None):
        log.info("Trying to reconnect to host")
        try:
            self.connect(timeout=timeout, context='reconnect')
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
        ip_addr_show = self.sh('PATH=/sbin:/usr/sbin ip addr show')
        regexp = 'inet.? %s' % self.ip_address
        for line in ip_addr_show.split('\n'):
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
            self._hostname = self.sh('hostname --fqdn').strip()
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
            self._shortname = host_shortname(self.hostname)
        return self._shortname

    @property
    def is_online(self):
        if self.ssh is None:
            return False
        if self.ssh.get_transport() is None:
            return False
        try:
            self.run(args="true")
        except Exception:
            return False
        return self.ssh.get_transport().is_active()

    def ensure_online(self):
        if self.is_online:
            return
        self.connect()
        if not self.is_online:
            raise Exception('unable to connect')

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
        if not self.ssh or \
           not self.ssh.get_transport() or \
           not self.ssh.get_transport().is_active():
            self.reconnect()
        r = self._runner(client=self.ssh, name=self.shortname, **kwargs)
        r.remote = self
        return r

    def mkdtemp(self, suffix=None, parentdir=None):
        """
        Create a temporary directory on remote machine and return it's path.
        """
        args = ['mktemp', '-d']

        if suffix:
            args.append('--suffix=%s' % suffix)
        if parentdir:
            args.append('--tmpdir=%s' % parentdir)

        return self.sh(args).strip()

    def mktemp(self, suffix=None, parentdir=None):
        """
        Make a remote temporary file

        Returns: the path of the temp file created.
        """
        args = ['mktemp']

        if suffix:
            args.append('--suffix=%s' % suffix)
        if parentdir:
            args.append('--tmpdir=%s' % parentdir)

        return self.sh(args).strip()

    def sh(self, script, **kwargs):
        """
        Shortcut for run method.

        Usage:
            my_name = remote.sh('whoami')
            remote_date = remote.sh('date')
        """
        if 'stdout' not in kwargs:
            kwargs['stdout'] = BytesIO()
        if 'args' not in kwargs:
            kwargs['args'] = script
        proc = self.run(**kwargs)
        out = proc.stdout.getvalue()
        if isinstance(out, bytes):
            return out.decode()
        else:
            return out

    def sh_file(self, script, label="script", sudo=False, **kwargs):
        """
        Run shell script after copying its contents to a remote file

        :param script:  string with script text, or file object
        :param sudo:    run command with sudo if True,
                        run as user name if string value (defaults to False)
        :param label:   string value which will be part of file name
        Returns: stdout
        """
        ftempl = '/tmp/teuthology-remote-$(date +%Y%m%d%H%M%S)-{}-XXXX'\
                 .format(label)
        script_file = self.sh("mktemp %s" % ftempl).strip()
        self.sh("cat - | tee {script} ; chmod a+rx {script}"\
            .format(script=script_file), stdin=script)
        if sudo:
            if isinstance(sudo, str):
                command="sudo -u %s %s" % (sudo, script_file)
            else:
                command="sudo %s" % script_file
        else:
            command="%s" % script_file

        return self.sh(command, **kwargs)

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
        if self.os.package_type != 'rpm' or \
                self.os.name in ['opensuse', 'sle']:
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
            '-f', '-',
            '-C', path,
            '--',
            '.',
            run.Raw('>'), remote_temp_path
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
            try:
                os_release = self.sh('cat /etc/os-release').strip()
                self._os = OS.from_os_release(os_release)
                return self._os
            except CommandFailedError:
                pass

            lsb_release = self.sh('lsb_release -a').strip()
            self._os = OS.from_lsb_release(lsb_release)
        return self._os

    @property
    def arch(self):
        if not hasattr(self, '_arch'):
            self._arch = self.sh('uname -m').strip()
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


def getRemoteConsole(name, ipmiuser=None, ipmipass=None, ipmidomain=None,
                     logfile=None, timeout=60):
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
