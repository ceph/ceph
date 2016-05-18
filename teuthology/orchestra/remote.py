"""
Support for paramiko remote objects.
"""
from . import run
from .opsys import OS
import connection
from teuthology import misc
import time
import pexpect
import re
import logging
from cStringIO import StringIO
from teuthology import lockstatus as ls
import os
import pwd
import tempfile
import netaddr

try:
    import libvirt
except ImportError:
    libvirt = None

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
        self.console = console
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
            remote_info = ls.get_status(self.hostname)
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
        if misc.is_vm(self.shortname):
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

    @property
    def os(self):
        if not hasattr(self, '_os'):
            proc = self.run(
                args=[
                    'python', '-c',
                    'import platform; print platform.linux_distribution()'],
                stdout=StringIO(), stderr=StringIO(), check_status=False)
            if proc.exitstatus == 0:
                self._os = OS.from_python(proc.stdout.getvalue().strip())
                return self._os

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


class PhysicalConsole():
    """
    Physical Console (set from getRemoteConsole)
    """
    def __init__(self, name, ipmiuser, ipmipass, ipmidomain, logfile=None,
                 timeout=20):
        self.name = name
        self.shortname = getShortName(name)
        self.timeout = timeout
        self.logfile = None
        self.ipmiuser = ipmiuser
        self.ipmipass = ipmipass
        self.ipmidomain = ipmidomain

    def _exec(self, cmd):
        """
        Run the cmd specified using ipmitool.
        """
        if not self.ipmiuser or not self.ipmipass or not self.ipmidomain:
            log.error('Must set ipmi_user, ipmi_password, and ipmi_domain in .teuthology.yaml')  # noqa
        log.debug('pexpect command: ipmitool -H {s}.{dn} -I lanplus -U {ipmiuser} -P {ipmipass} {cmd}'.format(  # noqa
                  cmd=cmd,
                  s=self.shortname,
                  dn=self.ipmidomain,
                  ipmiuser=self.ipmiuser,
                  ipmipass=self.ipmipass))

        child = pexpect.spawn('ipmitool -H {s}.{dn} -I lanplus -U {ipmiuser} -P {ipmipass} {cmd}'.format(  # noqa
                              cmd=cmd,
                              s=self.shortname,
                              dn=self.ipmidomain,
                              ipmiuser=self.ipmiuser,
                              ipmipass=self.ipmipass))
        if self.logfile:
            child.logfile = self.logfile
        return child

    def _exit_session(self, child, timeout=None):
        child.send('~.')
        t = timeout
        if not t:
            t = self.timeout
        r = child.expect(
            ['terminated ipmitool', pexpect.TIMEOUT, pexpect.EOF], timeout=t)
        if r != 0:
            self._exec('sol deactivate')

    def _wait_for_login(self, timeout=None, attempts=6):
        """
        Wait for login.  Retry if timeouts occur on commands.
        """
        log.debug('Waiting for login prompt on {s}'.format(s=self.shortname))
        # wait for login prompt to indicate boot completed
        t = timeout
        if not t:
            t = self.timeout
        for i in range(0, attempts):
            start = time.time()
            while time.time() - start < t:
                child = self._exec('sol activate')
                child.send('\n')
                log.debug('expect: {s} login'.format(s=self.shortname))
                r = child.expect(
                    ['{s} login: '.format(s=self.shortname),
                     pexpect.TIMEOUT,
                     pexpect.EOF],
                    timeout=(t - (time.time() - start)))
                log.debug('expect before: {b}'.format(b=child.before))
                log.debug('expect after: {a}'.format(a=child.after))

                self._exit_session(child)
                if r == 0:
                    return

    def check_power(self, state, timeout=None):
        """
        Check power.  Retry if EOF encountered on power check read.
        """
        total_timeout = timeout
        if not total_timeout:
            total_timeout = self.timeout
        t = 1
        total = t
        ta = time.time()
        while total < total_timeout:
            c = self._exec('power status')
            r = c.expect(['Chassis Power is {s}'.format(
                s=state), pexpect.EOF, pexpect.TIMEOUT], timeout=t)
            tb = time.time()
            if r == 0:
                return True
            elif r == 1:
                # keep trying if EOF is reached, first sleep for remaining
                # timeout interval
                if tb - ta < t:
                    time.sleep(t - (tb - ta))
            # go around again if EOF or TIMEOUT
            ta = tb
            t *= 2
            total += t
        return False

    def check_status(self, timeout=None):
        """
        Check status.  Returns True if console is at login prompt
        """
        try:
            # check for login prompt at console
            self._wait_for_login(timeout)
            return True
        except Exception as e:
            log.info('Failed to get ipmi console status for {s}: {e}'.format(
                s=self.shortname, e=e))
            return False

    def power_cycle(self):
        """
        Power cycle and wait for login.
        """
        log.info('Power cycling {s}'.format(s=self.shortname))
        child = self._exec('power cycle')
        child.expect('Chassis Power Control: Cycle', timeout=self.timeout)
        self._wait_for_login()
        log.info('Power cycle for {s} completed'.format(s=self.shortname))

    def hard_reset(self):
        """
        Perform physical hard reset.  Retry if EOF returned from read
        and wait for login when complete.
        """
        log.info('Performing hard reset of {s}'.format(s=self.shortname))
        start = time.time()
        while time.time() - start < self.timeout:
            child = self._exec('power reset')
            r = child.expect(['Chassis Power Control: Reset', pexpect.EOF],
                             timeout=self.timeout)
            if r == 0:
                break
        self._wait_for_login()
        log.info('Hard reset for {s} completed'.format(s=self.shortname))

    def power_on(self):
        """
        Physical power on.  Loop checking cmd return.
        """
        log.info('Power on {s}'.format(s=self.shortname))
        start = time.time()
        while time.time() - start < self.timeout:
            child = self._exec('power on')
            r = child.expect(['Chassis Power Control: Up/On', pexpect.EOF],
                             timeout=self.timeout)
            if r == 0:
                break
        if not self.check_power('on'):
            log.error('Failed to power on {s}'.format(s=self.shortname))
        log.info('Power on for {s} completed'.format(s=self.shortname))

    def power_off(self):
        """
        Physical power off.  Loop checking cmd return.
        """
        log.info('Power off {s}'.format(s=self.shortname))
        start = time.time()
        while time.time() - start < self.timeout:
            child = self._exec('power off')
            r = child.expect(['Chassis Power Control: Down/Off', pexpect.EOF],
                             timeout=self.timeout)
            if r == 0:
                break
        if not self.check_power('off', 60):
            log.error('Failed to power off {s}'.format(s=self.shortname))
        log.info('Power off for {s} completed'.format(s=self.shortname))

    def power_off_for_interval(self, interval=30):
        """
        Physical power off for an interval. Wait for login when complete.

        :param interval: Length of power-off period.
        """
        log.info('Power off {s} for {i} seconds'.format(
            s=self.shortname, i=interval))
        child = self._exec('power off')
        child.expect('Chassis Power Control: Down/Off', timeout=self.timeout)

        time.sleep(interval)

        child = self._exec('power on')
        child.expect('Chassis Power Control: Up/On', timeout=self.timeout)
        self._wait_for_login()
        log.info('Power off for {i} seconds completed'.format(
            s=self.shortname, i=interval))


class VirtualConsole():
    """
    Virtual Console (set from getRemoteConsole)
    """
    def __init__(self, name, ipmiuser, ipmipass, ipmidomain, logfile=None,
                 timeout=20):
        if libvirt is None:
            raise RuntimeError("libvirt not found")

        self.shortname = getShortName(name)
        status_info = ls.get_status(self.shortname)
        try:
            if status_info.get('is_vm', False):
                phys_host = status_info['vm_host']['name'].split('.')[0]
        except TypeError:
            return
        self.connection = libvirt.open(phys_host)
        for i in self.connection.listDomainsID():
            d = self.connection.lookupByID(i)
            if d.name() == self.shortname:
                self.vm_domain = d
                break
        return

    def check_power(self, state, timeout=None):
        """
        Return true if vm domain state indicates power is on.
        """
        return self.vm_domain.info[0] in [libvirt.VIR_DOMAIN_RUNNING,
                                          libvirt.VIR_DOMAIN_BLOCKED,
                                          libvirt.VIR_DOMAIN_PAUSED]

    def check_status(self, timeout=None):
        """
        Return true if running.
        """
        return self.vm_domain.info()[0] == libvirt.VIR_DOMAIN_RUNNING

    def power_cycle(self):
        """
        Simiulate virtual machine power cycle
        """
        self.vm_domain.info().destroy()
        self.vm_domain.info().create()

    def hard_reset(self):
        """
        Simiulate hard reset
        """
        self.vm_domain.info().destroy()

    def power_on(self):
        """
        Simiulate power on
        """
        self.vm_domain.info().create()

    def power_off(self):
        """
        Simiulate power off
        """
        self.vm_domain.info().destroy()

    def power_off_for_interval(self, interval=30):
        """
        Simiulate power off for an interval.
        """
        log.info('Power off {s} for {i} seconds'.format(
            s=self.shortname, i=interval))
        self.vm_domain.info().destroy()
        time.sleep(interval)
        self.vm_domain.info().create()
        log.info('Power off for {i} seconds completed'.format(
            s=self.shortname, i=interval))


def getRemoteConsole(name, ipmiuser, ipmipass, ipmidomain, logfile=None,
                     timeout=20):
    """
    Return either VirtualConsole or PhysicalConsole depending on name.
    """
    if misc.is_vm(name):
        return VirtualConsole(name, ipmiuser, ipmipass, ipmidomain, logfile,
                              timeout)
    return PhysicalConsole(name, ipmiuser, ipmipass, ipmidomain, logfile,
                           timeout)
