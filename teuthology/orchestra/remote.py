"""
Support for paramiko remote objects.
"""
from . import run
import connection
from teuthology import misc
import time
import pexpect
import re
import logging
from cStringIO import StringIO
from teuthology import lockstatus as ls
import paramiko

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
        self._shortname = shortname
        self.host_key = host_key
        self.keep_alive = keep_alive
        self.console = console
        self.ssh = ssh or self.connect()

    def connect(self):
        self.ssh = connection.connect(user_at_host=self.name,
                                      host_key=self.host_key,
                                      keep_alive=self.keep_alive)
        return self.ssh

    def reconnect(self):
        """
        Attempts to re-establish connection. Returns True for success; False
        for failure.
        """
        self.ssh.close()
        try:
            self.ssh = self.connect()
            return self.is_online
        except Exception as e:
            log.debug(e)
            return False

    @property
    def shortname(self):
        """
        shortname decorator
        """
        name = self._shortname
        if name is None:
            name = self.name
        return name

    @property
    def hostname(self):
        return self.name.split('@')[1]

    @property
    def is_online(self):
        if self.ssh is None:
            return False
        try:
            self.run(args="echo online")
        except Exception:
            return False
        return self.ssh.get_transport().is_active()

    @property
    def system_type(self):
        """
        System type decorator
        """
        return misc.get_system_type(self)

    def __str__(self):
        return self.shortname

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
        r = self._runner(client=self.ssh, **kwargs)
        r.remote = self
        return r

    def mktemp(self):
        """
        Make a remote temporary file 
        
        Returns: the name of the temp file created using
                 tempfile.mkstemp
        """
        args = [
            'python',
            '-c',
            'import os; import tempfile; import sys; (fd,fname) = tempfile.mkstemp(); os.close(fd); sys.stdout.write(fname.rstrip()); sys.stdout.flush()'
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

    def _sftp_get_file(self, file_path):
        """
        Use the Paramiko SFTPClient to copy the data from the remote
        file.  Returns the file's content.
        """
        conn = self.connect()
        transport = conn.get_transport()
        sftp = paramiko.SFTPClient.from_transport(transport)
        with sftp.open(file_path, 'rb') as file_sftp:
            result = file_sftp.read()
        return result

    def _sftp_copy_file(self, file_path, to_path):
        sftp = self.ssh.open_sftp()
        sftp.get(file_path, to_path)

    def remove(self, path):
        self.run(args=['rm', '-fr', path]) 

    def get_file(self, path, sudo=False): 
        """
        Read a file from the remote host into memory.
        """
        if not sudo:
            return self._sftp_get_file(path)
        temp_file_path = self.mktemp()
        args = [
            'sudo',
            'cp',
            path,
            temp_file_path,
            ]
        self.run(args=args)
        self.chmod(temp_file_path, '0666')
        ret = self._sftp_get_file(temp_file_path) 
        self.remove(temp_file_path)
        return ret

    def get_tar(self, path, to_path, sudo=False, zip_flag=False):
        """
        Tar a remote file.
        """
        zip_fld = lambda x: 'cz' if x else 'c'
        temp_file_path = self.mktemp()
        args = []
        if sudo:
            args.append('sudo')
        args.extend([
            'tar',
            zip_fld(zip_flag),
            '-f', temp_file_path,
            '-C', path,
            '--',
            '.',
            ])
        self.run(args=args)
        if sudo:
            self.chmod(temp_file_path, '0666')
        self._sftp_copy_file(temp_file_path, to_path)
        self.remove(temp_file_path)


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
        status_info = ls.get_status('', self.shortname)
        try:
            phys_host = status_info['vpshost']
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
