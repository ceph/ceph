from . import run
from teuthology import misc
import time

class Remote(object):
    """
    A connection to a remote host.

    This is a higher-level wrapper around Paramiko's `SSHClient`.
    """

    # for unit tests to hook into
    _runner = staticmethod(run.run)

    def __init__(self, name, ssh, shortname=None, console=None):
        self.name = name
        self._shortname = shortname
        self.ssh = ssh
        self.console = console

    @property
    def shortname(self):
        name = self._shortname
        if name is None:
            name = self.name
        return name

    @property
    def system_type(self):
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

import pexpect
import re
import logging
import libvirt
from teuthology import lockstatus as ls

log = logging.getLogger(__name__)

def getShortName(name):
    hn = name.split('@')[-1]
    p = re.compile('([^.]+)\.?.*')
    return p.match(hn).groups()[0]

class PhysicalConsole():

    def __init__(self, name, ipmiuser, ipmipass, ipmidomain, logfile=None, timeout=20):
        self.name = name
        self.shortname = getShortName(name)
        self.timeout = timeout
        self.logfile = None
        self.ipmiuser = ipmiuser
        self.ipmipass = ipmipass
        self.ipmidomain = ipmidomain

    def _exec(self, cmd):
        if not self.ipmiuser or not self.ipmipass or not self.ipmidomain:
           log.error('Must set ipmi_user, ipmi_password, and ipmi_domain in .teuthology.yaml')
        log.debug('pexpect command: ipmitool -H {s}.{dn} -I lanplus -U {ipmiuser} -P {ipmipass} {cmd}'.format(
                                   cmd=cmd,
                                   s=self.shortname,
                                   dn=self.ipmidomain,
                                   ipmiuser=self.ipmiuser,
                                   ipmipass=self.ipmipass))

        child = pexpect.spawn ('ipmitool -H {s}.{dn} -I lanplus -U {ipmiuser} -P {ipmipass} {cmd}'.format(
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
        r = child.expect(['terminated ipmitool', pexpect.TIMEOUT, pexpect.EOF], timeout=t)
        if r != 0:
            self._exec('sol deactivate')

    def _wait_for_login(self, timeout=None, attempts=6):
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
                r = child.expect(['{s} login: '.format(s=self.shortname), pexpect.TIMEOUT, pexpect.EOF], timeout=(t - (time.time() - start)))
                log.debug('expect before: {b}'.format(b=child.before))
                log.debug('expect after: {a}'.format(a=child.after))

                self._exit_session(child)
                if r == 0:
                    return
    def check_power(self, state, timeout=None):
       # check power
       total_timeout = timeout
       if not total_timeout:
           total_timeout = self.timeout
       t = 1
       total = t
       ta = time.time()
       while total < total_timeout:
            c = self._exec('power status')
            r = c.expect(['Chassis Power is {s}'.format(s=state), pexpect.EOF, pexpect.TIMEOUT], timeout=t)
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

    # returns True if console is at login prompt
    def check_status(self, timeout=None):
        try :
            # check for login prompt at console
            self._wait_for_login(timeout)
            return True
        except Exception as e:
            log.info('Failed to get ipmi console status for {s}: {e}'.format(s=self.shortname, e=e))
            return False

    def power_cycle(self):
        log.info('Power cycling {s}'.format(s=self.shortname))
        child = self._exec('power cycle')
        child.expect('Chassis Power Control: Cycle', timeout=self.timeout)
        self._wait_for_login()
        log.info('Power cycle for {s} completed'.format(s=self.shortname))

    def hard_reset(self):
        log.info('Performing hard reset of {s}'.format(s=self.shortname))
        start = time.time()
        while time.time() - start < self.timeout:
            child = self._exec('power reset')
            r = child.expect(['Chassis Power Control: Reset', pexpect.EOF], timeout=self.timeout)
            if r == 0:
                break
        self._wait_for_login()
        log.info('Hard reset for {s} completed'.format(s=self.shortname))

    def power_on(self):
        log.info('Power on {s}'.format(s=self.shortname))
        start = time.time()
        while time.time() - start < self.timeout:
            child = self._exec('power on')
            r = child.expect(['Chassis Power Control: Up/On', pexpect.EOF], timeout=self.timeout)
            if r == 0:
                break
        if not self.check_power('on'):
            log.error('Failed to power on {s}'.format(s=self.shortname))
        log.info('Power on for {s} completed'.format(s=self.shortname))

    def power_off(self):
        log.info('Power off {s}'.format(s=self.shortname))
        start = time.time()
        while time.time() - start < self.timeout:
            child = self._exec('power off')
            r = child.expect(['Chassis Power Control: Down/Off', pexpect.EOF], timeout=self.timeout)
            if r == 0:
                break
        if not self.check_power('off', 60):
            log.error('Failed to power off {s}'.format(s=self.shortname))
        log.info('Power off for {s} completed'.format(s=self.shortname))

    def power_off_for_interval(self, interval=30):
        log.info('Power off {s} for {i} seconds'.format(s=self.shortname, i=interval))
        child = self._exec('power off')
        child.expect('Chassis Power Control: Down/Off', timeout=self.timeout)

        time.sleep(interval)

        child = self._exec('power on')
        child.expect('Chassis Power Control: Up/On', timeout=self.timeout)
        self._wait_for_login()
        log.info('Power off for {i} seconds completed'.format(s=self.shortname, i=interval))

class VirtualConsole():

    def __init__(self, name, ipmiuser, ipmipass, ipmidomain, logfile=None, timeout=20):
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
        return self.vm_domain.info[0] in [libvirt.VIR_DOMAIN_RUNNING, libvirt.VIR_DOMAIN_BLOCKED,
                libvirt.VIR_DOMAIN_PAUSED]

    def check_status(self, timeout=None):
        return self.vm_domain.info()[0]  == libvirt.VIR_DOMAIN_RUNNING

    def power_cycle(self):
        self.vm_domain.info().destroy()
        self.vm_domain.info().create()

    def hard_reset(self):
        self.vm_domain.info().destroy()

    def power_on(self):
        self.vm_domain.info().create()

    def power_off(self):
        self.vm_domain.info().destroy()

    def power_off_for_interval(self, interval=30):
        log.info('Power off {s} for {i} seconds'.format(s=self.shortname, i=interval))
        self.vm_domain.info().destroy()
        time.sleep(interval)
        self.vm_domain.info().create()
        log.info('Power off for {i} seconds completed'.format(s=self.shortname, i=interval))

def getRemoteConsole(name, ipmiuser, ipmipass, ipmidomain, logfile=None, timeout=20):
    if misc.is_vm(name):
        return VirtualConsole(name, ipmiuser, ipmipass, ipmidomain, logfile, timeout)
    return PhysicalConsole(name, ipmiuser, ipmipass, ipmidomain, logfile, timeout)
