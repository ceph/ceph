"""
Task to run tests with network delay between two remotes using tc and netem.
Reference:https://wiki.linuxfoundation.org/networking/netem.

"""

import logging
import contextlib
from cStringIO import StringIO
from paramiko import SSHException
import socket
import time
import gevent
import argparse

log = logging.getLogger(__name__)


def set_priority(interface):

    # create a priority queueing discipline
    return ['sudo', 'tc', 'qdisc', 'add', 'dev', interface, 'root', 'handle', '1:', 'prio']


def show_tc(interface):

    # shows tc device present
    return ['sudo', 'tc', 'qdisc', 'show', 'dev', interface]


def del_tc(interface):

    return ['sudo', 'tc', 'qdisc', 'del', 'dev', interface, 'root']


def cmd_prefix(interface):

    # prepare command to set delay
    cmd1 = ['sudo', 'tc', 'qdisc', 'add', 'dev', interface, 'parent',
                     '1:1', 'handle', '2:', 'netem', 'delay']

    # prepare command to change delay
    cmd2 = ['sudo', 'tc', 'qdisc', 'replace', 'dev', interface, 'root', 'netem', 'delay']

    # prepare command to apply filter to the matched ip/host

    cmd3 = ['sudo', 'tc', 'filter', 'add', 'dev', interface,
                     'parent', '1:0', 'protocol', 'ip', 'pref', '55',
                     'handle', '::55', 'u32', 'match', 'ip', 'dst']

    return cmd1, cmd2, cmd3


def static_delay(remote, host, interface, delay):

    """ Sets a constant delay between two hosts to emulate network delays using tc qdisc and netem"""

    set_delay, change_delay, set_ip = cmd_prefix(interface)

    ip = socket.gethostbyname(host.hostname)

    r = remote.run(args=show_tc(interface), stdout=StringIO())
    if r.stdout.getvalue().strip().find('refcnt') == -1:
        # call set_priority() func to create priority queue
        # if not already created(indicated by -1)
        log.info('Create priority queue')
        remote.run(args=set_priority(interface))

        # set static delay, with +/- 5ms jitter with normal distribution as default
        log.info('Setting delay to %s' % delay)
        set_delay.extend(['%s' % delay, '5ms', 'distribution', 'normal'])
        remote.run(args=set_delay)

        # set delay to a particular remote node via ip
        log.info('Delay set on %s' % remote)
        set_ip.extend(['%s' % ip, 'flowid', '2:1'])
        remote.run(args=set_ip)
    else:
        # if the device is already created, only change the delay
        log.info('Setting delay to %s' % delay)
        change_delay.extend(['%s' % delay, '5ms', 'distribution', 'normal'])
        remote.run(args=change_delay)


def variable_delay(remote, host, interface, delay_range=[]):

    """ Vary delay between two values"""

    set_delay, change_delay, set_ip = cmd_prefix(interface)

    ip = socket.gethostbyname(host.hostname)

    # delay1 has to be lower than delay2
    delay1 = delay_range[0]
    delay2 = delay_range[1]

    r = remote.run(args=show_tc(interface), stdout=StringIO())
    if r.stdout.getvalue().strip().find('refcnt') == -1:
        # call set_priority() func to create priority queue
        # if not already created(indicated by -1)
        remote.run(args=set_priority(interface))

        # set variable delay
        log.info('Setting varying delay')
        set_delay.extend(['%s' % delay1, '%s' % delay2])
        remote.run(args=set_delay)

        # set delay to a particular remote node via ip
        log.info('Delay set on %s' % remote)
        set_ip.extend(['%s' % ip, 'flowid', '2:1'])
        remote.run(args=set_ip)
    else:
        # if the device is already created, only change the delay
        log.info('Setting varying delay')
        change_delay.extend(['%s' % delay1, '%s' % delay2])
        remote.run(args=change_delay)


def delete_dev(remote, interface):

    """ Delete the qdisc if present"""

    log.info('Delete tc')
    r = remote.run(args=show_tc(interface), stdout=StringIO())
    if r.stdout.getvalue().strip().find('refcnt') != -1:
        remote.run(args=del_tc(interface))


class Toggle:

    stop_event = gevent.event.Event()

    def __init__(self, ctx, remote, host, interface, interval):
        self.ctx = ctx
        self.remote = remote
        self.host = host
        self.interval = interval
        self.interface = interface
        self.ip = socket.gethostbyname(self.host.hostname)

    def packet_drop(self):

        """ Drop packets to the remote ip specified"""

        _, _, set_ip = cmd_prefix(self.interface)

        r = self.remote.run(args=show_tc(self.interface), stdout=StringIO())
        if r.stdout.getvalue().strip().find('refcnt') == -1:
            self.remote.run(args=set_priority(self.interface))
            # packet drop to specific ip
            log.info('Drop all packets to %s' % self.host)
            set_ip.extend(['%s' % self.ip, 'action', 'drop'])
            self.remote.run(args=set_ip)

    def link_toggle(self):

        """
         For toggling packet drop and recovery in regular interval.
         If interval is 5s, link is up for 5s and link is down for 5s
        """

        while not self.stop_event.is_set():
            self.stop_event.wait(timeout=self.interval)
            # simulate link down
            try:
                self.packet_drop()
                log.info('link down')
            except SSHException:
                log.debug('Failed to run command')

            self.stop_event.wait(timeout=self.interval)
            # if qdisc exist,delete it.
            try:
                delete_dev(self.remote, self.interface)
                log.info('link up')
            except SSHException:
                log.debug('Failed to run command')

    def begin(self, gname):
        self.thread = gevent.spawn(self.link_toggle)
        self.ctx.netem.names[gname] = self.thread

    def end(self, gname):
        self.stop_event.set()
        log.info('gname is {}'.format(self.ctx.netem.names[gname]))
        self.ctx.netem.names[gname].get()

    def cleanup(self):
        """
        Invoked during unwinding if the test fails or exits before executing task 'link_recover'
        """
        log.info('Clean up')
        self.stop_event.set()
        self.thread.get()


@contextlib.contextmanager
def task(ctx, config):

    """
    - netem:
          clients: [c1.rgw.0]
          iface: eno1
          dst_client: [c2.rgw.1]
          delay: 10ms

    - netem:
          clients: [c1.rgw.0]
          iface: eno1
          dst_client: [c2.rgw.1]
          delay_range: [10ms, 20ms] # (min, max)

    - netem:
          clients: [rgw.1, mon.0]
          iface: eno1
          gname: t1
          dst_client: [c2.rgw.1]
          link_toggle_interval: 10 # no unit mentioned. By default takes seconds.

    - netem:
          clients: [rgw.1, mon.0]
          iface: eno1
          link_recover: [t1, t2]


    """

    log.info('config %s' % config)

    assert isinstance(config, dict), \
        "please list clients to run on"
    if not hasattr(ctx, 'netem'):
        ctx.netem = argparse.Namespace()
        ctx.netem.names = {}

    if config.get('dst_client') is not None:
        dst = config.get('dst_client')
        (host,) = ctx.cluster.only(dst).remotes.keys()

    for role in config.get('clients', None):
        (remote,) = ctx.cluster.only(role).remotes.keys()
        ctx.netem.remote = remote
        if config.get('delay', False):
            static_delay(remote, host, config.get('iface'), config.get('delay'))
        if config.get('delay_range', False):
            variable_delay(remote, host, config.get('iface'), config.get('delay_range'))
        if config.get('link_toggle_interval', False):
            log.info('Toggling link for %s' % config.get('link_toggle_interval'))
            global toggle
            toggle = Toggle(ctx, remote, host, config.get('iface'), config.get('link_toggle_interval'))
            toggle.begin(config.get('gname'))
        if config.get('link_recover', False):
            log.info('Recovering link')
            for gname in config.get('link_recover'):
                toggle.end(gname)
                log.info('sleeping')
                time.sleep(config.get('link_toggle_interval'))
                delete_dev(ctx.netem.remote, config.get('iface'))
                del ctx.netem.names[gname]

    try:
        yield
    finally:
        if ctx.netem.names:
            toggle.cleanup()
        for role in config.get('clients'):
            (remote,) = ctx.cluster.only(role).remotes.keys()
            delete_dev(remote, config.get('iface'))

