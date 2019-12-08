"""
Monitor netsplit testing
"""
import logging
import contextlib
import ceph_manager
import time
import gevent
import itertools
import re
from teuthology import misc as teuthology
from teuthology.orchestra import run

log = logging.getLogger(__name__)

from mon_thrash import _get_mons

def get_mons(ctx):
    return _get_mons(ctx)

def get_ip_and_ports(ctx, daemon):
    assert daemon.startswith('mon.')
    addr = ctx.ceph['ceph'].mons['{a}'.format(a=daemon)]
    ips = re.findall("[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+[:[0-9]*]*", addr)
    assert len(ips) > 0
    plain_ip = re.match("[0-9\.]*", ips[0]).group()
    assert plain_ip is not None
    port_list = []
    for ip in ips:
        ip_str, port_str = re.match("([0-9\.]*)([:[0-9]*]*)", ip2).groups()
        assert ip_str = plain_ip
        if len(port_str) > 0:
            port_list.append(port_str)
    return (plain_ip, port_list)


def disconnect(ctx, config):
    assert len(config) == 2 # we can only disconnect pairs right now
    # and we can only disconnect mons right now
    assert config[0].startswith('mon.')
    assert config[1].startswith('mon.')
    (ip1, _) = get_ip_and_ports(config[0])
    (ip2, _) = get_ip_and_ports(config[1])

    (host1,) = ctx.cluster.only(config[0]).remotes.iterkeys()
    (host2,) = ctx.cluster.only(config[1]).remotes.iterkeys()
    assert host1 is not None
    assert host2 is not None

    host1.run(
        args = ["sudo", "iptables", "-A", "INPUT", "-p", "tcp", "-s",
                ip2, "-j", "DROP"]
    )
    host2.run(
        args = ["sudo", "iptables", "-A", "INPUT", "-p", "tcp", "-s",
                ip1, "-j", "DROP"]
    )

def reconnect(ctx, config):
    assert len(config) == 2 # we can only disconnect pairs right now
    # and we can only disconnect mons right now
    assert config[0].startswith('mon.')
    assert config[1].startswith('mon.')

    (ip1, _) = get_ip_and_ports(config[0])
    (ip2, _) = get_ip_and_ports(config[1])

    (host1,) = ctx.cluster.only(config[0]).remotes.iterkeys()
    (host2,) = ctx.cluster.only(config[1]).remotes.iterkeys()
    assert host1 is not None
    assert host2 is not None

    host1.run(
        args = ["sudo", "iptables", "-D", "INPUT", "-p", "tcp", "-s",
                ip2, "-j", "DROP"]
    )
    host2.run(
        args = ["sudo", "iptables", "-D", "INPUT", "-p", "tcp", "-s",
                ip1, "-j", "DROP"]
    )

    
class MonitorNetsplit:
    """
    How it works::

    - loop through the monitors
    - for each:
    -   kill its connection to next monitor
    -   sleep for 'netsplit_period' seconds
    -   restore connection
    -   wait for quorum to be formed
    -   sleep for 'netsplit_interval' seconds
    -   repeat for the other monitors


    Right now we just use iptables to black hole the IP address of each monitor from the other.
    In future we might make it easier to blackhole monitors on shared hosts by specifying ports, but
    that will require more work to cooperatively blackhole all the OSDs associated with them when you're
    testing for datacenter splits.

    Options::

    netsplit_period     Number of seconds to blackhole the connection for (default: 10)
    netsplit_interval   number of seconds to wait before blackholing the next connection (default: 10)

    scrub               Scrub after each iteration (default: True)

    For example::

    tasks:
    - ceph:
    - mon_netsplit:
        netsplit_period: 15
        netsplit_interval: 5
    - ceph-fuse:
    - workunit:
        clients:
          all:
            - mon/workloadgen.sh
    """
    def __init__(self, ctx, manager, config, logger):
        self.ctx = ctx
        self.manager = manager
        self.manager.wait_for_clean()

        self.stopping = False
        self.logger = logger
        self.config = config

        if self.config is None:
            self.config = dict()

        """ Configs """
        self.netsplit_period = float(self.config.get('netsplit_period', 10.0))
        self.netsplit_interval = float(self.config.get('netsplit_interval', 10.0))
        self.scrub = self.config.get('scrub', True)

        self.thread = gevent.spawn(self.do_netsplit)

    def log(self, x):
        """
        locally log info messages
        """
        self.logger.info(x)

    def do_join(self):
        """
        Break out of this processes thrashing loop.
        """
        self.stopping = True
        self.thread.get()

    def do_netsplit(self):
        """
        Cotinuously loop and netsplit the monitors.
        """
        self.log('start netsplitting')
        self.log('netsplit period: {r}, netsplit interval: {t} '.format(
                r=self.netsplit_period,t=self.netsplit_interval))

        mons = get_mons(self.ctx)
        while not self.stopping:
            for (mon_a_id, mon_b_id) in itertools.combinations(mons, 2):
                if self.stopping:
                    break

                mon_a = 'mon.{id}'.format(id=mon_a_id)
                mon_b = 'mon.{id}'.format(id=mon_b_id)
                
                self.manager.wait_for_mon_quorum_size(len(mons))
                self.log('making sure all monitors are in the quorum')
                for m in mons:
                    s = self.manager.get_mon_status(m)
                    assert s['state'] == 'leader' or s['state'] == 'peon'
                    assert len(s['quorum']) == len(mons)

                #netsplit here
                self.log('mons list is {mons}, mon_a is {mon_a}, mon_b is {mon_b}'.format(mons=mons, mon_a=mon_a, mon_b=mon_b))
                disconnect(self.ctx, [mon_a, mon_b])


                self.log('waiting for {delay} secs before restoring monitor connections'.format(
                    delay=self.netsplit_period))
                time.sleep(self.netsplit_period)

                #undo netsplit here
                reconnect(self.ctx, [mon_a, mon_b])
                self.manager.wait_for_mon_quorum_size(len(mons))
                for m in mons:
                    s = self.manager.get_mon_status(m)
                    assert s['state'] == 'leader' or s['state'] == 'peon'
                    assert len(s['quorum']) == len(mons)

                if self.scrub:
                    self.log('triggering scrub')
                    try:
                        self.manager.raw_cluster_cmd('scrub')
                    except Exception:
                        log.exception("Saw exception while triggering scrub")

                if self.netsplit_interval > 0.0:
                    self.log('waiting for {delay} secs before continuing thrashing'.format(
                        delay=self.netsplit_interval))
                    time.sleep(self.netsplit_interval)

@contextlib.contextmanager
def task(ctx, config):
    """
    Stress the monitor quorum by netsplitting them while another task/workunit
    is running.

    Please refer to MonitorNetsplit class for further information on the
    available options.
    """
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'mon_netsplit task only accepts a dict for configuration'
    assert len(get_mons(ctx)) > 2, \
        'mon_netsplit task requires at least 3 monitors'
    log.info('Beginning mon_netsplit...')
    first_mon = teuthology.get_first_mon(ctx, config)
    (mon,) = ctx.cluster.only(first_mon).remotes.iterkeys()
    manager = ceph_manager.CephManager(
        mon,
        ctx=ctx,
        logger=log.getChild('ceph_manager'),
        )
    netsplit_proc = MonitorNetsplit(ctx,
        manager, config,
        logger=log.getChild('mon_thrasher'))
    try:
        log.debug('Yielding')
        yield
    finally:
        log.info('joining mon_netsplit')
        netsplit_proc.do_join()
        mons = get_mons(ctx)
        manager.wait_for_mon_quorum_size(len(mons))
