"""
Task to power on/off or powercycle nodes
"""
import logging
import contextlib
from teuthology import misc as teuthology
from cStringIO import StringIO
from teuthology.orchestra import run
from teuthology.contextutil import safe_while
from teuthology.orchestra import remote as orchestra_remote
from teuthology import contextutil
import time

log = logging.getLogger(__name__)


class IpmiCapabilities:

    def __init__(self, remote, ipmi_user, ipmi_password, ipmi_domain, timeout):

        self.ipmi_user = ipmi_user
        self.ipmi_password = ipmi_password
        self.ipmi_domain = ipmi_domain
        self.timeout = timeout

        self.conn = orchestra_remote.getRemoteConsole(remote.hostname,
                                                 self.ipmi_user,
                                                 self.ipmi_password,
                                                 self.ipmi_domain,
                                                 self.timeout)

    def power_off(self):

        self.conn.power_off()

    def power_on(self):

        self.conn.power_on()

    def power_cycle(self):

        self.conn.power_cycle(timeout=400)

    def check_status(self):

        self.conn.check_status()


@contextlib.contextmanager
def poweroff(ctx, config):

    """
    tasks:
        ceph-ipmi.poweroff: [osd.0]

    """

    assert isinstance(config, dict) or isinstance(config, list), \
        "task ceph_ipmi only supports a list or dictionary for configuration"

    if config is None:
        config = {}
    elif isinstance(config, list):
        config = dict((role, None) for role in config)
    roles = config.keys()
    last_remote = []

    for role in roles:
        (remote,) = ctx.cluster.only(role).remotes.iterkeys()
        cluster_name, _, _ = teuthology.split_role(role)
        if remote not in last_remote:
            log.info("Powering off host containing %s" % role)
            ipmi = IpmiCapabilities(remote,
                     ctx.teuthology_config.get('ipmi_user', None),
                     ctx.teuthology_config.get('ipmi_password', None),
                     ctx.teuthology_config.get('ipmi_domain', None), timeout=20)

            ipmi.power_off()
            last_remote.append(remote)

    yield


@contextlib.contextmanager
def poweron(ctx, config):

    """
    tasks:
        ceph-ipmi.poweron: [osd.0]
        check_status: false

    """

    assert isinstance(config, dict) or isinstance(config, list), \
        "task ceph_ipmi only supports a list or dictionary for configuration"

    if config is None:
        config = {}
    elif isinstance(config, list):
        config = dict((role, None) for role in config)
    roles = config.keys()
    last_remote = []

    for role in roles:
        (remote,) = ctx.cluster.only(role).remotes.iterkeys()
        cluster_name, _, _ = teuthology.split_role(role)
        if remote not in last_remote:
            log.info("Powering on host containing %s" % role)
            ipmi = IpmiCapabilities(remote,
                     ctx.teuthology_config.get('ipmi_user', None),
                     ctx.teuthology_config.get('ipmi_password', None),
                     ctx.teuthology_config.get('ipmi_domain', None), timeout=180)
            ipmi.power_on()
            last_remote.append(remote)

            if config.get('check_status', True):
                ipmi.check_status()

            teuthology.reconnect(ctx, 360)

    yield


@contextlib.contextmanager
def task(ctx, config):

    """
     tasks:
         ceph_ipmi: [osd.0]

     """

    assert isinstance(config, dict) or isinstance(config, list), \
        "task ceph_ipmi only supports a list or dictionary for configuration"

    if config is None:
        config = {}
    elif isinstance(config, list):
        config = dict((role, None) for role in config)

    with contextutil.nested(
        lambda: poweroff(ctx, config),
        lambda: poweron(ctx, config),
    ):
     yield

