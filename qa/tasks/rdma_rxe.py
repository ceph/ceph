"""
Soft-RoCE: an rdma_rxe link on the public interface of each host, so that
the libfabric verbs provider works without RDMA hardware. With
``hardware: true``, the RDMA NIC of that interface instead, such as a
ConnectX RoCE port.
"""
import contextlib
import json
import logging
import time

from io import StringIO

from teuthology import misc as teuthology
from teuthology.exceptions import ConfigError

log = logging.getLogger(__name__)

# the package that holds the rdma_rxe module of a kernel, by package type
MODULE_PACKAGES = {
    'rpm': 'kernel-modules-extra-{kernel}',
    'deb': 'linux-modules-extra-{kernel}',
}


def link_of_netdev(output, netdev):
    """
    The RDMA device whose link is on netdev, from the output of
    'rdma -j link show', or None
    """
    for link in json.loads(output or '[]'):
        if link.get('netdev') == netdev:
            return link.get('ifname')
    return None


def port_active(output):
    """ Whether 'ibv_devinfo -d <device>' shows an active port """
    return 'PORT_ACTIVE' in output


def fi_info_has_domain(output, device):
    """ Whether 'fi_info -p verbs' lists device as a domain """
    for line in output.splitlines():
        key, _, value = line.strip().partition(':')
        if key.strip() == 'domain' and value.strip() == device:
            return True
    return False


def module_package(package_type, kernel):
    """
    The package that holds the rdma_rxe module of kernel, or None. The
    plain package name would install the package of the newest kernel in
    the repositories, which need not be the kernel that runs.
    """
    template = MODULE_PACKAGES.get(package_type)
    return template.format(kernel=kernel) if template else None


def module_loaded(remote):
    """ Whether rdma_rxe is loaded on remote, or built into its kernel """
    return 'rdma_rxe' in remote.sh(['ls', '/sys/module']).split()


def load_module(remote):
    """
    Load rdma_rxe on remote. When the running kernel has no such module,
    install the package that holds it for that kernel first.
    """
    # sudo for its secure_path: modinfo is in /usr/sbin or /sbin
    found = remote.run(args=['sudo', 'modinfo', '-n', 'rdma_rxe'],
                       check_status=False, stdout=StringIO(),
                       stderr=StringIO())
    if found.exitstatus != 0:
        kernel = remote.sh(['uname', '-r']).strip()
        package = module_package(remote.os.package_type, kernel)
        if package is None:
            raise RuntimeError('%s: kernel %s has no rdma_rxe module' %
                               (remote.shortname, kernel))
        log.info('%s: installing %s for rdma_rxe', remote.shortname, package)
        if remote.os.package_type == 'deb':
            remote.run(args=['sudo', 'env', 'DEBIAN_FRONTEND=noninteractive',
                             'apt-get', 'install', '-y', package])
        else:
            remote.run(args=['sudo', 'dnf', 'install', '-y', package])
    remote.run(args=['sudo', 'modprobe', 'rdma_rxe'])


def add_link(remote, name, iface):
    """
    Add an rxe link called name on iface of remote, unless iface has an
    RDMA link already. Returns the device, and whether this task added it.
    """
    # sudo for its secure_path: rdma is in /usr/sbin or /sbin
    existing = link_of_netdev(
        remote.sh(['sudo', 'rdma', '-j', 'link', 'show']), iface)
    added = False
    if existing:
        log.info('%s: %s already has the RDMA device %s',
                 remote.shortname, iface, existing)
        device = existing
    else:
        remote.run(args=['sudo', 'rdma', 'link', 'add', name,
                         'type', 'rxe', 'netdev', iface])
        device = name
        added = True
    return device, added


def hardware_link(remote, iface):
    """
    The RDMA device that the NIC of iface has, for hardware: true
    """
    links = remote.sh(['sudo', 'rdma', '-j', 'link', 'show'])
    device = link_of_netdev(links, iface)
    if device is None:
        raise RuntimeError(
            '%s: %s has no RDMA device, and hardware: true does not add '
            'soft-RoCE: run the job on machines with an RDMA NIC on the '
            'interface of their address' % (remote.shortname, iface))
    log.info('%s: using %s, the RDMA device of %s', remote.shortname, device,
             iface)
    return device


def check_link(remote, device, iface, timeout):
    """
    Wait until the port of device is active, and check that the libfabric
    verbs provider lists device
    """
    deadline = time.time() + timeout
    while True:
        info = remote.sh(['ibv_devinfo', '-d', device], check_status=False)
        if port_active(info):
            break
        if time.time() > deadline:
            raise RuntimeError('%s: the port of %s is not active:\n%s' %
                               (remote.shortname, device, info))
        time.sleep(1)
    # the ofi_rxm provider sizes its completion queues from the universe
    # size, and fails to open on soft-RoCE without a small one
    fi = remote.sh(['env', 'FI_UNIVERSE_SIZE=16', 'fi_info', '-p', 'verbs'],
                   check_status=False)
    if not fi_info_has_domain(fi, device):
        raise RuntimeError('%s: fi_info -p verbs does not list %s:\n%s' %
                           (remote.shortname, device, fi))
    log.info('%s: %s on %s works with the verbs provider',
             remote.shortname, device, iface)


@contextlib.contextmanager
def task(ctx, config):
    """
    Load rdma_rxe on each host and add an rxe link on the interface that
    carries the host's address, unless that interface has an RDMA link
    already. Check that the port is active (ibv_devinfo) and that the
    libfabric verbs provider lists the device (fi_info -p verbs). At the
    end, delete the links that the task added, also when a check failed,
    and unload rdma_rxe where the task loaded it.

    The hosts need rdma-core, the ibverbs utilities (ibv_devinfo),
    libfabric's fi_info and iproute2's rdma tool. When the running kernel
    has no rdma_rxe module, the task installs the package that holds it
    for that kernel: kernel-modules-extra-$(uname -r) on CentOS and Rocky,
    linux-modules-extra-$(uname -r) on Ubuntu.

        tasks:
        - install:
        - rdma_rxe:
            # the roles whose hosts get a link (default: every host)
            roles: [osd.0, client.0]
            # the name of each link (default rxe0)
            name: rxe0
            # the interface of each link (default: the one that carries
            # the address teuthology reaches the host on)
            iface: eth0
            # how long a port may take to become active (default 30 s)
            timeout: 30
            # use the RDMA NIC that each interface has, and fail on a host
            # whose interface has none, rather than add soft-RoCE
            # (default false)
            hardware: false
        - ceph:

    With ``hardware: true`` the task loads no module and adds no link:
    each interface must have an RDMA link of its own already, whose port
    is active and which the verbs provider lists. A host without one
    fails the task, so that a job on the wrong machines does not fall
    back to soft-RoCE without saying so.

    The device, the port and the GID of a daemon's address are what an
    OSD with a verbs transport looks for in oob_network, or in its public
    address when oob_network is empty. ctx.rdma_rxe maps each host's name
    to its device.
    """
    if config is None:
        config = {}
    if not isinstance(config, dict):
        raise ConfigError('rdma_rxe takes a dict')
    overrides = ctx.config.get('overrides', {})
    teuthology.deep_merge(config, overrides.get('rdma_rxe', {}))
    name = config.get('name', 'rxe0')
    timeout = config.get('timeout', 30)
    hardware = bool(config.get('hardware', False))
    roles = config.get('roles')
    if roles:
        remotes = set()
        for role in roles:
            (remote,) = ctx.cluster.only(role).remotes.keys()
            remotes.add(remote)
    else:
        remotes = set(ctx.cluster.remotes.keys())

    ctx.rdma_rxe = {}
    added = []
    loaded = []
    try:
        for remote in sorted(remotes, key=lambda r: r.name):
            # the interface of the address teuthology reaches the host on
            iface = config.get('iface') or remote.interface
            if hardware:
                device = hardware_link(remote, iface)
                ctx.rdma_rxe[remote.name] = device
                check_link(remote, device, iface, timeout)
                continue
            if not module_loaded(remote):
                load_module(remote)
                loaded.append(remote)
            device, new = add_link(remote, name, iface)
            # before the checks, so that a link whose check fails goes too
            if new:
                added.append((remote, device))
            ctx.rdma_rxe[remote.name] = device
            check_link(remote, device, iface, timeout)
        yield
    finally:
        for remote, device in added:
            log.info('%s: deleting %s', remote.shortname, device)
            remote.run(args=['sudo', 'rdma', 'link', 'delete', device],
                       check_status=False)
        for remote in loaded:
            log.info('%s: unloading rdma_rxe', remote.shortname)
            remote.run(args=['sudo', 'modprobe', '-r', 'rdma_rxe'],
                       check_status=False)
