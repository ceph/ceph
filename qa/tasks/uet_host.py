"""
UET on bare-metal nodes, over the reference provider's software core:
build the libfabric "uet" provider (uet-ref-prov, prov/, CORE=ref) on
each node, install it where libfabric finds it, and point the OOB
transports at the interface that carries each node's address.

Unlike the ernic task, this needs no UET NIC and no emulator. The
provider sends UET frames over a raw socket on an ordinary Ethernet port,
such as a ConnectX port of a mako node. It keeps one endpoint per
process, and a process uses one interface, so a job gives each process
that uses UET a node of its own, as the ernic suites do with VMs.

Checks and setup on each node, each fatal:

- libfabric is 1.20 or later, as Ceph with WITH_OOB_OFI needs;
- the interface that carries the node's address (or ``iface``) has an
  IPv4 address;
- after the install, ``fi_info -p uet`` lists an RDM endpoint whose
  domain is that interface;
- the kernel drops the UET frames that reach its IP stack (UDP port
  4793, or IP protocol 253 with ``FI_UET_ENCAP=ip``), so that it does not
  answer each of them with an ICMP error. The provider's raw socket gets
  them before the kernel's filter does;
- the S3 client tools may open raw sockets (``cap_net_raw`` on the
  binaries). The Ceph daemons start as root, and an OSD keeps
  CAP_NET_RAW past --setuser when its transport needs it.

Example::

    tasks:
    - install:
    - uet_host:
        uet_prov:
          repo: https://github.com/mmgaggle/uet-ref-prov.git
          ref: wip-uet-hold
    - ceph:
    - rgw: [client.0]

Options:

  uet_prov:         repo and ref of the reference provider with prov/, or
                    "tarball", a path on the teuthology host to a tarball
                    of that tree (see the ernic task). Default: the
                    wip-uet-hold branch of mmgaggle/uet-ref-prov, which
                    has the packet hold
  roles:            the roles whose nodes to set up (default: every node)
  iface:            the interface to use on every node (default: the one
                    that carries the address teuthology reaches it on)
  install_dir:      where libuet-fi.so goes (default: libfabric's own
                    provider directory)
  packages:         build dependencies to install (default: libfabric's
                    development files and tools, gcc, make, git, jq,
                    setcap and nft)
  pds:              rudi (the default) or rud, as for the ernic task
  hold:             the packet hold of the provider (UET_PKT_HOLD_*): a
                    dict of ms, dir, types, every, skip, count, max, dup
                    and log, given to the daemons and the workunits
  env:              more variables for the processes that use the provider
  drop_kernel_udp:  add the kernel filter above (default true)
  net_raw:          the client tools to give cap_net_raw (default the S3
                    get, put and soak tools of ceph-test)
  set_oob_network:  set oob_network and oob_network_interface in the ceph
                    overrides from the nodes' addresses (default true)
  set_client_domain: set OOB_DOMAIN for the workunits to the interface
                    name, when every node's interface has the same name
                    (default true)

pds, hold and env go into the overrides of the ceph task (daemon_env), the
rgw task (env) and the workunit task (env), as the ernic task does.

ctx.uet_host[<remote name>] keeps what each node reported: iface, addr,
the provider's fabric and domain, and provider_dir.
"""
import contextlib
import logging

from teuthology import misc as teuthology
from teuthology.orchestra import run
from teuthology.parallel import parallel

from tasks import ernic

log = logging.getLogger(__name__)

DEFAULT_PACKAGES = dict(
    deb=['libfabric-dev', 'libfabric-bin', 'gcc', 'make', 'git', 'jq',
         'libcap2-bin', 'nftables'],
    rpm=['libfabric-devel', 'libfabric', 'gcc', 'make', 'git', 'jq',
         'libcap', 'nftables'],
)

# the provider with the libfabric glue (prov/) and the packet hold; the
# provider of ultraethernet/uet-ref-prov has neither yet
DEFAULT_UET_PROV = dict(repo='https://github.com/mmgaggle/uet-ref-prov.git',
                        ref='wip-uet-hold')

DEFAULT_NET_RAW = ['ceph_test_rgw_ofi_get', 'ceph_test_rgw_ofi_put',
                   'ceph_test_rgw_ofi_soak']

# the provider's UDP port, and its IP protocol for FI_UET_ENCAP=ip
UET_UDP_PORT = 4793
UET_IPPROTO = 253

NFT_TABLE = 'ceph_uet_host'

# Builds the provider with the reference core. $1 is the directory that
# holds uet-ref-prov/.
BUILD_SCRIPT = r'''
set -e
cd "$1"
make -s -C uet-ref-prov/prov
test -f uet-ref-prov/prov/libuet-fi.so
test -x uet-ref-prov/prov/test_rma
'''

# $1: the build directory, $2: the provider directory
INSTALL_SCRIPT = r'''
set -e
sudo install -D -m 0755 "$1/uet-ref-prov/prov/libuet-fi.so" "$2/libuet-fi.so"
'''

# $1: the interface. Prints its IPv4 address with prefix.
ADDR_SCRIPT = r'''
ip -4 -o addr show dev "$1" | awk '{print $4; exit}'
'''

# $1: the binary. Gives it cap_net_raw, if it is installed.
NET_RAW_SCRIPT = r'''
p=$(command -v "$1") || exit 0
sudo setcap cap_net_raw+ep "$p"
echo "$p"
'''

HOLD_KEYS = ('ms', 'dir', 'types', 'every', 'skip', 'count', 'max', 'dup',
             'log')


def hold_env(hold):
    """
    :param hold: the hold option, e.g. dict(ms=600, types='rudi_resp')
    :returns: the provider's UET_PKT_HOLD_* variables for it
    :raises: RuntimeError for an unknown key, or a hold without ms
    """
    if not hold:
        return dict()
    unknown = sorted(set(hold) - set(HOLD_KEYS))
    if unknown:
        raise RuntimeError(f"uet_host: unknown hold options {unknown}; "
                           f"known: {', '.join(HOLD_KEYS)}")
    if 'ms' not in hold:
        raise RuntimeError("uet_host: a hold needs ms, the hold time")
    env = dict()
    for key, value in hold.items():
        if isinstance(value, bool):
            value = int(value)
        if isinstance(value, (list, tuple)):
            value = ','.join(str(v) for v in value)
        env[f"UET_PKT_HOLD_{key.upper()}"] = str(value)
    return env


def environment(config):
    """
    :returns: the variables the task gives the processes that use the
              provider: the PDS mode, the hold, and env
    """
    pds = config.get('pds', 'rudi')
    if pds not in ernic.PDS_ENV:
        raise RuntimeError(f"uet_host: pds is rudi or rud, not {pds!r}")
    env = dict(ernic.PDS_ENV[pds])
    env.update(hold_env(config.get('hold')))
    env.update((k, str(v)) for k, v in (config.get('env') or dict()).items())
    return env


def nft_script(add):
    """
    The commands that add, or delete, the kernel filter for UET frames
    """
    if not add:
        return f"sudo nft delete table inet {NFT_TABLE} 2>/dev/null || true"
    return (
        f"sudo nft delete table inet {NFT_TABLE} 2>/dev/null; "
        f"sudo nft add table inet {NFT_TABLE} && "
        f"sudo nft add chain inet {NFT_TABLE} input "
        "'{ type filter hook input priority 0; policy accept; }' && "
        f"sudo nft add rule inet {NFT_TABLE} input udp dport {UET_UDP_PORT} "
        "drop && "
        f"sudo nft add rule inet {NFT_TABLE} input meta l4proto {UET_IPPROTO} "
        "drop")


def check_fi_info(out, iface):
    """
    :returns: the RDM entries of the uet provider whose domain is iface
    :raises: RuntimeError when there are none
    """
    entries = ernic.check_fi_info(out, 'uet')
    on_iface = [e for e in entries if e.get('domain') == iface]
    if not on_iface:
        raise RuntimeError(
            f"fi_info -p uet has no RDM endpoint on {iface}, only on "
            f"{', '.join(sorted(set(str(e.get('domain')) for e in entries)))}")
    return on_iface


def client_domain(ifaces):
    """
    :param ifaces: the interface names of the nodes
    :returns: the name when all share it, else None
    """
    names = set(ifaces)
    return names.pop() if len(names) == 1 else None


def _sh(remote, script, *args, timeout=None):
    return remote.sh(['bash', '-c', script, 'uet_host'] + list(args),
                     timeout=timeout)


def setup_node(ctx, remote, config):
    testdir = teuthology.get_testdir(ctx)
    build = f"{testdir}/uet_host"
    found = ctx.uet_host.setdefault(remote.name, dict())
    packages = config.get('packages')
    if packages is None:
        packages = DEFAULT_PACKAGES[remote.os.package_type]
    if packages:
        ernic.install_packages(remote, packages)
    # the provider links the distro's libfabric, as Ceph does
    ernic.check_libfabric(_sh(remote, ernic.FI_VERSION_SCRIPT, timeout=60))
    iface = config.get('iface') or remote.interface
    addr = _sh(remote, ADDR_SCRIPT, iface, timeout=60).strip()
    if not addr:
        raise RuntimeError(f"{remote.shortname}: {iface} has no IPv4 address")
    found.update(iface=iface, addr=addr)
    ernic._fetch(ctx, remote, 'uet_prov',
                 config.get('uet_prov') or DEFAULT_UET_PROV,
                 f"{build}/uet-ref-prov")
    _sh(remote, BUILD_SCRIPT, build, timeout=900)
    install_dir = config.get('install_dir') or \
        _sh(remote, ernic.PROVIDER_DIR_SCRIPT, timeout=60).strip()
    _sh(remote, INSTALL_SCRIPT, build, install_dir, timeout=60)
    found['provider_dir'] = install_dir
    env = '' if not config.get('install_dir') else \
        f"FI_PROVIDER_PATH={install_dir} "
    entries = check_fi_info(_sh(remote, env + ernic.FI_INFO_SCRIPT,
                                timeout=90), iface)
    found.update(fabric=entries[0].get('fabric'), domain=entries[0]['domain'])
    log.info("%s: fi_info -p uet: fabric %s, domain %s (%s), from %s",
             remote.shortname, found['fabric'], found['domain'], addr,
             install_dir)
    if config.get('drop_kernel_udp', True):
        remote.run(args=['bash', '-c', nft_script(True)])
        found['nft'] = True
    found['net_raw'] = []
    for tool in config.get('net_raw', DEFAULT_NET_RAW):
        path = _sh(remote, NET_RAW_SCRIPT, tool, timeout=60).strip()
        if path:
            found['net_raw'].append(path)
    if found['net_raw']:
        log.info("%s: cap_net_raw on %s", remote.shortname,
                 ' '.join(found['net_raw']))


def cleanup_node(ctx, remote):
    testdir = teuthology.get_testdir(ctx)
    found = ctx.uet_host.get(remote.name, dict())
    if found.get('nft'):
        remote.run(args=['bash', '-c', nft_script(False)], check_status=False)
    for path in found.get('net_raw', []):
        remote.run(args=['sudo', 'setcap', '-r', path], check_status=False)
    args = ['rm', '-rf', f"{testdir}/uet_host"]
    if found.get('provider_dir'):
        args += [run.Raw(';'), 'sudo', 'rm', '-f',
                 f"{found['provider_dir']}/libuet-fi.so"]
    remote.run(args=args, check_status=False)


def set_overrides(ctx, config, remotes):
    """
    Point the OOB transports at the nodes' interfaces, and the workunits
    at the interface name when the nodes share one
    """
    overrides = ctx.config.setdefault('overrides', dict())
    if config.get('set_oob_network', True):
        conf = ernic.oob_overrides(
            [ctx.uet_host[r.name]['addr'] for r in remotes],
            [ctx.uet_host[r.name]['iface'] for r in remotes])
        global_conf = overrides.setdefault('ceph', dict()).setdefault(
            'conf', dict()).setdefault('global', dict())
        for key, value in conf.items():
            # a suite that names its own network wins
            if key.replace('_', ' ') in global_conf or key in global_conf:
                continue
            global_conf[key] = value
            log.info("ceph overrides: %s = %s", key, value)
    if config.get('set_client_domain', True):
        domain = client_domain(ctx.uet_host[r.name]['iface'] for r in remotes)
        if domain:
            wenv = overrides.setdefault('workunit', dict()).setdefault(
                'env', dict())
            wenv.setdefault('OOB_DOMAIN', domain)
            log.info("workunit overrides: OOB_DOMAIN=%s", wenv['OOB_DOMAIN'])
        else:
            log.warning("the nodes' interfaces have different names: the "
                        "workunits get no OOB_DOMAIN")


@contextlib.contextmanager
def task(ctx, config):
    """
    Build and install the uet provider over the reference core on each
    node, and point the OOB transports at the nodes' interfaces
    """
    if config is None:
        config = dict()
    assert isinstance(config, dict), 'uet_host: the config must be a dict'
    overrides = ctx.config.get('overrides', dict())
    teuthology.deep_merge(config, overrides.get('uet_host', dict()))
    if not hasattr(ctx, 'uet_host'):
        ctx.uet_host = dict()
    remotes = ernic._remotes(ctx, config)
    env = environment(config)
    try:
        with parallel() as p:
            for remote in remotes:
                p.spawn(setup_node, ctx, remote, config)
        if env:
            ernic.set_env_overrides(ctx, env)
        set_overrides(ctx, config, remotes)
        yield
    finally:
        log.info("Removing the uet provider")
        for remote in remotes:
            try:
                cleanup_node(ctx, remote)
            except Exception:
                log.exception("Failed to clean up %s", remote.shortname)
