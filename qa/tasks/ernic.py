"""
rocm-ernic guests: check the emulated ionic NIC, and install the libfabric
"uet" provider built over libuet_ernic, or check libfabric's verbs
provider on the NIC's RDMA device.

rocm-ernic emulates an AMD Pensando ionic RDMA NIC with a UET engine inside
it. Teuthology's ernic provisioner, which upstream teuthology does not have
yet, boots the VMs; this task checks, on each
node, that the guest drives the device, then builds the uet provider with
the engine as its core (``make -C prov ernic``) and installs it where
libfabric finds it, so that Ceph daemons and clients need no
FI_PROVIDER_PATH.

Checks, each fatal:

- the emulated device (PCI 1dd8:100a) is present, bound to ionic, and the
  ionic and ionic_rdma modules are loaded;
- the distro's libfabric, which the provider and Ceph's WITH_OOB_OFI
  build use, is 1.20 or later: Ubuntu 26.04 has 2.1, Ubuntu 24.04 only
  1.17;
- it has an RDMA device whose port is active, and a netdev with an IPv4
  address (the provisioner's guest_ip);
- locked memory is unlimited (``require_memlock``);
- after the install, ``fi_info -p uet`` lists the provider with an RDM
  endpoint.

With ``provider: verbs`` the task builds nothing. The guests use the ionic
RDMA driver through rdma-core and libfabric's verbs provider, as on any
RoCE NIC. After the checks above, ``fi_info -p 'verbs;ofi_rxm'`` must list
an RDM endpoint on the NIC's RDMA device.

Example::

    tasks:
    - ernic:
        rocm_ernic:
          repo: https://github.com/ROCm/rocm-ernic.git
          ref: 9a67362
        uet_prov:
          repo: https://github.com/mmgaggle/uet-ref-prov.git
          ref: wip-uet-hold
    - ceph:
    - rgw: [client.0]
    - ernic.rgw_endpoint:
        rgw: client.0
        clients: [client.1]

Options:

  provider:         uet (the default) or verbs
  roles:            the roles whose nodes to set up (default: every node)
  rocm_ernic:       repo and ref of rocm-ernic, for guest/libuet_ernic and
                    shared/uet_ernic_abi.h; or "tarball", a path on the
                    teuthology host to a tarball of that tree. The ref is
                    a branch, a tag or a commit id, full or abbreviated
                    (which fetches the whole history)
  uet_prov:         repo and ref of the reference provider with prov/, or
                    "tarball" as above
  install_dir:      where libuet-fi.so goes (default: libfabric's own
                    provider directory, <libdir>/libfabric)
  packages:         build dependencies to install (default: libfabric-dev,
                    libfabric-bin, libibverbs-dev, gcc, make, git)
  require_memlock:  fail unless "ulimit -l" is unlimited (default true)
  set_oob_network:  set oob_network and oob_network_interface in the ceph
                    overrides from the ionic NICs' addresses (default true)
  pds:              rudi (the provider's default) or rud: with rud, every
                    process registers its windows for RUD, so that peers
                    write them with RUD rather than RUDI (FI_UET_RUDI=0).
                    For the uet provider only
  env:              more variables for the processes that use the provider

pds and env go into the overrides of the ceph task (daemon_env), the rgw
task (env) and the workunit task (env), which those tasks read when they
start. So the Ceph daemons, the gateways and the workunits of the tasks
that come after this one get them. Other processes, such as the commands
of an exec task, do not.

ctx.ernic[<remote name>] keeps what each node reported: pci, driver, rdma,
port, netdev, addr, memlock and the provider's fabric and domain.
"""
import contextlib
import ipaddress
import logging
import os
import re

from teuthology import misc as teuthology
from teuthology.orchestra import run
from teuthology.parallel import parallel

log = logging.getLogger(__name__)

ERNIC_VENDOR = '0x1dd8'
ERNIC_DEVICE = '0x100a'

# the oldest libfabric that a Ceph built with WITH_OOB_OFI takes, which
# the uet provider must use too
MIN_LIBFABRIC = (1, 20)

DEFAULT_PACKAGES = dict(
    deb=['libfabric-dev', 'libfabric-bin', 'libibverbs-dev', 'gcc', 'make',
         'git', 'jq'],
    rpm=['libfabric-devel', 'libfabric', 'rdma-core-devel', 'gcc', 'make',
         'git', 'jq'],
)

# what the verbs provider needs: fi_info, and rdma-core's ionic provider
VERBS_PACKAGES = dict(
    deb=['libfabric-bin', 'ibverbs-providers', 'jq'],
    rpm=['libfabric', 'libibverbs', 'jq'],
)

PROVIDERS = ('uet', 'verbs')

# Prints key=value lines about the emulated NIC; parse_kv() reads them.
CHECK_SCRIPT = r'''
bdf=""
for d in /sys/bus/pci/devices/*; do
    if [ "$(cat "$d/vendor")" = VENDOR ] && [ "$(cat "$d/device")" = DEVICE ]; then
        bdf=${d##*/}
        break
    fi
done
echo "pci=$bdf"
if [ -n "$bdf" ]; then
    D=/sys/bus/pci/devices/$bdf
    echo "driver=$(basename "$(readlink "$D/driver" 2>/dev/null)" 2>/dev/null)"
    rdma=$(ls "$D/infiniband" 2>/dev/null | head -n 1)
    echo "rdma=$rdma"
    ifc=$(ls "$D/net" 2>/dev/null | head -n 1)
    echo "netdev=$ifc"
    if [ -n "$ifc" ]; then
        echo "addr=$(ip -4 -o addr show dev "$ifc" | awk '{print $4; exit}')"
        echo "mtu=$(cat "/sys/class/net/$ifc/mtu")"
    fi
    if [ -n "$rdma" ]; then
        echo "port=$(cat "/sys/class/infiniband/$rdma/ports/1/state" 2>/dev/null | awk '{print $2}')"
    fi
fi
mods=""
for m in ionic ionic_rdma ib_uverbs; do
    [ -d "/sys/module/$m" ] && mods="$mods $m"
done
echo "modules=${mods# }"
echo "memlock=$(ulimit -l)"
'''.replace('VENDOR', ERNIC_VENDOR).replace('DEVICE', ERNIC_DEVICE)

# Builds the provider with the engine as its core. $1 is the directory that
# holds rocm-ernic/ and uet-ref-prov/.
BUILD_SCRIPT = r'''
set -e
cd "$1"
make -s -C uet-ref-prov/prov ernic ERNIC="$1/rocm-ernic"
test -f uet-ref-prov/prov/ernic/libuet-fi.so
test -x uet-ref-prov/prov/test_rma
'''

# Where libfabric looks for DL providers when FI_PROVIDER_PATH is unset.
PROVIDER_DIR_SCRIPT = r'''
lib=$(ldconfig -p | awk '/libfabric\.so\.1 /{print $NF; exit}')
[ -n "$lib" ] || { echo "libfabric.so.1 is not installed" >&2; exit 1; }
echo "$(dirname "$(readlink -f "$lib")")/libfabric"
'''

# $1: the build directory, $2: the provider directory
INSTALL_SCRIPT = r'''
set -e
sudo install -D -m 0755 "$1/uet-ref-prov/prov/ernic/libuet-fi.so" "$2/libuet-fi.so"
'''

FI_INFO_SCRIPT = 'timeout 60 fi_info -p uet'
VERBS_FI_INFO_SCRIPT = "timeout 60 fi_info -p 'verbs;ofi_rxm' -t FI_EP_RDM"

FI_VERSION_SCRIPT = 'fi_info --version'

# $1: the repo, $2: the ref, $3: the directory to check it out in. A
# branch, a tag or a full commit id comes with a shallow fetch. A server
# resolves no abbreviated commit id, so for one the history comes too.
FETCH_SCRIPT = r'''
set -e
git init -q "$3"
cd "$3"
if git fetch -q --depth 1 "$1" "$2" 2>/dev/null; then
    git checkout -q FETCH_HEAD
else
    git fetch -q "$1" '+refs/heads/*:refs/remotes/origin/*' \
        '+refs/tags/*:refs/tags/*'
    git checkout -q "$2^{commit}" 2>/dev/null ||
        git checkout -q "origin/$2^{commit}"
fi
git log -1 --format='%H %s'
'''

PDS_ENV = dict(rudi=dict(), rud=dict(FI_UET_RUDI='0'))

# the overrides that give environment variables to the processes of a
# task: (task, key)
ENV_OVERRIDES = (('ceph', 'daemon_env'), ('rgw', 'env'), ('workunit', 'env'))


def parse_kv(out):
    """
    :returns: a dict of the key=value lines of a script's output
    """
    found = dict()
    for line in out.splitlines():
        key, sep, value = line.partition('=')
        if sep and key and ' ' not in key:
            found[key.strip()] = value.strip()
    return found


def check_nic(found, require_memlock=True):
    """
    :param found: what CHECK_SCRIPT printed, parsed
    :returns: a list of problems, empty when the node is fine
    """
    problems = []
    if not found.get('pci'):
        return [f"no emulated ionic device ({ERNIC_VENDOR}:{ERNIC_DEVICE}) on the PCI bus"]
    if found.get('driver') != 'ionic':
        problems.append(
            f"the device at {found['pci']} is bound to "
            f"{found.get('driver') or 'no driver'}, not ionic: the image needs "
            "rocm-ernic's ionic modules")
    modules = found.get('modules', '').split()
    for module in ('ionic', 'ionic_rdma'):
        if module not in modules:
            problems.append(f"the {module} module is not loaded")
    if not found.get('rdma'):
        problems.append("the device has no RDMA device")
    elif found.get('port') != 'ACTIVE':
        problems.append(f"RDMA port 1 of {found['rdma']} is {found.get('port') or 'missing'}")
    if not found.get('netdev'):
        problems.append("the device has no netdev")
    elif not found.get('addr'):
        problems.append(f"{found['netdev']} has no IPv4 address")
    if require_memlock and found.get('memlock') != 'unlimited':
        problems.append(f"locked memory is limited to {found.get('memlock')} KiB, not unlimited")
    return problems


def parse_fi_info(out):
    """
    :returns: a list of dicts, one per fi_info entry, with provider, fabric,
              domain and type
    """
    entries = []
    current = None
    for line in out.splitlines():
        line = line.strip()
        if line.startswith('provider:'):
            current = dict(provider=line.split(':', 1)[1].strip())
            entries.append(current)
        elif current is not None and ':' in line:
            key, value = line.split(':', 1)
            if key in ('fabric', 'domain', 'type', 'version', 'protocol'):
                current.setdefault(key, value.strip())
    return entries


def libfabric_version(out):
    """
    :returns: (major, minor) of the libfabric that "fi_info --version"
              reports, or None
    """
    m = re.search(r'^libfabric:\s*(\d+)\.(\d+)', out, re.M)
    return (int(m.group(1)), int(m.group(2))) if m else None


def check_libfabric(out):
    """
    :raises: RuntimeError when "fi_info --version" reports no libfabric of
             MIN_LIBFABRIC or later
    """
    version = libfabric_version(out)
    if version is None or version < MIN_LIBFABRIC:
        have = '.'.join(map(str, version)) if version else 'none'
        raise RuntimeError(
            f"libfabric {have}: Ceph with WITH_OOB_OFI, and the uet provider "
            f"built for it, need {'.'.join(map(str, MIN_LIBFABRIC))} or later "
            "(Ubuntu 24.04 has 1.17, Ubuntu 26.04 has 2.1)")


def check_fi_info(out, provider='uet'):
    """
    :returns: the RDM entries of provider in fi_info's output
    :raises: RuntimeError when there are none
    """
    name = 'verbs;ofi_rxm' if provider == 'verbs' else provider
    entries = [e for e in parse_fi_info(out)
               if e['provider'] == name and e.get('type') == 'FI_EP_RDM']
    if not entries:
        raise RuntimeError(f"fi_info -p {name} lists no {name} RDM endpoint: "
                           f"{out.strip()[-500:]}")
    return entries


def oob_overrides(addrs, netdevs):
    """
    The ceph overrides that point the OOB transports at the ionic NICs

    :param addrs: the NICs' addresses with prefix, e.g. ['10.88.0.61/24']
    :param netdevs: the NICs' names
    """
    subnets = sorted(set(str(ipaddress.ip_interface(a).network) for a in addrs))
    conf = dict(oob_network=', '.join(subnets))
    names = set(netdevs)
    if len(names) == 1:
        conf['oob_network_interface'] = names.pop()
    return conf


def _sh(remote, script, *args, timeout=None):
    return remote.sh(['bash', '-c', script, 'ernic'] + list(args),
                     timeout=timeout)


def _remotes(ctx, config):
    roles = config.get('roles')
    if not roles:
        return list(ctx.cluster.remotes.keys())
    remotes = []
    for role in roles:
        (remote,) = ctx.cluster.only(role).remotes.keys()
        if remote not in remotes:
            remotes.append(remote)
    return remotes


def check_node(ctx, remote, config):
    found = parse_kv(_sh(remote, CHECK_SCRIPT, timeout=60))
    log.info("%s: %s", remote.shortname,
             ' '.join(f"{k}={v}" for k, v in sorted(found.items())))
    problems = check_nic(found, config.get('require_memlock', True))
    if problems:
        raise RuntimeError(f"{remote.shortname} is not a usable ernic guest: "
                           f"{'; '.join(problems)}")
    ctx.ernic[remote.name] = found


def _fetch(ctx, remote, name, source, dest):
    """
    Put one source tree at dest: a git repo and ref, or a tarball on the
    teuthology host
    """
    remote.run(args=['rm', '-rf', dest, run.Raw('&&'), 'mkdir', '-p', dest])
    tarball = source.get('tarball')
    if tarball:
        remote_tar = f"{dest}.tar"
        remote.put_file(os.path.expanduser(tarball), remote_tar)
        remote.run(args=['tar', '-C', dest, '-xf', remote_tar,
                         run.Raw('&&'), 'rm', '-f', remote_tar])
        return
    repo = source.get('repo')
    ref = str(source.get('ref') or 'main')
    if not repo:
        raise RuntimeError(f"ernic: {name} needs a repo or a tarball")
    head = _sh(remote, FETCH_SCRIPT, repo, ref, dest, timeout=900).strip()
    log.info("%s: %s %s is %s", remote.shortname, name, ref, head)


def install_packages(remote, packages):
    if remote.os.package_type == 'deb':
        remote.run(args=[
            'sudo', 'env', 'DEBIAN_FRONTEND=noninteractive',
            'apt-get', 'install', '-y', '-q', '--no-install-recommends',
        ] + packages, timeout=1200)
    else:
        remote.run(args=['sudo', 'dnf', 'install', '-y'] + packages,
                   timeout=1200)


def check_verbs(ctx, remote, config):
    """
    Install what libfabric's verbs provider needs, and check that it
    reaches the NIC's RDMA device
    """
    packages = config.get('packages')
    if packages is None:
        packages = VERBS_PACKAGES[remote.os.package_type]
    if packages:
        install_packages(remote, packages)
    check_libfabric(_sh(remote, FI_VERSION_SCRIPT, timeout=60))
    entries = check_fi_info(_sh(remote, VERBS_FI_INFO_SCRIPT, timeout=90),
                            'verbs')
    found = ctx.ernic.setdefault(remote.name, dict())
    on_nic = [e for e in entries if e.get('domain') == found.get('rdma')]
    if not on_nic:
        raise RuntimeError(
            f"{remote.shortname}: the verbs provider has no RDM endpoint on "
            f"{found.get('rdma')}, only on "
            f"{', '.join(sorted(set(str(e.get('domain')) for e in entries)))}")
    found.update(fabric=on_nic[0].get('fabric'), domain=on_nic[0]['domain'])
    log.info("%s: fi_info -p 'verbs;ofi_rxm': fabric %s, domain %s",
             remote.shortname, found['fabric'], found['domain'])


def install_provider(ctx, remote, config):
    testdir = teuthology.get_testdir(ctx)
    build = f"{testdir}/ernic"
    packages = config.get('packages')
    if packages is None:
        packages = DEFAULT_PACKAGES[remote.os.package_type]
    if packages:
        install_packages(remote, packages)
    # the provider links the distro's libfabric, as Ceph does
    check_libfabric(_sh(remote, FI_VERSION_SCRIPT, timeout=60))
    _fetch(ctx, remote, 'rocm_ernic', config.get('rocm_ernic') or dict(),
           f"{build}/rocm-ernic")
    _fetch(ctx, remote, 'uet_prov', config.get('uet_prov') or dict(),
           f"{build}/uet-ref-prov")
    _sh(remote, BUILD_SCRIPT, build, timeout=900)
    install_dir = config.get('install_dir') or \
        _sh(remote, PROVIDER_DIR_SCRIPT, timeout=60).strip()
    _sh(remote, INSTALL_SCRIPT, build, install_dir, timeout=60)
    env = '' if not config.get('install_dir') else \
        f"FI_PROVIDER_PATH={install_dir} "
    out = _sh(remote, env + FI_INFO_SCRIPT, timeout=90)
    entries = check_fi_info(out)
    found = ctx.ernic.setdefault(remote.name, dict())
    found.update(provider_dir=install_dir, fabric=entries[0].get('fabric'),
                 domain=entries[0].get('domain'))
    log.info("%s: fi_info -p uet: fabric %s, domain %s, from %s",
             remote.shortname, found['fabric'], found['domain'], install_dir)
    if found.get('rdma') and found['domain'] != found['rdma']:
        log.warning("%s: the uet domain %s is not the ionic RDMA device %s",
                    remote.shortname, found['domain'], found['rdma'])


def remove_provider(ctx, remote):
    testdir = teuthology.get_testdir(ctx)
    found = ctx.ernic.get(remote.name, dict())
    args = ['rm', '-rf', f"{testdir}/ernic"]
    if found.get('provider_dir'):
        args += [run.Raw(';'), 'sudo', 'rm', '-f',
                 f"{found['provider_dir']}/libuet-fi.so"]
    remote.run(args=args, check_status=False)


def environment(config):
    """
    :returns: the variables the task gives the processes that use the
              provider
    """
    if config.get('provider', 'uet') != 'uet':
        if 'pds' in config:
            raise RuntimeError("ernic: pds is for the uet provider only")
        return dict((k, str(v)) for k, v in (config.get('env') or dict()).items())
    pds = config.get('pds', 'rudi')
    if pds not in PDS_ENV:
        raise RuntimeError(f"ernic: pds is rudi or rud, not {pds!r}")
    env = dict(PDS_ENV[pds])
    env.update((k, str(v)) for k, v in (config.get('env') or dict()).items())
    return env


def set_env_overrides(ctx, env):
    """
    Give env to the Ceph daemons, the gateways and the workunits, through
    the overrides of their tasks
    """
    overrides = ctx.config.setdefault('overrides', dict())
    for task_name, key in ENV_OVERRIDES:
        section = overrides.setdefault(task_name, dict()).setdefault(
            key, dict())
        section.update(env)
        log.info("%s overrides: %s %s", task_name, key,
                 ' '.join(f"{k}={v}" for k, v in sorted(env.items())))


def set_oob_network(ctx, remotes):
    addrs = [ctx.ernic[r.name]['addr'] for r in remotes]
    netdevs = [ctx.ernic[r.name]['netdev'] for r in remotes]
    conf = oob_overrides(addrs, netdevs)
    overrides = ctx.config.setdefault('overrides', dict())
    global_conf = overrides.setdefault('ceph', dict()).setdefault(
        'conf', dict()).setdefault('global', dict())
    for key, value in conf.items():
        # a suite that names its own network wins
        if key.replace('_', ' ') in global_conf or key in global_conf:
            continue
        global_conf[key] = value
        log.info("ceph overrides: %s = %s", key, value)


@contextlib.contextmanager
def task(ctx, config):
    """
    Check the ernic guests and install the uet provider over libuet_ernic,
    or check the verbs provider
    """
    if config is None:
        config = dict()
    assert isinstance(config, dict), 'ernic: the config must be a dict'
    overrides = ctx.config.get('overrides', dict())
    teuthology.deep_merge(config, overrides.get('ernic', dict()))
    if not hasattr(ctx, 'ernic'):
        ctx.ernic = dict()
    remotes = _remotes(ctx, config)
    provider = config.get('provider', 'uet')
    if provider not in PROVIDERS:
        raise RuntimeError(f"ernic: provider is uet or verbs, not {provider!r}")

    env = environment(config)
    with parallel() as p:
        for remote in remotes:
            p.spawn(check_node, ctx, remote, config)
    try:
        with parallel() as p:
            for remote in remotes:
                p.spawn(check_verbs if provider == 'verbs' else install_provider,
                        ctx, remote, config)
        if env:
            set_env_overrides(ctx, env)
        if config.get('set_oob_network', True):
            set_oob_network(ctx, remotes)
        yield
    finally:
        if provider == 'uet':
            log.info("Removing the uet provider")
        for remote in remotes if provider == 'uet' else []:
            try:
                remove_provider(ctx, remote)
            except Exception:
                log.exception("Failed to clean up %s", remote.shortname)


@contextlib.contextmanager
def rgw_endpoint(ctx, config):
    """
    Write the gateway's URL into url_file on other nodes, for workunits such
    as rgw/run-s3-oob.sh that run on a node of their own: in an ernic job,
    each process that uses UET needs its own VM, so the client cannot share
    the gateway's.

        - ernic.rgw_endpoint:
            rgw: client.0
            clients: [client.1]
    """
    config = config or dict()
    rgw = config.get('rgw', 'client.0')
    clients = config.get('clients') or []
    url = ctx.rgw.role_endpoints[rgw].url().rstrip('/')
    testdir = teuthology.get_testdir(ctx)
    url_file = f"{testdir}/url_file"
    for client in clients:
        (remote,) = ctx.cluster.only(client).remotes.keys()
        remote.write_file(url_file, url)
        log.info("%s: url_file is %s", client, url)
    try:
        yield
    finally:
        for client in clients:
            (remote,) = ctx.cluster.only(client).remotes.keys()
            remote.run(args=['rm', '-f', url_file], check_status=False)
