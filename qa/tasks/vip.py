import contextlib
import ipaddress
import logging
import re

from teuthology import misc as teuthology
from teuthology.config import config as teuth_config
from teuthology.exceptions import ConfigError

log = logging.getLogger(__name__)


def subst_vip(ctx, cmd):
    p = re.compile(r'({{VIP(\d+)}})')
    for m in p.findall(cmd):
        n = int(m[1])
        if n >= len(ctx.vip["vips"]):
            log.warning(f'no VIP{n} (we have {len(ctx.vip["vips"])})')
        else:
            cmd = cmd.replace(m[0], str(ctx.vip["vips"][n]))

    if '{{VIPPREFIXLEN}}' in cmd:
        cmd = cmd.replace('{{VIPPREFIXLEN}}', str(ctx.vip["vnet"].prefixlen))

    if '{{VIPSUBNET}}' in cmd:
        cmd = cmd.replace('{{VIPSUBNET}}', str(ctx.vip["vnet"].network_address))

    return cmd


def echo(ctx, config):
    """
    This is mostly for debugging
    """
    for remote in ctx.cluster.remotes.keys():
        log.info(subst_vip(ctx, config))


def exec(ctx, config):
    """
    This is similar to the standard 'exec' task, but does the VIP substitutions.
    """
    assert isinstance(config, dict), "task exec got invalid config"

    testdir = teuthology.get_testdir(ctx)

    if 'all-roles' in config and len(config) == 1:
        a = config['all-roles']
        roles = teuthology.all_roles(ctx.cluster)
        config = dict((id_, a) for id_ in roles if not id_.startswith('host.'))
    elif 'all-hosts' in config and len(config) == 1:
        a = config['all-hosts']
        roles = teuthology.all_roles(ctx.cluster)
        config = dict((id_, a) for id_ in roles if id_.startswith('host.'))

    for role, ls in config.items():
        (remote,) = ctx.cluster.only(role).remotes.keys()
        log.info('Running commands on role %s host %s', role, remote.name)
        for c in ls:
            c.replace('$TESTDIR', testdir)
            remote.run(
                args=[
                    'sudo',
                    'TESTDIR={tdir}'.format(tdir=testdir),
                    'bash',
                    '-ex',
                    '-c',
                    subst_vip(ctx, c)],
                )


def _map_vips(mip, count):
    vip_entries = teuth_config.get('vip', [])
    if not vip_entries:
        raise ConfigError(
            'at least one item must be configured for "vip" config key'
            ' to use the vip task'
        )
    for mapping in vip_entries:
        mnet = ipaddress.ip_network(mapping['machine_subnet'])
        vnet = ipaddress.ip_network(mapping['virtual_subnet'])
        if vnet.prefixlen >= mnet.prefixlen:
            log.error(f"virtual_subnet {vnet} prefix >= machine_subnet {mnet} prefix")
            raise ConfigError('virtual subnet too small')
        if mip not in mnet:
            # not our machine subnet
            log.info(f"machine ip {mip} not in machine subnet {mnet}")
            continue
        pos = list(mnet.hosts()).index(mip)
        log.info(f"{mip} in {mnet}, pos {pos}")
        r = []
        for sub in vnet.subnets(new_prefix=mnet.prefixlen):
            r += [list(sub.hosts())[pos]]
            count -= 1
            if count == 0:
                break
        return vnet, r
    raise ConfigError(f"no matching machine subnet found for {mip}")


@contextlib.contextmanager
def task(ctx, config):
    """
    Set up a virtual network and allocate virtual IP(s) for each machine.

    The strategy here is to set up a private virtual subnet that is larger than
    the subnet the machine(s) exist in, and allocate virtual IPs from that pool.

    - The teuthology.yaml must include a section like::

        vip:
          - machine_subnet: 172.21.0.0/20
            virtual_subnet: 10.0.0.0/16

      At least one item's machine_subnet should map the subnet the test machine's
      primary IP lives in (the one DNS resolves to).  The virtual_subnet must have a
      shorter prefix (i.e., larger than the machine_subnet).  If there are multiple
      machine_subnets, they cannot map into the same virtual_subnet.

    - Each machine gets an IP in the virtual_subset statically configured by the vip
      task. This lets all test machines reach each other and (most importantly) any
      virtual IPs.

    - 1 or more virtual IPs are then mapped for the task.  These IPs are chosen based
      on one of the remotes.  This uses a lot of network space but it avoids any
      conflicts between tests.

    To use a virtual IP, the {{VIP0}}, {{VIP1}}, etc. substitutions can be used.
    
    {{VIPSUBNET}} is the virtual_subnet address (10.0.0.0 in the example).

    {{VIPPREFIXLEN}} is the virtual_subnet prefix (16 in the example.

    These substitutions work for vip.echo, and (at the time of writing) cephadm.apply
    and cephadm.shell.
    """
    if config is None:
        config = {}
    count = config.get('count', 1)

    ctx.vip_static = {}
    ctx.vip = {}

    log.info("Allocating static IPs for each host...")
    for remote in ctx.cluster.remotes.keys():
        ip = remote.ssh.get_transport().getpeername()[0]
        log.info(f'peername {ip}')
        mip = ipaddress.ip_address(ip)
        vnet, vips = _map_vips(mip, count + 1)
        static = vips.pop(0)
        log.info(f"{remote.hostname} static {static}, vnet {vnet}")

        if not ctx.vip:
            # do this only once (use the first remote we see), since we only need 1
            # set of virtual IPs, regardless of how many remotes we have.
            log.info(f"VIPs are {vips!r}")
            ctx.vip = {
                'vnet': vnet,
                'vips': vips,
            }
        else:
            # all remotes must be in the same virtual network...
            assert vnet == ctx.vip['vnet']

        # pick interface
        p = re.compile(r'^(\S+) dev (\S+) (.*)scope link (.*)src (\S+)')
        iface = None
        for line in remote.sh(['sudo', 'ip','route','ls']).splitlines():
            m = p.findall(line)
            if not m:
                continue
            route_iface = m[0][1]
            route_ip = m[0][4]
            if route_ip == ip:
                iface = route_iface
                break

        if not iface:
            log.error(f"Unable to find {remote.hostname} interface for {ip}")
            continue

        # configure
        log.info(f"Configuring {static} on {remote.hostname} iface {iface}...")
        remote.sh(['sudo',
                   'ip', 'addr', 'add',
                   str(static) + '/' + str(vnet.prefixlen),
                   'dev', iface])

        ctx.vip_static[remote] = {
            "iface": iface,
            "static": static,
        }

    try:
        yield

    finally:
        for remote, m in ctx.vip_static.items():
            log.info(f"Removing {m['static']} (and any VIPs) on {remote.hostname} iface {m['iface']}...")
            remote.sh(['sudo',
                       'ip', 'addr', 'del',
                       str(m['static']) + '/' + str(ctx.vip['vnet'].prefixlen),
                       'dev', m['iface']])

            for vip in ctx.vip['vips']:
                remote.sh(
                    [
                        'sudo',
                        'ip', 'addr', 'del',
                        str(vip) + '/' + str(ctx.vip['vnet'].prefixlen),
                        'dev', m['iface']
                    ],
                    check_status=False,
                )
            
