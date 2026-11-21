"""
Export each OSD's scratch device over loopback NVMe/TCP for crimson
SeaStore's SPDK userspace driver.
"""
import contextlib
import logging

from io import StringIO
from teuthology import misc as teuthology
from teuthology.exceptions import ConfigError
from teuthology.orchestra import run


log = logging.getLogger(__name__)

NVMET = '/sys/kernel/config/nvmet'
NQN_PREFIX = 'nqn.2016-06.io.ceph.qa'
TRADDR = '127.0.0.1'
TRSVCID = '4420'
PORT = f'{NVMET}/ports/{TRSVCID}'


def _sh(remote, cmd):
    return remote.sh(['bash', '-c', cmd]).strip()


def _write(path, value):
    return f'echo -n {value} | sudo tee {path} >/dev/null'


def _setup_port(remote):
    _sh(remote, ' && '.join([
        'sudo modprobe nvmet-tcp',
        'mountpoint -q /sys/kernel/config || '
        'sudo mount -t configfs none /sys/kernel/config',
        f'sudo mkdir {PORT}',
        _write(f'{PORT}/addr_trtype', 'tcp'),
        _write(f'{PORT}/addr_adrfam', 'ipv4'),
        _write(f'{PORT}/addr_traddr', TRADDR),
        _write(f'{PORT}/addr_trsvcid', TRSVCID),
    ]))


def _export(remote, nqn, dev):
    subsys = f'{NVMET}/subsystems/{nqn}'
    _sh(remote, ' && '.join([
        f'sudo mkdir {subsys}',
        _write(f'{subsys}/attr_allow_any_host', '1'),
        f'sudo mkdir {subsys}/namespaces/1',
        _write(f'{subsys}/namespaces/1/device_path', dev),
        _write(f'{subsys}/namespaces/1/enable', '1'),
        f'sudo ln -s {subsys} {PORT}/subsystems/{nqn}',
    ]))


def _unexport(remote, nqn):
    subsys = f'{NVMET}/subsystems/{nqn}'
    remote.run(args=['bash', '-c', '; '.join([
        f'sudo rm -f {PORT}/subsystems/{nqn}',
        _write(f'{subsys}/namespaces/1/enable', '0'),
        f'sudo rmdir {subsys}/namespaces/1',
        f'sudo rmdir {subsys}',
    ])], check_status=False)


@contextlib.contextmanager
def task(ctx, config):
    """
    Export each OSD's scratch device (an LV is fine) through the kernel
    NVMe/TCP target on 127.0.0.1 for crimson-osd's SPDK driver. Must run
    before the ceph task, which then sets seastore_spdk_transport_id and
    skips the xfs mkfs/mount for these OSDs.

        tasks:
        - install:
        - spdk_nvmet_tcp:
            hugepages_per_osd: 512     # 2 MiB pages, default 512 (1 GiB)
        - ceph:
    """
    if config is None:
        config = {}
    cluster_name = config.get('cluster', 'ceph')
    hugepages_per_osd = int(config.get('hugepages_per_osd', 512))

    if not hasattr(ctx, 'spdk_nvmet_tcp'):
        ctx.spdk_nvmet_tcp = {}
    exported = {}
    old_hugepages = {}

    osds = ctx.cluster.only(teuthology.is_type('osd', cluster_name))
    try:
        for remote, roles_for_host in osds.remotes.items():
            if remote.is_container:
                continue
            # Same role -> device assignment as ceph.py's assign_devs().
            roles = list(teuthology.cluster_roles_of_type(
                roles_for_host, 'osd', cluster_name))
            devs = teuthology.get_scratch_devices(remote)
            if len(devs) < len(roles):
                raise ConfigError(
                    f'spdk_nvmet_tcp: {remote.shortname} has {len(roles)} '
                    f'osd roles but only {len(devs)} scratch devices')

            _setup_port(remote)
            exported[remote] = []
            role_to_trid = {}
            for role, dev in zip(roles, devs):
                dev = _sh(remote, f'readlink -f {dev}')
                # Drop any stale superblock before SeaStore's mkfs.
                remote.run(args=[
                    'sudo', 'dd', 'if=/dev/zero', f'of={dev}',
                    'bs=1M', 'count=16', 'oflag=direct',
                ], stdout=StringIO())
                nqn = f'{NQN_PREFIX}:{cluster_name}.{role}'
                _export(remote, nqn, dev)
                exported[remote].append(nqn)
                role_to_trid[role] = (
                    f'trtype:TCP adrfam:IPv4 traddr:{TRADDR} '
                    f'trsvcid:{TRSVCID} subnqn:{nqn}')
                log.info(f'spdk_nvmet_tcp: {remote.shortname} {role}: '
                         f'{dev} -> {role_to_trid[role]}')

            old = int(_sh(remote, 'cat /proc/sys/vm/nr_hugepages'))
            old_hugepages[remote] = old
            want = max(old, hugepages_per_osd * len(role_to_trid))
            remote.run(args=[
                'echo', str(want), run.Raw('|'),
                'sudo', 'tee', '/proc/sys/vm/nr_hugepages',
            ])
            got = int(_sh(remote, 'cat /proc/sys/vm/nr_hugepages'))
            if got < want:
                raise RuntimeError(
                    f'spdk_nvmet_tcp: {remote.shortname} reserved {got} of '
                    f'{want} hugepages')
            ctx.spdk_nvmet_tcp[remote] = role_to_trid

        yield

    finally:
        for remote, nqns in exported.items():
            for nqn in nqns:
                log.info(f'spdk_nvmet_tcp: {remote.shortname}: removing {nqn}')
                _unexport(remote, nqn)
            # also drop hugepage files a crashed DPDK process left behind
            remote.run(args=[
                'sudo', 'rmdir', PORT, run.Raw(';'),
                'sudo', 'rm', '-rf',
                run.Raw('/dev/hugepages/spdk_pid*'),
                run.Raw('/var/run/dpdk/spdk_pid*'),
            ], check_status=False)
        for remote, old in old_hugepages.items():
            remote.run(args=[
                'echo', str(old), run.Raw('|'),
                'sudo', 'tee', '/proc/sys/vm/nr_hugepages',
            ], check_status=False)
        for remote in exported:
            ctx.spdk_nvmet_tcp.pop(remote, None)
