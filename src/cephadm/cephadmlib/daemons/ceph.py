import logging
import os

from typing import Any, Dict, List, Optional, Tuple, Union

from ..container_daemon_form import ContainerDaemonForm, daemon_to_container
from ..container_types import CephContainer, extract_uid_gid
from ..context_getters import (
    fetch_configs,
    get_config_and_keyring,
    should_log_to_journald,
)
from ..daemon_form import register as register_daemon_form
from ..daemon_identity import DaemonIdentity
from ..constants import DEFAULT_IMAGE
from ..context import CephadmContext
from ..deployment_utils import to_deployment_container
from ..exceptions import Error
from ..file_utils import make_run_dir, pathify
from ..host_facts import HostFacts
from ..logging import Highlight
from ..net_utils import get_hostname, get_ip_addresses


logger = logging.getLogger()


@register_daemon_form
class Ceph(ContainerDaemonForm):
    _daemons = (
        'mon',
        'mgr',
        'osd',
        'mds',
        'rgw',
        'rbd-mirror',
        'crash',
        'cephfs-mirror',
    )

    @classmethod
    def for_daemon_type(cls, daemon_type: str) -> bool:
        # TODO: figure out a way to un-special-case osd
        return daemon_type in cls._daemons and daemon_type != 'osd'

    def __init__(self, ctx: CephadmContext, ident: DaemonIdentity) -> None:
        self.ctx = ctx
        self._identity = ident
        self.user_supplied_config = False

    @classmethod
    def create(cls, ctx: CephadmContext, ident: DaemonIdentity) -> 'Ceph':
        return cls(ctx, ident)

    @property
    def identity(self) -> DaemonIdentity:
        return self._identity

    def firewall_service_name(self) -> str:
        if self.identity.daemon_type == 'mon':
            return 'ceph-mon'
        elif self.identity.daemon_type in ['mgr', 'mds']:
            return 'ceph'
        return ''

    def container(self, ctx: CephadmContext) -> CephContainer:
        # previous to being a ContainerDaemonForm, this call to create the
        # var-run directory was hard coded in the deploy path. Eventually, it
        # would be good to move this somwhere cleaner and avoid needing to know
        # the uid/gid here.
        uid, gid = self.uid_gid(ctx)
        make_run_dir(ctx.fsid, uid, gid)

        # mon and osd need privileged in order for libudev to query devices
        privileged = self.identity.daemon_type in ['mon', 'osd']
        ctr = daemon_to_container(ctx, self, privileged=privileged)
        ctr = to_deployment_container(ctx, ctr)
        config_json = fetch_configs(ctx)
        if self.identity.daemon_type == 'mon' and config_json is not None:
            if 'crush_location' in config_json:
                c_loc = config_json['crush_location']
                # was originally "c.args.extend(['--set-crush-location', c_loc])"
                # but that doesn't seem to persist in the object after it's passed
                # in further function calls
                ctr.args = ctr.args + ['--set-crush-location', c_loc]
        return ctr

    _uid_gid: Optional[Tuple[int, int]] = None

    def uid_gid(self, ctx: CephadmContext) -> Tuple[int, int]:
        if self._uid_gid is None:
            self._uid_gid = extract_uid_gid(ctx)
        return self._uid_gid

    def config_and_keyring(
        self, ctx: CephadmContext
    ) -> Tuple[Optional[str], Optional[str]]:
        return get_config_and_keyring(ctx)

    def get_daemon_args(self) -> List[str]:
        if self.identity.daemon_type == 'crash':
            return []
        r = [
            '--setuser',
            'ceph',
            '--setgroup',
            'ceph',
            '--default-log-to-file=false',
        ]
        log_to_journald = should_log_to_journald(self.ctx)
        if log_to_journald:
            r += [
                '--default-log-to-journald=true',
                '--default-log-to-stderr=false',
            ]
        else:
            r += [
                '--default-log-to-stderr=true',
                '--default-log-stderr-prefix=debug ',
            ]
        if self.identity.daemon_type == 'mon':
            r += [
                '--default-mon-cluster-log-to-file=false',
            ]
            if log_to_journald:
                r += [
                    '--default-mon-cluster-log-to-journald=true',
                    '--default-mon-cluster-log-to-stderr=false',
                ]
            else:
                r += ['--default-mon-cluster-log-to-stderr=true']
        return r

    @staticmethod
    def get_ceph_mounts(
        ctx: CephadmContext,
        ident: DaemonIdentity,
        no_config: bool = False,
    ) -> Dict[str, str]:
        # Warning: This is a hack done for more expedient refactoring
        mounts = get_ceph_mounts_for_type(ctx, ident.fsid, ident.daemon_type)
        data_dir = ident.data_dir(ctx.data_dir)
        if ident.daemon_type == 'rgw':
            cdata_dir = '/var/lib/ceph/radosgw/ceph-rgw.%s' % (
                ident.daemon_id
            )
        else:
            cdata_dir = '/var/lib/ceph/%s/ceph-%s' % (
                ident.daemon_type,
                ident.daemon_id,
            )
        if ident.daemon_type != 'crash':
            mounts[data_dir] = cdata_dir + ':z'
        if not no_config:
            mounts[data_dir + '/config'] = '/etc/ceph/ceph.conf:z'
        if ident.daemon_type in [
            'rbd-mirror',
            'cephfs-mirror',
            'crash',
            'ceph-exporter',
        ]:
            # these do not search for their keyrings in a data directory
            mounts[
                data_dir + '/keyring'
            ] = '/etc/ceph/ceph.client.%s.%s.keyring' % (
                ident.daemon_type,
                ident.daemon_id,
            )
        return mounts

    def customize_container_mounts(
        self, ctx: CephadmContext, mounts: Dict[str, str]
    ) -> None:
        no_config = bool(
            getattr(ctx, 'config', None) and self.user_supplied_config
        )
        cm = self.get_ceph_mounts(
            ctx,
            self.identity,
            no_config=no_config,
        )
        mounts.update(cm)

    def customize_container_args(
        self, ctx: CephadmContext, args: List[str]
    ) -> None:
        args.append(ctx.container_engine.unlimited_pids_option)

    def customize_process_args(
        self, ctx: CephadmContext, args: List[str]
    ) -> None:
        ident = self.identity
        if ident.daemon_type == 'rgw':
            name = 'client.rgw.%s' % ident.daemon_id
        elif ident.daemon_type == 'rbd-mirror':
            name = 'client.rbd-mirror.%s' % ident.daemon_id
        elif ident.daemon_type == 'cephfs-mirror':
            name = 'client.cephfs-mirror.%s' % ident.daemon_id
        elif ident.daemon_type == 'crash':
            name = 'client.crash.%s' % ident.daemon_id
        elif ident.daemon_type in ['mon', 'mgr', 'mds', 'osd']:
            name = ident.daemon_name
        else:
            raise ValueError(ident)
        args.extend(['-n', name])
        if ident.daemon_type != 'crash':
            args.append('-f')
        args.extend(self.get_daemon_args())

    def customize_container_envs(
        self, ctx: CephadmContext, envs: List[str]
    ) -> None:
        envs.append('TCMALLOC_MAX_TOTAL_THREAD_CACHE_BYTES=134217728')

    def default_entrypoint(self) -> str:
        ep = {
            'rgw': '/usr/bin/radosgw',
            'rbd-mirror': '/usr/bin/rbd-mirror',
            'cephfs-mirror': '/usr/bin/cephfs-mirror',
        }
        daemon_type = self.identity.daemon_type
        return ep.get(daemon_type) or f'/usr/bin/ceph-{daemon_type}'


@register_daemon_form
class OSD(Ceph):
    @classmethod
    def for_daemon_type(cls, daemon_type: str) -> bool:
        # TODO: figure out a way to un-special-case osd
        return daemon_type == 'osd'

    def __init__(
        self,
        ctx: CephadmContext,
        ident: DaemonIdentity,
        osd_fsid: Optional[str] = None,
    ) -> None:
        super().__init__(ctx, ident)
        self._osd_fsid = osd_fsid

    @classmethod
    def create(cls, ctx: CephadmContext, ident: DaemonIdentity) -> 'OSD':
        osd_fsid = getattr(ctx, 'osd_fsid', None)
        if osd_fsid is None:
            logger.info(
                'Creating an OSD daemon form without an OSD FSID value'
            )
        return cls(ctx, ident, osd_fsid)

    @staticmethod
    def get_sysctl_settings() -> List[str]:
        return [
            '# allow a large number of OSDs',
            'fs.aio-max-nr = 1048576',
            'kernel.pid_max = 4194304',
        ]

    def firewall_service_name(self) -> str:
        return 'ceph'

    @property
    def osd_fsid(self) -> Optional[str]:
        return self._osd_fsid


@register_daemon_form
class CephExporter(ContainerDaemonForm):
    """Defines a Ceph exporter container"""

    daemon_type = 'ceph-exporter'
    entrypoint = '/usr/bin/ceph-exporter'
    DEFAULT_PORT = 9926
    port_map = {
        'ceph-exporter': DEFAULT_PORT,
    }

    @classmethod
    def for_daemon_type(cls, daemon_type: str) -> bool:
        return cls.daemon_type == daemon_type

    def __init__(
        self,
        ctx: CephadmContext,
        fsid: str,
        daemon_id: Union[int, str],
        config_json: Dict[str, Any],
        image: str = DEFAULT_IMAGE,
    ) -> None:
        self.ctx = ctx
        self.fsid = fsid
        self.daemon_id = daemon_id
        self.image = image

        self.sock_dir = config_json.get('sock-dir', '/var/run/ceph/')
        _, ipv6_addrs = get_ip_addresses(get_hostname())
        addrs = '::' if ipv6_addrs else '0.0.0.0'
        self.addrs = config_json.get('addrs', addrs)
        self.port = config_json.get('port', self.DEFAULT_PORT)
        self.prio_limit = config_json.get('prio-limit', 5)
        self.stats_period = config_json.get('stats-period', 5)

    @classmethod
    def init(
        cls, ctx: CephadmContext, fsid: str, daemon_id: Union[int, str]
    ) -> 'CephExporter':
        return cls(ctx, fsid, daemon_id, fetch_configs(ctx), ctx.image)

    @classmethod
    def create(
        cls, ctx: CephadmContext, ident: DaemonIdentity
    ) -> 'CephExporter':
        return cls.init(ctx, ident.fsid, ident.daemon_id)

    @property
    def identity(self) -> DaemonIdentity:
        return DaemonIdentity(self.fsid, self.daemon_type, self.daemon_id)

    def get_daemon_args(self) -> List[str]:
        args = [
            f'--sock-dir={self.sock_dir}',
            f'--addrs={self.addrs}',
            f'--port={self.port}',
            f'--prio-limit={self.prio_limit}',
            f'--stats-period={self.stats_period}',
        ]
        return args

    def validate(self) -> None:
        if not os.path.isdir(self.sock_dir):
            raise Error(
                f'Desired sock dir for ceph-exporter is not directory: {self.sock_dir}'
            )

    def container(self, ctx: CephadmContext) -> CephContainer:
        ctr = daemon_to_container(ctx, self)
        return to_deployment_container(ctx, ctr)

    def uid_gid(self, ctx: CephadmContext) -> Tuple[int, int]:
        return extract_uid_gid(ctx)

    def config_and_keyring(
        self, ctx: CephadmContext
    ) -> Tuple[Optional[str], Optional[str]]:
        return get_config_and_keyring(ctx)

    def customize_container_mounts(
        self, ctx: CephadmContext, mounts: Dict[str, str]
    ) -> None:
        cm = Ceph.get_ceph_mounts(ctx, self.identity)
        mounts.update(cm)

    def customize_process_args(
        self, ctx: CephadmContext, args: List[str]
    ) -> None:
        name = 'client.ceph-exporter.%s' % self.identity.daemon_id
        args.extend(['-n', name, '-f'])
        args.extend(self.get_daemon_args())

    def customize_container_args(
        self, ctx: CephadmContext, args: List[str]
    ) -> None:
        args.append(ctx.container_engine.unlimited_pids_option)

    def customize_container_envs(
        self, ctx: CephadmContext, envs: List[str]
    ) -> None:
        envs.append('TCMALLOC_MAX_TOTAL_THREAD_CACHE_BYTES=134217728')

    def default_entrypoint(self) -> str:
        return self.entrypoint

    def prepare_data_dir(self, data_dir: str, uid: int, gid: int) -> None:
        if not os.path.exists(self.sock_dir):
            os.mkdir(self.sock_dir)
        # part of validation is for the sock dir, so we postpone
        # it until now
        self.validate()


def get_ceph_mounts_for_type(
    ctx: CephadmContext, fsid: str, daemon_type: str
) -> Dict[str, str]:
    """The main implementation of get_container_mounts_for_type minus the call
    to _update_podman_mounts so that this can be called from
    get_container_mounts.
    """
    mounts = dict()

    if daemon_type in ceph_daemons():
        if fsid:
            run_path = os.path.join('/var/run/ceph', fsid)
            if os.path.exists(run_path):
                mounts[run_path] = '/var/run/ceph:z'
            log_dir = os.path.join(ctx.log_dir, fsid)
            mounts[log_dir] = '/var/log/ceph:z'
            crash_dir = '/var/lib/ceph/%s/crash' % fsid
            if os.path.exists(crash_dir):
                mounts[crash_dir] = '/var/lib/ceph/crash:z'
            if daemon_type != 'crash' and should_log_to_journald(ctx):
                journald_sock_dir = '/run/systemd/journal'
                mounts[journald_sock_dir] = journald_sock_dir

    if daemon_type in ['mon', 'osd', 'clusterless-ceph-volume']:
        mounts['/dev'] = '/dev'  # FIXME: narrow this down?
        mounts['/run/udev'] = '/run/udev'
    if daemon_type in ['osd', 'clusterless-ceph-volume']:
        mounts['/sys'] = '/sys'  # for numa.cc, pick_address, cgroups, ...
        mounts['/run/lvm'] = '/run/lvm'
        mounts['/run/lock/lvm'] = '/run/lock/lvm'
    if daemon_type == 'osd':
        # selinux-policy in the container may not match the host.
        if HostFacts(ctx).selinux_enabled:
            cluster_dir = f'{ctx.data_dir}/{fsid}'
            selinux_folder = f'{cluster_dir}/selinux'
            if os.path.exists(cluster_dir):
                if not os.path.exists(selinux_folder):
                    os.makedirs(selinux_folder, mode=0o755)
                mounts[selinux_folder] = '/sys/fs/selinux:ro'
            else:
                logger.error(
                    f'Cluster direcotry {cluster_dir} does not exist.'
                )
        mounts['/'] = '/rootfs'

    try:
        if (
            ctx.shared_ceph_folder
        ):  # make easy manager modules/ceph-volume development
            ceph_folder = pathify(ctx.shared_ceph_folder)
            if os.path.exists(ceph_folder):
                cephadm_binary = ceph_folder + '/src/cephadm/cephadm'
                if not os.path.exists(pathify(cephadm_binary)):
                    raise Error(
                        "cephadm binary does not exist. Please run './build.sh cephadm' from ceph/src/cephadm/ directory."
                    )
                mounts[cephadm_binary] = '/usr/sbin/cephadm'
                mounts[
                    ceph_folder + '/src/ceph-volume/ceph_volume'
                ] = '/usr/lib/python3.6/site-packages/ceph_volume'
                mounts[
                    ceph_folder + '/src/pybind/mgr'
                ] = '/usr/share/ceph/mgr'
                mounts[
                    ceph_folder + '/src/python-common/ceph'
                ] = '/usr/lib/python3.6/site-packages/ceph'
                mounts[
                    ceph_folder + '/monitoring/ceph-mixin/dashboards_out'
                ] = '/etc/grafana/dashboards/ceph-dashboard'
                mounts[
                    ceph_folder
                    + '/monitoring/ceph-mixin/prometheus_alerts.yml'
                ] = '/etc/prometheus/ceph/ceph_default_alerts.yml'
            else:
                logger.error(
                    'Ceph shared source folder does not exist.',
                    extra=Highlight.FAILURE.extra(),
                )
    except AttributeError:
        pass
    return mounts


def ceph_daemons() -> List[str]:
    """A legacy method that returns a list of all daemon types considered ceph
    daemons.
    """
    cds = list(Ceph._daemons)
    cds.append(CephExporter.daemon_type)
    return cds
