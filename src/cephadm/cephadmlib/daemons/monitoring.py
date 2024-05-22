import os

from typing import Dict, List, Tuple

from ..call_wrappers import call, CallVerbosity
from ..constants import (
    DEFAULT_ALERT_MANAGER_IMAGE,
    DEFAULT_GRAFANA_IMAGE,
    DEFAULT_LOKI_IMAGE,
    DEFAULT_NODE_EXPORTER_IMAGE,
    DEFAULT_PROMETHEUS_IMAGE,
    DEFAULT_PROMTAIL_IMAGE,
)
from ..container_daemon_form import ContainerDaemonForm, daemon_to_container
from ..container_types import CephContainer, extract_uid_gid
from ..context import CephadmContext
from ..context_getters import fetch_configs, fetch_meta
from ..daemon_form import register as register_daemon_form
from ..daemon_identity import DaemonIdentity
from ..deployment_utils import to_deployment_container
from ..exceptions import Error
from ..net_utils import get_fqdn, get_hostname, get_ip_addresses, wrap_ipv6


@register_daemon_form
class Monitoring(ContainerDaemonForm):
    """Define the configs for the monitoring containers"""

    port_map = {
        'prometheus': [
            9095
        ],  # Avoid default 9090, due to conflict with cockpit UI
        'node-exporter': [9100],
        'grafana': [3000],
        'alertmanager': [9093, 9094],
        'loki': [3100],
        'promtail': [9080],
    }

    components = {
        'prometheus': {
            'image': DEFAULT_PROMETHEUS_IMAGE,
            'cpus': '2',
            'memory': '4GB',
            'args': [
                '--config.file=/etc/prometheus/prometheus.yml',
                '--storage.tsdb.path=/prometheus',
            ],
            'config-json-files': [
                'prometheus.yml',
            ],
        },
        'loki': {
            'image': DEFAULT_LOKI_IMAGE,
            'cpus': '1',
            'memory': '1GB',
            'args': [
                '--config.file=/etc/loki/loki.yml',
            ],
            'config-json-files': ['loki.yml'],
        },
        'promtail': {
            'image': DEFAULT_PROMTAIL_IMAGE,
            'cpus': '1',
            'memory': '1GB',
            'args': [
                '--config.file=/etc/promtail/promtail.yml',
            ],
            'config-json-files': [
                'promtail.yml',
            ],
        },
        'node-exporter': {
            'image': DEFAULT_NODE_EXPORTER_IMAGE,
            'cpus': '1',
            'memory': '1GB',
            'args': ['--no-collector.timex'],
        },
        'grafana': {
            'image': DEFAULT_GRAFANA_IMAGE,
            'cpus': '2',
            'memory': '4GB',
            'args': [],
            'config-json-files': [
                'grafana.ini',
                'provisioning/datasources/ceph-dashboard.yml',
                'certs/cert_file',
                'certs/cert_key',
            ],
        },
        'alertmanager': {
            'image': DEFAULT_ALERT_MANAGER_IMAGE,
            'cpus': '2',
            'memory': '2GB',
            'args': [
                '--cluster.listen-address=:{}'.format(
                    port_map['alertmanager'][1]
                ),
            ],
            'config-json-files': [
                'alertmanager.yml',
            ],
            'config-json-args': [
                'peers',
            ],
        },
    }  # type: ignore

    @classmethod
    def for_daemon_type(cls, daemon_type: str) -> bool:
        return daemon_type in cls.components

    @staticmethod
    def get_version(ctx, container_id, daemon_type):
        # type: (CephadmContext, str, str) -> str
        """
        :param: daemon_type Either "prometheus", "alertmanager", "loki", "promtail" or "node-exporter"
        """
        assert daemon_type in (
            'prometheus',
            'alertmanager',
            'node-exporter',
            'loki',
            'promtail',
        )
        cmd = daemon_type.replace('-', '_')
        code = -1
        err = ''
        out = ''
        version = ''
        if daemon_type == 'alertmanager':
            for cmd in ['alertmanager', 'prometheus-alertmanager']:
                out, err, code = call(
                    ctx,
                    [
                        ctx.container_engine.path,
                        'exec',
                        container_id,
                        cmd,
                        '--version',
                    ],
                    verbosity=CallVerbosity.QUIET,
                )
                if code == 0:
                    break
            cmd = 'alertmanager'  # reset cmd for version extraction
        else:
            out, err, code = call(
                ctx,
                [
                    ctx.container_engine.path,
                    'exec',
                    container_id,
                    cmd,
                    '--version',
                ],
                verbosity=CallVerbosity.QUIET,
            )
        if code == 0:
            if err.startswith('%s, version ' % cmd):
                version = err.split(' ')[2]
            elif out.startswith('%s, version ' % cmd):
                version = out.split(' ')[2]
        return version

    @staticmethod
    def extract_uid_gid(
        ctx: CephadmContext, daemon_type: str
    ) -> Tuple[int, int]:
        if daemon_type == 'prometheus':
            uid, gid = extract_uid_gid(ctx, file_path='/etc/prometheus')
        elif daemon_type == 'node-exporter':
            uid, gid = 65534, 65534
        elif daemon_type == 'grafana':
            uid, gid = extract_uid_gid(ctx, file_path='/var/lib/grafana')
        elif daemon_type == 'loki':
            uid, gid = extract_uid_gid(ctx, file_path='/etc/loki')
        elif daemon_type == 'promtail':
            uid, gid = extract_uid_gid(ctx, file_path='/etc/promtail')
        elif daemon_type == 'alertmanager':
            uid, gid = extract_uid_gid(
                ctx, file_path=['/etc/alertmanager', '/etc/prometheus']
            )
        else:
            raise Error('{} not implemented yet'.format(daemon_type))
        return uid, gid

    def __init__(self, ctx: CephadmContext, ident: DaemonIdentity) -> None:
        self.ctx = ctx
        self._identity = ident

    @classmethod
    def create(
        cls, ctx: CephadmContext, ident: DaemonIdentity
    ) -> 'Monitoring':
        return cls(ctx, ident)

    @property
    def identity(self) -> DaemonIdentity:
        return self._identity

    def container(self, ctx: CephadmContext) -> CephContainer:
        self._prevalidate(ctx)
        ctr = daemon_to_container(ctx, self)
        return to_deployment_container(ctx, ctr)

    def uid_gid(self, ctx: CephadmContext) -> Tuple[int, int]:
        return self.extract_uid_gid(ctx, self.identity.daemon_type)

    def _prevalidate(self, ctx: CephadmContext) -> None:
        # before being refactored into a ContainerDaemonForm these checks were
        # done inside the deploy function. This was the only "family" of daemons
        # that performed these checks in that location
        daemon_type = self.identity.daemon_type
        config = fetch_configs(ctx)  # type: ignore
        required_files = self.components[daemon_type].get(
            'config-json-files', list()
        )
        required_args = self.components[daemon_type].get(
            'config-json-args', list()
        )
        if required_files:
            if not config or not all(c in config.get('files', {}).keys() for c in required_files):  # type: ignore
                raise Error(
                    '{} deployment requires config-json which must '
                    'contain file content for {}'.format(
                        daemon_type.capitalize(), ', '.join(required_files)
                    )
                )
        if required_args:
            if not config or not all(c in config.keys() for c in required_args):  # type: ignore
                raise Error(
                    '{} deployment requires config-json which must '
                    'contain arg for {}'.format(
                        daemon_type.capitalize(), ', '.join(required_args)
                    )
                )

    def get_daemon_args(self) -> List[str]:
        ctx = self.ctx
        daemon_type = self.identity.daemon_type
        metadata = self.components[daemon_type]
        r = list(metadata.get('args', []))
        # set ip and port to bind to for nodeexporter,alertmanager,prometheus
        if daemon_type not in ['grafana', 'loki', 'promtail']:
            ip = ''
            port = self.port_map[daemon_type][0]
            meta = fetch_meta(ctx)
            if meta:
                if 'ip' in meta and meta['ip']:
                    ip = meta['ip']
                if 'ports' in meta and meta['ports']:
                    port = meta['ports'][0]
            if daemon_type == 'prometheus':
                config = fetch_configs(ctx)
                ip_to_bind_to = config.get('ip_to_bind_to', '')
                if ip_to_bind_to:
                    ip = ip_to_bind_to
                retention_time = config.get('retention_time', '15d')
                retention_size = config.get(
                    'retention_size', '0'
                )  # default to disabled
                r += [f'--storage.tsdb.retention.time={retention_time}']
                r += [f'--storage.tsdb.retention.size={retention_size}']
                scheme = 'http'
                host = get_fqdn()
                # in case host is not an fqdn then we use the IP to
                # avoid producing a broken web.external-url link
                if '.' not in host:
                    ipv4_addrs, ipv6_addrs = get_ip_addresses(get_hostname())
                    # use the first ipv4 (if any) otherwise use the first ipv6
                    addr = next(iter(ipv4_addrs or ipv6_addrs), None)
                    host = wrap_ipv6(addr) if addr else host
                r += [f'--web.external-url={scheme}://{host}:{port}']
            r += [f'--web.listen-address={ip}:{port}']
        if daemon_type == 'alertmanager':
            config = fetch_configs(ctx)
            peers = config.get('peers', list())  # type: ignore
            for peer in peers:
                r += ['--cluster.peer={}'.format(peer)]
            try:
                r += [f'--web.config.file={config["web_config"]}']
            except KeyError:
                pass
            # some alertmanager, by default, look elsewhere for a config
            r += ['--config.file=/etc/alertmanager/alertmanager.yml']
        if daemon_type == 'promtail':
            r += ['--config.expand-env']
        if daemon_type == 'prometheus':
            config = fetch_configs(ctx)
            try:
                r += [f'--web.config.file={config["web_config"]}']
            except KeyError:
                pass
        if daemon_type == 'node-exporter':
            config = fetch_configs(ctx)
            try:
                r += [f'--web.config.file={config["web_config"]}']
            except KeyError:
                pass
            r += [
                '--path.procfs=/host/proc',
                '--path.sysfs=/host/sys',
                '--path.rootfs=/rootfs',
            ]
        return r

    def _get_container_mounts(self, data_dir: str) -> Dict[str, str]:
        ctx = self.ctx
        daemon_type = self.identity.daemon_type
        mounts: Dict[str, str] = {}
        log_dir = os.path.join(ctx.log_dir, self.identity.fsid)
        if daemon_type == 'prometheus':
            mounts[
                os.path.join(data_dir, 'etc/prometheus')
            ] = '/etc/prometheus:Z'
            mounts[os.path.join(data_dir, 'data')] = '/prometheus:Z'
        elif daemon_type == 'loki':
            mounts[os.path.join(data_dir, 'etc/loki')] = '/etc/loki:Z'
            mounts[os.path.join(data_dir, 'data')] = '/loki:Z'
        elif daemon_type == 'promtail':
            mounts[os.path.join(data_dir, 'etc/promtail')] = '/etc/promtail:Z'
            mounts[log_dir] = '/var/log/ceph:z'
            mounts[os.path.join(data_dir, 'data')] = '/promtail:Z'
        elif daemon_type == 'node-exporter':
            mounts[
                os.path.join(data_dir, 'etc/node-exporter')
            ] = '/etc/node-exporter:Z'
            mounts['/proc'] = '/host/proc:ro'
            mounts['/sys'] = '/host/sys:ro'
            mounts['/'] = '/rootfs:ro'
        elif daemon_type == 'grafana':
            mounts[
                os.path.join(data_dir, 'etc/grafana/grafana.ini')
            ] = '/etc/grafana/grafana.ini:Z'
            mounts[
                os.path.join(data_dir, 'etc/grafana/provisioning/datasources')
            ] = '/etc/grafana/provisioning/datasources:Z'
            mounts[
                os.path.join(data_dir, 'etc/grafana/provisioning/dashboards')
            ] = '/etc/grafana/provisioning/dashboards:Z'
            mounts[
                os.path.join(data_dir, 'etc/grafana/certs')
            ] = '/etc/grafana/certs:Z'
            mounts[
                os.path.join(data_dir, 'data/grafana.db')
            ] = '/var/lib/grafana/grafana.db:Z'
        elif daemon_type == 'alertmanager':
            mounts[
                os.path.join(data_dir, 'etc/alertmanager')
            ] = '/etc/alertmanager:Z'
        return mounts

    def customize_container_mounts(
        self, ctx: CephadmContext, mounts: Dict[str, str]
    ) -> None:
        data_dir = self.identity.data_dir(ctx.data_dir)
        mounts.update(self._get_container_mounts(data_dir))

    def customize_container_args(
        self, ctx: CephadmContext, args: List[str]
    ) -> None:
        uid, _ = self.uid_gid(ctx)
        monitoring_args = [
            '--user',
            str(uid),
            # FIXME: disable cpu/memory limits for the time being (not supported
            # by ubuntu 18.04 kernel!)
        ]
        args.extend(monitoring_args)
        if self.identity.daemon_type == 'node-exporter':
            # in order to support setting '--path.procfs=/host/proc','--path.sysfs=/host/sys',
            # '--path.rootfs=/rootfs' for node-exporter we need to disable selinux separation
            # between the node-exporter container and the host to avoid selinux denials
            args.extend(['--security-opt', 'label=disable'])

    def customize_process_args(
        self, ctx: CephadmContext, args: List[str]
    ) -> None:
        args.extend(self.get_daemon_args())

    def default_entrypoint(self) -> str:
        return ''
