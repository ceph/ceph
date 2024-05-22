import asyncio
import concurrent
import json
import errno
import ipaddress
import logging
import re
import shlex
from collections import defaultdict
from configparser import ConfigParser
from contextlib import contextmanager
from functools import wraps
from tempfile import TemporaryDirectory, NamedTemporaryFile
from urllib.error import HTTPError
from threading import Event

from cephadm.service_discovery import ServiceDiscovery

from ceph.deployment.service_spec import PrometheusSpec

import string
from typing import List, Dict, Optional, Callable, Tuple, TypeVar, \
    Any, Set, TYPE_CHECKING, cast, NamedTuple, Sequence, Type, \
    Awaitable, Iterator

import datetime
import os
import random
import multiprocessing.pool
import subprocess
from prettytable import PrettyTable

from ceph.deployment import inventory
from ceph.deployment.drive_group import DriveGroupSpec
from ceph.deployment.service_spec import \
    ServiceSpec, PlacementSpec, \
    HostPlacementSpec, IngressSpec, \
    TunedProfileSpec, IscsiServiceSpec
from ceph.utils import str_to_datetime, datetime_to_str, datetime_now
from cephadm.serve import CephadmServe
from cephadm.services.cephadmservice import CephadmDaemonDeploySpec
from cephadm.http_server import CephadmHttpServer
from cephadm.agent import CephadmAgentHelpers


from mgr_module import (
    MgrModule,
    HandleCommandResult,
    Option,
    NotifyType,
    MonCommandFailed,
)
from mgr_util import build_url
import orchestrator
from orchestrator.module import to_format, Format

from orchestrator import OrchestratorError, OrchestratorValidationError, HostSpec, \
    CLICommandMeta, DaemonDescription, DaemonDescriptionStatus, handle_orch_error, \
    service_to_daemon_types
from orchestrator._interface import GenericSpec
from orchestrator._interface import daemon_type_to_service

from . import utils
from . import ssh
from .migrations import Migrations
from .services.cephadmservice import MonService, MgrService, MdsService, RgwService, \
    RbdMirrorService, CrashService, CephadmService, CephfsMirrorService, CephadmAgent, \
    CephExporterService
from .services.ingress import IngressService
from .services.container import CustomContainerService
from .services.iscsi import IscsiService
from .services.nvmeof import NvmeofService
from .services.nfs import NFSService
from .services.osd import OSDRemovalQueue, OSDService, OSD, NotFoundError
from .services.monitoring import GrafanaService, AlertmanagerService, PrometheusService, \
    NodeExporterService, SNMPGatewayService, LokiService, PromtailService
from .services.jaeger import ElasticSearchService, JaegerAgentService, JaegerCollectorService, JaegerQueryService
from .services.node_proxy import NodeProxy
from .services.smb import SMBService
from .schedule import HostAssignment
from .inventory import Inventory, SpecStore, HostCache, AgentCache, EventStore, \
    ClientKeyringStore, ClientKeyringSpec, TunedProfileStore, NodeProxyCache
from .upgrade import CephadmUpgrade
from .template import TemplateMgr
from .utils import CEPH_IMAGE_TYPES, RESCHEDULE_FROM_OFFLINE_HOSTS_TYPES, forall_hosts, \
    cephadmNoImage, CEPH_UPGRADE_ORDER, SpecialHostLabels
from .configchecks import CephadmConfigChecks
from .offline_watcher import OfflineHostWatcher
from .tuned_profiles import TunedProfileUtils

try:
    import asyncssh
except ImportError as e:
    asyncssh = None  # type: ignore
    asyncssh_import_error = str(e)

logger = logging.getLogger(__name__)

T = TypeVar('T')

DEFAULT_SSH_CONFIG = """
Host *
  User root
  StrictHostKeyChecking no
  UserKnownHostsFile /dev/null
  ConnectTimeout=30
"""

# cherrypy likes to sys.exit on error.  don't let it take us down too!


def os_exit_noop(status: int) -> None:
    pass


os._exit = os_exit_noop   # type: ignore


# Default container images -----------------------------------------------------
DEFAULT_IMAGE = 'quay.io/ceph/ceph'
DEFAULT_PROMETHEUS_IMAGE = 'quay.io/prometheus/prometheus:v2.43.0'
DEFAULT_NODE_EXPORTER_IMAGE = 'quay.io/prometheus/node-exporter:v1.5.0'
DEFAULT_NVMEOF_IMAGE = 'quay.io/ceph/nvmeof:1.0.0'
DEFAULT_LOKI_IMAGE = 'docker.io/grafana/loki:2.4.0'
DEFAULT_PROMTAIL_IMAGE = 'docker.io/grafana/promtail:2.4.0'
DEFAULT_ALERT_MANAGER_IMAGE = 'quay.io/prometheus/alertmanager:v0.25.0'
DEFAULT_GRAFANA_IMAGE = 'quay.io/ceph/grafana:9.4.12'
DEFAULT_HAPROXY_IMAGE = 'quay.io/ceph/haproxy:2.3'
DEFAULT_KEEPALIVED_IMAGE = 'quay.io/ceph/keepalived:2.2.4'
DEFAULT_SNMP_GATEWAY_IMAGE = 'docker.io/maxwo/snmp-notifier:v1.2.1'
DEFAULT_ELASTICSEARCH_IMAGE = 'quay.io/omrizeneva/elasticsearch:6.8.23'
DEFAULT_JAEGER_COLLECTOR_IMAGE = 'quay.io/jaegertracing/jaeger-collector:1.29'
DEFAULT_JAEGER_AGENT_IMAGE = 'quay.io/jaegertracing/jaeger-agent:1.29'
DEFAULT_JAEGER_QUERY_IMAGE = 'quay.io/jaegertracing/jaeger-query:1.29'
DEFAULT_SAMBA_IMAGE = 'quay.io/samba.org/samba-server:devbuilds-centos-amd64'
# ------------------------------------------------------------------------------


def host_exists(hostname_position: int = 1) -> Callable:
    """Check that a hostname exists in the inventory"""
    def inner(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            this = args[0]  # self object
            hostname = args[hostname_position]
            if hostname not in this.cache.get_hosts():
                candidates = ','.join([h for h in this.cache.get_hosts() if h.startswith(hostname)])
                help_msg = f"Did you mean {candidates}?" if candidates else ""
                raise OrchestratorError(
                    f"Cannot find host '{hostname}' in the inventory. {help_msg}")

            return func(*args, **kwargs)
        return wrapper
    return inner


class CephadmOrchestrator(orchestrator.Orchestrator, MgrModule,
                          metaclass=CLICommandMeta):

    _STORE_HOST_PREFIX = "host"

    instance = None
    NOTIFY_TYPES = [NotifyType.mon_map, NotifyType.pg_summary]
    NATIVE_OPTIONS = []  # type: List[Any]
    MODULE_OPTIONS = [
        Option(
            'ssh_config_file',
            type='str',
            default=None,
            desc='customized SSH config file to connect to managed hosts',
        ),
        Option(
            'device_cache_timeout',
            type='secs',
            default=30 * 60,
            desc='seconds to cache device inventory',
        ),
        Option(
            'device_enhanced_scan',
            type='bool',
            default=False,
            desc='Use libstoragemgmt during device scans',
        ),
        Option(
            'inventory_list_all',
            type='bool',
            default=False,
            desc='Whether ceph-volume inventory should report '
            'more devices (mostly mappers (LVs / mpaths), partitions...)',
        ),
        Option(
            'daemon_cache_timeout',
            type='secs',
            default=10 * 60,
            desc='seconds to cache service (daemon) inventory',
        ),
        Option(
            'facts_cache_timeout',
            type='secs',
            default=1 * 60,
            desc='seconds to cache host facts data',
        ),
        Option(
            'host_check_interval',
            type='secs',
            default=10 * 60,
            desc='how frequently to perform a host check',
        ),
        Option(
            'mode',
            type='str',
            enum_allowed=['root', 'cephadm-package'],
            default='root',
            desc='mode for remote execution of cephadm',
        ),
        Option(
            'container_image_base',
            default=DEFAULT_IMAGE,
            desc='Container image name, without the tag',
            runtime=True,
        ),
        Option(
            'container_image_prometheus',
            default=DEFAULT_PROMETHEUS_IMAGE,
            desc='Prometheus container image',
        ),
        Option(
            'container_image_nvmeof',
            default=DEFAULT_NVMEOF_IMAGE,
            desc='Nvme-of container image',
        ),
        Option(
            'container_image_grafana',
            default=DEFAULT_GRAFANA_IMAGE,
            desc='Prometheus container image',
        ),
        Option(
            'container_image_alertmanager',
            default=DEFAULT_ALERT_MANAGER_IMAGE,
            desc='Prometheus container image',
        ),
        Option(
            'container_image_node_exporter',
            default=DEFAULT_NODE_EXPORTER_IMAGE,
            desc='Prometheus container image',
        ),
        Option(
            'container_image_loki',
            default=DEFAULT_LOKI_IMAGE,
            desc='Loki container image',
        ),
        Option(
            'container_image_promtail',
            default=DEFAULT_PROMTAIL_IMAGE,
            desc='Promtail container image',
        ),
        Option(
            'container_image_haproxy',
            default=DEFAULT_HAPROXY_IMAGE,
            desc='HAproxy container image',
        ),
        Option(
            'container_image_keepalived',
            default=DEFAULT_KEEPALIVED_IMAGE,
            desc='Keepalived container image',
        ),
        Option(
            'container_image_snmp_gateway',
            default=DEFAULT_SNMP_GATEWAY_IMAGE,
            desc='SNMP Gateway container image',
        ),
        Option(
            'container_image_elasticsearch',
            default=DEFAULT_ELASTICSEARCH_IMAGE,
            desc='elasticsearch container image',
        ),
        Option(
            'container_image_jaeger_agent',
            default=DEFAULT_JAEGER_AGENT_IMAGE,
            desc='Jaeger agent container image',
        ),
        Option(
            'container_image_jaeger_collector',
            default=DEFAULT_JAEGER_COLLECTOR_IMAGE,
            desc='Jaeger collector container image',
        ),
        Option(
            'container_image_jaeger_query',
            default=DEFAULT_JAEGER_QUERY_IMAGE,
            desc='Jaeger query container image',
        ),
        Option(
            'container_image_samba',
            default=DEFAULT_SAMBA_IMAGE,
            desc='Samba/SMB container image',
        ),
        Option(
            'warn_on_stray_hosts',
            type='bool',
            default=True,
            desc='raise a health warning if daemons are detected on a host '
            'that is not managed by cephadm',
        ),
        Option(
            'warn_on_stray_daemons',
            type='bool',
            default=True,
            desc='raise a health warning if daemons are detected '
            'that are not managed by cephadm',
        ),
        Option(
            'warn_on_failed_host_check',
            type='bool',
            default=True,
            desc='raise a health warning if the host check fails',
        ),
        Option(
            'log_to_cluster',
            type='bool',
            default=True,
            desc='log to the "cephadm" cluster log channel"',
        ),
        Option(
            'allow_ptrace',
            type='bool',
            default=False,
            desc='allow SYS_PTRACE capability on ceph containers',
            long_desc='The SYS_PTRACE capability is needed to attach to a '
            'process with gdb or strace.  Enabling this options '
            'can allow debugging daemons that encounter problems '
            'at runtime.',
        ),
        Option(
            'container_init',
            type='bool',
            default=True,
            desc='Run podman/docker with `--init`'
        ),
        Option(
            'prometheus_alerts_path',
            type='str',
            default='/etc/prometheus/ceph/ceph_default_alerts.yml',
            desc='location of alerts to include in prometheus deployments',
        ),
        Option(
            'grafana_dashboards_path',
            type='str',
            default='/etc/grafana/dashboards/ceph-dashboard/',
            desc='location of dashboards to include in grafana deployments',
        ),
        Option(
            'migration_current',
            type='int',
            default=None,
            desc='internal - do not modify',
            # used to track spec and other data migrations.
        ),
        Option(
            'config_dashboard',
            type='bool',
            default=True,
            desc='manage configs like API endpoints in Dashboard.'
        ),
        Option(
            'manage_etc_ceph_ceph_conf',
            type='bool',
            default=False,
            desc='Manage and own /etc/ceph/ceph.conf on the hosts.',
        ),
        Option(
            'manage_etc_ceph_ceph_conf_hosts',
            type='str',
            default='*',
            desc='PlacementSpec describing on which hosts to manage /etc/ceph/ceph.conf',
        ),
        # not used anymore
        Option(
            'registry_url',
            type='str',
            default=None,
            desc='Registry url for login purposes. This is not the default registry'
        ),
        Option(
            'registry_username',
            type='str',
            default=None,
            desc='Custom repository username. Only used for logging into a registry.'
        ),
        Option(
            'registry_password',
            type='str',
            default=None,
            desc='Custom repository password. Only used for logging into a registry.'
        ),
        ####
        Option(
            'registry_insecure',
            type='bool',
            default=False,
            desc='Registry is to be considered insecure (no TLS available). Only for development purposes.'
        ),
        Option(
            'use_repo_digest',
            type='bool',
            default=True,
            desc='Automatically convert image tags to image digest. Make sure all daemons use the same image',
        ),
        Option(
            'config_checks_enabled',
            type='bool',
            default=False,
            desc='Enable or disable the cephadm configuration analysis',
        ),
        Option(
            'default_registry',
            type='str',
            default='docker.io',
            desc='Search-registry to which we should normalize unqualified image names. '
                 'This is not the default registry',
        ),
        Option(
            'max_count_per_host',
            type='int',
            default=10,
            desc='max number of daemons per service per host',
        ),
        Option(
            'autotune_memory_target_ratio',
            type='float',
            default=.7,
            desc='ratio of total system memory to divide amongst autotuned daemons'
        ),
        Option(
            'autotune_interval',
            type='secs',
            default=10 * 60,
            desc='how frequently to autotune daemon memory'
        ),
        Option(
            'use_agent',
            type='bool',
            default=False,
            desc='Use cephadm agent on each host to gather and send metadata'
        ),
        Option(
            'agent_refresh_rate',
            type='secs',
            default=20,
            desc='How often agent on each host will try to gather and send metadata'
        ),
        Option(
            'agent_starting_port',
            type='int',
            default=4721,
            desc='First port agent will try to bind to (will also try up to next 1000 subsequent ports if blocked)'
        ),
        Option(
            'agent_down_multiplier',
            type='float',
            default=3.0,
            desc='Multiplied by agent refresh rate to calculate how long agent must not report before being marked down'
        ),
        Option(
            'hw_monitoring',
            type='bool',
            default=False,
            desc='Deploy hw monitoring daemon on every host.'
        ),
        Option(
            'max_osd_draining_count',
            type='int',
            default=10,
            desc='max number of osds that will be drained simultaneously when osds are removed'
        ),
        Option(
            'service_discovery_port',
            type='int',
            default=8765,
            desc='cephadm service discovery port'
        ),
        Option(
            'cgroups_split',
            type='bool',
            default=True,
            desc='Pass --cgroups=split when cephadm creates containers (currently podman only)'
        ),
        Option(
            'log_refresh_metadata',
            type='bool',
            default=False,
            desc='Log all refresh metadata. Includes daemon, device, and host info collected regularly. Only has effect if logging at debug level'
        ),
        Option(
            'secure_monitoring_stack',
            type='bool',
            default=False,
            desc='Enable TLS security for all the monitoring stack daemons'
        ),
        Option(
            'default_cephadm_command_timeout',
            type='int',
            default=15 * 60,
            desc='Default timeout applied to cephadm commands run directly on '
            'the host (in seconds)'
        ),
        Option(
            'cephadm_log_destination',
            type='str',
            default='',
            desc="Destination for cephadm command's persistent logging",
            enum_allowed=['file', 'syslog', 'file,syslog'],
        ),
        Option(
            'oob_default_addr',
            type='str',
            default='169.254.1.1',
            desc="Default address for RedFish API (oob management)."
        ),
    ]

    def __init__(self, *args: Any, **kwargs: Any):
        super(CephadmOrchestrator, self).__init__(*args, **kwargs)
        self._cluster_fsid: str = self.get('mon_map')['fsid']
        self.last_monmap: Optional[datetime.datetime] = None

        # for serve()
        self.run = True
        self.event = Event()

        self.ssh = ssh.SSHManager(self)

        if self.get_store('pause'):
            self.paused = True
        else:
            self.paused = False

        # for mypy which does not run the code
        if TYPE_CHECKING:
            self.ssh_config_file = None  # type: Optional[str]
            self.device_cache_timeout = 0
            self.daemon_cache_timeout = 0
            self.facts_cache_timeout = 0
            self.host_check_interval = 0
            self.max_count_per_host = 0
            self.mode = ''
            self.container_image_base = ''
            self.container_image_prometheus = ''
            self.container_image_nvmeof = ''
            self.container_image_grafana = ''
            self.container_image_alertmanager = ''
            self.container_image_node_exporter = ''
            self.container_image_loki = ''
            self.container_image_promtail = ''
            self.container_image_haproxy = ''
            self.container_image_keepalived = ''
            self.container_image_snmp_gateway = ''
            self.container_image_elasticsearch = ''
            self.container_image_jaeger_agent = ''
            self.container_image_jaeger_collector = ''
            self.container_image_jaeger_query = ''
            self.container_image_samba = ''
            self.warn_on_stray_hosts = True
            self.warn_on_stray_daemons = True
            self.warn_on_failed_host_check = True
            self.allow_ptrace = False
            self.container_init = True
            self.prometheus_alerts_path = ''
            self.grafana_dashboards_path = ''
            self.migration_current: Optional[int] = None
            self.config_dashboard = True
            self.manage_etc_ceph_ceph_conf = True
            self.manage_etc_ceph_ceph_conf_hosts = '*'
            self.registry_url: Optional[str] = None
            self.registry_username: Optional[str] = None
            self.registry_password: Optional[str] = None
            self.registry_insecure: bool = False
            self.use_repo_digest = True
            self.config_checks_enabled = False
            self.default_registry = ''
            self.autotune_memory_target_ratio = 0.0
            self.autotune_interval = 0
            self.ssh_user: Optional[str] = None
            self._ssh_options: Optional[str] = None
            self.tkey = NamedTemporaryFile()
            self.ssh_config_fname: Optional[str] = None
            self.ssh_config: Optional[str] = None
            self._temp_files: List = []
            self.ssh_key: Optional[str] = None
            self.ssh_pub: Optional[str] = None
            self.ssh_cert: Optional[str] = None
            self.use_agent = False
            self.agent_refresh_rate = 0
            self.agent_down_multiplier = 0.0
            self.agent_starting_port = 0
            self.hw_monitoring = False
            self.service_discovery_port = 0
            self.secure_monitoring_stack = False
            self.apply_spec_fails: List[Tuple[str, str]] = []
            self.max_osd_draining_count = 10
            self.device_enhanced_scan = False
            self.inventory_list_all = False
            self.cgroups_split = True
            self.log_refresh_metadata = False
            self.default_cephadm_command_timeout = 0
            self.cephadm_log_destination = ''
            self.oob_default_addr = ''

        self.notify(NotifyType.mon_map, None)
        self.config_notify()

        path = self.get_ceph_option('cephadm_path')
        try:
            assert isinstance(path, str)
            with open(path, 'rb') as f:
                self._cephadm = f.read()
        except (IOError, TypeError) as e:
            raise RuntimeError("unable to read cephadm at '%s': %s" % (
                path, str(e)))

        self.cephadm_binary_path = self._get_cephadm_binary_path()

        self._worker_pool = multiprocessing.pool.ThreadPool(10)

        self.ssh._reconfig_ssh()

        CephadmOrchestrator.instance = self

        self.upgrade = CephadmUpgrade(self)

        self.health_checks: Dict[str, dict] = {}

        self.inventory = Inventory(self)

        self.cache = HostCache(self)
        self.cache.load()

        self.node_proxy_cache = NodeProxyCache(self)
        self.node_proxy_cache.load()

        self.agent_cache = AgentCache(self)
        self.agent_cache.load()

        self.to_remove_osds = OSDRemovalQueue(self)
        self.to_remove_osds.load_from_store()

        self.spec_store = SpecStore(self)
        self.spec_store.load()

        self.keys = ClientKeyringStore(self)
        self.keys.load()

        self.tuned_profiles = TunedProfileStore(self)
        self.tuned_profiles.load()

        self.tuned_profile_utils = TunedProfileUtils(self)

        # ensure the host lists are in sync
        for h in self.inventory.keys():
            if h not in self.cache.daemons:
                self.cache.prime_empty_host(h)
        for h in self.cache.get_hosts():
            if h not in self.inventory:
                self.cache.rm_host(h)

        # in-memory only.
        self.events = EventStore(self)
        self.offline_hosts: Set[str] = set()

        self.migration = Migrations(self)

        _service_classes: Sequence[Type[CephadmService]] = [
            AlertmanagerService,
            CephExporterService,
            CephadmAgent,
            CephfsMirrorService,
            CrashService,
            CustomContainerService,
            ElasticSearchService,
            GrafanaService,
            IngressService,
            IscsiService,
            JaegerAgentService,
            JaegerCollectorService,
            JaegerQueryService,
            LokiService,
            MdsService,
            MgrService,
            MonService,
            NFSService,
            NodeExporterService,
            NodeProxy,
            NvmeofService,
            OSDService,
            PrometheusService,
            PromtailService,
            RbdMirrorService,
            RgwService,
            SMBService,
            SNMPGatewayService,
        ]

        # https://github.com/python/mypy/issues/8993
        self.cephadm_services: Dict[str, CephadmService] = {
            cls.TYPE: cls(self) for cls in _service_classes}  # type: ignore

        self.mgr_service: MgrService = cast(MgrService, self.cephadm_services['mgr'])
        self.osd_service: OSDService = cast(OSDService, self.cephadm_services['osd'])
        self.iscsi_service: IscsiService = cast(IscsiService, self.cephadm_services['iscsi'])
        self.nvmeof_service: NvmeofService = cast(NvmeofService, self.cephadm_services['nvmeof'])
        self.node_proxy_service: NodeProxy = cast(NodeProxy, self.cephadm_services['node-proxy'])

        self.scheduled_async_actions: List[Callable] = []

        self.template = TemplateMgr(self)

        self.requires_post_actions: Set[str] = set()
        self.need_connect_dashboard_rgw = False

        self.config_checker = CephadmConfigChecks(self)

        self.http_server = CephadmHttpServer(self)
        self.http_server.start()

        self.node_proxy = NodeProxy(self)

        self.agent_helpers = CephadmAgentHelpers(self)
        if self.use_agent:
            self.agent_helpers._apply_agent()

        self.offline_watcher = OfflineHostWatcher(self)
        self.offline_watcher.start()

    def shutdown(self) -> None:
        self.log.debug('shutdown')
        self._worker_pool.close()
        self._worker_pool.join()
        self.http_server.shutdown()
        self.offline_watcher.shutdown()
        self.run = False
        self.event.set()

    def _get_cephadm_service(self, service_type: str) -> CephadmService:
        assert service_type in ServiceSpec.KNOWN_SERVICE_TYPES
        return self.cephadm_services[service_type]

    def _get_cephadm_binary_path(self) -> str:
        import hashlib
        m = hashlib.sha256()
        m.update(self._cephadm)
        return f'/var/lib/ceph/{self._cluster_fsid}/cephadm.{m.hexdigest()}'

    def _kick_serve_loop(self) -> None:
        self.log.debug('_kick_serve_loop')
        self.event.set()

    def serve(self) -> None:
        """
        The main loop of cephadm.

        A command handler will typically change the declarative state
        of cephadm. This loop will then attempt to apply this new state.
        """
        # for ssh in serve
        self.event_loop = ssh.EventLoopThread()

        serve = CephadmServe(self)
        serve.serve()

    def wait_async(self, coro: Awaitable[T], timeout: Optional[int] = None) -> T:
        if not timeout:
            timeout = self.default_cephadm_command_timeout
            # put a lower bound of 60 seconds in case users
            # accidentally set it to something unreasonable.
            # For example if they though it was in minutes
            # rather than seconds
            if timeout < 60:
                self.log.info(f'Found default timeout set to {timeout}. Instead trying minimum of 60.')
                timeout = 60
        return self.event_loop.get_result(coro, timeout)

    @contextmanager
    def async_timeout_handler(self, host: Optional[str] = '',
                              cmd: Optional[str] = '',
                              timeout: Optional[int] = None) -> Iterator[None]:
        # this is meant to catch asyncio.TimeoutError and convert it into an
        # OrchestratorError which much of the cephadm codebase is better equipped to handle.
        # If the command being run, the host it is run on, or the timeout being used
        # are provided, that will be included in the OrchestratorError's message
        try:
            yield
        except (asyncio.TimeoutError, concurrent.futures.TimeoutError):
            err_str: str = ''
            if cmd:
                err_str = f'Command "{cmd}" timed out '
            else:
                err_str = 'Command timed out '
            if host:
                err_str += f'on host {host} '
            if timeout:
                err_str += f'(non-default {timeout} second timeout)'
            else:
                err_str += (f'(default {self.default_cephadm_command_timeout} second timeout)')
            raise OrchestratorError(err_str)
        except concurrent.futures.CancelledError as e:
            err_str = ''
            if cmd:
                err_str = f'Command "{cmd}" failed '
            else:
                err_str = 'Command failed '
            if host:
                err_str += f'on host {host} '
            err_str += f' - {str(e)}'
            raise OrchestratorError(err_str)

    def set_container_image(self, entity: str, image: str) -> None:
        self.check_mon_command({
            'prefix': 'config set',
            'name': 'container_image',
            'value': image,
            'who': entity,
        })

    def config_notify(self) -> None:
        """
        This method is called whenever one of our config options is changed.

        TODO: this method should be moved into mgr_module.py
        """
        for opt in self.MODULE_OPTIONS:
            setattr(self,
                    opt['name'],  # type: ignore
                    self.get_module_option(opt['name']))  # type: ignore
            self.log.debug(' mgr option %s = %s',
                           opt['name'], getattr(self, opt['name']))  # type: ignore
        for opt in self.NATIVE_OPTIONS:
            setattr(self,
                    opt,  # type: ignore
                    self.get_ceph_option(opt))
            self.log.debug(' native option %s = %s', opt, getattr(self, opt))  # type: ignore

        self.event.set()

    def notify(self, notify_type: NotifyType, notify_id: Optional[str]) -> None:
        if notify_type == NotifyType.mon_map:
            # get monmap mtime so we can refresh configs when mons change
            monmap = self.get('mon_map')
            self.last_monmap = str_to_datetime(monmap['modified'])
            if self.last_monmap and self.last_monmap > datetime_now():
                self.last_monmap = None  # just in case clocks are skewed
            if getattr(self, 'manage_etc_ceph_ceph_conf', False):
                # getattr, due to notify() being called before config_notify()
                self._kick_serve_loop()
        if notify_type == NotifyType.pg_summary:
            self._trigger_osd_removal()

    def _trigger_osd_removal(self) -> None:
        remove_queue = self.to_remove_osds.as_osd_ids()
        if not remove_queue:
            return
        data = self.get("osd_stats")
        for osd in data.get('osd_stats', []):
            if osd.get('num_pgs') == 0:
                # if _ANY_ osd that is currently in the queue appears to be empty,
                # start the removal process
                if int(osd.get('osd')) in remove_queue:
                    self.log.debug('Found empty osd. Starting removal process')
                    # if the osd that is now empty is also part of the removal queue
                    # start the process
                    self._kick_serve_loop()

    def pause(self) -> None:
        if not self.paused:
            self.log.info('Paused')
            self.set_store('pause', 'true')
            self.paused = True
            # wake loop so we update the health status
            self._kick_serve_loop()

    def resume(self) -> None:
        if self.paused:
            self.log.info('Resumed')
            self.paused = False
            self.set_store('pause', None)
        # unconditionally wake loop so that 'orch resume' can be used to kick
        # cephadm
        self._kick_serve_loop()

    def get_unique_name(
            self,
            daemon_type: str,
            host: str,
            existing: List[orchestrator.DaemonDescription],
            prefix: Optional[str] = None,
            forcename: Optional[str] = None,
            rank: Optional[int] = None,
            rank_generation: Optional[int] = None,
    ) -> str:
        """
        Generate a unique random service name
        """
        suffix = daemon_type not in [
            'mon', 'crash', 'ceph-exporter', 'node-proxy',
            'prometheus', 'node-exporter', 'grafana', 'alertmanager',
            'container', 'agent', 'snmp-gateway', 'loki', 'promtail',
            'elasticsearch', 'jaeger-collector', 'jaeger-agent', 'jaeger-query'
        ]
        if forcename:
            if len([d for d in existing if d.daemon_id == forcename]):
                raise orchestrator.OrchestratorValidationError(
                    f'name {daemon_type}.{forcename} already in use')
            return forcename

        if '.' in host:
            host = host.split('.')[0]
        while True:
            if prefix:
                name = prefix + '.'
            else:
                name = ''
            if rank is not None and rank_generation is not None:
                name += f'{rank}.{rank_generation}.'
            name += host
            if suffix:
                name += '.' + ''.join(random.choice(string.ascii_lowercase)
                                      for _ in range(6))
            if len([d for d in existing if d.daemon_id == name]):
                if not suffix:
                    raise orchestrator.OrchestratorValidationError(
                        f'name {daemon_type}.{name} already in use')
                self.log.debug('name %s exists, trying again', name)
                continue
            return name

    def validate_ssh_config_content(self, ssh_config: Optional[str]) -> None:
        if ssh_config is None or len(ssh_config.strip()) == 0:
            raise OrchestratorValidationError('ssh_config cannot be empty')
        # StrictHostKeyChecking is [yes|no] ?
        res = re.findall(r'StrictHostKeyChecking\s+.*', ssh_config)
        if not res:
            raise OrchestratorValidationError('ssh_config requires StrictHostKeyChecking')
        for s in res:
            if 'ask' in s.lower():
                raise OrchestratorValidationError(f'ssh_config cannot contain: \'{s}\'')

    def validate_ssh_config_fname(self, ssh_config_fname: str) -> None:
        if not os.path.isfile(ssh_config_fname):
            raise OrchestratorValidationError("ssh_config \"{}\" does not exist".format(
                ssh_config_fname))

    def _process_ls_output(self, host: str, ls: List[Dict[str, Any]]) -> None:
        def _as_datetime(value: Optional[str]) -> Optional[datetime.datetime]:
            return str_to_datetime(value) if value is not None else None

        dm = {}
        for d in ls:
            if not d['style'].startswith('cephadm'):
                continue
            if d['fsid'] != self._cluster_fsid:
                continue
            if '.' not in d['name']:
                continue
            daemon_type = d['name'].split('.')[0]
            if daemon_type not in orchestrator.KNOWN_DAEMON_TYPES:
                logger.warning(f"Found unknown daemon type {daemon_type} on host {host}")
                continue

            container_id = d.get('container_id')
            if container_id:
                # shorten the hash
                container_id = container_id[0:12]
            rank = int(d['rank']) if d.get('rank') is not None else None
            rank_generation = int(d['rank_generation']) if d.get(
                'rank_generation') is not None else None
            status, status_desc = None, 'unknown'
            if 'state' in d:
                status_desc = d['state']
                status = {
                    'running': DaemonDescriptionStatus.running,
                    'stopped': DaemonDescriptionStatus.stopped,
                    'error': DaemonDescriptionStatus.error,
                    'unknown': DaemonDescriptionStatus.error,
                }[d['state']]
            sd = orchestrator.DaemonDescription(
                daemon_type=daemon_type,
                daemon_id='.'.join(d['name'].split('.')[1:]),
                hostname=host,
                container_id=container_id,
                container_image_id=d.get('container_image_id'),
                container_image_name=d.get('container_image_name'),
                container_image_digests=d.get('container_image_digests'),
                version=d.get('version'),
                status=status,
                status_desc=status_desc,
                created=_as_datetime(d.get('created')),
                started=_as_datetime(d.get('started')),
                last_refresh=datetime_now(),
                last_configured=_as_datetime(d.get('last_configured')),
                last_deployed=_as_datetime(d.get('last_deployed')),
                memory_usage=d.get('memory_usage'),
                memory_request=d.get('memory_request'),
                memory_limit=d.get('memory_limit'),
                cpu_percentage=d.get('cpu_percentage'),
                service_name=d.get('service_name'),
                ports=d.get('ports'),
                ip=d.get('ip'),
                deployed_by=d.get('deployed_by'),
                rank=rank,
                rank_generation=rank_generation,
                extra_container_args=d.get('extra_container_args'),
                extra_entrypoint_args=d.get('extra_entrypoint_args'),
            )
            dm[sd.name()] = sd
        self.log.debug('Refreshed host %s daemons (%d)' % (host, len(dm)))
        self.cache.update_host_daemons(host, dm)
        self.cache.save_host(host)
        return None

    def update_watched_hosts(self) -> None:
        # currently, we are watching hosts with nfs daemons
        hosts_to_watch = [d.hostname for d in self.cache.get_daemons(
        ) if d.daemon_type in RESCHEDULE_FROM_OFFLINE_HOSTS_TYPES]
        self.offline_watcher.set_hosts(list(set([h for h in hosts_to_watch if h is not None])))

    def offline_hosts_remove(self, host: str) -> None:
        if host in self.offline_hosts:
            self.offline_hosts.remove(host)

    def update_failed_daemon_health_check(self) -> None:
        failed_daemons = []
        for dd in self.cache.get_error_daemons():
            if dd.daemon_type != 'agent':  # agents tracked by CEPHADM_AGENT_DOWN
                failed_daemons.append('daemon %s on %s is in %s state' % (
                    dd.name(), dd.hostname, dd.status_desc
                ))
        self.remove_health_warning('CEPHADM_FAILED_DAEMON')
        if failed_daemons:
            self.set_health_warning('CEPHADM_FAILED_DAEMON', f'{len(failed_daemons)} failed cephadm daemon(s)', len(
                failed_daemons), failed_daemons)

    def get_first_matching_network_ip(self, host: str, sspec: ServiceSpec) -> Optional[str]:
        sspec_networks = sspec.networks
        for subnet, ifaces in self.cache.networks.get(host, {}).items():
            host_network = ipaddress.ip_network(subnet)
            for spec_network_str in sspec_networks:
                spec_network = ipaddress.ip_network(spec_network_str)
                if host_network.overlaps(spec_network):
                    return list(ifaces.values())[0][0]
                logger.error(f'{spec_network} from {sspec.service_name()} spec does not overlap with {host_network} on {host}')
        return None

    @staticmethod
    def can_run() -> Tuple[bool, str]:
        if asyncssh is not None:
            return True, ""
        else:
            return False, "loading asyncssh library:{}".format(
                asyncssh_import_error)

    def available(self) -> Tuple[bool, str, Dict[str, Any]]:
        """
        The cephadm orchestrator is always available.
        """
        ok, err = self.can_run()
        if not ok:
            return ok, err, {}
        if not self.ssh_key or not self.ssh_pub:
            return False, 'SSH keys not set. Use `ceph cephadm set-priv-key` and `ceph cephadm set-pub-key` or `ceph cephadm generate-key`', {}

        # mypy is unable to determine type for _processes since it's private
        worker_count: int = self._worker_pool._processes  # type: ignore
        ret = {
            "workers": worker_count,
            "paused": self.paused,
        }

        return True, err, ret

    def _validate_and_set_ssh_val(self, what: str, new: Optional[str], old: Optional[str]) -> None:
        self.set_store(what, new)
        self.ssh._reconfig_ssh()
        if self.cache.get_hosts():
            # Can't check anything without hosts
            host = self.cache.get_hosts()[0]
            r = CephadmServe(self)._check_host(host)
            if r is not None:
                # connection failed reset user
                self.set_store(what, old)
                self.ssh._reconfig_ssh()
                raise OrchestratorError('ssh connection %s@%s failed' % (self.ssh_user, host))
        self.log.info(f'Set ssh {what}')

    @orchestrator._cli_write_command(
        prefix='cephadm set-ssh-config')
    def _set_ssh_config(self, inbuf: Optional[str] = None) -> Tuple[int, str, str]:
        """
        Set the ssh_config file (use -i <ssh_config>)
        """
        # Set an ssh_config file provided from stdin

        old = self.ssh_config
        if inbuf == old:
            return 0, "value unchanged", ""
        self.validate_ssh_config_content(inbuf)
        self._validate_and_set_ssh_val('ssh_config', inbuf, old)
        return 0, "", ""

    @orchestrator._cli_write_command('cephadm clear-ssh-config')
    def _clear_ssh_config(self) -> Tuple[int, str, str]:
        """
        Clear the ssh_config file
        """
        # Clear the ssh_config file provided from stdin
        self.set_store("ssh_config", None)
        self.ssh_config_tmp = None
        self.log.info('Cleared ssh_config')
        self.ssh._reconfig_ssh()
        return 0, "", ""

    @orchestrator._cli_read_command('cephadm get-ssh-config')
    def _get_ssh_config(self) -> HandleCommandResult:
        """
        Returns the ssh config as used by cephadm
        """
        if self.ssh_config_file:
            self.validate_ssh_config_fname(self.ssh_config_file)
            with open(self.ssh_config_file) as f:
                return HandleCommandResult(stdout=f.read())
        ssh_config = self.get_store("ssh_config")
        if ssh_config:
            return HandleCommandResult(stdout=ssh_config)
        return HandleCommandResult(stdout=DEFAULT_SSH_CONFIG)

    @orchestrator._cli_write_command('cephadm generate-key')
    def _generate_key(self) -> Tuple[int, str, str]:
        """
        Generate a cluster SSH key (if not present)
        """
        if not self.ssh_pub or not self.ssh_key:
            self.log.info('Generating ssh key...')
            tmp_dir = TemporaryDirectory()
            path = tmp_dir.name + '/key'
            try:
                subprocess.check_call([
                    '/usr/bin/ssh-keygen',
                    '-C', 'ceph-%s' % self._cluster_fsid,
                    '-N', '',
                    '-f', path
                ])
                with open(path, 'r') as f:
                    secret = f.read()
                with open(path + '.pub', 'r') as f:
                    pub = f.read()
            finally:
                os.unlink(path)
                os.unlink(path + '.pub')
                tmp_dir.cleanup()
            self.set_store('ssh_identity_key', secret)
            self.set_store('ssh_identity_pub', pub)
            self.ssh._reconfig_ssh()
        return 0, '', ''

    @orchestrator._cli_write_command(
        'cephadm set-priv-key')
    def _set_priv_key(self, inbuf: Optional[str] = None) -> Tuple[int, str, str]:
        """Set cluster SSH private key (use -i <private_key>)"""
        if inbuf is None or len(inbuf) == 0:
            return -errno.EINVAL, "", "empty private ssh key provided"
        old = self.ssh_key
        if inbuf == old:
            return 0, "value unchanged", ""
        self._validate_and_set_ssh_val('ssh_identity_key', inbuf, old)
        self.log.info('Set ssh private key')
        return 0, "", ""

    @orchestrator._cli_write_command(
        'cephadm set-pub-key')
    def _set_pub_key(self, inbuf: Optional[str] = None) -> Tuple[int, str, str]:
        """Set cluster SSH public key (use -i <public_key>)"""
        if inbuf is None or len(inbuf) == 0:
            return -errno.EINVAL, "", "empty public ssh key provided"
        old = self.ssh_pub
        if inbuf == old:
            return 0, "value unchanged", ""
        self._validate_and_set_ssh_val('ssh_identity_pub', inbuf, old)
        return 0, "", ""

    @orchestrator._cli_write_command(
        'cephadm set-signed-cert')
    def _set_signed_cert(self, inbuf: Optional[str] = None) -> Tuple[int, str, str]:
        """Set a signed cert if CA signed keys are being used (use -i <cert_filename>)"""
        if inbuf is None or len(inbuf) == 0:
            return -errno.EINVAL, "", "empty cert file provided"
        old = self.ssh_cert
        if inbuf == old:
            return 0, "value unchanged", ""
        self._validate_and_set_ssh_val('ssh_identity_cert', inbuf, old)
        return 0, "", ""

    @orchestrator._cli_write_command(
        'cephadm clear-key')
    def _clear_key(self) -> Tuple[int, str, str]:
        """Clear cluster SSH key"""
        self.set_store('ssh_identity_key', None)
        self.set_store('ssh_identity_pub', None)
        self.set_store('ssh_identity_cert', None)
        self.ssh._reconfig_ssh()
        self.log.info('Cleared cluster SSH key')
        return 0, '', ''

    @orchestrator._cli_read_command(
        'cephadm get-pub-key')
    def _get_pub_key(self) -> Tuple[int, str, str]:
        """Show SSH public key for connecting to cluster hosts"""
        if self.ssh_pub:
            return 0, self.ssh_pub, ''
        else:
            return -errno.ENOENT, '', 'No cluster SSH key defined'

    @orchestrator._cli_read_command(
        'cephadm get-signed-cert')
    def _get_signed_cert(self) -> Tuple[int, str, str]:
        """Show SSH signed cert for connecting to cluster hosts using CA signed keys"""
        if self.ssh_cert:
            return 0, self.ssh_cert, ''
        else:
            return -errno.ENOENT, '', 'No signed cert defined'

    @orchestrator._cli_read_command(
        'cephadm get-user')
    def _get_user(self) -> Tuple[int, str, str]:
        """
        Show user for SSHing to cluster hosts
        """
        if self.ssh_user is None:
            return -errno.ENOENT, '', 'No cluster SSH user configured'
        else:
            return 0, self.ssh_user, ''

    @orchestrator._cli_read_command(
        'cephadm set-user')
    def set_ssh_user(self, user: str) -> Tuple[int, str, str]:
        """
        Set user for SSHing to cluster hosts, passwordless sudo will be needed for non-root users
        """
        current_user = self.ssh_user
        if user == current_user:
            return 0, "value unchanged", ""

        self._validate_and_set_ssh_val('ssh_user', user, current_user)
        current_ssh_config = self._get_ssh_config()
        new_ssh_config = re.sub(r"(\s{2}User\s)(.*)", r"\1" + user, current_ssh_config.stdout)
        self._set_ssh_config(new_ssh_config)

        msg = 'ssh user set to %s' % user
        if user != 'root':
            msg += '. sudo will be used'
        self.log.info(msg)
        return 0, msg, ''

    @orchestrator._cli_read_command(
        'cephadm registry-login')
    def registry_login(self, url: Optional[str] = None, username: Optional[str] = None, password: Optional[str] = None, inbuf: Optional[str] = None) -> Tuple[int, str, str]:
        """
        Set custom registry login info by providing url, username and password or json file with login info (-i <file>)
        """
        # if password not given in command line, get it through file input
        if not (url and username and password) and (inbuf is None or len(inbuf) == 0):
            return -errno.EINVAL, "", ("Invalid arguments. Please provide arguments <url> <username> <password> "
                                       "or -i <login credentials json file>")
        elif (url and username and password):
            registry_json = {'url': url, 'username': username, 'password': password}
        else:
            assert isinstance(inbuf, str)
            registry_json = json.loads(inbuf)
            if "url" not in registry_json or "username" not in registry_json or "password" not in registry_json:
                return -errno.EINVAL, "", ("json provided for custom registry login did not include all necessary fields. "
                                           "Please setup json file as\n"
                                           "{\n"
                                           " \"url\": \"REGISTRY_URL\",\n"
                                           " \"username\": \"REGISTRY_USERNAME\",\n"
                                           " \"password\": \"REGISTRY_PASSWORD\"\n"
                                           "}\n")

        # verify login info works by attempting login on random host
        host = None
        for host_name in self.inventory.keys():
            host = host_name
            break
        if not host:
            raise OrchestratorError('no hosts defined')
        with self.async_timeout_handler(host, 'cephadm registry-login'):
            r = self.wait_async(CephadmServe(self)._registry_login(host, registry_json))
        if r is not None:
            return 1, '', r
        # if logins succeeded, store info
        self.log.debug("Host logins successful. Storing login info.")
        self.set_store('registry_credentials', json.dumps(registry_json))
        # distribute new login info to all hosts
        self.cache.distribute_new_registry_login_info()
        return 0, "registry login scheduled", ''

    @orchestrator._cli_read_command('cephadm check-host')
    def check_host(self, host: str, addr: Optional[str] = None) -> Tuple[int, str, str]:
        """Check whether we can access and manage a remote host"""
        try:
            with self.async_timeout_handler(host, f'cephadm check-host --expect-hostname {host}'):
                out, err, code = self.wait_async(
                    CephadmServe(self)._run_cephadm(
                        host, cephadmNoImage, 'check-host', ['--expect-hostname', host],
                        addr=addr, error_ok=True, no_fsid=True))
            if code:
                return 1, '', ('check-host failed:\n' + '\n'.join(err))
        except ssh.HostConnectionError as e:
            self.log.exception(
                f"check-host failed for '{host}' at addr ({e.addr}) due to connection failure: {str(e)}")
            return 1, '', ('check-host failed:\n'
                           + f"Failed to connect to {host} at address ({e.addr}): {str(e)}")
        except OrchestratorError:
            self.log.exception(f"check-host failed for '{host}'")
            return 1, '', ('check-host failed:\n'
                           + f"Host '{host}' not found. Use 'ceph orch host ls' to see all managed hosts.")
        # if we have an outstanding health alert for this host, give the
        # serve thread a kick
        if 'CEPHADM_HOST_CHECK_FAILED' in self.health_checks:
            for item in self.health_checks['CEPHADM_HOST_CHECK_FAILED']['detail']:
                if item.startswith('host %s ' % host):
                    self.event.set()
        return 0, '%s (%s) ok' % (host, addr), '\n'.join(err)

    @orchestrator._cli_read_command(
        'cephadm prepare-host')
    def _prepare_host(self, host: str, addr: Optional[str] = None) -> Tuple[int, str, str]:
        """Prepare a remote host for use with cephadm"""
        with self.async_timeout_handler(host, 'cephadm prepare-host'):
            out, err, code = self.wait_async(
                CephadmServe(self)._run_cephadm(
                    host, cephadmNoImage, 'prepare-host', ['--expect-hostname', host],
                    addr=addr, error_ok=True, no_fsid=True))
        if code:
            return 1, '', ('prepare-host failed:\n' + '\n'.join(err))
        # if we have an outstanding health alert for this host, give the
        # serve thread a kick
        if 'CEPHADM_HOST_CHECK_FAILED' in self.health_checks:
            for item in self.health_checks['CEPHADM_HOST_CHECK_FAILED']['detail']:
                if item.startswith('host %s ' % host):
                    self.event.set()
        return 0, '%s (%s) ok' % (host, addr), '\n'.join(err)

    @orchestrator._cli_write_command(
        prefix='cephadm set-extra-ceph-conf')
    def _set_extra_ceph_conf(self, inbuf: Optional[str] = None) -> HandleCommandResult:
        """
        Text that is appended to all daemon's ceph.conf.
        Mainly a workaround, till `config generate-minimal-conf` generates
        a complete ceph.conf.

        Warning: this is a dangerous operation.
        """
        if inbuf:
            # sanity check.
            cp = ConfigParser()
            cp.read_string(inbuf, source='<infile>')

        self.set_store("extra_ceph_conf", json.dumps({
            'conf': inbuf,
            'last_modified': datetime_to_str(datetime_now())
        }))
        self.log.info('Set extra_ceph_conf')
        self._kick_serve_loop()
        return HandleCommandResult()

    @orchestrator._cli_read_command(
        'cephadm get-extra-ceph-conf')
    def _get_extra_ceph_conf(self) -> HandleCommandResult:
        """
        Get extra ceph conf that is appended
        """
        return HandleCommandResult(stdout=self.extra_ceph_conf().conf)

    @orchestrator._cli_read_command('cephadm config-check ls')
    def _config_checks_list(self, format: Format = Format.plain) -> HandleCommandResult:
        """List the available configuration checks and their current state"""

        if format not in [Format.plain, Format.json, Format.json_pretty]:
            return HandleCommandResult(
                retval=1,
                stderr="Requested format is not supported when listing configuration checks"
            )

        if format in [Format.json, Format.json_pretty]:
            return HandleCommandResult(
                stdout=to_format(self.config_checker.health_checks,
                                 format,
                                 many=True,
                                 cls=None))

        # plain formatting
        table = PrettyTable(
            ['NAME',
             'HEALTHCHECK',
             'STATUS',
             'DESCRIPTION'
             ], border=False)
        table.align['NAME'] = 'l'
        table.align['HEALTHCHECK'] = 'l'
        table.align['STATUS'] = 'l'
        table.align['DESCRIPTION'] = 'l'
        table.left_padding_width = 0
        table.right_padding_width = 2
        for c in self.config_checker.health_checks:
            table.add_row((
                c.name,
                c.healthcheck_name,
                c.status,
                c.description,
            ))

        return HandleCommandResult(stdout=table.get_string())

    @orchestrator._cli_read_command('cephadm config-check status')
    def _config_check_status(self) -> HandleCommandResult:
        """Show whether the configuration checker feature is enabled/disabled"""
        status = self.config_checks_enabled
        return HandleCommandResult(stdout="Enabled" if status else "Disabled")

    @orchestrator._cli_write_command('cephadm config-check enable')
    def _config_check_enable(self, check_name: str) -> HandleCommandResult:
        """Enable a specific configuration check"""
        if not self._config_check_valid(check_name):
            return HandleCommandResult(retval=1, stderr="Invalid check name")

        err, msg = self._update_config_check(check_name, 'enabled')
        if err:
            return HandleCommandResult(
                retval=err,
                stderr=f"Failed to enable check '{check_name}' : {msg}")

        return HandleCommandResult(stdout="ok")

    @orchestrator._cli_write_command('cephadm config-check disable')
    def _config_check_disable(self, check_name: str) -> HandleCommandResult:
        """Disable a specific configuration check"""
        if not self._config_check_valid(check_name):
            return HandleCommandResult(retval=1, stderr="Invalid check name")

        err, msg = self._update_config_check(check_name, 'disabled')
        if err:
            return HandleCommandResult(retval=err, stderr=f"Failed to disable check '{check_name}': {msg}")
        else:
            # drop any outstanding raised healthcheck for this check
            config_check = self.config_checker.lookup_check(check_name)
            if config_check:
                if config_check.healthcheck_name in self.health_checks:
                    self.health_checks.pop(config_check.healthcheck_name, None)
                    self.set_health_checks(self.health_checks)
            else:
                self.log.error(
                    f"Unable to resolve a check name ({check_name}) to a healthcheck definition?")

        return HandleCommandResult(stdout="ok")

    def _config_check_valid(self, check_name: str) -> bool:
        return check_name in [chk.name for chk in self.config_checker.health_checks]

    def _update_config_check(self, check_name: str, status: str) -> Tuple[int, str]:
        checks_raw = self.get_store('config_checks')
        if not checks_raw:
            return 1, "config_checks setting is not available"

        checks = json.loads(checks_raw)
        checks.update({
            check_name: status
        })
        self.log.info(f"updated config check '{check_name}' : {status}")
        self.set_store('config_checks', json.dumps(checks))
        return 0, ""

    class ExtraCephConf(NamedTuple):
        conf: str
        last_modified: Optional[datetime.datetime]

    def extra_ceph_conf(self) -> 'CephadmOrchestrator.ExtraCephConf':
        data = self.get_store('extra_ceph_conf')
        if not data:
            return CephadmOrchestrator.ExtraCephConf('', None)
        try:
            j = json.loads(data)
        except ValueError:
            msg = 'Unable to load extra_ceph_conf: Cannot decode JSON'
            self.log.exception('%s: \'%s\'', msg, data)
            return CephadmOrchestrator.ExtraCephConf('', None)
        return CephadmOrchestrator.ExtraCephConf(j['conf'], str_to_datetime(j['last_modified']))

    def extra_ceph_conf_is_newer(self, dt: datetime.datetime) -> bool:
        conf = self.extra_ceph_conf()
        if not conf.last_modified:
            return False
        return conf.last_modified > dt

    @orchestrator._cli_write_command(
        'cephadm osd activate'
    )
    def _osd_activate(self, host: List[str]) -> HandleCommandResult:
        """
        Start OSD containers for existing OSDs
        """

        @forall_hosts
        def run(h: str) -> str:
            with self.async_timeout_handler(h, 'cephadm deploy (osd daemon)'):
                return self.wait_async(self.osd_service.deploy_osd_daemons_for_existing_osds(h, 'osd'))

        return HandleCommandResult(stdout='\n'.join(run(host)))

    @orchestrator._cli_read_command('orch client-keyring ls')
    def _client_keyring_ls(self, format: Format = Format.plain) -> HandleCommandResult:
        """
        List client keyrings under cephadm management
        """
        if format != Format.plain:
            output = to_format(self.keys.keys.values(), format, many=True, cls=ClientKeyringSpec)
        else:
            table = PrettyTable(
                ['ENTITY', 'PLACEMENT', 'MODE', 'OWNER', 'PATH'],
                border=False)
            table.align = 'l'
            table.left_padding_width = 0
            table.right_padding_width = 2
            for ks in sorted(self.keys.keys.values(), key=lambda ks: ks.entity):
                table.add_row((
                    ks.entity, ks.placement.pretty_str(),
                    utils.file_mode_to_str(ks.mode),
                    f'{ks.uid}:{ks.gid}',
                    ks.path,
                ))
            output = table.get_string()
        return HandleCommandResult(stdout=output)

    @orchestrator._cli_write_command('orch client-keyring set')
    def _client_keyring_set(
            self,
            entity: str,
            placement: str,
            owner: Optional[str] = None,
            mode: Optional[str] = None,
    ) -> HandleCommandResult:
        """
        Add or update client keyring under cephadm management
        """
        if not entity.startswith('client.'):
            raise OrchestratorError('entity must start with client.')
        if owner:
            try:
                uid, gid = map(int, owner.split(':'))
            except Exception:
                raise OrchestratorError('owner must look like "<uid>:<gid>", e.g., "0:0"')
        else:
            uid = 0
            gid = 0
        if mode:
            try:
                imode = int(mode, 8)
            except Exception:
                raise OrchestratorError('mode must be an octal mode, e.g. "600"')
        else:
            imode = 0o600
        pspec = PlacementSpec.from_string(placement)
        ks = ClientKeyringSpec(entity, pspec, mode=imode, uid=uid, gid=gid)
        self.keys.update(ks)
        self._kick_serve_loop()
        return HandleCommandResult()

    @orchestrator._cli_write_command('orch client-keyring rm')
    def _client_keyring_rm(
            self,
            entity: str,
    ) -> HandleCommandResult:
        """
        Remove client keyring from cephadm management
        """
        self.keys.rm(entity)
        self._kick_serve_loop()
        return HandleCommandResult()

    def _get_container_image(self, daemon_name: str) -> Optional[str]:
        daemon_type = daemon_name.split('.', 1)[0]  # type: ignore
        image: Optional[str] = None
        if daemon_type in CEPH_IMAGE_TYPES:
            # get container image
            image = str(self.get_foreign_ceph_option(
                utils.name_to_config_section(daemon_name),
                'container_image'
            )).strip()
        else:
            images = {
                'alertmanager': self.container_image_alertmanager,
                'elasticsearch': self.container_image_elasticsearch,
                'grafana': self.container_image_grafana,
                'haproxy': self.container_image_haproxy,
                'jaeger-agent': self.container_image_jaeger_agent,
                'jaeger-collector': self.container_image_jaeger_collector,
                'jaeger-query': self.container_image_jaeger_query,
                'keepalived': self.container_image_keepalived,
                'loki': self.container_image_loki,
                'node-exporter': self.container_image_node_exporter,
                'nvmeof': self.container_image_nvmeof,
                'prometheus': self.container_image_prometheus,
                'promtail': self.container_image_promtail,
                'snmp-gateway': self.container_image_snmp_gateway,
                # The image can't be resolved here, the necessary information
                # is only available when a container is deployed (given
                # via spec).
                CustomContainerService.TYPE: None,
                SMBService.TYPE: self.container_image_samba,
            }
            try:
                image = images[daemon_type]
            except KeyError:
                raise ValueError(f'no image for {daemon_type}')

        self.log.debug('%s container image %s' % (daemon_name, image))

        return image

    def _check_valid_addr(self, host: str, addr: str) -> str:
        # make sure hostname is resolvable before trying to make a connection
        try:
            ip_addr = utils.resolve_ip(addr)
        except OrchestratorError as e:
            msg = str(e) + f'''
You may need to supply an address for {addr}

Please make sure that the host is reachable and accepts connections using the cephadm SSH key
To add the cephadm SSH key to the host:
> ceph cephadm get-pub-key > ~/ceph.pub
> ssh-copy-id -f -i ~/ceph.pub {self.ssh_user}@{addr}

To check that the host is reachable open a new shell with the --no-hosts flag:
> cephadm shell --no-hosts

Then run the following:
> ceph cephadm get-ssh-config > ssh_config
> ceph config-key get mgr/cephadm/ssh_identity_key > ~/cephadm_private_key
> chmod 0600 ~/cephadm_private_key
> ssh -F ssh_config -i ~/cephadm_private_key {self.ssh_user}@{addr}'''
            raise OrchestratorError(msg)

        if ipaddress.ip_address(ip_addr).is_loopback and host == addr:
            # if this is a re-add, use old address. otherwise error
            if host not in self.inventory or self.inventory.get_addr(host) == host:
                raise OrchestratorError(
                    (f'Cannot automatically resolve ip address of host {host}. Ip resolved to loopback address: {ip_addr}\n'
                     + f'Please explicitly provide the address (ceph orch host add {host} --addr <ip-addr>)'))
            self.log.debug(
                f'Received loopback address resolving ip for {host}: {ip_addr}. Falling back to previous address.')
            ip_addr = self.inventory.get_addr(host)
        try:
            with self.async_timeout_handler(host, f'cephadm check-host --expect-hostname {host}'):
                out, err, code = self.wait_async(CephadmServe(self)._run_cephadm(
                    host, cephadmNoImage, 'check-host',
                    ['--expect-hostname', host],
                    addr=addr,
                    error_ok=True, no_fsid=True))
            if code:
                msg = 'check-host failed:\n' + '\n'.join(err)
                # err will contain stdout and stderr, so we filter on the message text to
                # only show the errors
                errors = [_i.replace("ERROR: ", "") for _i in err if _i.startswith('ERROR')]
                if errors:
                    msg = f'Host {host} ({addr}) failed check(s): {errors}'
                raise OrchestratorError(msg)
        except ssh.HostConnectionError as e:
            raise OrchestratorError(str(e))
        return ip_addr

    def _add_host(self, spec):
        # type: (HostSpec) -> str
        """
        Add a host to be managed by the orchestrator.

        :param host: host name
        """
        HostSpec.validate(spec)
        ip_addr = self._check_valid_addr(spec.hostname, spec.addr)
        if spec.addr == spec.hostname and ip_addr:
            spec.addr = ip_addr

        if spec.hostname in self.inventory and self.inventory.get_addr(spec.hostname) != spec.addr:
            self.cache.refresh_all_host_info(spec.hostname)

        if spec.oob:
            if not spec.oob.get('addr'):
                spec.oob['addr'] = self.oob_default_addr
            if not spec.oob.get('port'):
                spec.oob['port'] = '443'
            host_oob_info = dict()
            host_oob_info['addr'] = spec.oob['addr']
            host_oob_info['port'] = spec.oob['port']
            host_oob_info['username'] = spec.oob['username']
            host_oob_info['password'] = spec.oob['password']
            self.node_proxy_cache.update_oob(spec.hostname, host_oob_info)

        # prime crush map?
        if spec.location:
            self.check_mon_command({
                'prefix': 'osd crush add-bucket',
                'name': spec.hostname,
                'type': 'host',
                'args': [f'{k}={v}' for k, v in spec.location.items()],
            })

        if spec.hostname not in self.inventory:
            self.cache.prime_empty_host(spec.hostname)
        self.inventory.add_host(spec)
        self.offline_hosts_remove(spec.hostname)
        if spec.status == 'maintenance':
            self._set_maintenance_healthcheck()
        self.event.set()  # refresh stray health check
        self.log.info('Added host %s' % spec.hostname)
        return "Added host '{}' with addr '{}'".format(spec.hostname, spec.addr)

    @handle_orch_error
    def add_host(self, spec: HostSpec) -> str:
        return self._add_host(spec)

    @handle_orch_error
    def hardware_light(self, light_type: str, action: str, hostname: str, device: Optional[str] = None) -> Dict[str, Any]:
        try:
            result = self.node_proxy.led(light_type=light_type,
                                         action=action,
                                         hostname=hostname,
                                         device=device)
        except RuntimeError as e:
            self.log.error(e)
            raise OrchestratorValidationError(f'Make sure the node-proxy agent is deployed and running on {hostname}')
        except HTTPError as e:
            self.log.error(e)
            raise OrchestratorValidationError(f"http error while querying node-proxy API: {e}")
        return result

    @handle_orch_error
    def hardware_shutdown(self, hostname: str, force: Optional[bool] = False, yes_i_really_mean_it: bool = False) -> str:
        if not yes_i_really_mean_it:
            raise OrchestratorError("you must pass --yes-i-really-mean-it")

        try:
            self.node_proxy.shutdown(hostname, force)
        except RuntimeError as e:
            self.log.error(e)
            raise OrchestratorValidationError(f'Make sure the node-proxy agent is deployed and running on {hostname}')
        except HTTPError as e:
            self.log.error(e)
            raise OrchestratorValidationError(f"Can't shutdown node {hostname}: {e}")
        return f'Shutdown scheduled on {hostname}'

    @handle_orch_error
    def hardware_powercycle(self, hostname: str, yes_i_really_mean_it: bool = False) -> str:
        if not yes_i_really_mean_it:
            raise OrchestratorError("you must pass --yes-i-really-mean-it")

        try:
            self.node_proxy.powercycle(hostname)
        except RuntimeError as e:
            self.log.error(e)
            raise OrchestratorValidationError(f'Make sure the node-proxy agent is deployed and running on {hostname}')
        except HTTPError as e:
            self.log.error(e)
            raise OrchestratorValidationError(f"Can't perform powercycle on node {hostname}: {e}")
        return f'Powercycle scheduled on {hostname}'

    @handle_orch_error
    def node_proxy_fullreport(self, hostname: Optional[str] = None) -> Dict[str, Any]:
        return self.node_proxy_cache.fullreport(hostname=hostname)

    @handle_orch_error
    def node_proxy_summary(self, hostname: Optional[str] = None) -> Dict[str, Any]:
        return self.node_proxy_cache.summary(hostname=hostname)

    @handle_orch_error
    def node_proxy_firmwares(self, hostname: Optional[str] = None) -> Dict[str, Any]:
        return self.node_proxy_cache.firmwares(hostname=hostname)

    @handle_orch_error
    def node_proxy_criticals(self, hostname: Optional[str] = None) -> Dict[str, Any]:
        return self.node_proxy_cache.criticals(hostname=hostname)

    @handle_orch_error
    def node_proxy_common(self, category: str, hostname: Optional[str] = None) -> Dict[str, Any]:
        return self.node_proxy_cache.common(category, hostname=hostname)

    @handle_orch_error
    def remove_host(self, host: str, force: bool = False, offline: bool = False, rm_crush_entry: bool = False) -> str:
        """
        Remove a host from orchestrator management.

        :param host: host name
        :param force: bypass running daemons check
        :param offline: remove offline host
        """

        # check if host is offline
        host_offline = host in self.offline_hosts

        if host_offline and not offline:
            raise OrchestratorValidationError(
                "{} is offline, please use --offline and --force to remove this host. This can potentially cause data loss".format(host))

        if not host_offline and offline:
            raise OrchestratorValidationError(
                "{} is online, please remove host without --offline.".format(host))

        if offline and not force:
            raise OrchestratorValidationError("Removing an offline host requires --force")

        # check if there are daemons on the host
        if not force:
            daemons = self.cache.get_daemons_by_host(host)
            if daemons:
                self.log.warning(f"Blocked {host} removal. Daemons running: {daemons}")

                daemons_table = ""
                daemons_table += "{:<20} {:<15}\n".format("type", "id")
                daemons_table += "{:<20} {:<15}\n".format("-" * 20, "-" * 15)
                for d in daemons:
                    daemons_table += "{:<20} {:<15}\n".format(d.daemon_type, d.daemon_id)

                raise OrchestratorValidationError("Not allowed to remove %s from cluster. "
                                                  "The following daemons are running in the host:"
                                                  "\n%s\nPlease run 'ceph orch host drain %s' to remove daemons from host" % (
                                                      host, daemons_table, host))

        # check, if there we're removing the last _admin host
        if not force:
            p = PlacementSpec(label=SpecialHostLabels.ADMIN)
            admin_hosts = p.filter_matching_hostspecs(self.inventory.all_specs())
            if len(admin_hosts) == 1 and admin_hosts[0] == host:
                raise OrchestratorValidationError(f"Host {host} is the last host with the '{SpecialHostLabels.ADMIN}'"
                                                  f" label. Please add the '{SpecialHostLabels.ADMIN}' label to a host"
                                                  " or add --force to this command")

        def run_cmd(cmd_args: dict) -> None:
            ret, out, err = self.mon_command(cmd_args)
            if ret != 0:
                self.log.debug(f"ran {cmd_args} with mon_command")
                self.log.error(
                    f"cmd: {cmd_args.get('prefix')} failed with: {err}. (errno:{ret})")
            self.log.debug(f"cmd: {cmd_args.get('prefix')} returns: {out}")

        if offline:
            daemons = self.cache.get_daemons_by_host(host)
            for d in daemons:
                self.log.info(f"removing: {d.name()}")

                if d.daemon_type != 'osd':
                    self.cephadm_services[daemon_type_to_service(str(d.daemon_type))].pre_remove(d)
                    self.cephadm_services[daemon_type_to_service(
                        str(d.daemon_type))].post_remove(d, is_failed_deploy=False)
                else:
                    cmd_args = {
                        'prefix': 'osd purge-actual',
                        'id': int(str(d.daemon_id)),
                        'yes_i_really_mean_it': True
                    }
                    run_cmd(cmd_args)

            cmd_args = {
                'prefix': 'osd crush rm',
                'name': host
            }
            run_cmd(cmd_args)

        if rm_crush_entry:
            try:
                self.check_mon_command({
                    'prefix': 'osd crush remove',
                    'name': host,
                })
            except MonCommandFailed as e:
                self.log.error(f'Couldn\'t remove host {host} from CRUSH map: {str(e)}')
                return (f'Cephadm failed removing host {host}\n'
                        f'Failed to remove host {host} from the CRUSH map: {str(e)}')

        self.inventory.rm_host(host)
        self.cache.rm_host(host)
        self.ssh.reset_con(host)
        # if host was in offline host list, we should remove it now.
        self.offline_hosts_remove(host)
        self.event.set()  # refresh stray health check
        self.log.info('Removed host %s' % host)
        return "Removed {} host '{}'".format('offline' if offline else '', host)

    @handle_orch_error
    def update_host_addr(self, host: str, addr: str) -> str:
        self._check_valid_addr(host, addr)
        self.inventory.set_addr(host, addr)
        self.ssh.reset_con(host)
        self.event.set()  # refresh stray health check
        self.log.info('Set host %s addr to %s' % (host, addr))
        return "Updated host '{}' addr to '{}'".format(host, addr)

    @handle_orch_error
    def get_hosts(self):
        # type: () -> List[orchestrator.HostSpec]
        """
        Return a list of hosts managed by the orchestrator.

        Notes:
          - skip async: manager reads from cache.
        """
        return list(self.inventory.all_specs())

    @handle_orch_error
    def get_facts(self, hostname: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Return a list of hosts metadata(gather_facts) managed by the orchestrator.

        Notes:
          - skip async: manager reads from cache.
        """
        if hostname:
            return [self.cache.get_facts(hostname)]

        return [self.cache.get_facts(hostname) for hostname in self.cache.get_hosts()]

    @handle_orch_error
    def add_host_label(self, host: str, label: str) -> str:
        self.inventory.add_label(host, label)
        self.log.info('Added label %s to host %s' % (label, host))
        self._kick_serve_loop()
        return 'Added label %s to host %s' % (label, host)

    @handle_orch_error
    def remove_host_label(self, host: str, label: str, force: bool = False) -> str:
        # if we remove the _admin label from the only host that has it we could end up
        # removing the only instance of the config and keyring and cause issues
        if not force and label == SpecialHostLabels.ADMIN:
            p = PlacementSpec(label=SpecialHostLabels.ADMIN)
            admin_hosts = p.filter_matching_hostspecs(self.inventory.all_specs())
            if len(admin_hosts) == 1 and admin_hosts[0] == host:
                raise OrchestratorValidationError(f"Host {host} is the last host with the '{SpecialHostLabels.ADMIN}'"
                                                  f" label.\nRemoving the {SpecialHostLabels.ADMIN} label from this host could cause the removal"
                                                  " of the last cluster config/keyring managed by cephadm.\n"
                                                  f"It is recommended to add the {SpecialHostLabels.ADMIN} label to another host"
                                                  " before completing this operation.\nIf you're certain this is"
                                                  " what you want rerun this command with --force.")
        if self.inventory.has_label(host, label):
            self.inventory.rm_label(host, label)
            msg = f'Removed label {label} from host {host}'
        else:
            msg = f"Host {host} does not have label '{label}'. Please use 'ceph orch host ls' to list all the labels."
        self.log.info(msg)
        self._kick_serve_loop()
        return msg

    def _host_ok_to_stop(self, hostname: str, force: bool = False) -> Tuple[int, str]:
        self.log.debug("running host-ok-to-stop checks")
        daemons = self.cache.get_daemons()
        daemon_map: Dict[str, List[str]] = defaultdict(lambda: [])
        for dd in daemons:
            assert dd.hostname is not None
            assert dd.daemon_type is not None
            assert dd.daemon_id is not None
            if dd.hostname == hostname:
                daemon_map[dd.daemon_type].append(dd.daemon_id)

        notifications: List[str] = []
        error_notifications: List[str] = []
        okay: bool = True
        for daemon_type, daemon_ids in daemon_map.items():
            r = self.cephadm_services[daemon_type_to_service(
                daemon_type)].ok_to_stop(daemon_ids, force=force)
            if r.retval:
                okay = False
                # collect error notifications so user can see every daemon causing host
                # to not be okay to stop
                error_notifications.append(r.stderr)
            if r.stdout:
                # if extra notifications to print for user, add them to notifications list
                notifications.append(r.stdout)

        if not okay:
            # at least one daemon is not okay to stop
            return 1, '\n'.join(error_notifications)

        if notifications:
            return 0, (f'It is presumed safe to stop host {hostname}. '
                       + 'Note the following:\n\n' + '\n'.join(notifications))
        return 0, f'It is presumed safe to stop host {hostname}'

    @handle_orch_error
    def host_ok_to_stop(self, hostname: str) -> str:
        if hostname not in self.cache.get_hosts():
            raise OrchestratorError(f'Cannot find host "{hostname}"')

        rc, msg = self._host_ok_to_stop(hostname)
        if rc:
            raise OrchestratorError(msg, errno=rc)

        self.log.info(msg)
        return msg

    def _set_maintenance_healthcheck(self) -> None:
        """Raise/update or clear the maintenance health check as needed"""

        in_maintenance = self.inventory.get_host_with_state("maintenance")
        if not in_maintenance:
            self.remove_health_warning('HOST_IN_MAINTENANCE')
        else:
            s = "host is" if len(in_maintenance) == 1 else "hosts are"
            self.set_health_warning("HOST_IN_MAINTENANCE", f"{len(in_maintenance)} {s} in maintenance mode", 1, [
                                    f"{h} is in maintenance" for h in in_maintenance])

    @handle_orch_error
    @host_exists()
    def enter_host_maintenance(self, hostname: str, force: bool = False, yes_i_really_mean_it: bool = False) -> str:
        """ Attempt to place a cluster host in maintenance

        Placing a host into maintenance disables the cluster's ceph target in systemd
        and stops all ceph daemons. If the host is an osd host we apply the noout flag
        for the host subtree in crush to prevent data movement during a host maintenance
        window.

        :param hostname: (str) name of the host (must match an inventory hostname)

        :raises OrchestratorError: Hostname is invalid, host is already in maintenance
        """
        if yes_i_really_mean_it and not force:
            raise OrchestratorError("--force must be passed with --yes-i-really-mean-it")

        if len(self.cache.get_hosts()) == 1 and not yes_i_really_mean_it:
            raise OrchestratorError("Maintenance feature is not supported on single node clusters")

        # if upgrade is active, deny
        if self.upgrade.upgrade_state and not yes_i_really_mean_it:
            raise OrchestratorError(
                f"Unable to place {hostname} in maintenance with upgrade active/paused")

        tgt_host = self.inventory._inventory[hostname]
        if tgt_host.get("status", "").lower() == "maintenance":
            raise OrchestratorError(f"Host {hostname} is already in maintenance")

        host_daemons = self.cache.get_daemon_types(hostname)
        self.log.debug("daemons on host {}".format(','.join(host_daemons)))
        if host_daemons:
            # daemons on this host, so check the daemons can be stopped
            # and if so, place the host into maintenance by disabling the target
            rc, msg = self._host_ok_to_stop(hostname, force)
            if rc and not yes_i_really_mean_it:
                raise OrchestratorError(
                    msg + '\nNote: Warnings can be bypassed with the --force flag', errno=rc)

            # call the host-maintenance function
            with self.async_timeout_handler(hostname, 'cephadm host-maintenance enter'):
                _out, _err, _code = self.wait_async(
                    CephadmServe(self)._run_cephadm(
                        hostname, cephadmNoImage, "host-maintenance",
                        ["enter"],
                        error_ok=True))
            returned_msg = _err[0].split('\n')[-1]
            if (returned_msg.startswith('failed') or returned_msg.startswith('ERROR')) and not yes_i_really_mean_it:
                raise OrchestratorError(
                    f"Failed to place {hostname} into maintenance for cluster {self._cluster_fsid}")

            if "osd" in host_daemons:
                crush_node = hostname if '.' not in hostname else hostname.split('.')[0]
                rc, out, err = self.mon_command({
                    'prefix': 'osd set-group',
                    'flags': 'noout',
                    'who': [crush_node],
                    'format': 'json'
                })
                if rc and not yes_i_really_mean_it:
                    self.log.warning(
                        f"maintenance mode request for {hostname} failed to SET the noout group (rc={rc})")
                    raise OrchestratorError(
                        f"Unable to set the osds on {hostname} to noout (rc={rc})")
                elif not rc:
                    self.log.info(
                        f"maintenance mode request for {hostname} has SET the noout group")

        # update the host status in the inventory
        tgt_host["status"] = "maintenance"
        self.inventory._inventory[hostname] = tgt_host
        self.inventory.save()

        self._set_maintenance_healthcheck()
        return f'Daemons for Ceph cluster {self._cluster_fsid} stopped on host {hostname}. Host {hostname} moved to maintenance mode'

    @handle_orch_error
    @host_exists()
    def exit_host_maintenance(self, hostname: str) -> str:
        """Exit maintenance mode and return a host to an operational state

        Returning from maintenance will enable the clusters systemd target and
        start it, and remove any noout that has been added for the host if the
        host has osd daemons

        :param hostname: (str) host name

        :raises OrchestratorError: Unable to return from maintenance, or unset the
                                   noout flag
        """
        tgt_host = self.inventory._inventory[hostname]
        if tgt_host['status'] != "maintenance":
            raise OrchestratorError(f"Host {hostname} is not in maintenance mode")

        with self.async_timeout_handler(hostname, 'cephadm host-maintenance exit'):
            outs, errs, _code = self.wait_async(
                CephadmServe(self)._run_cephadm(hostname, cephadmNoImage,
                                                'host-maintenance', ['exit'], error_ok=True))
        returned_msg = errs[0].split('\n')[-1]
        if returned_msg.startswith('failed') or returned_msg.startswith('ERROR'):
            raise OrchestratorError(
                f"Failed to exit maintenance state for host {hostname}, cluster {self._cluster_fsid}")

        if "osd" in self.cache.get_daemon_types(hostname):
            crush_node = hostname if '.' not in hostname else hostname.split('.')[0]
            rc, _out, _err = self.mon_command({
                'prefix': 'osd unset-group',
                'flags': 'noout',
                'who': [crush_node],
                'format': 'json'
            })
            if rc:
                self.log.warning(
                    f"exit maintenance request failed to UNSET the noout group for {hostname}, (rc={rc})")
                raise OrchestratorError(f"Unable to set the osds on {hostname} to noout (rc={rc})")
            else:
                self.log.info(
                    f"exit maintenance request has UNSET for the noout group on host {hostname}")

        # update the host record status
        tgt_host['status'] = ""
        self.inventory._inventory[hostname] = tgt_host
        self.inventory.save()

        self._set_maintenance_healthcheck()

        return f"Ceph cluster {self._cluster_fsid} on {hostname} has exited maintenance mode"

    @handle_orch_error
    @host_exists()
    def rescan_host(self, hostname: str) -> str:
        """Use cephadm to issue a disk rescan on each HBA

        Some HBAs and external enclosures don't automatically register
        device insertion with the kernel, so for these scenarios we need
        to manually rescan

        :param hostname: (str) host name
        """
        self.log.info(f'disk rescan request sent to host "{hostname}"')
        with self.async_timeout_handler(hostname, 'cephadm disk-rescan'):
            _out, _err, _code = self.wait_async(
                CephadmServe(self)._run_cephadm(hostname, cephadmNoImage, "disk-rescan",
                                                [], no_fsid=True, error_ok=True))
        if not _err:
            raise OrchestratorError('Unexpected response from cephadm disk-rescan call')

        msg = _err[0].split('\n')[-1]
        log_msg = f'disk rescan: {msg}'
        if msg.upper().startswith('OK'):
            self.log.info(log_msg)
        else:
            self.log.warning(log_msg)

        return f'{msg}'

    def get_minimal_ceph_conf(self) -> str:
        _, config, _ = self.check_mon_command({
            "prefix": "config generate-minimal-conf",
        })
        extra = self.extra_ceph_conf().conf
        if extra:
            try:
                config = self._combine_confs(config, extra)
            except Exception as e:
                self.log.error(f'Failed to add extra ceph conf settings to minimal ceph conf: {e}')
        return config

    def _combine_confs(self, conf1: str, conf2: str) -> str:
        section_to_option: Dict[str, List[str]] = {}
        final_conf: str = ''
        for conf in [conf1, conf2]:
            if not conf:
                continue
            section = ''
            for line in conf.split('\n'):
                if line.strip().startswith('#') or not line.strip():
                    continue
                if line.strip().startswith('[') and line.strip().endswith(']'):
                    section = line.strip().replace('[', '').replace(']', '')
                    if section not in section_to_option:
                        section_to_option[section] = []
                else:
                    section_to_option[section].append(line.strip())

        first_section = True
        for section, options in section_to_option.items():
            if not first_section:
                final_conf += '\n'
            final_conf += f'[{section}]\n'
            for option in options:
                final_conf += f'{option}\n'
            first_section = False

        return final_conf

    def _invalidate_daemons_and_kick_serve(self, filter_host: Optional[str] = None) -> None:
        if filter_host:
            self.cache.invalidate_host_daemons(filter_host)
        else:
            for h in self.cache.get_hosts():
                # Also discover daemons deployed manually
                self.cache.invalidate_host_daemons(h)

        self._kick_serve_loop()

    @handle_orch_error
    def describe_service(self, service_type: Optional[str] = None, service_name: Optional[str] = None,
                         refresh: bool = False) -> List[orchestrator.ServiceDescription]:
        if refresh:
            self._invalidate_daemons_and_kick_serve()
            self.log.debug('Kicked serve() loop to refresh all services')

        sm: Dict[str, orchestrator.ServiceDescription] = {}

        # known services
        for nm, spec in self.spec_store.all_specs.items():
            if service_type is not None and service_type != spec.service_type:
                continue
            if service_name is not None and service_name != nm:
                continue

            if spec.service_type != 'osd':
                size = spec.placement.get_target_count(self.cache.get_schedulable_hosts())
            else:
                # osd counting is special
                size = 0

            sm[nm] = orchestrator.ServiceDescription(
                spec=spec,
                size=size,
                running=0,
                events=self.events.get_for_service(spec.service_name()),
                created=self.spec_store.spec_created[nm],
                deleted=self.spec_store.spec_deleted.get(nm, None),
                virtual_ip=spec.get_virtual_ip(),
                ports=spec.get_port_start(),
            )
            if spec.service_type == 'ingress':
                # ingress has 2 daemons running per host
                # but only if it's the full ingress service, not for keepalive-only
                if not cast(IngressSpec, spec).keepalive_only:
                    sm[nm].size *= 2

        # factor daemons into status
        for h, dm in self.cache.get_daemons_with_volatile_status():
            for name, dd in dm.items():
                assert dd.hostname is not None, f'no hostname for {dd!r}'
                assert dd.daemon_type is not None, f'no daemon_type for {dd!r}'

                n: str = dd.service_name()

                if (
                    service_type
                    and service_type != daemon_type_to_service(dd.daemon_type)
                ):
                    continue
                if service_name and service_name != n:
                    continue

                if n not in sm:
                    # new unmanaged service
                    spec = ServiceSpec(
                        unmanaged=True,
                        service_type=daemon_type_to_service(dd.daemon_type),
                        service_id=dd.service_id(),
                    )
                    sm[n] = orchestrator.ServiceDescription(
                        last_refresh=dd.last_refresh,
                        container_image_id=dd.container_image_id,
                        container_image_name=dd.container_image_name,
                        spec=spec,
                        size=0,
                    )

                if dd.status == DaemonDescriptionStatus.running:
                    sm[n].running += 1
                if dd.daemon_type == 'osd':
                    # The osd count can't be determined by the Placement spec.
                    # Showing an actual/expected representation cannot be determined
                    # here. So we're setting running = size for now.
                    sm[n].size += 1
                if (
                    not sm[n].last_refresh
                    or not dd.last_refresh
                    or dd.last_refresh < sm[n].last_refresh  # type: ignore
                ):
                    sm[n].last_refresh = dd.last_refresh

        return list(sm.values())

    @handle_orch_error
    def list_daemons(self,
                     service_name: Optional[str] = None,
                     daemon_type: Optional[str] = None,
                     daemon_id: Optional[str] = None,
                     host: Optional[str] = None,
                     refresh: bool = False) -> List[orchestrator.DaemonDescription]:
        if refresh:
            self._invalidate_daemons_and_kick_serve(host)
            self.log.debug('Kicked serve() loop to refresh all daemons')

        result = []
        for h, dm in self.cache.get_daemons_with_volatile_status():
            if host and h != host:
                continue
            for name, dd in dm.items():
                if daemon_type is not None and daemon_type != dd.daemon_type:
                    continue
                if daemon_id is not None and daemon_id != dd.daemon_id:
                    continue
                if service_name is not None and service_name != dd.service_name():
                    continue
                if not dd.memory_request and dd.daemon_type in ['osd', 'mon']:
                    dd.memory_request = cast(Optional[int], self.get_foreign_ceph_option(
                        dd.name(),
                        f"{dd.daemon_type}_memory_target"
                    ))
                result.append(dd)
        return result

    @handle_orch_error
    def service_action(self, action: str, service_name: str) -> List[str]:
        if service_name not in self.spec_store.all_specs.keys():
            raise OrchestratorError(f'Invalid service name "{service_name}".'
                                    + ' View currently running services using "ceph orch ls"')
        dds: List[DaemonDescription] = self.cache.get_daemons_by_service(service_name)
        if not dds:
            raise OrchestratorError(f'No daemons exist under service name "{service_name}".'
                                    + ' View currently running services using "ceph orch ls"')
        if action == 'stop' and service_name.split('.')[0].lower() in ['mgr', 'mon', 'osd']:
            return [f'Stopping entire {service_name} service is prohibited.']
        self.log.info('%s service %s' % (action.capitalize(), service_name))
        return [
            self._schedule_daemon_action(dd.name(), action)
            for dd in dds
        ]

    def _rotate_daemon_key(self, daemon_spec: CephadmDaemonDeploySpec) -> str:
        self.log.info(f'Rotating authentication key for {daemon_spec.name()}')
        rc, out, err = self.mon_command({
            'prefix': 'auth get-or-create-pending',
            'entity': daemon_spec.entity_name(),
            'format': 'json',
        })
        j = json.loads(out)
        pending_key = j[0]['pending_key']

        # deploy a new keyring file
        if daemon_spec.daemon_type != 'osd':
            daemon_spec = self.cephadm_services[daemon_type_to_service(
                daemon_spec.daemon_type)].prepare_create(daemon_spec)
        with self.async_timeout_handler(daemon_spec.host, f'cephadm deploy ({daemon_spec.daemon_type} daemon)'):
            self.wait_async(CephadmServe(self)._create_daemon(daemon_spec, reconfig=True))

        # try to be clever, or fall back to restarting the daemon
        rc = -1
        if daemon_spec.daemon_type == 'osd':
            rc, out, err = self.tool_exec(
                args=['ceph', 'tell', daemon_spec.name(), 'rotate-stored-key', '-i', '-'],
                stdin=pending_key.encode()
            )
            if not rc:
                rc, out, err = self.tool_exec(
                    args=['ceph', 'tell', daemon_spec.name(), 'rotate-key', '-i', '-'],
                    stdin=pending_key.encode()
                )
        elif daemon_spec.daemon_type == 'mds':
            rc, out, err = self.tool_exec(
                args=['ceph', 'tell', daemon_spec.name(), 'rotate-key', '-i', '-'],
                stdin=pending_key.encode()
            )
        elif (
                daemon_spec.daemon_type == 'mgr'
                and daemon_spec.daemon_id == self.get_mgr_id()
        ):
            rc, out, err = self.tool_exec(
                args=['ceph', 'tell', daemon_spec.name(), 'rotate-key', '-i', '-'],
                stdin=pending_key.encode()
            )
        if rc:
            self._daemon_action(daemon_spec, 'restart')

        return f'Rotated key for {daemon_spec.name()}'

    def _daemon_action(self,
                       daemon_spec: CephadmDaemonDeploySpec,
                       action: str,
                       image: Optional[str] = None) -> str:
        self._daemon_action_set_image(action, image, daemon_spec.daemon_type,
                                      daemon_spec.daemon_id)

        if (action == 'redeploy' or action == 'restart') and self.daemon_is_self(daemon_spec.daemon_type,
                                                                                 daemon_spec.daemon_id):
            self.mgr_service.fail_over()
            return ''  # unreachable

        if action == 'rotate-key':
            return self._rotate_daemon_key(daemon_spec)

        if action == 'redeploy' or action == 'reconfig':
            if daemon_spec.daemon_type != 'osd':
                daemon_spec = self.cephadm_services[daemon_type_to_service(
                    daemon_spec.daemon_type)].prepare_create(daemon_spec)
            else:
                # for OSDs, we still need to update config, just not carry out the full
                # prepare_create function
                daemon_spec.final_config, daemon_spec.deps = self.osd_service.generate_config(
                    daemon_spec)
            with self.async_timeout_handler(daemon_spec.host, f'cephadm deploy ({daemon_spec.daemon_type} daemon)'):
                return self.wait_async(
                    CephadmServe(self)._create_daemon(daemon_spec, reconfig=(action == 'reconfig')))

        actions = {
            'start': ['reset-failed', 'start'],
            'stop': ['stop'],
            'restart': ['reset-failed', 'restart'],
        }
        name = daemon_spec.name()
        for a in actions[action]:
            try:
                with self.async_timeout_handler(daemon_spec.host, f'cephadm unit --name {name}'):
                    out, err, code = self.wait_async(CephadmServe(self)._run_cephadm(
                        daemon_spec.host, name, 'unit',
                        ['--name', name, a]))
            except Exception:
                self.log.exception(f'`{daemon_spec.host}: cephadm unit {name} {a}` failed')
        self.cache.invalidate_host_daemons(daemon_spec.host)
        msg = "{} {} from host '{}'".format(action, name, daemon_spec.host)
        self.events.for_daemon(name, 'INFO', msg)
        return msg

    def _daemon_action_set_image(self, action: str, image: Optional[str], daemon_type: str, daemon_id: str) -> None:
        if image is not None:
            if action != 'redeploy':
                raise OrchestratorError(
                    f'Cannot execute {action} with new image. `action` needs to be `redeploy`')
            if daemon_type not in CEPH_IMAGE_TYPES:
                raise OrchestratorError(
                    f'Cannot redeploy {daemon_type}.{daemon_id} with a new image: Supported '
                    f'types are: {", ".join(CEPH_IMAGE_TYPES)}')

            self.check_mon_command({
                'prefix': 'config set',
                'name': 'container_image',
                'value': image,
                'who': utils.name_to_config_section(daemon_type + '.' + daemon_id),
            })

    @handle_orch_error
    def daemon_action(self, action: str, daemon_name: str, image: Optional[str] = None) -> str:
        d = self.cache.get_daemon(daemon_name)
        assert d.daemon_type is not None
        assert d.daemon_id is not None

        if (action == 'redeploy' or action == 'restart') and self.daemon_is_self(d.daemon_type, d.daemon_id) \
                and not self.mgr_service.mgr_map_has_standby():
            raise OrchestratorError(
                f'Unable to schedule redeploy for {daemon_name}: No standby MGRs')

        if action == 'rotate-key':
            if d.daemon_type not in ['mgr', 'osd', 'mds',
                                     'rgw', 'crash', 'nfs', 'rbd-mirror', 'iscsi']:
                raise OrchestratorError(
                    f'key rotation not supported for {d.daemon_type}'
                )

        self._daemon_action_set_image(action, image, d.daemon_type, d.daemon_id)

        self.log.info(f'Schedule {action} daemon {daemon_name}')
        return self._schedule_daemon_action(daemon_name, action)

    def daemon_is_self(self, daemon_type: str, daemon_id: str) -> bool:
        return daemon_type == 'mgr' and daemon_id == self.get_mgr_id()

    def get_active_mgr(self) -> DaemonDescription:
        return self.mgr_service.get_active_daemon(self.cache.get_daemons_by_type('mgr'))

    def get_active_mgr_digests(self) -> List[str]:
        digests = self.mgr_service.get_active_daemon(
            self.cache.get_daemons_by_type('mgr')).container_image_digests
        return digests if digests else []

    def _schedule_daemon_action(self, daemon_name: str, action: str) -> str:
        dd = self.cache.get_daemon(daemon_name)
        assert dd.daemon_type is not None
        assert dd.daemon_id is not None
        assert dd.hostname is not None
        if (action == 'redeploy' or action == 'restart') and self.daemon_is_self(dd.daemon_type, dd.daemon_id) \
                and not self.mgr_service.mgr_map_has_standby():
            raise OrchestratorError(
                f'Unable to schedule redeploy for {daemon_name}: No standby MGRs')
        self.cache.schedule_daemon_action(dd.hostname, dd.name(), action)
        self.cache.save_host(dd.hostname)
        msg = "Scheduled to {} {} on host '{}'".format(action, daemon_name, dd.hostname)
        self._kick_serve_loop()
        return msg

    @handle_orch_error
    def remove_daemons(self, names):
        # type: (List[str]) -> List[str]
        args = []
        for host, dm in self.cache.daemons.items():
            for name in names:
                if name in dm:
                    args.append((name, host))
        if not args:
            raise OrchestratorError('Unable to find daemon(s) %s' % (names))
        self.log.info('Remove daemons %s' % ' '.join([a[0] for a in args]))
        return self._remove_daemons(args)

    @handle_orch_error
    def remove_service(self, service_name: str, force: bool = False) -> str:
        self.log.info('Remove service %s' % service_name)
        self._trigger_preview_refresh(service_name=service_name)
        if service_name in self.spec_store:
            if self.spec_store[service_name].spec.service_type in ('mon', 'mgr'):
                return f'Unable to remove {service_name} service.\n' \
                       f'Note, you might want to mark the {service_name} service as "unmanaged"'
        else:
            return f"Invalid service '{service_name}'. Use 'ceph orch ls' to list available services.\n"

        # Report list of affected OSDs?
        if not force and service_name.startswith('osd.'):
            osds_msg = {}
            for h, dm in self.cache.get_daemons_with_volatile_status():
                osds_to_remove = []
                for name, dd in dm.items():
                    if dd.daemon_type == 'osd' and dd.service_name() == service_name:
                        osds_to_remove.append(str(dd.daemon_id))
                if osds_to_remove:
                    osds_msg[h] = osds_to_remove
            if osds_msg:
                msg = ''
                for h, ls in osds_msg.items():
                    msg += f'\thost {h}: {" ".join([f"osd.{id}" for id in ls])}'
                raise OrchestratorError(
                    f'If {service_name} is removed then the following OSDs will remain, --force to proceed anyway\n{msg}')

        found = self.spec_store.rm(service_name)
        if found and service_name.startswith('osd.'):
            self.spec_store.finally_rm(service_name)
        self._kick_serve_loop()
        return f'Removed service {service_name}'

    @handle_orch_error
    def get_inventory(self, host_filter: Optional[orchestrator.InventoryFilter] = None, refresh: bool = False) -> List[orchestrator.InventoryHost]:
        """
        Return the storage inventory of hosts matching the given filter.

        :param host_filter: host filter

        TODO:
          - add filtering by label
        """
        if refresh:
            if host_filter and host_filter.hosts:
                for h in host_filter.hosts:
                    self.log.debug(f'will refresh {h} devs')
                    self.cache.invalidate_host_devices(h)
                    self.cache.invalidate_host_networks(h)
            else:
                for h in self.cache.get_hosts():
                    self.log.debug(f'will refresh {h} devs')
                    self.cache.invalidate_host_devices(h)
                    self.cache.invalidate_host_networks(h)

            self.event.set()
            self.log.debug('Kicked serve() loop to refresh devices')

        result = []
        for host, dls in self.cache.devices.items():
            if host_filter and host_filter.hosts and host not in host_filter.hosts:
                continue
            result.append(orchestrator.InventoryHost(host,
                                                     inventory.Devices(dls)))
        return result

    @handle_orch_error
    def zap_device(self, host: str, path: str) -> str:
        """Zap a device on a managed host.

        Use ceph-volume zap to return a device to an unused/free state

        Args:
            host (str): hostname of the cluster host
            path (str): device path

        Raises:
            OrchestratorError: host is not a cluster host
            OrchestratorError: host is in maintenance and therefore unavailable
            OrchestratorError: device path not found on the host
            OrchestratorError: device is known to a different ceph cluster
            OrchestratorError: device holds active osd
            OrchestratorError: device cache hasn't been populated yet..

        Returns:
            str: output from the zap command
        """

        self.log.info('Zap device %s:%s' % (host, path))

        if host not in self.inventory.keys():
            raise OrchestratorError(
                f"Host '{host}' is not a member of the cluster")

        host_info = self.inventory._inventory.get(host, {})
        if host_info.get('status', '').lower() == 'maintenance':
            raise OrchestratorError(
                f"Host '{host}' is in maintenance mode, which prevents any actions against it.")

        if host not in self.cache.devices:
            raise OrchestratorError(
                f"Host '{host} hasn't been scanned yet to determine it's inventory. Please try again later.")

        host_devices = self.cache.devices[host]
        path_found = False
        osd_id_list: List[str] = []

        for dev in host_devices:
            if dev.path == path:
                path_found = True
                break
        if not path_found:
            raise OrchestratorError(
                f"Device path '{path}' not found on host '{host}'")

        if osd_id_list:
            dev_name = os.path.basename(path)
            active_osds: List[str] = []
            for osd_id in osd_id_list:
                metadata = self.get_metadata('osd', str(osd_id))
                if metadata:
                    if metadata.get('hostname', '') == host and dev_name in metadata.get('devices', '').split(','):
                        active_osds.append("osd." + osd_id)
            if active_osds:
                raise OrchestratorError(
                    f"Unable to zap: device '{path}' on {host} has {len(active_osds)} active "
                    f"OSD{'s' if len(active_osds) > 1 else ''}"
                    f" ({', '.join(active_osds)}). Use 'ceph orch osd rm' first.")

        cv_args = ['--', 'lvm', 'zap', '--destroy', path]
        with self.async_timeout_handler(host, f'cephadm ceph-volume {" ".join(cv_args)}'):
            out, err, code = self.wait_async(CephadmServe(self)._run_cephadm(
                host, 'osd', 'ceph-volume', cv_args, error_ok=True))

        self.cache.invalidate_host_devices(host)
        self.cache.invalidate_host_networks(host)
        if code:
            raise OrchestratorError('Zap failed: %s' % '\n'.join(out + err))
        msg = f'zap successful for {path} on {host}'
        self.log.info(msg)

        return msg + '\n'

    @handle_orch_error
    def blink_device_light(self, ident_fault: str, on: bool, locs: List[orchestrator.DeviceLightLoc]) -> List[str]:
        """
        Blink a device light. Calling something like::

          lsmcli local-disk-ident-led-on --path $path

        If you must, you can customize this via::

          ceph config-key set mgr/cephadm/blink_device_light_cmd '<my jinja2 template>'
          ceph config-key set mgr/cephadm/<host>/blink_device_light_cmd '<my jinja2 template>'

        See templates/blink_device_light_cmd.j2
        """
        @forall_hosts
        def blink(host: str, dev: str, path: str) -> str:
            cmd_line = self.template.render('blink_device_light_cmd.j2',
                                            {
                                                'on': on,
                                                'ident_fault': ident_fault,
                                                'dev': dev,
                                                'path': path
                                            },
                                            host=host)
            cmd_args = shlex.split(cmd_line)

            with self.async_timeout_handler(host, f'cephadm shell -- {" ".join(cmd_args)}'):
                out, err, code = self.wait_async(CephadmServe(self)._run_cephadm(
                    host, 'osd', 'shell', ['--'] + cmd_args,
                    error_ok=True))
            if code:
                raise OrchestratorError(
                    'Unable to affect %s light for %s:%s. Command: %s' % (
                        ident_fault, host, dev, ' '.join(cmd_args)))
            self.log.info('Set %s light for %s:%s %s' % (
                ident_fault, host, dev, 'on' if on else 'off'))
            return "Set %s light for %s:%s %s" % (
                ident_fault, host, dev, 'on' if on else 'off')

        return blink(locs)

    def get_osd_uuid_map(self, only_up=False):
        # type: (bool) -> Dict[str, str]
        osd_map = self.get('osd_map')
        r = {}
        for o in osd_map['osds']:
            # only include OSDs that have ever started in this map.  this way
            # an interrupted osd create can be repeated and succeed the second
            # time around.
            osd_id = o.get('osd')
            if osd_id is None:
                raise OrchestratorError("Could not retrieve osd_id from osd_map")
            if not only_up:
                r[str(osd_id)] = o.get('uuid', '')
        return r

    def get_osd_by_id(self, osd_id: int) -> Optional[Dict[str, Any]]:
        osd = [x for x in self.get('osd_map')['osds']
               if x['osd'] == osd_id]

        if len(osd) != 1:
            return None

        return osd[0]

    def _trigger_preview_refresh(self,
                                 specs: Optional[List[DriveGroupSpec]] = None,
                                 service_name: Optional[str] = None,
                                 ) -> None:
        # Only trigger a refresh when a spec has changed
        trigger_specs = []
        if specs:
            for spec in specs:
                preview_spec = self.spec_store.spec_preview.get(spec.service_name())
                # the to-be-preview spec != the actual spec, this means we need to
                # trigger a refresh, if the spec has been removed (==None) we need to
                # refresh as well.
                if not preview_spec or spec != preview_spec:
                    trigger_specs.append(spec)
        if service_name:
            trigger_specs = [cast(DriveGroupSpec, self.spec_store.spec_preview.get(service_name))]
        if not any(trigger_specs):
            return None

        refresh_hosts = self.osd_service.resolve_hosts_for_osdspecs(specs=trigger_specs)
        for host in refresh_hosts:
            self.log.info(f"Marking host: {host} for OSDSpec preview refresh.")
            self.cache.osdspec_previews_refresh_queue.append(host)

    @handle_orch_error
    def apply_drivegroups(self, specs: List[DriveGroupSpec]) -> List[str]:
        """
        Deprecated. Please use `apply()` instead.

        Keeping this around to be compatible to mgr/dashboard
        """
        return [self._apply(spec) for spec in specs]

    @handle_orch_error
    def create_osds(self, drive_group: DriveGroupSpec) -> str:
        hosts: List[HostSpec] = self.inventory.all_specs()
        filtered_hosts: List[str] = drive_group.placement.filter_matching_hostspecs(hosts)
        if not filtered_hosts:
            return "Invalid 'host:device' spec: host not found in cluster. Please check 'ceph orch host ls' for available hosts"
        return self.osd_service.create_from_spec(drive_group)

    def _preview_osdspecs(self,
                          osdspecs: Optional[List[DriveGroupSpec]] = None
                          ) -> dict:
        if not osdspecs:
            return {'n/a': [{'error': True,
                             'message': 'No OSDSpec or matching hosts found.'}]}
        matching_hosts = self.osd_service.resolve_hosts_for_osdspecs(specs=osdspecs)
        if not matching_hosts:
            return {'n/a': [{'error': True,
                             'message': 'No OSDSpec or matching hosts found.'}]}
        # Is any host still loading previews or still in the queue to be previewed
        pending_hosts = {h for h in self.cache.loading_osdspec_preview if h in matching_hosts}
        if pending_hosts or any(item in self.cache.osdspec_previews_refresh_queue for item in matching_hosts):
            # Report 'pending' when any of the matching hosts is still loading previews (flag is True)
            return {'n/a': [{'error': True,
                             'message': 'Preview data is being generated.. '
                                        'Please re-run this command in a bit.'}]}
        # drop all keys that are not in search_hosts and only select reports that match the requested osdspecs
        previews_for_specs = {}
        for host, raw_reports in self.cache.osdspec_previews.items():
            if host not in matching_hosts:
                continue
            osd_reports = []
            for osd_report in raw_reports:
                if osd_report.get('osdspec') in [x.service_id for x in osdspecs]:
                    osd_reports.append(osd_report)
            previews_for_specs.update({host: osd_reports})
        return previews_for_specs

    def _calc_daemon_deps(self,
                          spec: Optional[ServiceSpec],
                          daemon_type: str,
                          daemon_id: str) -> List[str]:

        def get_daemon_names(daemons: List[str]) -> List[str]:
            daemon_names = []
            for daemon_type in daemons:
                for dd in self.cache.get_daemons_by_type(daemon_type):
                    daemon_names.append(dd.name())
            return daemon_names

        alertmanager_user, alertmanager_password = self._get_alertmanager_credentials()
        prometheus_user, prometheus_password = self._get_prometheus_credentials()

        deps = []
        if daemon_type == 'haproxy':
            # because cephadm creates new daemon instances whenever
            # port or ip changes, identifying daemons by name is
            # sufficient to detect changes.
            if not spec:
                return []
            ingress_spec = cast(IngressSpec, spec)
            assert ingress_spec.backend_service
            daemons = self.cache.get_daemons_by_service(ingress_spec.backend_service)
            deps = [d.name() for d in daemons]
        elif daemon_type == 'keepalived':
            # because cephadm creates new daemon instances whenever
            # port or ip changes, identifying daemons by name is
            # sufficient to detect changes.
            if not spec:
                return []
            daemons = self.cache.get_daemons_by_service(spec.service_name())
            deps = [d.name() for d in daemons if d.daemon_type == 'haproxy']
        elif daemon_type == 'agent':
            root_cert = ''
            server_port = ''
            try:
                server_port = str(self.http_server.agent.server_port)
                root_cert = self.http_server.agent.ssl_certs.get_root_cert()
            except Exception:
                pass
            deps = sorted([self.get_mgr_ip(), server_port, root_cert,
                           str(self.device_enhanced_scan)])
        elif daemon_type == 'node-proxy':
            root_cert = ''
            server_port = ''
            try:
                server_port = str(self.http_server.agent.server_port)
                root_cert = self.http_server.agent.ssl_certs.get_root_cert()
            except Exception:
                pass
            deps = sorted([self.get_mgr_ip(), server_port, root_cert])
        elif daemon_type == 'iscsi':
            if spec:
                iscsi_spec = cast(IscsiServiceSpec, spec)
                deps = [self.iscsi_service.get_trusted_ips(iscsi_spec)]
            else:
                deps = [self.get_mgr_ip()]
        elif daemon_type == 'prometheus':
            # for prometheus we add the active mgr as an explicit dependency,
            # this way we force a redeploy after a mgr failover
            deps.append(self.get_active_mgr().name())
            deps.append(str(self.get_module_option_ex('prometheus', 'server_port', 9283)))
            deps.append(str(self.service_discovery_port))
            # prometheus yaml configuration file (generated by prometheus.yml.j2) contains
            # a scrape_configs section for each service type. This should be included only
            # when at least one daemon of the corresponding service is running. Therefore,
            # an explicit dependency is added for each service-type to force a reconfig
            # whenever the number of daemons for those service-type changes from 0 to greater
            # than zero and vice versa.
            deps += [s for s in ['node-exporter', 'alertmanager']
                     if self.cache.get_daemons_by_service(s)]
            if len(self.cache.get_daemons_by_type('ingress')) > 0:
                deps.append('ingress')
            # add dependency on ceph-exporter daemons
            deps += [d.name() for d in self.cache.get_daemons_by_service('ceph-exporter')]
            if self.secure_monitoring_stack:
                if prometheus_user and prometheus_password:
                    deps.append(f'{hash(prometheus_user + prometheus_password)}')
                if alertmanager_user and alertmanager_password:
                    deps.append(f'{hash(alertmanager_user + alertmanager_password)}')
        elif daemon_type == 'grafana':
            deps += get_daemon_names(['prometheus', 'loki'])
            if self.secure_monitoring_stack and prometheus_user and prometheus_password:
                deps.append(f'{hash(prometheus_user + prometheus_password)}')
        elif daemon_type == 'alertmanager':
            deps += get_daemon_names(['mgr', 'alertmanager', 'snmp-gateway'])
            if self.secure_monitoring_stack and alertmanager_user and alertmanager_password:
                deps.append(f'{hash(alertmanager_user + alertmanager_password)}')
        elif daemon_type == 'promtail':
            deps += get_daemon_names(['loki'])
        elif daemon_type == JaegerAgentService.TYPE:
            for dd in self.cache.get_daemons_by_type(JaegerCollectorService.TYPE):
                assert dd.hostname is not None
                port = dd.ports[0] if dd.ports else JaegerCollectorService.DEFAULT_SERVICE_PORT
                deps.append(build_url(host=dd.hostname, port=port).lstrip('/'))
            deps = sorted(deps)
        else:
            # TODO(redo): some error message!
            pass

        if daemon_type in ['prometheus', 'node-exporter', 'alertmanager', 'grafana']:
            deps.append(f'secure_monitoring_stack:{self.secure_monitoring_stack}')

        return sorted(deps)

    @forall_hosts
    def _remove_daemons(self, name: str, host: str) -> str:
        return CephadmServe(self)._remove_daemon(name, host)

    def _check_pool_exists(self, pool: str, service_name: str) -> None:
        logger.info(f'Checking pool "{pool}" exists for service {service_name}')
        if not self.rados.pool_exists(pool):
            raise OrchestratorError(f'Cannot find pool "{pool}" for '
                                    f'service {service_name}')

    def _add_daemon(self,
                    daemon_type: str,
                    spec: ServiceSpec) -> List[str]:
        """
        Add (and place) a daemon. Require explicit host placement.  Do not
        schedule, and do not apply the related scheduling limitations.
        """
        if spec.service_name() not in self.spec_store:
            raise OrchestratorError('Unable to add a Daemon without Service.\n'
                                    'Please use `ceph orch apply ...` to create a Service.\n'
                                    'Note, you might want to create the service with "unmanaged=true"')

        self.log.debug('_add_daemon %s spec %s' % (daemon_type, spec.placement))
        if not spec.placement.hosts:
            raise OrchestratorError('must specify host(s) to deploy on')
        count = spec.placement.count or len(spec.placement.hosts)
        daemons = self.cache.get_daemons_by_service(spec.service_name())
        return self._create_daemons(daemon_type, spec, daemons,
                                    spec.placement.hosts, count)

    def _create_daemons(self,
                        daemon_type: str,
                        spec: ServiceSpec,
                        daemons: List[DaemonDescription],
                        hosts: List[HostPlacementSpec],
                        count: int) -> List[str]:
        if count > len(hosts):
            raise OrchestratorError('too few hosts: want %d, have %s' % (
                count, hosts))

        did_config = False
        service_type = daemon_type_to_service(daemon_type)

        args = []  # type: List[CephadmDaemonDeploySpec]
        for host, network, name in hosts:
            daemon_id = self.get_unique_name(daemon_type, host, daemons,
                                             prefix=spec.service_id,
                                             forcename=name)

            if not did_config:
                self.cephadm_services[service_type].config(spec)
                did_config = True

            daemon_spec = self.cephadm_services[service_type].make_daemon_spec(
                host, daemon_id, network, spec,
                # NOTE: this does not consider port conflicts!
                ports=spec.get_port_start())
            self.log.debug('Placing %s.%s on host %s' % (
                daemon_type, daemon_id, host))
            args.append(daemon_spec)

            # add to daemon list so next name(s) will also be unique
            sd = orchestrator.DaemonDescription(
                hostname=host,
                daemon_type=daemon_type,
                daemon_id=daemon_id,
            )
            daemons.append(sd)

        @ forall_hosts
        def create_func_map(*args: Any) -> str:
            daemon_spec = self.cephadm_services[daemon_type].prepare_create(*args)
            with self.async_timeout_handler(daemon_spec.host, f'cephadm deploy ({daemon_spec.daemon_type} daemon)'):
                return self.wait_async(CephadmServe(self)._create_daemon(daemon_spec))

        return create_func_map(args)

    @handle_orch_error
    def add_daemon(self, spec: ServiceSpec) -> List[str]:
        ret: List[str] = []
        try:
            with orchestrator.set_exception_subject('service', spec.service_name(), overwrite=True):
                for d_type in service_to_daemon_types(spec.service_type):
                    ret.extend(self._add_daemon(d_type, spec))
                return ret
        except OrchestratorError as e:
            self.events.from_orch_error(e)
            raise

    def _get_alertmanager_credentials(self) -> Tuple[str, str]:
        user = self.get_store(AlertmanagerService.USER_CFG_KEY)
        password = self.get_store(AlertmanagerService.PASS_CFG_KEY)
        if user is None or password is None:
            user = 'admin'
            password = 'admin'
            self.set_store(AlertmanagerService.USER_CFG_KEY, user)
            self.set_store(AlertmanagerService.PASS_CFG_KEY, password)
        return (user, password)

    def _get_prometheus_credentials(self) -> Tuple[str, str]:
        user = self.get_store(PrometheusService.USER_CFG_KEY)
        password = self.get_store(PrometheusService.PASS_CFG_KEY)
        if user is None or password is None:
            user = 'admin'
            password = 'admin'
            self.set_store(PrometheusService.USER_CFG_KEY, user)
            self.set_store(PrometheusService.PASS_CFG_KEY, password)
        return (user, password)

    @handle_orch_error
    def set_prometheus_access_info(self, user: str, password: str) -> str:
        self.set_store(PrometheusService.USER_CFG_KEY, user)
        self.set_store(PrometheusService.PASS_CFG_KEY, password)
        return 'prometheus credentials updated correctly'

    @handle_orch_error
    def set_prometheus_target(self, url: str) -> str:
        prometheus_spec = cast(PrometheusSpec, self.spec_store['prometheus'].spec)
        if url not in prometheus_spec.targets:
            prometheus_spec.targets.append(url)
        else:
            return f"Target '{url}' already exists.\n"
        if not prometheus_spec:
            return "Service prometheus not found\n"
        daemons: List[orchestrator.DaemonDescription] = self.cache.get_daemons_by_type('prometheus')
        spec = ServiceSpec.from_json(prometheus_spec.to_json())
        self.apply([spec], no_overwrite=False)
        for daemon in daemons:
            self.daemon_action(action='redeploy', daemon_name=daemon.daemon_name)
        return 'prometheus multi-cluster targets updated'

    @handle_orch_error
    def remove_prometheus_target(self, url: str) -> str:
        prometheus_spec = cast(PrometheusSpec, self.spec_store['prometheus'].spec)
        if url in prometheus_spec.targets:
            prometheus_spec.targets.remove(url)
        else:
            return f"Target '{url}' does not exist.\n"
        if not prometheus_spec:
            return "Service prometheus not found\n"
        daemons: List[orchestrator.DaemonDescription] = self.cache.get_daemons_by_type('prometheus')
        spec = ServiceSpec.from_json(prometheus_spec.to_json())
        self.apply([spec], no_overwrite=False)
        for daemon in daemons:
            self.daemon_action(action='redeploy', daemon_name=daemon.daemon_name)
        return 'prometheus multi-cluster targets updated'

    @handle_orch_error
    def set_alertmanager_access_info(self, user: str, password: str) -> str:
        self.set_store(AlertmanagerService.USER_CFG_KEY, user)
        self.set_store(AlertmanagerService.PASS_CFG_KEY, password)
        return 'alertmanager credentials updated correctly'

    @handle_orch_error
    def get_prometheus_access_info(self) -> Dict[str, str]:
        user, password = self._get_prometheus_credentials()
        return {'user': user,
                'password': password,
                'certificate': self.http_server.service_discovery.ssl_certs.get_root_cert()}

    @handle_orch_error
    def get_alertmanager_access_info(self) -> Dict[str, str]:
        user, password = self._get_alertmanager_credentials()
        return {'user': user,
                'password': password,
                'certificate': self.http_server.service_discovery.ssl_certs.get_root_cert()}

    @handle_orch_error
    def apply_mon(self, spec: ServiceSpec) -> str:
        return self._apply(spec)

    def _apply(self, spec: GenericSpec) -> str:
        if spec.service_type == 'host':
            return self._add_host(cast(HostSpec, spec))

        if spec.service_type == 'osd':
            # _trigger preview refresh needs to be smart and
            # should only refresh if a change has been detected
            self._trigger_preview_refresh(specs=[cast(DriveGroupSpec, spec)])

        return self._apply_service_spec(cast(ServiceSpec, spec))

    def _get_candidate_hosts(self, placement: PlacementSpec) -> List[str]:
        """Return a list of candidate hosts according to the placement specification."""
        all_hosts = self.cache.get_schedulable_hosts()
        candidates = []
        if placement.hosts:
            candidates = [h.hostname for h in placement.hosts if h.hostname in placement.hosts]
        elif placement.label:
            candidates = [x.hostname for x in [h for h in all_hosts if placement.label in h.labels]]
        elif placement.host_pattern:
            candidates = [x for x in placement.filter_matching_hostspecs(all_hosts)]
        elif (placement.count is not None or placement.count_per_host is not None):
            candidates = [x.hostname for x in all_hosts]
        return [h for h in candidates if not self.cache.is_host_draining(h)]

    def _validate_one_shot_placement_spec(self, spec: PlacementSpec) -> None:
        """Validate placement specification for TunedProfileSpec and ClientKeyringSpec."""
        if spec.count is not None:
            raise OrchestratorError(
                "Placement 'count' field is no supported for this specification.")
        if spec.count_per_host is not None:
            raise OrchestratorError(
                "Placement 'count_per_host' field is no supported for this specification.")
        if spec.hosts:
            all_hosts = [h.hostname for h in self.inventory.all_specs()]
            invalid_hosts = [h.hostname for h in spec.hosts if h.hostname not in all_hosts]
            if invalid_hosts:
                raise OrchestratorError(f"Found invalid host(s) in placement section: {invalid_hosts}. "
                                        f"Please check 'ceph orch host ls' for available hosts.")
        elif not self._get_candidate_hosts(spec):
            raise OrchestratorError("Invalid placement specification. No host(s) matched placement spec.\n"
                                    "Please check 'ceph orch host ls' for available hosts.\n"
                                    "Note: draining hosts are excluded from the candidate list.")

    def _validate_tunedprofile_settings(self, spec: TunedProfileSpec) -> Dict[str, List[str]]:
        candidate_hosts = spec.placement.filter_matching_hostspecs(self.inventory.all_specs())
        invalid_options: Dict[str, List[str]] = {}
        for host in candidate_hosts:
            host_sysctl_options = self.cache.get_facts(host).get('sysctl_options', {})
            invalid_options[host] = []
            for option in spec.settings:
                if option not in host_sysctl_options:
                    invalid_options[host].append(option)
        return invalid_options

    def _validate_tuned_profile_spec(self, spec: TunedProfileSpec) -> None:
        if not spec.settings:
            raise OrchestratorError("Invalid spec: settings section cannot be empty.")
        self._validate_one_shot_placement_spec(spec.placement)
        invalid_options = self._validate_tunedprofile_settings(spec)
        if any(e for e in invalid_options.values()):
            raise OrchestratorError(
                f'Failed to apply tuned profile. Invalid sysctl option(s) for host(s) detected: {invalid_options}')

    @handle_orch_error
    def apply_tuned_profiles(self, specs: List[TunedProfileSpec], no_overwrite: bool = False) -> str:
        outs = []
        for spec in specs:
            self._validate_tuned_profile_spec(spec)
            if no_overwrite and self.tuned_profiles.exists(spec.profile_name):
                outs.append(
                    f"Tuned profile '{spec.profile_name}' already exists (--no-overwrite was passed)")
            else:
                # done, let's save the specs
                self.tuned_profiles.add_profile(spec)
                outs.append(f'Saved tuned profile {spec.profile_name}')
        self._kick_serve_loop()
        return '\n'.join(outs)

    @handle_orch_error
    def rm_tuned_profile(self, profile_name: str) -> str:
        if profile_name not in self.tuned_profiles:
            raise OrchestratorError(
                f'Tuned profile {profile_name} does not exist. Nothing to remove.')
        self.tuned_profiles.rm_profile(profile_name)
        self._kick_serve_loop()
        return f'Removed tuned profile {profile_name}'

    @handle_orch_error
    def tuned_profile_ls(self) -> List[TunedProfileSpec]:
        return self.tuned_profiles.list_profiles()

    @handle_orch_error
    def tuned_profile_add_setting(self, profile_name: str, setting: str, value: str) -> str:
        if profile_name not in self.tuned_profiles:
            raise OrchestratorError(
                f'Tuned profile {profile_name} does not exist. Cannot add setting.')
        self.tuned_profiles.add_setting(profile_name, setting, value)
        self._kick_serve_loop()
        return f'Added setting {setting} with value {value} to tuned profile {profile_name}'

    @handle_orch_error
    def tuned_profile_rm_setting(self, profile_name: str, setting: str) -> str:
        if profile_name not in self.tuned_profiles:
            raise OrchestratorError(
                f'Tuned profile {profile_name} does not exist. Cannot remove setting.')
        self.tuned_profiles.rm_setting(profile_name, setting)
        self._kick_serve_loop()
        return f'Removed setting {setting} from tuned profile {profile_name}'

    @handle_orch_error
    def service_discovery_dump_cert(self) -> str:
        root_cert = self.get_store(ServiceDiscovery.KV_STORE_SD_ROOT_CERT)
        if not root_cert:
            raise OrchestratorError('No certificate found for service discovery')
        return root_cert

    def set_health_warning(self, name: str, summary: str, count: int, detail: List[str]) -> None:
        self.health_checks[name] = {
            'severity': 'warning',
            'summary': summary,
            'count': count,
            'detail': detail,
        }
        self.set_health_checks(self.health_checks)

    def remove_health_warning(self, name: str) -> None:
        if name in self.health_checks:
            del self.health_checks[name]
            self.set_health_checks(self.health_checks)

    def _plan(self, spec: ServiceSpec) -> dict:
        if spec.service_type == 'osd':
            return {'service_name': spec.service_name(),
                    'service_type': spec.service_type,
                    'data': self._preview_osdspecs(osdspecs=[cast(DriveGroupSpec, spec)])}

        svc = self.cephadm_services[spec.service_type]
        ha = HostAssignment(
            spec=spec,
            hosts=self.cache.get_schedulable_hosts(),
            unreachable_hosts=self.cache.get_unreachable_hosts(),
            draining_hosts=self.cache.get_draining_hosts(),
            networks=self.cache.networks,
            daemons=self.cache.get_daemons_by_service(spec.service_name()),
            allow_colo=svc.allow_colo(),
            rank_map=self.spec_store[spec.service_name()].rank_map if svc.ranked() else None
        )
        ha.validate()
        hosts, to_add, to_remove = ha.place()

        return {
            'service_name': spec.service_name(),
            'service_type': spec.service_type,
            'add': [hs.hostname for hs in to_add],
            'remove': [d.name() for d in to_remove]
        }

    @handle_orch_error
    def plan(self, specs: Sequence[GenericSpec]) -> List:
        results = [{'warning': 'WARNING! Dry-Runs are snapshots of a certain point in time and are bound \n'
                               'to the current inventory setup. If any of these conditions change, the \n'
                               'preview will be invalid. Please make sure to have a minimal \n'
                               'timeframe between planning and applying the specs.'}]
        if any([spec.service_type == 'host' for spec in specs]):
            return [{'error': 'Found <HostSpec>. Previews that include Host Specifications are not supported, yet.'}]
        for spec in specs:
            results.append(self._plan(cast(ServiceSpec, spec)))
        return results

    def _apply_service_spec(self, spec: ServiceSpec) -> str:
        if spec.placement.is_empty():
            # fill in default placement
            defaults = {
                'mon': PlacementSpec(count=5),
                'mgr': PlacementSpec(count=2),
                'mds': PlacementSpec(count=2),
                'rgw': PlacementSpec(count=2),
                'ingress': PlacementSpec(count=2),
                'iscsi': PlacementSpec(count=1),
                'nvmeof': PlacementSpec(count=1),
                'rbd-mirror': PlacementSpec(count=2),
                'cephfs-mirror': PlacementSpec(count=1),
                'nfs': PlacementSpec(count=1),
                'grafana': PlacementSpec(count=1),
                'alertmanager': PlacementSpec(count=1),
                'prometheus': PlacementSpec(count=1),
                'node-exporter': PlacementSpec(host_pattern='*'),
                'ceph-exporter': PlacementSpec(host_pattern='*'),
                'loki': PlacementSpec(count=1),
                'promtail': PlacementSpec(host_pattern='*'),
                'crash': PlacementSpec(host_pattern='*'),
                'container': PlacementSpec(count=1),
                'snmp-gateway': PlacementSpec(count=1),
                'elasticsearch': PlacementSpec(count=1),
                'jaeger-agent': PlacementSpec(host_pattern='*'),
                'jaeger-collector': PlacementSpec(count=1),
                'jaeger-query': PlacementSpec(count=1),
                SMBService.TYPE: PlacementSpec(count=1),
            }
            spec.placement = defaults[spec.service_type]
        elif spec.service_type in ['mon', 'mgr'] and \
                spec.placement.count is not None and \
                spec.placement.count < 1:
            raise OrchestratorError('cannot scale %s service below 1' % (
                spec.service_type))

        host_count = len(self.inventory.keys())
        max_count = self.max_count_per_host

        if spec.placement.count is not None:
            if spec.service_type in ['mon', 'mgr']:
                if spec.placement.count > max(5, host_count):
                    raise OrchestratorError(
                        (f'The maximum number of {spec.service_type} daemons allowed with {host_count} hosts is {max(5, host_count)}.'))
            elif spec.service_type != 'osd':
                if spec.placement.count > (max_count * host_count):
                    raise OrchestratorError((f'The maximum number of {spec.service_type} daemons allowed with {host_count} hosts is {host_count*max_count} ({host_count}x{max_count}).'
                                             + ' This limit can be adjusted by changing the mgr/cephadm/max_count_per_host config option'))

        if spec.placement.count_per_host is not None and spec.placement.count_per_host > max_count and spec.service_type != 'osd':
            raise OrchestratorError((f'The maximum count_per_host allowed is {max_count}.'
                                     + ' This limit can be adjusted by changing the mgr/cephadm/max_count_per_host config option'))

        HostAssignment(
            spec=spec,
            hosts=self.inventory.all_specs(),  # All hosts, even those without daemon refresh
            unreachable_hosts=self.cache.get_unreachable_hosts(),
            draining_hosts=self.cache.get_draining_hosts(),
            networks=self.cache.networks,
            daemons=self.cache.get_daemons_by_service(spec.service_name()),
            allow_colo=self.cephadm_services[spec.service_type].allow_colo(),
        ).validate()

        self.log.info('Saving service %s spec with placement %s' % (
            spec.service_name(), spec.placement.pretty_str()))
        self.spec_store.save(spec)
        self._kick_serve_loop()
        return "Scheduled %s update..." % spec.service_name()

    @handle_orch_error
    def apply(self, specs: Sequence[GenericSpec], no_overwrite: bool = False) -> List[str]:
        results = []
        for spec in specs:
            if no_overwrite:
                if spec.service_type == 'host' and cast(HostSpec, spec).hostname in self.inventory:
                    results.append('Skipped %s host spec. To change %s spec omit --no-overwrite flag'
                                   % (cast(HostSpec, spec).hostname, spec.service_type))
                    continue
                elif cast(ServiceSpec, spec).service_name() in self.spec_store:
                    results.append('Skipped %s service spec. To change %s spec omit --no-overwrite flag'
                                   % (cast(ServiceSpec, spec).service_name(), cast(ServiceSpec, spec).service_name()))
                    continue
            results.append(self._apply(spec))
        return results

    @handle_orch_error
    def apply_mgr(self, spec: ServiceSpec) -> str:
        return self._apply(spec)

    @handle_orch_error
    def apply_mds(self, spec: ServiceSpec) -> str:
        return self._apply(spec)

    @handle_orch_error
    def apply_rgw(self, spec: ServiceSpec) -> str:
        return self._apply(spec)

    @handle_orch_error
    def apply_ingress(self, spec: ServiceSpec) -> str:
        return self._apply(spec)

    @handle_orch_error
    def apply_iscsi(self, spec: ServiceSpec) -> str:
        return self._apply(spec)

    @handle_orch_error
    def apply_rbd_mirror(self, spec: ServiceSpec) -> str:
        return self._apply(spec)

    @handle_orch_error
    def apply_nfs(self, spec: ServiceSpec) -> str:
        return self._apply(spec)

    def _get_dashboard_url(self):
        # type: () -> str
        return self.get('mgr_map').get('services', {}).get('dashboard', '')

    @handle_orch_error
    def apply_prometheus(self, spec: ServiceSpec) -> str:
        return self._apply(spec)

    @handle_orch_error
    def apply_loki(self, spec: ServiceSpec) -> str:
        return self._apply(spec)

    @handle_orch_error
    def apply_promtail(self, spec: ServiceSpec) -> str:
        return self._apply(spec)

    @handle_orch_error
    def apply_node_exporter(self, spec: ServiceSpec) -> str:
        return self._apply(spec)

    @handle_orch_error
    def apply_ceph_exporter(self, spec: ServiceSpec) -> str:
        return self._apply(spec)

    @handle_orch_error
    def apply_crash(self, spec: ServiceSpec) -> str:
        return self._apply(spec)

    @handle_orch_error
    def apply_grafana(self, spec: ServiceSpec) -> str:
        return self._apply(spec)

    @handle_orch_error
    def apply_alertmanager(self, spec: ServiceSpec) -> str:
        return self._apply(spec)

    @handle_orch_error
    def apply_container(self, spec: ServiceSpec) -> str:
        return self._apply(spec)

    @handle_orch_error
    def apply_snmp_gateway(self, spec: ServiceSpec) -> str:
        return self._apply(spec)

    @handle_orch_error
    def apply_smb(self, spec: ServiceSpec) -> str:
        return self._apply(spec)

    @handle_orch_error
    def set_unmanaged(self, service_name: str, value: bool) -> str:
        return self.spec_store.set_unmanaged(service_name, value)

    @handle_orch_error
    def upgrade_check(self, image: str, version: str) -> str:
        if self.inventory.get_host_with_state("maintenance"):
            raise OrchestratorError("check aborted - you have hosts in maintenance state")

        if version:
            target_name = self.container_image_base + ':v' + version
        elif image:
            target_name = image
        else:
            raise OrchestratorError('must specify either image or version')

        with self.async_timeout_handler(cmd=f'cephadm inspect-image (image {target_name})'):
            image_info = self.wait_async(CephadmServe(self)._get_container_image_info(target_name))

        ceph_image_version = image_info.ceph_version
        if not ceph_image_version:
            return f'Unable to extract ceph version from {target_name}.'
        if ceph_image_version.startswith('ceph version '):
            ceph_image_version = ceph_image_version.split(' ')[2]
        version_error = self.upgrade._check_target_version(ceph_image_version)
        if version_error:
            return f'Incompatible upgrade: {version_error}'

        self.log.debug(f'image info {image} -> {image_info}')
        r: dict = {
            'target_name': target_name,
            'target_id': image_info.image_id,
            'target_version': image_info.ceph_version,
            'needs_update': dict(),
            'up_to_date': list(),
            'non_ceph_image_daemons': list()
        }
        for host, dm in self.cache.daemons.items():
            for name, dd in dm.items():
                # check if the container digest for the digest we're checking upgrades for matches
                # the container digests for the daemon if "use_repo_digest" setting is true
                # or that the image name matches the daemon's image name if "use_repo_digest"
                # is false. The idea is to generally check if the daemon is already using
                # the image we're checking upgrade to.
                if (
                    (self.use_repo_digest and dd.matches_digests(image_info.repo_digests))
                    or (not self.use_repo_digest and dd.matches_image_name(image))
                ):
                    r['up_to_date'].append(dd.name())
                elif dd.daemon_type in CEPH_IMAGE_TYPES:
                    r['needs_update'][dd.name()] = {
                        'current_name': dd.container_image_name,
                        'current_id': dd.container_image_id,
                        'current_version': dd.version,
                    }
                else:
                    r['non_ceph_image_daemons'].append(dd.name())
        if self.use_repo_digest and image_info.repo_digests:
            # FIXME: we assume the first digest is the best one to use
            r['target_digest'] = image_info.repo_digests[0]

        return json.dumps(r, indent=4, sort_keys=True)

    @handle_orch_error
    def upgrade_status(self) -> orchestrator.UpgradeStatusSpec:
        return self.upgrade.upgrade_status()

    @handle_orch_error
    def upgrade_ls(self, image: Optional[str], tags: bool, show_all_versions: Optional[bool]) -> Dict[Any, Any]:
        return self.upgrade.upgrade_ls(image, tags, show_all_versions)

    @handle_orch_error
    def upgrade_start(self, image: str, version: str, daemon_types: Optional[List[str]] = None, host_placement: Optional[str] = None,
                      services: Optional[List[str]] = None, limit: Optional[int] = None) -> str:
        if self.inventory.get_host_with_state("maintenance"):
            raise OrchestratorError("Upgrade aborted - you have host(s) in maintenance state")
        if self.offline_hosts:
            raise OrchestratorError(
                f"Upgrade aborted - Some host(s) are currently offline: {self.offline_hosts}")
        if daemon_types is not None and services is not None:
            raise OrchestratorError('--daemon-types and --services are mutually exclusive')
        if daemon_types is not None:
            for dtype in daemon_types:
                if dtype not in CEPH_UPGRADE_ORDER:
                    raise OrchestratorError(f'Upgrade aborted - Got unexpected daemon type "{dtype}".\n'
                                            f'Viable daemon types for this command are: {utils.CEPH_TYPES + utils.GATEWAY_TYPES}')
        if services is not None:
            for service in services:
                if service not in self.spec_store:
                    raise OrchestratorError(f'Upgrade aborted - Got unknown service name "{service}".\n'
                                            f'Known services are: {self.spec_store.all_specs.keys()}')
        hosts: Optional[List[str]] = None
        if host_placement is not None:
            all_hosts = list(self.inventory.all_specs())
            placement = PlacementSpec.from_string(host_placement)
            hosts = placement.filter_matching_hostspecs(all_hosts)
            if not hosts:
                raise OrchestratorError(
                    f'Upgrade aborted - hosts parameter "{host_placement}" provided did not match any hosts')

        if limit is not None:
            if limit < 1:
                raise OrchestratorError(
                    f'Upgrade aborted - --limit arg must be a positive integer, not {limit}')

        return self.upgrade.upgrade_start(image, version, daemon_types, hosts, services, limit)

    @handle_orch_error
    def upgrade_pause(self) -> str:
        return self.upgrade.upgrade_pause()

    @handle_orch_error
    def upgrade_resume(self) -> str:
        return self.upgrade.upgrade_resume()

    @handle_orch_error
    def upgrade_stop(self) -> str:
        return self.upgrade.upgrade_stop()

    @handle_orch_error
    def remove_osds(self, osd_ids: List[str],
                    replace: bool = False,
                    force: bool = False,
                    zap: bool = False,
                    no_destroy: bool = False) -> str:
        """
        Takes a list of OSDs and schedules them for removal.
        The function that takes care of the actual removal is
        process_removal_queue().
        """

        daemons: List[orchestrator.DaemonDescription] = self.cache.get_daemons_by_type('osd')
        to_remove_daemons = list()
        for daemon in daemons:
            if daemon.daemon_id in osd_ids:
                to_remove_daemons.append(daemon)
        if not to_remove_daemons:
            return f"Unable to find OSDs: {osd_ids}"

        for daemon in to_remove_daemons:
            assert daemon.daemon_id is not None
            try:
                self.to_remove_osds.enqueue(OSD(osd_id=int(daemon.daemon_id),
                                                replace=replace,
                                                force=force,
                                                zap=zap,
                                                no_destroy=no_destroy,
                                                hostname=daemon.hostname,
                                                process_started_at=datetime_now(),
                                                remove_util=self.to_remove_osds.rm_util))
            except NotFoundError:
                return f"Unable to find OSDs: {osd_ids}"

        # trigger the serve loop to initiate the removal
        self._kick_serve_loop()
        warning_zap = "" if zap else ("\nVG/LV for the OSDs won't be zapped (--zap wasn't passed).\n"
                                      "Run the `ceph-volume lvm zap` command with `--destroy`"
                                      " against the VG/LV if you want them to be destroyed.")
        return f"Scheduled OSD(s) for removal.{warning_zap}"

    @handle_orch_error
    def stop_remove_osds(self, osd_ids: List[str]) -> str:
        """
        Stops a `removal` process for a List of OSDs.
        This will revert their weight and remove it from the osds_to_remove queue
        """
        for osd_id in osd_ids:
            try:
                self.to_remove_osds.rm_by_osd_id(int(osd_id))
            except (NotFoundError, KeyError, ValueError):
                return f'Unable to find OSD in the queue: {osd_id}'

        # trigger the serve loop to halt the removal
        self._kick_serve_loop()
        return "Stopped OSD(s) removal"

    @handle_orch_error
    def remove_osds_status(self) -> List[OSD]:
        """
        The CLI call to retrieve an osd removal report
        """
        return self.to_remove_osds.all_osds()

    @handle_orch_error
    def drain_host(self, hostname: str, force: bool = False, keep_conf_keyring: bool = False, zap_osd_devices: bool = False) -> str:
        """
        Drain all daemons from a host.
        :param host: host name
        """

        # if we drain the last admin host we could end up removing the only instance
        # of the config and keyring and cause issues
        if not force:
            p = PlacementSpec(label=SpecialHostLabels.ADMIN)
            admin_hosts = p.filter_matching_hostspecs(self.inventory.all_specs())
            if len(admin_hosts) == 1 and admin_hosts[0] == hostname:
                raise OrchestratorValidationError(f"Host {hostname} is the last host with the '{SpecialHostLabels.ADMIN}'"
                                                  " label.\nDraining this host could cause the removal"
                                                  " of the last cluster config/keyring managed by cephadm.\n"
                                                  f"It is recommended to add the {SpecialHostLabels.ADMIN} label to another host"
                                                  " before completing this operation.\nIf you're certain this is"
                                                  " what you want rerun this command with --force.")
            # if the user has specified the host we are going to drain
            # explicitly in any service spec, warn the user. Having a
            # drained host listed in a placement provides no value, so
            # they may want to fix it.
            services_matching_drain_host: List[str] = []
            for sname, sspec in self.spec_store.all_specs.items():
                if sspec.placement.hosts and hostname in [h.hostname for h in sspec.placement.hosts]:
                    services_matching_drain_host.append(sname)
            if services_matching_drain_host:
                raise OrchestratorValidationError(f'Host {hostname} was found explicitly listed in the placements '
                                                  f'of services:\n  {services_matching_drain_host}.\nPlease update those '
                                                  'specs to not list this host.\nThis warning can be bypassed with --force')

        self.add_host_label(hostname, '_no_schedule')
        if not keep_conf_keyring:
            self.add_host_label(hostname, SpecialHostLabels.DRAIN_CONF_KEYRING)

        daemons: List[orchestrator.DaemonDescription] = self.cache.get_daemons_by_host(hostname)

        osds_to_remove = [d.daemon_id for d in daemons if d.daemon_type == 'osd']
        self.remove_osds(osds_to_remove, zap=zap_osd_devices)

        daemons_table = ""
        daemons_table += "{:<20} {:<15}\n".format("type", "id")
        daemons_table += "{:<20} {:<15}\n".format("-" * 20, "-" * 15)
        for d in daemons:
            daemons_table += "{:<20} {:<15}\n".format(d.daemon_type, d.daemon_id)

        return "Scheduled to remove the following daemons from host '{}'\n{}".format(hostname, daemons_table)

    def trigger_connect_dashboard_rgw(self) -> None:
        self.need_connect_dashboard_rgw = True
        self.event.set()
