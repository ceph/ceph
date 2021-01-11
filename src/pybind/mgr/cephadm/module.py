import json
import errno
import logging
import re
import shlex
from collections import defaultdict
from configparser import ConfigParser
from contextlib import contextmanager
from functools import wraps
from tempfile import TemporaryDirectory
from threading import Event

import string
from typing import List, Dict, Optional, Callable, Tuple, TypeVar, \
    Any, Set, TYPE_CHECKING, cast, Iterator, Union, NamedTuple

import datetime
import os
import random
import tempfile
import multiprocessing.pool
import subprocess

from ceph.deployment import inventory
from ceph.deployment.drive_group import DriveGroupSpec
from ceph.deployment.service_spec import \
    NFSServiceSpec, ServiceSpec, PlacementSpec, assert_valid_host, \
    CustomContainerSpec, HostPlacementSpec, HA_RGWSpec
from ceph.utils import str_to_datetime, datetime_to_str, datetime_now
from cephadm.serve import CephadmServe
from cephadm.services.cephadmservice import CephadmDaemonSpec

from mgr_module import MgrModule, HandleCommandResult, Option
from mgr_util import create_self_signed_cert, verify_tls, ServerConfigException
import secrets
import orchestrator
from orchestrator import OrchestratorError, OrchestratorValidationError, HostSpec, \
    CLICommandMeta, OrchestratorEvent, set_exception_subject, DaemonDescription
from orchestrator._interface import GenericSpec
from orchestrator._interface import daemon_type_to_service, service_to_daemon_types

from . import remotes
from . import utils
from .migrations import Migrations
from .services.cephadmservice import MonService, MgrService, MdsService, RgwService, \
    RbdMirrorService, CrashService, CephadmService, CephadmExporter, CephadmExporterConfig
from .services.container import CustomContainerService
from .services.iscsi import IscsiService
from .services.ha_rgw import HA_RGWService
from .services.nfs import NFSService
from .services.osd import RemoveUtil, OSDQueue, OSDService, OSD, NotFoundError
from .services.monitoring import GrafanaService, AlertmanagerService, PrometheusService, \
    NodeExporterService
from .schedule import HostAssignment
from .inventory import Inventory, SpecStore, HostCache, EventStore
from .upgrade import CEPH_UPGRADE_ORDER, CephadmUpgrade
from .template import TemplateMgr
from .utils import forall_hosts, CephadmNoImage, cephadmNoImage

try:
    import remoto
    # NOTE(mattoliverau) Patch remoto until remoto PR
    # (https://github.com/alfredodeza/remoto/pull/56) lands
    from distutils.version import StrictVersion
    if StrictVersion(remoto.__version__) <= StrictVersion('1.2'):
        def remoto_has_connection(self: Any) -> bool:
            return self.gateway.hasreceiver()

        from remoto.backends import BaseConnection
        BaseConnection.has_connection = remoto_has_connection
    import remoto.process
    import execnet.gateway_bootstrap
except ImportError as e:
    remoto = None
    remoto_import_error = str(e)

try:
    from typing import List
except ImportError:
    pass

logger = logging.getLogger(__name__)

T = TypeVar('T')

DEFAULT_SSH_CONFIG = """
Host *
  User root
  StrictHostKeyChecking no
  UserKnownHostsFile /dev/null
  ConnectTimeout=30
"""

CEPH_TYPES = set(CEPH_UPGRADE_ORDER)


class CephadmCompletion(orchestrator.Completion[T]):
    def evaluate(self) -> None:
        self.finalize(None)


def trivial_completion(f: Callable[..., T]) -> Callable[..., CephadmCompletion[T]]:
    """
    Decorator to make CephadmCompletion methods return
    a completion object that executes themselves.
    """

    @wraps(f)
    def wrapper(*args: Any, **kwargs: Any) -> CephadmCompletion:
        return CephadmCompletion(on_complete=lambda _: f(*args, **kwargs))

    return wrapper


def service_inactive(spec_name: str) -> Callable:
    def inner(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            obj = args[0]
            if obj.get_store(f"spec.{spec_name}") is not None:
                return 1, "", f"Unable to change configuration of an active service {spec_name}"
            return func(*args, **kwargs)
        return wrapper
    return inner


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


class ContainerInspectInfo(NamedTuple):
    image_id: str
    ceph_version: Optional[str]
    repo_digest: Optional[str]


class CephadmOrchestrator(orchestrator.Orchestrator, MgrModule,
                          metaclass=CLICommandMeta):

    _STORE_HOST_PREFIX = "host"

    instance = None
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
            default='docker.io/ceph/ceph',
            desc='Container image name, without the tag',
            runtime=True,
        ),
        Option(
            'container_image_prometheus',
            default='docker.io/prom/prometheus:v2.18.1',
            desc='Prometheus container image',
        ),
        Option(
            'container_image_grafana',
            default='docker.io/ceph/ceph-grafana:6.7.4',
            desc='Prometheus container image',
        ),
        Option(
            'container_image_alertmanager',
            default='docker.io/prom/alertmanager:v0.20.0',
            desc='Prometheus container image',
        ),
        Option(
            'container_image_node_exporter',
            default='docker.io/prom/node-exporter:v0.18.1',
            desc='Prometheus container image',
        ),
        Option(
            'container_image_haproxy',
            default='haproxy',
            desc='HAproxy container image',
        ),
        Option(
            'container_image_keepalived',
            default='arcts/keepalived',
            desc='Keepalived container image',
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
            default=False,
            desc='Run podman/docker with `--init`'
        ),
        Option(
            'prometheus_alerts_path',
            type='str',
            default='/etc/prometheus/ceph/ceph_default_alerts.yml',
            desc='location of alerts to include in prometheus deployments',
        ),
        Option(
            'migration_current',
            type='int',
            default=None,
            desc='internal - do not modify',
            # used to track track spec and other data migrations.
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
            'registry_url',
            type='str',
            default=None,
            desc='Custom repository url'
        ),
        Option(
            'registry_username',
            type='str',
            default=None,
            desc='Custom repository username'
        ),
        Option(
            'registry_password',
            type='str',
            default=None,
            desc='Custom repository password'
        ),
        Option(
            'use_repo_digest',
            type='bool',
            default=False,
            desc='Automatically convert image tags to image digest. Make sure all daemons use the same image',
        ),
    ]

    def __init__(self, *args: Any, **kwargs: Any):
        super(CephadmOrchestrator, self).__init__(*args, **kwargs)
        self._cluster_fsid = self.get('mon_map')['fsid']
        self.last_monmap: Optional[datetime.datetime] = None

        # for serve()
        self.run = True
        self.event = Event()

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
            self.mode = ''
            self.container_image_base = ''
            self.container_image_prometheus = ''
            self.container_image_grafana = ''
            self.container_image_alertmanager = ''
            self.container_image_node_exporter = ''
            self.container_image_haproxy = ''
            self.container_image_keepalived = ''
            self.warn_on_stray_hosts = True
            self.warn_on_stray_daemons = True
            self.warn_on_failed_host_check = True
            self.allow_ptrace = False
            self.container_init = False
            self.prometheus_alerts_path = ''
            self.migration_current: Optional[int] = None
            self.config_dashboard = True
            self.manage_etc_ceph_ceph_conf = True
            self.registry_url: Optional[str] = None
            self.registry_username: Optional[str] = None
            self.registry_password: Optional[str] = None
            self.use_repo_digest = False

        self._cons: Dict[str, Tuple[remoto.backends.BaseConnection,
                                    remoto.backends.LegacyModuleExecute]] = {}

        self.notify('mon_map', None)
        self.config_notify()

        path = self.get_ceph_option('cephadm_path')
        try:
            with open(path, 'r') as f:
                self._cephadm = f.read()
        except (IOError, TypeError) as e:
            raise RuntimeError("unable to read cephadm at '%s': %s" % (
                path, str(e)))

        self._worker_pool = multiprocessing.pool.ThreadPool(10)

        self._reconfig_ssh()

        CephadmOrchestrator.instance = self

        self.upgrade = CephadmUpgrade(self)

        self.health_checks: Dict[str, dict] = {}

        self.all_progress_references = list()  # type: List[orchestrator.ProgressReference]

        self.inventory = Inventory(self)

        self.cache = HostCache(self)
        self.cache.load()

        self.rm_util = RemoveUtil(self)
        self.to_remove_osds = OSDQueue()
        self.rm_util.load_from_store()

        self.spec_store = SpecStore(self)
        self.spec_store.load()

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

        # services:
        self.osd_service = OSDService(self)
        self.nfs_service = NFSService(self)
        self.mon_service = MonService(self)
        self.mgr_service = MgrService(self)
        self.mds_service = MdsService(self)
        self.rgw_service = RgwService(self)
        self.rbd_mirror_service = RbdMirrorService(self)
        self.grafana_service = GrafanaService(self)
        self.alertmanager_service = AlertmanagerService(self)
        self.prometheus_service = PrometheusService(self)
        self.node_exporter_service = NodeExporterService(self)
        self.crash_service = CrashService(self)
        self.iscsi_service = IscsiService(self)
        self.ha_rgw_service = HA_RGWService(self)
        self.container_service = CustomContainerService(self)
        self.cephadm_exporter_service = CephadmExporter(self)
        self.cephadm_services = {
            'mon': self.mon_service,
            'mgr': self.mgr_service,
            'osd': self.osd_service,
            'mds': self.mds_service,
            'rgw': self.rgw_service,
            'rbd-mirror': self.rbd_mirror_service,
            'nfs': self.nfs_service,
            'grafana': self.grafana_service,
            'alertmanager': self.alertmanager_service,
            'prometheus': self.prometheus_service,
            'node-exporter': self.node_exporter_service,
            'crash': self.crash_service,
            'iscsi': self.iscsi_service,
            'ha-rgw': self.ha_rgw_service,
            'container': self.container_service,
            'cephadm-exporter': self.cephadm_exporter_service,
        }

        self.template = TemplateMgr(self)

        self.requires_post_actions: Set[str] = set()

    def shutdown(self) -> None:
        self.log.debug('shutdown')
        self._worker_pool.close()
        self._worker_pool.join()
        self.run = False
        self.event.set()

    def _get_cephadm_service(self, service_type: str) -> CephadmService:
        assert service_type in ServiceSpec.KNOWN_SERVICE_TYPES
        return self.cephadm_services[service_type]

    def _kick_serve_loop(self) -> None:
        self.log.debug('_kick_serve_loop')
        self.event.set()

    # function responsible for logging single host into custom registry
    def _registry_login(self, host: str, url: Optional[str], username: Optional[str], password: Optional[str]) -> Optional[str]:
        self.log.debug(f"Attempting to log host {host} into custom registry @ {url}")
        # want to pass info over stdin rather than through normal list of args
        args_str = json.dumps({
            'url': url,
            'username': username,
            'password': password,
        })
        out, err, code = self._run_cephadm(
            host, 'mon', 'registry-login',
            ['--registry-json', '-'], stdin=args_str, error_ok=True)
        if code:
            return f"Host {host} failed to login to {url} as {username} with given password"
        return None

    def serve(self) -> None:
        """
        The main loop of cephadm.

        A command handler will typically change the declarative state
        of cephadm. This loop will then attempt to apply this new state.
        """
        serve = CephadmServe(self)
        serve.serve()

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

    def notify(self, notify_type: str, notify_id: Optional[str]) -> None:
        if notify_type == "mon_map":
            # get monmap mtime so we can refresh configs when mons change
            monmap = self.get('mon_map')
            self.last_monmap = str_to_datetime(monmap['modified'])
            if self.last_monmap and self.last_monmap > datetime_now():
                self.last_monmap = None  # just in case clocks are skewed
            if getattr(self, 'manage_etc_ceph_ceph_conf', False):
                # getattr, due to notify() being called before config_notify()
                self._kick_serve_loop()
        if notify_type == "pg_summary":
            self._trigger_osd_removal()

    def _trigger_osd_removal(self) -> None:
        data = self.get("osd_stats")
        for osd in data.get('osd_stats', []):
            if osd.get('num_pgs') == 0:
                # if _ANY_ osd that is currently in the queue appears to be empty,
                # start the removal process
                if int(osd.get('osd')) in self.to_remove_osds.as_osd_ids():
                    self.log.debug(f"Found empty osd. Starting removal process")
                    # if the osd that is now empty is also part of the removal queue
                    # start the process
                    self.rm_util.process_removal_queue()

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

    def get_unique_name(self, daemon_type, host, existing, prefix=None,
                        forcename=None):
        # type: (str, str, List[orchestrator.DaemonDescription], Optional[str], Optional[str]) -> str
        """
        Generate a unique random service name
        """
        suffix = daemon_type not in [
            'mon', 'crash', 'nfs',
            'prometheus', 'node-exporter', 'grafana', 'alertmanager',
            'container', 'cephadm-exporter',
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

    def _reconfig_ssh(self) -> None:
        temp_files = []  # type: list
        ssh_options = []  # type: List[str]

        # ssh_config
        ssh_config_fname = self.ssh_config_file
        ssh_config = self.get_store("ssh_config")
        if ssh_config is not None or ssh_config_fname is None:
            if not ssh_config:
                ssh_config = DEFAULT_SSH_CONFIG
            f = tempfile.NamedTemporaryFile(prefix='cephadm-conf-')
            os.fchmod(f.fileno(), 0o600)
            f.write(ssh_config.encode('utf-8'))
            f.flush()  # make visible to other processes
            temp_files += [f]
            ssh_config_fname = f.name
        if ssh_config_fname:
            self.validate_ssh_config_fname(ssh_config_fname)
            ssh_options += ['-F', ssh_config_fname]
        self.ssh_config = ssh_config

        # identity
        ssh_key = self.get_store("ssh_identity_key")
        ssh_pub = self.get_store("ssh_identity_pub")
        self.ssh_pub = ssh_pub
        self.ssh_key = ssh_key
        if ssh_key and ssh_pub:
            tkey = tempfile.NamedTemporaryFile(prefix='cephadm-identity-')
            tkey.write(ssh_key.encode('utf-8'))
            os.fchmod(tkey.fileno(), 0o600)
            tkey.flush()  # make visible to other processes
            tpub = open(tkey.name + '.pub', 'w')
            os.fchmod(tpub.fileno(), 0o600)
            tpub.write(ssh_pub)
            tpub.flush()  # make visible to other processes
            temp_files += [tkey, tpub]
            ssh_options += ['-i', tkey.name]

        self._temp_files = temp_files
        if ssh_options:
            self._ssh_options = ' '.join(ssh_options)  # type: Optional[str]
        else:
            self._ssh_options = None

        if self.mode == 'root':
            self.ssh_user = self.get_store('ssh_user', default='root')
        elif self.mode == 'cephadm-package':
            self.ssh_user = 'cephadm'

        self._reset_cons()

    def validate_ssh_config_content(self, ssh_config: Optional[str]) -> None:
        if ssh_config is None or len(ssh_config.strip()) == 0:
            raise OrchestratorValidationError('ssh_config cannot be empty')
        # StrictHostKeyChecking is [yes|no] ?
        l = re.findall(r'StrictHostKeyChecking\s+.*', ssh_config)
        if not l:
            raise OrchestratorValidationError('ssh_config requires StrictHostKeyChecking')
        for s in l:
            if 'ask' in s.lower():
                raise OrchestratorValidationError(f'ssh_config cannot contain: \'{s}\'')

    def validate_ssh_config_fname(self, ssh_config_fname: str) -> None:
        if not os.path.isfile(ssh_config_fname):
            raise OrchestratorValidationError("ssh_config \"{}\" does not exist".format(
                ssh_config_fname))

    def _reset_con(self, host: str) -> None:
        conn, r = self._cons.get(host, (None, None))
        if conn:
            self.log.debug('_reset_con close %s' % host)
            conn.exit()
            del self._cons[host]

    def _reset_cons(self) -> None:
        for host, conn_and_r in self._cons.items():
            self.log.debug('_reset_cons close %s' % host)
            conn, r = conn_and_r
            conn.exit()
        self._cons = {}

    def offline_hosts_remove(self, host: str) -> None:
        if host in self.offline_hosts:
            self.offline_hosts.remove(host)

    @staticmethod
    def can_run() -> Tuple[bool, str]:
        if remoto is not None:
            return True, ""
        else:
            return False, "loading remoto library:{}".format(
                remoto_import_error)

    def available(self) -> Tuple[bool, str]:
        """
        The cephadm orchestrator is always available.
        """
        ok, err = self.can_run()
        if not ok:
            return ok, err
        if not self.ssh_key or not self.ssh_pub:
            return False, 'SSH keys not set. Use `ceph cephadm set-priv-key` and `ceph cephadm set-pub-key` or `ceph cephadm generate-key`'
        return True, ''

    def process(self, completions: List[CephadmCompletion]) -> None:
        """
        Does nothing, as completions are processed in another thread.
        """
        if completions:
            self.log.debug("process: completions={0}".format(
                orchestrator.pretty_print(completions)))

            for p in completions:
                p.evaluate()

    @orchestrator._cli_write_command(
        prefix='cephadm set-ssh-config',
        desc='Set the ssh_config file (use -i <ssh_config>)')
    def _set_ssh_config(self, inbuf: Optional[str] = None) -> Tuple[int, str, str]:
        """
        Set an ssh_config file provided from stdin
        """
        if inbuf == self.ssh_config:
            return 0, "value unchanged", ""
        self.validate_ssh_config_content(inbuf)
        self.set_store("ssh_config", inbuf)
        self.log.info('Set ssh_config')
        self._reconfig_ssh()
        return 0, "", ""

    @orchestrator._cli_write_command(
        prefix='cephadm clear-ssh-config',
        desc='Clear the ssh_config file')
    def _clear_ssh_config(self) -> Tuple[int, str, str]:
        """
        Clear the ssh_config file provided from stdin
        """
        self.set_store("ssh_config", None)
        self.ssh_config_tmp = None
        self.log.info('Cleared ssh_config')
        self._reconfig_ssh()
        return 0, "", ""

    @orchestrator._cli_read_command(
        prefix='cephadm get-ssh-config',
        desc='Returns the ssh config as used by cephadm'
    )
    def _get_ssh_config(self) -> HandleCommandResult:
        if self.ssh_config_file:
            self.validate_ssh_config_fname(self.ssh_config_file)
            with open(self.ssh_config_file) as f:
                return HandleCommandResult(stdout=f.read())
        ssh_config = self.get_store("ssh_config")
        if ssh_config:
            return HandleCommandResult(stdout=ssh_config)
        return HandleCommandResult(stdout=DEFAULT_SSH_CONFIG)

    @orchestrator._cli_write_command(
        'cephadm generate-key',
        desc='Generate a cluster SSH key (if not present)')
    def _generate_key(self) -> Tuple[int, str, str]:
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
            self._reconfig_ssh()
        return 0, '', ''

    @orchestrator._cli_write_command(
        'cephadm set-priv-key',
        desc='Set cluster SSH private key (use -i <private_key>)')
    def _set_priv_key(self, inbuf: Optional[str] = None) -> Tuple[int, str, str]:
        if inbuf is None or len(inbuf) == 0:
            return -errno.EINVAL, "", "empty private ssh key provided"
        if inbuf == self.ssh_key:
            return 0, "value unchanged", ""
        self.set_store("ssh_identity_key", inbuf)
        self.log.info('Set ssh private key')
        self._reconfig_ssh()
        return 0, "", ""

    @orchestrator._cli_write_command(
        'cephadm set-pub-key',
        desc='Set cluster SSH public key (use -i <public_key>)')
    def _set_pub_key(self, inbuf: Optional[str] = None) -> Tuple[int, str, str]:
        if inbuf is None or len(inbuf) == 0:
            return -errno.EINVAL, "", "empty public ssh key provided"
        if inbuf == self.ssh_pub:
            return 0, "value unchanged", ""
        self.set_store("ssh_identity_pub", inbuf)
        self.log.info('Set ssh public key')
        self._reconfig_ssh()
        return 0, "", ""

    @orchestrator._cli_write_command(
        'cephadm clear-key',
        desc='Clear cluster SSH key')
    def _clear_key(self) -> Tuple[int, str, str]:
        self.set_store('ssh_identity_key', None)
        self.set_store('ssh_identity_pub', None)
        self._reconfig_ssh()
        self.log.info('Cleared cluster SSH key')
        return 0, '', ''

    @orchestrator._cli_read_command(
        'cephadm get-pub-key',
        desc='Show SSH public key for connecting to cluster hosts')
    def _get_pub_key(self) -> Tuple[int, str, str]:
        if self.ssh_pub:
            return 0, self.ssh_pub, ''
        else:
            return -errno.ENOENT, '', 'No cluster SSH key defined'

    @orchestrator._cli_read_command(
        'cephadm get-user',
        desc='Show user for SSHing to cluster hosts')
    def _get_user(self) -> Tuple[int, str, str]:
        return 0, self.ssh_user, ''

    @orchestrator._cli_read_command(
        'cephadm set-user',
        'name=user,type=CephString',
        'Set user for SSHing to cluster hosts, passwordless sudo will be needed for non-root users')
    def set_ssh_user(self, user: str) -> Tuple[int, str, str]:
        current_user = self.ssh_user
        if user == current_user:
            return 0, "value unchanged", ""

        self.set_store('ssh_user', user)
        self._reconfig_ssh()

        host = self.cache.get_hosts()[0]
        r = CephadmServe(self)._check_host(host)
        if r is not None:
            # connection failed reset user
            self.set_store('ssh_user', current_user)
            self._reconfig_ssh()
            return -errno.EINVAL, '', 'ssh connection %s@%s failed' % (user, host)

        msg = 'ssh user set to %s' % user
        if user != 'root':
            msg += ' sudo will be used'
        self.log.info(msg)
        return 0, msg, ''

    @orchestrator._cli_read_command(
        'cephadm registry-login',
        "name=url,type=CephString,req=false "
        "name=username,type=CephString,req=false "
        "name=password,type=CephString,req=false",
        'Set custom registry login info by providing url, username and password or json file with login info (-i <file>)')
    def registry_login(self, url: Optional[str] = None, username: Optional[str] = None, password: Optional[str] = None, inbuf: Optional[str] = None) -> Tuple[int, str, str]:
        # if password not given in command line, get it through file input
        if not (url and username and password) and (inbuf is None or len(inbuf) == 0):
            return -errno.EINVAL, "", ("Invalid arguments. Please provide arguments <url> <username> <password> "
                                       "or -i <login credentials json file>")
        elif not (url and username and password):
            assert isinstance(inbuf, str)
            login_info = json.loads(inbuf)
            if "url" in login_info and "username" in login_info and "password" in login_info:
                url = login_info["url"]
                username = login_info["username"]
                password = login_info["password"]
            else:
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
        r = self._registry_login(host, url, username, password)
        if r is not None:
            return 1, '', r
        # if logins succeeded, store info
        self.log.debug("Host logins successful. Storing login info.")
        self.set_module_option('registry_url', url)
        self.set_module_option('registry_username', username)
        self.set_module_option('registry_password', password)
        # distribute new login info to all hosts
        self.cache.distribute_new_registry_login_info()
        return 0, "registry login scheduled", ''

    @orchestrator._cli_read_command(
        'cephadm check-host',
        'name=host,type=CephString '
        'name=addr,type=CephString,req=false',
        'Check whether we can access and manage a remote host')
    def check_host(self, host: str, addr: Optional[str] = None) -> Tuple[int, str, str]:
        try:
            out, err, code = self._run_cephadm(host, cephadmNoImage, 'check-host',
                                               ['--expect-hostname', host],
                                               addr=addr,
                                               error_ok=True, no_fsid=True)
            if code:
                return 1, '', ('check-host failed:\n' + '\n'.join(err))
        except OrchestratorError as e:
            self.log.exception(f"check-host failed for '{host}'")
            return 1, '', ('check-host failed:\n' +
                           f"Host '{host}' not found. Use 'ceph orch host ls' to see all managed hosts.")
        # if we have an outstanding health alert for this host, give the
        # serve thread a kick
        if 'CEPHADM_HOST_CHECK_FAILED' in self.health_checks:
            for item in self.health_checks['CEPHADM_HOST_CHECK_FAILED']['detail']:
                if item.startswith('host %s ' % host):
                    self.event.set()
        return 0, '%s (%s) ok' % (host, addr), '\n'.join(err)

    @orchestrator._cli_read_command(
        'cephadm prepare-host',
        'name=host,type=CephString '
        'name=addr,type=CephString,req=false',
        'Prepare a remote host for use with cephadm')
    def _prepare_host(self, host: str, addr: Optional[str] = None) -> Tuple[int, str, str]:
        out, err, code = self._run_cephadm(host, cephadmNoImage, 'prepare-host',
                                           ['--expect-hostname', host],
                                           addr=addr,
                                           error_ok=True, no_fsid=True)
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
        prefix='cephadm set-extra-ceph-conf',
        desc="Text that is appended to all daemon's ceph.conf.\n"
             "Mainly a workaround, till `config generate-minimal-conf` generates\n"
             "a complete ceph.conf.\n\n"
             "Warning: this is a dangerous operation.")
    def _set_extra_ceph_conf(self, inbuf: Optional[str] = None) -> HandleCommandResult:
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
        'cephadm get-extra-ceph-conf',
        desc='Get extra ceph conf that is appended')
    def _get_extra_ceph_conf(self) -> HandleCommandResult:
        return HandleCommandResult(stdout=self.extra_ceph_conf().conf)

    def _set_exporter_config(self, config: Dict[str, str]) -> None:
        self.set_store('exporter_config', json.dumps(config))

    def _get_exporter_config(self) -> Dict[str, str]:
        cfg_str = self.get_store('exporter_config')
        return json.loads(cfg_str) if cfg_str else {}

    def _set_exporter_option(self, option: str, value: Optional[str] = None) -> None:
        kv_option = f'exporter_{option}'
        self.set_store(kv_option, value)

    def _get_exporter_option(self, option: str) -> Optional[str]:
        kv_option = f'exporter_{option}'
        return self.get_store(kv_option)

    @orchestrator._cli_write_command(
        prefix='cephadm generate-exporter-config',
        desc='Generate default SSL crt/key and token for cephadm exporter daemons')
    @service_inactive('cephadm-exporter')
    def _generate_exporter_config(self) -> Tuple[int, str, str]:
        self._set_exporter_defaults()
        self.log.info('Default settings created for cephadm exporter(s)')
        return 0, "", ""

    def _set_exporter_defaults(self) -> None:
        crt, key = self._generate_exporter_ssl()
        token = self._generate_exporter_token()
        self._set_exporter_config({
            "crt": crt,
            "key": key,
            "token": token,
            "port": CephadmExporterConfig.DEFAULT_PORT
        })
        self._set_exporter_option('enabled', 'true')

    def _generate_exporter_ssl(self) -> Tuple[str, str]:
        return create_self_signed_cert(dname={"O": "Ceph", "OU": "cephadm-exporter"})

    def _generate_exporter_token(self) -> str:
        return secrets.token_hex(32)

    @orchestrator._cli_write_command(
        prefix='cephadm clear-exporter-config',
        desc='Clear the SSL configuration used by cephadm exporter daemons')
    @service_inactive('cephadm-exporter')
    def _clear_exporter_config(self) -> Tuple[int, str, str]:
        self._clear_exporter_config_settings()
        self.log.info('Cleared cephadm exporter configuration')
        return 0, "", ""

    def _clear_exporter_config_settings(self) -> None:
        self.set_store('exporter_config', None)
        self._set_exporter_option('enabled', None)

    @orchestrator._cli_write_command(
        prefix='cephadm set-exporter-config',
        desc='Set custom cephadm-exporter configuration from a json file (-i <file>). JSON must contain crt, key, token and port')
    @service_inactive('cephadm-exporter')
    def _store_exporter_config(self, inbuf: Optional[str] = None) -> Tuple[int, str, str]:

        if not inbuf:
            return 1, "", "JSON configuration has not been provided (-i <filename>)"

        cfg = CephadmExporterConfig(self)
        rc, reason = cfg.load_from_json(inbuf)
        if rc:
            return 1, "", reason

        rc, reason = cfg.validate_config()
        if rc:
            return 1, "", reason

        self._set_exporter_config({
            "crt": cfg.crt,
            "key": cfg.key,
            "token": cfg.token,
            "port": cfg.port
        })
        self.log.info("Loaded and verified the TLS configuration")
        return 0, "", ""

    @orchestrator._cli_read_command(
        'cephadm get-exporter-config',
        desc='Show the current cephadm-exporter configuraion (JSON)')
    def _show_exporter_config(self) -> Tuple[int, str, str]:
        cfg = self._get_exporter_config()
        return 0, json.dumps(cfg, indent=2), ""

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

    def _get_connection(self, host: str) -> Tuple['remoto.backends.BaseConnection',
                                                  'remoto.backends.LegacyModuleExecute']:
        """
        Setup a connection for running commands on remote host.
        """
        conn, r = self._cons.get(host, (None, None))
        if conn:
            if conn.has_connection():
                self.log.debug('Have connection to %s' % host)
                return conn, r
            else:
                self._reset_con(host)
        n = self.ssh_user + '@' + host
        self.log.debug("Opening connection to {} with ssh options '{}'".format(
            n, self._ssh_options))
        child_logger = self.log.getChild(n)
        child_logger.setLevel('WARNING')
        conn = remoto.Connection(
            n,
            logger=child_logger,
            ssh_options=self._ssh_options,
            sudo=True if self.ssh_user != 'root' else False)

        r = conn.import_module(remotes)
        self._cons[host] = conn, r

        return conn, r

    def _executable_path(self, conn: 'remoto.backends.BaseConnection', executable: str) -> str:
        """
        Remote validator that accepts a connection object to ensure that a certain
        executable is available returning its full path if so.

        Otherwise an exception with thorough details will be raised, informing the
        user that the executable was not found.
        """
        executable_path = conn.remote_module.which(executable)
        if not executable_path:
            raise RuntimeError("Executable '{}' not found on host '{}'".format(
                executable, conn.hostname))
        self.log.debug("Found executable '{}' at path '{}'".format(executable,
                                                                   executable_path))
        return executable_path

    @contextmanager
    def _remote_connection(self,
                           host: str,
                           addr: Optional[str] = None,
                           ) -> Iterator[Tuple["BaseConnection", Any]]:
        if not addr and host in self.inventory:
            addr = self.inventory.get_addr(host)

        self.offline_hosts_remove(host)

        try:
            try:
                if not addr:
                    raise OrchestratorError("host address is empty")
                conn, connr = self._get_connection(addr)
            except OSError as e:
                self._reset_con(host)
                msg = f"Can't communicate with remote host `{addr}`, possibly because python3 is not installed there: {str(e)}"
                raise execnet.gateway_bootstrap.HostNotFound(msg)

            yield (conn, connr)

        except execnet.gateway_bootstrap.HostNotFound as e:
            # this is a misleading exception as it seems to be thrown for
            # any sort of connection failure, even those having nothing to
            # do with "host not found" (e.g., ssh key permission denied).
            self.offline_hosts.add(host)
            self._reset_con(host)

            user = self.ssh_user if self.mode == 'root' else 'cephadm'
            if str(e).startswith("Can't communicate"):
                msg = str(e)
            else:
                msg = f'''Failed to connect to {host} ({addr}).
Please make sure that the host is reachable and accepts connections using the cephadm SSH key

To add the cephadm SSH key to the host:
> ceph cephadm get-pub-key > ~/ceph.pub
> ssh-copy-id -f -i ~/ceph.pub {user}@{host}

To check that the host is reachable:
> ceph cephadm get-ssh-config > ssh_config
> ceph config-key get mgr/cephadm/ssh_identity_key > ~/cephadm_private_key
> ssh -F ssh_config -i ~/cephadm_private_key {user}@{host}'''
            raise OrchestratorError(msg) from e
        except Exception as ex:
            self.log.exception(ex)
            raise

    def _get_container_image(self, daemon_name: str) -> Optional[str]:
        daemon_type = daemon_name.split('.', 1)[0]  # type: ignore
        if daemon_type in CEPH_TYPES or \
                daemon_type == 'nfs' or \
                daemon_type == 'iscsi':
            # get container image
            ret, image, err = self.check_mon_command({
                'prefix': 'config get',
                'who': utils.name_to_config_section(daemon_name),
                'key': 'container_image',
            })
            image = image.strip()  # type: ignore
        elif daemon_type == 'prometheus':
            image = self.container_image_prometheus
        elif daemon_type == 'grafana':
            image = self.container_image_grafana
        elif daemon_type == 'alertmanager':
            image = self.container_image_alertmanager
        elif daemon_type == 'node-exporter':
            image = self.container_image_node_exporter
        elif daemon_type == 'haproxy':
            image = self.container_image_haproxy
        elif daemon_type == 'keepalived':
            image = self.container_image_keepalived
        elif daemon_type == CustomContainerService.TYPE:
            # The image can't be resolved, the necessary information
            # is only available when a container is deployed (given
            # via spec).
            image = None
        else:
            assert False, daemon_type

        self.log.debug('%s container image %s' % (daemon_name, image))

        return image

    def _run_cephadm(self,
                     host: str,
                     entity: Union[CephadmNoImage, str],
                     command: str,
                     args: List[str],
                     addr: Optional[str] = "",
                     stdin: Optional[str] = "",
                     no_fsid: Optional[bool] = False,
                     error_ok: Optional[bool] = False,
                     image: Optional[str] = "",
                     env_vars: Optional[List[str]] = None,
                     ) -> Tuple[List[str], List[str], int]:
        """
        Run cephadm on the remote host with the given command + args

        :env_vars: in format -> [KEY=VALUE, ..]
        """
        self.log.debug(f"_run_cephadm : command = {command}")
        self.log.debug(f"_run_cephadm : args = {args}")

        bypass_image = ('cephadm-exporter',)

        with self._remote_connection(host, addr) as tpl:
            conn, connr = tpl
            assert image or entity
            # Skip the image check for daemons deployed that are not ceph containers
            if not str(entity).startswith(bypass_image):
                if not image and entity is not cephadmNoImage:
                    image = self._get_container_image(entity)

            final_args = []

            if env_vars:
                for env_var_pair in env_vars:
                    final_args.extend(['--env', env_var_pair])

            if image:
                final_args.extend(['--image', image])
            final_args.append(command)

            if not no_fsid:
                final_args += ['--fsid', self._cluster_fsid]

            if self.container_init:
                final_args += ['--container-init']

            final_args += args

            self.log.debug('args: %s' % (' '.join(final_args)))
            if self.mode == 'root':
                if stdin:
                    self.log.debug('stdin: %s' % stdin)
                script = 'injected_argv = ' + json.dumps(final_args) + '\n'
                if stdin:
                    script += 'injected_stdin = ' + json.dumps(stdin) + '\n'
                script += self._cephadm
                python = connr.choose_python()
                if not python:
                    raise RuntimeError(
                        'unable to find python on %s (tried %s in %s)' % (
                            host, remotes.PYTHONS, remotes.PATH))
                try:
                    out, err, code = remoto.process.check(
                        conn,
                        [python, '-u'],
                        stdin=script.encode('utf-8'))
                except RuntimeError as e:
                    self._reset_con(host)
                    if error_ok:
                        return [], [str(e)], 1
                    raise
            elif self.mode == 'cephadm-package':
                try:
                    out, err, code = remoto.process.check(
                        conn,
                        ['sudo', '/usr/bin/cephadm'] + final_args,
                        stdin=stdin)
                except RuntimeError as e:
                    self._reset_con(host)
                    if error_ok:
                        return [], [str(e)], 1
                    raise
            else:
                assert False, 'unsupported mode'

            self.log.debug('code: %d' % code)
            if out:
                self.log.debug('out: %s' % '\n'.join(out))
            if err:
                self.log.debug('err: %s' % '\n'.join(err))
            if code and not error_ok:
                raise OrchestratorError(
                    'cephadm exited with an error code: %d, stderr:%s' % (
                        code, '\n'.join(err)))
            return out, err, code

    def _hosts_with_daemon_inventory(self) -> List[HostSpec]:
        """
        Returns all usable hosts that went through _refresh_host_daemons().

        This mitigates a potential race, where new host was added *after*
        ``_refresh_host_daemons()`` was called, but *before*
        ``_apply_all_specs()`` was called. thus we end up with a hosts
        where daemons might be running, but we have not yet detected them.
        """
        return [
            h for h in self.inventory.all_specs()
            if self.cache.host_had_daemon_refresh(h.hostname) and
            h.status.lower() not in ['maintenance', 'offline']
        ]

    def _add_host(self, spec):
        # type: (HostSpec) -> str
        """
        Add a host to be managed by the orchestrator.

        :param host: host name
        """
        assert_valid_host(spec.hostname)
        out, err, code = self._run_cephadm(spec.hostname, cephadmNoImage, 'check-host',
                                           ['--expect-hostname', spec.hostname],
                                           addr=spec.addr,
                                           error_ok=True, no_fsid=True)
        if code:
            # err will contain stdout and stderr, so we filter on the message text to 
            # only show the errors
            errors = [_i.replace("ERROR: ", "") for _i in err if _i.startswith('ERROR')]
            raise OrchestratorError('New host %s (%s) failed check(s): %s' % (
                spec.hostname, spec.addr, errors))

        self.inventory.add_host(spec)
        self.cache.prime_empty_host(spec.hostname)
        self.offline_hosts_remove(spec.hostname)
        self.event.set()  # refresh stray health check
        self.log.info('Added host %s' % spec.hostname)
        return "Added host '{}'".format(spec.hostname)

    @trivial_completion
    def add_host(self, spec: HostSpec) -> str:
        return self._add_host(spec)

    @trivial_completion
    def remove_host(self, host):
        # type: (str) -> str
        """
        Remove a host from orchestrator management.

        :param host: host name
        """
        self.inventory.rm_host(host)
        self.cache.rm_host(host)
        self._reset_con(host)
        self.event.set()  # refresh stray health check
        self.log.info('Removed host %s' % host)
        return "Removed host '{}'".format(host)

    @trivial_completion
    def update_host_addr(self, host: str, addr: str) -> str:
        self.inventory.set_addr(host, addr)
        self._reset_con(host)
        self.event.set()  # refresh stray health check
        self.log.info('Set host %s addr to %s' % (host, addr))
        return "Updated host '{}' addr to '{}'".format(host, addr)

    @trivial_completion
    def get_hosts(self):
        # type: () -> List[orchestrator.HostSpec]
        """
        Return a list of hosts managed by the orchestrator.

        Notes:
          - skip async: manager reads from cache.
        """
        return list(self.inventory.all_specs())

    @trivial_completion
    def add_host_label(self, host: str, label: str) -> str:
        self.inventory.add_label(host, label)
        self.log.info('Added label %s to host %s' % (label, host))
        return 'Added label %s to host %s' % (label, host)

    @trivial_completion
    def remove_host_label(self, host: str, label: str) -> str:
        self.inventory.rm_label(host, label)
        self.log.info('Removed label %s to host %s' % (label, host))
        return 'Removed label %s from host %s' % (label, host)

    def _host_ok_to_stop(self, hostname: str) -> Tuple[int, str]:
        self.log.debug("running host-ok-to-stop checks")
        daemons = self.cache.get_daemons()
        daemon_map = defaultdict(lambda: [])
        for dd in daemons:
            if dd.hostname == hostname:
                daemon_map[dd.daemon_type].append(dd.daemon_id)

        for daemon_type, daemon_ids in daemon_map.items():
            r = self.cephadm_services[daemon_type_to_service(daemon_type)].ok_to_stop(daemon_ids)
            if r.retval:
                self.log.error(f'It is NOT safe to stop host {hostname}')
                return r.retval, r.stderr

        return 0, f'It is presumed safe to stop host {hostname}'

    @trivial_completion
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
            self.set_health_checks({
                "HOST_IN_MAINTENANCE": {}
            })
        else:
            s = "host is" if len(in_maintenance) == 1 else "hosts are"
            self.set_health_checks({
                "HOST_IN_MAINTENANCE": {
                    "severity": "warning",
                    "summary": f"{len(in_maintenance)} {s} in maintenance mode",
                    "detail": [f"{h} is in maintenance" for h in in_maintenance],
                }
            })

    @trivial_completion
    @host_exists()
    def enter_host_maintenance(self, hostname: str) -> str:
        """ Attempt to place a cluster host in maintenance

        Placing a host into maintenance disables the cluster's ceph target in systemd
        and stops all ceph daemons. If the host is an osd host we apply the noout flag
        for the host subtree in crush to prevent data movement during a host maintenance 
        window.

        :param hostname: (str) name of the host (must match an inventory hostname)

        :raises OrchestratorError: Hostname is invalid, host is already in maintenance
        """
        if len(self.cache.get_hosts()) == 1:
            raise OrchestratorError(f"Maintenance feature is not supported on single node clusters")

        # if upgrade is active, deny
        if self.upgrade.upgrade_state:
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
            rc, msg = self._host_ok_to_stop(hostname)
            if rc:
                raise OrchestratorError(msg, errno=rc)

            # call the host-maintenance function
            out, _err, _code = self._run_cephadm(hostname, cephadmNoImage, "host-maintenance",
                                                 ["enter"],
                                                 error_ok=True)
            if out:
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
                if rc:
                    self.log.warning(
                        f"maintenance mode request for {hostname} failed to SET the noout group (rc={rc})")
                    raise OrchestratorError(
                        f"Unable to set the osds on {hostname} to noout (rc={rc})")
                else:
                    self.log.info(
                        f"maintenance mode request for {hostname} has SET the noout group")

        # update the host status in the inventory
        tgt_host["status"] = "maintenance"
        self.inventory._inventory[hostname] = tgt_host
        self.inventory.save()

        self._set_maintenance_healthcheck()

        return f"Ceph cluster {self._cluster_fsid} on {hostname} moved to maintenance"

    @trivial_completion
    @host_exists()
    def exit_host_maintenance(self, hostname: str) -> str:
        """Exit maintenance mode and return a host to an operational state

        Returning from maintnenance will enable the clusters systemd target and
        start it, and remove any noout that has been added for the host if the 
        host has osd daemons

        :param hostname: (str) host name

        :raises OrchestratorError: Unable to return from maintenance, or unset the
                                   noout flag
        """
        tgt_host = self.inventory._inventory[hostname]
        if tgt_host['status'] != "maintenance":
            raise OrchestratorError(f"Host {hostname} is not in maintenance mode")

        out, _err, _code = self._run_cephadm(hostname, cephadmNoImage, 'host-maintenance',
                                             ['exit'],
                                             error_ok=True)
        if out:
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

    def get_minimal_ceph_conf(self) -> str:
        _, config, _ = self.check_mon_command({
            "prefix": "config generate-minimal-conf",
        })
        extra = self.extra_ceph_conf().conf
        if extra:
            config += '\n\n' + extra.strip() + '\n'
        return config

    def _invalidate_daemons_and_kick_serve(self, filter_host: Optional[str] = None) -> None:
        if filter_host:
            self.cache.invalidate_host_daemons(filter_host)
        else:
            for h in self.cache.get_hosts():
                # Also discover daemons deployed manually
                self.cache.invalidate_host_daemons(h)

        self._kick_serve_loop()

    @trivial_completion
    def describe_service(self, service_type: Optional[str] = None, service_name: Optional[str] = None,
                         refresh: bool = False) -> List[orchestrator.ServiceDescription]:
        if refresh:
            self._invalidate_daemons_and_kick_serve()
            self.log.info('Kicked serve() loop to refresh all services')

        # <service_map>
        sm: Dict[str, orchestrator.ServiceDescription] = {}
        osd_count = 0
        for h, dm in self.cache.get_daemons_with_volatile_status():
            for name, dd in dm.items():
                if service_type and service_type != dd.daemon_type:
                    continue
                n: str = dd.service_name()
                if service_name and service_name != n:
                    continue
                if dd.daemon_type == 'osd':
                    """
                    OSDs do not know the affinity to their spec out of the box.
                    """
                    n = f"osd.{dd.osdspec_affinity}"
                    if not dd.osdspec_affinity:
                        # If there is no osdspec_affinity, the spec should suffice for displaying
                        continue
                if n in self.spec_store.specs:
                    spec = self.spec_store.specs[n]
                else:
                    spec = ServiceSpec(
                        unmanaged=True,
                        service_type=dd.daemon_type,
                        service_id=dd.service_id(),
                        placement=PlacementSpec(
                            hosts=[dd.hostname]
                        )
                    )
                if n not in sm:
                    sm[n] = orchestrator.ServiceDescription(
                        last_refresh=dd.last_refresh,
                        container_image_id=dd.container_image_id,
                        container_image_name=dd.container_image_name,
                        spec=spec,
                        events=self.events.get_for_service(spec.service_name()),
                    )
                if n in self.spec_store.specs:
                    if dd.daemon_type == 'osd':
                        """
                        The osd count can't be determined by the Placement spec.
                        Showing an actual/expected representation cannot be determined
                        here. So we're setting running = size for now.
                        """
                        osd_count += 1
                        sm[n].size = osd_count
                    else:
                        sm[n].size = spec.placement.get_host_selection_size(
                            self.inventory.all_specs())

                    sm[n].created = self.spec_store.spec_created[n]
                    if service_type == 'nfs':
                        spec = cast(NFSServiceSpec, spec)
                        sm[n].rados_config_location = spec.rados_config_location()
                else:
                    sm[n].size = 0
                if dd.status == 1:
                    sm[n].running += 1
                if not sm[n].last_refresh or not dd.last_refresh or dd.last_refresh < sm[n].last_refresh:  # type: ignore
                    sm[n].last_refresh = dd.last_refresh
                if sm[n].container_image_id != dd.container_image_id:
                    sm[n].container_image_id = 'mix'
                if sm[n].container_image_name != dd.container_image_name:
                    sm[n].container_image_name = 'mix'
                if dd.daemon_type == 'haproxy' or dd.daemon_type == 'keepalived':
                    # ha-rgw has 2 daemons running per host
                    sm[n].size = sm[n].size*2
        for n, spec in self.spec_store.specs.items():
            if n in sm:
                continue
            if service_type is not None and service_type != spec.service_type:
                continue
            if service_name is not None and service_name != n:
                continue
            sm[n] = orchestrator.ServiceDescription(
                spec=spec,
                size=spec.placement.get_host_selection_size(self.inventory.all_specs()),
                running=0,
                events=self.events.get_for_service(spec.service_name()),
            )
            if service_type == 'nfs':
                spec = cast(NFSServiceSpec, spec)
                sm[n].rados_config_location = spec.rados_config_location()
            if spec.service_type == 'ha-rgw':
                # ha-rgw has 2 daemons running per host
                sm[n].size = sm[n].size*2
        return list(sm.values())

    @trivial_completion
    def list_daemons(self,
                     service_name: Optional[str] = None,
                     daemon_type: Optional[str] = None,
                     daemon_id: Optional[str] = None,
                     host: Optional[str] = None,
                     refresh: bool = False) -> List[orchestrator.DaemonDescription]:
        if refresh:
            self._invalidate_daemons_and_kick_serve(host)
            self.log.info('Kicked serve() loop to refresh all daemons')

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
                result.append(dd)
        return result

    @trivial_completion
    def service_action(self, action: str, service_name: str) -> List[str]:
        dds: List[DaemonDescription] = self.cache.get_daemons_by_service(service_name)
        self.log.info('%s service %s' % (action.capitalize(), service_name))
        return [
            self._schedule_daemon_action(dd.name(), action)
            for dd in dds
        ]

    def _daemon_action(self, daemon_type: str, daemon_id: str, host: str, action: str, image: Optional[str] = None) -> str:
        daemon_spec: CephadmDaemonSpec = CephadmDaemonSpec(
            host=host,
            daemon_id=daemon_id,
            daemon_type=daemon_type,
        )

        self._daemon_action_set_image(action, image, daemon_type, daemon_id)

        if action == 'redeploy':
            if self.daemon_is_self(daemon_type, daemon_id):
                self.mgr_service.fail_over()
                return ''  # unreachable
            # stop, recreate the container+unit, then restart
            return self._create_daemon(daemon_spec)
        elif action == 'reconfig':
            return self._create_daemon(daemon_spec, reconfig=True)

        actions = {
            'start': ['reset-failed', 'start'],
            'stop': ['stop'],
            'restart': ['reset-failed', 'restart'],
        }
        name = daemon_spec.name()
        for a in actions[action]:
            try:
                out, err, code = self._run_cephadm(
                    host, name, 'unit',
                    ['--name', name, a])
            except Exception:
                self.log.exception(f'`{host}: cephadm unit {name} {a}` failed')
        self.cache.invalidate_host_daemons(daemon_spec.host)
        msg = "{} {} from host '{}'".format(action, name, daemon_spec.host)
        self.events.for_daemon(name, 'INFO', msg)
        return msg

    def _daemon_action_set_image(self, action: str, image: Optional[str], daemon_type: str, daemon_id: str) -> None:
        if image is not None:
            if action != 'redeploy':
                raise OrchestratorError(
                    f'Cannot execute {action} with new image. `action` needs to be `redeploy`')
            if daemon_type not in CEPH_TYPES:
                raise OrchestratorError(
                    f'Cannot redeploy {daemon_type}.{daemon_id} with a new image: Supported '
                    f'types are: {", ".join(CEPH_TYPES)}')

            self.check_mon_command({
                'prefix': 'config set',
                'name': 'container_image',
                'value': image,
                'who': utils.name_to_config_section(daemon_type + '.' + daemon_id),
            })

    @trivial_completion
    def daemon_action(self, action: str, daemon_name: str, image: Optional[str] = None) -> str:
        d = self.cache.get_daemon(daemon_name)

        if action == 'redeploy' and self.daemon_is_self(d.daemon_type, d.daemon_id) \
                and not self.mgr_service.mgr_map_has_standby():
            raise OrchestratorError(
                f'Unable to schedule redeploy for {daemon_name}: No standby MGRs')

        self._daemon_action_set_image(action, image, d.daemon_type, d.daemon_id)

        self.log.info(f'Schedule {action} daemon {daemon_name}')
        return self._schedule_daemon_action(daemon_name, action)

    def daemon_is_self(self, daemon_type: str, daemon_id: str) -> bool:
        return daemon_type == 'mgr' and daemon_id == self.get_mgr_id()

    def _schedule_daemon_action(self, daemon_name: str, action: str) -> str:
        dd = self.cache.get_daemon(daemon_name)
        if action == 'redeploy' and self.daemon_is_self(dd.daemon_type, dd.daemon_id) \
                and not self.mgr_service.mgr_map_has_standby():
            raise OrchestratorError(
                f'Unable to schedule redeploy for {daemon_name}: No standby MGRs')
        self.cache.schedule_daemon_action(dd.hostname, dd.name(), action)
        msg = "Scheduled to {} {} on host '{}'".format(action, daemon_name, dd.hostname)
        self._kick_serve_loop()
        return msg

    @trivial_completion
    def remove_daemons(self, names):
        # type: (List[str]) -> List[str]
        args = []
        for host, dm in self.cache.daemons.items():
            for name in names:
                if name in dm:
                    args.append((name, host))
        if not args:
            raise OrchestratorError('Unable to find daemon(s) %s' % (names))
        self.log.info('Remove daemons %s' % [a[0] for a in args])
        return self._remove_daemons(args)

    @trivial_completion
    def remove_service(self, service_name: str) -> str:
        self.log.info('Remove service %s' % service_name)
        self._trigger_preview_refresh(service_name=service_name)
        found = self.spec_store.rm(service_name)
        if found:
            self._kick_serve_loop()
            service = self.cephadm_services.get(service_name, None)
            if service:
                service.purge()
            return 'Removed service %s' % service_name
        else:
            # must be idempotent: still a success.
            return f'Failed to remove service. <{service_name}> was not found.'

    @trivial_completion
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
                    self.cache.invalidate_host_devices(h)
            else:
                for h in self.cache.get_hosts():
                    self.cache.invalidate_host_devices(h)

            self.event.set()
            self.log.info('Kicked serve() loop to refresh devices')

        result = []
        for host, dls in self.cache.devices.items():
            if host_filter and host_filter.hosts and host not in host_filter.hosts:
                continue
            result.append(orchestrator.InventoryHost(host,
                                                     inventory.Devices(dls)))
        return result

    @trivial_completion
    def zap_device(self, host: str, path: str) -> str:
        self.log.info('Zap device %s:%s' % (host, path))
        out, err, code = self._run_cephadm(
            host, 'osd', 'ceph-volume',
            ['--', 'lvm', 'zap', '--destroy', path],
            error_ok=True)
        self.cache.invalidate_host_devices(host)
        if code:
            raise OrchestratorError('Zap failed: %s' % '\n'.join(out + err))
        return '\n'.join(out + err)

    @trivial_completion
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

            out, err, code = self._run_cephadm(
                host, 'osd', 'shell', ['--'] + cmd_args,
                error_ok=True)
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
            if not only_up or (o['up_from'] > 0):
                r[str(osd_id)] = o.get('uuid', '')
        return r

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

    @trivial_completion
    def apply_drivegroups(self, specs: List[DriveGroupSpec]) -> List[str]:
        """
        Deprecated. Please use `apply()` instead.

        Keeping this around to be compapatible to mgr/dashboard
        """
        return [self._apply(spec) for spec in specs]

    @trivial_completion
    def create_osds(self, drive_group: DriveGroupSpec) -> str:
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

    def _calc_daemon_deps(self, daemon_type: str, daemon_id: str) -> List[str]:
        need = {
            'prometheus': ['mgr', 'alertmanager', 'node-exporter'],
            'grafana': ['prometheus'],
            'alertmanager': ['mgr', 'alertmanager'],
        }
        deps = []
        for dep_type in need.get(daemon_type, []):
            for dd in self.cache.get_daemons_by_service(dep_type):
                deps.append(dd.name())
        return sorted(deps)

    def _create_daemon(self,
                       daemon_spec: CephadmDaemonSpec,
                       reconfig: bool = False,
                       osd_uuid_map: Optional[Dict[str, Any]] = None,
                       ) -> str:

        with set_exception_subject('service', orchestrator.DaemonDescription(
                daemon_type=daemon_spec.daemon_type,
                daemon_id=daemon_spec.daemon_id,
                hostname=daemon_spec.host,
        ).service_id(), overwrite=True):

            image = ''
            start_time = datetime_now()
            ports: List[int] = daemon_spec.ports if daemon_spec.ports else []

            if daemon_spec.daemon_type == 'container':
                spec: Optional[CustomContainerSpec] = daemon_spec.spec
                if spec is None:
                    # Exit here immediately because the required service
                    # spec to create a daemon is not provided. This is only
                    # provided when a service is applied via 'orch apply'
                    # command.
                    msg = "Failed to {} daemon {} on {}: Required " \
                          "service specification not provided".format(
                              'reconfigure' if reconfig else 'deploy',
                              daemon_spec.name(), daemon_spec.host)
                    self.log.info(msg)
                    return msg
                image = spec.image
                if spec.ports:
                    ports.extend(spec.ports)

            if daemon_spec.daemon_type == 'cephadm-exporter':
                if not reconfig:
                    assert daemon_spec.host
                    deploy_ok = self._deploy_cephadm_binary(daemon_spec.host)
                    if not deploy_ok:
                        msg = f"Unable to deploy the cephadm binary to {daemon_spec.host}"
                        self.log.warning(msg)
                        return msg

            if daemon_spec.daemon_type == 'haproxy':
                haspec = cast(HA_RGWSpec, daemon_spec.spec)
                if haspec.haproxy_container_image:
                    image = haspec.haproxy_container_image

            if daemon_spec.daemon_type == 'keepalived':
                haspec = cast(HA_RGWSpec, daemon_spec.spec)
                if haspec.keepalived_container_image:
                    image = haspec.keepalived_container_image

            cephadm_config, deps = self.cephadm_services[daemon_type_to_service(daemon_spec.daemon_type)].generate_config(
                daemon_spec)

            # TCP port to open in the host firewall
            if len(ports) > 0:
                daemon_spec.extra_args.extend([
                    '--tcp-ports', ' '.join(map(str, ports))
                ])

            # osd deployments needs an --osd-uuid arg
            if daemon_spec.daemon_type == 'osd':
                if not osd_uuid_map:
                    osd_uuid_map = self.get_osd_uuid_map()
                osd_uuid = osd_uuid_map.get(daemon_spec.daemon_id)
                if not osd_uuid:
                    raise OrchestratorError('osd.%s not in osdmap' % daemon_spec.daemon_id)
                daemon_spec.extra_args.extend(['--osd-fsid', osd_uuid])

            if reconfig:
                daemon_spec.extra_args.append('--reconfig')
            if self.allow_ptrace:
                daemon_spec.extra_args.append('--allow-ptrace')

            if self.cache.host_needs_registry_login(daemon_spec.host) and self.registry_url:
                self._registry_login(daemon_spec.host, self.registry_url,
                                     self.registry_username, self.registry_password)

            daemon_spec.extra_args.extend(['--config-json', '-'])

            self.log.info('%s daemon %s on %s' % (
                'Reconfiguring' if reconfig else 'Deploying',
                daemon_spec.name(), daemon_spec.host))

            out, err, code = self._run_cephadm(
                daemon_spec.host, daemon_spec.name(), 'deploy',
                [
                    '--name', daemon_spec.name(),
                ] + daemon_spec.extra_args,
                stdin=json.dumps(cephadm_config),
                image=image)
            if not code and daemon_spec.host in self.cache.daemons:
                # prime cached service state with what we (should have)
                # just created
                sd = orchestrator.DaemonDescription()
                sd.daemon_type = daemon_spec.daemon_type
                sd.daemon_id = daemon_spec.daemon_id
                sd.hostname = daemon_spec.host
                sd.status = 1
                sd.status_desc = 'starting'
                self.cache.add_daemon(daemon_spec.host, sd)
                if daemon_spec.daemon_type in ['grafana', 'iscsi', 'prometheus', 'alertmanager']:
                    self.requires_post_actions.add(daemon_spec.daemon_type)
            self.cache.invalidate_host_daemons(daemon_spec.host)
            self.cache.update_daemon_config_deps(
                daemon_spec.host, daemon_spec.name(), deps, start_time)
            self.cache.save_host(daemon_spec.host)
            msg = "{} {} on host '{}'".format(
                'Reconfigured' if reconfig else 'Deployed', daemon_spec.name(), daemon_spec.host)
            if not code:
                self.events.for_daemon(daemon_spec.name(), OrchestratorEvent.INFO, msg)
            else:
                what = 'reconfigure' if reconfig else 'deploy'
                self.events.for_daemon(
                    daemon_spec.name(), OrchestratorEvent.ERROR, f'Failed to {what}: {err}')
            return msg

    def _deploy_cephadm_binary(self, host: str) -> bool:
        # Use tee (from coreutils) to create a copy of cephadm on the target machine
        self.log.info(f"Deploying cephadm binary to {host}")
        with self._remote_connection(host) as tpl:
            conn, _connr = tpl
            _out, _err, code = remoto.process.check(
                conn,
                ['tee', '-', '/var/lib/ceph/{}/cephadm'.format(self._cluster_fsid)],
                stdin=self._cephadm.encode('utf-8'))
        return code == 0

    @forall_hosts
    def _remove_daemons(self, name: str, host: str) -> str:
        return self._remove_daemon(name, host)

    def _remove_daemon(self, name: str, host: str) -> str:
        """
        Remove a daemon
        """
        (daemon_type, daemon_id) = name.split('.', 1)
        daemon = orchestrator.DaemonDescription(
            daemon_type=daemon_type,
            daemon_id=daemon_id,
            hostname=host)

        with set_exception_subject('service', daemon.service_id(), overwrite=True):

            self.cephadm_services[daemon_type_to_service(daemon_type)].pre_remove(daemon)

            args = ['--name', name, '--force']
            self.log.info('Removing daemon %s from %s' % (name, host))
            out, err, code = self._run_cephadm(
                host, name, 'rm-daemon', args)
            if not code:
                # remove item from cache
                self.cache.rm_daemon(host, name)
            self.cache.invalidate_host_daemons(host)

            self.cephadm_services[daemon_type_to_service(daemon_type)].post_remove(daemon)

            return "Removed {} from host '{}'".format(name, host)

    def _check_pool_exists(self, pool: str, service_name: str) -> None:
        logger.info(f'Checking pool "{pool}" exists for service {service_name}')
        if not self.rados.pool_exists(pool):
            raise OrchestratorError(f'Cannot find pool "{pool}" for '
                                    f'service {service_name}')

    def _add_daemon(self,
                    daemon_type: str,
                    spec: ServiceSpec,
                    create_func: Callable[..., CephadmDaemonSpec],
                    config_func: Optional[Callable] = None) -> List[str]:
        """
        Add (and place) a daemon. Require explicit host placement.  Do not
        schedule, and do not apply the related scheduling limitations.
        """
        self.log.debug('_add_daemon %s spec %s' % (daemon_type, spec.placement))
        if not spec.placement.hosts:
            raise OrchestratorError('must specify host(s) to deploy on')
        count = spec.placement.count or len(spec.placement.hosts)
        daemons = self.cache.get_daemons_by_service(spec.service_name())
        return self._create_daemons(daemon_type, spec, daemons,
                                    spec.placement.hosts, count,
                                    create_func, config_func)

    def _create_daemons(self,
                        daemon_type: str,
                        spec: ServiceSpec,
                        daemons: List[DaemonDescription],
                        hosts: List[HostPlacementSpec],
                        count: int,
                        create_func: Callable[..., CephadmDaemonSpec],
                        config_func: Optional[Callable] = None) -> List[str]:
        if count > len(hosts):
            raise OrchestratorError('too few hosts: want %d, have %s' % (
                count, hosts))

        did_config = False

        args = []  # type: List[CephadmDaemonSpec]
        for host, network, name in hosts:
            daemon_id = self.get_unique_name(daemon_type, host, daemons,
                                             prefix=spec.service_id,
                                             forcename=name)

            if not did_config and config_func:
                if daemon_type == 'rgw':
                    config_func(spec, daemon_id)
                else:
                    config_func(spec)
                did_config = True

            daemon_spec = self.cephadm_services[daemon_type_to_service(daemon_type)].make_daemon_spec(
                host, daemon_id, network, spec)
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

        @forall_hosts
        def create_func_map(*args: Any) -> str:
            daemon_spec = create_func(*args)
            return self._create_daemon(daemon_spec)

        return create_func_map(args)

    @trivial_completion
    def apply_mon(self, spec: ServiceSpec) -> str:
        return self._apply(spec)

    @trivial_completion
    def add_mon(self, spec):
        # type: (ServiceSpec) -> List[str]
        return self._add_daemon('mon', spec, self.mon_service.prepare_create)

    @trivial_completion
    def add_mgr(self, spec):
        # type: (ServiceSpec) -> List[str]
        return self._add_daemon('mgr', spec, self.mgr_service.prepare_create)

    def _apply(self, spec: GenericSpec) -> str:
        if spec.service_type == 'host':
            return self._add_host(cast(HostSpec, spec))

        if spec.service_type == 'osd':
            # _trigger preview refresh needs to be smart and
            # should only refresh if a change has been detected
            self._trigger_preview_refresh(specs=[cast(DriveGroupSpec, spec)])

        return self._apply_service_spec(cast(ServiceSpec, spec))

    def _plan(self, spec: ServiceSpec) -> dict:
        if spec.service_type == 'osd':
            return {'service_name': spec.service_name(),
                    'service_type': spec.service_type,
                    'data': self._preview_osdspecs(osdspecs=[cast(DriveGroupSpec, spec)])}

        ha = HostAssignment(
            spec=spec,
            hosts=self._hosts_with_daemon_inventory(),
            get_daemons_func=self.cache.get_daemons_by_service,
        )
        ha.validate()
        hosts = ha.place()

        add_daemon_hosts = ha.add_daemon_hosts(hosts)
        remove_daemon_hosts = ha.remove_daemon_hosts(hosts)

        return {
            'service_name': spec.service_name(),
            'service_type': spec.service_type,
            'add': [hs.hostname for hs in add_daemon_hosts],
            'remove': [d.hostname for d in remove_daemon_hosts]
        }

    @trivial_completion
    def plan(self, specs: List[GenericSpec]) -> List:
        results = [{'warning': 'WARNING! Dry-Runs are snapshots of a certain point in time and are bound \n'
                               'to the current inventory setup. If any on these conditions changes, the \n'
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
                'ha-rgw': PlacementSpec(count=2),
                'iscsi': PlacementSpec(count=1),
                'rbd-mirror': PlacementSpec(count=2),
                'nfs': PlacementSpec(count=1),
                'grafana': PlacementSpec(count=1),
                'alertmanager': PlacementSpec(count=1),
                'prometheus': PlacementSpec(count=1),
                'node-exporter': PlacementSpec(host_pattern='*'),
                'crash': PlacementSpec(host_pattern='*'),
                'container': PlacementSpec(count=1),
                'cephadm-exporter': PlacementSpec(host_pattern='*'),
            }
            spec.placement = defaults[spec.service_type]
        elif spec.service_type in ['mon', 'mgr'] and \
                spec.placement.count is not None and \
                spec.placement.count < 1:
            raise OrchestratorError('cannot scale %s service below 1' % (
                spec.service_type))

        HostAssignment(
            spec=spec,
            hosts=self.inventory.all_specs(),  # All hosts, even those without daemon refresh
            get_daemons_func=self.cache.get_daemons_by_service,
        ).validate()

        self.log.info('Saving service %s spec with placement %s' % (
            spec.service_name(), spec.placement.pretty_str()))
        self.spec_store.save(spec)
        self._kick_serve_loop()
        return "Scheduled %s update..." % spec.service_name()

    @trivial_completion
    def apply(self, specs: List[GenericSpec]) -> List[str]:
        results = []
        for spec in specs:
            results.append(self._apply(spec))
        return results

    @trivial_completion
    def apply_mgr(self, spec: ServiceSpec) -> str:
        return self._apply(spec)

    @trivial_completion
    def add_mds(self, spec: ServiceSpec) -> List[str]:
        return self._add_daemon('mds', spec, self.mds_service.prepare_create, self.mds_service.config)

    @trivial_completion
    def apply_mds(self, spec: ServiceSpec) -> str:
        return self._apply(spec)

    @trivial_completion
    def add_rgw(self, spec: ServiceSpec) -> List[str]:
        return self._add_daemon('rgw', spec, self.rgw_service.prepare_create, self.rgw_service.config)

    @trivial_completion
    def apply_rgw(self, spec: ServiceSpec) -> str:
        return self._apply(spec)

    @trivial_completion
    def apply_ha_rgw(self, spec: ServiceSpec) -> str:
        return self._apply(spec)

    @trivial_completion
    def add_iscsi(self, spec):
        # type: (ServiceSpec) -> List[str]
        return self._add_daemon('iscsi', spec, self.iscsi_service.prepare_create, self.iscsi_service.config)

    @trivial_completion
    def apply_iscsi(self, spec: ServiceSpec) -> str:
        return self._apply(spec)

    @trivial_completion
    def add_rbd_mirror(self, spec: ServiceSpec) -> List[str]:
        return self._add_daemon('rbd-mirror', spec, self.rbd_mirror_service.prepare_create)

    @trivial_completion
    def apply_rbd_mirror(self, spec: ServiceSpec) -> str:
        return self._apply(spec)

    @trivial_completion
    def add_nfs(self, spec: ServiceSpec) -> List[str]:
        return self._add_daemon('nfs', spec, self.nfs_service.prepare_create, self.nfs_service.config)

    @trivial_completion
    def apply_nfs(self, spec: ServiceSpec) -> str:
        return self._apply(spec)

    def _get_dashboard_url(self):
        # type: () -> str
        return self.get('mgr_map').get('services', {}).get('dashboard', '')

    @trivial_completion
    def add_prometheus(self, spec: ServiceSpec) -> List[str]:
        return self._add_daemon('prometheus', spec, self.prometheus_service.prepare_create)

    @trivial_completion
    def apply_prometheus(self, spec: ServiceSpec) -> str:
        return self._apply(spec)

    @trivial_completion
    def add_node_exporter(self, spec):
        # type: (ServiceSpec) -> List[str]
        return self._add_daemon('node-exporter', spec,
                                self.node_exporter_service.prepare_create)

    @trivial_completion
    def apply_node_exporter(self, spec: ServiceSpec) -> str:
        return self._apply(spec)

    @trivial_completion
    def add_crash(self, spec):
        # type: (ServiceSpec) -> List[str]
        return self._add_daemon('crash', spec,
                                self.crash_service.prepare_create)

    @trivial_completion
    def apply_crash(self, spec: ServiceSpec) -> str:
        return self._apply(spec)

    @trivial_completion
    def add_grafana(self, spec):
        # type: (ServiceSpec) -> List[str]
        return self._add_daemon('grafana', spec, self.grafana_service.prepare_create)

    @trivial_completion
    def apply_grafana(self, spec: ServiceSpec) -> str:
        return self._apply(spec)

    @trivial_completion
    def add_alertmanager(self, spec):
        # type: (ServiceSpec) -> List[str]
        return self._add_daemon('alertmanager', spec, self.alertmanager_service.prepare_create)

    @trivial_completion
    def apply_alertmanager(self, spec: ServiceSpec) -> str:
        return self._apply(spec)

    @trivial_completion
    def add_container(self, spec: ServiceSpec) -> List[str]:
        return self._add_daemon('container', spec,
                                self.container_service.prepare_create)

    @trivial_completion
    def apply_container(self, spec: ServiceSpec) -> str:
        return self._apply(spec)

    @trivial_completion
    def add_cephadm_exporter(self, spec):
        # type: (ServiceSpec) -> List[str]
        return self._add_daemon('cephadm-exporter',
                                spec,
                                self.cephadm_exporter_service.prepare_create)

    @trivial_completion
    def apply_cephadm_exporter(self, spec: ServiceSpec) -> str:
        return self._apply(spec)

    def _get_container_image_info(self, image_name: str) -> ContainerInspectInfo:
        # pick a random host...
        host = None
        for host_name in self.inventory.keys():
            host = host_name
            break
        if not host:
            raise OrchestratorError('no hosts defined')
        if self.cache.host_needs_registry_login(host) and self.registry_url:
            self._registry_login(host, self.registry_url,
                                 self.registry_username, self.registry_password)
        out, err, code = self._run_cephadm(
            host, '', 'pull', [],
            image=image_name,
            no_fsid=True,
            error_ok=True)
        if code:
            raise OrchestratorError('Failed to pull %s on %s: %s' % (
                image_name, host, '\n'.join(out)))
        try:
            j = json.loads('\n'.join(out))
            r = ContainerInspectInfo(
                j['image_id'],
                j.get('ceph_version'),
                j.get('repo_digest')
            )
            self.log.debug(f'image {image_name} -> {r}')
            return r
        except (ValueError, KeyError) as _:
            msg = 'Failed to pull %s on %s: Cannot decode JSON' % (image_name, host)
            self.log.exception('%s: \'%s\'' % (msg, '\n'.join(out)))
            raise OrchestratorError(msg)

    @trivial_completion
    def upgrade_check(self, image: str, version: str) -> str:
        if self.inventory.get_host_with_state("maintenance"):
            raise OrchestratorError("check aborted - you have hosts in maintenance state")

        if version:
            target_name = self.container_image_base + ':v' + version
        elif image:
            target_name = image
        else:
            raise OrchestratorError('must specify either image or version')

        image_info = self._get_container_image_info(target_name)
        self.log.debug(f'image info {image} -> {image_info}')
        r: dict = {
            'target_name': target_name,
            'target_id': image_info.image_id,
            'target_version': image_info.ceph_version,
            'needs_update': dict(),
            'up_to_date': list(),
        }
        for host, dm in self.cache.daemons.items():
            for name, dd in dm.items():
                if image_info.image_id == dd.container_image_id:
                    r['up_to_date'].append(dd.name())
                else:
                    r['needs_update'][dd.name()] = {
                        'current_name': dd.container_image_name,
                        'current_id': dd.container_image_id,
                        'current_version': dd.version,
                    }
        if self.use_repo_digest:
            r['target_digest'] = image_info.repo_digest

        return json.dumps(r, indent=4, sort_keys=True)

    @trivial_completion
    def upgrade_status(self) -> orchestrator.UpgradeStatusSpec:
        return self.upgrade.upgrade_status()

    @trivial_completion
    def upgrade_start(self, image: str, version: str) -> str:
        if self.inventory.get_host_with_state("maintenance"):
            raise OrchestratorError("upgrade aborted - you have host(s) in maintenance state")
        return self.upgrade.upgrade_start(image, version)

    @trivial_completion
    def upgrade_pause(self) -> str:
        return self.upgrade.upgrade_pause()

    @trivial_completion
    def upgrade_resume(self) -> str:
        return self.upgrade.upgrade_resume()

    @trivial_completion
    def upgrade_stop(self) -> str:
        return self.upgrade.upgrade_stop()

    @trivial_completion
    def remove_osds(self, osd_ids: List[str],
                    replace: bool = False,
                    force: bool = False) -> str:
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
            try:
                self.to_remove_osds.enqueue(OSD(osd_id=int(daemon.daemon_id),
                                                replace=replace,
                                                force=force,
                                                hostname=daemon.hostname,
                                                fullname=daemon.name(),
                                                process_started_at=datetime_now(),
                                                remove_util=self.rm_util))
            except NotFoundError:
                return f"Unable to find OSDs: {osd_ids}"

        # trigger the serve loop to initiate the removal
        self._kick_serve_loop()
        return "Scheduled OSD(s) for removal"

    @trivial_completion
    def stop_remove_osds(self, osd_ids: List[str]) -> str:
        """
        Stops a `removal` process for a List of OSDs.
        This will revert their weight and remove it from the osds_to_remove queue
        """
        for osd_id in osd_ids:
            try:
                self.to_remove_osds.rm(OSD(osd_id=int(osd_id),
                                           remove_util=self.rm_util))
            except (NotFoundError, KeyError):
                return f'Unable to find OSD in the queue: {osd_id}'

        # trigger the serve loop to halt the removal
        self._kick_serve_loop()
        return "Stopped OSD(s) removal"

    @trivial_completion
    def remove_osds_status(self) -> List[OSD]:
        """
        The CLI call to retrieve an osd removal report
        """
        return self.to_remove_osds.all_osds()
