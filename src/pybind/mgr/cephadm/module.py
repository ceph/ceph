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
    CustomContainerSpec
from cephadm.serve import CephadmServe
from cephadm.services.cephadmservice import CephadmDaemonSpec

from mgr_module import MgrModule, HandleCommandResult
import orchestrator
from orchestrator import OrchestratorError, OrchestratorValidationError, HostSpec, \
    CLICommandMeta, OrchestratorEvent, set_exception_subject, DaemonDescription
from orchestrator._interface import GenericSpec

from . import remotes
from . import utils
from .migrations import Migrations
from .services.cephadmservice import MonService, MgrService, MdsService, RgwService, \
    RbdMirrorService, CrashService, CephadmService
from .services.container import CustomContainerService
from .services.iscsi import IscsiService
from .services.nfs import NFSService
from .services.osd import RemoveUtil, OSDQueue, OSDService, OSD, NotFoundError
from .services.monitoring import GrafanaService, AlertmanagerService, PrometheusService, \
    NodeExporterService
from .schedule import HostAssignment
from .inventory import Inventory, SpecStore, HostCache, EventStore
from .upgrade import CEPH_UPGRADE_ORDER, CephadmUpgrade
from .template import TemplateMgr
from .utils import forall_hosts, CephadmNoImage, cephadmNoImage, \
    str_to_datetime, datetime_to_str

try:
    import remoto
    # NOTE(mattoliverau) Patch remoto until remoto PR
    # (https://github.com/alfredodeza/remoto/pull/56) lands
    from distutils.version import StrictVersion
    if StrictVersion(remoto.__version__) <= StrictVersion('1.2'):
        def remoto_has_connection(self):
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

CEPH_DATEFMT = '%Y-%m-%dT%H:%M:%S.%fZ'

CEPH_TYPES = set(CEPH_UPGRADE_ORDER)


class CephadmCompletion(orchestrator.Completion[T]):
    def evaluate(self):
        self.finalize(None)


def trivial_completion(f: Callable[..., T]) -> Callable[..., CephadmCompletion[T]]:
    """
    Decorator to make CephadmCompletion methods return
    a completion object that executes themselves.
    """

    @wraps(f)
    def wrapper(*args, **kwargs):
        return CephadmCompletion(on_complete=lambda _: f(*args, **kwargs))

    return wrapper


class ContainerInspectInfo(NamedTuple):
    image_id: str
    ceph_version: Optional[str]
    repo_digest: Optional[str]


class CephadmOrchestrator(orchestrator.Orchestrator, MgrModule,
                          metaclass=CLICommandMeta):

    _STORE_HOST_PREFIX = "host"

    instance = None
    NATIVE_OPTIONS = []  # type: List[Any]
    MODULE_OPTIONS: List[dict] = [
        {
            'name': 'ssh_config_file',
            'type': 'str',
            'default': None,
            'desc': 'customized SSH config file to connect to managed hosts',
        },
        {
            'name': 'device_cache_timeout',
            'type': 'secs',
            'default': 30 * 60,
            'desc': 'seconds to cache device inventory',
        },
        {
            'name': 'daemon_cache_timeout',
            'type': 'secs',
            'default': 10 * 60,
            'desc': 'seconds to cache service (daemon) inventory',
        },
        {
            'name': 'host_check_interval',
            'type': 'secs',
            'default': 10 * 60,
            'desc': 'how frequently to perform a host check',
        },
        {
            'name': 'mode',
            'type': 'str',
            'enum_allowed': ['root', 'cephadm-package'],
            'default': 'root',
            'desc': 'mode for remote execution of cephadm',
        },
        {
            'name': 'container_image_base',
            'default': 'docker.io/ceph/ceph',
            'desc': 'Container image name, without the tag',
            'runtime': True,
        },
        {
            'name': 'container_image_prometheus',
            'default': 'docker.io/prom/prometheus:v2.18.1',
            'desc': 'Prometheus container image',
        },
        {
            'name': 'container_image_grafana',
            'default': 'docker.io/ceph/ceph-grafana:6.6.2',
            'desc': 'Prometheus container image',
        },
        {
            'name': 'container_image_alertmanager',
            'default': 'docker.io/prom/alertmanager:v0.20.0',
            'desc': 'Prometheus container image',
        },
        {
            'name': 'container_image_node_exporter',
            'default': 'docker.io/prom/node-exporter:v0.18.1',
            'desc': 'Prometheus container image',
        },
        {
            'name': 'warn_on_stray_hosts',
            'type': 'bool',
            'default': True,
            'desc': 'raise a health warning if daemons are detected on a host '
                    'that is not managed by cephadm',
        },
        {
            'name': 'warn_on_stray_daemons',
            'type': 'bool',
            'default': True,
            'desc': 'raise a health warning if daemons are detected '
                    'that are not managed by cephadm',
        },
        {
            'name': 'warn_on_failed_host_check',
            'type': 'bool',
            'default': True,
            'desc': 'raise a health warning if the host check fails',
        },
        {
            'name': 'log_to_cluster',
            'type': 'bool',
            'default': True,
            'desc': 'log to the "cephadm" cluster log channel"',
        },
        {
            'name': 'allow_ptrace',
            'type': 'bool',
            'default': False,
            'desc': 'allow SYS_PTRACE capability on ceph containers',
            'long_desc': 'The SYS_PTRACE capability is needed to attach to a '
                         'process with gdb or strace.  Enabling this options '
                         'can allow debugging daemons that encounter problems '
                         'at runtime.',
        },
        {
            'name': 'container_init',
            'type': 'bool',
            'default': False,
            'desc': 'Run podman/docker with `--init`',
        },
        {
            'name': 'prometheus_alerts_path',
            'type': 'str',
            'default': '/etc/prometheus/ceph/ceph_default_alerts.yml',
            'desc': 'location of alerts to include in prometheus deployments',
        },
        {
            'name': 'migration_current',
            'type': 'int',
            'default': None,
            'desc': 'internal - do not modify',
            # used to track track spec and other data migrations.
        },
        {
            'name': 'config_dashboard',
            'type': 'bool',
            'default': True,
            'desc': 'manage configs like API endpoints in Dashboard.'
        },
        {
            'name': 'manage_etc_ceph_ceph_conf',
            'type': 'bool',
            'default': False,
            'desc': 'Manage and own /etc/ceph/ceph.conf on the hosts.',
        },
        {
            'name': 'registry_url',
            'type': 'str',
            'default': None,
            'desc': 'Custom repository url'
        },
        {
            'name': 'registry_username',
            'type': 'str',
            'default': None,
            'desc': 'Custom repository username'
        },
        {
            'name': 'registry_password',
            'type': 'str',
            'default': None,
            'desc': 'Custom repository password'
        },
        {
            'name': 'use_repo_digest',
            'type': 'bool',
            'default': False,
            'desc': 'Automatically convert image tags to image digest. Make sure all daemons use the same image',
        }
    ]

    def __init__(self, *args, **kwargs):
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
            self.host_check_interval = 0
            self.mode = ''
            self.container_image_base = ''
            self.container_image_prometheus = ''
            self.container_image_grafana = ''
            self.container_image_alertmanager = ''
            self.container_image_node_exporter = ''
            self.warn_on_stray_hosts = True
            self.warn_on_stray_daemons = True
            self.warn_on_failed_host_check = True
            self.allow_ptrace = False
            self.container_init = False
            self.prometheus_alerts_path = ''
            self.migration_current = None
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

        self.health_checks = {}

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
        self.container_service = CustomContainerService(self)
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
            'container': self.container_service,
        }

        self.template = TemplateMgr(self)

        self.requires_post_actions = set()

    def shutdown(self):
        self.log.debug('shutdown')
        self._worker_pool.close()
        self._worker_pool.join()
        self.run = False
        self.event.set()

    def _get_cephadm_service(self, service_type: str) -> CephadmService:
        assert service_type in ServiceSpec.KNOWN_SERVICE_TYPES
        return self.cephadm_services[service_type]

    def _kick_serve_loop(self):
        self.log.debug('_kick_serve_loop')
        self.event.set()

    # function responsible for logging single host into custom registry
    def _registry_login(self, host, url, username, password):
        self.log.debug(f"Attempting to log host {host} into custom registry @ {url}")
        # want to pass info over stdin rather than through normal list of args
        args_str = ("{\"url\": \"" + url + "\", \"username\": \"" + username + "\", "
                    " \"password\": \"" + password + "\"}")
        out, err, code = self._run_cephadm(
            host, 'mon', 'registry-login',
            ['--registry-json', '-'], stdin=args_str, error_ok=True)
        if code:
            return f"Host {host} failed to login to {url} as {username} with given password"
        return

    def serve(self) -> None:
        """
        The main loop of cephadm.

        A command handler will typically change the declarative state
        of cephadm. This loop will then attempt to apply this new state.
        """
        serve = CephadmServe(self)
        serve.serve()

    def set_container_image(self, entity: str, image):
        self.check_mon_command({
            'prefix': 'config set',
            'name': 'container_image',
            'value': image,
            'who': entity,
        })

    def config_notify(self):
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

    def notify(self, notify_type, notify_id):
        if notify_type == "mon_map":
            # get monmap mtime so we can refresh configs when mons change
            monmap = self.get('mon_map')
            self.last_monmap = datetime.datetime.strptime(
                monmap['modified'], CEPH_DATEFMT)
            if self.last_monmap and self.last_monmap > datetime.datetime.utcnow():
                self.last_monmap = None  # just in case clocks are skewed
            if getattr(self, 'manage_etc_ceph_ceph_conf', False):
                # getattr, due to notify() being called before config_notify()
                self._kick_serve_loop()
        if notify_type == "pg_summary":
            self._trigger_osd_removal()

    def _trigger_osd_removal(self):
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

    def pause(self):
        if not self.paused:
            self.log.info('Paused')
            self.set_store('pause', 'true')
            self.paused = True
            # wake loop so we update the health status
            self._kick_serve_loop()

    def resume(self):
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
            'container'
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

    def _reconfig_ssh(self):
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

    def validate_ssh_config_content(self, ssh_config):
        if ssh_config is None or len(ssh_config.strip()) == 0:
            raise OrchestratorValidationError('ssh_config cannot be empty')
        # StrictHostKeyChecking is [yes|no] ?
        l = re.findall(r'StrictHostKeyChecking\s+.*', ssh_config)
        if not l:
            raise OrchestratorValidationError('ssh_config requires StrictHostKeyChecking')
        for s in l:
            if 'ask' in s.lower():
                raise OrchestratorValidationError(f'ssh_config cannot contain: \'{s}\'')

    def validate_ssh_config_fname(self, ssh_config_fname):
        if not os.path.isfile(ssh_config_fname):
            raise OrchestratorValidationError("ssh_config \"{}\" does not exist".format(
                ssh_config_fname))

    def _reset_con(self, host):
        conn, r = self._cons.get(host, (None, None))
        if conn:
            self.log.debug('_reset_con close %s' % host)
            conn.exit()
            del self._cons[host]

    def _reset_cons(self):
        for host, conn_and_r in self._cons.items():
            self.log.debug('_reset_cons close %s' % host)
            conn, r = conn_and_r
            conn.exit()
        self._cons = {}

    def offline_hosts_remove(self, host):
        if host in self.offline_hosts:
            self.offline_hosts.remove(host)

    @staticmethod
    def can_run():
        if remoto is not None:
            return True, ""
        else:
            return False, "loading remoto library:{}".format(
                remoto_import_error)

    def available(self):
        """
        The cephadm orchestrator is always available.
        """
        ok, err = self.can_run()
        if not ok:
            return ok, err
        if not self.ssh_key or not self.ssh_pub:
            return False, 'SSH keys not set. Use `ceph cephadm set-priv-key` and `ceph cephadm set-pub-key` or `ceph cephadm generate-key`'
        return True, ''

    def process(self, completions):
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
    def _set_ssh_config(self, inbuf=None):
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
    def _clear_ssh_config(self):
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
    def _get_ssh_config(self):
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
    def _generate_key(self):
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
    def _set_priv_key(self, inbuf=None):
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
    def _set_pub_key(self, inbuf=None):
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
    def _clear_key(self):
        self.set_store('ssh_identity_key', None)
        self.set_store('ssh_identity_pub', None)
        self._reconfig_ssh()
        self.log.info('Cleared cluster SSH key')
        return 0, '', ''

    @orchestrator._cli_read_command(
        'cephadm get-pub-key',
        desc='Show SSH public key for connecting to cluster hosts')
    def _get_pub_key(self):
        if self.ssh_pub:
            return 0, self.ssh_pub, ''
        else:
            return -errno.ENOENT, '', 'No cluster SSH key defined'

    @orchestrator._cli_read_command(
        'cephadm get-user',
        desc='Show user for SSHing to cluster hosts')
    def _get_user(self):
        return 0, self.ssh_user, ''

    @orchestrator._cli_read_command(
        'cephadm set-user',
        'name=user,type=CephString',
        'Set user for SSHing to cluster hosts, passwordless sudo will be needed for non-root users')
    def set_ssh_user(self, user):
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
    def registry_login(self, url=None, username=None, password=None, inbuf=None):
        # if password not given in command line, get it through file input
        if not (url and username and password) and (inbuf is None or len(inbuf) == 0):
            return -errno.EINVAL, "", ("Invalid arguments. Please provide arguments <url> <username> <password> "
                                       "or -i <login credentials json file>")
        elif not (url and username and password):
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
    def check_host(self, host, addr=None):
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
        return 0, '%s (%s) ok' % (host, addr), err

    @orchestrator._cli_read_command(
        'cephadm prepare-host',
        'name=host,type=CephString '
        'name=addr,type=CephString,req=false',
        'Prepare a remote host for use with cephadm')
    def _prepare_host(self, host, addr=None):
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
        return 0, '%s (%s) ok' % (host, addr), err

    @orchestrator._cli_write_command(
        prefix='cephadm set-extra-ceph-conf',
        desc="Text that is appended to all daemon's ceph.conf.\n"
             "Mainly a workaround, till `config generate-minimal-conf` generates\n"
             "a complete ceph.conf.\n\n"
             "Warning: this is a dangerous operation.")
    def _set_extra_ceph_conf(self, inbuf=None) -> HandleCommandResult:
        if inbuf:
            # sanity check.
            cp = ConfigParser()
            cp.read_string(inbuf, source='<infile>')

        self.set_store("extra_ceph_conf", json.dumps({
            'conf': inbuf,
            'last_modified': datetime_to_str(datetime.datetime.utcnow())
        }))
        self.log.info('Set extra_ceph_conf')
        self._kick_serve_loop()
        return HandleCommandResult()

    @orchestrator._cli_read_command(
        'cephadm get-extra-ceph-conf',
        desc='Get extra ceph conf that is appended')
    def _get_extra_ceph_conf(self) -> HandleCommandResult:
        return HandleCommandResult(stdout=self.extra_ceph_conf().conf)

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
            self.log.exception('unable to laod extra_ceph_conf')
            return CephadmOrchestrator.ExtraCephConf('', None)
        return CephadmOrchestrator.ExtraCephConf(j['conf'], str_to_datetime(j['last_modified']))

    def extra_ceph_conf_is_newer(self, dt: datetime.datetime) -> bool:
        conf = self.extra_ceph_conf()
        if not conf.last_modified:
            return False
        return conf.last_modified > dt

    def _get_connection(self, host: str):
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

    def _executable_path(self, conn, executable):
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
        with self._remote_connection(host, addr) as tpl:
            conn, connr = tpl
            assert image or entity
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
        Returns all hosts that went through _refresh_host_daemons().

        This mitigates a potential race, where new host was added *after*
        ``_refresh_host_daemons()`` was called, but *before*
        ``_apply_all_specs()`` was called. thus we end up with a hosts
        where daemons might be running, but we have not yet detected them.
        """
        return [
            h for h in self.inventory.all_specs()
            if self.cache.host_had_daemon_refresh(h.hostname)
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
            raise OrchestratorError('New host %s (%s) failed check: %s' % (
                spec.hostname, spec.addr, err))

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
    def update_host_addr(self, host, addr) -> str:
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
    def add_host_label(self, host, label) -> str:
        self.inventory.add_label(host, label)
        self.log.info('Added label %s to host %s' % (label, host))
        return 'Added label %s to host %s' % (label, host)

    @trivial_completion
    def remove_host_label(self, host, label) -> str:
        self.inventory.rm_label(host, label)
        self.log.info('Removed label %s to host %s' % (label, host))
        return 'Removed label %s from host %s' % (label, host)

    @trivial_completion
    def host_ok_to_stop(self, hostname: str):
        if hostname not in self.cache.get_hosts():
            raise OrchestratorError(f'Cannot find host "{hostname}"')

        daemons = self.cache.get_daemons()
        daemon_map = defaultdict(lambda: [])
        for dd in daemons:
            if dd.hostname == hostname:
                daemon_map[dd.daemon_type].append(dd.daemon_id)

        for daemon_type, daemon_ids in daemon_map.items():
            r = self.cephadm_services[daemon_type].ok_to_stop(daemon_ids)
            if r.retval:
                self.log.error(f'It is NOT safe to stop host {hostname}')
                raise orchestrator.OrchestratorError(
                    r.stderr,
                    errno=r.retval)

        msg = f'It is presumed safe to stop host {hostname}'
        self.log.info(msg)
        return msg

    def get_minimal_ceph_conf(self) -> str:
        _, config, _ = self.check_mon_command({
            "prefix": "config generate-minimal-conf",
        })
        extra = self.extra_ceph_conf().conf
        if extra:
            config += '\n\n' + extra.strip() + '\n'
        return config

    def _invalidate_daemons_and_kick_serve(self, filter_host=None):
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
    def service_action(self, action, service_name) -> List[str]:
        args = []
        for host, dm in self.cache.daemons.items():
            for name, d in dm.items():
                if d.matches_service(service_name):
                    args.append((d.daemon_type, d.daemon_id,
                                 d.hostname, action))
        self.log.info('%s service %s' % (action.capitalize(), service_name))
        return self._daemon_actions(args)

    @forall_hosts
    def _daemon_actions(self, daemon_type, daemon_id, host, action) -> str:
        with set_exception_subject('daemon', DaemonDescription(
            daemon_type=daemon_type,
            daemon_id=daemon_id
        ).name()):
            return self._daemon_action(daemon_type, daemon_id, host, action)

    def _daemon_action(self, daemon_type, daemon_id, host, action, image=None) -> str:
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

    def _daemon_action_set_image(self, action: str, image: Optional[str], daemon_type: str, daemon_id: str):
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

    def _schedule_daemon_action(self, daemon_name: str, action: str):
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
    def remove_service(self, service_name) -> str:
        self.log.info('Remove service %s' % service_name)
        self._trigger_preview_refresh(service_name=service_name)
        found = self.spec_store.rm(service_name)
        if found:
            self._kick_serve_loop()
            return 'Removed service %s' % service_name
        else:
            # must be idempotent: still a success.
            return f'Failed to remove service. <{service_name}> was not found.'

    @trivial_completion
    def get_inventory(self, host_filter: Optional[orchestrator.InventoryFilter] = None, refresh=False) -> List[orchestrator.InventoryHost]:
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
    def zap_device(self, host, path) -> str:
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
        def blink(host, dev, path):
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
                          ):
        if not osdspecs:
            return {'n/a': [{'error': True,
                             'message': 'No OSDSpec or matching hosts found.'}]}
        matching_hosts = self.osd_service.resolve_hosts_for_osdspecs(specs=osdspecs)
        if not matching_hosts:
            return {'n/a': [{'error': True,
                             'message': 'No OSDSpec or matching hosts found.'}]}
        # Is any host still loading previews
        pending_hosts = {h for h in self.cache.loading_osdspec_preview if h in matching_hosts}
        if pending_hosts:
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

    def _calc_daemon_deps(self, daemon_type, daemon_id):
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
                       reconfig=False,
                       osd_uuid_map: Optional[Dict[str, Any]] = None,
                       ) -> str:

        with set_exception_subject('service', orchestrator.DaemonDescription(
                daemon_type=daemon_spec.daemon_type,
                daemon_id=daemon_spec.daemon_id,
                hostname=daemon_spec.host,
        ).service_id(), overwrite=True):

            image = ''
            start_time = datetime.datetime.utcnow()
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

            cephadm_config, deps = self.cephadm_services[daemon_spec.daemon_type].generate_config(
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

    @forall_hosts
    def _remove_daemons(self, name, host) -> str:
        return self._remove_daemon(name, host)

    def _remove_daemon(self, name, host) -> str:
        """
        Remove a daemon
        """
        (daemon_type, daemon_id) = name.split('.', 1)
        daemon = orchestrator.DaemonDescription(
            daemon_type=daemon_type,
            daemon_id=daemon_id,
            hostname=host)

        with set_exception_subject('service', daemon.service_id(), overwrite=True):

            self.cephadm_services[daemon_type].pre_remove(daemon)

            args = ['--name', name, '--force']
            self.log.info('Removing daemon %s from %s' % (name, host))
            out, err, code = self._run_cephadm(
                host, name, 'rm-daemon', args)
            if not code:
                # remove item from cache
                self.cache.rm_daemon(host, name)
            self.cache.invalidate_host_daemons(host)

            self.cephadm_services[daemon_type].post_remove(daemon)

            return "Removed {} from host '{}'".format(name, host)

    def _check_pool_exists(self, pool, service_name):
        logger.info(f'Checking pool "{pool}" exists for service {service_name}')
        if not self.rados.pool_exists(pool):
            raise OrchestratorError(f'Cannot find pool "{pool}" for '
                                    f'service {service_name}')

    def _add_daemon(self, daemon_type, spec,
                    create_func: Callable[..., CephadmDaemonSpec], config_func=None) -> List[str]:
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

    def _create_daemons(self, daemon_type, spec, daemons,
                        hosts, count,
                        create_func: Callable[..., CephadmDaemonSpec], config_func=None) -> List[str]:
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

            daemon_spec = self.cephadm_services[daemon_type].make_daemon_spec(
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
        def create_func_map(*args):
            daemon_spec = create_func(*args)
            return self._create_daemon(daemon_spec)

        return create_func_map(args)

    @trivial_completion
    def apply_mon(self, spec) -> str:
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

    def _plan(self, spec: ServiceSpec):
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
                'iscsi': PlacementSpec(count=1),
                'rbd-mirror': PlacementSpec(count=2),
                'nfs': PlacementSpec(count=1),
                'grafana': PlacementSpec(count=1),
                'alertmanager': PlacementSpec(count=1),
                'prometheus': PlacementSpec(count=1),
                'node-exporter': PlacementSpec(host_pattern='*'),
                'crash': PlacementSpec(host_pattern='*'),
                'container': PlacementSpec(count=1),
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
    def apply_mgr(self, spec) -> str:
        return self._apply(spec)

    @trivial_completion
    def add_mds(self, spec: ServiceSpec) -> List[str]:
        return self._add_daemon('mds', spec, self.mds_service.prepare_create, self.mds_service.config)

    @trivial_completion
    def apply_mds(self, spec: ServiceSpec) -> str:
        return self._apply(spec)

    @trivial_completion
    def add_rgw(self, spec) -> List[str]:
        return self._add_daemon('rgw', spec, self.rgw_service.prepare_create, self.rgw_service.config)

    @trivial_completion
    def apply_rgw(self, spec) -> str:
        return self._apply(spec)

    @trivial_completion
    def add_iscsi(self, spec):
        # type: (ServiceSpec) -> List[str]
        return self._add_daemon('iscsi', spec, self.iscsi_service.prepare_create, self.iscsi_service.config)

    @trivial_completion
    def apply_iscsi(self, spec) -> str:
        return self._apply(spec)

    @trivial_completion
    def add_rbd_mirror(self, spec) -> List[str]:
        return self._add_daemon('rbd-mirror', spec, self.rbd_mirror_service.prepare_create)

    @trivial_completion
    def apply_rbd_mirror(self, spec) -> str:
        return self._apply(spec)

    @trivial_completion
    def add_nfs(self, spec) -> List[str]:
        return self._add_daemon('nfs', spec, self.nfs_service.prepare_create, self.nfs_service.config)

    @trivial_completion
    def apply_nfs(self, spec) -> str:
        return self._apply(spec)

    def _get_dashboard_url(self):
        # type: () -> str
        return self.get('mgr_map').get('services', {}).get('dashboard', '')

    @trivial_completion
    def add_prometheus(self, spec) -> List[str]:
        return self._add_daemon('prometheus', spec, self.prometheus_service.prepare_create)

    @trivial_completion
    def apply_prometheus(self, spec) -> str:
        return self._apply(spec)

    @trivial_completion
    def add_node_exporter(self, spec):
        # type: (ServiceSpec) -> List[str]
        return self._add_daemon('node-exporter', spec,
                                self.node_exporter_service.prepare_create)

    @trivial_completion
    def apply_node_exporter(self, spec) -> str:
        return self._apply(spec)

    @trivial_completion
    def add_crash(self, spec):
        # type: (ServiceSpec) -> List[str]
        return self._add_daemon('crash', spec,
                                self.crash_service.prepare_create)

    @trivial_completion
    def apply_crash(self, spec) -> str:
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

    def _get_container_image_info(self, image_name) -> ContainerInspectInfo:
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
            msg = 'Failed to pull %s on %s: %s' % (image_name, host, '\n'.join(out))
            self.log.exception(msg)
            raise OrchestratorError(msg)

    @trivial_completion
    def upgrade_check(self, image, version) -> str:
        if version:
            target_name = self.container_image_base + ':v' + version
        elif image:
            target_name = image
        else:
            raise OrchestratorError('must specify either image or version')

        image_info = self._get_container_image_info(target_name)
        self.log.debug(f'image info {image} -> {image_info}')
        r = {
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
    def upgrade_start(self, image, version) -> str:
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
                                                process_started_at=datetime.datetime.utcnow(),
                                                remove_util=self.rm_util))
            except NotFoundError:
                return f"Unable to find OSDs: {osd_ids}"

        # trigger the serve loop to initiate the removal
        self._kick_serve_loop()
        return "Scheduled OSD(s) for removal"

    @trivial_completion
    def stop_remove_osds(self, osd_ids: List[str]):
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
    def remove_osds_status(self):
        """
        The CLI call to retrieve an osd removal report
        """
        return self.to_remove_osds.all_osds()
