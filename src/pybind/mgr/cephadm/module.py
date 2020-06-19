import json
import errno
import logging
from collections import defaultdict
from functools import wraps
from tempfile import TemporaryDirectory
from threading import Event

import string
from typing import List, Dict, Optional, Callable, Tuple, TypeVar, \
    Any, Set, TYPE_CHECKING, cast

import datetime
import six
import os
import random
import tempfile
import multiprocessing.pool
import shutil
import subprocess

from ceph.deployment import inventory
from ceph.deployment.drive_group import DriveGroupSpec
from ceph.deployment.service_spec import \
    NFSServiceSpec, ServiceSpec, PlacementSpec, assert_valid_host

from mgr_module import MgrModule, HandleCommandResult
import orchestrator
from orchestrator import OrchestratorError, OrchestratorValidationError, HostSpec, \
    CLICommandMeta
from orchestrator._interface import GenericSpec

from . import remotes
from . import utils
from .migrations import Migrations
from .services.cephadmservice import MonService, MgrService, MdsService, RgwService, \
    RbdMirrorService, CrashService, CephadmService
from .services.iscsi import IscsiService
from .services.nfs import NFSService
from .services.osd import RemoveUtil, OSDRemoval, OSDService
from .services.monitoring import GrafanaService, AlertmanagerService, PrometheusService, \
    NodeExporterService
from .schedule import HostAssignment, HostPlacementSpec
from .inventory import Inventory, SpecStore, HostCache
from .upgrade import CEPH_UPGRADE_ORDER, CephadmUpgrade
from .template import TemplateMgr

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

DATEFMT = '%Y-%m-%dT%H:%M:%S.%f'
CEPH_DATEFMT = '%Y-%m-%dT%H:%M:%S.%fZ'

CEPH_TYPES = set(CEPH_UPGRADE_ORDER)


def forall_hosts(f: Callable[..., T]) -> Callable[..., List[T]]:
    @wraps(f)
    def forall_hosts_wrapper(*args) -> List[T]:

        # Some weired logic to make calling functions with multiple arguments work.
        if len(args) == 1:
            vals = args[0]
            self = None
        elif len(args) == 2:
            self, vals = args
        else:
            assert 'either f([...]) or self.f([...])'

        def do_work(arg):
            if not isinstance(arg, tuple):
                arg = (arg, )
            try:
                if self:
                    return f(self, *arg)
                return f(*arg)
            except Exception as e:
                logger.exception(f'executing {f.__name__}({args}) failed.')
                raise

        assert CephadmOrchestrator.instance is not None
        return CephadmOrchestrator.instance._worker_pool.map(do_work, vals)


    return forall_hosts_wrapper


class CephadmCompletion(orchestrator.Completion):
    def evaluate(self):
        self.finalize(None)

def trivial_completion(f: Callable) -> Callable[..., CephadmCompletion]:
    """
    Decorator to make CephadmCompletion methods return
    a completion object that executes themselves.
    """

    @wraps(f)
    def wrapper(*args, **kwargs):
        return CephadmCompletion(on_complete=lambda _: f(*args, **kwargs))

    return wrapper


@six.add_metaclass(CLICommandMeta)
class CephadmOrchestrator(orchestrator.Orchestrator, MgrModule):

    _STORE_HOST_PREFIX = "host"

    instance = None
    NATIVE_OPTIONS = []  # type: List[Any]
    MODULE_OPTIONS = [
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
            'default': 'prom/prometheus:v2.18.1',
            'desc': 'Prometheus container image',
        },
        {
            'name': 'container_image_grafana',
            'default': 'ceph/ceph-grafana:latest',
            'desc': 'Prometheus container image',
        },
        {
            'name': 'container_image_alertmanager',
            'default': 'prom/alertmanager:v0.20.0',
            'desc': 'Prometheus container image',
        },
        {
            'name': 'container_image_node_exporter',
            'default': 'prom/node-exporter:v0.18.1',
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
        }
    ]

    def __init__(self, *args, **kwargs):
        super(CephadmOrchestrator, self).__init__(*args, **kwargs)
        self._cluster_fsid = self.get('mon_map')['fsid']

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
            self.prometheus_alerts_path = ''
            self.migration_current = None
            self.config_dashboard = True

        self._cons = {}  # type: Dict[str, Tuple[remoto.backends.BaseConnection,remoto.backends.LegacyModuleExecute]]

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
        }

        self.template = TemplateMgr()

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

    def _check_safe_to_destroy_mon(self, mon_id):
        # type: (str) -> None
        ret, out, err = self.check_mon_command({
            'prefix': 'quorum_status',
        })
        try:
            j = json.loads(out)
        except Exception as e:
            raise OrchestratorError('failed to parse quorum status')

        mons = [m['name'] for m in j['monmap']['mons']]
        if mon_id not in mons:
            self.log.info('Safe to remove mon.%s: not in monmap (%s)' % (
                mon_id, mons))
            return
        new_mons = [m for m in mons if m != mon_id]
        new_quorum = [m for m in j['quorum_names'] if m != mon_id]
        if len(new_quorum) > len(new_mons) / 2:
            self.log.info('Safe to remove mon.%s: new quorum should be %s (from %s)' % (mon_id, new_quorum, new_mons))
            return
        raise OrchestratorError('Removing %s would break mon quorum (new quorum %s, new mons %s)' % (mon_id, new_quorum, new_mons))

    def _check_host(self, host):
        if host not in self.inventory:
            return
        self.log.debug(' checking %s' % host)
        try:
            out, err, code = self._run_cephadm(
                host, 'client', 'check-host', [],
                error_ok=True, no_fsid=True)
            self.cache.update_last_host_check(host)
            self.cache.save_host(host)
            if code:
                self.log.debug(' host %s failed check' % host)
                if self.warn_on_failed_host_check:
                    return 'host %s failed check: %s' % (host, err)
            else:
                self.log.debug(' host %s ok' % host)
        except Exception as e:
            self.log.debug(' host %s failed check' % host)
            return 'host %s failed check: %s' % (host, e)

    def _check_for_strays(self):
        self.log.debug('_check_for_strays')
        for k in ['CEPHADM_STRAY_HOST',
                  'CEPHADM_STRAY_DAEMON']:
            if k in self.health_checks:
                del self.health_checks[k]
        if self.warn_on_stray_hosts or self.warn_on_stray_daemons:
            ls = self.list_servers()
            managed = self.cache.get_daemon_names()
            host_detail = []     # type: List[str]
            host_num_daemons = 0
            daemon_detail = []  # type: List[str]
            for item in ls:
                host = item.get('hostname')
                daemons = item.get('services')  # misnomer!
                missing_names = []
                for s in daemons:
                    name = '%s.%s' % (s.get('type'), s.get('id'))
                    if host not in self.inventory:
                        missing_names.append(name)
                        host_num_daemons += 1
                    if name not in managed:
                        daemon_detail.append(
                            'stray daemon %s on host %s not managed by cephadm' % (name, host))
                if missing_names:
                    host_detail.append(
                        'stray host %s has %d stray daemons: %s' % (
                            host, len(missing_names), missing_names))
            if self.warn_on_stray_hosts and host_detail:
                self.health_checks['CEPHADM_STRAY_HOST'] = {
                    'severity': 'warning',
                    'summary': '%d stray host(s) with %s daemon(s) '
                    'not managed by cephadm' % (
                        len(host_detail), host_num_daemons),
                    'count': len(host_detail),
                    'detail': host_detail,
                }
            if self.warn_on_stray_daemons and daemon_detail:
                self.health_checks['CEPHADM_STRAY_DAEMON'] = {
                    'severity': 'warning',
                    'summary': '%d stray daemons(s) not managed by cephadm' % (
                        len(daemon_detail)),
                    'count': len(daemon_detail),
                    'detail': daemon_detail,
                }
        self.set_health_checks(self.health_checks)

    def _serve_sleep(self):
        sleep_interval = 600
        self.log.debug('Sleeping for %d seconds', sleep_interval)
        ret = self.event.wait(sleep_interval)
        self.event.clear()

    def serve(self):
        # type: () -> None
        self.log.debug("serve starting")
        while self.run:

            # refresh daemons
            self.log.debug('refreshing hosts and daemons')
            self._refresh_hosts_and_daemons()

            self._check_for_strays()

            if self.paused:
                self.health_checks['CEPHADM_PAUSED'] = {
                    'severity': 'warning',
                    'summary': 'cephadm background work is paused',
                    'count': 1,
                    'detail': ["'ceph orch resume' to resume"],
                }
                self.set_health_checks(self.health_checks)
            else:
                if 'CEPHADM_PAUSED' in self.health_checks:
                    del self.health_checks['CEPHADM_PAUSED']
                    self.set_health_checks(self.health_checks)

                self.rm_util._remove_osds_bg()

                self.migration.migrate()
                if self.migration.is_migration_ongoing():
                    continue

                if self._apply_all_services():
                    continue  # did something, refresh

                self._check_daemons()

                if self.upgrade.continue_upgrade():
                    continue

            self._serve_sleep()
        self.log.debug("serve exit")

    def config_notify(self):
        """
        This method is called whenever one of our config options is changed.
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
        pass

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
        ]
        if forcename:
            if len([d for d in existing if d.daemon_id == forcename]):
                raise orchestrator.OrchestratorValidationError('name %s already in use', forcename)
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
                    raise orchestrator.OrchestratorValidationError('name %s already in use', name)
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
            self.ssh_user = 'root'
        elif self.mode == 'cephadm-package':
            self.ssh_user = 'cephadm'

        self._reset_cons()

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
            self.log.debug("process: completions={0}".format(orchestrator.pretty_print(completions)))

            for p in completions:
                p.evaluate()

    @orchestrator._cli_write_command(
        prefix='cephadm set-ssh-config',
        desc='Set the ssh_config file (use -i <ssh_config>)')
    def _set_ssh_config(self, inbuf=None):
        """
        Set an ssh_config file provided from stdin

        TODO:
          - validation
        """
        if inbuf is None or len(inbuf) == 0:
            return -errno.EINVAL, "", "empty ssh config provided"
        self.set_store("ssh_config", inbuf)
        self.log.info('Set ssh_config')
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
        'cephadm check-host',
        'name=host,type=CephString '
        'name=addr,type=CephString,req=false',
        'Check whether we can access and manage a remote host')
    def check_host(self, host, addr=None):
        out, err, code = self._run_cephadm(host, 'client', 'check-host',
                                           ['--expect-hostname', host],
                                           addr=addr,
                                           error_ok=True, no_fsid=True)
        if code:
            return 1, '', ('check-host failed:\n' + '\n'.join(err))
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
        out, err, code = self._run_cephadm(host, 'client', 'prepare-host',
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

    def _get_connection(self, host):
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
        child_logger=self.log.getChild(n)
        child_logger.setLevel('WARNING')
        conn = remoto.Connection(
            n,
            logger=child_logger,
            ssh_options=self._ssh_options)

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

    def _run_cephadm(self,
                     host: str,
                     entity: Optional[str],
                     command: str,
                     args: List[str],
                     addr: Optional[str] = None,
                     stdin: Optional[str] = None,
                     no_fsid=False,
                     error_ok=False,
                     image: Optional[str] = None,
                     env_vars: Optional[List[str]] = None,
                     ) -> Tuple[List[str], List[str], int]:
        """
        Run cephadm on the remote host with the given command + args

        :env_vars: in format -> [KEY=VALUE, ..]
        """
        if not addr and host in self.inventory:
            addr = self.inventory.get_addr(host)

        self.offline_hosts_remove(host)

        try:
            try:
                conn, connr = self._get_connection(addr)
            except OSError as e:
                if error_ok:
                    self.log.exception('failed to establish ssh connection')
                    return [], [str("Can't communicate with remote host, possibly because python3 is not installed there")], 1
                raise execnet.gateway_bootstrap.HostNotFound(str(e)) from e

            assert image or entity
            if not image:
                daemon_type = entity.split('.', 1)[0]  # type: ignore
                if daemon_type in CEPH_TYPES or \
                        daemon_type == 'nfs' or \
                        daemon_type == 'iscsi':
                    # get container image
                    ret, image, err = self.check_mon_command({
                        'prefix': 'config get',
                        'who': utils.name_to_config_section(entity),
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

            self.log.debug('%s container image %s' % (entity, image))

            final_args = []

            if env_vars:
                for env_var_pair in env_vars:
                    final_args.extend(['--env', env_var_pair])

            if image:
                final_args.extend(['--image', image])
            final_args.append(command)

            if not no_fsid:
                final_args += ['--fsid', self._cluster_fsid]
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
                raise RuntimeError(
                    'cephadm exited with an error code: %d, stderr:%s' % (
                        code, '\n'.join(err)))
            return out, err, code

        except execnet.gateway_bootstrap.HostNotFound as e:
            # this is a misleading exception as it seems to be thrown for
            # any sort of connection failure, even those having nothing to
            # do with "host not found" (e.g., ssh key permission denied).
            self.offline_hosts.add(host)
            user = 'root' if self.mode == 'root' else 'cephadm'
            msg = f'''Failed to connect to {host} ({addr}).
Check that the host is reachable and accepts connections using the cephadm SSH key

you may want to run:
> ceph cephadm get-ssh-config > ssh_config
> ceph config-key get mgr/cephadm/ssh_identity_key > key
> ssh -F ssh_config -i key {user}@{host}'''
            raise OrchestratorError(msg) from e
        except Exception as ex:
            self.log.exception(ex)
            raise

    def _get_hosts(self, label: Optional[str] = '', as_hostspec: bool = False) -> List:
        return list(self.inventory.filter_by_label(label=label, as_hostspec=as_hostspec))

    def _add_host(self, spec):
        # type: (HostSpec) -> str
        """
        Add a host to be managed by the orchestrator.

        :param host: host name
        """
        assert_valid_host(spec.hostname)
        out, err, code = self._run_cephadm(spec.hostname, 'client', 'check-host',
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
    def update_host_addr(self, host, addr):
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
    def add_host_label(self, host, label):
        self.inventory.add_label(host, label)
        self.log.info('Added label %s to host %s' % (label, host))
        return 'Added label %s to host %s' % (label, host)

    @trivial_completion
    def remove_host_label(self, host, label):
        self.inventory.rm_label(host, label)
        self.log.info('Removed label %s to host %s' % (label, host))
        return 'Removed label %s from host %s' % (label, host)

    def update_osdspec_previews(self, search_host: str = ''):
        # Set global 'pending' flag for host
        self.cache.loading_osdspec_preview.add(search_host)
        previews = []
        # query OSDSpecs for host <search host> and generate/get the preview
        # There can be multiple previews for one host due to multiple OSDSpecs.
        previews.extend(self.osd_service.get_previews(search_host))
        self.log.debug(f"Loading OSDSpec previews to HostCache")
        self.cache.osdspec_previews[search_host] = previews
        # Unset global 'pending' flag for host
        self.cache.loading_osdspec_preview.remove(search_host)

    def _refresh_host_osdspec_previews(self, host) -> bool:
        self.update_osdspec_previews(host)
        self.cache.save_host(host)
        self.log.debug(f'Refreshed OSDSpec previews for host <{host}>')
        return True

    def _refresh_hosts_and_daemons(self):
        bad_hosts = []
        failures = []
        for host in self.cache.get_hosts():
            if self.cache.host_needs_check(host):
                r = self._check_host(host)
                if r is not None:
                    bad_hosts.append(r)
            if self.cache.host_needs_daemon_refresh(host):
                self.log.debug('refreshing %s daemons' % host)
                r = self._refresh_host_daemons(host)
                if r:
                    failures.append(r)
            if self.cache.host_needs_device_refresh(host):
                self.log.debug('refreshing %s devices' % host)
                r = self._refresh_host_devices(host)
                if r:
                    failures.append(r)

            if self.cache.host_needs_osdspec_preview_refresh(host):
                self.log.debug(f"refreshing OSDSpec previews for {host}")
                r = self._refresh_host_osdspec_previews(host)
                if r:
                    failures.append(r)


        health_changed = False
        if 'CEPHADM_HOST_CHECK_FAILED' in self.health_checks:
            del self.health_checks['CEPHADM_HOST_CHECK_FAILED']
            health_changed = True
        if bad_hosts:
            self.health_checks['CEPHADM_HOST_CHECK_FAILED'] = {
                'severity': 'warning',
                'summary': '%d hosts fail cephadm check' % len(bad_hosts),
                'count': len(bad_hosts),
                'detail': bad_hosts,
            }
            health_changed = True
        if failures:
            self.health_checks['CEPHADM_REFRESH_FAILED'] = {
                'severity': 'warning',
                'summary': 'failed to probe daemons or devices',
                'count': len(failures),
                'detail': failures,
            }
            health_changed = True
        elif 'CEPHADM_REFRESH_FAILED' in self.health_checks:
            del self.health_checks['CEPHADM_REFRESH_FAILED']
            health_changed = True
        if health_changed:
            self.set_health_checks(self.health_checks)

    def _refresh_host_daemons(self, host):
        try:
            out, err, code = self._run_cephadm(
                host, 'mon', 'ls', [], no_fsid=True)
            if code:
                return 'host %s cephadm ls returned %d: %s' % (
                    host, code, err)
        except Exception as e:
            return 'host %s scrape failed: %s' % (host, e)
        ls = json.loads(''.join(out))
        dm = {}
        for d in ls:
            if not d['style'].startswith('cephadm'):
                continue
            if d['fsid'] != self._cluster_fsid:
                continue
            if '.' not in d['name']:
                continue
            sd = orchestrator.DaemonDescription()
            sd.last_refresh = datetime.datetime.utcnow()
            for k in ['created', 'started', 'last_configured', 'last_deployed']:
                v = d.get(k, None)
                if v:
                    setattr(sd, k, datetime.datetime.strptime(d[k], DATEFMT))
            sd.daemon_type = d['name'].split('.')[0]
            sd.daemon_id = '.'.join(d['name'].split('.')[1:])
            sd.hostname = host
            sd.container_id = d.get('container_id')
            if sd.container_id:
                # shorten the hash
                sd.container_id = sd.container_id[0:12]
            sd.container_image_name = d.get('container_image_name')
            sd.container_image_id = d.get('container_image_id')
            sd.version = d.get('version')
            if sd.daemon_type == 'osd':
                sd.osdspec_affinity = self.osd_service.get_osdspec_affinity(sd.daemon_id)
            if 'state' in d:
                sd.status_desc = d['state']
                sd.status = {
                    'running': 1,
                    'stopped': 0,
                    'error': -1,
                    'unknown': -1,
                }[d['state']]
            else:
                sd.status_desc = 'unknown'
                sd.status = None
            dm[sd.name()] = sd
        self.log.debug('Refreshed host %s daemons (%d)' % (host, len(dm)))
        self.cache.update_host_daemons(host, dm)
        self.cache.save_host(host)
        return None

    def _refresh_host_devices(self, host):
        try:
            out, err, code = self._run_cephadm(
                host, 'osd',
                'ceph-volume',
                ['--', 'inventory', '--format=json'])
            if code:
                return 'host %s ceph-volume inventory returned %d: %s' % (
                    host, code, err)
        except Exception as e:
            return 'host %s ceph-volume inventory failed: %s' % (host, e)
        devices = json.loads(''.join(out))
        try:
            out, err, code = self._run_cephadm(
                host, 'mon',
                'list-networks',
                [],
                no_fsid=True)
            if code:
                return 'host %s list-networks returned %d: %s' % (
                    host, code, err)
        except Exception as e:
            return 'host %s list-networks failed: %s' % (host, e)
        networks = json.loads(''.join(out))
        self.log.debug('Refreshed host %s devices (%d) networks (%s)' % (
            host, len(devices), len(networks)))
        devices = inventory.Devices.from_json(devices)
        self.cache.update_host_devices_networks(host, devices.devices, networks)
        self.update_osdspec_previews(host)
        self.cache.save_host(host)
        return None

    @trivial_completion
    def describe_service(self, service_type=None, service_name=None,
                         refresh=False):
        if refresh:
            # ugly sync path, FIXME someday perhaps?
            for host in self.inventory.keys():
                self._refresh_host_daemons(host)
        # <service_map>
        sm = {}  # type: Dict[str, orchestrator.ServiceDescription]
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
                    )
                if n in self.spec_store.specs:
                    if dd.daemon_type == 'osd':
                        """
                        The osd count can't be determined by the Placement spec.
                        It's rather pointless to show a actual/expected representation 
                        here. So we're setting running = size for now.
                        """
                        osd_count += 1
                        sm[n].size = osd_count
                    else:
                        sm[n].size = spec.placement.get_host_selection_size(self.inventory.all_specs())

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
            )
            if service_type == 'nfs':
                spec = cast(NFSServiceSpec, spec)
                sm[n].rados_config_location = spec.rados_config_location()
        return list(sm.values())

    @trivial_completion
    def list_daemons(self, service_name=None, daemon_type=None, daemon_id=None,
                     host=None, refresh=False):
        if refresh:
            # ugly sync path, FIXME someday perhaps?
            if host:
                self._refresh_host_daemons(host)
            else:
                for hostname in self.inventory.keys():
                    self._refresh_host_daemons(hostname)
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
    def service_action(self, action, service_name):
        args = []
        for host, dm in self.cache.daemons.items():
            for name, d in dm.items():
                if d.matches_service(service_name):
                    args.append((d.daemon_type, d.daemon_id,
                                 d.hostname, action))
        self.log.info('%s service %s' % (action.capitalize(), service_name))
        return self._daemon_actions(args)

    @forall_hosts
    def _daemon_actions(self, daemon_type, daemon_id, host, action):
        return self._daemon_action(daemon_type, daemon_id, host, action)

    def _daemon_action(self, daemon_type, daemon_id, host, action):
        if action == 'redeploy':
            # stop, recreate the container+unit, then restart
            return self._create_daemon(daemon_type, daemon_id, host)
        elif action == 'reconfig':
            return self._create_daemon(daemon_type, daemon_id, host,
                                       reconfig=True)

        actions = {
            'start': ['reset-failed', 'start'],
            'stop': ['stop'],
            'restart': ['reset-failed', 'restart'],
        }
        name = '%s.%s' % (daemon_type, daemon_id)
        for a in actions[action]:
            out, err, code = self._run_cephadm(
                host, name, 'unit',
                ['--name', name, a],
                error_ok=True)
        self.cache.invalidate_host_daemons(host)
        return "{} {} from host '{}'".format(action, name, host)

    @trivial_completion
    def daemon_action(self, action, daemon_type, daemon_id):
        args = []
        for host, dm in self.cache.daemons.items():
            for name, d in dm.items():
                if d.daemon_type == daemon_type and d.daemon_id == daemon_id:
                    args.append((d.daemon_type, d.daemon_id,
                                 d.hostname, action))
        if not args:
            raise orchestrator.OrchestratorError(
                'Unable to find %s.%s daemon(s)' % (
                    daemon_type, daemon_id))
        self.log.info('%s daemons %s' % (
            action.capitalize(),
            ','.join(['%s.%s' % (a[0], a[1]) for a in args])))
        return self._daemon_actions(args)

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
    def remove_service(self, service_name):
        self.log.info('Remove service %s' % service_name)
        self._trigger_preview_refresh(service_name=service_name)
        found = self.spec_store.rm(service_name)
        if found:
            self._kick_serve_loop()
            return ['Removed service %s' % service_name]
        else:
            # must be idempotent: still a success.
            return [f'Failed to remove service. <{service_name}> was not found.']

    @trivial_completion
    def get_inventory(self, host_filter=None, refresh=False):
        """
        Return the storage inventory of hosts matching the given filter.

        :param host_filter: host filter

        TODO:
          - add filtering by label
        """
        if refresh:
            # ugly sync path, FIXME someday perhaps?
            if host_filter:
                for host in host_filter.hosts:
                    self._refresh_host_devices(host)
            else:
                for host in self.inventory.keys():
                    self._refresh_host_devices(host)

        result = []
        for host, dls in self.cache.devices.items():
            if host_filter and host not in host_filter.hosts:
                continue
            result.append(orchestrator.InventoryHost(host,
                                                     inventory.Devices(dls)))
        return result

    @trivial_completion
    def zap_device(self, host, path):
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
    def blink_device_light(self, ident_fault, on, locs):
        @forall_hosts
        def blink(host, dev, path):
            cmd = [
                'lsmcli',
                'local-disk-%s-led-%s' % (
                    ident_fault,
                    'on' if on else 'off'),
                '--path', path or dev,
            ]
            out, err, code = self._run_cephadm(
                host, 'osd', 'shell', ['--'] + cmd,
                error_ok=True)
            if code:
                raise RuntimeError(
                    'Unable to affect %s light for %s:%s. Command: %s' % (
                        ident_fault, host, dev, ' '.join(cmd)))
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

    def resolve_hosts_for_osdspecs(self,
                                   specs: Optional[List[DriveGroupSpec]] = None,
                                   service_name: Optional[str] = None
                                   ) -> List[str]:
        osdspecs = []
        if service_name:
            self.log.debug(f"Looking for OSDSpec with service_name: {service_name}")
            osdspecs = self.spec_store.find(service_name=service_name)
            self.log.debug(f"Found OSDSpecs: {osdspecs}")
        if specs:
            osdspecs = [cast(DriveGroupSpec, spec) for spec in specs]
        if not service_name and not specs:
            # if neither parameters are fulfilled, search for all available osdspecs
            osdspecs = self.spec_store.find(service_name='osd')
            self.log.debug(f"Found OSDSpecs: {osdspecs}")
        if not osdspecs:
            self.log.debug("No OSDSpecs found")
            return []
        return sum([spec.placement.filter_matching_hosts(self._get_hosts) for spec in osdspecs], [])

    def resolve_osdspecs_for_host(self, host):
        matching_specs = []
        self.log.debug(f"Finding OSDSpecs for host: <{host}>")
        for spec in self.spec_store.find('osd'):
            if host in spec.placement.filter_matching_hosts(self._get_hosts):
                self.log.debug(f"Found OSDSpecs for host: <{host}> -> <{spec}>")
                matching_specs.append(spec)
        return matching_specs

    def _trigger_preview_refresh(self,
                                 specs: Optional[List[DriveGroupSpec]] = None,
                                 service_name: Optional[str] = None):
        refresh_hosts = self.resolve_hosts_for_osdspecs(specs=specs, service_name=service_name)
        for host in refresh_hosts:
            self.log.info(f"Marking host: {host} for OSDSpec preview refresh.")
            self.cache.osdspec_previews_refresh_queue.append(host)

    @trivial_completion
    def apply_drivegroups(self, specs: List[DriveGroupSpec]):
        self._trigger_preview_refresh(specs=specs)
        return [self._apply(spec) for spec in specs]

    @trivial_completion
    def create_osds(self, drive_group: DriveGroupSpec):
        return self.osd_service.create(drive_group)

    @trivial_completion
    def preview_osdspecs(self,
                         osdspec_name: Optional[str] = None,
                         osdspecs: Optional[List[DriveGroupSpec]] = None
                         ):
        matching_hosts = self.resolve_hosts_for_osdspecs(specs=osdspecs, service_name=osdspec_name)
        if not matching_hosts:
            return {'n/a': [{'error': True,
                             'message': 'No OSDSpec or matching hosts found.'}]}
        # Is any host still loading previews
        pending_hosts = {h for h in self.cache.loading_osdspec_preview if h in matching_hosts}
        if pending_hosts:
            # Report 'pending' when any of the matching hosts is still loading previews (flag is True)
            return {'n/a': [{'error': True,
                             'message': 'Preview data is being generated.. '
                                        'Please try again in a bit.'}]}
        # drop all keys that are not in search_hosts and return preview struct
        return {k: v for (k, v) in self.cache.osdspec_previews.items() if k in matching_hosts}

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

    def _get_config_and_keyring(self, daemon_type, daemon_id,
                                keyring=None,
                                extra_ceph_config=None):
        # type: (str, str, Optional[str], Optional[str]) -> Dict[str, Any]
        # keyring
        if not keyring:
            ename = utils.name_to_auth_entity(daemon_type + '.' + daemon_id)
            ret, keyring, err = self.check_mon_command({
                'prefix': 'auth get',
                'entity': ename,
            })

        # generate config
        ret, config, err = self.check_mon_command({
            "prefix": "config generate-minimal-conf",
        })
        if extra_ceph_config:
            config += extra_ceph_config

        return {
            'config': config,
            'keyring': keyring,
        }

    def _create_daemon(self,
                       daemon_type: str,
                       daemon_id: str,
                       host: str,
                       keyring: Optional[str] = None,
                       extra_args: Optional[List[str]] = None,
                       extra_config: Optional[Dict[str, Any]] = None,
                       reconfig=False,
                       osd_uuid_map: Optional[Dict[str, Any]] = None,
                       redeploy=False,
                       ) -> str:

        if not extra_args:
            extra_args = []
        if not extra_config:
            extra_config = {}
        name = '%s.%s' % (daemon_type, daemon_id)

        start_time = datetime.datetime.utcnow()
        deps = []  # type: List[str]
        cephadm_config = {}  # type: Dict[str, Any]
        if daemon_type == 'prometheus':
            cephadm_config, deps = self.prometheus_service.generate_config()
            extra_args.extend(['--config-json', '-'])
        elif daemon_type == 'grafana':
            cephadm_config, deps = self.grafana_service.generate_config()
            extra_args.extend(['--config-json', '-'])
        elif daemon_type == 'nfs':
            cephadm_config, deps = \
                    self.nfs_service._generate_nfs_config(daemon_type, daemon_id, host)
            extra_args.extend(['--config-json', '-'])
        elif daemon_type == 'alertmanager':
            cephadm_config, deps = self.alertmanager_service.generate_config()
            extra_args.extend(['--config-json', '-'])
        elif daemon_type == 'node-exporter':
            cephadm_config, deps = self.node_exporter_service.generate_config()
            extra_args.extend(['--config-json', '-'])
        else:
            # Ceph.daemons (mon, mgr, mds, osd, etc)
            cephadm_config = self._get_config_and_keyring(
                    daemon_type, daemon_id,
                    keyring=keyring,
                    extra_ceph_config=extra_config.pop('config', ''))
            if extra_config:
                cephadm_config.update({'files': extra_config})
            extra_args.extend(['--config-json', '-'])

            # osd deployments needs an --osd-uuid arg
            if daemon_type == 'osd':
                if not osd_uuid_map:
                    osd_uuid_map = self.get_osd_uuid_map()
                osd_uuid = osd_uuid_map.get(daemon_id)
                if not osd_uuid:
                    raise OrchestratorError('osd.%s not in osdmap' % daemon_id)
                extra_args.extend(['--osd-fsid', osd_uuid])

        if reconfig:
            extra_args.append('--reconfig')
        if self.allow_ptrace:
            extra_args.append('--allow-ptrace')

        self.log.info('%s daemon %s on %s' % (
            'Reconfiguring' if reconfig else 'Deploying',
            name, host))

        out, err, code = self._run_cephadm(
            host, name, 'deploy',
            [
                '--name', name,
            ] + extra_args,
            stdin=json.dumps(cephadm_config))
        if not code and host in self.cache.daemons:
            # prime cached service state with what we (should have)
            # just created
            sd = orchestrator.DaemonDescription()
            sd.daemon_type = daemon_type
            sd.daemon_id = daemon_id
            sd.hostname = host
            sd.status = 1
            sd.status_desc = 'starting'
            self.cache.add_daemon(host, sd)
        self.cache.invalidate_host_daemons(host)
        self.cache.update_daemon_config_deps(host, name, deps, start_time)
        self.cache.save_host(host)
        return "{} {} on host '{}'".format(
            'Reconfigured' if reconfig else 'Deployed', name, host)

    @forall_hosts
    def _remove_daemons(self, name, host) -> str:
        return self._remove_daemon(name, host)

    def _remove_daemon(self, name, host) -> str:
        """
        Remove a daemon
        """
        (daemon_type, daemon_id) = name.split('.', 1)
        if daemon_type == 'mon':
            self._check_safe_to_destroy_mon(daemon_id)

            # remove mon from quorum before we destroy the daemon
            self.log.info('Removing monitor %s from monmap...' % name)
            ret, out, err = self.check_mon_command({
                'prefix': 'mon rm',
                'name': daemon_id,
            })

        args = ['--name', name, '--force']
        self.log.info('Removing daemon %s from %s' % (name, host))
        out, err, code = self._run_cephadm(
            host, name, 'rm-daemon', args)
        if not code:
            # remove item from cache
            self.cache.rm_daemon(host, name)
        self.cache.invalidate_host_daemons(host)
        return "Removed {} from host '{}'".format(name, host)

    def _create_fn(self, service_type: str) -> Callable[..., str]:
        try:
            d: Dict[str, function] = {
                'mon': self.mon_service.create,
                'mgr': self.mgr_service.create,
                'osd': self.osd_service.create,
                'mds': self.mds_service.create,
                'rgw': self.rgw_service.create,
                'rbd-mirror': self.rbd_mirror_service.create,
                'nfs': self.nfs_service.create,
                'grafana': self.grafana_service.create,
                'alertmanager': self.alertmanager_service.create,
                'prometheus': self.prometheus_service.create,
                'node-exporter': self.node_exporter_service.create,
                'crash': self.crash_service.create,
                'iscsi': self.iscsi_service.create,
            }
            return d[service_type]  # type: ignore
        except KeyError:
            self.log.exception(f'unknown service type {service_type}')
            raise OrchestratorError(f'unknown service type {service_type}') from e

    def _config_fn(self, service_type) -> Optional[Callable[[ServiceSpec], None]]:
        return {
            'mds': self.mds_service.config,
            'rgw': self.rgw_service.config,
            'nfs': self.nfs_service.config,
            'iscsi': self.iscsi_service.config,
        }.get(service_type)

    def _apply_service(self, spec: ServiceSpec) -> bool:
        """
        Schedule a service.  Deploy new daemons or remove old ones, depending
        on the target label and count specified in the placement.
        """
        daemon_type = spec.service_type
        service_name = spec.service_name()
        if spec.unmanaged:
            self.log.debug('Skipping unmanaged service %s spec' % service_name)
            return False
        self.log.debug('Applying service %s spec' % service_name)

        create_func = self._create_fn(daemon_type)
        config_func = self._config_fn(daemon_type)

        if daemon_type == 'osd':
            create_func(spec)
            # TODO: return True would result in a busy loop
            return False

        daemons = self.cache.get_daemons_by_service(service_name)

        public_network = None
        if daemon_type == 'mon':
            ret, out, err = self.check_mon_command({
                'prefix': 'config get',
                'who': 'mon',
                'key': 'public_network',
            })
            if '/' in out:
                public_network = out.strip()
                self.log.debug('mon public_network is %s' % public_network)

        def matches_network(host):
            # type: (str) -> bool
            if not public_network:
                return False
            # make sure we have 1 or more IPs for that network on that
            # host
            return len(self.cache.networks[host].get(public_network, [])) > 0

        ha = HostAssignment(
            spec=spec,
            get_hosts_func=self._get_hosts,
            get_daemons_func=self.cache.get_daemons_by_service,
            filter_new_host=matches_network if daemon_type == 'mon' else None,
        )

        hosts: List[HostPlacementSpec] = ha.place()
        self.log.debug('Usable hosts: %s' % hosts)

        r = False

        # sanity check
        if daemon_type in ['mon', 'mgr'] and len(hosts) < 1:
            self.log.debug('cannot scale mon|mgr below 1 (hosts=%s)' % hosts)
            return False

        # add any?
        did_config = False

        add_daemon_hosts: Set[HostPlacementSpec] = ha.add_daemon_hosts(hosts)
        self.log.debug('Hosts that will receive new daemons: %s' % add_daemon_hosts)

        remove_daemon_hosts: Set[orchestrator.DaemonDescription] = ha.remove_daemon_hosts(hosts)
        self.log.debug('Hosts that will loose daemons: %s' % remove_daemon_hosts)

        for host, network, name in add_daemon_hosts:
            if not did_config and config_func:
                config_func(spec)
                did_config = True
            daemon_id = self.get_unique_name(daemon_type, host, daemons,
                                             prefix=spec.service_id,
                                             forcename=name)
            self.log.debug('Placing %s.%s on host %s' % (
                daemon_type, daemon_id, host))
            if daemon_type == 'mon':
                create_func(daemon_id, host, network)  # type: ignore
            elif daemon_type in ['nfs', 'iscsi']:
                create_func(daemon_id, host, spec)  # type: ignore
            else:
                create_func(daemon_id, host)  # type: ignore

            # add to daemon list so next name(s) will also be unique
            sd = orchestrator.DaemonDescription(
                hostname=host,
                daemon_type=daemon_type,
                daemon_id=daemon_id,
            )
            daemons.append(sd)
            r = True

        # remove any?
        for d in remove_daemon_hosts:
            # NOTE: we are passing the 'force' flag here, which means
            # we can delete a mon instances data.
            self._remove_daemon(d.name(), d.hostname)
            r = True

        return r

    def _apply_all_services(self):
        r = False
        specs = [] # type: List[ServiceSpec]
        for sn, spec in self.spec_store.specs.items():
            specs.append(spec)
        for spec in specs:
            try:
                if self._apply_service(spec):
                    r = True
            except Exception as e:
                self.log.exception('Failed to apply %s spec %s: %s' % (
                    spec.service_name(), spec, e))
        return r

    def _check_pool_exists(self, pool, service_name):
        logger.info(f'Checking pool "{pool}" exists for service {service_name}')
        if not self.rados.pool_exists(pool):
            raise OrchestratorError(f'Cannot find pool "{pool}" for '
                                    f'service {service_name}')

    def _check_daemons(self):
        # get monmap mtime so we can refresh configs when mons change
        monmap = self.get('mon_map')
        last_monmap: Optional[datetime.datetime] = datetime.datetime.strptime(
            monmap['modified'], CEPH_DATEFMT)
        if last_monmap and last_monmap > datetime.datetime.utcnow():
            last_monmap = None   # just in case clocks are skewed

        daemons = self.cache.get_daemons()
        daemons_post = defaultdict(list)
        for dd in daemons:
            # orphan?
            spec = self.spec_store.specs.get(dd.service_name(), None)
            if not spec and dd.daemon_type not in ['mon', 'mgr', 'osd']:
                # (mon and mgr specs should always exist; osds aren't matched
                # to a service spec)
                self.log.info('Removing orphan daemon %s...' % dd.name())
                self._remove_daemon(dd.name(), dd.hostname)

            # ignore unmanaged services
            if spec and spec.unmanaged:
                continue

            # These daemon types require additional configs after creation
            if dd.daemon_type in ['grafana', 'iscsi', 'prometheus', 'alertmanager', 'nfs']:
                daemons_post[dd.daemon_type].append(dd)

            deps = self._calc_daemon_deps(dd.daemon_type, dd.daemon_id)
            last_deps, last_config = self.cache.get_daemon_last_config_deps(
                dd.hostname, dd.name())
            if last_deps is None:
                last_deps = []
            reconfig = False
            if not last_config:
                self.log.info('Reconfiguring %s (unknown last config time)...'% (
                    dd.name()))
                reconfig = True
            elif last_deps != deps:
                self.log.debug('%s deps %s -> %s' % (dd.name(), last_deps,
                                                     deps))
                self.log.info('Reconfiguring %s (dependencies changed)...' % (
                    dd.name()))
                reconfig = True
            elif last_monmap and \
               last_monmap > last_config and \
               dd.daemon_type in CEPH_TYPES:
                self.log.info('Reconfiguring %s (monmap changed)...' % dd.name())
                reconfig = True
            if reconfig:
                self._create_daemon(dd.daemon_type, dd.daemon_id,
                                    dd.hostname, reconfig=True)

        # do daemon post actions
        for daemon_type, daemon_descs in daemons_post.items():
            self._get_cephadm_service(daemon_type).daemon_check_post(daemon_descs)

    def _add_daemon(self, daemon_type, spec,
                    create_func: Callable[..., T], config_func=None) -> List[T]:
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
                        create_func: Callable[..., T], config_func=None) -> List[T]:
        if count > len(hosts):
            raise OrchestratorError('too few hosts: want %d, have %s' % (
                count, hosts))

        if config_func:
            config_func(spec)

        args = []  # type: List[tuple]
        for host, network, name in hosts:
            daemon_id = self.get_unique_name(daemon_type, host, daemons,
                                             prefix=spec.service_id,
                                             forcename=name)
            self.log.debug('Placing %s.%s on host %s' % (
                daemon_type, daemon_id, host))
            if daemon_type == 'mon':
                args.append((daemon_id, host, network))  # type: ignore
            elif daemon_type in ['nfs', 'iscsi']:
                args.append((daemon_id, host, spec))  # type: ignore
            else:
                args.append((daemon_id, host))  # type: ignore

            # add to daemon list so next name(s) will also be unique
            sd = orchestrator.DaemonDescription(
                hostname=host,
                daemon_type=daemon_type,
                daemon_id=daemon_id,
            )
            daemons.append(sd)

        @forall_hosts
        def create_func_map(*args):
            return create_func(*args)

        return create_func_map(args)

    @trivial_completion
    def apply_mon(self, spec):
        return self._apply(spec)

    @trivial_completion
    def add_mon(self, spec):
        # type: (ServiceSpec) -> List[str]
        return self._add_daemon('mon', spec, self.mon_service.create)

    @trivial_completion
    def add_mgr(self, spec):
        # type: (ServiceSpec) -> List[str]
        return self._add_daemon('mgr', spec, self.mgr_service.create)

    def _apply(self, spec: GenericSpec) -> str:
        self.migration.verify_no_migration()

        if spec.service_type == 'host':
            return self._add_host(cast(HostSpec, spec))

        return self._apply_service_spec(cast(ServiceSpec, spec))

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
            }
            spec.placement = defaults[spec.service_type]
        elif spec.service_type in ['mon', 'mgr'] and \
             spec.placement.count is not None and \
             spec.placement.count < 1:
            raise OrchestratorError('cannot scale %s service below 1' % (
                spec.service_type))

        HostAssignment(
            spec=spec,
            get_hosts_func=self._get_hosts,
            get_daemons_func=self.cache.get_daemons_by_service,
        ).validate()

        self.log.info('Saving service %s spec with placement %s' % (
            spec.service_name(), spec.placement.pretty_str()))
        self.spec_store.save(spec)
        self._kick_serve_loop()
        return "Scheduled %s update..." % spec.service_name()

    @trivial_completion
    def apply(self, specs: List[GenericSpec]):
        results = []
        for spec in specs:
            results.append(self._apply(spec))
        return results

    @trivial_completion
    def apply_mgr(self, spec):
        return self._apply(spec)

    @trivial_completion
    def add_mds(self, spec: ServiceSpec):
        return self._add_daemon('mds', spec, self.mds_service.create, self.mds_service.config)

    @trivial_completion
    def apply_mds(self, spec: ServiceSpec):
        return self._apply(spec)

    @trivial_completion
    def add_rgw(self, spec):
        return self._add_daemon('rgw', spec, self.rgw_service.create, self.rgw_service.config)

    @trivial_completion
    def apply_rgw(self, spec):
        return self._apply(spec)

    @trivial_completion
    def add_iscsi(self, spec):
        # type: (ServiceSpec) -> List[str]
        return self._add_daemon('iscsi', spec, self.iscsi_service.create, self.iscsi_service.config)

    @trivial_completion
    def apply_iscsi(self, spec):
        return self._apply(spec)

    @trivial_completion
    def add_rbd_mirror(self, spec):
        return self._add_daemon('rbd-mirror', spec, self.rbd_mirror_service.create)

    @trivial_completion
    def apply_rbd_mirror(self, spec):
        return self._apply(spec)

    @trivial_completion
    def add_nfs(self, spec):
        return self._add_daemon('nfs', spec, self.nfs_service.create, self.nfs_service.config)

    @trivial_completion
    def apply_nfs(self, spec):
        return self._apply(spec)

    def _get_dashboard_url(self):
        # type: () -> str
        return self.get('mgr_map').get('services', {}).get('dashboard', '')

    @trivial_completion
    def add_prometheus(self, spec):
        return self._add_daemon('prometheus', spec, self.prometheus_service.create)

    @trivial_completion
    def apply_prometheus(self, spec):
        return self._apply(spec)

    @trivial_completion
    def add_node_exporter(self, spec):
        # type: (ServiceSpec) -> List[str]
        return self._add_daemon('node-exporter', spec,
                                self.node_exporter_service.create)

    @trivial_completion
    def apply_node_exporter(self, spec):
        return self._apply(spec)

    @trivial_completion
    def add_crash(self, spec):
        # type: (ServiceSpec) -> List[str]
        return self._add_daemon('crash', spec,
                                self.crash_service.create)

    @trivial_completion
    def apply_crash(self, spec):
        return self._apply(spec)

    @trivial_completion
    def add_grafana(self, spec):
        # type: (ServiceSpec) -> List[str]
        return self._add_daemon('grafana', spec, self.grafana_service.create)

    @trivial_completion
    def apply_grafana(self, spec: ServiceSpec):
        return self._apply(spec)

    @trivial_completion
    def add_alertmanager(self, spec):
        # type: (ServiceSpec) -> List[str]
        return self._add_daemon('alertmanager', spec, self.alertmanager_service.create)

    @trivial_completion
    def apply_alertmanager(self, spec: ServiceSpec):
        return self._apply(spec)

    def _get_container_image_id(self, image_name):
        # pick a random host...
        host = None
        for host_name in self.inventory.keys():
            host = host_name
            break
        if not host:
            raise OrchestratorError('no hosts defined')
        out, err, code = self._run_cephadm(
            host, None, 'pull', [],
            image=image_name,
            no_fsid=True,
            error_ok=True)
        if code:
            raise OrchestratorError('Failed to pull %s on %s: %s' % (
                image_name, host, '\n'.join(out)))
        j = json.loads('\n'.join(out))
        image_id = j.get('image_id')
        ceph_version = j.get('ceph_version')
        self.log.debug('image %s -> id %s version %s' %
                       (image_name, image_id, ceph_version))
        return image_id, ceph_version

    @trivial_completion
    def upgrade_check(self, image, version):
        if version:
            target_name = self.container_image_base + ':v' + version
        elif image:
            target_name = image
        else:
            raise OrchestratorError('must specify either image or version')

        target_id, target_version = self._get_container_image_id(target_name)
        self.log.debug('Target image %s id %s version %s' % (
            target_name, target_id, target_version))
        r = {
            'target_name': target_name,
            'target_id': target_id,
            'target_version': target_version,
            'needs_update': dict(),
            'up_to_date': list(),
        }
        for host, dm in self.cache.daemons.items():
            for name, dd in dm.items():
                if target_id == dd.container_image_id:
                    r['up_to_date'].append(dd.name())
                else:
                    r['needs_update'][dd.name()] = {
                        'current_name': dd.container_image_name,
                        'current_id': dd.container_image_id,
                        'current_version': dd.version,
                    }
        return json.dumps(r, indent=4, sort_keys=True)

    @trivial_completion
    def upgrade_status(self):
        return self.upgrade.upgrade_status()

    @trivial_completion
    def upgrade_start(self, image, version):
        return self.upgrade.upgrade_start(image, version)

    @trivial_completion
    def upgrade_pause(self):
        return self.upgrade.upgrade_pause()

    @trivial_completion
    def upgrade_resume(self):
        return self.upgrade.upgrade_resume()

    @trivial_completion
    def upgrade_stop(self):
        return self.upgrade.upgrade_stop()

    @trivial_completion
    def remove_osds(self, osd_ids: List[str],
                    replace: bool = False,
                    force: bool = False):
        """
        Takes a list of OSDs and schedules them for removal.
        The function that takes care of the actual removal is
        _remove_osds_bg().
        """

        daemons = self.cache.get_daemons_by_service('osd')
        found: Set[OSDRemoval] = set()
        for daemon in daemons:
            if daemon.daemon_id not in osd_ids:
                continue
            found.add(OSDRemoval(daemon.daemon_id, replace, force,
                                 daemon.hostname, daemon.name(),
                                 datetime.datetime.utcnow(), -1))

        not_found = {osd_id for osd_id in osd_ids if osd_id not in [x.osd_id for x in found]}
        if not_found:
            raise OrchestratorError('Unable to find OSD: %s' % not_found)

        self.rm_util.queue_osds_for_removal(found)

        # trigger the serve loop to initiate the removal
        self._kick_serve_loop()
        return "Scheduled OSD(s) for removal"

    @trivial_completion
    def remove_osds_status(self):
        """
        The CLI call to retrieve an osd removal report
        """
        return self.rm_util.report
