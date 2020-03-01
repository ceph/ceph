import json
import errno
import logging
import time
import yaml
from threading import Event
from functools import wraps

from mgr_util import create_self_signed_cert

import string
try:
    from typing import List, Dict, Optional, Callable, Tuple, TypeVar, Type, Any, NamedTuple, Iterator
    from typing import TYPE_CHECKING
except ImportError:
    TYPE_CHECKING = False  # just for type checking


import datetime
import six
import os
import random
import tempfile
import multiprocessing.pool
import re
import shutil
import subprocess
import uuid

from ceph.deployment import inventory, translate
from ceph.deployment.drive_group import DriveGroupSpec
from ceph.deployment.drive_selection import selector

from mgr_module import MgrModule
import orchestrator
from orchestrator import OrchestratorError, HostPlacementSpec, OrchestratorValidationError, HostSpec, \
    CLICommandMeta, ServiceSpec

from . import remotes
from ._utils import RemoveUtil


try:
    import remoto
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

DEFAULT_SSH_CONFIG = ('Host *\n'
                      'User root\n'
                      'StrictHostKeyChecking no\n'
                      'UserKnownHostsFile /dev/null\n')

DATEFMT = '%Y-%m-%dT%H:%M:%S.%f'

HOST_CACHE_PREFIX = "host."
SPEC_STORE_PREFIX = "spec."

# for py2 compat
try:
    from tempfile import TemporaryDirectory # py3
except ImportError:
    # define a minimal (but sufficient) equivalent for <= py 3.2
    class TemporaryDirectory(object): # type: ignore
        def __init__(self):
            self.name = tempfile.mkdtemp()

        def __enter__(self):
            if not self.name:
                self.name = tempfile.mkdtemp()
            return self.name

        def cleanup(self):
            shutil.rmtree(self.name)

        def __exit__(self, exc_type, exc_value, traceback):
            self.cleanup()

class OSDRemoval(NamedTuple):
    osd_id: int
    replace: bool
    force: bool
    nodename: str
    fullname: str
    started_at: datetime.datetime

    # needed due to changing 'started_at' attr
    def __eq__(self, other):
        return self.osd_id == other.osd_id

    def __hash__(self):
        return hash(self.osd_id)

# high-level TODO:
#  - bring over some of the protections from ceph-deploy that guard against
#    multiple bootstrapping / initialization

def _name_to_entity_name(name):
    """
    Map from daemon names to ceph entity names (as seen in config)
    """
    if name.startswith('rgw.') or name.startswith('rbd-mirror'):
        return 'client.' + name
    else:
        return name

def assert_valid_host(name):
    p = re.compile('^[a-zA-Z0-9-]+$')
    try:
        assert len(name) <= 250, 'name is too long (max 250 chars)'
        parts = name.split('.')
        for part in name.split('.'):
            assert len(part) > 0, '.-delimited name component must not be empty'
            assert len(part) <= 63, '.-delimited name component must not be more than 63 chars'
            assert p.match(part), 'name component must include only a-z, 0-9, and -'
    except AssertionError as e:
        raise OrchestratorError(e)


class SpecStore():
    def __init__(self, mgr):
        # type: (CephadmOrchestrator) -> None
        self.mgr = mgr
        self.specs = {} # type: Dict[str, orchestrator.ServiceSpec]

    def load(self):
        # type: () -> None
        for k, v in six.iteritems(self.mgr.get_store_prefix(SPEC_STORE_PREFIX)):
            service_name = k[len(SPEC_STORE_PREFIX):]
            try:
                spec = ServiceSpec.from_json(json.loads(v))
                self.specs[service_name] = spec
                self.mgr.log.debug('SpecStore: loaded spec for %s' % (
                    service_name))
            except Exception as e:
                self.mgr.log.warning('unable to load spec for %s: %s' % (
                    service_name, e))
                pass

    def save(self, spec):
        # type: (orchestrator.ServiceSpec) -> None
        self.specs[spec.service_name()] = spec
        self.mgr.set_store(SPEC_STORE_PREFIX + spec.service_name(),
                           spec.to_json())

    def rm(self, service_name):
        # type: (str) -> None
        if service_name in self.specs:
            del self.specs[service_name]
            self.mgr.set_store(SPEC_STORE_PREFIX + service_name, None)

    def find(self, service_name):
        # type: (str) -> List[orchestrator.ServiceSpec]
        specs = []
        for sn, spec in self.specs.items():
            if sn == service_name or sn.startswith(service_name + '.'):
                specs.append(spec)
        return specs

class HostCache():
    def __init__(self, mgr):
        # type: (CephadmOrchestrator) -> None
        self.mgr = mgr
        self.daemons = {}   # type: Dict[str, Dict[str, orchestrator.DaemonDescription]]
        self.last_daemon_update = {}   # type: Dict[str, datetime.datetime]
        self.devices = {}              # type: Dict[str, List[inventory.Device]]
        self.last_device_update = {}   # type: Dict[str, datetime.datetime]
        self.daemon_refresh_queue = [] # type: List[str]
        self.device_refresh_queue = [] # type: List[str]

    def load(self):
        # type: () -> None
        for k, v in six.iteritems(self.mgr.get_store_prefix(HOST_CACHE_PREFIX)):
            host = k[len(HOST_CACHE_PREFIX):]
            if host not in self.mgr.inventory:
                self.mgr.log.warning('removing stray HostCache host record %s' % (
                    host))
                self.mgr.set_store(k, None)
            try:
                j = json.loads(v)
                if 'last_device_update' in j:
                    self.last_device_update[host] = datetime.datetime.strptime(
                        j['last_device_update'], DATEFMT)
                else:
                    self.device_refresh_queue.append(host)
                # for services, we ignore the persisted last_*_update
                # and always trigger a new scrape on mgr restart.
                self.daemon_refresh_queue.append(host)
                self.daemons[host] = {}
                self.devices[host] = []
                for name, d in j.get('daemons', {}).items():
                    self.daemons[host][name] = \
                        orchestrator.DaemonDescription.from_json(d)
                for d in j.get('devices', []):
                    self.devices[host].append(inventory.Device.from_json(d))
                self.mgr.log.debug('HostCache.load: host %s has %d daemons, %d devices' % (
                    host, len(self.daemons[host]), len(self.devices[host])))
            except Exception as e:
                self.mgr.log.warning('unable to load cached state for %s: %s' % (
                    host, e))
                pass

    def update_host_daemons(self, host, dm):
        # type: (str, Dict[str, orchestrator.DaemonDescription]) -> None
        self.daemons[host] = dm
        self.last_daemon_update[host] = datetime.datetime.utcnow()

    def update_host_devices(self, host, dls):
        # type: (str, List[inventory.Device]) -> None
        self.devices[host] = dls
        self.last_device_update[host] = datetime.datetime.utcnow()

    def prime_empty_host(self, host):
        # type: (str) -> None
        """
        Install an empty entry for a host
        """
        self.daemons[host] = {}
        self.devices[host] = []
        self.daemon_refresh_queue.append(host)
        self.device_refresh_queue.append(host)

    def invalidate_host_daemons(self, host):
        # type: (str) -> None
        self.daemon_refresh_queue.append(host)
        if host in self.last_daemon_update:
            del self.last_daemon_update[host]
        self.mgr.event.set()

    def invalidate_host_devices(self, host):
        # type: (str) -> None
        self.device_refresh_queue.append(host)
        if host in self.last_device_update:
            del self.last_device_update[host]
        self.mgr.event.set()

    def save_host(self, host):
        # type: (str) -> None
        j = {   # type: ignore
            'daemons': {},
            'devices': [],
        }
        if host in self.last_daemon_update:
            j['last_daemon_update'] = self.last_daemon_update[host].strftime(DATEFMT) # type: ignore
        if host in self.last_device_update:
            j['last_device_update'] = self.last_device_update[host].strftime(DATEFMT) # type: ignore
        for name, dd in self.daemons[host].items():
            j['daemons'][name] = dd.to_json()  # type: ignore
        for d in self.devices[host]:
            j['devices'].append(d.to_json())  # type: ignore
        self.mgr.set_store(HOST_CACHE_PREFIX + host, json.dumps(j))

    def rm_host(self, host):
        # type: (str) -> None
        if host in self.daemons:
            del self.daemons[host]
        if host in self.devices:
            del self.devices[host]
        if host in self.last_daemon_update:
            del self.last_daemon_update[host]
        if host in self.last_device_update:
            del self.last_device_update[host]
        self.mgr.set_store(HOST_CACHE_PREFIX + host, None)

    def get_hosts(self):
        # type: () -> List[str]
        r = []
        for host, di in self.daemons.items():
            r.append(host)
        return r

    def get_daemons(self):
        # type: () -> List[orchestrator.DaemonDescription]
        r = []
        for host, dm in self.daemons.items():
            for name, dd in dm.items():
                r.append(dd)
        return r

    def get_daemons_by_service(self, service_name):
        # type: (str) -> List[orchestrator.DaemonDescription]
        result = []   # type: List[orchestrator.DaemonDescription]
        for host, dm in self.daemons.items():
            for name, d in dm.items():
                if name.startswith(service_name + '.'):
                    result.append(d)
        return result

    def get_daemon_names(self):
        # type: () -> List[str]
        r = []
        for host, dm in self.daemons.items():
            for name, dd in dm.items():
                r.append(name)
        return r

    def host_needs_daemon_refresh(self, host):
        # type: (str) -> bool
        if host in self.daemon_refresh_queue:
            self.daemon_refresh_queue.remove(host)
            return True
        cutoff = datetime.datetime.utcnow() - datetime.timedelta(
            seconds=self.mgr.daemon_cache_timeout)
        if host not in self.last_daemon_update or self.last_daemon_update[host] < cutoff:
            return True
        return False

    def host_needs_device_refresh(self, host):
        # type: (str) -> bool
        if host in self.device_refresh_queue:
            self.device_refresh_queue.remove(host)
            return True
        cutoff = datetime.datetime.utcnow() - datetime.timedelta(
            seconds=self.mgr.device_cache_timeout)
        if host not in self.last_device_update or self.last_device_update[host] < cutoff:
            return True
        return False

    def add_daemon(self, host, dd):
        # type: (str, orchestrator.DaemonDescription) -> None
        assert host in self.daemons
        self.daemons[host][dd.name()] = dd

    def rm_daemon(self, host, name):
        if host in self.daemons:
            if name in self.daemons[host]:
                del self.daemons[host][name]


class AsyncCompletion(orchestrator.Completion):
    def __init__(self,
                 _first_promise=None,  # type: Optional[orchestrator.Completion]
                 value=orchestrator._Promise.NO_RESULT,  # type: Any
                 on_complete=None,  # type: Optional[Callable]
                 name=None,  # type: Optional[str]
                 many=False, # type: bool
                 update_progress=False,  # type: bool
                 ):

        assert CephadmOrchestrator.instance is not None
        self.many = many
        self.update_progress = update_progress
        if name is None and on_complete is not None:
            name = getattr(on_complete, '__name__', None)
        super(AsyncCompletion, self).__init__(_first_promise, value, on_complete, name)

    @property
    def _progress_reference(self):
        # type: () -> Optional[orchestrator.ProgressReference]
        if hasattr(self._on_complete_, 'progress_id'):  # type: ignore
            return self._on_complete_  # type: ignore
        return None

    @property
    def _on_complete(self):
        # type: () -> Optional[Callable]
        if self._on_complete_ is None:
            return None

        def callback(result):
            try:
                if self.update_progress:
                    assert self.progress_reference
                    self.progress_reference.progress = 1.0
                self._on_complete_ = None
                self._finalize(result)
            except Exception as e:
                try:
                    self.fail(e)
                except Exception:
                    logger.exception(f'failed to fail AsyncCompletion: >{repr(self)}<')
                    if 'UNITTEST' in os.environ:
                        assert False

        def error_callback(e):
            pass

        def run(value):
            def do_work(*args, **kwargs):
                assert self._on_complete_ is not None
                try:
                    res = self._on_complete_(*args, **kwargs)
                    if self.update_progress and self.many:
                        assert self.progress_reference
                        self.progress_reference.progress += 1.0 / len(value)
                    return res
                except Exception as e:
                    self.fail(e)
                    raise

            assert CephadmOrchestrator.instance
            if self.many:
                if not value:
                    logger.info('calling map_async without values')
                    callback([])
                if six.PY3:
                    CephadmOrchestrator.instance._worker_pool.map_async(do_work, value,
                                                                    callback=callback,
                                                                    error_callback=error_callback)
                else:
                    CephadmOrchestrator.instance._worker_pool.map_async(do_work, value,
                                                                    callback=callback)
            else:
                if six.PY3:
                    CephadmOrchestrator.instance._worker_pool.apply_async(do_work, (value,),
                                                                      callback=callback, error_callback=error_callback)
                else:
                    CephadmOrchestrator.instance._worker_pool.apply_async(do_work, (value,),
                                                                      callback=callback)
            return self.ASYNC_RESULT

        return run

    @_on_complete.setter
    def _on_complete(self, inner):
        # type: (Callable) -> None
        self._on_complete_ = inner


def ssh_completion(cls=AsyncCompletion, **c_kwargs):
    # type: (Type[orchestrator.Completion], Any) -> Callable
    """
    See ./HACKING.rst for a how-to
    """
    def decorator(f):
        @wraps(f)
        def wrapper(*args):

            name = f.__name__
            many = c_kwargs.get('many', False)

            # Some weired logic to make calling functions with multiple arguments work.
            if len(args) == 1:
                [value] = args
                if many and value and isinstance(value[0], tuple):
                    return cls(on_complete=lambda x: f(*x), value=value, name=name, **c_kwargs)
                else:
                    return cls(on_complete=f, value=value, name=name, **c_kwargs)
            else:
                if many:
                    self, value = args

                    def call_self(inner_args):
                        if not isinstance(inner_args, tuple):
                            inner_args = (inner_args, )
                        return f(self, *inner_args)

                    return cls(on_complete=call_self, value=value, name=name, **c_kwargs)
                else:
                    return cls(on_complete=lambda x: f(*x), value=args, name=name, **c_kwargs)

        return wrapper
    return decorator


def async_completion(f):
    # type: (Callable) -> Callable[..., AsyncCompletion]
    """
    See ./HACKING.rst for a how-to

    :param f: wrapped function
    """
    return ssh_completion()(f)


def async_map_completion(f):
    # type: (Callable) -> Callable[..., AsyncCompletion]
    """
    See ./HACKING.rst for a how-to

    :param f: wrapped function

    kind of similar to

    >>> def sync_map(f):
    ...     return lambda x: map(f, x)

    """
    return ssh_completion(many=True)(f)


def trivial_completion(f):
    # type: (Callable) -> Callable[..., orchestrator.Completion]
    return ssh_completion(cls=orchestrator.Completion)(f)


def trivial_result(val):
    # type: (Any) -> AsyncCompletion
    return AsyncCompletion(value=val, name='trivial_result')


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
            'default': 10 * 60,
            'desc': 'seconds to cache device inventory',
        },
        {
            'name': 'daemon_cache_timeout',
            'type': 'secs',
            'default': 60,
            'desc': 'seconds to cache service (daemon) inventory',
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
            'default': 'ceph/ceph',
            'desc': 'Container image name, without the tag',
            'runtime': True,
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
    ]

    def __init__(self, *args, **kwargs):
        super(CephadmOrchestrator, self).__init__(*args, **kwargs)
        self._cluster_fsid = self.get('mon_map')['fsid']

        # for serve()
        self.run = True
        self.event = Event()

        # for mypy which does not run the code
        if TYPE_CHECKING:
            self.ssh_config_file = None  # type: Optional[str]
            self.device_cache_timeout = 0
            self.daemon_cache_timeout = 0
            self.mode = ''
            self.container_image_base = ''
            self.warn_on_stray_hosts = True
            self.warn_on_stray_daemons = True
            self.warn_on_failed_host_check = True

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

        t = self.get_store('upgrade_state')
        if t:
            self.upgrade_state = json.loads(t)
        else:
            self.upgrade_state = None

        self.health_checks = {}

        self.all_progress_references = list()  # type: List[orchestrator.ProgressReference]

        # load inventory
        i = self.get_store('inventory')
        if i:
            self.inventory: Dict[str, dict] = json.loads(i)
        else:
            self.inventory = dict()
        self.log.debug('Loaded inventory %s' % self.inventory)

        self.cache = HostCache(self)
        self.cache.load()
        self.to_remove_osds: set = set()
        self.osd_removal_report: dict = dict()
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

    def shutdown(self):
        self.log.debug('shutdown')
        self._worker_pool.close()
        self._worker_pool.join()
        self.run = False
        self.event.set()

    def _kick_serve_loop(self):
        self.log.debug('_kick_serve_loop')
        self.event.set()

    def _wait_for_ok_to_stop(self, s):
        # only wait a little bit; the service might go away for something
        tries = 4
        while tries > 0:
            if s.daemon_type not in ['mon', 'osd', 'mds']:
                self.log.info('Upgrade: It is presumed safe to stop %s.%s' %
                              (s.daemon_type, s.daemon_id))
                return True
            ret, out, err = self.mon_command({
                'prefix': '%s ok-to-stop' % s.daemon_type,
                'ids': [s.daemon_id],
            })
            if not self.upgrade_state or self.upgrade_state.get('paused'):
                return False
            if ret:
                self.log.info('Upgrade: It is NOT safe to stop %s.%s' %
                              (s.daemon_type, s.daemon_id))
                time.sleep(15)
                tries -= 1
            else:
                self.log.info('Upgrade: It is safe to stop %s.%s' %
                              (s.daemon_type, s.daemon_id))
                return True
        return False

    def _clear_upgrade_health_checks(self):
        for k in ['UPGRADE_NO_STANDBY_MGR',
                  'UPGRADE_FAILED_PULL']:
            if k in self.health_checks:
                del self.health_checks[k]
        self.set_health_checks(self.health_checks)

    def _fail_upgrade(self, alert_id, alert):
        self.log.error('Upgrade: Paused due to %s: %s' % (alert_id,
                                                          alert['summary']))
        self.upgrade_state['error'] = alert_id + ': ' + alert['summary']
        self.upgrade_state['paused'] = True
        self._save_upgrade_state()
        self.health_checks[alert_id] = alert
        self.set_health_checks(self.health_checks)

    def _update_upgrade_progress(self, progress):
        if 'progress_id' not in self.upgrade_state:
            self.upgrade_state['progress_id'] = str(uuid.uuid4())
            self._save_upgrade_state()
        self.remote('progress', 'update', self.upgrade_state['progress_id'],
                    ev_msg='Upgrade to %s' % self.upgrade_state['target_name'],
                    ev_progress=progress)

    def _do_upgrade(self):
        # type: () -> Optional[AsyncCompletion]
        if not self.upgrade_state:
            self.log.debug('_do_upgrade no state, exiting')
            return None

        target_name = self.upgrade_state.get('target_name')
        target_id = self.upgrade_state.get('target_id', None)
        if not target_id:
            # need to learn the container hash
            self.log.info('Upgrade: First pull of %s' % target_name)
            try:
                target_id, target_version = self._get_container_image_id(target_name)
            except OrchestratorError as e:
                self._fail_upgrade('UPGRADE_FAILED_PULL', {
                    'severity': 'warning',
                    'summary': 'Upgrade: failed to pull target image',
                    'count': 1,
                    'detail': [str(e)],
                })
                return None
            self.upgrade_state['target_id'] = target_id
            self.upgrade_state['target_version'] = target_version
            self._save_upgrade_state()
        target_version = self.upgrade_state.get('target_version')
        self.log.info('Upgrade: Target is %s with id %s' % (target_name,
                                                            target_id))

        # get all distinct container_image settings
        image_settings = {}
        ret, out, err = self.mon_command({
            'prefix': 'config dump',
            'format': 'json',
        })
        config = json.loads(out)
        for opt in config:
            if opt['name'] == 'container_image':
                image_settings[opt['section']] = opt['value']

        daemons = self.cache.get_daemons()
        done = 0
        for daemon_type in ['mgr', 'mon', 'osd', 'rgw', 'mds']:
            self.log.info('Upgrade: Checking %s daemons...' % daemon_type)
            need_upgrade_self = False
            for d in daemons:
                if d.daemon_type != daemon_type:
                    continue
                if not d.container_image_id:
                    self.log.debug('daemon %s.%s image_id is not known' % (
                        daemon_type, d.daemon_id))
                    return None
                if d.container_image_id == target_id:
                    self.log.debug('daemon %s.%s version correct' % (
                        daemon_type, d.daemon_id))
                    done += 1
                    continue
                self.log.debug('daemon %s.%s version incorrect (%s, %s)' % (
                    daemon_type, d.daemon_id,
                    d.container_image_id, d.version))

                if daemon_type == 'mgr' and \
                   d.daemon_id == self.get_mgr_id():
                    self.log.info('Upgrade: Need to upgrade myself (mgr.%s)' %
                                  self.get_mgr_id())
                    need_upgrade_self = True
                    continue

                # make sure host has latest container image
                out, err, code = self._run_cephadm(
                    d.hostname, None, 'inspect-image', [],
                    image=target_name, no_fsid=True, error_ok=True)
                if code or json.loads(''.join(out)).get('image_id') != target_id:
                    self.log.info('Upgrade: Pulling %s on %s' % (target_name,
                                                                 d.hostname))
                    out, err, code = self._run_cephadm(
                        d.hostname, None, 'pull', [],
                        image=target_name, no_fsid=True, error_ok=True)
                    if code:
                        self._fail_upgrade('UPGRADE_FAILED_PULL', {
                            'severity': 'warning',
                            'summary': 'Upgrade: failed to pull target image',
                            'count': 1,
                            'detail': [
                                'failed to pull %s on host %s' % (target_name,
                                                                  d.hostname)],
                        })
                        return None
                    r = json.loads(''.join(out))
                    if r.get('image_id') != target_id:
                        self.log.info('Upgrade: image %s pull on %s got new image %s (not %s), restarting' % (target_name, d.hostname, r['image_id'], target_id))
                        self.upgrade_state['target_id'] = r['image_id']
                        self._save_upgrade_state()
                        return None

                self._update_upgrade_progress(done / len(daemons))

                if d.status <= 0:
                    if d.container_image_name == target_name:
                        self.log.debug('daemon %s is stopped but has correct image name' % (d.name()))
                        continue
                if not self._wait_for_ok_to_stop(d):
                    return None
                self.log.info('Upgrade: Redeploying %s.%s' %
                              (d.daemon_type, d.daemon_id))
                ret, out, err = self.mon_command({
                    'prefix': 'config set',
                    'name': 'container_image',
                    'value': target_name,
                    'who': daemon_type + '.' + d.daemon_id,
                })
                return self._daemon_action([(
                    d.daemon_type,
                    d.daemon_id,
                    d.hostname,
                    'redeploy'
                )])

            if need_upgrade_self:
                mgr_map = self.get('mgr_map')
                num = len(mgr_map.get('standbys'))
                if not num:
                    self._fail_upgrade('UPGRADE_NO_STANDBY_MGR', {
                        'severity': 'warning',
                        'summary': 'Upgrade: Need standby mgr daemon',
                        'count': 1,
                        'detail': [
                            'The upgrade process needs to upgrade the mgr, '
                            'but it needs at least one standby to proceed.',
                        ],
                    })
                    return None

                self.log.info('Upgrade: there are %d other already-upgraded '
                              'standby mgrs, failing over' % num)

                self._update_upgrade_progress(done / len(daemons))

                # fail over
                ret, out, err = self.mon_command({
                    'prefix': 'mgr fail',
                    'who': self.get_mgr_id(),
                })
                return None
            elif daemon_type == 'mgr':
                if 'UPGRADE_NO_STANDBY_MGR' in self.health_checks:
                    del self.health_checks['UPGRADE_NO_STANDBY_MGR']
                    self.set_health_checks(self.health_checks)

            # make sure 'ceph versions' agrees
            ret, out, err = self.mon_command({
                'prefix': 'versions',
            })
            j = json.loads(out)
            for version, count in j.get(daemon_type, {}).items():
                if version != target_version:
                    self.log.warning(
                        'Upgrade: %d %s daemon(s) are %s != target %s' %
                        (count, daemon_type, version, target_version))

            # push down configs
            if image_settings.get(daemon_type) != target_name:
                self.log.info('Upgrade: Setting container_image for all %s...' %
                              daemon_type)
                ret, out, err = self.mon_command({
                    'prefix': 'config set',
                    'name': 'container_image',
                    'value': target_name,
                    'who': daemon_type,
                })
            to_clean = []
            for section in image_settings.keys():
                if section.startswith(daemon_type + '.'):
                    to_clean.append(section)
            if to_clean:
                self.log.debug('Upgrade: Cleaning up container_image for %s...' %
                               to_clean)
                for section in to_clean:
                    ret, image, err = self.mon_command({
                        'prefix': 'config rm',
                        'name': 'container_image',
                        'who': section,
                    })
            self.log.info('Upgrade: All %s daemons are up to date.' %
                          daemon_type)

        # clean up
        self.log.info('Upgrade: Finalizing container_image settings')
        ret, out, err = self.mon_command({
            'prefix': 'config set',
            'name': 'container_image',
            'value': target_name,
            'who': 'global',
        })
        for daemon_type in ['mgr', 'mon', 'osd', 'rgw', 'mds']:
            ret, image, err = self.mon_command({
                'prefix': 'config rm',
                'name': 'container_image',
                'who': daemon_type,
            })

        self.log.info('Upgrade: Complete!')
        if 'progress_id' in self.upgrade_state:
            self.remote('progress', 'complete',
                        self.upgrade_state['progress_id'])
        self.upgrade_state = None
        self._save_upgrade_state()
        return None

    def _check_hosts(self):
        self.log.debug('_check_hosts')
        bad_hosts = []
        hosts = self.inventory.keys()
        for host in hosts:
            if host not in self.inventory:
                continue
            self.log.debug(' checking %s' % host)
            try:
                out, err, code = self._run_cephadm(
                    host, 'client', 'check-host', [],
                    error_ok=True, no_fsid=True)
                if code:
                    self.log.debug(' host %s failed check' % host)
                    if self.warn_on_failed_host_check:
                        bad_hosts.append('host %s failed check: %s' % (host, err))
                else:
                    self.log.debug(' host %s ok' % host)
            except Exception as e:
                self.log.debug(' host %s failed check' % host)
                bad_hosts.append('host %s failed check: %s' % (host, e))
        if 'CEPHADM_HOST_CHECK_FAILED' in self.health_checks:
            del self.health_checks['CEPHADM_HOST_CHECK_FAILED']
        if bad_hosts:
            self.health_checks['CEPHADM_HOST_CHECK_FAILED'] = {
                'severity': 'warning',
                'summary': '%d hosts fail cephadm check' % len(bad_hosts),
                'count': len(bad_hosts),
                'detail': bad_hosts,
            }
        self.set_health_checks(self.health_checks)

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
            if host_detail:
                self.health_checks['CEPHADM_STRAY_HOST'] = {
                    'severity': 'warning',
                    'summary': '%d stray host(s) with %s daemon(s) '
                    'not managed by cephadm' % (
                        len(host_detail), host_num_daemons),
                    'count': len(host_detail),
                    'detail': host_detail,
                }
            if daemon_detail:
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
            self._check_hosts()

            # refresh daemons
            self.log.debug('refreshing hosts')
            failures = []
            for host in self.cache.get_hosts():
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
            if failures:
                self.health_checks['CEPHADM_REFRESH_FAILED'] = {
                    'severity': 'warning',
                    'summary': 'failed to probe daemons or devices',
                    'count': len(failures),
                    'detail': failures,
                }
                self.set_health_checks(self.health_checks)
            elif 'CEPHADM_REFRESH_FAILED' in self.health_checks:
                del self.health_checks['CEPHADM_REFRESH_FAILED']
                self.set_health_checks(self.health_checks)

            self._check_for_strays()

            self._remove_osds_bg()

            service_completions = self._apply_all_services()
            for service_completion in service_completions:
                if service_completion:
                    while not service_completion.has_result:
                        self.process([service_completion])
                        self.log.debug(f'Still processing {service_completion}')
                        if service_completion.needs_result:
                            time.sleep(1)
                        else:
                            break
                    if service_completion.exception is not None:
                        self.log.error(str(service_completion.exception))

            if self.upgrade_state and not self.upgrade_state.get('paused'):
                upgrade_completion = self._do_upgrade()
                if upgrade_completion:
                    while not upgrade_completion.has_result:
                        self.process([upgrade_completion])
                        if upgrade_completion.needs_result:
                            time.sleep(1)
                        else:
                            break
                    if upgrade_completion.exception is not None:
                        self.log.error(str(upgrade_completion.exception))
                self.log.debug('did _do_upgrade')
            else:
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

    def get_unique_name(self, daemon_type, host, existing, prefix=None,
                        forcename=None):
        # type: (str, str, List[orchestrator.DaemonDescription], Optional[str], Optional[str]) -> str
        """
        Generate a unique random service name
        """
        suffix = daemon_type not in [
            'mon', 'crash',
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

    def _save_inventory(self):
        self.set_store('inventory', json.dumps(self.inventory))

    def _save_upgrade_state(self):
        self.set_store('upgrade_state', json.dumps(self.upgrade_state))

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
            if not os.path.isfile(ssh_config_fname):
                raise Exception("ssh_config \"{}\" does not exist".format(
                    ssh_config_fname))
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
        return self.can_run()

    def process(self, completions):
        """
        Does nothing, as completions are processed in another thread.
        """
        if completions:
            self.log.debug("process: completions={0}".format(orchestrator.pretty_print(completions)))

            for p in completions:
                p.finalize()

    def _require_hosts(self, hosts):
        """
        Raise an error if any of the given hosts are unregistered.
        """
        if isinstance(hosts, six.string_types):
            hosts = [hosts]
        keys = self.inventory.keys()
        unregistered_hosts = set(hosts) - keys
        if unregistered_hosts:
            logger.warning('keys = {}'.format(keys))
            raise RuntimeError("Host(s) {} not registered".format(
                ", ".join(map(lambda h: "'{}'".format(h),
                    unregistered_hosts))))

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

    @orchestrator._cli_write_command(
        'cephadm generate-key',
        desc='Generate a cluster SSH key (if not present)')
    def _generate_key(self):
        if not self.ssh_pub or not self.ssh_key:
            self.log.info('Generating ssh key...')
            tmp_dir = TemporaryDirectory()
            path = tmp_dir.name + '/key'
            try:
                subprocess.call([
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
    def _check_host(self, host, addr=None):
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
        conn_and_r = self._cons.get(host)
        if conn_and_r:
            self.log.debug('Have connection to %s' % host)
            return conn_and_r
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

    def _run_cephadm(self, host, entity, command, args,
                     addr=None,
                     stdin=None,
                     no_fsid=False,
                     error_ok=False,
                     image=None):
        """
        Run cephadm on the remote host with the given command + args
        """
        if not addr and host in self.inventory:
            addr = self.inventory[host].get('addr', host)
        conn, connr = self._get_connection(addr)

        try:
            if not image:
                # get container image
                ret, image, err = self.mon_command({
                    'prefix': 'config get',
                    'who': _name_to_entity_name(entity),
                    'key': 'container_image',
                })
                image = image.strip()
            self.log.debug('%s container image %s' % (entity, image))

            final_args = [
                '--image', image,
                command
            ]
            if not no_fsid:
                final_args += ['--fsid', self._cluster_fsid]
            final_args += args

            if self.mode == 'root':
                self.log.debug('args: %s' % (' '.join(final_args)))
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
                        return '', str(e), 1
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
                        return '', str(e), 1
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
            raise OrchestratorError('New host %s (%s) failed to connect: `ssh %s`' % (
                host, addr, str(e))) from e
        except Exception as ex:
            self.log.exception(ex)
            raise

    def _get_hosts(self, label=None):
        # type: (Optional[str]) -> List[str]
        r = []
        for h, hostspec in self.inventory.items():
            if not label or label in hostspec.get('labels', []):
                r.append(h)
        return r

    @async_completion
    def add_host(self, spec):
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

        self.inventory[spec.hostname] = spec.to_json()
        self._save_inventory()
        self.cache.prime_empty_host(spec.hostname)
        self.event.set()  # refresh stray health check
        self.log.info('Added host %s' % spec.hostname)
        return "Added host '{}'".format(spec.hostname)

    @async_completion
    def remove_host(self, host):
        # type: (str) -> str
        """
        Remove a host from orchestrator management.

        :param host: host name
        """
        del self.inventory[host]
        self._save_inventory()
        self.cache.rm_host(host)
        self._reset_con(host)
        self.event.set()  # refresh stray health check
        self.log.info('Removed host %s' % host)
        return "Removed host '{}'".format(host)

    @async_completion
    def update_host_addr(self, host, addr):
        if host not in self.inventory:
            raise OrchestratorError('host %s not registered' % host)
        self.inventory[host]['addr'] = addr
        self._save_inventory()
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
        r = []
        for hostname, info in self.inventory.items():
            r.append(orchestrator.HostSpec(
                hostname,
                addr=info.get('addr', hostname),
                labels=info.get('labels', []),
                status=info.get('status', ''),
            ))
        return r

    @async_completion
    def add_host_label(self, host, label):
        if host not in self.inventory:
            raise OrchestratorError('host %s does not exist' % host)

        if 'labels' not in self.inventory[host]:
            self.inventory[host]['labels'] = list()
        if label not in self.inventory[host]['labels']:
            self.inventory[host]['labels'].append(label)
        self._save_inventory()
        self.log.info('Added label %s to host %s' % (label, host))
        return 'Added label %s to host %s' % (label, host)

    @async_completion
    def remove_host_label(self, host, label):
        if host not in self.inventory:
            raise OrchestratorError('host %s does not exist' % host)

        if 'labels' not in self.inventory[host]:
            self.inventory[host]['labels'] = list()
        if label in self.inventory[host]['labels']:
            self.inventory[host]['labels'].remove(label)
        self._save_inventory()
        self.log.info('Removed label %s to host %s' % (label, host))
        return 'Removed label %s from host %s' % (label, host)

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
            sd.daemon_type = d['name'].split('.')[0]
            sd.daemon_id = '.'.join(d['name'].split('.')[1:])
            sd.hostname = host
            sd.container_id = d.get('container_id')
            sd.container_image_name = d.get('container_image_name')
            sd.container_image_id = d.get('container_image_id')
            sd.version = d.get('version')
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
        data = json.loads(''.join(out))
        self.log.debug('Refreshed host %s devices (%d)' % (host, len(data)))
        devices = inventory.Devices.from_json(data)
        self.cache.update_host_devices(host, devices.devices)
        self.cache.save_host(host)
        return None

    def _get_spec_size(self, spec):
        if spec.placement.count:
            return spec.placement.count
        elif spec.placement.all_hosts:
            return len(self.inventory)
        elif spec.placement.label:
            return len(self._get_hosts(spec.placement.label))
        elif spec.placement.hosts:
            return len(spec.placement.hosts)
        # hmm!
        return 0

    def describe_service(self, service_type=None, service_name=None,
                         refresh=False):
        if refresh:
            # ugly sync path, FIXME someday perhaps?
            for host, hi in self.inventory.items():
                self._refresh_host_daemons(host)
        # <service_map>
        sm = {}  # type: Dict[str, orchestrator.ServiceDescription]
        for h, dm in self.cache.daemons.items():
            for name, dd in dm.items():
                if service_type and service_type != dd.daemon_type:
                    continue
                # <name> i.e. rgw.realm.zone
                n: str = dd.service_name()
                if service_name and service_name != n:
                    continue
                spec = None
                if dd.service_name() in self.spec_store.specs:
                    spec_presence = "present"
                    spec = self.spec_store.specs[dd.service_name()]
                else:
                    spec_presence = "absent"
                if dd.daemon_type == 'osd':
                    spec_presence = "not applicable"
                if n not in sm:
                    sm[n] = orchestrator.ServiceDescription(
                        service_name=n,
                        last_refresh=dd.last_refresh,
                        container_image_id=dd.container_image_id,
                        container_image_name=dd.container_image_name,
                        spec_presence=spec_presence,
                    )
                if spec:
                    sm[n].size = self._get_spec_size(spec)
                else:
                    sm[n].size += 1
                if dd.status == 1:
                    sm[n].running += 1
                if not sm[n].last_refresh or not dd.last_refresh or dd.last_refresh < sm[n].last_refresh:  # type: ignore
                    sm[n].last_refresh = dd.last_refresh
                if sm[n].container_image_id != dd.container_image_id:
                    sm[n].container_image_id = 'mix'
                if sm[n].container_image_name != dd.container_image_name:
                    sm[n].container_image_name = 'mix'
        return trivial_result([s for n, s in sm.items()])

    def list_daemons(self, daemon_type=None, daemon_id=None,
                     host=None, refresh=False):
        if refresh:
            # ugly sync path, FIXME someday perhaps?
            if host:
                self._refresh_host_daemons(host)
            else:
                for host, hi in self.inventory.items():
                    self._refresh_host_daemons(host)
        result = []
        for h, dm in self.cache.daemons.items():
            if host and h != host:
                continue
            for name, dd in dm.items():
                if daemon_type and daemon_type != dd.daemon_type:
                    continue
                if daemon_id and daemon_id != dd.daemon_id:
                    continue
                result.append(dd)
        return trivial_result(result)

    def service_action(self, action, service_name):
        args = []
        for host, dm in self.cache.daemons.items():
            for name, d in dm.items():
                if d.matches_service(service_name):
                    args.append((d.daemon_type, d.daemon_id,
                                 d.hostname, action))
        if not args:
            raise orchestrator.OrchestratorError(
                'Unable to find %s.%s.* daemon(s)' % (service_name))
        self.log.info('%s service %s' % (action, service_name))
        return self._daemon_action(args)

    @async_map_completion
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
        self.log.info('%s daemons %s' % (action,
                                         ['%s.%s' % (a[0], a[1]) for a in args]))
        return self._daemon_action(args)

    def remove_daemons(self, names, force):
        # type: (List[str], bool) -> orchestrator.Completion
        args = []
        for host, dm in self.cache.daemons.items():
            for name in names:
                if name in dm:
                    args.append((name, host, force))
        if not args:
            raise OrchestratorError('Unable to find daemon(s) %s' % (names))
        self.log.info('Remove daemons %s' % [a[0] for a in args])
        return self._remove_daemon(args)

    def remove_service(self, service_name, force=False):
        args = []
        for host, dm in self.cache.daemons.items():
            for name, d in dm.items():
                if d.matches_service(service_name):
                    args.append(
                        (d.name(), d.hostname, force)
                    )
                    self.spec_store.rm(d.service_name())
        if not args:
            raise OrchestratorError('Unable to find daemons in %s service' % (
                service_name))
        self.log.info('Remove service %s (daemons %s)' % (
            service_name, [a[0] for a in args]))
        return self._remove_daemon(args)

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
                for host, hi in self.inventory.items():
                    self._refresh_host_devices(host)

        result = []
        for host, dls in self.cache.devices.items():
            if host_filter and host not in host_filter.hosts:
                continue
            result.append(orchestrator.InventoryHost(host,
                                                     inventory.Devices(dls)))
        return trivial_result(result)

    def zap_device(self, host, path):
        self.log.info('Zap device %s:%s' % (host, path))
        out, err, code = self._run_cephadm(
            host, 'osd', 'ceph-volume',
            ['--', 'lvm', 'zap', '--destroy', path],
            error_ok=True)
        self.cache.invalidate_host_devices(host)
        if code:
            raise OrchestratorError('Zap failed: %s' % '\n'.join(out + err))
        return trivial_result('\n'.join(out + err))

    def blink_device_light(self, ident_fault, on, locs):
        @async_map_completion
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

    def get_osd_uuid_map(self):
        # type: () -> Dict[str,str]
        osd_map = self.get('osd_map')
        r = {}
        for o in osd_map['osds']:
            r[str(o['osd'])] = o['uuid']
        return r

    def call_inventory(self, hosts, drive_groups):
        def call_create(inventory):
            return self._prepare_deployment(hosts, drive_groups, inventory)

        return self.get_inventory().then(call_create)

    def create_osds(self, drive_groups):
        # type: (List[DriveGroupSpec]) -> AsyncCompletion
        return self.get_hosts().then(lambda hosts: self.call_inventory(hosts, drive_groups))

    def _prepare_deployment(self,
                            all_hosts,  # type: List[orchestrator.HostSpec]
                            drive_groups,  # type: List[DriveGroupSpec]
                            inventory_list  # type: List[orchestrator.InventoryHost]
                            ):
        # type: (...) -> orchestrator.Completion

        for drive_group in drive_groups:
            self.log.info("Processing DriveGroup {}".format(drive_group))
            # 1) use fn_filter to determine matching_hosts
            matching_hosts = drive_group.hosts([x.hostname for x in all_hosts])
            # 2) Map the inventory to the InventoryHost object
            # FIXME: lazy-load the inventory from a InventoryHost object;
            #        this would save one call to the inventory(at least externally)

            def _find_inv_for_host(hostname, inventory_list):
                # This is stupid and needs to be loaded with the host
                for _inventory in inventory_list:
                    if _inventory.name == hostname:
                        return _inventory
                raise OrchestratorError("No inventory found for host: {}".format(hostname))

            cmds = []
            # 3) iterate over matching_host and call DriveSelection and to_ceph_volume
            for host in matching_hosts:
                inventory_for_host = _find_inv_for_host(host, inventory_list)
                drive_selection = selector.DriveSelection(drive_group, inventory_for_host.devices)
                cmd = translate.to_ceph_volume(drive_group, drive_selection).run()
                if not cmd:
                    self.log.info("No data_devices, skipping DriveGroup: {}".format(drive_group.name))
                    continue
                cmds.append((host, cmd))

        return self._create_osd(cmds)

    @async_map_completion
    def _create_osd(self, host, cmd):

        self._require_hosts(host)

        # get bootstrap key
        ret, keyring, err = self.mon_command({
            'prefix': 'auth get',
            'entity': 'client.bootstrap-osd',
        })

        # generate config
        ret, config, err = self.mon_command({
            "prefix": "config generate-minimal-conf",
        })

        j = json.dumps({
            'config': config,
            'keyring': keyring,
        })

        before_osd_uuid_map = self.get_osd_uuid_map()

        split_cmd = cmd.split(' ')
        _cmd = ['--config-and-keyring', '-', '--']
        _cmd.extend(split_cmd)
        out, err, code = self._run_cephadm(
            host, 'osd', 'ceph-volume',
            _cmd,
            stdin=j)

        # check result
        out, err, code = self._run_cephadm(
            host, 'osd', 'ceph-volume',
            [
                '--',
                'lvm', 'list',
                '--format', 'json',
            ])
        osds_elems = json.loads('\n'.join(out))
        fsid = self._cluster_fsid
        osd_uuid_map = self.get_osd_uuid_map()
        for osd_id, osds in osds_elems.items():
            for osd in osds:
                if osd['tags']['ceph.cluster_fsid'] != fsid:
                    self.log.debug('mismatched fsid, skipping %s' % osd)
                    continue
                if osd_id in before_osd_uuid_map:
                    # this osd existed before we ran prepare
                    continue
                if osd_id not in osd_uuid_map:
                    self.log.debug('osd id %d does not exist in cluster' % osd_id)
                    continue
                if osd_uuid_map[osd_id] != osd['tags']['ceph.osd_fsid']:
                    self.log.debug('mismatched osd uuid (cluster has %s, osd '
                                   'has %s)' % (
                                       osd_uuid_map[osd_id],
                                       osd['tags']['ceph.osd_fsid']))
                    continue

                self._create_daemon(
                    'osd', osd_id, host,
                    osd_uuid_map=osd_uuid_map)

        self.cache.invalidate_host_devices(host)
        return "Created osd(s) on host '{}'".format(host)

    def _create_daemon(self, daemon_type, daemon_id, host,
                       keyring=None,
                       extra_args=None, extra_config=None,
                       reconfig=False,
                       osd_uuid_map=None):
        if not extra_args:
            extra_args = []
        name = '%s.%s' % (daemon_type, daemon_id)

        if daemon_type == 'prometheus':
            j = self._generate_prometheus_config()
            extra_args.extend(['--config-json', '-'])
        elif daemon_type == 'node-exporter':
            j = None
        elif daemon_type == 'grafana':
            j = self._generate_grafana_config()
            extra_args.extend(['--config-json', '-'])
        elif daemon_type == 'alertmanager':
            j = self._generate_alertmanager_config()
            extra_args.extend(['--config-json', '-'])
        else:
            # keyring
            if not keyring:
                if daemon_type == 'mon':
                    ename = 'mon.'
                else:
                    ename = '%s.%s' % (daemon_type, daemon_id)
                ret, keyring, err = self.mon_command({
                    'prefix': 'auth get',
                    'entity': ename,
                })

            # generate config
            ret, config, err = self.mon_command({
                "prefix": "config generate-minimal-conf",
            })
            if extra_config:
                config += extra_config

            if daemon_type != 'crash':
                # crash_keyring
                ret, crash_keyring, err = self.mon_command({
                    'prefix': 'auth get-or-create',
                    'entity': 'client.crash.%s' % host,
                    'caps': ['mon', 'profile crash',
                             'mgr', 'profile crash'],
                })
            else:
                crash_keyring = None

            j = json.dumps({
                'config': config,
                'keyring': keyring,
                'crash_keyring': crash_keyring,
            })
            extra_args.extend(['--config-and-keyrings', '-'])

            # osd deployments needs an --osd-uuid arg
            if daemon_type == 'osd':
                if not osd_uuid_map:
                    osd_uuid_map = self.get_osd_uuid_map()
                osd_uuid = osd_uuid_map.get(daemon_id, None)
                if not osd_uuid:
                    raise OrchestratorError('osd.%d not in osdmap' % daemon_id)
                extra_args.extend(['--osd-fsid', osd_uuid])

        if reconfig:
            extra_args.append('--reconfig')

        out, err, code = self._run_cephadm(
            host, name, 'deploy',
            [
                '--name', name,
            ] + extra_args,
            stdin=j)
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
        return "{} {} on host '{}'".format(
            'Reconfigured' if reconfig else 'Deployed', name, host)

    @async_map_completion
    def _remove_daemon(self, name, host, force=False):
        """
        Remove a daemon
        """
        args = ['--name', name]
        if force:
            args.extend(['--force'])
        out, err, code = self._run_cephadm(
            host, name, 'rm-daemon', args)
        if not code:
            # remove item from cache
            self.cache.rm_daemon(host, name)
        self.cache.invalidate_host_daemons(host)
        return "Removed {} from host '{}'".format(name, host)

    def _apply_service(self, spec):
        """
        Schedule a service.  Deploy new daemons or remove old ones, depending
        on the target label and count specified in the placement.
        """
        daemon_type = spec.service_type
        create_fns = {
            'mon': self._create_mon,
            'mgr': self._create_mgr,
            'mds': self._create_mds,
            'rgw': self._create_rgw,
            'rbd-mirror': self._create_rbd_mirror,
            'grafana': self._create_grafana,
            'alertmanager': self._create_alertmanager,
            'prometheus': self._create_prometheus,
            'node-exporter': self._create_node_exporter,
        }
        config_fns = {
            'mds': self._config_mds,
            'rgw': self._config_rgw,
        }
        create_func = create_fns.get(daemon_type, None)
        if not create_func:
            self.log.debug('unrecognized service type %s' % daemon_type)
            return trivial_result([])
        config_func = config_fns.get(daemon_type, None)

        service_name = spec.service_name()
        self.log.debug('Applying service %s spec' % service_name)
        daemons = self.cache.get_daemons_by_service(service_name)
        spec = HostAssignment(
            spec=spec,
            get_hosts_func=self._get_hosts,
            service_type=daemon_type).load()
        if len(daemons) > spec.placement.count:
            # remove some
            to_remove = len(daemons) - spec.placement.count
            args = []
            for d in daemons[0:to_remove]:
                args.append(
                    ('%s.%s' % (d.daemon_type, d.daemon_id), d.hostname)
                )
            return self._remove_daemon(args)
        elif len(daemons) < spec.placement.count:
            # add some
            spec.placement.count -= len(daemons)
            hosts_with_daemons = {d.hostname for d in daemons}
            hosts_without_daemons = {p for p in spec.placement.hosts
                                     if p.hostname not in hosts_with_daemons}
            spec.placement.hosts = hosts_without_daemons
            return self._create_daemons(daemon_type, spec, daemons,
                                        create_func, config_func)
        return trivial_result([])

    def _apply_all_services(self):
        r : List[orchestrator.Completion] = []
        for sn, spec in self.spec_store.specs.items():
            try:
                r.extend(self._apply_service(spec))
            except Exception as e:
                self.log.warning('Failed to apply %s spec %s: %s' % (
                    spec.service_name(), spec, e))
        return r

    def _add_daemon(self, daemon_type, spec,
                    create_func, config_func=None):
        """
        Add (and place) a daemon. Require explicit host placement.  Do not
        schedule, and do not apply the related scheduling limitations.
        """
        self.log.debug('_add_daemon %s spec %s' % (daemon_type, spec.placement))
        if not spec.placement.hosts:
            raise OrchestratorError('must specify host(s) to deploy on')
        if not spec.placement.count:
            spec.placement.count = len(spec.placement.hosts)
        daemons = self.cache.get_daemons_by_service(spec.service_name())
        return self._create_daemons(daemon_type, spec, daemons,
                                    create_func, config_func)

    def _create_daemons(self, daemon_type, spec, daemons,
                        create_func, config_func=None):
        if spec.placement.count > len(spec.placement.hosts):
            raise OrchestratorError('too few hosts: want %d, have %s' % (
                spec.placement.count, spec.placement.hosts))

        if config_func:
            config_func(spec)

        args = [] # type: ignore
        for host, network, name in spec.placement.hosts:
            daemon_id = self.get_unique_name(daemon_type, host, daemons,
                                             spec.service_id, name)
            self.log.debug('Placing %s.%s on host %s' % (
                daemon_type, daemon_id, host))
            if daemon_type == 'mon':
                args.append((daemon_id, host, network))  # type: ignore
            else:
                args.append((daemon_id, host))  # type: ignore

            # add to daemon list so next name(s) will also be unique
            sd = orchestrator.DaemonDescription(
                hostname=host,
                daemon_type=daemon_type,
                daemon_id=daemon_id,
            )
            daemons.append(sd)
        return create_func(args)

    @async_map_completion
    def _create_mon(self, name, host, network):
        """
        Create a new monitor on the given host.
        """
        # get mon. key
        ret, keyring, err = self.mon_command({
            'prefix': 'auth get',
            'entity': 'mon.',
        })

        # infer whether this is a CIDR network, addrvec, or plain IP
        extra_config = '[mon.%s]\n' % name
        if '/' in network:
            extra_config += 'public network = %s\n' % network
        elif network.startswith('[v') and network.endswith(']'):
            extra_config += 'public addrv = %s\n' % network
        elif ':' not in network:
            extra_config += 'public addr = %s\n' % network
        else:
            raise RuntimeError('Must specify a CIDR network, ceph addrvec, or plain IP: \'%s\'' % network)

        return self._create_daemon('mon', name, host,
                                   keyring=keyring,
                                   extra_config=extra_config)

    def add_mon(self, spec):
        # type: (orchestrator.ServiceSpec) -> orchestrator.Completion
        # current support requires a network to be specified
        orchestrator.servicespec_validate_hosts_have_network_spec(spec)
        return self._add_daemon('mon', spec, self._create_mon)

    @async_map_completion
    def _create_mgr(self, mgr_id, host):
        """
        Create a new manager instance on a host.
        """
        # get mgr. key
        ret, keyring, err = self.mon_command({
            'prefix': 'auth get-or-create',
            'entity': 'mgr.%s' % mgr_id,
            'caps': ['mon', 'profile mgr',
                     'osd', 'allow *',
                     'mds', 'allow *'],
        })

        return self._create_daemon('mgr', mgr_id, host, keyring=keyring)

    def add_mgr(self, spec):
        # type: (orchestrator.ServiceSpec) -> orchestrator.Completion
        return self._add_daemon('mgr', spec, self._create_mgr)

    def _apply(self, spec):
        self.log.info('Saving service %s spec' % spec.service_name())
        self.spec_store.save(spec)
        self._kick_serve_loop()
        return trivial_result("Scheduled %s update..." % spec.service_type)

    def apply_mgr(self, spec):
        return self._apply(spec)

    def add_mds(self, spec):
        # type: (orchestrator.ServiceSpec) -> AsyncCompletion
        return self._add_daemon('mds', spec, self._create_mds, self._config_mds)

    def apply_mds(self, spec: orchestrator.ServiceSpec) -> orchestrator.Completion:
        return self._apply(spec)

    def _config_mds(self, spec):
        # ensure mds_join_fs is set for these daemons
        assert spec.service_id
        ret, out, err = self.mon_command({
            'prefix': 'config set',
            'who': 'mds.' + spec.service_id,
            'name': 'mds_join_fs',
            'value': spec.service_id,
        })

    @async_map_completion
    def _create_mds(self, mds_id, host):
        # get mgr. key
        ret, keyring, err = self.mon_command({
            'prefix': 'auth get-or-create',
            'entity': 'mds.' + mds_id,
            'caps': ['mon', 'profile mds',
                     'osd', 'allow rwx',
                     'mds', 'allow'],
        })
        return self._create_daemon('mds', mds_id, host, keyring=keyring)

    def add_rgw(self, spec):
        return self._add_daemon('rgw', spec, self._create_rgw, self._config_rgw)

    def _config_rgw(self, spec):
        # ensure rgw_realm and rgw_zone is set for these daemons
        ret, out, err = self.mon_command({
            'prefix': 'config set',
            'who': 'client.rgw.' + spec.service_id,
            'name': 'rgw_zone',
            'value': spec.rgw_zone,
        })
        ret, out, err = self.mon_command({
            'prefix': 'config set',
            'who': 'client.rgw.' + spec.rgw_realm,
            'name': 'rgw_realm',
            'value': spec.rgw_realm,
        })

    @async_map_completion
    def _create_rgw(self, rgw_id, host):
        ret, keyring, err = self.mon_command({
            'prefix': 'auth get-or-create',
            'entity': 'client.rgw.' + rgw_id,
            'caps': ['mon', 'allow rw',
                     'mgr', 'allow rw',
                     'osd', 'allow rwx'],
        })
        return self._create_daemon('rgw', rgw_id, host, keyring=keyring)

    def apply_rgw(self, spec):
        return self._apply(spec)

    def add_rbd_mirror(self, spec):
        return self._add_daemon('rbd-mirror', spec, self._create_rbd_mirror)

    @async_map_completion
    def _create_rbd_mirror(self, daemon_id, host):
        ret, keyring, err = self.mon_command({
            'prefix': 'auth get-or-create',
            'entity': 'client.rbd-mirror.' + daemon_id,
            'caps': ['mon', 'profile rbd-mirror',
                     'osd', 'profile rbd'],
        })
        return self._create_daemon('rbd-mirror', daemon_id, host,
                                   keyring=keyring)

    def apply_rbd_mirror(self, spec):
        return self._apply(spec)

    def _generate_prometheus_config(self):
        # scrape mgrs
        mgr_scrape_list = []
        mgr_map = self.get('mgr_map')
        t = mgr_map.get('services', {}).get('prometheus', None)
        if t:
            t = t.split('/')[2]
            mgr_scrape_list.append(t)
            port = '9283'
            if ':' in t:
                port = t.split(':')[1]
            # get standbys too.  assume that they are all on the same port
            # as the active.
            for dd in self.cache.get_daemons_by_service('mgr'):
                if dd.daemon_id == self.get_mgr_id():
                    continue
                hi = self.inventory.get(dd.hostname, None)
                if hi:
                    addr = hi.get('addr', dd.hostname)
                mgr_scrape_list.append(addr.split(':')[0] + ':' + port)

        # scrape node exporters
        node_configs = ''
        for dd in self.cache.get_daemons_by_service('node-exporter'):
            hi = self.inventory.get(dd.hostname, None)
            if hi:
                addr = hi.get('addr', dd.hostname)
                if not node_configs:
                    node_configs = """
  - job_name: 'node'
    static_configs:
"""
                node_configs += """    - targets: {}
      labels:
        instance: '{}'
""".format([addr.split(':')[0] + ':9100'],
           dd.hostname)

        # scrape alert managers
        alertmgr_configs = ""
        alertmgr_targets = []
        for dd in self.cache.get_daemons_by_service('alertmanager'):
            hi = self.inventory.get(dd.hostname, None)
            if hi is None:
                continue
            addr = hi.get('addr', dd.hostname)
            alertmgr_targets.append("'{}:9093'".format(addr.split(':')[0]))
        if alertmgr_targets:
            alertmgr_configs = """alerting:
  alertmanagers:
    - scheme: http
      path_prefix: /alertmanager
      static_configs:
        - targets: [{}]
""".format(", ".join(alertmgr_targets))

        # generate the prometheus configuration
        j = json.dumps({
            'files': {
                'prometheus.yml': """# generated by cephadm
global:
  scrape_interval: 5s
  evaluation_interval: 10s
rule_files:
  - /etc/prometheus/alerting/*
{alertmgr_configs}
scrape_configs:
  - job_name: 'ceph'
    static_configs:
    - targets: {mgr_scrape_list}
      labels:
        instance: 'ceph_cluster'
{node_configs}
""".format(
    mgr_scrape_list=str(mgr_scrape_list),
    node_configs=str(node_configs),
    alertmgr_configs=str(alertmgr_configs)
    ),
            },
        })
        return j

    def _generate_grafana_config(self):
        def generate_grafana_ds_config(hosts: List[str]) -> str:
            config = '''# generated by cephadm
deleteDatasources:
{delete_data_sources}

datasources:
{data_sources}
'''
            delete_ds_template = '''
  - name: '{name}'
    orgId: 1\n'''.lstrip('\n')
            ds_template = '''
  - name: '{name}'
    type: 'prometheus'
    access: 'proxy'
    orgId: 1
    url: 'http://{host}:9095'
    basicAuth: false
    isDefault: {is_default}
    editable: false\n'''.lstrip('\n')

            delete_data_sources = ''
            data_sources = ''
            for i, host in enumerate(hosts):
                name = "Dashboard %d" % (i + 1)
                data_sources += ds_template.format(
                    name=name,
                    host=host,
                    is_default=str(i == 0).lower()
                )
                delete_data_sources += delete_ds_template.format(
                    name=name
                )
            return config.format(
                delete_data_sources=delete_data_sources,
                data_sources=data_sources,
            )

        prom_services = [ps.hostname for ps in self.cache.get_daemons_by_service('prometheus')]
        cert, pkey = create_self_signed_cert('Ceph', 'cephadm')
        config_file = json.dumps({
            'files': {
                "grafana.ini": """# generated by cephadm
[users]
  default_theme = light
[auth.anonymous]
  enabled = true
  org_name = 'Main Org.'
  org_role = 'Viewer'
[server]
  domain = 'bootstrap.storage.lab'
  protocol = https
  cert_file = /etc/grafana/certs/cert_file
  cert_key = /etc/grafana/certs/cert_key
  http_port = 3000
  http_addr = localhost
[security]
  admin_user = admin
  admin_password = admin
  allow_embedding = true
""",
                'provisioning/datasources/ceph-dashboard.yml': generate_grafana_ds_config(prom_services),
                'certs/cert_file': '# generated by cephadm\n%s' % cert,
                'certs/cert_key': '# generated by cephadm\n%s' % pkey,
            }
        })
        return config_file

    def _get_dashboard_url(self):
        # type: () -> str
        return self.get('mgr_map').get('services', {}).get('dashboard', '')

    def _generate_alertmanager_config(self):
        # type: () -> str
        yml = """# generated by cephadm
# See https://prometheus.io/docs/alerting/configuration/ for documentation.

global:
  resolve_timeout: 5m

route:
  group_by: ['alertname']
  group_wait: 10s
  group_interval: 10s
  repeat_interval: 1h
  receiver: 'ceph-dashboard'
receivers:
- name: 'ceph-dashboard'
  webhook_configs:
  - url: '{url}/api/prometheus_receiver'
        """.format(url=self._get_dashboard_url())
        peers = []
        port = '9094'
        for dd in self.cache.get_daemons_by_service('alertmanager'):
            if dd.daemon_id == self.get_mgr_id():
                continue
            hi = self.inventory.get(dd.hostname, None)
            addr = hi.get('addr', dd.hostname) if hi else ""
            peers.append(addr.split(':')[0] + ':' + port)
        return json.dumps({
            "files": {
                "alertmanager.yml": yml
            },
            "peers": peers
        })

    def add_prometheus(self, spec):
        return self._add_daemon('prometheus', spec, self._create_prometheus)

    @async_map_completion
    def _create_prometheus(self, daemon_id, host):
        return self._create_daemon('prometheus', daemon_id, host)

    def apply_prometheus(self, spec):
        return self._apply(spec)

    def add_node_exporter(self, spec):
        # type: (orchestrator.ServiceSpec) -> AsyncCompletion
        return self._add_daemon('node-exporter', spec,
                                self._create_node_exporter)

    def apply_node_exporter(self, spec):
        return self._apply(spec)

    @async_map_completion
    def _create_node_exporter(self, daemon_id, host):
        return self._create_daemon('node-exporter', daemon_id, host)

    def add_grafana(self, spec):
        # type: (orchestrator.ServiceSpec) -> AsyncCompletion
        return self._add_daemon('grafana', spec, self._create_grafana)

    def apply_grafana(self, spec):
        # type: (orchestrator.ServiceSpec) -> AsyncCompletion
        return self._apply(spec)

    @async_map_completion
    def _create_grafana(self, daemon_id, host):
        return self._create_daemon('grafana', daemon_id, host)

    def add_alertmanager(self, spec):
        # type: (orchestrator.ServiceSpec) -> AsyncCompletion
        return self._add_daemon('alertmanager', spec, self._create_alertmanager)

    def apply_alertmanager(self, spec):
        # type: (orchestrator.ServiceSpec) -> AsyncCompletion
        return self._apply(spec)

    @async_map_completion
    def _create_alertmanager(self, daemon_id, host):
        return self._create_daemon('alertmanager', daemon_id, host)


    def _get_container_image_id(self, image_name):
        # pick a random host...
        host = None
        for host_name, hi in self.inventory.items():
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

    def upgrade_status(self):
        r = orchestrator.UpgradeStatusSpec()
        if self.upgrade_state:
            r.target_image = self.upgrade_state.get('target_name')
            r.in_progress = True
            if self.upgrade_state.get('error'):
                r.message = 'Error: ' + self.upgrade_state.get('error')
            elif self.upgrade_state.get('paused'):
                r.message = 'Upgrade paused'
        return trivial_result(r)

    def upgrade_start(self, image, version):
        if version:
            target_name = self.container_image_base + ':v' + version
        elif image:
            target_name = image
        else:
            raise OrchestratorError('must specify either image or version')
        if self.upgrade_state:
            if self.upgrade_state.get('target_name') != target_name:
                raise OrchestratorError(
                    'Upgrade to %s (not %s) already in progress' %
                (self.upgrade_state.get('target_name'), target_name))
            if self.upgrade_state.get('paused'):
                del self.upgrade_state['paused']
                self._save_upgrade_state()
                return trivial_result('Resumed upgrade to %s' %
                                      self.upgrade_state.get('target_name'))
            return trivial_result('Upgrade to %s in progress' %
                                  self.upgrade_state.get('target_name'))
        self.upgrade_state = {
            'target_name': target_name,
            'progress_id': str(uuid.uuid4()),
        }
        self._update_upgrade_progress(0.0)
        self._save_upgrade_state()
        self._clear_upgrade_health_checks()
        self.event.set()
        return trivial_result('Initiating upgrade to %s' % (image))

    def upgrade_pause(self):
        if not self.upgrade_state:
            raise OrchestratorError('No upgrade in progress')
        if self.upgrade_state.get('paused'):
            return trivial_result('Upgrade to %s already paused' %
                                  self.upgrade_state.get('target_name'))
        self.upgrade_state['paused'] = True
        self._save_upgrade_state()
        return trivial_result('Paused upgrade to %s' %
                              self.upgrade_state.get('target_name'))

    def upgrade_resume(self):
        if not self.upgrade_state:
            raise OrchestratorError('No upgrade in progress')
        if not self.upgrade_state.get('paused'):
            return trivial_result('Upgrade to %s not paused' %
                                  self.upgrade_state.get('target_name'))
        del self.upgrade_state['paused']
        self._save_upgrade_state()
        self.event.set()
        return trivial_result('Resumed upgrade to %s' %
                              self.upgrade_state.get('target_name'))

    def upgrade_stop(self):
        if not self.upgrade_state:
            return trivial_result('No upgrade in progress')
        target_name = self.upgrade_state.get('target_name')
        if 'progress_id' in self.upgrade_state:
            self.remote('progress', 'complete',
                        self.upgrade_state['progress_id'])
        self.upgrade_state = None
        self._save_upgrade_state()
        self._clear_upgrade_health_checks()
        self.event.set()
        return trivial_result('Stopped upgrade to %s' % target_name)

    def remove_osds(self, osd_ids: List[str],
                    replace: bool = False,
                    force: bool = False) -> orchestrator.Completion:
        """
        Takes a list of OSDs and schedules them for removal.
        The function that takes care of the actual removal is
        _remove_osds_bg().
        """

        daemons = self.cache.get_daemons_by_service('osd')
        found = set()
        for daemon in daemons:
            if daemon.daemon_id not in osd_ids:
                continue
            found.add(OSDRemoval(daemon.daemon_id, replace, force,
                                 daemon.hostname, daemon.name(),
                                 datetime.datetime.utcnow()))

        not_found: set = {osd_id for osd_id in osd_ids if osd_id not in [x.osd_id for x in found]}
        if not_found:
            raise OrchestratorError('Unable to find OSD: %s' % not_found)

        for osd in found:
            self.to_remove_osds.add(osd)
            # trigger the serve loop to initiate the removal
        self._kick_serve_loop()
        return trivial_result(f"Scheduled OSD(s) for removal")

    def _remove_osds_bg(self) -> None:
        """
        Performs actions in the _serve() loop to remove an OSD
        when criteria is met.
        """
        self.log.debug(f"{len(self.to_remove_osds)} OSDs are scheduled for removal: {list(self.to_remove_osds)}")
        self.osd_removal_report = self._generate_osd_removal_status()
        remove_osds: set = self.to_remove_osds.copy()
        for osd in remove_osds:
            if not osd.force:
                self.rm_util.drain_osd(osd.osd_id)
                # skip criteria
                if not self.rm_util.is_empty(osd.osd_id):
                    self.log.info(f"OSD <{osd.osd_id}> is not empty yet. Waiting a bit more")
                    continue

            if not self.rm_util.ok_to_destroy([osd.osd_id]):
                self.log.info(f"OSD <{osd.osd_id}> is not safe-to-destroy yet. Waiting a bit more")
                continue

            # abort criteria
            if not self.rm_util.down_osd([osd.osd_id]):
                # also remove it from the remove_osd list and set a health_check warning?
                raise orchestrator.OrchestratorError(f"Could not set OSD <{osd.osd_id}> to 'down'")

            if osd.replace:
                if not self.rm_util.destroy_osd(osd.osd_id):
                    # also remove it from the remove_osd list and set a health_check warning?
                    raise orchestrator.OrchestratorError(f"Could not destroy OSD <{osd.osd_id}>")
            else:
                if not self.rm_util.purge_osd(osd.osd_id):
                    # also remove it from the remove_osd list and set a health_check warning?
                    raise orchestrator.OrchestratorError(f"Could not purge OSD <{osd.osd_id}>")

            completion = self._remove_daemon([(osd.fullname, osd.nodename, True)])
            completion.add_progress('Removing OSDs', self)
            completion.update_progress = True
            if completion:
                while not completion.has_result:
                    self.process([completion])
                    if completion.needs_result:
                        time.sleep(1)
                    else:
                        break
                if completion.exception is not None:
                    self.log.error(str(completion.exception))
            else:
                raise orchestrator.OrchestratorError("Did not receive a completion from _remove_daemon")

            self.log.info(f"Successfully removed removed OSD <{osd.osd_id}> on {osd.nodename}")
            self.log.debug(f"Removing {osd.osd_id} from the queue.")
            self.to_remove_osds.remove(osd)

    def _generate_osd_removal_status(self) -> Dict[Any, object]:
        """
        Generate a OSD report that can be printed to the CLI
        """
        self.log.debug("Assembling report for osd rm status")
        report = {}
        for osd in self.to_remove_osds:
            pg_count = self.rm_util.get_pg_count(osd.osd_id)
            report[osd] = pg_count if pg_count != -1 else 'n/a'
        self.log.debug(f"Reporting: {report}")
        return report

    def remove_osds_status(self) -> orchestrator.Completion:
        """
        The CLI call to retrieve an osd removal report
        """
        return trivial_result(self.osd_removal_report)

    def list_specs(self) -> orchestrator.Completion:
        """
        Loads all entries from the service_spec mon_store root.
        """
        specs = list()
        for service_name, spec in self.spec_store.specs.items():
            specs.append('---')
            specs.append(yaml.dump(spec))
        return trivial_result(specs)

    def apply_service_config(self, spec_document: str) -> orchestrator.Completion:
        """
        Parse a multi document yaml file (represented in a inbuf object)
        and loads it with it's respective ServiceSpec to validate the
        initial input.
        If no errors are raised, save them.
        """
        content: Iterator[Any] = yaml.load_all(spec_document)
        # Load all specs from a multi document yaml file.
        loaded_specs: List[ServiceSpec] = list()
        for spec in content:
            # load ServiceSpec once to validate
            spec_o = ServiceSpec.from_json(spec)
            loaded_specs.append(spec_o)
        for spec in loaded_specs:
            self.spec_store.save(spec)
        self._kick_serve_loop()
        return trivial_result("ServiceSpecs saved")


class BaseScheduler(object):
    """
    Base Scheduler Interface

    * requires a placement_spec

    `place(host_pool)` needs to return a List[HostPlacementSpec, ..]
    """

    def __init__(self, placement_spec):
        # type: (orchestrator.PlacementSpec) -> None
        self.placement_spec = placement_spec

    def place(self, host_pool, count=None):
        # type: (List, Optional[int]) -> List[HostPlacementSpec]
        raise NotImplementedError


class SimpleScheduler(BaseScheduler):
    """
    The most simple way to pick/schedule a set of hosts.
    1) Shuffle the provided host_pool
    2) Select from list up to :count
    """
    def __init__(self, placement_spec):
        super(SimpleScheduler, self).__init__(placement_spec)

    def place(self, host_pool, count=None):
        # type: (List, Optional[int]) -> List[HostPlacementSpec]
        if not host_pool:
            raise Exception('List of host candidates is empty')
        host_pool = [HostPlacementSpec(x, '', '') for x in host_pool]
        # shuffle for pseudo random selection
        random.shuffle(host_pool)
        return host_pool[:count]


class HostAssignment(object):
    """
    A class to detect if hosts are being passed imperative or declarative
    If the spec is populated via the `hosts/hosts` field it will not load
    any hosts into the list.
    If the spec isn't populated, i.e. when only num or label is present (declarative)
    it will use the provided `get_host_func` to load it from the inventory.

    Schedulers can be assigned to pick hosts from the pool.
    """

    def __init__(self,
                 spec=None,  # type: Optional[orchestrator.ServiceSpec]
                 scheduler=None,  # type: Optional[BaseScheduler]
                 get_hosts_func=None,  # type: Optional[Callable[[Optional[str]],List[str]]]
                 service_type=None,  # type: Optional[str]
                 ):
        assert spec and get_hosts_func and service_type
        self.spec = spec  # type: orchestrator.ServiceSpec
        self.scheduler = scheduler if scheduler else SimpleScheduler(self.spec.placement)
        self.get_hosts_func = get_hosts_func
        self.daemon_type = service_type

    def load(self):
        # type: () -> orchestrator.ServiceSpec
        """
        Load hosts into the spec.placement.hosts container.
        """
        self.load_labeled_hosts()
        self.assign_hosts()
        return self.spec

    def load_labeled_hosts(self):
        # type: () -> None
        """
        Assign hosts based on their label
        """
        if self.spec.placement.label:
            logger.info("Matching label '%s'" % self.spec.placement.label)
            candidates = [
                HostPlacementSpec(x, '', '')
                for x in self.get_hosts_func(self.spec.placement.label)
            ]
            logger.info('Assigning hostss to spec: {}'.format(candidates))
            self.spec.placement.set_hosts(candidates)

    def assign_hosts(self):
        # type: () -> None
        """
        Use the assigned scheduler to load hosts into the spec.placement.hosts container
        """
        # If no imperative or declarative host assignments, use the scheduler to pick from the
        # host pool (assuming `count` is set)
        if not self.spec.placement.label and not self.spec.placement.hosts and self.spec.placement.count:
            logger.info("Found num spec. Looking for labeled hosts.")
            # TODO: actually query for labels (self.daemon_type)
            candidates = self.scheduler.place([x for x in self.get_hosts_func(None)],
                                              count=self.spec.placement.count)
            # Not enough hosts to deploy on
            if len(candidates) != self.spec.placement.count:
                logger.warning("Did not find enough labeled hosts to \
                               scale to <{}> services. Falling back to unlabeled hosts.".
                               format(self.spec.placement.count))
            else:
                logger.info('Assigning hosts to spec: {}'.format(candidates))
                self.spec.placement.set_hosts(candidates)
                return None

            candidates = self.scheduler.place([x for x in self.get_hosts_func(None)], count=self.spec.placement.count)
            # Not enough hosts to deploy on
            if len(candidates) != self.spec.placement.count:
                raise OrchestratorValidationError("Cannot place {} daemons on {} hosts.".
                                                  format(self.spec.placement.count, len(candidates)))

            logger.info('Assigning hosts to spec: {}'.format(candidates))
            self.spec.placement.set_hosts(candidates)
            return None
