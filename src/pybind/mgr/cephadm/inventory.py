import datetime
from copy import copy
import json
import logging
from typing import TYPE_CHECKING, Dict, List, Iterator, Optional, Any, Tuple, Set, Mapping, cast, \
    NamedTuple

import orchestrator
from ceph.deployment import inventory
from ceph.deployment.service_spec import ServiceSpec
from ceph.utils import str_to_datetime, datetime_to_str, datetime_now
from orchestrator import OrchestratorError, HostSpec, OrchestratorEvent, service_to_daemon_types

if TYPE_CHECKING:
    from .module import CephadmOrchestrator


logger = logging.getLogger(__name__)

HOST_CACHE_PREFIX = "host."
SPEC_STORE_PREFIX = "spec."


class Inventory:
    """
    The inventory stores a HostSpec for all hosts persistently.
    """

    def __init__(self, mgr: 'CephadmOrchestrator'):
        self.mgr = mgr
        # load inventory
        i = self.mgr.get_store('inventory')
        if i:
            self._inventory: Dict[str, dict] = json.loads(i)
            # handle old clusters missing 'hostname' key from hostspec
            for k, v in self._inventory.items():
                if 'hostname' not in v:
                    v['hostname'] = k
        else:
            self._inventory = dict()
        logger.debug('Loaded inventory %s' % self._inventory)

    def keys(self) -> List[str]:
        return list(self._inventory.keys())

    def __contains__(self, host: str) -> bool:
        return host in self._inventory

    def assert_host(self, host: str) -> None:
        if host not in self._inventory:
            raise OrchestratorError('host %s does not exist' % host)

    def add_host(self, spec: HostSpec) -> None:
        self._inventory[spec.hostname] = spec.to_json()
        self.save()

    def rm_host(self, host: str) -> None:
        self.assert_host(host)
        del self._inventory[host]
        self.save()

    def set_addr(self, host: str, addr: str) -> None:
        self.assert_host(host)
        self._inventory[host]['addr'] = addr
        self.save()

    def add_label(self, host: str, label: str) -> None:
        self.assert_host(host)

        if 'labels' not in self._inventory[host]:
            self._inventory[host]['labels'] = list()
        if label not in self._inventory[host]['labels']:
            self._inventory[host]['labels'].append(label)
        self.save()

    def rm_label(self, host: str, label: str) -> None:
        self.assert_host(host)

        if 'labels' not in self._inventory[host]:
            self._inventory[host]['labels'] = list()
        if label in self._inventory[host]['labels']:
            self._inventory[host]['labels'].remove(label)
        self.save()

    def get_addr(self, host: str) -> str:
        self.assert_host(host)
        return self._inventory[host].get('addr', host)

    def filter_by_label(self, label: Optional[str] = '', as_hostspec: bool = False) -> Iterator:
        for h, hostspec in self._inventory.items():
            if not label or label in hostspec.get('labels', []):
                if as_hostspec:
                    yield self.spec_from_dict(hostspec)
                else:
                    yield h

    def spec_from_dict(self, info: dict) -> HostSpec:
        hostname = info['hostname']
        return HostSpec(
            hostname,
            addr=info.get('addr', hostname),
            labels=info.get('labels', []),
            status='Offline' if hostname in self.mgr.offline_hosts else info.get('status', ''),
        )

    def all_specs(self) -> List[HostSpec]:
        return list(map(self.spec_from_dict, self._inventory.values()))

    def get_host_with_state(self, state: str = "") -> List[str]:
        """return a list of host names in a specific state"""
        return [h for h in self._inventory if self._inventory[h].get("status", "").lower() == state]

    def save(self) -> None:
        self.mgr.set_store('inventory', json.dumps(self._inventory))


class SpecDescription(NamedTuple):
    spec: ServiceSpec
    created: datetime.datetime
    deleted: Optional[datetime.datetime]


class SpecStore():
    def __init__(self, mgr):
        # type: (CephadmOrchestrator) -> None
        self.mgr = mgr
        self._specs = {}  # type: Dict[str, ServiceSpec]
        self.spec_created = {}  # type: Dict[str, datetime.datetime]
        self.spec_deleted = {}  # type: Dict[str, datetime.datetime]
        self.spec_preview = {}  # type: Dict[str, ServiceSpec]

    @property
    def all_specs(self) -> Mapping[str, ServiceSpec]:
        """
        returns active and deleted specs. Returns read-only dict.
        """
        return self._specs

    def __contains__(self, name: str) -> bool:
        return name in self._specs

    def __getitem__(self, name: str) -> SpecDescription:
        if name not in self._specs:
            raise OrchestratorError(f'Service {name} not found.')
        return SpecDescription(self._specs[name],
                               self.spec_created[name],
                               self.spec_deleted.get(name, None))

    @property
    def active_specs(self) -> Mapping[str, ServiceSpec]:
        return {k: v for k, v in self._specs.items() if k not in self.spec_deleted}

    def load(self):
        # type: () -> None
        for k, v in self.mgr.get_store_prefix(SPEC_STORE_PREFIX).items():
            service_name = k[len(SPEC_STORE_PREFIX):]
            try:
                j = cast(Dict[str, dict], json.loads(v))
                spec = ServiceSpec.from_json(j['spec'])
                created = str_to_datetime(cast(str, j['created']))
                self._specs[service_name] = spec
                self.spec_created[service_name] = created

                if 'deleted' in v:
                    deleted = str_to_datetime(cast(str, j['deleted']))
                    self.spec_deleted[service_name] = deleted

                self.mgr.log.debug('SpecStore: loaded spec for %s' % (
                    service_name))
            except Exception as e:
                self.mgr.log.warning('unable to load spec for %s: %s' % (
                    service_name, e))
                pass

    def save(self, spec: ServiceSpec, update_create: bool = True) -> None:
        name = spec.service_name()
        if spec.preview_only:
            self.spec_preview[name] = spec
            return None
        self._specs[name] = spec

        if update_create:
            self.spec_created[name] = datetime_now()

        data = {
            'spec': spec.to_json(),
            'created': datetime_to_str(self.spec_created[name]),
        }
        if name in self.spec_deleted:
            data['deleted'] = datetime_to_str(self.spec_deleted[name])

        self.mgr.set_store(
            SPEC_STORE_PREFIX + name,
            json.dumps(data, sort_keys=True),
        )
        self.mgr.events.for_service(spec, OrchestratorEvent.INFO, 'service was created')

    def rm(self, service_name: str) -> bool:
        if service_name not in self._specs:
            return False

        if self._specs[service_name].preview_only:
            self.finally_rm(service_name)
            return True

        self.spec_deleted[service_name] = datetime_now()
        self.save(self._specs[service_name], update_create=False)
        return True

    def finally_rm(self, service_name):
        # type: (str) -> bool
        found = service_name in self._specs
        if found:
            del self._specs[service_name]
            del self.spec_created[service_name]
            if service_name in self.spec_deleted:
                del self.spec_deleted[service_name]
            self.mgr.set_store(SPEC_STORE_PREFIX + service_name, None)
        return found

    def get_created(self, spec: ServiceSpec) -> Optional[datetime.datetime]:
        return self.spec_created.get(spec.service_name())


class HostCache():
    """
    HostCache stores different things:

    1. `daemons`: Deployed daemons O(daemons)

    They're part of the configuration nowadays and need to be
    persistent. The name "daemon cache" is unfortunately a bit misleading.
    Like for example we really need to know where daemons are deployed on
    hosts that are offline.

    2. `devices`: ceph-volume inventory cache O(hosts)

    As soon as this is populated, it becomes more or less read-only.

    3. `networks`: network interfaces for each host. O(hosts)

    This is needed in order to deploy MONs. As this is mostly read-only.

    4. `last_etc_ceph_ceph_conf` O(hosts)

    Stores the last refresh time for the /etc/ceph/ceph.conf. Used
    to avoid deploying new configs when failing over to a new mgr.

    5. `scheduled_daemon_actions`: O(daemons)

    Used to run daemon actions after deploying a daemon. We need to
    store it persistently, in order to stay consistent across
    MGR failovers.
    """

    def __init__(self, mgr):
        # type: (CephadmOrchestrator) -> None
        self.mgr: CephadmOrchestrator = mgr
        self.daemons = {}   # type: Dict[str, Dict[str, orchestrator.DaemonDescription]]
        self.last_daemon_update = {}   # type: Dict[str, datetime.datetime]
        self.devices = {}              # type: Dict[str, List[inventory.Device]]
        self.facts = {}                # type: Dict[str, Dict[str, Any]]
        self.last_facts_update = {}    # type: Dict[str, datetime.datetime]
        self.osdspec_previews = {}     # type: Dict[str, List[Dict[str, Any]]]
        self.osdspec_last_applied = {}  # type: Dict[str, Dict[str, datetime.datetime]]
        self.networks = {}             # type: Dict[str, Dict[str, List[str]]]
        self.last_device_update = {}   # type: Dict[str, datetime.datetime]
        self.last_device_change = {}   # type: Dict[str, datetime.datetime]
        self.daemon_refresh_queue = []  # type: List[str]
        self.device_refresh_queue = []  # type: List[str]
        self.osdspec_previews_refresh_queue = []  # type: List[str]

        # host -> daemon name -> dict
        self.daemon_config_deps = {}   # type: Dict[str, Dict[str, Dict[str,Any]]]
        self.last_host_check = {}      # type: Dict[str, datetime.datetime]
        self.loading_osdspec_preview = set()  # type: Set[str]
        self.last_etc_ceph_ceph_conf: Dict[str, datetime.datetime] = {}
        self.registry_login_queue: Set[str] = set()

        self.scheduled_daemon_actions: Dict[str, Dict[str, str]] = {}

    def load(self):
        # type: () -> None
        for k, v in self.mgr.get_store_prefix(HOST_CACHE_PREFIX).items():
            host = k[len(HOST_CACHE_PREFIX):]
            if host not in self.mgr.inventory:
                self.mgr.log.warning('removing stray HostCache host record %s' % (
                    host))
                self.mgr.set_store(k, None)
            try:
                j = json.loads(v)
                if 'last_device_update' in j:
                    self.last_device_update[host] = str_to_datetime(j['last_device_update'])
                else:
                    self.device_refresh_queue.append(host)
                if 'last_device_change' in j:
                    self.last_device_change[host] = str_to_datetime(j['last_device_change'])
                # for services, we ignore the persisted last_*_update
                # and always trigger a new scrape on mgr restart.
                self.daemon_refresh_queue.append(host)
                self.daemons[host] = {}
                self.osdspec_previews[host] = []
                self.osdspec_last_applied[host] = {}
                self.devices[host] = []
                self.networks[host] = {}
                self.daemon_config_deps[host] = {}
                for name, d in j.get('daemons', {}).items():
                    self.daemons[host][name] = \
                        orchestrator.DaemonDescription.from_json(d)
                for d in j.get('devices', []):
                    self.devices[host].append(inventory.Device.from_json(d))
                self.networks[host] = j.get('networks', {})
                self.osdspec_previews[host] = j.get('osdspec_previews', {})
                for name, ts in j.get('osdspec_last_applied', {}).items():
                    self.osdspec_last_applied[host][name] = str_to_datetime(ts)

                for name, d in j.get('daemon_config_deps', {}).items():
                    self.daemon_config_deps[host][name] = {
                        'deps': d.get('deps', []),
                        'last_config': str_to_datetime(d['last_config']),
                    }
                if 'last_host_check' in j:
                    self.last_host_check[host] = str_to_datetime(j['last_host_check'])
                if 'last_etc_ceph_ceph_conf' in j:
                    self.last_etc_ceph_ceph_conf[host] = str_to_datetime(
                        j['last_etc_ceph_ceph_conf'])
                self.registry_login_queue.add(host)
                self.scheduled_daemon_actions[host] = j.get('scheduled_daemon_actions', {})

                self.mgr.log.debug(
                    'HostCache.load: host %s has %d daemons, '
                    '%d devices, %d networks' % (
                        host, len(self.daemons[host]), len(self.devices[host]),
                        len(self.networks[host])))
            except Exception as e:
                self.mgr.log.warning('unable to load cached state for %s: %s' % (
                    host, e))
                pass

    def update_host_daemons(self, host, dm):
        # type: (str, Dict[str, orchestrator.DaemonDescription]) -> None
        self.daemons[host] = dm
        self.last_daemon_update[host] = datetime_now()

    def update_host_facts(self, host, facts):
        # type: (str, Dict[str, Dict[str, Any]]) -> None
        self.facts[host] = facts
        self.last_facts_update[host] = datetime_now()

    def devices_changed(self, host: str, b: List[inventory.Device]) -> bool:
        a = self.devices[host]
        if len(a) != len(b):
            return True
        aj = {d.path: d.to_json() for d in a}
        bj = {d.path: d.to_json() for d in b}
        if aj != bj:
            self.mgr.log.info("Detected new or changed devices on %s" % host)
            return True
        return False

    def update_host_devices_networks(self, host, dls, nets):
        # type: (str, List[inventory.Device], Dict[str,List[str]]) -> None
        if (
                host not in self.devices
                or host not in self.last_device_change
                or self.devices_changed(host, dls)
        ):
            self.last_device_change[host] = datetime_now()
        self.last_device_update[host] = datetime_now()
        self.devices[host] = dls
        self.networks[host] = nets

    def update_daemon_config_deps(self, host: str, name: str, deps: List[str], stamp: datetime.datetime) -> None:
        self.daemon_config_deps[host][name] = {
            'deps': deps,
            'last_config': stamp,
        }

    def update_last_host_check(self, host):
        # type: (str) -> None
        self.last_host_check[host] = datetime_now()

    def update_osdspec_last_applied(self, host, service_name, ts):
        # type: (str, str, datetime.datetime) -> None
        self.osdspec_last_applied[host][service_name] = ts

    def prime_empty_host(self, host):
        # type: (str) -> None
        """
        Install an empty entry for a host
        """
        self.daemons[host] = {}
        self.devices[host] = []
        self.networks[host] = {}
        self.osdspec_previews[host] = []
        self.osdspec_last_applied[host] = {}
        self.daemon_config_deps[host] = {}
        self.daemon_refresh_queue.append(host)
        self.device_refresh_queue.append(host)
        self.osdspec_previews_refresh_queue.append(host)
        self.registry_login_queue.add(host)

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

    def distribute_new_registry_login_info(self) -> None:
        self.registry_login_queue = set(self.mgr.inventory.keys())

    def save_host(self, host: str) -> None:
        j: Dict[str, Any] = {
            'daemons': {},
            'devices': [],
            'osdspec_previews': [],
            'osdspec_last_applied': {},
            'daemon_config_deps': {},
        }
        if host in self.last_daemon_update:
            j['last_daemon_update'] = datetime_to_str(self.last_daemon_update[host])
        if host in self.last_device_update:
            j['last_device_update'] = datetime_to_str(self.last_device_update[host])
        if host in self.last_device_change:
            j['last_device_change'] = datetime_to_str(self.last_device_change[host])
        if host in self.daemons:
            for name, dd in self.daemons[host].items():
                j['daemons'][name] = dd.to_json()
        if host in self.devices:
            for d in self.devices[host]:
                j['devices'].append(d.to_json())
        if host in self.networks:
            j['networks'] = self.networks[host]
        if host in self.daemon_config_deps:
            for name, depi in self.daemon_config_deps[host].items():
                j['daemon_config_deps'][name] = {
                    'deps': depi.get('deps', []),
                    'last_config': datetime_to_str(depi['last_config']),
                }
        if host in self.osdspec_previews and self.osdspec_previews[host]:
            j['osdspec_previews'] = self.osdspec_previews[host]
        if host in self.osdspec_last_applied:
            for name, ts in self.osdspec_last_applied[host].items():
                j['osdspec_last_applied'][name] = datetime_to_str(ts)

        if host in self.last_host_check:
            j['last_host_check'] = datetime_to_str(self.last_host_check[host])

        if host in self.last_etc_ceph_ceph_conf:
            j['last_etc_ceph_ceph_conf'] = datetime_to_str(self.last_etc_ceph_ceph_conf[host])
        if host in self.scheduled_daemon_actions:
            j['scheduled_daemon_actions'] = self.scheduled_daemon_actions[host]

        self.mgr.set_store(HOST_CACHE_PREFIX + host, json.dumps(j))

    def rm_host(self, host):
        # type: (str) -> None
        if host in self.daemons:
            del self.daemons[host]
        if host in self.devices:
            del self.devices[host]
        if host in self.facts:
            del self.facts[host]
        if host in self.last_facts_update:
            del self.last_facts_update[host]
        if host in self.osdspec_previews:
            del self.osdspec_previews[host]
        if host in self.osdspec_last_applied:
            del self.osdspec_last_applied[host]
        if host in self.loading_osdspec_preview:
            self.loading_osdspec_preview.remove(host)
        if host in self.networks:
            del self.networks[host]
        if host in self.last_daemon_update:
            del self.last_daemon_update[host]
        if host in self.last_device_update:
            del self.last_device_update[host]
        if host in self.last_device_change:
            del self.last_device_change[host]
        if host in self.daemon_config_deps:
            del self.daemon_config_deps[host]
        if host in self.scheduled_daemon_actions:
            del self.scheduled_daemon_actions[host]
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

    def get_daemon(self, daemon_name: str) -> orchestrator.DaemonDescription:
        assert not daemon_name.startswith('ha-rgw.')
        for _, dm in self.daemons.items():
            for _, dd in dm.items():
                if dd.name() == daemon_name:
                    return dd
        raise orchestrator.OrchestratorError(f'Unable to find {daemon_name} daemon(s)')

    def get_daemons_with_volatile_status(self) -> Iterator[Tuple[str, Dict[str, orchestrator.DaemonDescription]]]:
        def alter(host: str, dd_orig: orchestrator.DaemonDescription) -> orchestrator.DaemonDescription:
            dd = copy(dd_orig)
            if host in self.mgr.offline_hosts:
                dd.status = orchestrator.DaemonDescriptionStatus.error
                dd.status_desc = 'host is offline'
            dd.events = self.mgr.events.get_for_daemon(dd.name())
            return dd

        for host, dm in self.daemons.items():
            yield host, {name: alter(host, d) for name, d in dm.items()}

    def get_daemons_by_service(self, service_name):
        # type: (str) -> List[orchestrator.DaemonDescription]
        assert not service_name.startswith('keepalived.')
        assert not service_name.startswith('haproxy.')

        result = []   # type: List[orchestrator.DaemonDescription]
        for host, dm in self.daemons.items():
            for name, d in dm.items():
                if d.service_name() == service_name:
                    result.append(d)
        return result

    def get_daemons_by_type(self, service_type):
        # type: (str) -> List[orchestrator.DaemonDescription]
        assert service_type not in ['keepalived', 'haproxy']

        result = []   # type: List[orchestrator.DaemonDescription]
        for host, dm in self.daemons.items():
            for name, d in dm.items():
                if d.daemon_type in service_to_daemon_types(service_type):
                    result.append(d)
        return result

    def get_daemon_types(self, hostname: str) -> List[str]:
        """Provide a list of the types of daemons on the host"""
        result = set()
        for _d, dm in self.daemons[hostname].items():
            assert dm.daemon_type is not None, f'no daemon type for {dm!r}'
            result.add(dm.daemon_type)
        return list(result)

    def get_daemon_names(self):
        # type: () -> List[str]
        r = []
        for host, dm in self.daemons.items():
            for name, dd in dm.items():
                r.append(name)
        return r

    def get_daemon_last_config_deps(self, host: str, name: str) -> Tuple[Optional[List[str]], Optional[datetime.datetime]]:
        if host in self.daemon_config_deps:
            if name in self.daemon_config_deps[host]:
                return self.daemon_config_deps[host][name].get('deps', []), \
                    self.daemon_config_deps[host][name].get('last_config', None)
        return None, None

    def host_needs_daemon_refresh(self, host):
        # type: (str) -> bool
        if host in self.mgr.offline_hosts:
            logger.debug(f'Host "{host}" marked as offline. Skipping daemon refresh')
            return False
        if host in self.daemon_refresh_queue:
            self.daemon_refresh_queue.remove(host)
            return True
        cutoff = datetime_now() - datetime.timedelta(
            seconds=self.mgr.daemon_cache_timeout)
        if host not in self.last_daemon_update or self.last_daemon_update[host] < cutoff:
            return True
        return False

    def host_needs_facts_refresh(self, host):
        # type: (str) -> bool
        if host in self.mgr.offline_hosts:
            logger.debug(f'Host "{host}" marked as offline. Skipping gather facts refresh')
            return False
        cutoff = datetime_now() - datetime.timedelta(
            seconds=self.mgr.facts_cache_timeout)
        if host not in self.last_facts_update or self.last_facts_update[host] < cutoff:
            return True
        return False

    def host_had_daemon_refresh(self, host: str) -> bool:
        """
        ... at least once.
        """
        if host in self.last_daemon_update:
            return True
        if host not in self.daemons:
            return False
        return bool(self.daemons[host])

    def host_needs_device_refresh(self, host):
        # type: (str) -> bool
        if host in self.mgr.offline_hosts:
            logger.debug(f'Host "{host}" marked as offline. Skipping device refresh')
            return False
        if host in self.device_refresh_queue:
            self.device_refresh_queue.remove(host)
            return True
        cutoff = datetime_now() - datetime.timedelta(
            seconds=self.mgr.device_cache_timeout)
        if host not in self.last_device_update or self.last_device_update[host] < cutoff:
            return True
        return False

    def host_needs_osdspec_preview_refresh(self, host: str) -> bool:
        if host in self.mgr.offline_hosts:
            logger.debug(f'Host "{host}" marked as offline. Skipping osdspec preview refresh')
            return False
        if host in self.osdspec_previews_refresh_queue:
            self.osdspec_previews_refresh_queue.remove(host)
            return True
        #  Since this is dependent on other factors (device and spec) this does not  need
        #  to be updated periodically.
        return False

    def host_needs_check(self, host):
        # type: (str) -> bool
        cutoff = datetime_now() - datetime.timedelta(
            seconds=self.mgr.host_check_interval)
        return host not in self.last_host_check or self.last_host_check[host] < cutoff

    def host_needs_new_etc_ceph_ceph_conf(self, host: str) -> bool:
        if not self.mgr.manage_etc_ceph_ceph_conf:
            return False
        if self.mgr.paused:
            return False
        if host in self.mgr.offline_hosts:
            return False
        if not self.mgr.last_monmap:
            return False
        if host not in self.last_etc_ceph_ceph_conf:
            return True
        if self.mgr.last_monmap > self.last_etc_ceph_ceph_conf[host]:
            return True
        if self.mgr.extra_ceph_conf_is_newer(self.last_etc_ceph_ceph_conf[host]):
            return True
        # already up to date:
        return False

    def osdspec_needs_apply(self, host: str, spec: ServiceSpec) -> bool:
        if (
            host not in self.devices
            or host not in self.last_device_change
            or host not in self.last_device_update
            or host not in self.osdspec_last_applied
            or spec.service_name() not in self.osdspec_last_applied[host]
        ):
            return True
        created = self.mgr.spec_store.get_created(spec)
        if not created or created > self.last_device_change[host]:
            return True
        return self.osdspec_last_applied[host][spec.service_name()] < self.last_device_change[host]

    def update_last_etc_ceph_ceph_conf(self, host: str) -> None:
        if not self.mgr.last_monmap:
            return
        self.last_etc_ceph_ceph_conf[host] = datetime_now()

    def host_needs_registry_login(self, host: str) -> bool:
        if host in self.mgr.offline_hosts:
            return False
        if host in self.registry_login_queue:
            self.registry_login_queue.remove(host)
            return True
        return False

    def add_daemon(self, host, dd):
        # type: (str, orchestrator.DaemonDescription) -> None
        assert host in self.daemons
        self.daemons[host][dd.name()] = dd

    def rm_daemon(self, host: str, name: str) -> None:
        assert not name.startswith('ha-rgw.')

        if host in self.daemons:
            if name in self.daemons[host]:
                del self.daemons[host][name]

    def daemon_cache_filled(self) -> bool:
        """
        i.e. we have checked the daemons for each hosts at least once.
        excluding offline hosts.

        We're not checking for `host_needs_daemon_refresh`, as this might never be
        False for all hosts.
        """
        return all((self.host_had_daemon_refresh(h) or h in self.mgr.offline_hosts)
                   for h in self.get_hosts())

    def schedule_daemon_action(self, host: str, daemon_name: str, action: str) -> None:
        assert not daemon_name.startswith('ha-rgw.')

        priorities = {
            'start': 1,
            'restart': 2,
            'reconfig': 3,
            'redeploy': 4,
            'stop': 5,
        }
        existing_action = self.scheduled_daemon_actions.get(host, {}).get(daemon_name, None)
        if existing_action and priorities[existing_action] > priorities[action]:
            logger.debug(
                f'skipping {action}ing {daemon_name}, cause {existing_action} already scheduled.')
            return

        if host not in self.scheduled_daemon_actions:
            self.scheduled_daemon_actions[host] = {}
        self.scheduled_daemon_actions[host][daemon_name] = action

    def rm_scheduled_daemon_action(self, host: str, daemon_name: str) -> None:
        if host in self.scheduled_daemon_actions:
            if daemon_name in self.scheduled_daemon_actions[host]:
                del self.scheduled_daemon_actions[host][daemon_name]
            if not self.scheduled_daemon_actions[host]:
                del self.scheduled_daemon_actions[host]

    def get_scheduled_daemon_action(self, host: str, daemon: str) -> Optional[str]:
        assert not daemon.startswith('ha-rgw.')

        return self.scheduled_daemon_actions.get(host, {}).get(daemon)


class EventStore():
    def __init__(self, mgr):
        # type: (CephadmOrchestrator) -> None
        self.mgr: CephadmOrchestrator = mgr
        self.events = {}  # type: Dict[str, List[OrchestratorEvent]]

    def add(self, event: OrchestratorEvent) -> None:

        if event.kind_subject() not in self.events:
            self.events[event.kind_subject()] = [event]

        for e in self.events[event.kind_subject()]:
            if e.message == event.message:
                return

        self.events[event.kind_subject()].append(event)

        # limit to five events for now.
        self.events[event.kind_subject()] = self.events[event.kind_subject()][-5:]

    def for_service(self, spec: ServiceSpec, level: str, message: str) -> None:
        e = OrchestratorEvent(datetime_now(), 'service',
                              spec.service_name(), level, message)
        self.add(e)

    def from_orch_error(self, e: OrchestratorError) -> None:
        if e.event_subject is not None:
            self.add(OrchestratorEvent(
                datetime_now(),
                e.event_subject[0],
                e.event_subject[1],
                "ERROR",
                str(e)
            ))

    def for_daemon(self, daemon_name: str, level: str, message: str) -> None:
        e = OrchestratorEvent(datetime_now(), 'daemon', daemon_name, level, message)
        self.add(e)

    def for_daemon_from_exception(self, daemon_name: str, e: Exception) -> None:
        self.for_daemon(
            daemon_name,
            "ERROR",
            str(e)
        )

    def cleanup(self) -> None:
        # Needs to be properly done, in case events are persistently stored.

        unknowns: List[str] = []
        daemons = self.mgr.cache.get_daemon_names()
        specs = self.mgr.spec_store.all_specs.keys()
        for k_s, v in self.events.items():
            kind, subject = k_s.split(':')
            if kind == 'service':
                if subject not in specs:
                    unknowns.append(k_s)
            elif kind == 'daemon':
                if subject not in daemons:
                    unknowns.append(k_s)

        for k_s in unknowns:
            del self.events[k_s]

    def get_for_service(self, name: str) -> List[OrchestratorEvent]:
        return self.events.get('service:' + name, [])

    def get_for_daemon(self, name: str) -> List[OrchestratorEvent]:
        return self.events.get('daemon:' + name, [])
