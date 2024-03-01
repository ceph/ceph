import datetime
import enum
from copy import copy
import ipaddress
import itertools
import json
import logging
import math
import socket
from typing import TYPE_CHECKING, Dict, List, Iterator, Optional, Any, Tuple, Set, Mapping, cast, \
    NamedTuple, Type

import orchestrator
from ceph.deployment import inventory
from ceph.deployment.service_spec import ServiceSpec, PlacementSpec, TunedProfileSpec, IngressSpec
from ceph.utils import str_to_datetime, datetime_to_str, datetime_now
from orchestrator import OrchestratorError, HostSpec, OrchestratorEvent, service_to_daemon_types
from cephadm.services.cephadmservice import CephadmDaemonDeploySpec

from .utils import resolve_ip, SpecialHostLabels
from .migrations import queue_migrate_nfs_spec, queue_migrate_rgw_spec

if TYPE_CHECKING:
    from .module import CephadmOrchestrator


logger = logging.getLogger(__name__)

HOST_CACHE_PREFIX = "host."
SPEC_STORE_PREFIX = "spec."
AGENT_CACHE_PREFIX = 'agent.'
NODE_PROXY_CACHE_PREFIX = 'node_proxy'


class HostCacheStatus(enum.Enum):
    stray = 'stray'
    host = 'host'
    devices = 'devices'


class Inventory:
    """
    The inventory stores a HostSpec for all hosts persistently.
    """

    def __init__(self, mgr: 'CephadmOrchestrator'):
        self.mgr = mgr
        adjusted_addrs = False

        def is_valid_ip(ip: str) -> bool:
            try:
                ipaddress.ip_address(ip)
                return True
            except ValueError:
                return False

        # load inventory
        i = self.mgr.get_store('inventory')
        if i:
            self._inventory: Dict[str, dict] = json.loads(i)
            # handle old clusters missing 'hostname' key from hostspec
            for k, v in self._inventory.items():
                if 'hostname' not in v:
                    v['hostname'] = k

                # convert legacy non-IP addr?
                if is_valid_ip(str(v.get('addr'))):
                    continue
                if len(self._inventory) > 1:
                    if k == socket.gethostname():
                        # Never try to resolve our own host!  This is
                        # fraught and can lead to either a loopback
                        # address (due to podman's futzing with
                        # /etc/hosts) or a private IP based on the CNI
                        # configuration.  Instead, wait until the mgr
                        # fails over to another host and let them resolve
                        # this host.
                        continue
                    ip = resolve_ip(cast(str, v.get('addr')))
                else:
                    # we only have 1 node in the cluster, so we can't
                    # rely on another host doing the lookup.  use the
                    # IP the mgr binds to.
                    ip = self.mgr.get_mgr_ip()
                if is_valid_ip(ip) and not ip.startswith('127.0.'):
                    self.mgr.log.info(
                        f"inventory: adjusted host {v['hostname']} addr '{v['addr']}' -> '{ip}'"
                    )
                    v['addr'] = ip
                    adjusted_addrs = True
            if adjusted_addrs:
                self.save()
        else:
            self._inventory = dict()
        self._all_known_names: Dict[str, List[str]] = {}
        logger.debug('Loaded inventory %s' % self._inventory)

    def keys(self) -> List[str]:
        return list(self._inventory.keys())

    def __contains__(self, host: str) -> bool:
        return host in self._inventory or host in itertools.chain.from_iterable(self._all_known_names.values())

    def _get_stored_name(self, host: str) -> str:
        self.assert_host(host)
        if host in self._inventory:
            return host
        for stored_name, all_names in self._all_known_names.items():
            if host in all_names:
                return stored_name
        return host

    def update_known_hostnames(self, hostname: str, shortname: str, fqdn: str) -> None:
        for hname in [hostname, shortname, fqdn]:
            # if we know the host by any of the names, store the full set of names
            # in order to be able to check against those names for matching a host
            if hname in self._inventory:
                self._all_known_names[hname] = [hostname, shortname, fqdn]
                return
        logger.debug(f'got hostname set from gather-facts for unknown host: {[hostname, shortname, fqdn]}')

    def assert_host(self, host: str) -> None:
        if host not in self:
            raise OrchestratorError('host %s does not exist' % host)

    def add_host(self, spec: HostSpec) -> None:
        if spec.hostname in self:
            # addr
            if self.get_addr(spec.hostname) != spec.addr:
                self.set_addr(spec.hostname, spec.addr)
            # labels
            for label in spec.labels:
                self.add_label(spec.hostname, label)
        else:
            self._inventory[spec.hostname] = spec.to_json()
            self.save()

    def rm_host(self, host: str) -> None:
        host = self._get_stored_name(host)
        del self._inventory[host]
        self._all_known_names.pop(host, [])
        self.save()

    def set_addr(self, host: str, addr: str) -> None:
        host = self._get_stored_name(host)
        self._inventory[host]['addr'] = addr
        self.save()

    def add_label(self, host: str, label: str) -> None:
        host = self._get_stored_name(host)

        if 'labels' not in self._inventory[host]:
            self._inventory[host]['labels'] = list()
        if label not in self._inventory[host]['labels']:
            self._inventory[host]['labels'].append(label)
        self.save()

    def rm_label(self, host: str, label: str) -> None:
        host = self._get_stored_name(host)

        if 'labels' not in self._inventory[host]:
            self._inventory[host]['labels'] = list()
        if label in self._inventory[host]['labels']:
            self._inventory[host]['labels'].remove(label)
        self.save()

    def has_label(self, host: str, label: str) -> bool:
        host = self._get_stored_name(host)
        return (
            host in self._inventory
            and label in self._inventory[host].get('labels', [])
        )

    def get_addr(self, host: str) -> str:
        host = self._get_stored_name(host)
        return self._inventory[host].get('addr', host)

    def spec_from_dict(self, info: dict) -> HostSpec:
        hostname = info['hostname']
        hostname = self._get_stored_name(hostname)
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
    rank_map: Optional[Dict[int, Dict[int, Optional[str]]]]
    created: datetime.datetime
    deleted: Optional[datetime.datetime]


class SpecStore():
    def __init__(self, mgr):
        # type: (CephadmOrchestrator) -> None
        self.mgr = mgr
        self._specs = {}  # type: Dict[str, ServiceSpec]
        # service_name -> rank -> gen -> daemon_id
        self._rank_maps = {}    # type: Dict[str, Dict[int, Dict[int, Optional[str]]]]
        self.spec_created = {}  # type: Dict[str, datetime.datetime]
        self.spec_deleted = {}  # type: Dict[str, datetime.datetime]
        self.spec_preview = {}  # type: Dict[str, ServiceSpec]
        self._needs_configuration: Dict[str, bool] = {}

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
                               self._rank_maps.get(name),
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
                if (
                        (self.mgr.migration_current or 0) < 3
                        and j['spec'].get('service_type') == 'nfs'
                ):
                    self.mgr.log.debug(f'found legacy nfs spec {j}')
                    queue_migrate_nfs_spec(self.mgr, j)

                if (
                        (self.mgr.migration_current or 0) < 6
                        and j['spec'].get('service_type') == 'rgw'
                ):
                    queue_migrate_rgw_spec(self.mgr, j)

                spec = ServiceSpec.from_json(j['spec'])
                created = str_to_datetime(cast(str, j['created']))
                self._specs[service_name] = spec
                self.spec_created[service_name] = created

                if 'deleted' in j:
                    deleted = str_to_datetime(cast(str, j['deleted']))
                    self.spec_deleted[service_name] = deleted

                if 'needs_configuration' in j:
                    self._needs_configuration[service_name] = cast(bool, j['needs_configuration'])

                if 'rank_map' in j and isinstance(j['rank_map'], dict):
                    self._rank_maps[service_name] = {}
                    for rank_str, m in j['rank_map'].items():
                        try:
                            rank = int(rank_str)
                        except ValueError:
                            logger.exception(f"failed to parse rank in {j['rank_map']}")
                            continue
                        if isinstance(m, dict):
                            self._rank_maps[service_name][rank] = {}
                            for gen_str, name in m.items():
                                try:
                                    gen = int(gen_str)
                                except ValueError:
                                    logger.exception(f"failed to parse gen in {j['rank_map']}")
                                    continue
                                if isinstance(name, str) or m is None:
                                    self._rank_maps[service_name][rank][gen] = name

                self.mgr.log.debug('SpecStore: loaded spec for %s' % (
                    service_name))
            except Exception as e:
                self.mgr.log.warning('unable to load spec for %s: %s' % (
                    service_name, e))
                pass

    def save(
            self,
            spec: ServiceSpec,
            update_create: bool = True,
    ) -> None:
        name = spec.service_name()
        if spec.preview_only:
            self.spec_preview[name] = spec
            return None
        self._specs[name] = spec
        self._needs_configuration[name] = True

        if update_create:
            self.spec_created[name] = datetime_now()
        self._save(name)

    def save_rank_map(self,
                      name: str,
                      rank_map: Dict[int, Dict[int, Optional[str]]]) -> None:
        self._rank_maps[name] = rank_map
        self._save(name)

    def _save(self, name: str) -> None:
        data: Dict[str, Any] = {
            'spec': self._specs[name].to_json(),
        }
        if name in self.spec_created:
            data['created'] = datetime_to_str(self.spec_created[name])
        if name in self._rank_maps:
            data['rank_map'] = self._rank_maps[name]
        if name in self.spec_deleted:
            data['deleted'] = datetime_to_str(self.spec_deleted[name])
        if name in self._needs_configuration:
            data['needs_configuration'] = self._needs_configuration[name]

        self.mgr.set_store(
            SPEC_STORE_PREFIX + name,
            json.dumps(data, sort_keys=True),
        )
        self.mgr.events.for_service(self._specs[name],
                                    OrchestratorEvent.INFO,
                                    'service was created')

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
            if service_name in self._rank_maps:
                del self._rank_maps[service_name]
            del self.spec_created[service_name]
            if service_name in self.spec_deleted:
                del self.spec_deleted[service_name]
            if service_name in self._needs_configuration:
                del self._needs_configuration[service_name]
            self.mgr.set_store(SPEC_STORE_PREFIX + service_name, None)
        return found

    def get_created(self, spec: ServiceSpec) -> Optional[datetime.datetime]:
        return self.spec_created.get(spec.service_name())

    def set_unmanaged(self, service_name: str, value: bool) -> str:
        if service_name not in self._specs:
            return f'No service of name {service_name} found. Check "ceph orch ls" for all known services'
        if self._specs[service_name].unmanaged == value:
            return f'Service {service_name}{" already " if value else " not "}marked unmanaged. No action taken.'
        self._specs[service_name].unmanaged = value
        self.save(self._specs[service_name])
        return f'Set unmanaged to {str(value)} for service {service_name}'

    def needs_configuration(self, name: str) -> bool:
        return self._needs_configuration.get(name, False)

    def mark_needs_configuration(self, name: str) -> None:
        if name in self._specs:
            self._needs_configuration[name] = True
            self._save(name)
        else:
            self.mgr.log.warning(f'Attempted to mark unknown service "{name}" as needing configuration')

    def mark_configured(self, name: str) -> None:
        if name in self._specs:
            self._needs_configuration[name] = False
            self._save(name)
        else:
            self.mgr.log.warning(f'Attempted to mark unknown service "{name}" as having been configured')


class ClientKeyringSpec(object):
    """
    A client keyring file that we should maintain
    """

    def __init__(
            self,
            entity: str,
            placement: PlacementSpec,
            mode: Optional[int] = None,
            uid: Optional[int] = None,
            gid: Optional[int] = None,
    ) -> None:
        self.entity = entity
        self.placement = placement
        self.mode = mode or 0o600
        self.uid = uid or 0
        self.gid = gid or 0

    def validate(self) -> None:
        pass

    def to_json(self) -> Dict[str, Any]:
        return {
            'entity': self.entity,
            'placement': self.placement.to_json(),
            'mode': self.mode,
            'uid': self.uid,
            'gid': self.gid,
        }

    @property
    def path(self) -> str:
        return f'/etc/ceph/ceph.{self.entity}.keyring'

    @classmethod
    def from_json(cls: Type, data: dict) -> 'ClientKeyringSpec':
        c = data.copy()
        if 'placement' in c:
            c['placement'] = PlacementSpec.from_json(c['placement'])
        _cls = cls(**c)
        _cls.validate()
        return _cls


class ClientKeyringStore():
    """
    Track client keyring files that we are supposed to maintain
    """

    def __init__(self, mgr):
        # type: (CephadmOrchestrator) -> None
        self.mgr: CephadmOrchestrator = mgr
        self.mgr = mgr
        self.keys: Dict[str, ClientKeyringSpec] = {}

    def load(self) -> None:
        c = self.mgr.get_store('client_keyrings') or b'{}'
        j = json.loads(c)
        for e, d in j.items():
            self.keys[e] = ClientKeyringSpec.from_json(d)

    def save(self) -> None:
        data = {
            k: v.to_json() for k, v in self.keys.items()
        }
        self.mgr.set_store('client_keyrings', json.dumps(data))

    def update(self, ks: ClientKeyringSpec) -> None:
        self.keys[ks.entity] = ks
        self.save()

    def rm(self, entity: str) -> None:
        if entity in self.keys:
            del self.keys[entity]
            self.save()


class TunedProfileStore():
    """
    Store for out tuned profile information
    """

    def __init__(self, mgr: "CephadmOrchestrator") -> None:
        self.mgr: CephadmOrchestrator = mgr
        self.mgr = mgr
        self.profiles: Dict[str, TunedProfileSpec] = {}

    def __contains__(self, profile: str) -> bool:
        return profile in self.profiles

    def load(self) -> None:
        c = self.mgr.get_store('tuned_profiles') or b'{}'
        j = json.loads(c)
        for k, v in j.items():
            self.profiles[k] = TunedProfileSpec.from_json(v)
            self.profiles[k]._last_updated = datetime_to_str(datetime_now())

    def exists(self, profile_name: str) -> bool:
        return profile_name in self.profiles

    def save(self) -> None:
        profiles_json = {k: v.to_json() for k, v in self.profiles.items()}
        self.mgr.set_store('tuned_profiles', json.dumps(profiles_json))

    def add_setting(self, profile: str, setting: str, value: str) -> None:
        if profile in self.profiles:
            self.profiles[profile].settings[setting] = value
            self.profiles[profile]._last_updated = datetime_to_str(datetime_now())
            self.save()
        else:
            logger.error(
                f'Attempted to set setting "{setting}" for nonexistent os tuning profile "{profile}"')

    def rm_setting(self, profile: str, setting: str) -> None:
        if profile in self.profiles:
            if setting in self.profiles[profile].settings:
                self.profiles[profile].settings.pop(setting, '')
                self.profiles[profile]._last_updated = datetime_to_str(datetime_now())
                self.save()
            else:
                logger.error(
                    f'Attemped to remove nonexistent setting "{setting}" from os tuning profile "{profile}"')
        else:
            logger.error(
                f'Attempted to remove setting "{setting}" from nonexistent os tuning profile "{profile}"')

    def add_profile(self, spec: TunedProfileSpec) -> None:
        spec._last_updated = datetime_to_str(datetime_now())
        self.profiles[spec.profile_name] = spec
        self.save()

    def rm_profile(self, profile: str) -> None:
        if profile in self.profiles:
            self.profiles.pop(profile, TunedProfileSpec(''))
        else:
            logger.error(f'Attempted to remove nonexistent os tuning profile "{profile}"')
        self.save()

    def last_updated(self, profile: str) -> Optional[datetime.datetime]:
        if profile not in self.profiles or not self.profiles[profile]._last_updated:
            return None
        return str_to_datetime(self.profiles[profile]._last_updated)

    def set_last_updated(self, profile: str, new_datetime: datetime.datetime) -> None:
        if profile in self.profiles:
            self.profiles[profile]._last_updated = datetime_to_str(new_datetime)

    def list_profiles(self) -> List[TunedProfileSpec]:
        return [p for p in self.profiles.values()]


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

    4. `last_client_files` O(hosts)

    Stores the last digest and owner/mode for files we've pushed to /etc/ceph
    (ceph.conf or client keyrings).

    5. `scheduled_daemon_actions`: O(daemons)

    Used to run daemon actions after deploying a daemon. We need to
    store it persistently, in order to stay consistent across
    MGR failovers.
    """

    def __init__(self, mgr):
        # type: (CephadmOrchestrator) -> None
        self.mgr: CephadmOrchestrator = mgr
        self.daemons = {}   # type: Dict[str, Dict[str, orchestrator.DaemonDescription]]
        self._tmp_daemons = {}  # type: Dict[str, Dict[str, orchestrator.DaemonDescription]]
        self.last_daemon_update = {}   # type: Dict[str, datetime.datetime]
        self.devices = {}              # type: Dict[str, List[inventory.Device]]
        self.facts = {}                # type: Dict[str, Dict[str, Any]]
        self.last_facts_update = {}    # type: Dict[str, datetime.datetime]
        self.last_autotune = {}        # type: Dict[str, datetime.datetime]
        self.osdspec_previews = {}     # type: Dict[str, List[Dict[str, Any]]]
        self.osdspec_last_applied = {}  # type: Dict[str, Dict[str, datetime.datetime]]
        self.networks = {}             # type: Dict[str, Dict[str, Dict[str, List[str]]]]
        self.last_network_update = {}   # type: Dict[str, datetime.datetime]
        self.last_device_update = {}   # type: Dict[str, datetime.datetime]
        self.last_device_change = {}   # type: Dict[str, datetime.datetime]
        self.last_tuned_profile_update = {}  # type: Dict[str, datetime.datetime]
        self.daemon_refresh_queue = []  # type: List[str]
        self.device_refresh_queue = []  # type: List[str]
        self.network_refresh_queue = []  # type: List[str]
        self.osdspec_previews_refresh_queue = []  # type: List[str]

        # host -> daemon name -> dict
        self.daemon_config_deps = {}   # type: Dict[str, Dict[str, Dict[str,Any]]]
        self.last_host_check = {}      # type: Dict[str, datetime.datetime]
        self.loading_osdspec_preview = set()  # type: Set[str]
        self.last_client_files: Dict[str, Dict[str, Tuple[str, int, int, int]]] = {}
        self.registry_login_queue: Set[str] = set()

        self.scheduled_daemon_actions: Dict[str, Dict[str, str]] = {}

        self.metadata_up_to_date = {}  # type: Dict[str, bool]

    def load(self):
        # type: () -> None
        for k, v in self.mgr.get_store_prefix(HOST_CACHE_PREFIX).items():
            host = k[len(HOST_CACHE_PREFIX):]
            if self._get_host_cache_entry_status(host) != HostCacheStatus.host:
                if self._get_host_cache_entry_status(host) == HostCacheStatus.devices:
                    continue
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
                self.network_refresh_queue.append(host)
                self.daemons[host] = {}
                self.osdspec_previews[host] = []
                self.osdspec_last_applied[host] = {}
                self.networks[host] = {}
                self.daemon_config_deps[host] = {}
                for name, d in j.get('daemons', {}).items():
                    self.daemons[host][name] = \
                        orchestrator.DaemonDescription.from_json(d)
                self.devices[host] = []
                # still want to check old device location for upgrade scenarios
                for d in j.get('devices', []):
                    self.devices[host].append(inventory.Device.from_json(d))
                self.devices[host] += self.load_host_devices(host)
                self.networks[host] = j.get('networks_and_interfaces', {})
                self.osdspec_previews[host] = j.get('osdspec_previews', {})
                self.last_client_files[host] = j.get('last_client_files', {})
                for name, ts in j.get('osdspec_last_applied', {}).items():
                    self.osdspec_last_applied[host][name] = str_to_datetime(ts)

                for name, d in j.get('daemon_config_deps', {}).items():
                    self.daemon_config_deps[host][name] = {
                        'deps': d.get('deps', []),
                        'last_config': str_to_datetime(d['last_config']),
                    }
                if 'last_host_check' in j:
                    self.last_host_check[host] = str_to_datetime(j['last_host_check'])
                if 'last_tuned_profile_update' in j:
                    self.last_tuned_profile_update[host] = str_to_datetime(
                        j['last_tuned_profile_update'])
                self.registry_login_queue.add(host)
                self.scheduled_daemon_actions[host] = j.get('scheduled_daemon_actions', {})
                self.metadata_up_to_date[host] = j.get('metadata_up_to_date', False)

                self.mgr.log.debug(
                    'HostCache.load: host %s has %d daemons, '
                    '%d devices, %d networks' % (
                        host, len(self.daemons[host]), len(self.devices[host]),
                        len(self.networks[host])))
            except Exception as e:
                self.mgr.log.warning('unable to load cached state for %s: %s' % (
                    host, e))
                pass

    def _get_host_cache_entry_status(self, host: str) -> HostCacheStatus:
        # return whether a host cache entry in the config-key
        # store is for a host, a set of devices or is stray.
        # for a host, the entry name will match a hostname in our
        # inventory. For devices, it will be formatted
        # <hostname>.devices.<integer> where <hostname> is
        # in out inventory. If neither case applies, it is stray
        if host in self.mgr.inventory:
            return HostCacheStatus.host
        try:
            # try stripping off the ".devices.<integer>" and see if we get
            # a host name that matches our inventory
            actual_host = '.'.join(host.split('.')[:-2])
            return HostCacheStatus.devices if actual_host in self.mgr.inventory else HostCacheStatus.stray
        except Exception:
            return HostCacheStatus.stray

    def update_host_daemons(self, host, dm):
        # type: (str, Dict[str, orchestrator.DaemonDescription]) -> None
        self.daemons[host] = dm
        self._tmp_daemons.pop(host, {})
        self.last_daemon_update[host] = datetime_now()

    def append_tmp_daemon(self, host: str, dd: orchestrator.DaemonDescription) -> None:
        # for storing empty daemon descriptions representing daemons we have
        # just deployed but not yet had the chance to pick up in a daemon refresh
        # _tmp_daemons is cleared for a host upon receiving a real update of the
        # host's dameons
        if host not in self._tmp_daemons:
            self._tmp_daemons[host] = {}
        self._tmp_daemons[host][dd.name()] = dd

    def update_host_facts(self, host, facts):
        # type: (str, Dict[str, Dict[str, Any]]) -> None
        self.facts[host] = facts
        hostnames: List[str] = []
        for k in ['hostname', 'shortname', 'fqdn']:
            v = facts.get(k, '')
            hostnames.append(v if isinstance(v, str) else '')
        self.mgr.inventory.update_known_hostnames(hostnames[0], hostnames[1], hostnames[2])
        self.last_facts_update[host] = datetime_now()

    def update_autotune(self, host: str) -> None:
        self.last_autotune[host] = datetime_now()

    def invalidate_autotune(self, host: str) -> None:
        if host in self.last_autotune:
            del self.last_autotune[host]

    def devices_changed(self, host: str, b: List[inventory.Device]) -> bool:
        old_devs = inventory.Devices(self.devices[host])
        new_devs = inventory.Devices(b)
        # relying on Devices class __eq__ function here
        if old_devs != new_devs:
            self.mgr.log.info("Detected new or changed devices on %s" % host)
            return True
        return False

    def update_host_devices(
            self,
            host: str,
            dls: List[inventory.Device],
    ) -> None:
        if (
                host not in self.devices
                or host not in self.last_device_change
                or self.devices_changed(host, dls)
        ):
            self.last_device_change[host] = datetime_now()
        self.last_device_update[host] = datetime_now()
        self.devices[host] = dls

    def update_host_networks(
            self,
            host: str,
            nets: Dict[str, Dict[str, List[str]]]
    ) -> None:
        self.networks[host] = nets
        self.last_network_update[host] = datetime_now()

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

    def update_client_file(self,
                           host: str,
                           path: str,
                           digest: str,
                           mode: int,
                           uid: int,
                           gid: int) -> None:
        if host not in self.last_client_files:
            self.last_client_files[host] = {}
        self.last_client_files[host][path] = (digest, mode, uid, gid)

    def removed_client_file(self, host: str, path: str) -> None:
        if (
            host in self.last_client_files
            and path in self.last_client_files[host]
        ):
            del self.last_client_files[host][path]

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
        self.network_refresh_queue.append(host)
        self.osdspec_previews_refresh_queue.append(host)
        self.registry_login_queue.add(host)
        self.last_client_files[host] = {}

    def refresh_all_host_info(self, host):
        # type: (str) -> None

        self.last_host_check.pop(host, None)
        self.daemon_refresh_queue.append(host)
        self.registry_login_queue.add(host)
        self.device_refresh_queue.append(host)
        self.last_facts_update.pop(host, None)
        self.osdspec_previews_refresh_queue.append(host)
        self.last_autotune.pop(host, None)

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

    def invalidate_host_networks(self, host):
        # type: (str) -> None
        self.network_refresh_queue.append(host)
        if host in self.last_network_update:
            del self.last_network_update[host]
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
        if host in self.last_network_update:
            j['last_network_update'] = datetime_to_str(self.last_network_update[host])
        if host in self.last_device_change:
            j['last_device_change'] = datetime_to_str(self.last_device_change[host])
        if host in self.last_tuned_profile_update:
            j['last_tuned_profile_update'] = datetime_to_str(self.last_tuned_profile_update[host])
        if host in self.daemons:
            for name, dd in self.daemons[host].items():
                j['daemons'][name] = dd.to_json()
        if host in self.networks:
            j['networks_and_interfaces'] = self.networks[host]
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

        if host in self.last_client_files:
            j['last_client_files'] = self.last_client_files[host]
        if host in self.scheduled_daemon_actions:
            j['scheduled_daemon_actions'] = self.scheduled_daemon_actions[host]
        if host in self.metadata_up_to_date:
            j['metadata_up_to_date'] = self.metadata_up_to_date[host]
        if host in self.devices:
            self.save_host_devices(host)

        self.mgr.set_store(HOST_CACHE_PREFIX + host, json.dumps(j))

    def save_host_devices(self, host: str) -> None:
        if host not in self.devices or not self.devices[host]:
            logger.debug(f'Host {host} has no devices to save')
            return

        devs: List[Dict[str, Any]] = []
        for d in self.devices[host]:
            devs.append(d.to_json())

        def byte_len(s: str) -> int:
            return len(s.encode('utf-8'))

        dev_cache_counter: int = 0
        cache_size: int = self.mgr.get_foreign_ceph_option('mon', 'mon_config_key_max_entry_size')
        if cache_size is not None and cache_size != 0 and byte_len(json.dumps(devs)) > cache_size - 1024:
            # no guarantee all device entries take up the same amount of space
            # splitting it up so there's one more entry than we need should be fairly
            # safe and save a lot of extra logic checking sizes
            cache_entries_needed = math.ceil(byte_len(json.dumps(devs)) / cache_size) + 1
            dev_sublist_size = math.ceil(len(devs) / cache_entries_needed)
            dev_lists: List[List[Dict[str, Any]]] = [devs[i:i + dev_sublist_size]
                                                     for i in range(0, len(devs), dev_sublist_size)]
            for dev_list in dev_lists:
                dev_dict: Dict[str, Any] = {'devices': dev_list}
                if dev_cache_counter == 0:
                    dev_dict.update({'entries': len(dev_lists)})
                self.mgr.set_store(HOST_CACHE_PREFIX + host + '.devices.'
                                   + str(dev_cache_counter), json.dumps(dev_dict))
                dev_cache_counter += 1
        else:
            self.mgr.set_store(HOST_CACHE_PREFIX + host + '.devices.'
                               + str(dev_cache_counter), json.dumps({'devices': devs, 'entries': 1}))

    def load_host_devices(self, host: str) -> List[inventory.Device]:
        dev_cache_counter: int = 0
        devs: List[Dict[str, Any]] = []
        dev_entries: int = 0
        try:
            # number of entries for the host's devices should be in
            # the "entries" field of the first entry
            dev_entries = json.loads(self.mgr.get_store(
                HOST_CACHE_PREFIX + host + '.devices.0')).get('entries')
        except Exception:
            logger.debug(f'No device entries found for host {host}')
        for i in range(dev_entries):
            try:
                new_devs = json.loads(self.mgr.get_store(
                    HOST_CACHE_PREFIX + host + '.devices.' + str(i))).get('devices', [])
                if len(new_devs) > 0:
                    # verify list contains actual device objects by trying to load one from json
                    inventory.Device.from_json(new_devs[0])
                    # if we didn't throw an Exception on above line, we can add the devices
                    devs = devs + new_devs
                    dev_cache_counter += 1
            except Exception as e:
                logger.error(('Hit exception trying to load devices from '
                             + f'{HOST_CACHE_PREFIX + host + ".devices." + str(dev_cache_counter)} in key store: {e}'))
                return []
        return [inventory.Device.from_json(d) for d in devs]

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
        if host in self.last_autotune:
            del self.last_autotune[host]
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
        if host in self.last_network_update:
            del self.last_network_update[host]
        if host in self.last_device_change:
            del self.last_device_change[host]
        if host in self.last_tuned_profile_update:
            del self.last_tuned_profile_update[host]
        if host in self.daemon_config_deps:
            del self.daemon_config_deps[host]
        if host in self.scheduled_daemon_actions:
            del self.scheduled_daemon_actions[host]
        if host in self.last_client_files:
            del self.last_client_files[host]
        self.mgr.set_store(HOST_CACHE_PREFIX + host, None)

    def get_hosts(self):
        # type: () -> List[str]
        return list(self.daemons)

    def get_schedulable_hosts(self) -> List[HostSpec]:
        """
        Returns all usable hosts that went through _refresh_host_daemons().

        This mitigates a potential race, where new host was added *after*
        ``_refresh_host_daemons()`` was called, but *before*
        ``_apply_all_specs()`` was called. thus we end up with a hosts
        where daemons might be running, but we have not yet detected them.
        """
        return [
            h for h in self.mgr.inventory.all_specs()
            if (
                self.host_had_daemon_refresh(h.hostname)
                and SpecialHostLabels.DRAIN_DAEMONS not in h.labels
            )
        ]

    def get_conf_keyring_available_hosts(self) -> List[HostSpec]:
        """
        Returns all hosts without the drain conf and keyrings
        label (SpecialHostLabels.DRAIN_CONF_KEYRING) that have
        had a refresh. That is equivalent to all hosts we
        consider eligible for deployment of conf and keyring files

        Any host without that label is considered fair game for
        a client keyring spec to match. However, we want to still
        wait for refresh here so that we know what keyrings we've
        already deployed here
        """
        return [
            h for h in self.mgr.inventory.all_specs()
            if (
                self.host_had_daemon_refresh(h.hostname)
                and SpecialHostLabels.DRAIN_CONF_KEYRING not in h.labels
            )
        ]

    def get_non_draining_hosts(self) -> List[HostSpec]:
        """
        Returns all hosts that do not have drain daemon label
        (SpecialHostLabels.DRAIN_DAEMONS).

        Useful for the agent who needs this specific list rather than the
        schedulable_hosts since the agent needs to be deployed on hosts with
        no daemon refresh
        """
        return [
            h for h in self.mgr.inventory.all_specs() if SpecialHostLabels.DRAIN_DAEMONS not in h.labels
        ]

    def get_draining_hosts(self) -> List[HostSpec]:
        """
        Returns all hosts that have the drain daemons label (SpecialHostLabels.DRAIN_DAEMONS)
        and therefore should have no daemons placed on them, but are potentially still reachable
        """
        return [
            h for h in self.mgr.inventory.all_specs() if SpecialHostLabels.DRAIN_DAEMONS in h.labels
        ]

    def get_conf_keyring_draining_hosts(self) -> List[HostSpec]:
        """
        Returns all hosts that have drain conf and keyrings label (SpecialHostLabels.DRAIN_CONF_KEYRING)
        and therefore should have no config files or client keyring placed on them, but are
        potentially still reachable
        """
        return [
            h for h in self.mgr.inventory.all_specs() if SpecialHostLabels.DRAIN_CONF_KEYRING in h.labels
        ]

    def get_unreachable_hosts(self) -> List[HostSpec]:
        """
        Return all hosts that are offline or in maintenance mode.

        The idea is we should not touch the daemons on these hosts (since
        in theory the hosts are inaccessible so we CAN'T touch them) but
        we still want to count daemons that exist on these hosts toward the
        placement so daemons on these hosts aren't just moved elsewhere
        """
        return [
            h for h in self.mgr.inventory.all_specs()
            if (
                h.status.lower() in ['maintenance', 'offline']
                or h.hostname in self.mgr.offline_hosts
            )
        ]

    def is_host_unreachable(self, hostname: str) -> bool:
        # take hostname and return if it matches the hostname of an unreachable host
        return hostname in [h.hostname for h in self.get_unreachable_hosts()]

    def is_host_schedulable(self, hostname: str) -> bool:
        # take hostname and return if it matches the hostname of a schedulable host
        return hostname in [h.hostname for h in self.get_schedulable_hosts()]

    def is_host_draining(self, hostname: str) -> bool:
        # take hostname and return if it matches the hostname of a draining host
        return hostname in [h.hostname for h in self.get_draining_hosts()]

    def get_facts(self, host: str) -> Dict[str, Any]:
        return self.facts.get(host, {})

    def _get_daemons(self) -> Iterator[orchestrator.DaemonDescription]:
        for dm in self.daemons.copy().values():
            yield from dm.values()

    def _get_tmp_daemons(self) -> Iterator[orchestrator.DaemonDescription]:
        for dm in self._tmp_daemons.copy().values():
            yield from dm.values()

    def get_daemons(self):
        # type: () -> List[orchestrator.DaemonDescription]
        return list(self._get_daemons())

    def get_error_daemons(self) -> List[orchestrator.DaemonDescription]:
        r = []
        for dd in self._get_daemons():
            if dd.status is not None and dd.status == orchestrator.DaemonDescriptionStatus.error:
                r.append(dd)
        return r

    def get_daemons_by_host(self, host: str) -> List[orchestrator.DaemonDescription]:
        return list(self.daemons.get(host, {}).values())

    def get_daemon(self, daemon_name: str, host: Optional[str] = None) -> orchestrator.DaemonDescription:
        assert not daemon_name.startswith('ha-rgw.')
        dds = self.get_daemons_by_host(host) if host else self._get_daemons()
        for dd in dds:
            if dd.name() == daemon_name:
                return dd

        raise orchestrator.OrchestratorError(f'Unable to find {daemon_name} daemon(s)')

    def has_daemon(self, daemon_name: str, host: Optional[str] = None) -> bool:
        try:
            self.get_daemon(daemon_name, host)
        except orchestrator.OrchestratorError:
            return False
        return True

    def get_daemons_with_volatile_status(self) -> Iterator[Tuple[str, Dict[str, orchestrator.DaemonDescription]]]:
        def alter(host: str, dd_orig: orchestrator.DaemonDescription) -> orchestrator.DaemonDescription:
            dd = copy(dd_orig)
            if host in self.mgr.offline_hosts:
                dd.status = orchestrator.DaemonDescriptionStatus.error
                dd.status_desc = 'host is offline'
            elif self.mgr.inventory._inventory[host].get("status", "").lower() == "maintenance":
                # We do not refresh daemons on hosts in maintenance mode, so stored daemon statuses
                # could be wrong. We must assume maintenance is working and daemons are stopped
                dd.status = orchestrator.DaemonDescriptionStatus.stopped
            dd.events = self.mgr.events.get_for_daemon(dd.name())
            return dd

        for host, dm in self.daemons.copy().items():
            yield host, {name: alter(host, d) for name, d in dm.items()}

    def get_daemons_by_service(self, service_name):
        # type: (str) -> List[orchestrator.DaemonDescription]
        assert not service_name.startswith('keepalived.')
        assert not service_name.startswith('haproxy.')

        return list(dd for dd in self._get_daemons() if dd.service_name() == service_name)

    def get_related_service_daemons(self, service_spec: ServiceSpec) -> Optional[List[orchestrator.DaemonDescription]]:
        if service_spec.service_type == 'ingress':
            dds = list(dd for dd in self._get_daemons() if dd.service_name() == cast(IngressSpec, service_spec).backend_service)
            dds += list(dd for dd in self._get_tmp_daemons() if dd.service_name() == cast(IngressSpec, service_spec).backend_service)
            logger.debug(f'Found related daemons {dds} for service {service_spec.service_name()}')
            return dds
        else:
            for ingress_spec in [cast(IngressSpec, s) for s in self.mgr.spec_store.active_specs.values() if s.service_type == 'ingress']:
                if ingress_spec.backend_service == service_spec.service_name():
                    dds = list(dd for dd in self._get_daemons() if dd.service_name() == ingress_spec.service_name())
                    dds += list(dd for dd in self._get_tmp_daemons() if dd.service_name() == ingress_spec.service_name())
                    logger.debug(f'Found related daemons {dds} for service {service_spec.service_name()}')
                    return dds
        return None

    def get_daemons_by_type(self, service_type: str, host: str = '') -> List[orchestrator.DaemonDescription]:
        assert service_type not in ['keepalived', 'haproxy']

        daemons = self.daemons[host].values() if host else self._get_daemons()

        return [d for d in daemons if d.daemon_type in service_to_daemon_types(service_type)]

    def get_daemon_types(self, hostname: str) -> Set[str]:
        """Provide a list of the types of daemons on the host"""
        return cast(Set[str], {d.daemon_type for d in self.daemons[hostname].values()})

    def get_daemon_names(self):
        # type: () -> List[str]
        return [d.name() for d in self._get_daemons()]

    def get_daemon_last_config_deps(self, host: str, name: str) -> Tuple[Optional[List[str]], Optional[datetime.datetime]]:
        if host in self.daemon_config_deps:
            if name in self.daemon_config_deps[host]:
                return self.daemon_config_deps[host][name].get('deps', []), \
                    self.daemon_config_deps[host][name].get('last_config', None)
        return None, None

    def get_host_client_files(self, host: str) -> Dict[str, Tuple[str, int, int, int]]:
        return self.last_client_files.get(host, {})

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
        if not self.mgr.cache.host_metadata_up_to_date(host):
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
        if not self.mgr.cache.host_metadata_up_to_date(host):
            return True
        return False

    def host_needs_autotune_memory(self, host):
        # type: (str) -> bool
        if host in self.mgr.offline_hosts:
            logger.debug(f'Host "{host}" marked as offline. Skipping autotune')
            return False
        cutoff = datetime_now() - datetime.timedelta(
            seconds=self.mgr.autotune_interval)
        if host not in self.last_autotune or self.last_autotune[host] < cutoff:
            return True
        return False

    def host_needs_tuned_profile_update(self, host: str, profile: str) -> bool:
        if host in self.mgr.offline_hosts:
            logger.debug(f'Host "{host}" marked as offline. Cannot apply tuned profile')
            return False
        if profile not in self.mgr.tuned_profiles:
            logger.debug(
                f'Cannot apply tuned profile {profile} on host {host}. Profile does not exist')
            return False
        if host not in self.last_tuned_profile_update:
            return True
        last_profile_update = self.mgr.tuned_profiles.last_updated(profile)
        if last_profile_update is None:
            self.mgr.tuned_profiles.set_last_updated(profile, datetime_now())
            return True
        if self.last_tuned_profile_update[host] < last_profile_update:
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
        if not self.mgr.cache.host_metadata_up_to_date(host):
            return True
        return False

    def host_needs_network_refresh(self, host):
        # type: (str) -> bool
        if host in self.mgr.offline_hosts:
            logger.debug(f'Host "{host}" marked as offline. Skipping network refresh')
            return False
        if host in self.network_refresh_queue:
            self.network_refresh_queue.remove(host)
            return True
        cutoff = datetime_now() - datetime.timedelta(
            seconds=self.mgr.device_cache_timeout)
        if host not in self.last_network_update or self.last_network_update[host] < cutoff:
            return True
        if not self.mgr.cache.host_metadata_up_to_date(host):
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

    def host_needs_registry_login(self, host: str) -> bool:
        if host in self.mgr.offline_hosts:
            return False
        if host in self.registry_login_queue:
            self.registry_login_queue.remove(host)
            return True
        return False

    def host_metadata_up_to_date(self, host: str) -> bool:
        if host not in self.metadata_up_to_date or not self.metadata_up_to_date[host]:
            return False
        return True

    def all_host_metadata_up_to_date(self) -> bool:
        if [h for h in self.get_hosts() if (not self.host_metadata_up_to_date(h) and not self.is_host_unreachable(h))]:
            # this function is primarily for telling if it's safe to try and apply a service
            # spec. Since offline/maintenance hosts aren't considered in that process anyway
            # we don't want to return False if the host without up-to-date metadata is in one
            # of those two categories.
            return False
        return True

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
            'rotate-key': 6,
        }
        existing_action = self.scheduled_daemon_actions.get(host, {}).get(daemon_name, None)
        if existing_action and priorities[existing_action] > priorities[action]:
            logger.debug(
                f'skipping {action}ing {daemon_name}, cause {existing_action} already scheduled.')
            return

        if host not in self.scheduled_daemon_actions:
            self.scheduled_daemon_actions[host] = {}
        self.scheduled_daemon_actions[host][daemon_name] = action

    def rm_scheduled_daemon_action(self, host: str, daemon_name: str) -> bool:
        found = False
        if host in self.scheduled_daemon_actions:
            if daemon_name in self.scheduled_daemon_actions[host]:
                del self.scheduled_daemon_actions[host][daemon_name]
                found = True
            if not self.scheduled_daemon_actions[host]:
                del self.scheduled_daemon_actions[host]
        return found

    def get_scheduled_daemon_action(self, host: str, daemon: str) -> Optional[str]:
        assert not daemon.startswith('ha-rgw.')

        return self.scheduled_daemon_actions.get(host, {}).get(daemon)


class NodeProxyCache:
    def __init__(self, mgr: "CephadmOrchestrator") -> None:
        self.mgr = mgr
        self.data: Dict[str, Any] = {}
        self.oob: Dict[str, Any] = {}
        self.keyrings: Dict[str, str] = {}

    def load(self) -> None:
        _oob = self.mgr.get_store(f'{NODE_PROXY_CACHE_PREFIX}/oob', '{}')
        self.oob = json.loads(_oob)

        _keyrings = self.mgr.get_store(f'{NODE_PROXY_CACHE_PREFIX}/keyrings', '{}')
        self.keyrings = json.loads(_keyrings)

        for k, v in self.mgr.get_store_prefix(f'{NODE_PROXY_CACHE_PREFIX}/data').items():
            host = k.split('/')[-1:][0]

            if host not in self.mgr.inventory.keys():
                # remove entry for host that no longer exists
                self.mgr.set_store(f"{NODE_PROXY_CACHE_PREFIX}/data/{host}", None)
                try:
                    self.oob.pop(host)
                    self.data.pop(host)
                    self.keyrings.pop(host)
                except KeyError:
                    pass
                continue

            self.data[host] = json.loads(v)

    def save(self,
             host: str = '',
             data: Dict[str, Any] = {}) -> None:
        self.mgr.set_store(f"{NODE_PROXY_CACHE_PREFIX}/data/{host}", json.dumps(data))

    def update_oob(self, host: str, host_oob_info: Dict[str, str]) -> None:
        self.oob[host] = host_oob_info
        self.mgr.set_store(f"{NODE_PROXY_CACHE_PREFIX}/oob", json.dumps(self.oob))

    def update_keyring(self, host: str, key: str) -> None:
        self.keyrings[host] = key
        self.mgr.set_store(f"{NODE_PROXY_CACHE_PREFIX}/keyrings", json.dumps(self.keyrings))

    def fullreport(self, **kw: Any) -> Dict[str, Any]:
        """
        Retrieves the full report for the specified hostname.

        If a hostname is provided in the keyword arguments, it retrieves the full report
        data for that specific host. If no hostname is provided, it fetches the full
        report data for all hosts available.

        :param kw: Keyword arguments including 'hostname'.
        :type kw: dict

        :return: The full report data for the specified hostname(s).
        :rtype: dict
        """
        hostname = kw.get('hostname')
        hosts = [hostname] if hostname else self.data.keys()
        return {host: self.data[host] for host in hosts}

    def summary(self, **kw: Any) -> Dict[str, Any]:
        """
        Summarizes the health status of components for specified hosts or all hosts.

        Generates a summary of the health status of components for given hosts. If
        no hostname is provided, it generates the health status summary for all hosts.
        It inspects the status of each component and categorizes it as 'ok' or 'error'
        based on the health status of its members.

        :param kw: Keyword arguments including 'hostname'.
        :type kw: dict

        :return: A dictionary containing the health status summary for each specified
                host or all hosts and their components.
        :rtype: Dict[str, Dict[str, str]]
        """
        hostname = kw.get('hostname')
        hosts = [hostname] if hostname else self.data.keys()
        mapper: Dict[bool, str] = {
            True: 'error',
            False: 'ok'
        }

        _result: Dict[str, Any] = {}

        for host in hosts:
            _result[host] = {}
            _result[host]['status'] = {}
            data = self.data[host]
            for component, details in data['status'].items():
                res = any([member['status']['health'].lower() != 'ok' for member in data['status'][component].values()])
                _result[host]['status'][component] = mapper[res]
            _result[host]['sn'] = data['sn']
            _result[host]['host'] = data['host']
            _result[host]['firmwares'] = data['firmwares']
        return _result

    def common(self, endpoint: str, **kw: Any) -> Dict[str, Any]:
        """
        Retrieves specific endpoint information for a specific hostname or all hosts.

        Retrieves information from the specified 'endpoint' for all available hosts.
        If 'hostname' is provided, retrieves the specified 'endpoint' information for that host.

        :param endpoint: The endpoint for which information is retrieved.
        :type endpoint: str
        :param kw: Keyword arguments, including 'hostname' if specified.
        :type kw: dict

        :return: Endpoint information for the specified host(s).
        :rtype: Union[Dict[str, Any], Any]
        """
        hostname = kw.get('hostname')
        _result = {}
        hosts = [hostname] if hostname else self.data.keys()

        for host in hosts:
            try:
                _result[host] = self.data[host]['status'][endpoint]
            except KeyError:
                raise KeyError(f'Invalid host {host} or component {endpoint}.')
        return _result

    def firmwares(self, **kw: Any) -> Dict[str, Any]:
        """
        Retrieves firmware information for a specific hostname or all hosts.

        If a 'hostname' is provided in the keyword arguments, retrieves firmware
        information for that specific host. Otherwise, retrieves firmware
        information for all available hosts.

        :param kw: Keyword arguments, including 'hostname' if specified.
        :type kw: dict

        :return: A dictionary containing firmware information for each host.
        :rtype: Dict[str, Any]
        """
        hostname = kw.get('hostname')
        hosts = [hostname] if hostname else self.data.keys()

        return {host: self.data[host]['firmwares'] for host in hosts}

    def get_critical_from_host(self, hostname: str) -> Dict[str, Any]:
        results: Dict[str, Any] = {}
        for component, data_component in self.data[hostname]['status'].items():
            if component not in results.keys():
                results[component] = {}
            for member, data_member in data_component.items():
                if component == 'power':
                    data_member['status']['health'] = 'critical'
                    data_member['status']['state'] = 'unplugged'
                if component == 'memory':
                    data_member['status']['health'] = 'critical'
                    data_member['status']['state'] = 'errors detected'
                if data_member['status']['health'].lower() != 'ok':
                    results[component][member] = data_member
        return results

    def criticals(self, **kw: Any) -> Dict[str, Any]:
        """
        Retrieves critical information for a specific hostname or all hosts.

        If a 'hostname' is provided in the keyword arguments, retrieves critical
        information for that specific host. Otherwise, retrieves critical
        information for all available hosts.

        :param kw: Keyword arguments, including 'hostname' if specified.
        :type kw: dict

        :return: A dictionary containing critical information for each host.
        :rtype: List[Dict[str, Any]]
        """
        hostname = kw.get('hostname')
        results: Dict[str, Any] = {}

        hosts = [hostname] if hostname else self.data.keys()
        for host in hosts:
            results[host] = self.get_critical_from_host(host)
        return results


class AgentCache():
    """
    AgentCache is used for storing metadata about agent daemons that must be kept
    through MGR failovers
    """

    def __init__(self, mgr):
        # type: (CephadmOrchestrator) -> None
        self.mgr: CephadmOrchestrator = mgr
        self.agent_config_deps = {}   # type: Dict[str, Dict[str,Any]]
        self.agent_counter = {}  # type: Dict[str, int]
        self.agent_timestamp = {}  # type: Dict[str, datetime.datetime]
        self.agent_keys = {}  # type: Dict[str, str]
        self.agent_ports = {}  # type: Dict[str, int]
        self.sending_agent_message = {}  # type: Dict[str, bool]

    def load(self):
        # type: () -> None
        for k, v in self.mgr.get_store_prefix(AGENT_CACHE_PREFIX).items():
            host = k[len(AGENT_CACHE_PREFIX):]
            if host not in self.mgr.inventory:
                self.mgr.log.warning('removing stray AgentCache record for agent on %s' % (
                    host))
                self.mgr.set_store(k, None)
            try:
                j = json.loads(v)
                self.agent_config_deps[host] = {}
                conf_deps = j.get('agent_config_deps', {})
                if conf_deps:
                    conf_deps['last_config'] = str_to_datetime(conf_deps['last_config'])
                self.agent_config_deps[host] = conf_deps
                self.agent_counter[host] = int(j.get('agent_counter', 1))
                self.agent_timestamp[host] = str_to_datetime(
                    j.get('agent_timestamp', datetime_to_str(datetime_now())))
                self.agent_keys[host] = str(j.get('agent_keys', ''))
                agent_port = int(j.get('agent_ports', 0))
                if agent_port:
                    self.agent_ports[host] = agent_port

            except Exception as e:
                self.mgr.log.warning('unable to load cached state for agent on host %s: %s' % (
                    host, e))
                pass

    def save_agent(self, host: str) -> None:
        j: Dict[str, Any] = {}
        if host in self.agent_config_deps:
            j['agent_config_deps'] = {
                'deps': self.agent_config_deps[host].get('deps', []),
                'last_config': datetime_to_str(self.agent_config_deps[host]['last_config']),
            }
        if host in self.agent_counter:
            j['agent_counter'] = self.agent_counter[host]
        if host in self.agent_keys:
            j['agent_keys'] = self.agent_keys[host]
        if host in self.agent_ports:
            j['agent_ports'] = self.agent_ports[host]
        if host in self.agent_timestamp:
            j['agent_timestamp'] = datetime_to_str(self.agent_timestamp[host])

        self.mgr.set_store(AGENT_CACHE_PREFIX + host, json.dumps(j))

    def update_agent_config_deps(self, host: str, deps: List[str], stamp: datetime.datetime) -> None:
        self.agent_config_deps[host] = {
            'deps': deps,
            'last_config': stamp,
        }

    def get_agent_last_config_deps(self, host: str) -> Tuple[Optional[List[str]], Optional[datetime.datetime]]:
        if host in self.agent_config_deps:
            return self.agent_config_deps[host].get('deps', []), \
                self.agent_config_deps[host].get('last_config', None)
        return None, None

    def messaging_agent(self, host: str) -> bool:
        if host not in self.sending_agent_message or not self.sending_agent_message[host]:
            return False
        return True

    def agent_config_successfully_delivered(self, daemon_spec: CephadmDaemonDeploySpec) -> None:
        # agent successfully received new config. Update config/deps
        assert daemon_spec.service_name == 'agent'
        self.update_agent_config_deps(
            daemon_spec.host, daemon_spec.deps, datetime_now())
        self.agent_timestamp[daemon_spec.host] = datetime_now()
        self.agent_counter[daemon_spec.host] = 1
        self.save_agent(daemon_spec.host)


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
                # if subject and message match, just update the timestamp
                e.created = event.created
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
