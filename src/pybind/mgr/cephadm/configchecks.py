import json
import ipaddress
import logging

from mgr_module import ServiceInfoT

from typing import TYPE_CHECKING, Any, Dict, List, Optional, cast, Tuple, Callable

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator

logger = logging.getLogger(__name__)


class HostFacts:

    def __init__(self) -> None:
        self.arch: Optional[str] = None
        self.bios_date: Optional[str] = None
        self.bios_version: Optional[str] = None
        self.cpu_cores: Optional[int] = None
        self.cpu_count: Optional[int] = None
        self.cpu_load: Optional[Dict[str, float]] = None
        self.cpu_model: Optional[str] = None
        self.cpu_threads: Optional[int] = None
        self.flash_capacity: Optional[str] = None
        self.flash_capacity_bytes: Optional[int] = None
        self.flash_count: Optional[int] = None
        self.flash_list: Optional[List[Dict[str, Any]]] = None
        self.hdd_capacity: Optional[str] = None
        self.hdd_capacity_bytes: Optional[int] = None
        self.hdd_count: Optional[int] = None
        self.hdd_list: Optional[List[Dict[str, Any]]] = None
        self.hostname: Optional[str] = None
        self.interfaces: Optional[Dict[str, Dict[str, Any]]] = None
        self.kernel: Optional[str] = None
        self.kernel_parameters: Optional[Dict[str, Any]] = None
        self.kernel_security: Optional[Dict[str, str]] = None
        self.memory_available_kb: Optional[int] = None
        self.memory_free_kb: Optional[int] = None
        self.memory_total_kb: Optional[int] = None
        self.model: Optional[str] = None
        self.nic_count: Optional[int] = None
        self.operating_system: Optional[str] = None
        self.subscribed: Optional[str] = None
        self.system_uptime: Optional[float] = None
        self.timestamp: Optional[float] = None
        self.vendor: Optional[str] = None
        self._valid = False

    def load_facts(self, json_data: Dict[str, Any]) -> None:

        if isinstance(json_data, dict):
            keys = json_data.keys()
            if all([k in keys for k in self.__dict__ if not k.startswith('_')]):
                self._valid = True
                for k in json_data.keys():
                    if hasattr(self, k):
                        setattr(self, k, json_data[k])
            else:
                self._valid = False
        else:
            self._valid = False

    def subnet_to_nic(self, subnet: str) -> Optional[str]:
        ip_version = ipaddress.ip_network(subnet).version
        logger.debug(f"subnet {subnet} is IP version {ip_version}")
        interfaces = cast(Dict[str, Dict[str, Any]], self.interfaces)
        nic = None
        for iface in interfaces.keys():
            addr = ''
            if ip_version == 4:
                addr = interfaces[iface].get('ipv4_address', '')
            else:
                addr = interfaces[iface].get('ipv6_address', '')
            if addr:
                a = addr.split('/')[0]
                if ipaddress.ip_address(a) in ipaddress.ip_network(subnet):
                    nic = iface
                    break
        return nic


class SubnetLookup:
    def __init__(self, subnet: str, hostname: str, mtu: str, speed: str):
        self.subnet = subnet
        self.mtu_map = {
            mtu: [hostname]
        }
        self.speed_map = {
            speed: [hostname]
        }

    @ property
    def host_list(self) -> List[str]:
        hosts = []
        for mtu in self.mtu_map:
            hosts.extend(self.mtu_map.get(mtu, []))
        return hosts

    def update(self, hostname: str, mtu: str, speed: str) -> None:
        if mtu in self.mtu_map and hostname not in self.mtu_map[mtu]:
            self.mtu_map[mtu].append(hostname)
        else:
            self.mtu_map[mtu] = [hostname]

        if speed in self.speed_map and hostname not in self.speed_map[speed]:
            self.speed_map[speed].append(hostname)
        else:
            self.speed_map[speed] = [hostname]

    def __repr__(self) -> str:
        return json.dumps({
            "subnet": self.subnet,
            "mtu_map": self.mtu_map,
            "speed_map": self.speed_map
        })


class CephadmCheckDefinition:
    def __init__(self, mgr: "CephadmOrchestrator", healthcheck_name: str, description: str, name: str, func: Callable) -> None:
        self.mgr = mgr
        self.log = logger
        self.healthcheck_name = healthcheck_name
        self.description = description
        self.name = name
        self.func = func

    @property
    def status(self) -> str:
        check_states: Dict[str, str] = {}
        # Issuing a get each time, since the value could be set at the CLI
        raw_states = self.mgr.get_store('config_checks')
        if not raw_states:
            self.log.error(
                "config_checks setting is not defined - unable to determine healthcheck state")
            return "Unknown"

        try:
            check_states = json.loads(raw_states)
        except json.JSONDecodeError:
            self.log.error("Unable to serialize the config_checks settings to JSON")
            return "Unavailable"

        return check_states.get(self.name, 'Missing')

    def to_json(self) -> Dict[str, Any]:
        return {
            "healthcheck_name": self.healthcheck_name,
            "description": self.description,
            "name": self.name,
            "status": self.status,
            "valid": True if self.func else False
        }


class CephadmConfigChecks:
    def __init__(self, mgr: "CephadmOrchestrator"):
        self.mgr: "CephadmOrchestrator" = mgr
        self.health_checks: List[CephadmCheckDefinition] = [
            CephadmCheckDefinition(mgr, "CEPHADM_CHECK_KERNEL_LSM",
                                   "checks SELINUX/Apparmor profiles are consistent across cluster hosts",
                                   "kernel_security",
                                   self._check_kernel_lsm),
            CephadmCheckDefinition(mgr, "CEPHADM_CHECK_SUBSCRIPTION",
                                   "checks subscription states are consistent for all cluster hosts",
                                   "os_subscription",
                                   self._check_subscription),
            CephadmCheckDefinition(mgr, "CEPHADM_CHECK_PUBLIC_MEMBERSHIP",
                                   "check that all hosts have a NIC on the Ceph public_netork",
                                   "public_network",
                                   self._check_public_network),
            CephadmCheckDefinition(mgr, "CEPHADM_CHECK_MTU",
                                   "check that OSD hosts share a common MTU setting",
                                   "osd_mtu_size",
                                   self._check_osd_mtu),
            CephadmCheckDefinition(mgr, "CEPHADM_CHECK_LINKSPEED",
                                   "check that OSD hosts share a common linkspeed",
                                   "osd_linkspeed",
                                   self._check_osd_linkspeed),
            CephadmCheckDefinition(mgr, "CEPHADM_CHECK_NETWORK_MISSING",
                                   "checks that the cluster/public networks defined exist on the Ceph hosts",
                                   "network_missing",
                                   self._check_network_missing),
            CephadmCheckDefinition(mgr, "CEPHADM_CHECK_CEPH_RELEASE",
                                   "check for Ceph version consistency - ceph daemons should be on the same release (unless upgrade is active)",
                                   "ceph_release",
                                   self._check_release_parity),
            CephadmCheckDefinition(mgr, "CEPHADM_CHECK_KERNEL_VERSION",
                                   "checks that the MAJ.MIN of the kernel on Ceph hosts is consistent",
                                   "kernel_version",
                                   self._check_kernel_version),
        ]
        self.log = logger
        self.host_facts: Dict[str, HostFacts] = {}
        self.subnet_lookup: Dict[str, SubnetLookup] = {}  # subnet CIDR -> SubnetLookup Object
        self.lsm_to_host: Dict[str, List[str]] = {}
        self.subscribed: Dict[str, List[str]] = {
            "yes": [],
            "no": [],
            "unknown": [],
        }
        self.host_to_role: Dict[str, List[str]] = {}
        self.kernel_to_hosts: Dict[str, List[str]] = {}

        self.public_network_list: List[str] = []
        self.cluster_network_list: List[str] = []
        self.health_check_raised = False
        self.active_checks: List[str] = []   # checks enabled and executed
        self.skipped_checks: List[str] = []  # checks enabled, but skipped due to a pre-req failure

        raw_checks = self.mgr.get_store('config_checks')
        if not raw_checks:
            # doesn't exist, so seed the checks
            self.seed_config_checks()
        else:
            # setting is there, so ensure there is an entry for each of the checks that
            # this module supports (account for upgrades/changes)
            try:
                config_checks = json.loads(raw_checks)
            except json.JSONDecodeError:
                self.log.error("Unable to serialize config_checks config. Reset to defaults")
                self.seed_config_checks()
            else:
                # Ensure the config_checks setting is consistent with this module
                from_config = set(config_checks.keys())
                from_module = set([c.name for c in self.health_checks])
                old_checks = from_config.difference(from_module)
                new_checks = from_module.difference(from_config)

                if old_checks:
                    self.log.debug(f"old checks being removed from config_checks: {old_checks}")
                    for i in old_checks:
                        del config_checks[i]
                if new_checks:
                    self.log.debug(f"new checks being added to config_checks: {new_checks}")
                    for i in new_checks:
                        config_checks[i] = 'enabled'

                if old_checks or new_checks:
                    self.log.info(
                        f"config_checks updated: {len(old_checks)} removed, {len(new_checks)} added")
                    self.mgr.set_store('config_checks', json.dumps(config_checks))
                else:
                    self.log.debug("config_checks match module definition")

    def lookup_check(self, key_value: str, key_name: str = 'name') -> Optional[CephadmCheckDefinition]:

        for c in self.health_checks:
            if getattr(c, key_name) == key_value:
                return c
        return None

    @property
    def defined_checks(self) -> int:
        return len(self.health_checks)

    @property
    def active_checks_count(self) -> int:
        return len(self.active_checks)

    def seed_config_checks(self) -> None:
        defaults = {check.name: 'enabled' for check in self.health_checks}
        self.mgr.set_store('config_checks', json.dumps(defaults))

    @property
    def skipped_checks_count(self) -> int:
        return len(self.skipped_checks)

    def to_json(self) -> List[Dict[str, str]]:
        return [check.to_json() for check in self.health_checks]

    def load_network_config(self) -> None:
        ret, out, _err = self.mgr.check_mon_command({
            'prefix': 'config dump',
            'format': 'json'
        })
        assert ret == 0
        js = json.loads(out)
        for item in js:
            if item['name'] == "cluster_network":
                self.cluster_network_list = item['value'].strip().split(',')
            if item['name'] == "public_network":
                self.public_network_list = item['value'].strip().split(',')

        self.log.debug(f"public networks {self.public_network_list}")
        self.log.debug(f"cluster networks {self.cluster_network_list}")

    def _update_subnet(self, subnet: str, hostname: str, nic: Dict[str, Any]) -> None:
        mtu = nic.get('mtu', None)
        speed = nic.get('speed', None)
        if not mtu or not speed:
            return

        this_subnet = self.subnet_lookup.get(subnet, None)
        if this_subnet:
            this_subnet.update(hostname, mtu, speed)
        else:
            self.subnet_lookup[subnet] = SubnetLookup(subnet, hostname, mtu, speed)

    def _update_subnet_lookups(self, hostname: str, devname: str, nic: Dict[str, Any]) -> None:
        if nic['ipv4_address']:
            try:
                iface4 = ipaddress.IPv4Interface(nic['ipv4_address'])
                subnet = str(iface4.network)
            except ipaddress.AddressValueError as e:
                self.log.exception(f"Invalid network on {hostname}, interface {devname} : {str(e)}")
            else:
                self._update_subnet(subnet, hostname, nic)

        if nic['ipv6_address']:
            try:
                iface6 = ipaddress.IPv6Interface(nic['ipv6_address'])
                subnet = str(iface6.network)
            except ipaddress.AddressValueError as e:
                self.log.exception(f"Invalid network on {hostname}, interface {devname} : {str(e)}")
            else:
                self._update_subnet(subnet, hostname, nic)

    def hosts_with_role(self, role: str) -> List[str]:
        host_list = []
        for hostname, roles in self.host_to_role.items():
            if role in roles:
                host_list.append(hostname)
        return host_list

    def reset(self) -> None:
        self.subnet_lookup.clear()
        self.lsm_to_host.clear()
        self.subscribed['yes'] = []
        self.subscribed['no'] = []
        self.subscribed['unknown'] = []
        self.host_to_role.clear()
        self.kernel_to_hosts.clear()

    def _get_majority(self, data: Dict[str, List[str]]) -> Tuple[str, int]:
        assert isinstance(data, dict)

        majority_key = ''
        majority_count = 0
        for key in data:
            if len(data[key]) > majority_count:
                majority_count = len(data[key])
                majority_key = key
        return majority_key, majority_count

    def get_ceph_metadata(self) -> Dict[str, Optional[Dict[str, str]]]:
        """Build a map of service -> service metadata"""
        service_map: Dict[str, Optional[Dict[str, str]]] = {}

        for server in self.mgr.list_servers():
            for service in cast(List[ServiceInfoT], server.get('services', [])):
                if service:
                    service_map.update(
                        {
                            f"{service['type']}.{service['id']}":
                            self.mgr.get_metadata(service['type'], service['id'])
                        }
                    )
        return service_map

    def _check_kernel_lsm(self) -> None:
        if len(self.lsm_to_host.keys()) > 1:

            majority_hosts_ptr, majority_hosts_count = self._get_majority(self.lsm_to_host)
            lsm_copy = self.lsm_to_host.copy()
            del lsm_copy[majority_hosts_ptr]
            details = []
            for lsm_key in lsm_copy.keys():
                for host in lsm_copy[lsm_key]:
                    details.append(
                        f"{host} has inconsistent KSM settings compared to the "
                        f"majority of hosts({majority_hosts_count}) in the cluster")
            host_sfx = 's' if len(details) > 1 else ''
            self.mgr.health_checks['CEPHADM_CHECK_KERNEL_LSM'] = {
                'severity': 'warning',
                'summary': f"Kernel Security Module (SELinux/AppArmor) is inconsistent for "
                           f"{len(details)} host{host_sfx}",
                'count': len(details),
                'detail': details,
            }
            self.health_check_raised = True
        else:
            self.mgr.health_checks.pop('CEPHADM_CHECK_KERNEL_LSM', None)

    def _check_subscription(self) -> None:
        if len(self.subscribed['yes']) > 0 and len(self.subscribed['no']) > 0:
            # inconsistent subscription states - CEPHADM_CHECK_SUBSCRIPTION
            details = []
            for host in self.subscribed['no']:
                details.append(f"{host} does not have an active subscription")
            self.mgr.health_checks['CEPHADM_CHECK_SUBSCRIPTION'] = {
                'severity': 'warning',
                'summary': f"Support subscriptions inactive on {len(details)} host(s)"
                           f"({len(self.subscribed['yes'])} subscriptions active)",
                'count': len(details),
                'detail': details,
            }
            self.health_check_raised = True
        else:
            self.mgr.health_checks.pop('CEPHADM_CHECK_SUBSCRIPTION', None)

    def _check_public_network(self) -> None:
        hosts_remaining: List[str] = list(self.mgr.cache.facts.keys())
        hosts_removed: List[str] = []
        self.log.debug(f"checking public network membership for: {hosts_remaining}")

        for p_net in self.public_network_list:
            self.log.debug(f"checking network {p_net}")
            subnet_data = self.subnet_lookup.get(p_net, None)
            self.log.debug(f"subnet data - {subnet_data}")

            if subnet_data:
                hosts_in_subnet = subnet_data.host_list
                for host in hosts_in_subnet:
                    if host in hosts_remaining:
                        hosts_remaining.remove(host)
                        hosts_removed.append(host)
                    else:
                        if host not in hosts_removed:
                            self.log.debug(f"host={host}, subnet={p_net}")
                            self.log.exception(
                                "Host listed for a subnet but not present in the host facts?")

        # Ideally all hosts will have been removed since they have an IP on at least
        # one of the public networks
        if hosts_remaining:
            if len(hosts_remaining) != len(self.mgr.cache.facts):
                # public network is visible on some hosts
                details = [
                    f"{host} does not have an interface on any public network" for host in hosts_remaining]

                self.mgr.health_checks['CEPHADM_CHECK_PUBLIC_MEMBERSHIP'] = {
                    'severity': 'warning',
                    'summary': f"Public network(s) is not directly accessible from {len(hosts_remaining)} "
                                "cluster hosts",
                    'count': len(details),
                    'detail': details,
                }
                self.health_check_raised = True
        else:
            self.mgr.health_checks.pop('CEPHADM_CHECK_PUBLIC_MEMBERSHIP', None)

    def _check_osd_mtu(self) -> None:
        osd_hosts = set(self.hosts_with_role('osd'))
        osd_network_list = self.cluster_network_list or self.public_network_list
        mtu_errors: List[str] = []

        for osd_net in osd_network_list:
            subnet_data = self.subnet_lookup.get(osd_net, None)

            if subnet_data:

                self.log.debug(f"processing mtu map : {json.dumps(subnet_data.mtu_map)}")
                mtu_count = {}
                max_hosts = 0
                mtu_ptr = ''
                diffs = {}
                for mtu, host_list in subnet_data.mtu_map.items():
                    mtu_hosts = set(host_list)
                    mtu_count[mtu] = len(mtu_hosts)
                    errors = osd_hosts.difference(mtu_hosts)
                    if errors:
                        diffs[mtu] = errors
                    if len(errors) > max_hosts:
                        mtu_ptr = mtu

                if diffs:
                    self.log.debug("MTU problems detected")
                    self.log.debug(f"most hosts using {mtu_ptr}")
                    mtu_copy = subnet_data.mtu_map.copy()
                    del mtu_copy[mtu_ptr]
                    for bad_mtu in mtu_copy:
                        for h in mtu_copy[bad_mtu]:
                            host = HostFacts()
                            host.load_facts(self.mgr.cache.facts[h])
                            mtu_errors.append(
                                f"host {h}({host.subnet_to_nic(osd_net)}) is using MTU "
                                f"{bad_mtu} on {osd_net}, NICs on other hosts use {mtu_ptr}")

        if mtu_errors:
            self.mgr.health_checks['CEPHADM_CHECK_MTU'] = {
                'severity': 'warning',
                'summary': f"MTU setting inconsistent on osd network NICs on {len(mtu_errors)} host(s)",
                'count': len(mtu_errors),
                'detail': mtu_errors,
            }
            self.health_check_raised = True
        else:
            self.mgr.health_checks.pop('CEPHADM_CHECK_MTU', None)

    def _check_osd_linkspeed(self) -> None:
        osd_hosts = set(self.hosts_with_role('osd'))
        osd_network_list = self.cluster_network_list or self.public_network_list

        linkspeed_errors = []

        for osd_net in osd_network_list:
            subnet_data = self.subnet_lookup.get(osd_net, None)

            if subnet_data:

                self.log.debug(f"processing subnet : {subnet_data}")

                speed_count = {}
                max_hosts = 0
                speed_ptr = ''
                diffs = {}
                for speed, host_list in subnet_data.speed_map.items():
                    speed_hosts = set(host_list)
                    speed_count[speed] = len(speed_hosts)
                    errors = osd_hosts.difference(speed_hosts)
                    if errors:
                        diffs[speed] = errors
                        if len(errors) > max_hosts:
                            speed_ptr = speed

                if diffs:
                    self.log.debug("linkspeed issue(s) detected")
                    self.log.debug(f"most hosts using {speed_ptr}")
                    speed_copy = subnet_data.speed_map.copy()
                    del speed_copy[speed_ptr]
                    for bad_speed in speed_copy:
                        if bad_speed > speed_ptr:
                            # skip speed is better than most...it can stay!
                            continue
                        for h in speed_copy[bad_speed]:
                            host = HostFacts()
                            host.load_facts(self.mgr.cache.facts[h])
                            linkspeed_errors.append(
                                f"host {h}({host.subnet_to_nic(osd_net)}) has linkspeed of "
                                f"{bad_speed} on {osd_net}, NICs on other hosts use {speed_ptr}")

        if linkspeed_errors:
            self.mgr.health_checks['CEPHADM_CHECK_LINKSPEED'] = {
                'severity': 'warning',
                'summary': "Link speed is inconsistent on osd network NICs for "
                           f"{len(linkspeed_errors)} host(s)",
                'count': len(linkspeed_errors),
                'detail': linkspeed_errors,
            }
            self.health_check_raised = True
        else:
            self.mgr.health_checks.pop('CEPHADM_CHECK_LINKSPEED', None)

    def _check_network_missing(self) -> None:
        all_networks = self.public_network_list.copy()
        all_networks.extend(self.cluster_network_list)

        missing_networks = []
        for subnet in all_networks:
            subnet_data = self.subnet_lookup.get(subnet, None)

            if not subnet_data:
                missing_networks.append(f"{subnet} not found on any host in the cluster")
                self.log.warning(
                    f"Network {subnet} has been defined, but is not present on any host")

        if missing_networks:
            net_sfx = 's' if len(missing_networks) > 1 else ''
            self.mgr.health_checks['CEPHADM_CHECK_NETWORK_MISSING'] = {
                'severity': 'warning',
                'summary': f"Public/cluster network{net_sfx} defined, but can not be found on "
                           "any host",
                'count': len(missing_networks),
                'detail': missing_networks,
            }
            self.health_check_raised = True
        else:
            self.mgr.health_checks.pop('CEPHADM_CHECK_NETWORK_MISSING', None)

    def _check_release_parity(self) -> None:
        upgrade_status = self.mgr.upgrade.upgrade_status()
        if upgrade_status.in_progress:
            # skip version consistency checks during an upgrade cycle
            self.skipped_checks.append('ceph_release')
            return

        services = self.get_ceph_metadata()
        self.log.debug(json.dumps(services))
        version_to_svcs: Dict[str, List[str]] = {}

        for svc in services:
            if services[svc]:
                metadata = cast(Dict[str, str], services[svc])
                v = metadata.get('ceph_release', '')
                if v in version_to_svcs:
                    version_to_svcs[v].append(svc)
                else:
                    version_to_svcs[v] = [svc]

        if len(version_to_svcs) > 1:
            majority_ptr, _majority_count = self._get_majority(version_to_svcs)
            ver_copy = version_to_svcs.copy()
            del ver_copy[majority_ptr]
            details = []
            for v in ver_copy:
                for svc in ver_copy[v]:
                    details.append(
                        f"{svc} is running {v} (majority of cluster is using {majority_ptr})")

            self.mgr.health_checks['CEPHADM_CHECK_CEPH_RELEASE'] = {
                'severity': 'warning',
                'summary': 'Ceph cluster running mixed ceph releases',
                'count': len(details),
                'detail': details,
            }
            self.health_check_raised = True
            self.log.warning(
                f"running with {len(version_to_svcs)} different ceph releases within this cluster")
        else:
            self.mgr.health_checks.pop('CEPHADM_CHECK_CEPH_RELEASE', None)

    def _check_kernel_version(self) -> None:
        if len(self.kernel_to_hosts.keys()) > 1:
            majority_hosts_ptr, majority_hosts_count = self._get_majority(self.kernel_to_hosts)
            kver_copy = self.kernel_to_hosts.copy()
            del kver_copy[majority_hosts_ptr]
            details = []
            for k in kver_copy:
                for h in kver_copy[k]:
                    details.append(
                        f"host {h} running kernel {k}, majority of hosts({majority_hosts_count}) "
                        f"running {majority_hosts_ptr}")

            self.log.warning("mixed kernel versions detected")
            self.mgr.health_checks['CEPHADM_CHECK_KERNEL_VERSION'] = {
                'severity': 'warning',
                'summary': f"{len(details)} host(s) running different kernel versions",
                'count': len(details),
                'detail': details,
            }
            self.health_check_raised = True
        else:
            self.mgr.health_checks.pop('CEPHADM_CHECK_KERNEL_VERSION', None)

    def _process_hosts(self) -> None:
        self.log.debug(f"processing data from {len(self.mgr.cache.facts)} hosts")
        for hostname in self.mgr.cache.facts:
            host = HostFacts()
            host.load_facts(self.mgr.cache.facts[hostname])
            if not host._valid:
                self.log.warning(f"skipping {hostname} - incompatible host facts")
                continue

            kernel_lsm = cast(Dict[str, str], host.kernel_security)
            lsm_desc = kernel_lsm.get('description', '')
            if lsm_desc:
                if lsm_desc in self.lsm_to_host:
                    self.lsm_to_host[lsm_desc].append(hostname)
                else:
                    self.lsm_to_host[lsm_desc] = [hostname]

            subscription_state = host.subscribed.lower() if host.subscribed else None
            if subscription_state:
                self.subscribed[subscription_state].append(hostname)

            interfaces = cast(Dict[str, Dict[str, Any]], host.interfaces)
            for name in interfaces.keys():
                if name in ['lo']:
                    continue
                self._update_subnet_lookups(hostname, name, interfaces[name])

            if host.kernel:
                kernel_maj_min = '.'.join(host.kernel.split('.')[0:2])
                if kernel_maj_min in self.kernel_to_hosts:
                    self.kernel_to_hosts[kernel_maj_min].append(hostname)
                else:
                    self.kernel_to_hosts[kernel_maj_min] = [hostname]
            else:
                self.log.warning(f"Host gather facts for {hostname} is missing kernel information")

            # NOTE: if daemondescription had systemd enabled state, we could check for systemd 'tampering'
            self.host_to_role[hostname] = list(self.mgr.cache.get_daemon_types(hostname))

    def run_checks(self) -> None:
        checks_enabled = self.mgr.config_checks_enabled
        if checks_enabled is not True:
            return

        self.reset()

        check_config: Dict[str, str] = {}
        checks_raw: Optional[str] = self.mgr.get_store('config_checks')
        if checks_raw:
            try:
                check_config.update(json.loads(checks_raw))
            except json.JSONDecodeError:
                self.log.exception(
                    "mgr/cephadm/config_checks is not JSON serializable - all checks will run")

        # build lookup "maps" by walking the host facts, once
        self._process_hosts()

        self.health_check_raised = False
        self.active_checks = []
        self.skipped_checks = []

        # process all healthchecks that are not explcitly disabled
        for health_check in self.health_checks:
            if check_config.get(health_check.name, '') != 'disabled':
                self.active_checks.append(health_check.name)
                health_check.func()

        self.mgr.set_health_checks(self.mgr.health_checks)
