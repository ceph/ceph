import json
import ipaddress
import logging

from typing import TYPE_CHECKING, Any, Dict, List, Optional

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
        self.interfaces: Dict[str, Dict[str, Any]] = {}
        self.kernel: Optional[str] = None
        self.kernel_parameters: Optional[Dict[str, Any]] = None
        self.kernel_security: Dict[str, str] = {}
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
        return self.mtu_map[next(iter(self.mtu_map))]

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


class CephadmConfigChecks:
    def __init__(self, mgr: "CephadmOrchestrator"):
        self.mgr: "CephadmOrchestrator" = mgr
        self.log = logger
        self.healthchecks: Dict[str, List[str]] = {
            "CEPHADM_CHECK_OS": [],
            "CEPHADM_CHECK_SUBSCRIPTION": [],
            "CEPHADM_CHECK_KERNEL_LSM": [],
            "CEPHADM_CHECK_MTU": [],
            "CEPHADM_CHECK_LINKSPEED": [],
            "CEPHADM_CHECK_PUBLIC_MEMBERSHIP": [],
            "CEPHADM_CHECK_NETWORK_MISSING": [],
            "CEPHADM_CHECK_DAEMON_DISABLED": [],
            "CEPHADM_CHECK_CEPH_VERSION": [],
            "CEPHADM_CHECK_KERNEL_VERSION": [],
        }
        self.subnet_lookup: Dict[str, SubnetLookup] = {}  # subnet CIDR -> SubnetLookup Object
        self.lsm_to_host: Dict[str, List[str]] = {}
        self.subscribed: Dict[str, List[str]] = {
            "yes": [],
            "no": [],
        }
        self.host_to_role: Dict[str, List[Optional[str]]] = {}
        self.kernel_to_hosts: Dict[str, List[str]] = {}

        self.public_network_list: List[str] = []
        self.cluster_network_list: List[str] = []

    def load_network_config(self) -> None:
        ret, out, _err = self.mgr.mon_command({
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

        self.log.error(f"public networks {self.public_network_list}")
        self.log.error(f"cluster networks {self.cluster_network_list}")

    def _update_subnet_lookups(self, hostname: str, devname: str, nic: Dict[str, Any]) -> None:
        if nic['ipv4_address']:
            try:
                iface = ipaddress.IPv4Interface(nic['ipv4_address'])
                subnet = str(iface.network)
            except ipaddress.AddressValueError as e:
                self.log.error(f"Invalid network on {hostname}, interface {devname} : {str(e)}")
                return

            if subnet:
                mtu = nic.get('mtu', None)
                speed = nic.get('speed', None)
                if not mtu or not speed:
                    return

                this_subnet = self.subnet_lookup.get(subnet, None)
                if this_subnet:
                    this_subnet.update(hostname, mtu, speed)
                else:
                    self.subnet_lookup[subnet] = SubnetLookup(subnet, hostname, mtu, speed)

        if nic['ipv6_address']:
            pass

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
        self.host_to_role.clear()
        self.kernel_to_hosts.clear()

    def _check_kernel_lsm(self) -> None:
        if len(self.lsm_to_host.keys()) > 1:
            # selinux security policy issue - CEPHADM_CHECK_KERNEL_LSM
            pass

    def _check_subscription(self) -> None:
        if len(self.subscribed['yes']) > 0 and len(self.subscribed['no']) > 0:
            # inconsistent subscription states - CEPHADM_CHECK_SUBSCRIPTION
            pass

    def _check_public_network(self) -> None:
        all_hosts = set(self.mgr.cache.facts.keys())
        for c_net in self.public_network_list:
            subnet_data = self.subnet_lookup.get(c_net, None)
            if subnet_data:
                hosts_in_subnet = set(subnet_data.host_list)
                errors = all_hosts.difference(hosts_in_subnet)
                if errors:
                    # CEPHADM_CHECK_PUBLIC_MEMBERSHIP
                    self.log.error(f"subnet check for {c_net} found {len(errors)} host(s) missing")

    def _check_osd_network(self) -> None:
        osd_hosts = set(self.hosts_with_role('osd'))
        osd_network_list = self.cluster_network_list or self.public_network_list
        for osd_net in osd_network_list:
            subnet_data = self.subnet_lookup.get(osd_net, None)
            if subnet_data:

                for mtu, host_list in subnet_data.mtu_map.items():
                    mtu_hosts = set(host_list)
                    errors = osd_hosts.difference(mtu_hosts)
                    if errors:
                        self.log.error(f"MTU problem {mtu}: {errors}")
                        # CEPHADM_CHECK_MTU
                        pass
                for speed, host_list in subnet_data.speed_map.items():
                    speed_hosts = set(host_list)
                    errors = osd_hosts.difference(speed_hosts)
                    if errors:
                        self.log.error(f"Linkspeed problem {speed} : {errors}")
                        # CEPHADM_CHECK_LINKSPEED
                        pass

            else:
                # set CEPHADM_CHECK_NETWORK_MISSING healthcheck for this subnet
                self.log.warning(
                    f"Network {osd_net} has been defined, but is not present on any host")

    def _check_version_parity(self) -> None:
        upgrade_status = self.mgr.upgrade.upgrade_status()
        if upgrade_status.in_progress:
            # skip version consistency checks during an upgrade cycle
            return
        # service_map = self.mgr.get('service_map')
        # CEPHADM_CHECK_CEPH_VERSION
        pass

    def _check_kernel_version(self) -> None:
        if len(self.kernel_to_hosts.keys()) > 1:
            self.log.warning("mixed kernel versions")
            # CEPHADM_CHECK_KERNEL_VERSION

    def run_checks(self) -> None:

        # os_to_host = {}  # TODO needs better support in gather-facts
        self.reset()

        # build lookup "maps" by walking the host facts
        for hostname in self.mgr.cache.facts:
            self.log.error(json.dumps(self.mgr.cache.facts[hostname]))
            host = HostFacts()
            host.load_facts(self.mgr.cache.facts[hostname])
            if not host._valid:
                self.log.warning(f"skipping {hostname} - incompatible host facts")
                continue

            lsm_desc = host.kernel_security.get('description', '')
            if lsm_desc:
                if lsm_desc in self.lsm_to_host:
                    self.lsm_to_host[lsm_desc].append(hostname)
                else:
                    self.lsm_to_host[lsm_desc] = [hostname]

            subscription_state = host.subscribed.lower() if host.subscribed else None
            if subscription_state:
                self.subscribed[subscription_state].append(hostname)

            interfaces: Dict[str, Dict[str, Any]] = host.interfaces
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

            host_daemons = self.mgr.cache.daemons[hostname]

            # NOTE: should daemondescription provide the systemd enabled state?
            self.host_to_role[hostname] = list({host_daemons[name].daemon_type
                                                for name in host_daemons})

        # checks
        # TODO only call the check if the check is enabled
        self._check_kernel_lsm()
        self._check_subscription()
        self._check_public_network()
        self._check_osd_network()
        self._check_version_parity()
        self._check_kernel_version()

        self.log.error(f"lsm status {json.dumps(self.lsm_to_host)}")
        self.log.error(f"subscription states : {json.dumps(self.subscribed)}")

        self.log.error(f"mtu data : {str(self.subnet_lookup)}")
        self.log.error(f"roles : {json.dumps(self.host_to_role)}")
        self.log.error(f"kernel versions : {json.dumps(self.kernel_to_hosts)}")
