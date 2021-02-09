# import ssl
import json
# import sys
# import tempfile
# import time
# import socket
import ipaddress
import logging
# import inspect
# from threading import Event
# from urllib.request import Request, urlopen

from typing import Any, Dict, List, Set

logger = logging.getLogger(__name__)


class Hosts:
    def __init__(self):
        self.map = {}

    def add_host(self, hostname: str, hostData: Dict[str, Any]) -> None:
        return None

    def hosts_with_role(self, role: str) -> List[str]:
        return []


class HostData:
    def __init__(self, json_data: Dict[str, Any]):
        data = json_data.get('data', None)
        assert data
        self.valid = True if data and len(data.keys()) > 1 else False
        if self.valid:
            data = json_data['data']
            for k in data:
                setattr(self, k, data[k])


class CephHost:
    def __init__(self, json_data: Dict[str, Any]):
        self.os = HostData(json_data.get('host', {}))
        daemon_section = json_data.get('daemons', {})
        self.daemons = daemon_section['data'] if 'data' in daemon_section else []


class SubnetLookup:
    def __init__(self, subnet, hostname, mtu, speed):
        self.subnet = subnet
        self.mtu_map = {
            mtu: [hostname]
        }
        self.speed_map = {
            speed: [hostname]
        }

    @property
    def host_list(self) -> List[str]:
        return self.mtu_map[next(iter(self.mtu_map))]

    def update(self, hostname, mtu, speed) -> None:
        if mtu in self.mtu_map and hostname not in self.mtu_map[mtu]:
            self.mtu_map[mtu].append(hostname)
        else:
            self.mtu_map[mtu] = [hostname]

        if speed in self.speed_map and hostname not in self.speed_map[speed]:
            self.speed_map[speed].append(hostname)
        else:
            self.speed_map[speed] = [hostname]

    def __repr__(self) -> Dict[str, Any]:
        return json.dumps({
            "subnet": self.subnet,
            "mtu_map": self.mtu_map,
            "speed_map": self.speed_map
        })


class CephadmConfigChecks:
    def __init__(self, mgr):
        self.mgr = mgr
        self.log = logger
        self.healthchecks = {
            "CEPHADM_CHECK_OS": [],
            "CEPHADM_CHECK_SUBSCRIPTION": [],
            "CEPHADM_CHECK_KERNEL_LSM": [],
            "CEPHADM_CHECK_MTU": [],
            "CEPHADM_CHECK_LINKSPEED": [],
            "CEPHADM_CHECK_PUBLIC_MEMBERSHIP": [],
            "CEPHADM_CHECK_DAEMON_DISABLED": [],
        }
        self.subnet_lookup = {}  # subnet CIDR -> SubnetLookup Object

        self.public_network_list = None
        self.cluster_network_list = None
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

    def role_from_name(self, name: str) -> str:
        return name.split('.')[0]

    def hosts_with_role(self, role_name: str) -> List[str]:
        host_list = []
        for hostname in self.mgr.cache.facts:
            host = CephHost(self.mgr.cache.facts[hostname])
            for d in host.daemons:
                if self.role_from_name(d['name']) == role_name:
                    host_list.append(hostname)
        return host_list

    def host_roles(self, hostname: str) -> List[str]:
        role_list = []
        host = CephHost(self.mgr.cache.facts[hostname])
        for d in host.daemons:
            role_list.append(self.role_from_name(d['name']))
        return role_list

    def run_checks(self) -> None:

        # os_to_host = {}  # TODO needs better support in gather-facts
        lsm_to_host = {}
        subscribed: Dict[str, List[str]] = {
            "Yes": [],
            "No": [],
        }
        host_to_role = {}
        disabled_daemons = {}
        osd_hosts = set()

        # build lookup "maps"
        for hostname in self.mgr.cache.facts:
            self.log.error(json.dumps(self.mgr.cache.facts[hostname]))
        #     host = CephHost(self.mgr.cache.facts[hostname])
        #     # self.log.error(f"object contains {list(host.os.__dict__.keys())}")
        #     # self.log.error(f"sec {host.os.kernel_security['description']}")
        #     lsm_to_host[host.os.kernel_security['description']] = hostname
        #     # self.log.error(f"{host.os.subscribed}")
        #     subscribed[host.os.subscribed].append(hostname)

        #     interfaces = host.os.interfaces
        #     for name in interfaces:
        #         if name in ['lo']:
        #             continue
        #         self._update_subnet_lookups(hostname, name, interfaces[name])

        #     roles: Set[str] = set()
        #     # for d in host.daemons:
        #     #     roles.add(self.role_from_name(d['name'])
        #     #     if not d['enabled']:
        #     #         unit_name=d['systemd_unit']
        #     #         disabled_daemons[unit_name]=hostname

        #     host_to_role[hostname] = list(roles)

        # osd_hosts = self.hosts_with_role('osd')

        # # checks
        # if len(lsm_to_host.keys()) > 1:
        #     # selinux security policy issue - CEPHADM_CHECK_KERNEL_LSM
        #     pass
        # if len(subscribed['Yes']) > 0 and len(subscribed['No']) > 0:
        #     # inconsistent subscription states - CEPHADM_CHECK_SUBSCRIPTION
        #     pass

        # # all hosts must have NIC on the public network
        # all_hosts = set(self.mgr.cache.facts.keys())
        # for c_net in self.public_network_list:
        #     subnet_data = self.subnet_lookup.get(c_net, None)
        #     if subnet_data:
        #         hosts_in_subnet = set(subnet_data.host_list)
        #         errors = all_hosts.difference(hosts_in_subnet)
        #         if errors:
        #             # CEPHADM_CHECK_PUBLIC_MEMBERSHIP
        #             self.log.error(f"subnet check for {c_net} found {len(errors)} host(s) missing")

        # # all hosts with the osd role must share a common mtu and speed
        # osd_network_list = self.cluster_network_list or self.public_network_list
        # for osd_net in osd_network_list:
        #     subnet_data = self.subnet_lookup.get(osd_net, None)
        #     if subnet_data:

        #         # FIXME this feels overly complex
        #         # what do I want to show as the error?
        #         for mtu, host_list in subnet_data.mtu_map.items():
        #             mtu_hosts = set(host_list)
        #             errors = osd_hosts.difference(mtu_hosts)
        #             if errors:
        #                 self.log.error(f"MTU problem {mtu}: {errors}")
        #                 # CEPHADM_CHECK_MTU
        #                 pass
        #         for speed, host_list in subnet_data.speed_map.items():
        #             speed_hosts = set(host_list)
        #             errors = osd_hosts.difference(speed_hosts)
        #             if errors:
        #                 self.log.error(f"Linkspeed problem {speed} : {errors}")
        #                 # CEPHADM_CHECK_LINKSPEED
        #                 pass

        #     else:
        #         self.log.warning(
        #             f"Network {osd_net} has been defined, but is not present on any host")

        # # All daemons should be in an enabled state
        # if disabled_daemons:
        #     self.log.error("disabled daemons found")
        #     # CEPHADM_CHECK_DAEMON_DISABLED
        #     pass

        # self.log.error(f"lsm status {json.dumps(lsm_to_host)}")
        # self.log.error(f"subscription states : {json.dumps(subscribed)}")

        # self.log.error(f"mtu data : {str(self.subnet_lookup)}")
        # self.log.error(f"roles : {json.dumps(host_to_role)}")
