import copy
import json
import logging
import ipaddress
import pytest
import uuid

from time import time as now

from ..configchecks import CephadmConfigChecks
from ..inventory import HostCache
from ..upgrade import CephadmUpgrade, UpgradeState
from orchestrator import DaemonDescription

from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

host_sample = {
    "arch": "x86_64",
    "bios_date": "04/01/2014",
    "bios_version": "F2",
    "cpu_cores": 16,
    "cpu_count": 2,
    "cpu_load": {
            "15min": 0.0,
            "1min": 0.01,
            "5min": 0.01
    },
    "cpu_model": "Intel® Xeon® Processor E5-2698 v3",
    "cpu_threads": 64,
    "flash_capacity": "4.0TB",
    "flash_capacity_bytes": 4000797868032,
    "flash_count": 2,
    "flash_list": [
        {
            "description": "ATA CT2000MX500SSD1 (2.0TB)",
            "dev_name": "sda",
            "disk_size_bytes": 2000398934016,
            "model": "CT2000MX500SSD1",
            "rev": "023",
            "vendor": "ATA",
            "wwid": "t10.ATA     CT2000MX500SSD1                         193023156DE0"
        },
        {
            "description": "ATA CT2000MX500SSD1 (2.0TB)",
            "dev_name": "sdb",
            "disk_size_bytes": 2000398934016,
            "model": "CT2000MX500SSD1",
            "rev": "023",
            "vendor": "ATA",
            "wwid": "t10.ATA     CT2000MX500SSD1                         193023156DE0"
        },
    ],
    "hdd_capacity": "16.0TB",
    "hdd_capacity_bytes": 16003148120064,
    "hdd_count": 4,
    "hdd_list": [
        {
                    "description": "ST4000VN008-2DR1 (4.0TB)",
                    "dev_name": "sdc",
                    "disk_size_bytes": 4000787030016,
                    "model": "ST4000VN008-2DR1",
                    "rev": "SC60",
                    "vendor": "ATA",
                    "wwid": "t10.ATA     ST4000VN008-2DR1                                  Z340EPBJ"
        },
        {
            "description": "ST4000VN008-2DR1 (4.0TB)",
            "dev_name": "sdd",
            "disk_size_bytes": 4000787030016,
            "model": "ST4000VN008-2DR1",
            "rev": "SC60",
            "vendor": "ATA",
            "wwid": "t10.ATA     ST4000VN008-2DR1                                  Z340EPBJ"
        },
        {
            "description": "ST4000VN008-2DR1 (4.0TB)",
            "dev_name": "sde",
            "disk_size_bytes": 4000787030016,
            "model": "ST4000VN008-2DR1",
            "rev": "SC60",
            "vendor": "ATA",
            "wwid": "t10.ATA     ST4000VN008-2DR1                                  Z340EPBJ"
        },
        {
            "description": "ST4000VN008-2DR1 (4.0TB)",
            "dev_name": "sdf",
            "disk_size_bytes": 4000787030016,
            "model": "ST4000VN008-2DR1",
            "rev": "SC60",
            "vendor": "ATA",
            "wwid": "t10.ATA     ST4000VN008-2DR1                                  Z340EPBJ"
        },
    ],
    "hostname": "dummy",
    "interfaces": {
        "eth0": {
                "driver": "e1000e",
                "iftype": "physical",
                "ipv4_address": "10.7.17.1/24",
                "ipv6_address": "fe80::215:17ff:feab:50e2/64",
                "lower_devs_list": [],
                "mtu": 9000,
                "nic_type": "ethernet",
                "operstate": "up",
                "speed": 1000,
                "upper_devs_list": [],
        },
        "eth1": {
            "driver": "e1000e",
            "iftype": "physical",
            "ipv4_address": "10.7.18.1/24",
            "ipv6_address": "fe80::215:17ff:feab:50e2/64",
            "lower_devs_list": [],
            "mtu": 9000,
            "nic_type": "ethernet",
            "operstate": "up",
            "speed": 1000,
            "upper_devs_list": [],
        },
        "eth2": {
            "driver": "r8169",
            "iftype": "physical",
            "ipv4_address": "10.7.19.1/24",
            "ipv6_address": "fe80::76d4:35ff:fe58:9a79/64",
            "lower_devs_list": [],
            "mtu": 1500,
            "nic_type": "ethernet",
            "operstate": "up",
            "speed": 1000,
            "upper_devs_list": []
        },
    },
    "kernel": "4.18.0-240.10.1.el8_3.x86_64",
    "kernel_parameters": {
        "net.ipv4.ip_nonlocal_bind": "0",
    },
    "kernel_security": {
        "SELINUX": "enforcing",
        "SELINUXTYPE": "targeted",
        "description": "SELinux: Enabled(enforcing, targeted)",
        "type": "SELinux"
    },
    "memory_available_kb": 19489212,
    "memory_free_kb": 245164,
    "memory_total_kb": 32900916,
    "model": "StorageHeavy",
    "nic_count": 3,
    "operating_system": "Red Hat Enterprise Linux 8.3 (Ootpa)",
    "subscribed": "Yes",
    "system_uptime": 777600.0,
    "timestamp": now(),
    "vendor": "Ceph Servers Inc",
}


def role_list(n: int) -> List[str]:
    if n == 1:
        return ['mon', 'mgr', 'osd']
    if n in [2, 3]:
        return ['mon', 'mds', 'osd']

    return ['osd']


def generate_testdata(count: int = 10, public_network: str = '10.7.17.0/24', cluster_network: str = '10.7.18.0/24'):
    # public network = eth0, cluster_network = eth1
    assert count > 3
    assert public_network
    num_disks = host_sample['hdd_count']
    hosts = {}
    daemons = {}
    daemon_to_host = {}
    osd_num = 0
    public_netmask = public_network.split('/')[1]
    cluster_ip_list = []
    cluster_netmask = ''

    public_ip_list = [str(i) for i in list(ipaddress.ip_network(public_network).hosts())]
    if cluster_network:
        cluster_ip_list = [str(i) for i in list(ipaddress.ip_network(cluster_network).hosts())]
        cluster_netmask = cluster_network.split('/')[1]

    for n in range(1, count + 1, 1):

        new_host = copy.deepcopy(host_sample)
        hostname = f"node-{n}.ceph.com"

        new_host['hostname'] = hostname
        new_host['interfaces']['eth0']['ipv4_address'] = f"{public_ip_list.pop(0)}/{public_netmask}"
        if cluster_ip_list:
            new_host['interfaces']['eth1']['ipv4_address'] = f"{cluster_ip_list.pop(0)}/{cluster_netmask}"
        else:
            new_host['interfaces']['eth1']['ipv4_address'] = ''

        hosts[hostname] = new_host
        daemons[hostname] = {}
        for r in role_list(n):
            name = ''
            if r == 'osd':
                for n in range(num_disks):
                    osd = DaemonDescription(
                        hostname=hostname, daemon_type='osd', daemon_id=osd_num)
                    name = f"osd.{osd_num}"
                    daemons[hostname][name] = osd
                    daemon_to_host[name] = hostname
                    osd_num += 1
            else:
                name = f"{r}.{hostname}"
                daemons[hostname][name] = DaemonDescription(
                    hostname=hostname, daemon_type=r, daemon_id=hostname)
                daemon_to_host[name] = hostname

    logger.debug(f"daemon to host lookup - {json.dumps(daemon_to_host)}")
    return hosts, daemons, daemon_to_host


@pytest.fixture()
def mgr():
    """Provide a fake ceph mgr object preloaded with a configuration"""
    mgr = FakeMgr()
    mgr.cache.facts, mgr.cache.daemons, mgr.daemon_to_host = \
        generate_testdata(public_network='10.9.64.0/24', cluster_network='')
    mgr.module_option.update({
        "config_checks_enabled": True,
    })
    yield mgr


class FakeMgr:

    def __init__(self):
        self.datastore = {}
        self.module_option = {}
        self.health_checks = {}
        self.default_version = 'quincy'
        self.version_overrides = {}
        self.daemon_to_host = {}
        self.config_checks_enabled = True

        self.cache = HostCache(self)
        self.upgrade = CephadmUpgrade(self)

    def set_health_checks(self, checks: dict):
        return

    def get_module_option(self, keyname: str) -> Optional[str]:
        return self.module_option.get(keyname, None)

    def set_module_option(self, keyname: str, value: str) -> None:
        return None

    def get_store(self, keyname: str, default=None) -> Optional[str]:
        return self.datastore.get(keyname, None)

    def set_store(self, keyname: str, value: str) -> None:
        self.datastore[keyname] = value
        return None

    def _ceph_get_server(self) -> None:
        pass

    def get_metadata(self, daemon_type: str, daemon_id: str) -> Dict[str, Any]:
        key = f"{daemon_type}.{daemon_id}"
        if key in self.version_overrides:
            logger.debug(f"override applied for {key}")
            version_str = self.version_overrides[key]
        else:
            version_str = self.default_version

        return {"ceph_release": version_str, "hostname": self.daemon_to_host[key]}

    def list_servers(self) -> List[Dict[str, List[Dict[str, str]]]]:
        num_disks = host_sample['hdd_count']
        osd_num = 0
        service_map = []

        for hostname in self.cache.facts:

            host_num = int(hostname.split('.')[0].split('-')[1])
            svc_list = []
            for r in role_list(host_num):
                if r == 'osd':
                    for _n in range(num_disks):
                        svc_list.append({
                            "type": "osd",
                            "id": osd_num,
                        })
                        osd_num += 1
                else:
                    svc_list.append({
                        "type": r,
                        "id": hostname,
                    })

            service_map.append({"services": svc_list})
        logger.debug(f"services map - {json.dumps(service_map)}")
        return service_map

    def use_repo_digest(self) -> None:
        return None


class TestConfigCheck:

    def test_to_json(self, mgr):
        checker = CephadmConfigChecks(mgr)
        out = checker.to_json()
        assert out
        assert len(out) == len(checker.health_checks)

    def test_lookup_check(self, mgr):
        checker = CephadmConfigChecks(mgr)
        check = checker.lookup_check('osd_mtu_size')
        logger.debug(json.dumps(check.to_json()))
        assert check
        assert check.healthcheck_name == "CEPHADM_CHECK_MTU"

    def test_old_checks_removed(self, mgr):
        mgr.datastore.update({
            "config_checks": '{"bogus_one": "enabled", "bogus_two": "enabled", '
                             '"kernel_security": "enabled", "public_network": "enabled", '
                             '"kernel_version": "enabled", "network_missing": "enabled", '
                             '"osd_mtu_size": "enabled", "osd_linkspeed": "enabled", '
                             '"os_subscription": "enabled", "ceph_release": "enabled"}'
        })
        checker = CephadmConfigChecks(mgr)
        raw = mgr.get_store('config_checks')
        checks = json.loads(raw)
        assert "bogus_one" not in checks
        assert "bogus_two" not in checks
        assert len(checks) == len(checker.health_checks)

    def test_new_checks(self, mgr):
        mgr.datastore.update({
            "config_checks": '{"kernel_security": "enabled", "public_network": "enabled", '
                             '"osd_mtu_size": "enabled", "osd_linkspeed": "enabled"}'
        })
        checker = CephadmConfigChecks(mgr)
        raw = mgr.get_store('config_checks')
        checks = json.loads(raw)
        assert len(checks) == len(checker.health_checks)

    def test_no_issues(self, mgr):
        checker = CephadmConfigChecks(mgr)
        checker.cluster_network_list = []
        checker.public_network_list = ['10.9.64.0/24']
        checker.run_checks()

        assert not mgr.health_checks

    def test_no_public_network(self, mgr):
        bad_node = mgr.cache.facts['node-1.ceph.com']
        bad_node['interfaces']['eth0']['ipv4_address'] = "192.168.1.20/24"
        checker = CephadmConfigChecks(mgr)
        checker.cluster_network_list = []
        checker.public_network_list = ['10.9.64.0/24']
        checker.run_checks()
        logger.debug(mgr.health_checks)
        assert len(mgr.health_checks) == 1
        assert 'CEPHADM_CHECK_PUBLIC_MEMBERSHIP' in mgr.health_checks
        assert mgr.health_checks['CEPHADM_CHECK_PUBLIC_MEMBERSHIP']['detail'][0] == \
            'node-1.ceph.com does not have an interface on any public network'

    def test_missing_networks(self, mgr):

        checker = CephadmConfigChecks(mgr)
        checker.cluster_network_list = []
        checker.public_network_list = ['10.9.66.0/24']
        checker.run_checks()

        logger.info(json.dumps(mgr.health_checks))
        logger.info(checker.subnet_lookup)
        assert len(mgr.health_checks) == 1
        assert 'CEPHADM_CHECK_NETWORK_MISSING' in mgr.health_checks
        assert mgr.health_checks['CEPHADM_CHECK_NETWORK_MISSING']['detail'][0] == \
            "10.9.66.0/24 not found on any host in the cluster"

    def test_bad_mtu_single(self, mgr):

        bad_node = mgr.cache.facts['node-1.ceph.com']
        bad_node['interfaces']['eth0']['mtu'] = 1500

        checker = CephadmConfigChecks(mgr)
        checker.cluster_network_list = []
        checker.public_network_list = ['10.9.64.0/24']

        checker.run_checks()
        logger.info(json.dumps(mgr.health_checks))
        logger.info(checker.subnet_lookup)
        assert "CEPHADM_CHECK_MTU" in mgr.health_checks and len(mgr.health_checks) == 1
        assert mgr.health_checks['CEPHADM_CHECK_MTU']['detail'][0] == \
            'host node-1.ceph.com(eth0) is using MTU 1500 on 10.9.64.0/24, NICs on other hosts use 9000'

    def test_bad_mtu_multiple(self, mgr):

        for n in [1, 5]:
            bad_node = mgr.cache.facts[f'node-{n}.ceph.com']
            bad_node['interfaces']['eth0']['mtu'] = 1500

        checker = CephadmConfigChecks(mgr)
        checker.cluster_network_list = []
        checker.public_network_list = ['10.9.64.0/24']

        checker.run_checks()
        logger.info(json.dumps(mgr.health_checks))
        logger.info(checker.subnet_lookup)
        assert "CEPHADM_CHECK_MTU" in mgr.health_checks and len(mgr.health_checks) == 1
        assert mgr.health_checks['CEPHADM_CHECK_MTU']['count'] == 2

    def test_bad_linkspeed_single(self, mgr):

        bad_node = mgr.cache.facts['node-1.ceph.com']
        bad_node['interfaces']['eth0']['speed'] = 100

        checker = CephadmConfigChecks(mgr)
        checker.cluster_network_list = []
        checker.public_network_list = ['10.9.64.0/24']

        checker.run_checks()
        logger.info(json.dumps(mgr.health_checks))
        logger.info(checker.subnet_lookup)
        assert mgr.health_checks
        assert "CEPHADM_CHECK_LINKSPEED" in mgr.health_checks and len(mgr.health_checks) == 1
        assert mgr.health_checks['CEPHADM_CHECK_LINKSPEED']['detail'][0] == \
            'host node-1.ceph.com(eth0) has linkspeed of 100 on 10.9.64.0/24, NICs on other hosts use 1000'

    def test_super_linkspeed_single(self, mgr):

        bad_node = mgr.cache.facts['node-1.ceph.com']
        bad_node['interfaces']['eth0']['speed'] = 10000

        checker = CephadmConfigChecks(mgr)
        checker.cluster_network_list = []
        checker.public_network_list = ['10.9.64.0/24']

        checker.run_checks()
        logger.info(json.dumps(mgr.health_checks))
        logger.info(checker.subnet_lookup)
        assert not mgr.health_checks

    def test_release_mismatch_single(self, mgr):

        mgr.version_overrides = {
            "osd.1": "pacific",
        }

        checker = CephadmConfigChecks(mgr)
        checker.cluster_network_list = []
        checker.public_network_list = ['10.9.64.0/24']

        checker.run_checks()
        logger.info(json.dumps(mgr.health_checks))
        assert mgr.health_checks
        assert "CEPHADM_CHECK_CEPH_RELEASE" in mgr.health_checks and len(mgr.health_checks) == 1
        assert mgr.health_checks['CEPHADM_CHECK_CEPH_RELEASE']['detail'][0] == \
            'osd.1 is running pacific (majority of cluster is using quincy)'

    def test_release_mismatch_multi(self, mgr):

        mgr.version_overrides = {
            "osd.1": "pacific",
            "osd.5": "octopus",
        }

        checker = CephadmConfigChecks(mgr)
        checker.cluster_network_list = []
        checker.public_network_list = ['10.9.64.0/24']

        checker.run_checks()
        logger.info(json.dumps(mgr.health_checks))
        assert mgr.health_checks
        assert "CEPHADM_CHECK_CEPH_RELEASE" in mgr.health_checks and len(mgr.health_checks) == 1
        assert len(mgr.health_checks['CEPHADM_CHECK_CEPH_RELEASE']['detail']) == 2

    def test_kernel_mismatch(self, mgr):

        bad_host = mgr.cache.facts['node-1.ceph.com']
        bad_host['kernel'] = "5.10.18.0-241.10.1.el8.x86_64"

        checker = CephadmConfigChecks(mgr)
        checker.cluster_network_list = []
        checker.public_network_list = ['10.9.64.0/24']

        checker.run_checks()
        logger.info(json.dumps(mgr.health_checks))
        assert len(mgr.health_checks) == 1
        assert 'CEPHADM_CHECK_KERNEL_VERSION' in mgr.health_checks
        assert mgr.health_checks['CEPHADM_CHECK_KERNEL_VERSION']['detail'][0] == \
            "host node-1.ceph.com running kernel 5.10, majority of hosts(9) running 4.18"
        assert mgr.health_checks['CEPHADM_CHECK_KERNEL_VERSION']['count'] == 1

    def test_inconsistent_subscription(self, mgr):

        bad_host = mgr.cache.facts['node-5.ceph.com']
        bad_host['subscribed'] = "no"

        checker = CephadmConfigChecks(mgr)
        checker.cluster_network_list = []
        checker.public_network_list = ['10.9.64.0/24']

        checker.run_checks()
        logger.info(json.dumps(mgr.health_checks))
        assert len(mgr.health_checks) == 1
        assert "CEPHADM_CHECK_SUBSCRIPTION" in mgr.health_checks
        assert mgr.health_checks['CEPHADM_CHECK_SUBSCRIPTION']['detail'][0] == \
            "node-5.ceph.com does not have an active subscription"

    def test_kernel_security_inconsistent(self, mgr):

        bad_node = mgr.cache.facts['node-3.ceph.com']
        bad_node['kernel_security'] = {
            "SELINUX": "permissive",
            "SELINUXTYPE": "targeted",
            "description": "SELinux: Enabled(permissive, targeted)",
            "type": "SELinux"
        }
        checker = CephadmConfigChecks(mgr)
        checker.cluster_network_list = []
        checker.public_network_list = ['10.9.64.0/24']

        checker.run_checks()
        logger.info(json.dumps(mgr.health_checks))
        assert len(mgr.health_checks) == 1
        assert 'CEPHADM_CHECK_KERNEL_LSM' in mgr.health_checks
        assert mgr.health_checks['CEPHADM_CHECK_KERNEL_LSM']['detail'][0] == \
            "node-3.ceph.com has inconsistent KSM settings compared to the majority of hosts(9) in the cluster"

    def test_release_and_bad_mtu(self, mgr):

        mgr.version_overrides = {
            "osd.1": "pacific",
        }
        bad_node = mgr.cache.facts['node-1.ceph.com']
        bad_node['interfaces']['eth0']['mtu'] = 1500

        checker = CephadmConfigChecks(mgr)
        checker.cluster_network_list = []
        checker.public_network_list = ['10.9.64.0/24']

        checker.run_checks()
        logger.info(json.dumps(mgr.health_checks))
        logger.info(checker.subnet_lookup)
        assert mgr.health_checks
        assert len(mgr.health_checks) == 2
        assert "CEPHADM_CHECK_CEPH_RELEASE" in mgr.health_checks and \
            "CEPHADM_CHECK_MTU" in mgr.health_checks

    def test_release_mtu_LSM(self, mgr):

        mgr.version_overrides = {
            "osd.1": "pacific",
        }
        bad_node1 = mgr.cache.facts['node-1.ceph.com']
        bad_node1['interfaces']['eth0']['mtu'] = 1500
        bad_node2 = mgr.cache.facts['node-3.ceph.com']
        bad_node2['kernel_security'] = {
            "SELINUX": "permissive",
            "SELINUXTYPE": "targeted",
            "description": "SELinux: Enabled(permissive, targeted)",
            "type": "SELinux"
        }
        checker = CephadmConfigChecks(mgr)
        checker.cluster_network_list = []
        checker.public_network_list = ['10.9.64.0/24']

        checker.run_checks()
        logger.info(json.dumps(mgr.health_checks))
        logger.info(checker.subnet_lookup)
        assert mgr.health_checks
        assert len(mgr.health_checks) == 3
        assert \
            "CEPHADM_CHECK_CEPH_RELEASE" in mgr.health_checks and \
            "CEPHADM_CHECK_MTU" in mgr.health_checks and \
            "CEPHADM_CHECK_KERNEL_LSM" in mgr.health_checks

    def test_release_mtu_LSM_subscription(self, mgr):

        mgr.version_overrides = {
            "osd.1": "pacific",
        }
        bad_node1 = mgr.cache.facts['node-1.ceph.com']
        bad_node1['interfaces']['eth0']['mtu'] = 1500
        bad_node1['subscribed'] = "no"
        bad_node2 = mgr.cache.facts['node-3.ceph.com']
        bad_node2['kernel_security'] = {
            "SELINUX": "permissive",
            "SELINUXTYPE": "targeted",
            "description": "SELinux: Enabled(permissive, targeted)",
            "type": "SELinux"
        }
        checker = CephadmConfigChecks(mgr)
        checker.cluster_network_list = []
        checker.public_network_list = ['10.9.64.0/24']

        checker.run_checks()
        logger.info(json.dumps(mgr.health_checks))
        logger.info(checker.subnet_lookup)
        assert mgr.health_checks
        assert len(mgr.health_checks) == 4
        assert \
            "CEPHADM_CHECK_CEPH_RELEASE" in mgr.health_checks and \
            "CEPHADM_CHECK_MTU" in mgr.health_checks and \
            "CEPHADM_CHECK_KERNEL_LSM" in mgr.health_checks and \
            "CEPHADM_CHECK_SUBSCRIPTION" in mgr.health_checks

    def test_skip_release_during_upgrade(self, mgr):
        mgr.upgrade.upgrade_state = UpgradeState.from_json({
            'target_name': 'wah',
            'progress_id': str(uuid.uuid4()),
            'target_id': 'wah',
            'error': '',
            'paused': False,
        })
        checker = CephadmConfigChecks(mgr)
        checker.cluster_network_list = []
        checker.public_network_list = ['10.9.64.0/24']

        checker.run_checks()
        logger.info(f"{checker.skipped_checks_count} skipped check(s): {checker.skipped_checks}")
        assert checker.skipped_checks_count == 1
        assert 'ceph_release' in checker.skipped_checks

    def test_skip_when_disabled(self, mgr):
        mgr.config_checks_enabled = False
        checker = CephadmConfigChecks(mgr)
        checker.cluster_network_list = []
        checker.public_network_list = ['10.9.64.0/24']

        checker.run_checks()
        logger.info(checker.active_checks)
        logger.info(checker.defined_checks)
        assert checker.active_checks_count == 0

    def test_skip_mtu_checks(self, mgr):
        mgr.datastore.update({
            'config_checks': '{"osd_mtu_size": "disabled"}'
        })

        checker = CephadmConfigChecks(mgr)
        checker.cluster_network_list = []
        checker.public_network_list = ['10.9.64.0/24']

        checker.run_checks()
        logger.info(checker.active_checks)
        logger.info(checker.defined_checks)
        assert 'osd_mtu_size' not in checker.active_checks
        assert checker.defined_checks == 8 and checker.active_checks_count == 7

    def test_skip_mtu_lsm_checks(self, mgr):
        mgr.datastore.update({
            'config_checks': '{"osd_mtu_size": "disabled", "kernel_security": "disabled"}'
        })

        checker = CephadmConfigChecks(mgr)
        checker.cluster_network_list = []
        checker.public_network_list = ['10.9.64.0/24']

        checker.run_checks()
        logger.info(checker.active_checks)
        logger.info(checker.defined_checks)
        assert 'osd_mtu_size' not in checker.active_checks and \
            'kernel_security' not in checker.active_checks
        assert checker.defined_checks == 8 and checker.active_checks_count == 6
        assert not mgr.health_checks
