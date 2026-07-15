

from typing import Dict, List
from unittest.mock import MagicMock

from cephadm.services.cephadmservice import MonService
from cephadm.module import CephadmOrchestrator
from orchestrator._interface import DaemonDescription
from ceph.deployment.service_spec import ServiceSpec


class FakeInventory:
    def get_addr(self, name: str) -> str:
        return '1.2.3.4'


class FakeMgr:
    def __init__(self):
        self.config = ''
        self.set_mon_crush_locations: Dict[str, List[str]] = {}
        self.check_mon_command = MagicMock(side_effect=self._check_mon_command)
        self.mon_command = MagicMock(side_effect=self._check_mon_command)
        self.template = MagicMock()
        self.log = MagicMock()
        self.cert_mgr = MagicMock()
        self.inventory = FakeInventory()

    def _check_mon_command(self, cmd_dict, inbuf=None):
        prefix = cmd_dict.get('prefix')
        if prefix == 'get-cmd':
            return 0, self.config, ''
        if prefix == 'set-cmd':
            self.config = cmd_dict.get('value')
            return 0, 'value set', ''
        if prefix in ['auth get']:
            return 0, '[foo]\nkeyring = asdf\n', ''
        if prefix == 'quorum_status':
            # actual quorum status output from testing
            # note in this output all of the mons have blank crush locations
            return 0, """{"election_epoch": 14, "quorum": [0, 1, 2], "quorum_names": ["vm-00", "vm-01", "vm-02"], "quorum_leader_name": "vm-00", "quorum_age": 101, "features": {"quorum_con": "4540138322906710015", "quorum_mon": ["kraken", "luminous", "mimic", "osdmap-prune", "nautilus", "octopus", "pacific", "elector-pinging", "quincy", "reef"]}, "monmap": {"epoch": 3, "fsid": "9863e1b8-6f24-11ed-8ad8-525400c13ad2", "modified": "2022-11-28T14:00:29.972488Z", "created": "2022-11-28T13:57:55.847497Z", "min_mon_release": 18, "min_mon_release_name": "reef", "election_strategy": 1, "disallowed_leaders: ": "", "stretch_mode": false, "tiebreaker_mon": "", "features": {"persistent": ["kraken", "luminous", "mimic", "osdmap-prune", "nautilus", "octopus", "pacific", "elector-pinging", "quincy", "reef"], "optional": []}, "mons": [{"rank": 0, "name": "vm-00", "public_addrs": {"addrvec": [{"type": "v2", "addr": "192.168.122.61:3300", "nonce": 0}, {"type": "v1", "addr": "192.168.122.61:6789", "nonce": 0}]}, "addr": "192.168.122.61:6789/0", "public_addr": "192.168.122.61:6789/0", "priority": 0, "weight": 0, "crush_location": "{}"}, {"rank": 1, "name": "vm-01", "public_addrs": {"addrvec": [{"type": "v2", "addr": "192.168.122.63:3300", "nonce": 0}, {"type": "v1", "addr": "192.168.122.63:6789", "nonce": 0}]}, "addr": "192.168.122.63:6789/0", "public_addr": "192.168.122.63:6789/0", "priority": 0, "weight": 0, "crush_location": "{}"}, {"rank": 2, "name": "vm-02", "public_addrs": {"addrvec": [{"type": "v2", "addr": "192.168.122.82:3300", "nonce": 0}, {"type": "v1", "addr": "192.168.122.82:6789", "nonce": 0}]}, "addr": "192.168.122.82:6789/0", "public_addr": "192.168.122.82:6789/0", "priority": 0, "weight": 0, "crush_location": "{}"}]}}""", ''
        if prefix == 'mon set_location':
            self.set_mon_crush_locations[cmd_dict.get('name')] = cmd_dict.get('args')
            return 0, '', ''
        return -1, '', 'error'

    def get_minimal_ceph_conf(self) -> str:
        return ''

    def get_mgr_ip(self) -> str:
        return '1.2.3.4'


class TestMonService:

    def test_set_crush_locations(self, cephadm_module: CephadmOrchestrator):
        mgr = FakeMgr()
        mon_service = MonService(mgr)
        mon_spec = ServiceSpec(service_type='mon', crush_locations={'vm-00': ['datacenter=a', 'rack=1'], 'vm-01': ['datacenter=a'], 'vm-02': ['datacenter=b', 'rack=3']})

        mon_daemons = [
            DaemonDescription(daemon_type='mon', daemon_id='vm-00', hostname='vm-00'),
            DaemonDescription(daemon_type='mon', daemon_id='vm-01', hostname='vm-01'),
            DaemonDescription(daemon_type='mon', daemon_id='vm-02', hostname='vm-02')
        ]
        mon_service.set_crush_locations(mon_daemons, mon_spec)
        assert 'vm-00' in mgr.set_mon_crush_locations
        assert mgr.set_mon_crush_locations['vm-00'] == ['datacenter=a', 'rack=1']
        assert 'vm-01' in mgr.set_mon_crush_locations
        assert mgr.set_mon_crush_locations['vm-01'] == ['datacenter=a']
        assert 'vm-02' in mgr.set_mon_crush_locations
        assert mgr.set_mon_crush_locations['vm-02'] == ['datacenter=b', 'rack=3']
